---

### **实时数据缓存服务 - 技术设计规约 (TDD) - Python版本**

**版本**: 1.0
**日期**: 2025-06-27

---

### **1. 项目概述**

#### 1.1. 项目目标
本项目旨在创建一个独立的后台服务，用于实时缓存工业生产数据。服务将从上游的 SQL Server 数据库中拉取数据，并将其存储在一个本地的、高性能的嵌入式数据库文件中。该服务需要保证本地缓存中始终只保留最近三天的时序数据，并为下游的数据分析工具（如 Python 脚本）提供一个稳定、高效的直接数据查询接口。

#### 1.2. 核心功能需求
* **数据源**: 从 SQL Server 的`历史表`和`TagDatabase`表获取数据。
* **数据存储**: 使用嵌入式数据库文件（DuckDB）作为本地缓存。
* **数据窗口**: 严格维护一个3天（72小时）的滚动数据窗口。
* **启动行为**: 服务启动时，清空历史缓存，并从 SQL Server 全量拉取最近3天的数据作为初始状态。
* **运行时行为**: 服务以10秒为周期，增量拉取最新数据，写入本地缓存，并清理超出时间窗口的过期数据。
* **查询接口**: 本地数据库文件本身即为查询接口，允许第三方工具并发只读访问。

---

### **2. 系统架构**

系统由三个主要部分构成：数据源、数据处理服务和数据消费端。

* **数据源 (Upstream)**: Microsoft SQL Server 数据库。
* **数据处理服务 (Core Service)**: 本项目需要构建的 Python 后台服务。它作为数据管道的中心，负责调度、转换和维护本地缓存。
* **数据存储与消费端 (Downstream & Storage)**: 一个本地的 DuckDB 数据库文件 (`.duckdb`)。Python 脚本、数据分析工具或任何支持 DuckDB 的客户端可以直接连接此文件进行数据分析和查询。

**数据流:**
1.  **初始化流程**: `SQL Server` -> `Python 服务` -> `DuckDB 文件`
2.  **增量更新流程**: `SQL Server` -> `Python 服务` -> `DuckDB 文件`
3.  **查询流程**: `Python/分析工具` -> `DuckDB 文件`

---

### **3. 技术选型详述**

| 分类 | 技术/库 | 选用理由 | 关键配置/特性 |
| :--- | :--- | :--- | :--- |
| **编程语言** | Python 3.11+ | 成熟的数据处理生态系统，丰富的数据库驱动和异步支持，便于快速开发和维护。 | 使用 `asyncio` 进行异步编程 |
| **异步框架** | `asyncio` + `aiofiles` | Python 标准库的异步运行时，提供任务调度、定时器、I/O 等核心功能。 | 使用 `asyncio.create_task()` 进行任务管理 |
| **上游数据库驱动** | `aioodbc` + `pyodbc` | 支持异步操作的 ODBC 驱动，与 SQL Server 兼容性最佳，支持连接池管理。 | 配置连接池大小和超时参数 |
| **本地缓存数据库** | `duckdb` | **核心选型**。高性能的嵌入式分析型（OLAP）数据库。其列式存储结构对时序数据分析有巨大性能优势。支持并发读写（MVCC），非常适合本项目的"一写多读"场景。 | 使用 Python `duckdb` 库，支持批量插入和事务管理 |
| **时间处理** | `datetime` + `pytz` | Python 标准库的时间处理模块配合时区库，用于精确处理时间戳和时间窗口计算。 | 统一使用 UTC 时间进行存储和计算 |
| **配置管理** | `pydantic` + `pydantic-settings` | 基于类型注解的配置管理，提供数据验证、环境变量支持和配置文件加载。 | 支持 TOML、YAML、JSON 等多种配置格式 |
| **日志框架** | `loguru` | 现代化的 Python 日志库，提供结构化日志、异步日志和丰富的格式化选项。 | 配置为输出 JSON 格式，便于日志聚合系统的采集和分析 |
| **数据处理** | `polars` | 高性能的数据处理库，基于 Rust 实现，提供快速的数据转换和批量操作能力。 | 用于大批量数据的预处理和转换 |
| **进程管理** | `supervisor` 或 `systemd` | 用于生产环境的进程守护和自动重启，确保服务的高可用性。 | 配置自动重启策略和资源限制 |

---

### **4. 数据模型与数据库 Schema**

#### 4.1. 数据库文件
* **数据库类型**: DuckDB
* **默认文件名**: `realtime_data.duckdb` (应在配置中可指定)

#### 4.2. 表结构 (Schema)
系统将在 DuckDB 文件中维护一个核心数据表。

* **表名**: `ts_data`

| 列名 (Column) | 数据类型 (SQL) | 描述 | 约束/备注 |
| :--- | :--- | :--- | :--- |
| `tag_name` | `VARCHAR` | 工业数据点的标签名/位号。 | `NOT NULL` |
| `ts` | `TIMESTAMP` | 数据点的时间戳 (UTC 标准)。 | `NOT NULL` |
| `value` | `DOUBLE` | 数据点的数值。 | `NOT NULL` |

#### 4.3. 索引 (Index)
为了优化查询和数据清理（删除）的性能，需要创建一个复合索引。

* **索引名**: `idx_tag_ts`
* **索引类型**: 标准 B-Tree 索引
* **索引列**: `(tag_name, ts)`
* **目的**:
    1.  加速按 `tag_name` 和时间范围的查询。
    2.  加速按 `ts` 删除过期数据的 `DELETE` 操作。

---

### **5. 核心组件功能规约**

#### 5.1. 配置模块 (`config.py`)
* **职责**: 提供统一的配置加载和访问接口。
* **实现细节**:
    1.  在项目根目录定义一个 `config.toml` 文件模板。
    2.  定义一个 Pydantic `BaseSettings` 类，支持从配置文件和环境变量加载配置。
    3.  **必须包含的配置项**:
        * `database_url`: 上游 SQL Server 的连接字符串。
        * `update_interval_secs`: 增量更新周期，单位为秒（例如: 10）。
        * `data_window_days`: 数据保留窗口，单位为天（例如: 3）。
        * `db_file_path`: 本地 DuckDB 文件的路径。
        * `log_level`: 日志级别配置。
        * `max_retry_attempts`: 最大重试次数。

#### 5.2. 应用入口与主流程 (`main.py`)
* **职责**: 初始化服务，协调各个模块的启动和关闭。
* **执行流程**:
    1.  **初始化**:
        * 启动 `loguru` 日志系统。
        * 调用配置模块，加载并验证应用配置。
        * 设置异常处理和信号监听。
    2.  **数据库准备**:
        * 根据配置的 `db_file_path`，强制删除已存在的旧数据库文件。
        * 创建并打开一个新的 DuckDB 数据库连接。
        * 在该连接上执行 SQL，创建 `ts_data` 表和 `idx_tag_ts` 索引。
        * 关闭此初始化连接。
    3.  **任务调度**:
        * 使用 `asyncio.run()` 启动主异步循环。
        * 同步执行**初始数据加载模块**，等待其完成后再进行下一步。
        * 将**周期性更新模块**作为一个新的异步任务（`asyncio.create_task()`）在后台启动。
    4.  **优雅停机**:
        * 监听操作系统的终止信号 (SIGINT, SIGTERM)。
        * 收到信号后，记录停机日志，取消正在执行的任务，然后退出进程。

#### 5.3. 初始数据加载模块 (`initial_loader.py`)
* **职责**: 在服务启动时，一次性从 SQL Server 拉取历史数据以填充本地缓存。
* **执行流程**:
    1.  使用 `aioodbc` 建立到 SQL Server 的异步连接。
    2.  根据配置的 `data_window_days`，计算出起始时间戳。
    3.  构造 SQL 查询语句，从 `历史表` 中 `SELECT` 所有时间戳大于等于起始时间戳的数据。
    4.  执行查询，获取结果流。
    5.  打开到本地 DuckDB 文件的连接。
    6.  **使用 DuckDB 的批量插入功能** 进行高性能批量写入。
    7.  使用 `polars` 进行数据预处理和格式转换。
    8.  分批次处理数据，避免内存溢出。
    9.  完成处理后，提交事务并关闭数据库连接。

#### 5.4. 周期性更新模块 (`periodic_updater.py`)
* **职责**: 定期从 SQL Server 拉取增量数据，并清理本地缓存中的过期数据。
* **执行流程**:
    1.  在模块内部维护一个状态变量 `last_seen_timestamp`，初始化为服务启动时间。
    2.  启动一个基于 `update_interval_secs` 配置的 `asyncio` 定时器。
    3.  **在每个定时器周期内，执行以下原子性操作**:
        a. **拉取增量**:
            i.   使用 `aioodbc` 连接到 SQL Server。
            ii.  `SELECT` `TagDatabase` 表中 `Timestamp` 大于 `last_seen_timestamp` 的所有记录。
            iii. 如果查询到新数据，则使用批量插入将其全部写入本地 DuckDB 文件。
            iv.  更新 `last_seen_timestamp` 为本批次数据中的最大时间戳。
        b. **清理过期数据 (Pruning)**:
            i.  计算清理截止时间点 `cutoff_ts` (当前时间 - `data_window_days`)。
            ii. 在本地 DuckDB 连接上，执行 `DELETE FROM ts_data WHERE ts < ?`，参数为 `cutoff_ts`。
            iii. 记录被删除的行数。
        c. **资源释放**: 确保在此周期内打开的所有数据库连接都已关闭。

---

### **6. 外部接口规约**

#### 6.1. 上游接口 (SQL Server)
* **依赖表 1**: `历史表` (用于初始加载)
* **依赖表 2**: `TagDatabase` (用于增量更新)
* **所需列**: `TagName` (NVARCHAR), `Timestamp` (DATETIME2/TIMESTAMP), `Value` (FLOAT/REAL/DECIMAL)。
* **访问权限**: 服务运行账户必须拥有对上述表的只读 (`SELECT`) 权限。

#### 6.2. 下游接口 (DuckDB 文件)
* **接口形式**: 数据库文件本身。
* **访问协议**: 标准 DuckDB 连接协议。
* **支持的客户端**: Python `duckdb` 库、DBeaver、命令行工具等。
* **并发模型**: 服务本身对文件进行"多读一写"操作。外部客户端应以**只读模式**连接，以避免文件锁定冲突并确保最高性能。DuckDB 的 MVCC 机制能保证外部读取操作不会被内部写入长时间阻塞。

---

### **7. 非功能性需求**

#### 7.1. 错误处理与韧性
* 所有 I/O 操作（数据库连接、查询、文件写入）都必须有完善的错误处理逻辑。
* 对于可恢复的错误（如数据库瞬时连接中断），应实施有限次数的**重试机制**（例如，间隔5秒重试3次）。
* 使用 `tenacity` 库实现装饰器式的重试逻辑。
* 若重试后依然失败，应记录详细的 `ERROR` 级别日志，并继续下一个更新周期，而不是使服务崩溃。

#### 7.2. 日志规范
* **日志级别**:
    * `INFO`: 记录关键生命周期事件（服务启停、任务开始/结束、加载/删除的数据量）。
    * `WARNING`: 记录非致命性问题（如重试操作）。
    * `ERROR`: 记录所有导致功能失败的错误。
    * `DEBUG`: 用于开发阶段的详细信息追踪。
* **日志内容**: 每条日志应包含时间戳、日志级别、模块来源以及结构化的消息内容。
* **日志格式**: 建议输出为 **JSON** 格式，使用 `loguru` 的结构化日志功能。

#### 7.3. 部署与运维
* **依赖管理**: 使用 `requirements.txt` 或 `pyproject.toml` 管理项目依赖。
* **虚拟环境**: 推荐使用 `venv` 或 `conda` 创建独立的 Python 环境。
* **运行方式**: 推荐将此 Python 服务注册为操作系统服务（如 Linux `systemd` 或使用 `supervisor`），以实现开机自启、进程守护和自动重启。
* **容器化**: 支持 Docker 容器化部署，提供 `Dockerfile` 和 `docker-compose.yml`。
* **资源占用**: 服务应是轻量级的，主要 CPU 消耗集中在更新周期的短暂瞬间，内存占用主要由数据处理和 DuckDB 自身管理。需监控其 CPU 和内存使用情况。
* **健康检查**: 提供 HTTP 健康检查端点，便于监控系统检测服务状态。

---

### **8. 项目结构**

```
rt_db_python/
├── src/
│   ├── __init__.py
│   ├── main.py                 # 应用入口
│   ├── config.py              # 配置管理
│   ├── database/
│   │   ├── __init__.py
│   │   ├── sql_server.py      # SQL Server 连接和操作
│   │   └── duckdb_manager.py  # DuckDB 管理
│   ├── services/
│   │   ├── __init__.py
│   │   ├── initial_loader.py  # 初始数据加载
│   │   └── periodic_updater.py # 周期性更新
│   └── utils/
│       ├── __init__.py
│       ├── logger.py          # 日志配置
│       └── retry.py           # 重试机制
├── config.toml                # 配置文件
├── requirements.txt           # 依赖列表
├── Dockerfile                 # Docker 构建文件
├── docker-compose.yml         # Docker Compose 配置
├── systemd/
│   └── rt-db-service.service  # systemd 服务配置
└── README.md                  # 项目说明
```

---

### **9. 开发和部署指南**

#### 9.1. 开发环境设置
```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 运行服务
python src/main.py
```

#### 9.2. 生产环境部署
```bash
# 使用 systemd 部署
sudo cp systemd/rt-db-service.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable rt-db-service
sudo systemctl start rt-db-service

# 或使用 Docker 部署
docker-compose up -d
```

#### 9.3. 监控和维护
* 使用 `systemctl status rt-db-service` 检查服务状态
* 查看日志：`journalctl -u rt-db-service -f`
* 监控数据库文件大小和服务资源使用情况
* 定期检查数据完整性和时间窗口维护情况