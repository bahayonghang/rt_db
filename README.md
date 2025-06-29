# rt_db

一个高性能的工业时间序列数据实时同步服务，专为工业数据处理场景设计，支持从 SQL Server 实时同步数据到本地 DuckDB 数据库，并提供宽表格式的高效存储。

## 功能特性

- **实时数据同步**: 从 SQL Server 历史表和 TagDatabase 表定期拉取工业数据
- **宽表存储**: 自动将时序数据转换为宽表格式，优化查询性能
- **动态列管理**: 自动检测新标签并动态添加列到宽表
- **滚动数据窗口**: 自动维护指定天数的数据窗口，定期清理过期数据
- **高性能存储**: 使用 DuckDB 列式存储，支持高效的时序数据查询
- **并发访问**: 支持多个客户端同时只读访问本地数据库
- **容错机制**: 内置重试机制、连接池和错误恢复
- **优雅停机**: 支持 SIGTERM/SIGINT 信号处理和优雅关闭
- **结构化日志**: 使用 tracing 框架提供详细的结构化日志
- **灵活配置**: 支持连接字符串和结构化配置两种模式

## 系统架构

### 整体架构图

```mermaid
graph TB
    subgraph "上游数据源 (SQL Server)"
        HT["历史表<br/>(初始数据加载)"]
        TDB["TagDatabase表<br/>(增量更新)"]
    end
    
    subgraph "Rust 数据同步服务"
        subgraph "核心模块"
            CFG["配置模块<br/>(config.rs)"]
            DS["数据源模块<br/>(data_source.rs)"]
            DB["数据库管理<br/>(database.rs)"]
            SYNC["同步服务<br/>(sync_service.rs)"]
            MAIN["主程序<br/>(main.rs)"]
        end
        
        subgraph "数据处理流程"
            INIT["初始数据加载"]
            PERIOD["周期性更新"]
            CLEAN["过期数据清理"]
        end
    end
    
    subgraph "本地存储 (DuckDB)"
        DUCK[("realtime_data.duckdb<br/>ts_wide表")]
        IDX["索引: idx_datetime<br/>(DateTime)"]
    end
    
    subgraph "数据消费端"
        PY["Python 脚本"]
        DBA["DBeaver"]
        OTHER["其他分析工具"]
    end
    
    %% 数据流连接
    HT -->|"全量拉取<br/>(启动时)"| DS
    TDB -->|"增量拉取<br/>(10秒周期)"| DS
    
    DS --> SYNC
    SYNC --> DB
    DB --> DUCK
    DUCK --> IDX
    
    %% 查询连接
    DUCK -.->|"只读访问"| PY
    DUCK -.->|"只读访问"| DBA
    DUCK -.->|"只读访问"| OTHER
    
    %% 配置连接
    CFG -.-> DS
    CFG -.-> DB
    CFG -.-> SYNC
    
    %% 主程序协调
    MAIN --> CFG
    MAIN --> INIT
    MAIN --> PERIOD
    MAIN --> CLEAN
    
    %% 样式定义
    classDef sqlserver fill:#e1f5fe
    classDef rust fill:#fff3e0
    classDef duckdb fill:#f3e5f5
    classDef consumer fill:#e8f5e8
    
    class HT,TDB sqlserver
    class CFG,DS,DB,SYNC,MAIN,INIT,PERIOD,CLEAN rust
    class DUCK,IDX duckdb
    class PY,DBA,OTHER consumer
```

### 数据流时序图

```mermaid
sequenceDiagram
    participant SQL as SQL Server
    participant RS as Rust Service
    participant Duck as DuckDB
    participant Client as 客户端工具
    
    Note over RS: 服务启动
    RS->>Duck: 初始化数据库和表结构
    RS->>Duck: 创建ts_wide表和索引
    
    Note over RS: 初始数据加载
    RS->>SQL: 查询历史表(指定天数)
    SQL-->>RS: 返回历史数据
    RS->>RS: 转换为宽表格式
    RS->>Duck: 批量插入宽表数据
    
    Note over RS: 周期性更新
    loop 每个更新周期
        RS->>SQL: 查询TagDatabase增量数据
        SQL-->>RS: 返回新数据
        alt 有新数据
            RS->>Duck: 动态添加新列(如需要)
            RS->>Duck: 追加新数据到宽表
        end
        RS->>Duck: 清理过期数据
    end
    
    Note over Client: 数据查询
    Client->>Duck: 只读查询宽表
    Duck-->>Client: 返回结果
```

### 核心组件交互图

```mermaid
graph LR
    subgraph "配置层"
        CONFIG["config.toml"]
        APPCONFIG["AppConfig结构体"]
    end
    
    subgraph "数据访问层"
        SQLCONN["SQL Server连接"]
        DUCKCONN["DuckDB连接"]
    end
    
    subgraph "业务逻辑层"
        DATASOURCE["SqlServerDataSource"]
        DBMANAGER["DatabaseManager"]
        SYNCSERVICE["SyncService"]
    end
    
    subgraph "数据模型"
        TSRECORD["TimeSeriesRecord"]
    end
    
    CONFIG --> APPCONFIG
    APPCONFIG --> DATASOURCE
    APPCONFIG --> DBMANAGER
    APPCONFIG --> SYNCSERVICE
    
    DATASOURCE --> SQLCONN
    DBMANAGER --> DUCKCONN
    
    SQLCONN --> TSRECORD
    TSRECORD --> DUCKCONN
    
    SYNCSERVICE --> DATASOURCE
    SYNCSERVICE --> DBMANAGER
    
    classDef config fill:#fff2cc
    classDef data fill:#d5e8d4
    classDef business fill:#dae8fc
    classDef model fill:#f8cecc
    
    class CONFIG,APPCONFIG config
    class SQLCONN,DUCKCONN data
    class DATASOURCE,DBMANAGER,SYNCSERVICE business
    class TSRECORD model
```

## 快速开始

### 1. 环境要求

- Rust 1.70+ (推荐使用最新稳定版)
- SQL Server 数据库访问权限
- Windows/Linux 操作系统

### 2. 配置设置

复制并修改配置文件：

```bash
cp config.toml.example config.toml
```

编辑 `config.toml`：

#### 方式一：使用连接字符串（推荐）

```toml
# 数据库连接方式
database_connection_type = "ConnectionString"

# SQL Server 数据库连接字符串
database_url = "server=tcp:your-server,1433;database=YourDatabase;user=YourUser;password=YourPassword;TrustServerCertificate=true"

# 增量更新周期，单位为秒
update_interval_secs = 10

# 数据保留窗口，单位为天
data_window_days = 3

# 本地 DuckDB 文件路径
db_file_path = "./realtime_data.duckdb"

# 日志级别
log_level = "info"

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[query]
days_back = 3
history_table = "历史表"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30
```

#### 方式二：使用结构化配置

```toml
# 数据库连接方式
database_connection_type = "StructuredConfig"

# 增量更新周期，单位为秒
update_interval_secs = 10

# 数据保留窗口，单位为天
data_window_days = 3

# 本地 DuckDB 文件路径
db_file_path = "./realtime_data.duckdb"

# 日志级别
log_level = "info"

[database]
server = "your-server"
port = 1433
database = "YourDatabase"
user = "YourUser"
password = "YourPassword"
trust_server_certificate = true

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[query]
days_back = 3
history_table = "历史表"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30
```

### 3. 编译和运行

```bash
# 编译项目
cargo build --release

# 运行服务
cargo run --release

# 或直接运行编译后的二进制文件
./target/release/rt_db
```

### 4. 数据访问

服务运行后，可以通过多种方式访问本地缓存的数据：

#### Python 示例

```python
import duckdb
import pandas as pd

# 连接到本地数据库文件
conn = duckdb.connect('realtime_data.duckdb', read_only=True)

# 查询最近1小时的数据（宽表格式）
query = """
SELECT * 
FROM ts_wide 
WHERE DateTime >= NOW() - INTERVAL '1 hour'
ORDER BY DateTime DESC
"""

df = conn.execute(query).fetchdf()
print(df.head())

# 查询特定标签的数据
query_specific = """
SELECT DateTime, tag_1, tag_2
FROM ts_wide 
WHERE DateTime >= NOW() - INTERVAL '1 hour'
  AND (tag_1 IS NOT NULL OR tag_2 IS NOT NULL)
ORDER BY DateTime DESC
"""

df_specific = conn.execute(query_specific).fetchdf()
print(df_specific.head())

conn.close()
```

#### DBeaver 连接

1. 创建新的 DuckDB 连接
2. 设置数据库文件路径为 `realtime_data.duckdb`
3. 连接模式设置为只读

## 数据库结构

### ts_wide 表（宽表格式）

| 列名 | 类型 | 描述 |
|------|------|------|
| DateTime | TIMESTAMP | 数据时间戳 (UTC) |
| tag_1 | DOUBLE | 工业标签1的数值 |
| tag_2 | DOUBLE | 工业标签2的数值 |
| ... | DOUBLE | 其他工业标签的数值 |

**说明**：
- 宽表结构将每个时间戳的所有标签数据存储在同一行
- 标签列名会根据实际标签名动态生成，特殊字符会被转换为下划线
- 如果标签名以数字开头，会自动添加 `tag_` 前缀
- 缺失的标签值会填充为 NULL

### 索引

- `idx_datetime`: 主索引 (DateTime)，优化时间范围查询和数据清理性能

## 运维指南

### 日志管理

服务使用 `tracing` 框架提供结构化日志，支持以下级别：
- `ERROR`: 严重错误
- `WARN`: 警告信息
- `INFO`: 一般信息
- `DEBUG`: 调试信息
- `TRACE`: 详细跟踪信息

**日志输出**：
- 控制台输出：实时显示日志信息
- 文件输出：自动按天滚动，保存在 `logs/rt_db.log`
- 时间格式：北京时间 (UTC+8)

设置日志级别：
```bash
# 通过配置文件
log_level = "debug"

# 或通过环境变量
RUST_LOG=debug cargo run
```

### 性能监控

服务每5分钟输出一次状态报告，包括：
- 总记录数
- 最新数据时间戳
- 最后同步时间戳
- 数据窗口配置（天数）
- 更新间隔配置（秒）

**关键监控指标**：
- 数据同步频率和延迟
- 数据库连接状态
- 内存使用情况
- 错误重试次数

### 系统服务部署

#### Linux (systemd)

创建服务文件 `/etc/systemd/system/rt-db.service`：

```ini
[Unit]
Description=Real-time Data Cache Service
After=network.target

[Service]
Type=simple
User=rt-db
WorkingDirectory=/opt/rt-db
ExecStart=/opt/rt-db/rt_db
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

启动服务：
```bash
sudo systemctl enable rt-db
sudo systemctl start rt-db
sudo systemctl status rt-db
```

#### Windows Service

可以使用 NSSM (Non-Sucking Service Manager) 将程序注册为 Windows 服务。

### 故障排除

#### 常见问题

1. **连接 SQL Server 失败**
   - 检查网络连接
   - 验证连接字符串
   - 确认数据库权限

2. **DuckDB 文件锁定**
   - 确保没有其他进程以写模式打开文件
   - 客户端应使用只读模式连接

3. **内存使用过高**
   - 减少数据窗口天数
   - 增加更新间隔
   - 监控数据量增长

#### 日志分析

关键日志信息：
- 服务启动和停止
- 数据同步统计
- 错误和重试信息
- 性能指标

## 开发指南

### 项目结构

```
src/
├── main.rs           # 主程序入口，服务启动和信号处理
├── config.rs         # 配置管理，支持多种连接方式
├── database.rs       # DuckDB 数据库操作，宽表管理
├── data_source.rs    # SQL Server 数据源，历史和实时数据获取
└── sync_service.rs   # 数据同步服务，周期性更新和清理
```

### 核心模块说明

#### main.rs
- 服务启动和初始化
- 日志系统配置（tracing + 文件滚动）
- 信号处理（SIGTERM/SIGINT）
- 异步任务协调

#### config.rs
- `AppConfig`: 主配置结构体
- `DatabaseConfig`: 数据库连接配置
- 支持连接字符串和结构化配置两种模式
- 配置验证和连接字符串生成

#### database.rs
- `DatabaseManager`: DuckDB 数据库管理器
- 宽表创建和动态列管理
- 数据插入、查询和清理操作
- 连接池和事务管理

#### data_source.rs
- `SqlServerDataSource`: SQL Server 数据源
- 历史数据批量加载
- TagDatabase 增量数据获取
- 连接重试和错误处理

#### sync_service.rs
- `SyncService`: 数据同步服务
- 周期性更新任务
- 数据窗口管理和清理
- 服务状态监控

### 关键设计模式

- **异步编程**: 使用 Tokio 运行时处理并发任务
- **错误处理**: 使用 `anyhow` 进行统一错误处理
- **配置管理**: 使用 `serde` 和 `config` crate 进行配置解析
- **日志记录**: 使用 `tracing` 框架进行结构化日志
- **数据库抽象**: 封装数据库操作，支持连接池

### 扩展功能建议

- **HTTP API 接口**: 添加 REST API 用于数据查询和状态监控
- **更多数据源**: 支持 PostgreSQL、InfluxDB 等其他数据源
- **数据压缩**: 实现历史数据压缩存储
- **监控指标**: 集成 Prometheus 指标导出
- **集群支持**: 支持多实例部署和负载均衡
- **数据验证**: 添加数据质量检查和异常检测
- **配置热重载**: 支持运行时配置更新

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！