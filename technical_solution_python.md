---

### **实时数据缓存服务 - Python版本技术设计规约 (TDD)**

**版本**: 1.0
**日期**: 2025-06-29

---

### **1. 项目概述**

#### 1.1. 项目目标
本项目旨在创建一个基于Python的实时工业数据缓存服务，功能上与Rust版本保持一致。服务将从上游的 SQL Server 数据库中拉取数据，并将其存储在一个本地的高性能嵌入式数据库文件中。该服务需要保证本地缓存中始终只保留配置天数的时序数据，采用宽表格式优化查询性能，并为下游的数据分析工具提供稳定、高效的直接数据查询接口。

#### 1.2. 核心功能需求
* **数据源**: 从 SQL Server 的`历史表`和`TagDatabase`表获取数据
* **数据存储**: 使用 DuckDB 作为本地缓存，采用宽表格式存储
* **数据窗口**: 维护可配置天数的滚动数据窗口（默认3天）
* **启动行为**: 服务启动时，初始化数据库并加载过去1小时的历史数据，然后拉取TagDatabase当前数据
* **运行时行为**: 以配置的周期（默认10秒）拉取TagDatabase最新数据，动态管理标签变化，清理过期数据
* **宽表存储**: 将时序数据转换为宽表格式，每行包含一个时间戳及所有标签的值
* **查询接口**: 本地数据库文件支持多客户端并发只读访问

---

### **2. 系统架构**

系统采用模块化设计，包含配置管理、数据源连接、数据库操作、同步服务和主程序协调五个核心模块。

* **数据源 (Upstream)**: Microsoft SQL Server 数据库
* **数据处理服务 (Core Service)**: Python 后台服务，负责数据同步、转换和维护
* **数据存储与消费端 (Downstream & Storage)**: DuckDB 数据库文件 (`.duckdb`)，支持Python、DBeaver等工具直接访问

**数据流:**
1.  **初始化流程**: `SQL Server历史表` + `TagDatabase` -> `Python服务` -> `DuckDB宽表`
2.  **增量更新流程**: `TagDatabase` -> `Python服务` -> `DuckDB宽表`
3.  **查询流程**: `Python/分析工具` -> `DuckDB文件`

---

### **3. 技术选型详述**

| 分类 | 技术/库 | 选用理由 | 关键配置/特性 |
| :--- | :--- | :--- | :--- |
| **编程语言** | Python 3.8+ | 开发效率高，数据处理生态丰富，易于维护和扩展 | 推荐使用 Python 3.10+ |
| **异步框架** | `asyncio` + `aiofiles` | Python原生异步支持，提供高效的I/O处理能力 | 使用事件循环管理异步任务 |
| **SQL Server驱动** | `pyodbc` | 成熟稳定的SQL Server连接库，支持连接池 | 配置ODBC Driver 17/18 for SQL Server |
| **本地缓存数据库** | `duckdb` | 高性能嵌入式分析数据库，列式存储，支持并发读写 | 启用线程安全模式 |
| **时间处理** | `datetime` + `pytz` | Python标准时间库，支持时区转换和时间计算 | 统一使用UTC时间存储 |
| **配置管理** | `pydantic` + `toml` | 类型安全的配置解析，支持数据验证 | 使用BaseSettings支持环境变量 |
| **日志框架** | `loguru` | 简洁易用的结构化日志库，支持文件轮转 | 配置JSON格式输出便于分析 |
| **数据处理** | `pandas` + `polars` | 高效的数据处理库，polars提供更好的性能 | 优先使用polars进行大数据处理 |
| **任务调度** | `apscheduler` | 灵活的任务调度库，支持多种触发器 | 使用AsyncIOScheduler |

---

### **4. 数据模型与数据库 Schema**

#### 4.1. 数据库文件
* **数据库类型**: DuckDB
* **默认文件名**: `realtime_data.duckdb` (配置文件中可指定)

#### 4.2. 表结构 (Schema)
系统采用宽表格式存储时序数据，优化查询性能。

* **表名**: `ts_wide`

| 列名 (Column) | 数据类型 (SQL) | 描述 | 约束/备注 |
| :--- | :--- | :--- | :--- |
| `DateTime` | `TIMESTAMP` | 数据时间戳 (UTC 标准) | `PRIMARY KEY` |
| `tag_*` | `DOUBLE` | 各工业标签的数值，列名动态生成 | 允许 NULL，缺失数据用NULL表示 |

**注**: 标签列名规则
- 特殊字符转换为下划线
- 数字开头的标签名添加 `tag_` 前缀
- 支持动态添加新标签列

#### 4.3. 索引 (Index)
为了优化时间范围查询和数据清理性能。

* **索引名**: `idx_datetime`
* **索引类型**: B-Tree 索引
* **索引列**: `DateTime`
* **目的**: 加速时间范围查询和过期数据删除

---

### **5. 核心组件功能规约**

#### 5.1. 配置模块 (`config.py`)
* **职责**: 提供统一的配置加载和访问接口。
* **实现细节**:
    1.  定义 Pydantic `BaseSettings` 类，支持从配置文件和环境变量加载配置。
    2.  **必须包含的配置项**:
        * `database_connection_type`: 连接方式选择 (connection_string/structured_config)
        * `database_url`: SQL Server 连接字符串（连接字符串模式）
        * `database`: 结构化数据库配置（结构化模式）
        * `update_interval_secs`: 增量更新周期，单位为秒（例如: 10）
        * `data_window_days`: 数据保留窗口，单位为天（例如: 3）
        * `db_file_path`: 本地 DuckDB 文件的路径
        * `log_level`: 日志级别配置
        * `tables`: 表名配置
        * `connection`: 连接重试配置
        * `batch`: 批量处理配置

```python
from pydantic import BaseSettings, Field
from typing import Optional, Literal
from enum import Enum

class DatabaseConnectionType(str, Enum):
    CONNECTION_STRING = "connection_string"
    STRUCTURED_CONFIG = "structured_config"

class DatabaseConfig(BaseSettings):
    server: str
    port: int = 1433
    database: str
    user: str
    password: str
    trust_server_certificate: bool = True

class TableConfig(BaseSettings):
    history_table: str = "History"
    tag_database_table: str = "TagDatabase"

class ConnectionConfig(BaseSettings):
    max_retries: int = 3
    retry_interval_secs: int = 5
    connection_timeout_secs: int = 30

class BatchConfig(BaseSettings):
    batch_size: int = 1000
    max_memory_records: int = 50000
    enable_parallel_insert: bool = True
    history_load_batch_days: int = 1

class AppConfig(BaseSettings):
    database_url: Optional[str] = None
    database: Optional[DatabaseConfig] = None
    database_connection_type: DatabaseConnectionType = DatabaseConnectionType.STRUCTURED_CONFIG
    update_interval_secs: int = 10
    data_window_days: int = 3
    db_file_path: str = "./realtime_data.duckdb"
    log_level: str = "info"
    tables: TableConfig = Field(default_factory=TableConfig)
    connection: ConnectionConfig = Field(default_factory=ConnectionConfig)
    batch: BatchConfig = Field(default_factory=BatchConfig)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
```

#### 5.2. 应用入口与主流程 (`main.py`)
* **职责**: 初始化服务，协调各个模块的启动和关闭。
* **执行流程**:
    1.  **初始化**:
        * 启动 `loguru` 日志系统
        * 调用配置模块，加载并验证应用配置
        * 设置异常处理和信号监听
    2.  **数据库准备**:
        * 根据配置的 `db_file_path`，初始化DuckDB数据库
        * 创建 `ts_wide` 表和 `idx_datetime` 索引
    3.  **任务调度**:
        * 使用 `asyncio.run()` 启动主异步循环
        * 执行初始数据加载（过去1小时历史数据 + TagDatabase当前数据）
        * 启动周期性更新任务
        * 启动状态监控任务
    4.  **优雅停机**:
        * 监听 SIGTERM/SIGINT 信号
        * 记录停机日志，等待任务完成后退出

```python
import asyncio
import signal
from loguru import logger
from config import AppConfig
from database_manager import DatabaseManager
from data_source import SqlServerDataSource
from sync_service import SyncService

async def main():
    # 加载配置
    config = AppConfig()
    
    # 初始化日志
    setup_logging(config)
    
    logger.info("=== 实时数据缓存服务启动 ===")
    
    # 初始化组件
    db_manager = DatabaseManager(config.db_file_path)
    await db_manager.initialize()
    
    data_source = SqlServerDataSource(config)
    await data_source.test_connection()
    
    sync_service = SyncService(config, db_manager, data_source)
    
    # 设置信号处理
    setup_signal_handlers()
    
    try:
        # 执行初始数据加载
        await sync_service.initial_load()
        
        # 启动周期性任务
        await asyncio.gather(
            sync_service.start_periodic_update(),
            sync_service.start_status_monitor()
        )
    except KeyboardInterrupt:
        logger.info("收到停机信号，开始优雅停机...")
    finally:
        await db_manager.close()
        logger.info("服务已停止")

if __name__ == "__main__":
    asyncio.run(main())
```

#### 5.3. 初始数据加载模块 (`initial_loader.py`)
* **职责**: 在服务启动时，从 SQL Server 加载初始数据填充本地缓存。
* **执行流程**:
    1.  建立到 SQL Server 的连接
    2.  查询过去1小时的历史数据（而非配置的data_window_days）
    3.  查询TagDatabase中的当前数据
    4.  将数据转换为宽表格式
    5.  使用 `polars` 进行数据预处理和格式转换
    6.  分批次处理数据，避免内存溢出
    7.  动态检测标签变化并添加新列
    8.  批量插入到DuckDB

```python
import polars as pl
from datetime import datetime, timedelta
from typing import List, Dict
import duckdb

class InitialLoader:
    def __init__(self, config: AppConfig, db_manager: DatabaseManager, data_source: SqlServerDataSource):
        self.config = config
        self.db_manager = db_manager
        self.data_source = data_source
    
    async def load_initial_data(self) -> int:
        """加载初始数据，返回加载的记录数"""
        logger.info("开始初始数据加载...")
        
        # 计算时间范围（过去1小时）
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        
        # 加载历史数据
        history_records = await self.data_source.load_data_in_range(one_hour_ago, now)
        
        # 加载TagDatabase当前数据
        tagdb_records = await self.data_source.get_latest_tagdb_data()
        
        # 合并数据
        all_records = history_records + tagdb_records
        
        if not all_records:
            logger.warning("未找到初始数据")
            return 0
        
        # 转换为宽表格式并插入
        total_loaded = 0
        for chunk in self._chunk_records(all_records, self.config.batch.max_memory_records):
            wide_data = self._convert_to_wide_format(chunk)
            await self.db_manager.insert_wide_data(wide_data)
            total_loaded += len(chunk)
            logger.info(f"已加载 {len(chunk)} 条记录，累计: {total_loaded}")
        
        logger.info(f"初始数据加载完成，共加载 {total_loaded} 条记录")
        return total_loaded
    
    def _convert_to_wide_format(self, records: List[TimeSeriesRecord]) -> pl.DataFrame:
        """将时序记录转换为宽表格式"""
        # 使用polars进行高效的数据透视
        df = pl.DataFrame([
            {
                "DateTime": record.timestamp,
                "tag_name": record.tag_name,
                "value": record.value
            }
            for record in records
        ])
        
        # 透视为宽表格式
        wide_df = df.pivot(
            values="value",
            index="DateTime",
            columns="tag_name",
            aggregate_function="first"
        )
        
        return wide_df
```

#### 5.4. 周期性更新模块 (`periodic_updater.py`)
* **职责**: 定期从 SQL Server 拉取增量数据，管理标签变化，清理过期数据。
* **执行流程**:
    1.  使用 `APScheduler` 按配置间隔执行更新
    2.  **在每个更新周期内，执行以下操作**:
        a. **检测标签变化**: 比较当前TagDatabase中的标签与已知标签
        b. **处理标签变化**: 动态添加新列，标记删除的标签
        c. **拉取最新数据**: 获取TagDatabase中的当前数据
        d. **转换并插入**: 转换为宽表格式并插入DuckDB
        e. **清理过期数据**: 删除超过配置天数的数据
    3.  使用 `polars` 进行高效的数据处理和转换
    4.  实现连接重试和错误恢复机制

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import polars as pl
from datetime import datetime, timedelta

class PeriodicUpdater:
    def __init__(self, config: AppConfig, db_manager: DatabaseManager, data_source: SqlServerDataSource):
        self.config = config
        self.db_manager = db_manager
        self.data_source = data_source
        self.scheduler = AsyncIOScheduler()
        self.last_seen_timestamp = None
    
    async def start_periodic_update(self):
        """启动周期性更新任务"""
        trigger = IntervalTrigger(seconds=self.config.update_interval_secs)
        self.scheduler.add_job(
            self._update_cycle,
            trigger=trigger,
            id='periodic_update',
            max_instances=1
        )
        self.scheduler.start()
        logger.info(f"周期性更新任务已启动，间隔: {self.config.update_interval_secs}秒")
    
    async def _update_cycle(self):
        """执行一次更新周期"""
        try:
            logger.debug("开始执行更新周期")
            
            # 1. 检测标签变化
            await self._handle_tag_changes()
            
            # 2. 获取最新数据
            latest_data = await self.data_source.get_latest_tagdb_data()
            
            if latest_data:
                # 3. 转换并插入宽表
                wide_data = self._convert_to_wide_format(latest_data)
                await self.db_manager.append_latest_data(wide_data)
                logger.info(f"更新成功: {len(latest_data)} 条记录")
            
            # 4. 清理过期数据
            await self._cleanup_old_data()
            
        except Exception as e:
            logger.error(f"更新周期执行失败: {e}")
    
    async def _handle_tag_changes(self):
        """处理标签变化（加点/少点）"""
        known_tags = await self.db_manager.get_known_tags()
        tag_changes = await self.data_source.detect_tag_changes(known_tags)
        
        if tag_changes.added_tags:
            logger.info(f"检测到新增标签: {tag_changes.added_tags}")
            await self.db_manager.add_columns(tag_changes.added_tags)
        
        if tag_changes.removed_tags:
            logger.warning(f"检测到删除标签: {tag_changes.removed_tags}")
            # 标记删除但保留历史数据
```

#### 5.5. 数据库管理模块 (`database_manager.py`)
* **职责**: 管理DuckDB数据库操作，包括宽表创建、动态列管理、数据插入和清理。
* **核心功能**:
    1.  **宽表管理**: 创建和维护`ts_wide`表结构
    2.  **动态列管理**: 自动检测新标签并添加对应列
    3.  **批量插入**: 高效的宽表数据批量插入
    4.  **数据清理**: 按时间窗口清理过期数据
    5.  **并发控制**: 支持多读一写的并发访问

```python
import duckdb
import polars as pl
from typing import Set, List, Dict, Optional
import asyncio
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.known_tags: Set[str] = set()
    
    async def initialize(self):
        """初始化数据库"""
        # 删除旧文件
        if Path(self.db_path).exists():
            Path(self.db_path).unlink()
            logger.info("已删除旧的数据库文件")
        
        # 创建新数据库
        conn = duckdb.connect(self.db_path)
        
        # 创建宽表
        conn.execute("""
            CREATE TABLE ts_wide (
                DateTime TIMESTAMP PRIMARY KEY
            )
        """)
        
        # 创建索引
        conn.execute("CREATE INDEX idx_datetime ON ts_wide (DateTime)")
        
        conn.close()
        logger.info("数据库初始化完成")
    
    async def add_columns(self, tag_names: List[str]):
        """动态添加标签列"""
        conn = duckdb.connect(self.db_path)
        
        for tag in tag_names:
            safe_name = self._sanitize_column_name(tag)
            if safe_name not in self.known_tags:
                conn.execute(f"ALTER TABLE ts_wide ADD COLUMN {safe_name} DOUBLE")
                self.known_tags.add(safe_name)
                logger.debug(f"添加新列: {safe_name}")
        
        conn.close()
    
    async def insert_wide_data(self, wide_df: pl.DataFrame):
        """插入宽表数据"""
        if wide_df.is_empty():
            return
        
        conn = duckdb.connect(self.db_path)
        
        # 使用polars的to_duckdb方法进行高效插入
        conn.register('temp_data', wide_df.to_pandas())
        conn.execute("""
            INSERT OR REPLACE INTO ts_wide 
            SELECT * FROM temp_data
        """)
        
        conn.close()
    
    def _sanitize_column_name(self, tag_name: str) -> str:
        """清理列名，确保SQL安全"""
        # 替换特殊字符为下划线
        safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in tag_name)
        
        # 确保不以数字开头
        if safe_name and safe_name[0].isdigit():
            safe_name = f"tag_{safe_name}"
        
        return safe_name or "unknown_tag"
```

#### 5.6. 数据源模块 (`data_source.py`)
* **职责**: 管理SQL Server连接，执行数据查询和标签变化检测。
* **核心功能**:
    1.  **连接管理**: 支持连接字符串和结构化配置两种模式
    2.  **历史数据查询**: 按时间范围查询历史表数据
    3.  **增量数据获取**: 从TagDatabase获取最新数据
    4.  **标签变化检测**: 动态检测标签的增减变化
    5.  **连接重试**: 实现连接失败的自动重试机制

```python
import pyodbc
import asyncio
from typing import List, Set, Optional
from datetime import datetime, timedelta
import urllib.parse

class SqlServerDataSource:
    def __init__(self, config: AppConfig):
        self.config = config
        self._connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """构建SQL Server连接字符串"""
        if self.config.database_connection_type == DatabaseConnectionType.CONNECTION_STRING:
            return self.config.database_url
        else:
            db_config = self.config.database
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_config.server},{db_config.port};"
                f"DATABASE={db_config.database};"
                f"UID={db_config.user};"
                f"PWD={db_config.password};"
                f"TrustServerCertificate={'yes' if db_config.trust_server_certificate else 'no'}"
            )
    
    async def test_connection(self):
        """测试数据库连接"""
        conn = await self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        logger.info("SQL Server 连接测试成功")
    
    async def load_data_in_range(self, start_time: datetime, end_time: datetime) -> List[TimeSeriesRecord]:
        """按时间范围加载历史数据"""
        conn = await self._get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT DateTime, TagName, TagVal 
            FROM [{self.config.tables.history_table}] 
            WHERE DateTime >= ? AND DateTime < ? 
            ORDER BY DateTime
        """
        
        cursor.execute(sql, start_time, end_time)
        rows = cursor.fetchall()
        conn.close()
        
        records = []
        for row in rows:
            if all(row):  # 确保数据完整
                records.append(TimeSeriesRecord(
                    timestamp=row[0],
                    tag_name=row[1].strip(),
                    value=float(row[2]) if row[2] is not None else 0.0
                ))
        
        logger.debug(f"加载历史数据: {len(records)} 条记录")
        return records
    
    async def get_latest_tagdb_data(self) -> List[TimeSeriesRecord]:
        """获取TagDatabase最新数据"""
        conn = await self._get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT TagName, TagVal 
            FROM [{self.config.tables.tag_database_table}]
            WHERE TagName IS NOT NULL
        """
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.close()
        
        current_time = datetime.utcnow()
        records = []
        
        for row in rows:
            if row[0]:  # 确保TagName不为空
                records.append(TimeSeriesRecord(
                    timestamp=current_time,
                    tag_name=row[0].strip(),
                    value=float(row[1]) if row[1] is not None else 0.0
                ))
        
        logger.debug(f"获取TagDatabase数据: {len(records)} 条记录")
        return records
    
    async def detect_tag_changes(self, known_tags: Set[str]) -> TagChanges:
        """检测标签变化"""
        conn = await self._get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT DISTINCT TagName 
            FROM [{self.config.tables.tag_database_table}] 
            WHERE TagName IS NOT NULL
        """
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.close()
        
        current_tags = {row[0].strip() for row in rows if row[0]}
        
        added_tags = list(current_tags - known_tags)
        removed_tags = list(known_tags - current_tags)
        
        return TagChanges(
            added_tags=added_tags,
            removed_tags=removed_tags,
            current_tags=current_tags
        )
```

---

### **6. 部署与运维**

#### 6.1. 环境要求
* **Python版本**: 3.8+ (推荐3.10+)
* **系统依赖**: ODBC Driver 17/18 for SQL Server
* **Python依赖**: 见requirements.txt

#### 6.2. 配置文件
支持与Rust版本相同的配置格式，`config.toml`:

```toml
# 数据库连接方式
database_connection_type = "connection_string"

# SQL Server 连接字符串
database_url = "server=tcp:localhost,1433;database=控制器数据库;user=sa;password=123456;TrustServerCertificate=true"

# 更新配置
update_interval_secs = 10
data_window_days = 3
db_file_path = "./realtime_data.duckdb"
log_level = "info"

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30

[batch]
batch_size = 1000
max_memory_records = 50000
enable_parallel_insert = true
history_load_batch_days = 1
```

#### 6.3. 依赖管理
`requirements.txt`:
```txt
pydantic[dotenv]>=1.10.0
loguru>=0.6.0
duckdb>=0.8.0
polars>=0.18.0
pandas>=1.5.0
pyodbc>=4.0.0
apscheduler>=3.10.0
toml>=0.10.0
asyncio-utils>=0.1.0
```

#### 6.4. 服务部署
```bash
# 安装依赖
pip install -r requirements.txt

# 配置数据库连接
cp config.toml.example config.toml
# 编辑 config.toml

# 运行服务
python main.py

# 或使用systemd部署
sudo systemctl enable rt-db-python
sudo systemctl start rt-db-python
```

---

### **7. 性能对比与选择建议**

| 特性 | Rust版本 | Python版本 |
|------|----------|-------------|
| **性能** | 极高，接近C++ | 良好，适合大多数场景 |
| **内存占用** | 很低 | 中等 |
| **开发效率** | 中等 | 高 |
| **维护难度** | 中等 | 低 |
| **生态兼容** | 好 | 极好 |
| **部署复杂度** | 低（单文件） | 中等（需Python环境） |

**选择建议**:
- **高性能要求**: 选择Rust版本
- **快速开发**: 选择Python版本  
- **与现有Python环境集成**: 选择Python版本
- **生产环境长期运行**: 两个版本都适合

---

### **8. 错误处理与监控**

#### 8.1. 错误处理策略
- 使用 `asyncio` 异常处理机制
- 实现连接重试和指数退避
- 详细的错误日志记录
- 优雅降级处理

#### 8.2. 监控指标
- 数据同步频率和延迟
- 数据库连接状态
- 内存和CPU使用情况
- 错误率和重试次数

#### 8.3. 日志管理
```python
from loguru import logger
import sys

def setup_logging(config: AppConfig):
    logger.remove()
    
    # 控制台输出
    logger.add(
        sys.stdout,
        level=config.log_level.upper(),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # 文件输出（按天轮转）
    logger.add(
        "logs/rt_db_python.log",
        level=config.log_level.upper(),
        rotation="1 day",
        retention="7 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
```

---