# 实时数据缓存服务 (Real-time Data Cache Service)

一个高性能的工业时间序列数据缓存服务，用于从 SQL Server 实时同步数据到本地 DuckDB 数据库。

## 功能特性

- **实时数据同步**: 从 SQL Server 定期拉取最新的工业数据
- **滚动数据窗口**: 自动维护指定天数的数据窗口，自动清理过期数据
- **高性能存储**: 使用 DuckDB 列式存储，优化时序数据查询性能
- **并发访问**: 支持多个客户端同时只读访问本地数据库
- **容错机制**: 内置重试机制和错误恢复
- **优雅停机**: 支持信号处理和优雅关闭

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SQL Server    │───▶│   Rust Service  │───▶│   DuckDB File   │
│   (上游数据源)   │    │   (数据同步服务) │    │   (本地缓存)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │ Python/分析工具  │
                                               │   (数据消费者)   │
                                               └─────────────────┘
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

```toml
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

# 查询最近1小时的数据
query = """
SELECT tag_name, ts, value 
FROM ts_data 
WHERE ts >= NOW() - INTERVAL '1 hour'
ORDER BY ts DESC
"""

df = conn.execute(query).fetchdf()
print(df.head())

conn.close()
```

#### DBeaver 连接

1. 创建新的 DuckDB 连接
2. 设置数据库文件路径为 `realtime_data.duckdb`
3. 连接模式设置为只读

## 数据库结构

### ts_data 表

| 列名 | 类型 | 描述 |
|------|------|------|
| tag_name | VARCHAR | 工业数据点标签名 |
| ts | TIMESTAMP | 数据时间戳 (UTC) |
| value | DOUBLE | 数据数值 |

### 索引

- `idx_tag_ts`: 复合索引 (tag_name, ts)，优化查询和数据清理性能

## 运维指南

### 日志管理

服务使用结构化日志，支持以下级别：
- `ERROR`: 严重错误
- `WARN`: 警告信息
- `INFO`: 一般信息
- `DEBUG`: 调试信息

设置日志级别：
```bash
RUST_LOG=debug cargo run
```

### 性能监控

服务每5分钟输出一次状态报告，包括：
- 总记录数
- 最新数据时间
- 最后同步时间
- 数据窗口配置

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
├── main.rs           # 主程序入口
├── config.rs         # 配置管理
├── database.rs       # DuckDB 数据库操作
├── data_source.rs    # SQL Server 数据源
└── sync_service.rs   # 数据同步服务
```

### 扩展功能

- 添加 HTTP API 接口
- 支持更多数据源类型
- 实现数据压缩
- 添加监控指标导出

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！