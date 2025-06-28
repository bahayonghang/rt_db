# 实时数据查询服务 - 精简版

一个简单的Rust命令行工具，用于连接SQL Server数据库并查询指定表的数据。

## 功能特性

- 🔗 连接SQL Server数据库
- 📊 查询历史表最近N天的数据
- 🏷️ 查询TagDatabase表的数据
- ⚙️ 通过配置文件管理数据库连接参数
- 📝 详细的日志记录
- 🛡️ 完善的错误处理

## 技术栈

- **Rust** - 系统编程语言
- **Tokio** - 异步运行时
- **Tiberius** - SQL Server数据库驱动
- **Serde + TOML** - 配置文件解析
- **Chrono** - 时间处理
- **Anyhow** - 错误处理

## 项目结构

```
rt_db_simple/
├── Cargo.toml              # 项目依赖配置
├── config.toml             # 数据库连接配置
├── src/
│   ├── main.rs            # 程序入口
│   ├── config.rs          # 配置管理模块
│   ├── database.rs        # 数据库连接和查询
│   └── models.rs          # 数据模型定义
└── README.md              # 使用说明
```

## 快速开始

### 1. 配置数据库连接

编辑 `config.toml` 文件，设置你的数据库连接参数：

```toml
[database]
server = "your_server_address"
database = "your_database_name"
username = "your_username"
password = "your_password"
port = 1433

[query]
history_table = "HistoryTable"
tag_table = "TagDatabase"
days_back = 3
```

### 2. 编译项目

```bash
cargo build --release
```

### 3. 运行程序

```bash
# 直接运行
cargo run

# 或运行编译后的二进制文件
./target/release/rt_db_simple
```

### 4. 查看输出

程序会输出类似以下的结果：

```
2024-01-15 10:30:00 INFO  [rt_db_simple] 开始连接数据库...
2024-01-15 10:30:01 INFO  [rt_db_simple] 数据库连接成功
2024-01-15 10:30:01 INFO  [rt_db_simple] 数据库连接测试通过
2024-01-15 10:30:01 INFO  [rt_db_simple] 开始查询历史数据...
找到 150 条历史记录:
[1] 标签: Temperature_01, 时间: 2024-01-15 10:25:00 UTC, 值: 25.30, 质量: Good
[2] 标签: Pressure_02, 时间: 2024-01-15 10:24:00 UTC, 值: 1.25, 质量: Good
...
2024-01-15 10:30:02 INFO  [rt_db_simple] 开始查询TagDatabase数据...
找到 50 条标签记录:
[1] ID: 1, 标签: Temperature_01, OPC名称: Temp01, 服务器: OPCServer1, 单位: °C, 类型: Real, 描述: 温度传感器1, 当前值: 25.30, 最小值: -10.00, 最大值: 100.00, 记录标志: Y, 输入输出: I, 质量: Good
[2] ID: 2, 标签: Pressure_02, OPC名称: Press02, 服务器: OPCServer1, 单位: MPa, 类型: Real, 描述: 压力传感器2, 当前值: 1.25, 最小值: 0.00, 最大值: 5.00, 记录标志: Y, 输入输出: I, 质量: Good
...
```

## 历史数据查询

程序会查询指定天数内的历史数据，并显示：
- 标签名称 (TagName)
- 时间戳 (DateTime)
- 数值 (TagVal)
- 质量状态 (TagQuality)

### 历史表结构
历史表包含以下字段：
- `DateTime`: 时间戳
- `TagName`: 标签名称
- `TagVal`: 标签值
- `TagQuality`: 质量状态（如：已连接、断开等）

## 配置说明

### 数据库配置 `[database]`

- `server`: SQL Server服务器地址
- `database`: 数据库名称
- `username`: 用户名
- `password`: 密码
- `port`: 端口号（可选，默认1433）

### 查询配置 `[query]`

- `history_table`: 历史数据表名
- `tag_table`: 标签数据表名（TagDatabase）
- `days_back`: 查询最近几天的数据

### TagDatabase 表结构说明

TagDatabase 表包含以下字段：
- `TagID`: 标签唯一标识符
- `TagName`: 标签名称
- `TagOPCName`: OPC 标签名称
- `OpcServerName`: OPC 服务器名称
- `TagUnit`: 标签单位
- `TagType`: 标签数据类型
- `TagDescrip`: 标签描述
- `TagVal`: 当前标签值
- `TagMinVal`: 标签最小值
- `TagMaxVal`: 标签最大值
- `DataRecFlag`: 数据记录标志
- `InOrOutFlag`: 输入输出标志
- `TagQuality`: 标签质量状态

## 日志配置

程序使用环境变量控制日志级别：

```bash
# 设置日志级别为debug
export RUST_LOG=debug
cargo run

# 设置日志级别为info
export RUST_LOG=info
cargo run
```

## 错误处理

程序包含完善的错误处理机制：

- 配置文件解析错误
- 数据库连接失败
- SQL查询错误
- 数据类型转换错误

所有错误都会记录到日志中，并提供详细的错误信息。

## 扩展功能

如果需要扩展功能，可以考虑：

1. **命令行参数支持**: 使用 `clap` 库
2. **多种输出格式**: JSON、CSV、XML等
3. **查询条件过滤**: 支持更复杂的WHERE条件
4. **连接池**: 提高数据库连接性能
5. **定时任务**: 定期执行查询
6. **Web API**: 提供HTTP接口

## 部署说明

1. 编译发布版本：`cargo build --release`
2. 将 `target/release/rt_db_simple` 和 `config.toml` 复制到目标服务器
3. 确保目标服务器可以访问SQL Server数据库
4. 运行程序：`./rt_db_simple`

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！