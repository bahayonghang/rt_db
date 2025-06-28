---

### **实时数据查询服务 - 精简版技术方案**

**版本**: 1.0
**日期**: 2025-06-27

---

### **1. 项目概述**

#### 1.1. 项目目标
创建一个简单的Rust命令行工具，用于连接SQL Server数据库并查询指定表的数据。该工具支持通过配置文件连接数据库，并提供两个核心功能：查询历史表最近三天的数据和查询TagDatabase表的数据。

#### 1.2. 核心功能需求
* **数据源**: 连接SQL Server数据库
* **查询功能1**: 从`历史表`查询最近3天的数据并打印到控制台
* **查询功能2**: 从`TagDatabase`表查询数据并打印到控制台
* **配置管理**: 通过配置文件管理数据库连接参数

---

### **2. 技术选型**

| 分类 | 技术/库 | 选用理由 | 版本要求 |
| :--- | :--- | :--- | :--- |
| **编程语言** | Rust | 内存安全、高性能、优秀的错误处理 | 最新稳定版 |
| **异步运行时** | `tokio` | Rust异步生态标准，支持异步数据库操作 | 1.0+ |
| **数据库驱动** | `tiberius` | 纯Rust实现的SQL Server驱动 | 0.12+ |
| **配置管理** | `serde` + `toml` | 类型安全的配置文件解析 | - |
| **时间处理** | `chrono` | 时间计算和格式化 | 0.4+ |
| **错误处理** | `anyhow` | 简化错误处理和传播 | 1.0+ |
| **日志** | `env_logger` | 简单的日志输出 | 0.10+ |

---

### **3. 项目结构**

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

---

### **4. 核心模块设计**

#### 4.1. 配置模块 (`config.rs`)
```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub query: QueryConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub server: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize)]
pub struct QueryConfig {
    pub history_table: String,
    pub tag_table: String,
    pub days_back: i32,
}
```

#### 4.2. 数据模型 (`models.rs`)
```rust
use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct HistoryRecord {
    pub tag_name: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

#[derive(Debug, Deserialize)]
pub struct TagRecord {
    pub tag_name: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub quality: Option<String>,
}
```

#### 4.3. 数据库模块 (`database.rs`)
```rust
use anyhow::Result;
use tiberius::{Client, Config as TiberiusConfig};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub struct DatabaseClient {
    client: Client<tokio_util::compat::Compat<TcpStream>>,
}

impl DatabaseClient {
    pub async fn new(config: &crate::config::DatabaseConfig) -> Result<Self> {
        // 数据库连接实现
    }
    
    pub async fn query_history_data(&mut self, table: &str, days: i32) -> Result<Vec<crate::models::HistoryRecord>> {
        // 查询历史数据实现
    }
    
    pub async fn query_tag_data(&mut self, table: &str) -> Result<Vec<crate::models::TagRecord>> {
        // 查询标签数据实现
    }
}
```

#### 4.4. 主程序 (`main.rs`)
```rust
use anyhow::Result;
use log::info;

mod config;
mod database;
mod models;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    // 1. 加载配置
    let config = config::load_config("config.toml")?;
    info!("配置加载成功");
    
    // 2. 连接数据库
    let mut db_client = database::DatabaseClient::new(&config.database).await?;
    info!("数据库连接成功");
    
    // 3. 查询历史表数据
    println!("\n=== 查询历史表最近{}天数据 ===", config.query.days_back);
    let history_data = db_client.query_history_data(&config.query.history_table, config.query.days_back).await?;
    for record in history_data {
        println!("{:?}", record);
    }
    
    // 4. 查询TagDatabase数据
    println!("\n=== 查询TagDatabase数据 ===");
    let tag_data = db_client.query_tag_data(&config.query.tag_table).await?;
    for record in tag_data {
        println!("{:?}", record);
    }
    
    Ok(())
}
```

---

### **5. 配置文件示例**

#### `config.toml`
```toml
[database]
server = "localhost"
database = "IndustryDB"
username = "your_username"
password = "your_password"
port = 1433

[query]
history_table = "HistoryTable"
tag_table = "TagDatabase"
days_back = 3
```

---

### **6. Cargo.toml 依赖配置**

```toml
[package]
name = "rt_db_simple"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.0", features = ["full"] }
tiberius = { version = "0.12", features = ["chrono"] }
tokio-util = { version = "0.7", features = ["compat"] }
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"
chrono = { version = "0.4", features = ["serde"] }
anyhow = "1.0"
env_logger = "0.10"
log = "0.4"
```

---

### **7. 使用方法**

#### 7.1. 编译和运行
```bash
# 编译项目
cargo build --release

# 运行程序
cargo run

# 或直接运行编译后的二进制文件
./target/release/rt_db_simple
```

#### 7.2. 配置数据库连接
1. 复制 `config.toml` 文件到项目根目录
2. 修改数据库连接参数
3. 设置要查询的表名和时间范围

#### 7.3. 输出示例
```
=== 查询历史表最近3天数据 ===
HistoryRecord { tag_name: "TEMP_01", timestamp: 2025-06-27T10:30:00Z, value: 25.5 }
HistoryRecord { tag_name: "PRESS_01", timestamp: 2025-06-27T10:30:00Z, value: 1.2 }
...

=== 查询TagDatabase数据 ===
TagRecord { tag_name: "FLOW_01", timestamp: 2025-06-27T10:35:00Z, value: 150.0, quality: Some("Good") }
TagRecord { tag_name: "LEVEL_01", timestamp: 2025-06-27T10:35:00Z, value: 75.5, quality: Some("Good") }
...
```

---

### **8. 错误处理**

- 使用 `anyhow` 进行统一的错误处理
- 数据库连接失败时提供清晰的错误信息
- 配置文件解析错误时给出具体的错误位置
- 查询失败时记录详细的错误日志

---

### **9. 扩展建议**

如果后续需要扩展功能，可以考虑：

1. **添加命令行参数**: 使用 `clap` 库支持命令行参数
2. **输出格式化**: 支持JSON、CSV等格式输出
3. **查询条件**: 支持更复杂的查询条件和过滤
4. **连接池**: 使用连接池提高性能
5. **配置验证**: 添加配置参数的验证逻辑

---

### **10. 部署说明**

- 编译后的二进制文件可以直接部署到目标服务器
- 确保目标服务器可以访问SQL Server数据库
- 配置文件需要与二进制文件放在同一目录
- 建议在生产环境中使用环境变量管理敏感信息（如密码）