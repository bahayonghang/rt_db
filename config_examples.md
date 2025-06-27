# 配置文件使用说明

本项目支持两种数据库连接配置方式，您可以根据需要选择其中一种。

## 配置方式选择

在配置文件中通过 `database_connection_type` 参数选择连接方式：

- `"connection_string"`: 使用连接字符串方式（简单配置）
- `"structured_config"`: 使用结构化配置方式（详细配置）

## 方式一：连接字符串配置

### 优点
- 配置简单，只需一行连接字符串
- 与标准SQL Server连接字符串兼容
- 支持中文数据库名、用户名和密码（自动URL编码）

### 配置示例

```toml
# 选择连接字符串模式
database_connection_type = "connection_string"

# SQL Server 数据库连接字符串
database_url = "server=tcp:localhost,1433;database=IndustryDB;user=sa;password=YourPassword;TrustServerCertificate=true"

# 其他配置...
update_interval_secs = 30
data_window_days = 1
db_file_path = "./realtime_data.duckdb"
log_level = "info"

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30
```

### 中文数据库名示例

```toml
database_connection_type = "connection_string"

# 中文数据库名会自动进行URL编码处理
database_url = "server=tcp:localhost,1433;database=控制器数据库;user=sa;password=ysdxdckj@666;TrustServerCertificate=true"
```

## 方式二：结构化配置

### 优点
- 配置结构清晰，易于理解和维护
- 支持配置验证
- 便于程序化处理

### 配置示例

```toml
# 选择结构化配置模式
database_connection_type = "structured_config"

# 其他配置...
update_interval_secs = 30
data_window_days = 1
db_file_path = "./realtime_data.duckdb"
log_level = "info"

# 数据库连接配置
[database]
server = "localhost"
port = 1433
database = "控制器数据库"  # 直接支持中文
user = "sa"
password = "ysdxdckj@666"
trust_server_certificate = true

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30
```

## 配置切换

要在两种配置方式之间切换，只需：

1. 修改 `database_connection_type` 的值
2. 根据选择的方式提供相应的配置：
   - 连接字符串模式：提供 `database_url`
   - 结构化配置模式：提供 `[database]` 配置块

## 配置验证

程序启动时会自动验证配置的有效性：

- 检查连接方式与提供的配置是否匹配
- 验证必需的配置项是否存在
- 对于连接字符串，会解析并验证格式
- 对于结构化配置，会验证各字段的有效性

## 测试配置

可以使用以下命令测试配置解析功能：

```bash
cargo run -- --test-config
```

这将运行内置的配置解析测试，验证：
- 连接字符串解析功能
- 中文字符处理
- 配置文件加载
- 错误处理

## 常见问题

### Q: 如何处理包含特殊字符的密码？
A: 
- 连接字符串模式：特殊字符会自动进行URL编码
- 结构化配置模式：直接输入原始密码即可

### Q: 如何使用中文数据库名？
A: 两种方式都支持中文数据库名，程序会自动处理编码问题。

### Q: 配置加载失败怎么办？
A: 
1. 检查配置文件语法是否正确
2. 确认 `database_connection_type` 与提供的配置匹配
3. 运行 `cargo run -- --test-config` 进行诊断
4. 查看错误信息中的具体提示

### Q: 两种配置方式的性能有差异吗？
A: 没有性能差异，两种方式最终都会转换为相同的内部表示。选择哪种方式主要取决于个人偏好和维护需求。