use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// 数据库连接方式
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseConnectionType {
    /// 使用连接字符串
    ConnectionString,
    /// 使用结构化配置
    StructuredConfig,
}

impl Default for DatabaseConnectionType {
    fn default() -> Self {
        DatabaseConnectionType::StructuredConfig
    }
}

/// 应用配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// 数据库连接字符串（当使用 connection_string 模式时）
    pub database_url: Option<String>,
    /// 数据库连接配置（当使用 structured_config 模式时）
    pub database: Option<DatabaseConfig>,
    /// 数据库连接方式选择
    #[serde(default)]
    pub database_connection_type: DatabaseConnectionType,
    /// 增量更新周期，单位为秒
    pub update_interval_secs: u64,
    /// 数据保留窗口，单位为天
    pub data_window_days: u32,
    /// 本地 DuckDB 文件路径
    pub db_file_path: String,
    /// 日志级别
    pub log_level: String,
    /// 表名配置
    pub tables: TableConfig,
    /// 连接配置
    pub connection: ConnectionConfig,
    /// 查询配置
    pub query: QueryConfig,
    /// 批量处理配置
    #[serde(default)]
    pub batch: BatchConfig,
}

/// 数据库连接配置
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    /// 服务器地址
    pub server: String,
    /// 端口号
    pub port: u16,
    /// 数据库名
    pub database: String,
    /// 用户名
    pub user: String,
    /// 密码
    pub password: String,
    /// 是否信任服务器证书
    pub trust_server_certificate: bool,
}

impl DatabaseConfig {
    /// 生成数据库连接字符串
    pub fn to_connection_string(&self) -> String {
        // 对数据库名、用户名和密码进行URL编码以支持中文字符
        let encoded_database = urlencoding::encode(&self.database);
        let encoded_user = urlencoding::encode(&self.user);
        let encoded_password = urlencoding::encode(&self.password);
        
        format!(
            "server=tcp:{},{};database={};user={};password={};TrustServerCertificate={}",
            self.server,
            self.port,
            encoded_database,
            encoded_user,
            encoded_password,
            self.trust_server_certificate
        )
    }
    
    /// 从连接字符串解析数据库配置
    pub fn from_connection_string(connection_string: &str) -> Result<Self> {
        let mut server = String::new();
        let mut port = 1433u16;
        let mut database = String::new();
        let mut user = String::new();
        let mut password = String::new();
        let mut trust_server_certificate = false;
        
        // 解析连接字符串中的键值对
        for pair in connection_string.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() != 2 {
                continue;
            }
            
            let key = parts[0].trim().to_lowercase();
            let value = parts[1].trim();
            
            match key.as_str() {
                "server" => {
                    // 处理 server=tcp:localhost,1433 格式
                    if value.starts_with("tcp:") {
                        let server_part = &value[4..]; // 去掉 "tcp:" 前缀
                        if let Some(comma_pos) = server_part.find(',') {
                            server = server_part[..comma_pos].to_string();
                            if let Ok(parsed_port) = server_part[comma_pos + 1..].parse::<u16>() {
                                port = parsed_port;
                            }
                        } else {
                            server = server_part.to_string();
                        }
                    } else {
                        server = value.to_string();
                    }
                }
                "database" => {
                    // URL解码数据库名以支持中文字符
                    database = urlencoding::decode(value)
                        .map_err(|e| anyhow::anyhow!("数据库名解码失败: {}", e))?
                        .into_owned();
                }
                "user" => {
                    // URL解码用户名
                    user = urlencoding::decode(value)
                        .map_err(|e| anyhow::anyhow!("用户名解码失败: {}", e))?
                        .into_owned();
                }
                "password" => {
                    // URL解码密码
                    password = urlencoding::decode(value)
                        .map_err(|e| anyhow::anyhow!("密码解码失败: {}", e))?
                        .into_owned();
                }
                "trustservercertificate" => {
                    trust_server_certificate = value.to_lowercase() == "true";
                }
                _ => {
                    // 忽略未知的键
                }
            }
        }
        
        let config = DatabaseConfig {
            server,
            port,
            database,
            user,
            password,
            trust_server_certificate,
        };
        
        // 验证解析结果
        config.validate()?;
        
        Ok(config)
    }
    
    /// 验证数据库配置的有效性
    fn validate(&self) -> Result<()> {
        if self.server.is_empty() {
            anyhow::bail!("数据库服务器地址不能为空");
        }
        
        if self.port == 0 {
            anyhow::bail!("数据库端口号必须大于 0");
        }
        
        if self.database.is_empty() {
            anyhow::bail!("数据库名不能为空");
        }
        
        if self.user.is_empty() {
            anyhow::bail!("数据库用户名不能为空");
        }
        
        if self.password.is_empty() {
            anyhow::bail!("数据库密码不能为空");
        }
        
        Ok(())
    }
}

/// 表名配置
#[derive(Debug, Deserialize, Clone)]
pub struct TableConfig {
    /// 历史表名
    pub history_table: String,
    /// TagDatabase 表名
    pub tag_database_table: String,
}

/// 查询配置
#[derive(Debug, Deserialize, Clone)]
pub struct QueryConfig {
    /// 历史数据查询天数
    pub days_back: i32,
    /// 历史数据表名（用于查询）
    pub history_table: String,
}

/// 连接配置
#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionConfig {
    /// 最大重试次数
    pub max_retries: u32,
    /// 重试间隔，单位为秒
    pub retry_interval_secs: u64,
    /// 连接超时，单位为秒
    pub connection_timeout_secs: u64,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            history_table: "History".to_string(),
            tag_database_table: "TagDatabase".to_string(),
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            days_back: 30,
            history_table: "History".to_string(),
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_interval_secs: 5,
            connection_timeout_secs: 30,
        }
    }
}

impl AppConfig {
    /// 从配置文件加载配置
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(
                config_path.as_ref().to_str().unwrap_or("config")
            ))
            .build()?;
        
        let config: AppConfig = settings.try_deserialize()?;
        
        // 验证配置
        config.validate()?;
        
        Ok(config)
    }
    
    /// 获取数据库配置
    /// 根据连接方式返回相应的数据库配置
    pub fn get_database_config(&self) -> Result<DatabaseConfig> {
        match self.database_connection_type {
            DatabaseConnectionType::ConnectionString => {
                if let Some(ref connection_string) = self.database_url {
                    DatabaseConfig::from_connection_string(connection_string)
                } else {
                    anyhow::bail!("使用连接字符串模式时，database_url 不能为空")
                }
            }
            DatabaseConnectionType::StructuredConfig => {
                if let Some(ref database_config) = self.database {
                    database_config.validate()?;
                    Ok(database_config.clone())
                } else {
                    anyhow::bail!("使用结构化配置模式时，database 配置不能为空")
                }
            }
        }
    }
    
    /// 获取数据库连接字符串
    /// 无论使用哪种配置方式，都返回标准的连接字符串
    pub fn get_connection_string(&self) -> Result<String> {
        let db_config = self.get_database_config()?;
        Ok(db_config.to_connection_string())
    }
    
    /// 验证配置的有效性
    fn validate(&self) -> Result<()> {
        // 验证数据库配置
        self.get_database_config()?;
        
        if self.update_interval_secs == 0 {
            anyhow::bail!("update_interval_secs 必须大于 0");
        }
        
        if self.data_window_days == 0 {
            anyhow::bail!("data_window_days 必须大于 0");
        }
        
        if self.db_file_path.is_empty() {
            anyhow::bail!("db_file_path 不能为空");
        }
        
        // 验证连接方式和对应配置的一致性
        match self.database_connection_type {
            DatabaseConnectionType::ConnectionString => {
                if self.database_url.is_none() {
                    anyhow::bail!("选择连接字符串模式时，必须提供 database_url");
                }
                if let Some(ref url) = self.database_url {
                    if url.trim().is_empty() {
                        anyhow::bail!("database_url 不能为空字符串");
                    }
                }
            }
            DatabaseConnectionType::StructuredConfig => {
                if self.database.is_none() {
                    anyhow::bail!("选择结构化配置模式时，必须提供 database 配置");
                }
            }
        }
        
        Ok(())
    }
    
    /// 获取数据窗口的持续时间（以秒为单位）
    pub fn data_window_duration_secs(&self) -> i64 {
        self.data_window_days as i64 * 24 * 60 * 60
    }
}

/// 批量处理配置
#[derive(Debug, Deserialize, Clone)]
pub struct BatchConfig {
    /// 批量插入大小
    pub batch_size: usize,
    /// 最大内存记录数
    pub max_memory_records: usize,
    /// 是否启用并行插入
    pub enable_parallel_insert: bool,
    /// 历史数据加载批次大小（按天）
    pub history_load_batch_days: u32,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            max_memory_records: 50000,
            enable_parallel_insert: true,
            history_load_batch_days: 1,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_url: None,
            database: None,
            database_connection_type: DatabaseConnectionType::default(),
            update_interval_secs: 60,
            data_window_days: 30,
            db_file_path: "rt_db.duckdb".to_string(),
            log_level: "info".to_string(),
            tables: TableConfig::default(),
            connection: ConnectionConfig::default(),
            query: QueryConfig::default(),
            batch: BatchConfig::default(),
        }
    }
}