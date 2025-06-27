use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// 应用配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// 数据库连接配置
    pub database: DatabaseConfig,
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
        // 对数据库名进行URL编码以支持中文字符
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
    
    /// 验证配置的有效性
    fn validate(&self) -> Result<()> {
        self.database.validate()?;
        
        if self.update_interval_secs == 0 {
            anyhow::bail!("update_interval_secs 必须大于 0");
        }
        
        if self.data_window_days == 0 {
            anyhow::bail!("data_window_days 必须大于 0");
        }
        
        if self.db_file_path.is_empty() {
            anyhow::bail!("db_file_path 不能为空");
        }
        
        Ok(())
    }
    
    /// 获取数据窗口的持续时间（以秒为单位）
    pub fn data_window_duration_secs(&self) -> i64 {
        self.data_window_days as i64 * 24 * 60 * 60
    }
}