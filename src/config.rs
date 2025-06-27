use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// 应用配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// SQL Server 数据库连接字符串
    pub database_url: String,
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
        if self.database_url.is_empty() {
            anyhow::bail!("database_url 不能为空");
        }
        
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