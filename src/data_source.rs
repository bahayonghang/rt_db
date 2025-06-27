use anyhow::Result;
use chrono::{DateTime, Utc};
use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use tracing::{info, debug, warn, error};
use crate::database::TimeSeriesRecord;
use crate::config::AppConfig;
use std::time::Duration;

/// SQL Server 数据源管理器
pub struct SqlServerDataSource {
    config: AppConfig,
}

impl SqlServerDataSource {
    /// 创建新的数据源管理器
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }
    
    /// 创建数据库连接
    async fn create_connection(&self) -> Result<Client<Compat<TcpStream>>> {
        let database_config = self.config.get_database_config()?;
        let connection_string = database_config.to_connection_string();
        info!("尝试连接 SQL Server，连接字符串: {}", connection_string);
        
        // 记录数据库配置详情
        info!("数据库配置详情:");
        info!("  连接方式: {:?}", self.config.database_connection_type);
        info!("  服务器: {}:{}", database_config.server, database_config.port);
        info!("  数据库: {}", database_config.database);
        info!("  用户名: {}", database_config.user);
        info!("  密码长度: {} 字符", database_config.password.len());
        info!("  信任证书: {}", database_config.trust_server_certificate);
        
        let config = match Config::from_ado_string(&connection_string) {
            Ok(cfg) => {
                info!("连接字符串解析成功");
                info!("  解析后服务器地址: {:?}", cfg.get_addr());
                cfg
            }
            Err(e) => {
                error!("连接字符串解析失败: {}", e);
                return Err(e.into());
            }
        };
        
        // 设置连接超时
        info!("开始建立TCP连接，超时时间: {} 秒", self.config.connection.connection_timeout_secs);
        let tcp = match tokio::time::timeout(
            Duration::from_secs(self.config.connection.connection_timeout_secs),
            TcpStream::connect(config.get_addr())
        ).await {
            Ok(Ok(stream)) => {
                info!("TCP连接建立成功");
                stream
            }
            Ok(Err(e)) => {
                error!("TCP连接失败: {}", e);
                return Err(e.into());
            }
            Err(_) => {
                error!("TCP连接超时");
                return Err(anyhow::anyhow!("TCP连接超时"));
            }
        };
        
        tcp.set_nodelay(true)?;
        info!("开始SQL Server认证");
        
        let client = match Client::connect(config, tcp.compat_write()).await {
            Ok(c) => {
                info!("SQL Server认证成功，连接建立完成");
                c
            }
            Err(e) => {
                error!("SQL Server认证失败: {}", e);
                error!("可能的原因:");
                error!("  1. 用户名或密码错误");
                error!("  2. sa账户被禁用");
                error!("  3. SQL Server未启用混合模式认证");
                error!("  4. 数据库不存在或无权限访问");
                error!("  5. 服务器防火墙阻止连接");
                return Err(e.into());
            }
        };
        
        debug!("成功连接到 SQL Server");
        Ok(client)
    }
    
    /// 带重试机制的连接创建
    async fn create_connection_with_retry(&self) -> Result<Client<Compat<TcpStream>>> {
        let mut last_error = None;
        
        for attempt in 1..=self.config.connection.max_retries {
            match self.create_connection().await {
                Ok(client) => {
                    if attempt > 1 {
                        info!("第 {} 次尝试连接成功", attempt);
                    }
                    return Ok(client);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.connection.max_retries {
                        warn!("第 {} 次连接失败，{} 秒后重试: {}", 
                              attempt, self.config.connection.retry_interval_secs, last_error.as_ref().unwrap());
                        tokio::time::sleep(Duration::from_secs(self.config.connection.retry_interval_secs)).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap())
    }
    
    /// 从历史表加载初始数据
    pub async fn load_initial_data(&self, start_time: DateTime<Utc>) -> Result<Vec<TimeSeriesRecord>> {
        info!("开始从历史表加载初始数据，起始时间: {}", start_time);
        
        let mut client = self.create_connection_with_retry().await?;
        
        let sql = format!(
            "SELECT TagName, Timestamp, Value FROM {} WHERE Timestamp >= @P1 ORDER BY Timestamp",
            self.config.tables.history_table
        );
        
        let mut query = tiberius::Query::new(sql);
        query.bind(start_time);
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        
        for row in rows {
            if let Some(record) = self.parse_row(row)? {
                records.push(record);
            }
        }
        
        info!("从历史表加载了 {} 条记录", records.len());
        Ok(records)
    }
    
    /// 从TagDatabase表获取增量数据
    pub async fn get_incremental_data(&self, last_timestamp: DateTime<Utc>) -> Result<Vec<TimeSeriesRecord>> {
        debug!("获取增量数据，上次时间戳: {}", last_timestamp);
        
        let mut client = self.create_connection_with_retry().await?;
        
        let sql = format!(
            "SELECT TagName, Timestamp, Value FROM {} WHERE Timestamp > @P1 ORDER BY Timestamp",
            self.config.tables.tag_database_table
        );
        
        let mut query = tiberius::Query::new(sql);
        query.bind(last_timestamp);
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        
        for row in rows {
            if let Some(record) = self.parse_row(row)? {
                records.push(record);
            }
        }
        
        if !records.is_empty() {
            debug!("获取到 {} 条增量数据", records.len());
        }
        
        Ok(records)
    }
    
    /// 解析数据库行为时序记录
    fn parse_row(&self, row: Row) -> Result<Option<TimeSeriesRecord>> {
        let tag_name: Option<&str> = row.get(0);
        let timestamp: Option<DateTime<Utc>> = row.get(1);
        let value: Option<f64> = row.get(2);
        
        match (tag_name, timestamp, value) {
            (Some(tag), Some(ts), Some(val)) => {
                // 过滤无效数值
                if val.is_finite() {
                    Ok(Some(TimeSeriesRecord {
                        tag_name: tag.to_string(),
                        timestamp: ts,
                        value: val,
                    }))
                } else {
                    debug!("跳过无效数值: tag={}, value={}", tag, val);
                    Ok(None)
                }
            }
            _ => {
                warn!("跳过不完整的数据行: tag={:?}, timestamp={:?}, value={:?}", 
                      tag_name, timestamp, value);
                Ok(None)
            }
        }
    }
    
    /// 测试数据库连接
    pub async fn test_connection(&self) -> Result<()> {
        info!("测试 SQL Server 连接");
        let mut client = self.create_connection_with_retry().await?;
        
        let stream = tiberius::Query::new("SELECT 1 as test").query(&mut client).await?;
        let _rows = stream.into_first_result().await?;
        
        info!("SQL Server 连接测试成功");
        Ok(())
    }
}