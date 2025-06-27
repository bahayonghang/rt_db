use anyhow::Result;
use chrono::{DateTime, Utc};
use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use tracing::{info, debug, warn};
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
        let config = Config::from_ado_string(&self.config.database_url)?;
        
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        
        let client = Client::connect(config, tcp.compat_write()).await?;
        
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