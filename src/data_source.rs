use anyhow::{Result, Context};
use chrono::{DateTime, Utc, Local, NaiveDateTime};
use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use tracing::{info, debug, warn, error};
use crate::database::TimeSeriesRecord;
use crate::config::AppConfig;
use std::time::Duration;
use std::collections::HashSet;

/// 标签变化信息
#[derive(Debug, Clone)]
pub struct TagChanges {
    /// 新增的标签
    pub added_tags: Vec<String>,
    /// 删除的标签
    pub removed_tags: Vec<String>,
    /// 当前所有标签
    pub current_tags: std::collections::HashSet<String>,
}

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
    
        debug!("正在连接数据库: {}:{}", database_config.server, database_config.port);
        
        // 使用与简化版相同的连接方式
        let mut tiberius_config = Config::new();
        tiberius_config.host(&database_config.server);
        tiberius_config.port(database_config.port);
        tiberius_config.database(&database_config.database);
        tiberius_config.authentication(tiberius::AuthMethod::sql_server(&database_config.user, &database_config.password));
        tiberius_config.trust_cert();
        
        let tcp = tokio::net::TcpStream::connect(tiberius_config.get_addr())
            .await
            .context("无法连接到SQL Server")?;
        
        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .context("无法建立数据库连接")?;
        
        debug!("数据库连接成功");
        Ok(client)
    }
    
    /// 带重试机制的连接创建
    pub async fn create_connection_with_retry(&self) -> Result<Client<Compat<TcpStream>>> {
        let mut last_error = None;
        
        for attempt in 1..=self.config.connection.max_retries {
            match self.create_connection().await {
                Ok(client) => {
                    if attempt > 1 {
                        debug!("第 {} 次尝试连接成功", attempt);
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
    
    /// 从历史表加载初始数据 - 只查询DateTime、TagName、TagVal三个字段
    pub async fn load_initial_data(&self, start_time: DateTime<Utc>) -> Result<Vec<TimeSeriesRecord>> {
        debug!("开始从历史表加载初始数据，起始时间: {}", start_time);
        
        let mut client = self.create_connection_with_retry().await?;
        
        let sql = format!(
            "SELECT * FROM [{}] WHERE [DateTime] >= @P1 ORDER BY [DateTime]",
            self.config.tables.history_table
        );
        
        let mut query = tiberius::Query::new(sql);
        query.bind(start_time);
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        
        for row in rows {
            if let Some(record) = self.parse_tagdb_row(row)? {
                records.push(record);
            }
        }
        
        debug!("从历史表加载了 {} 条记录", records.len());
        Ok(records)
    }
    
    /// 按时间范围从历史表加载数据（分批加载优化）
    pub async fn load_data_in_range(&self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Result<Vec<TimeSeriesRecord>> {
        debug!("按时间范围加载数据: {} 到 {}", start_time, end_time);
        
        let mut client = self.create_connection_with_retry().await?;
        
        let sql = format!(
            "SELECT * FROM [{}] WHERE [DateTime] >= @P1 AND [DateTime] < @P2 ORDER BY [DateTime]",
            self.config.tables.history_table
        );
        
        let mut query = tiberius::Query::new(sql);
        query.bind(start_time);
        query.bind(end_time);
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        
        for row in rows {
            if let Some(record) = self.parse_tagdb_row(row)? {
                records.push(record);
            }
        }
        
        debug!("按时间范围加载了 {} 条记录", records.len());
        Ok(records)
    }
    
    /// 从TagDatabase表获取增量数据 - 只查询DateTime、TagName、TagVal三个字段
    pub async fn get_incremental_data(&self, last_timestamp: DateTime<Utc>) -> Result<Vec<TimeSeriesRecord>> {
        debug!("获取增量数据，上次时间戳: {}", last_timestamp);
        
        let mut client = self.create_connection_with_retry().await?;
        
        // 将DateTime转换为SQL Server兼容的字符串格式
        let timestamp_str = last_timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        
        let sql = format!(
            "SELECT [DataTime], [TagName], [TagVal] FROM [{}] WHERE [DataTime] > '{}' ORDER BY [DataTime]",
            self.config.tables.tag_database_table, timestamp_str
        );
        
        let query = tiberius::Query::new(sql);
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        
        for row in rows {
            if let Some(record) = self.parse_simplified_row(row)? {
                records.push(record);
            }
        }
        
        if !records.is_empty() {
            debug!("获取到 {} 条增量数据", records.len());
        }
        
        Ok(records)
    }
    
    /// 获取TagDatabase表的最新数据（忽略DataTime，使用当前时间）
    pub async fn get_latest_tagdb_data(&self) -> Result<Vec<TimeSeriesRecord>> {
        debug!("开始查询TagDatabase表的最新数据");
        
        let mut client = self.create_connection_with_retry().await?;
        
        // 查询TagDatabase表的TagName和TagVal，忽略DataTime
        let sql = format!(
            "SELECT [TagName], [TagVal] FROM [{}]",
            self.config.tables.tag_database_table
        );
        
        let query = tiberius::Query::new(sql);
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        // 直接使用UTC时间，database.rs中会自动转换为北京时间显示
        let current_time = Utc::now();
        
        for row in rows {
            if let Some(record) = self.parse_tagdb_current_row(row, current_time)? {
                records.push(record);
            }
        }
        
        debug!("从TagDatabase表获取到 {} 条最新数据", records.len());
        
        Ok(records)
    }
    
    /// 检测TagDatabase表的标签变化（加点/少点）
    pub async fn detect_tag_changes(&self, known_tags: &std::collections::HashSet<String>) -> Result<TagChanges> {
        debug!("开始检测TagDatabase表的标签变化");
        
        let mut client = self.create_connection_with_retry().await?;
        
        // 查询TagDatabase表中所有唯一的TagName
        let sql = format!(
            "SELECT DISTINCT [TagName] FROM [{}] WHERE [TagName] IS NOT NULL",
            self.config.tables.tag_database_table
        );
        
        let query = tiberius::Query::new(sql);
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut current_tags = std::collections::HashSet::new();
        for row in rows {
            if let Some(tag_name) = row.get::<&str, _>(0) {
                current_tags.insert(tag_name.trim().to_string());
            }
        }
        
        // 计算新增和删除的标签
        let added_tags: Vec<String> = current_tags.difference(known_tags)
            .cloned()
            .collect();
        let removed_tags: Vec<String> = known_tags.difference(&current_tags)
            .cloned()
            .collect();
        
        let changes = TagChanges {
            added_tags,
            removed_tags,
            current_tags,
        };
        
        if !changes.added_tags.is_empty() {
            info!("检测到新增标签: {:?}", changes.added_tags);
        }
        if !changes.removed_tags.is_empty() {
            warn!("检测到删除标签: {:?}", changes.removed_tags);
        }
        
        Ok(changes)
    }
    
    /// 获取指定标签的最新数据
    pub async fn get_specific_tags_data(&self, tag_names: &[String]) -> Result<Vec<TimeSeriesRecord>> {
        if tag_names.is_empty() {
            return Ok(Vec::new());
        }
        
        debug!("开始查询指定标签的最新数据: {:?}", tag_names);
        
        let mut client = self.create_connection_with_retry().await?;
        
        // 构建IN子句
        let tag_placeholders: Vec<String> = (1..=tag_names.len())
            .map(|i| format!("@P{}", i))
            .collect();
        let in_clause = tag_placeholders.join(", ");
        
        let sql = format!(
            "SELECT [TagName], [TagVal] FROM [{}] WHERE [TagName] IN ({})",
            self.config.tables.tag_database_table, in_clause
        );
        
        let mut query = tiberius::Query::new(sql);
        for tag_name in tag_names {
            query.bind(tag_name.as_str());
        }
        
        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;
        
        let mut records = Vec::new();
        let current_time = Utc::now();
        
        for row in rows {
            if let Some(record) = self.parse_tagdb_current_row(row, current_time)? {
                records.push(record);
            }
        }
        
        debug!("获取到 {} 条指定标签数据", records.len());
        Ok(records)
    }
    
    /// 解析日期时间字符串 (格式: "21/5/2024 10:15:01")
    fn parse_datetime_string(&self, datetime_str: &str) -> Result<DateTime<Utc>> {
        // 尝试解析 DD/M/YYYY HH:MM:SS 格式
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(datetime_str, "%d/%m/%Y %H:%M:%S") {
            return Ok(naive_dt.and_utc());
        }
        
        // 尝试解析 D/M/YYYY HH:MM:SS 格式
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(datetime_str, "%d/%m/%Y %H:%M:%S") {
            return Ok(naive_dt.and_utc());
        }
        
        // 如果都失败，返回错误
        Err(anyhow::anyhow!("无法解析日期时间字符串: {}", datetime_str))
    }
    
    /// 解析简化的数据库行为时序记录 (DateTime, TagName, TagVal)
    fn parse_simplified_row(&self, row: Row) -> Result<Option<TimeSeriesRecord>> {
        // SQL Server的datetime类型应该使用NaiveDateTime获取，然后转换为UTC
        let timestamp: Option<NaiveDateTime> = row.get(0);
        let tag_name: Option<&str> = row.get(1);
        
        // 尝试获取f64，如果失败则尝试f32并转换
        let value: Option<f64> = match row.try_get::<f64, _>(2) {
            Ok(val) => val,
            Err(_) => {
                // 如果f64失败，尝试f32并转换为f64
                match row.try_get::<f32, _>(2) {
                    Ok(Some(f32_val)) => Some(f32_val as f64),
                    Ok(None) => None,
                    Err(e) => {
                        warn!("无法解析数值字段: {}", e);
                        None
                    }
                }
            }
        };
        
        match (timestamp, tag_name) {
            (Some(naive_ts), Some(tag)) => {
                // 处理None值为0.0，保持总行数不变
                let val = value.unwrap_or(0.0);
                
                // 过滤无效数值，将其设为0.0
                let final_val = if val.is_finite() { val } else { 0.0 };
                
                // 假设SQL Server中的时间是北京时间，需要转换为UTC存储
                // 将NaiveDateTime转换为UTC DateTime，然后减去8小时
                let utc_timestamp = naive_ts.and_utc();
                let beijing_timestamp = utc_timestamp - chrono::Duration::hours(8);
                
                Ok(Some(TimeSeriesRecord {
                    tag_name: tag.trim().to_string(), // 去除标签名的空格
                    timestamp: beijing_timestamp,
                    value: final_val,
                }))
            }
            _ => {
                warn!("跳过不完整的数据行: timestamp={:?}, tag={:?}, value={:?}", 
                      timestamp, tag_name, value);
                Ok(None)
            }
        }
    }
    
    /// 解析TagDatabase表的行为时序记录 (DateTime, 标签名, 数值)
    fn parse_tagdb_row(&self, row: Row) -> Result<Option<TimeSeriesRecord>> {
        // SQL Server的datetime类型应该使用NaiveDateTime获取
        let timestamp: Option<NaiveDateTime> = row.get(0);
        let tag_name: Option<&str> = row.get(1);
        
        // 尝试获取f64，如果失败则尝试f32并转换
        let value: Option<f64> = match row.try_get::<f64, _>(2) {
            Ok(val) => val,
            Err(_) => {
                // 如果f64失败，尝试f32并转换为f64
                match row.try_get::<f32, _>(2) {
                    Ok(Some(f32_val)) => Some(f32_val as f64),
                    Ok(None) => None,
                    Err(e) => {
                        warn!("无法解析数值字段: {}", e);
                        None
                    }
                }
            }
        };
        
        match (timestamp, tag_name) {
            (Some(naive_ts), Some(tag)) => {
                // 处理None值为0.0，保持总行数不变
                let val = value.unwrap_or(0.0);
                
                // 过滤无效数值，将其设为0.0
                let final_val = if val.is_finite() { val } else { 0.0 };
                
                // 假设SQL Server中的时间是北京时间，需要转换为UTC存储
                // 将NaiveDateTime转换为UTC DateTime，然后减去8小时
                let utc_timestamp = naive_ts.and_utc();
                let beijing_timestamp = utc_timestamp - chrono::Duration::hours(8);
                
                Ok(Some(TimeSeriesRecord {
                    tag_name: tag.trim().to_string(), // 去除标签名的空格
                    timestamp: beijing_timestamp,
                    value: final_val,
                }))
            }
            _ => {
                warn!("跳过不完整的数据行: timestamp={:?}, tag={:?}, value={:?}", 
                      timestamp, tag_name, value);
                Ok(None)
            }
        }
    }
    
    /// 解析TagDatabase表当前数据行（只有TagName和TagVal，使用当前时间）
    fn parse_tagdb_current_row(&self, row: Row, current_time: DateTime<Utc>) -> Result<Option<TimeSeriesRecord>> {
        let tag_name: Option<&str> = row.get(0);
        
        // 尝试获取f64，如果失败则尝试f32并转换
        let value: Option<f64> = match row.try_get::<f64, _>(1) {
            Ok(val) => val,
            Err(_) => {
                // 如果f64失败，尝试f32并转换为f64
                match row.try_get::<f32, _>(1) {
                    Ok(Some(f32_val)) => Some(f32_val as f64),
                    Ok(None) => None,
                    Err(e) => {
                        warn!("无法解析数值字段: {}", e);
                        None
                    }
                }
            }
        };
        
        match tag_name {
            Some(tag) => {
                // 处理None值为0.0，保持总行数不变
                let val = value.unwrap_or(0.0);
                
                // 过滤无效数值，将其设为0.0
                let final_val = if val.is_finite() { val } else { 0.0 };
                
                Ok(Some(TimeSeriesRecord {
                    tag_name: tag.trim().to_string(), // 去除标签名的空格
                    timestamp: current_time,
                    value: final_val,
                }))
            }
            _ => {
                warn!("跳过不完整的数据行: tag={:?}, value={:?}", 
                      tag_name, value);
                Ok(None)
            }
        }
    }
    
    /// 解析数据库行为时序记录 (保留兼容性)
    fn parse_row(&self, row: Row) -> Result<Option<TimeSeriesRecord>> {
        let tag_name: Option<&str> = row.get(0);
        // SQL Server的datetime类型应该使用NaiveDateTime获取
        let timestamp: Option<NaiveDateTime> = row.get(1);
        
        // 尝试获取f64，如果失败则尝试f32并转换
        let value: Option<f64> = match row.try_get::<f64, _>(2) {
            Ok(val) => val,
            Err(_) => {
                // 如果f64失败，尝试f32并转换为f64
                match row.try_get::<f32, _>(2) {
                    Ok(Some(f32_val)) => Some(f32_val as f64),
                    Ok(None) => None,
                    Err(e) => {
                        warn!("无法解析数值字段: {}", e);
                        None
                    }
                }
            }
        };
        
        match (tag_name, timestamp) {
            (Some(tag), Some(naive_ts)) => {
                // 处理None值为0.0，保持总行数不变
                let val = value.unwrap_or(0.0);
                
                // 过滤无效数值，将其设为0.0
                let final_val = if val.is_finite() { val } else { 0.0 };
                
                // 将NaiveDateTime转换为UTC DateTime
                let utc_timestamp = naive_ts.and_utc();
                
                Ok(Some(TimeSeriesRecord {
                    tag_name: tag.trim().to_string(), // 去除标签名的空格
                    timestamp: utc_timestamp,
                    value: final_val,
                }))
            }
            _ => {
                warn!("跳过不完整的数据行: tag={:?}, timestamp={:?}, value={:?}", 
                      tag_name, timestamp, value);
                Ok(None)
            }
        }
    }
    
    /// 查询历史数据
    pub async fn query_history_data(&self, table: &str, days: i32) -> Result<Vec<TimeSeriesRecord>> {
        info!("开始查询历史数据，表: {}, 天数: {}", table, days);
        
        let mut client = self.create_connection_with_retry().await?;
        
        // 使用本地时间计算日期范围，精确到天
        let end_date = Local::now().date_naive();
        let start_date = end_date - chrono::Duration::days(days as i64);
        
        let query = format!(
            "SELECT * FROM [{}] WHERE CAST([DateTime] AS DATE) >= '{}' AND CAST([DateTime] AS DATE) <= '{}' ORDER BY [DateTime]",
            table, start_date, end_date
        );
        
        info!("执行历史数据查询: {}", query);
        
        let stream = tiberius::Query::new(query)
            .query(&mut client)
            .await
            .context("历史数据查询失败")?;
        
        let rows = stream.into_first_result().await?;
        
        if rows.is_empty() {
            warn!("未找到历史数据，请检查:");
            warn!("  - 表名是否正确: {}", table);
            warn!("  - 时间范围: {} 到 {}", start_date, end_date);
            
            // 尝试查询表的总记录数
            let count_query = format!("SELECT COUNT(*) FROM {}", table);
            match tiberius::Query::new(count_query).query(&mut client).await {
                Ok(count_stream) => {
                    if let Ok(count_rows) = count_stream.into_first_result().await {
                        if let Some(count_row) = count_rows.into_iter().next() {
                            if let Some(count) = count_row.get::<i32, _>(0) {
                                warn!("  - 表 {} 总记录数: {}", table, count);
                            }
                        }
                    }
                }
                Err(e) => warn!("无法查询表记录数: {}", e),
            }
        }
        
        let mut records = Vec::new();
        
        for row in rows {
            if let Some(record) = self.parse_simplified_row(row)? {
                records.push(record);
            }
        }
        
        info!("查询到 {} 条历史记录", records.len());
        Ok(records)
    }
    
    /// 解析历史数据行
    fn parse_history_row(&self, row: Row) -> Result<Option<TimeSeriesRecord>> {
        let tag_name: Option<&str> = row.get(0);
        let timestamp: Option<DateTime<Utc>> = row.get(1);
        
        // 尝试获取f64，如果失败则尝试f32并转换
        let value: Option<f64> = match row.try_get::<f64, _>(2) {
            Ok(val) => val,
            Err(_) => {
                // 如果f64失败，尝试f32并转换为f64
                match row.try_get::<f32, _>(2) {
                    Ok(Some(f32_val)) => Some(f32_val as f64),
                    Ok(None) => None,
                    Err(e) => {
                        warn!("无法解析数值字段: {}", e);
                        None
                    }
                }
            }
        };
        let _quality: Option<&str> = row.get(3);
        
        match (tag_name, timestamp, value) {
            (Some(tag), Some(ts), Some(val)) => {
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
        debug!("测试 SQL Server 连接");
        let mut client = self.create_connection_with_retry().await?;
        
        let stream = tiberius::Query::new("SELECT 1 as test").query(&mut client).await?;
        let _rows = stream.into_first_result().await?;
        
        info!("SQL Server 连接成功");
        Ok(())
    }
}