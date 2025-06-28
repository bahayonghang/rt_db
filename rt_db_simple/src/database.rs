use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc, Local};
use log::{info, warn};
use tiberius::{Client, Config as TiberiusConfig, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::DatabaseConfig;
use crate::models::{HistoryRecord, TagRecord};

pub struct DatabaseClient {
    client: Client<Compat<TcpStream>>,
}

impl DatabaseClient {
    pub async fn new(config: &DatabaseConfig) -> Result<Self> {
        info!("正在连接数据库: {}:{}", config.server, config.port.unwrap_or(1433));
        
        let mut tiberius_config = TiberiusConfig::new();
        tiberius_config.host(&config.server);
        tiberius_config.port(config.port.unwrap_or(1433));
        tiberius_config.database(&config.database);
        tiberius_config.authentication(tiberius::AuthMethod::sql_server(&config.username, &config.password));
        tiberius_config.trust_cert();
        
        let tcp = TcpStream::connect(tiberius_config.get_addr())
            .await
            .context("无法连接到SQL Server")?;
        
        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .context("无法建立数据库连接")?;
        
        info!("数据库连接成功");
        Ok(Self { client })
    }
    
    pub async fn query_history_data(&mut self, table: &str, days: i32) -> Result<Vec<HistoryRecord>> {
        // 使用本地时间而不是UTC时间
        let end_date = Local::now();
        let start_date = end_date - Duration::days(days as i64);
        
        // 使用更宽松的日期范围查询，只比较日期部分
        let start_date_str = start_date.format("%Y-%m-%d 00:00:00").to_string();
        let end_date_str = end_date.format("%Y-%m-%d 23:59:59").to_string();
        
        let query = format!(
            "SELECT TagName, DateTime, TagVal, TagQuality FROM {} WHERE DateTime >= '{}' AND DateTime <= '{}' ORDER BY DateTime DESC",
            table,
            start_date_str,
            end_date_str
        );
        
        info!("执行历史数据查询: {}", query);
        info!("查询时间范围: {} 到 {}", start_date_str, end_date_str);
        
        let stream = self.client.query(&query, &[])
            .await
            .context("历史数据查询失败")?;
        
        let rows: Vec<Row> = stream.into_first_result().await?;
        
        if rows.is_empty() {
            warn!("未找到历史数据，请检查:");
            warn!("1. 表名是否正确: {}", table);
            warn!("2. 时间范围是否合适: {} 到 {}", start_date_str, end_date_str);
            warn!("3. 数据库中是否有数据");
            
            // 尝试查询表中总记录数
            let count_query = format!("SELECT COUNT(*) as total FROM {}", table);
            info!("尝试查询表总记录数: {}", count_query);
            
            if let Ok(count_stream) = self.client.query(&count_query, &[]).await {
                if let Ok(count_rows) = count_stream.into_first_result().await {
                    if let Some(row) = count_rows.first() {
                        if let Some(total) = row.get::<i32, _>(0) {
                            info!("表 {} 中总共有 {} 条记录", table, total);
                        }
                    }
                }
            }
            
            return Ok(vec![]);
        }
        
        let mut records = Vec::new();
        
        for row in rows {
            let tag_name: &str = row.get(0).unwrap_or("");
            let timestamp: DateTime<Utc> = row.get(1).unwrap_or(Utc::now());
            let value: f64 = row.get::<f64, _>(2).unwrap_or(0.0);
            let tag_quality: Option<&str> = row.get(3);
            
            records.push(HistoryRecord::new(
                tag_name.to_string(),
                timestamp,
                value,
                tag_quality.map(|s| s.to_string()),
            ));
        }
        
        info!("查询到 {} 条历史记录", records.len());
        Ok(records)
    }
    
    pub async fn query_tag_data(&mut self, table: &str) -> Result<Vec<TagRecord>> {
        let query = format!(
            "SELECT TagID, TagName, TagOPCName, OpcServerName, TagUnit, TagType, TagDescrip, TagVal, TagMinVal, TagMaxVal, DataRecFlag, InOrOutFlag, TagQuality FROM {} ORDER BY TagID",
            table
        );
        
        info!("执行标签数据查询: {}", query);
        
        let stream = self.client.query(&query, &[])
            .await
            .context("标签数据查询失败")?;
        
        let rows: Vec<Row> = stream.into_first_result().await?;
        let mut records = Vec::new();
        
        for row in rows {
            let tag_id: i32 = row.get(0).unwrap_or(0);
            let tag_name: &str = row.get(1).unwrap_or("");
            let tag_opc_name: Option<&str> = row.get(2);
            let opc_server_name: Option<&str> = row.get(3);
            let tag_unit: Option<&str> = row.get(4);
            let tag_type: Option<&str> = row.get(5);
            let tag_descrip: Option<&str> = row.get(6);
            let tag_val: Option<f64> = row.get::<f32, _>(7).map(|f| f as f64);
            let tag_min_val: Option<f64> = row.get::<f32, _>(8).map(|f| f as f64);
            let tag_max_val: Option<f64> = row.get::<f32, _>(9).map(|f| f as f64);
            let data_rec_flag: Option<&str> = row.get(10);
            let in_or_out_flag: Option<&str> = row.get(11);
            let tag_quality: Option<&str> = row.get(12);
            
            records.push(TagRecord::new(
                tag_id,
                tag_name.to_string(),
                tag_opc_name.map(|s| s.to_string()),
                opc_server_name.map(|s| s.to_string()),
                tag_unit.map(|s| s.to_string()),
                tag_type.map(|s| s.to_string()),
                tag_descrip.map(|s| s.to_string()),
                tag_val,
                tag_min_val,
                tag_max_val,
                data_rec_flag.map(|s| s.to_string()),
                in_or_out_flag.map(|s| s.to_string()),
                tag_quality.map(|s| s.to_string()),
            ));
        }
        
        info!("查询到 {} 条标签记录", records.len());
        Ok(records)
    }
    
    pub async fn test_connection(&mut self) -> Result<()> {
        let query = "SELECT 1 as test_value";
        let stream = self.client.query(query, &[])
            .await
            .context("连接测试失败")?;
        
        let rows: Vec<Row> = stream.into_first_result().await?;
        if !rows.is_empty() {
            info!("数据库连接测试成功");
            Ok(())
        } else {
            warn!("数据库连接测试返回空结果");
            Ok(())
        }
    }
}