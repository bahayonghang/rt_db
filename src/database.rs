use anyhow::Result;
use chrono::{DateTime, Utc};
use duckdb::Connection;
use std::path::Path;
use tracing::{info, debug, error};

/// 时序数据记录
#[derive(Debug, Clone)]
pub struct TimeSeriesRecord {
    pub tag_name: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

/// DuckDB 数据库管理器
pub struct DatabaseManager {
    db_path: String,
}

impl DatabaseManager {
    /// 创建新的数据库管理器
    pub fn new(db_path: String) -> Self {
        Self { db_path }
    }
    
    /// 初始化数据库（删除旧文件并创建新的数据库结构）
    pub fn initialize(&self) -> Result<()> {
        info!("初始化数据库: {}", self.db_path);
        
        // 删除已存在的数据库文件
        if Path::new(&self.db_path).exists() {
            std::fs::remove_file(&self.db_path)?;
            info!("已删除旧的数据库文件");
        }
        
        // 创建新的数据库连接
        let conn = Connection::open(&self.db_path)?;
        
        // 创建表
        self.create_table(&conn)?;
        
        // 创建索引
        self.create_index(&conn)?;
        
        info!("数据库初始化完成");
        Ok(())
    }
    
    /// 创建时序数据表
    fn create_table(&self, conn: &Connection) -> Result<()> {
        let sql = r#"
            CREATE TABLE ts_data (
                tag_name VARCHAR NOT NULL,
                ts TIMESTAMP NOT NULL,
                value DOUBLE NOT NULL
            )
        "#;
        
        conn.execute(sql, [])?;
        info!("已创建 ts_data 表");
        Ok(())
    }
    
    /// 创建索引
    fn create_index(&self, conn: &Connection) -> Result<()> {
        let sql = "CREATE INDEX idx_tag_ts ON ts_data (tag_name, ts)";
        conn.execute(sql, [])?;
        info!("已创建 idx_tag_ts 索引");
        Ok(())
    }
    
    /// 获取数据库连接
    pub fn get_connection(&self) -> Result<Connection> {
        Ok(Connection::open(&self.db_path)?)
    }
    
    /// 批量插入数据
    pub fn batch_insert(&self, records: &[TimeSeriesRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        
        let conn = self.get_connection()?;
        let mut appender = conn.appender("ts_data")?;
        
        for record in records {
            appender.append_row(duckdb::params![
                record.tag_name.clone(),
                record.timestamp,
                record.value,
            ])?;
        }
        
        appender.flush()?;
        debug!("批量插入 {} 条记录", records.len());
        Ok(())
    }
    
    /// 删除过期数据
    pub fn delete_expired_data(&self, cutoff_time: DateTime<Utc>) -> Result<usize> {
        let conn = self.get_connection()?;
        
        let sql = "DELETE FROM ts_data WHERE ts < ?";
        let cutoff_str = cutoff_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        
        let deleted_rows = conn.execute(sql, [&cutoff_str])?;
        
        if deleted_rows > 0 {
            info!("删除了 {} 条过期数据，截止时间: {}", deleted_rows, cutoff_str);
        }
        
        Ok(deleted_rows)
    }
    
    /// 获取数据库中的记录总数
    pub fn get_record_count(&self) -> Result<i64> {
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM ts_data")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }
    
    /// 获取最新的时间戳
    pub fn get_latest_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT MAX(ts) FROM ts_data")?;
        
        let result = stmt.query_row([], |row| {
            let ts_str: Option<String> = row.get(0)?;
            Ok(ts_str)
        });
        
        match result {
            Ok(Some(ts_str)) => {
                let timestamp = DateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S%.3f")
                    .or_else(|_| DateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S"))?;
                Ok(Some(timestamp.with_timezone(&Utc)))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                error!("获取最新时间戳失败: {}", e);
                Ok(None)
            }
        }
    }
}