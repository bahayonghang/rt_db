use anyhow::Result as AnyhowResult;
use chrono::{DateTime, Utc};
use duckdb::Connection;
use std::path::Path;
use tracing::{info, debug, error};
use anyhow::Result;

/// 时序数据记录
#[derive(Debug, Clone)]
pub struct TimeSeriesRecord {
    pub tag_name: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

/// 宽表格式的时序数据记录
#[derive(Debug, Clone)]
pub struct WideTimeSeriesRecord {
    pub timestamp: DateTime<Utc>,
    pub tag_values: std::collections::HashMap<String, f64>,
}

/// DuckDB 数据库管理器
pub struct DatabaseManager {
    db_path: String,
    known_tags: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl DatabaseManager {
    /// 创建新的数据库管理器
    pub fn new(db_path: String) -> Self {
        Self { 
            db_path,
            known_tags: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
    
    /// 初始化数据库（删除旧文件并创建新的数据库结构）
    pub fn initialize(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("初始化数据库: {}", self.db_path);
        
        // 删除已存在的数据库文件
        if Path::new(&self.db_path).exists() {
            std::fs::remove_file(&self.db_path)?;
            info!("已删除旧的数据库文件");
        }
        
        // 创建新的数据库连接
        let conn = Connection::open(&self.db_path)?;
        
        // 只创建宽表
        self.create_wide_table(&conn)?;
        
        // 创建索引
        self.create_wide_table_index(&conn)?;
        
        info!("数据库初始化完成");
        Ok(())
    }
    
    /// 创建宽表格式的时序数据表
    fn create_wide_table(&self, conn: &Connection) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sql = r#"
            CREATE TABLE ts_wide (
                DateTime TIMESTAMP PRIMARY KEY
            )
        "#;
        
        conn.execute(sql, [])?;
        info!("已创建 ts_wide 宽表");
        Ok(())
    }
    
    /// 创建宽表索引
    fn create_wide_table_index(&self, conn: &Connection) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sql = "CREATE INDEX idx_datetime ON ts_wide (DateTime)";
        conn.execute(sql, [])?;
        info!("已创建 idx_datetime 索引");
        Ok(())
    }
    
    /// 获取数据库连接
    pub fn get_connection(&self) -> Result<Connection, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Connection::open(&self.db_path)?)
    }
    
    /// 重构历史数据为宽表格式并插入
    pub fn convert_and_insert_wide(&self, records: &[TimeSeriesRecord]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if records.is_empty() {
            return Ok(());
        }
        
        // 按时间戳分组数据
        let mut grouped_data: std::collections::HashMap<DateTime<Utc>, std::collections::HashMap<String, f64>> = std::collections::HashMap::new();
        
        for record in records {
            grouped_data
                .entry(record.timestamp)
                .or_insert_with(std::collections::HashMap::new)
                .insert(record.tag_name.clone(), record.value);
        }
        
        // 获取所有唯一的标签名
        let all_tags: std::collections::HashSet<String> = records.iter()
            .map(|r| r.tag_name.clone())
            .collect();
        
        // 动态添加列到宽表
        self.add_columns_to_wide_table(&all_tags)?;
        
        // 插入宽表数据
        self.insert_wide_data(&grouped_data, &all_tags)?;
        
        debug!("重构并插入 {} 个时间点的历史数据到宽表", grouped_data.len());
        Ok(())
    }
    
    /// 将TagDatabase的最新数据拼接到宽表
    pub fn append_latest_tagdb_data(&self, records: &[TimeSeriesRecord]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if records.is_empty() {
            return Ok(());
        }
        
        // 使用北京时间作为时间戳 (UTC+8)
        let current_time = Utc::now() + chrono::Duration::hours(8);
        
        // 将所有记录按当前时间分组
        let mut tag_values = std::collections::HashMap::new();
        for record in records {
            tag_values.insert(record.tag_name.clone(), record.value);
        }
        
        // 获取所有标签名
        let all_tags: std::collections::HashSet<String> = records.iter()
            .map(|r| r.tag_name.clone())
            .collect();
        
        // 动态添加列到宽表
        self.add_columns_to_wide_table(&all_tags)?;
        
        // 创建分组数据
        let mut grouped_data = std::collections::HashMap::new();
        grouped_data.insert(current_time, tag_values);
        
        // 插入宽表数据
        self.insert_wide_data(&grouped_data, &all_tags)?;
        
        debug!("拼接 {} 个标签的最新数据到宽表，时间戳: {}", records.len(), current_time);
        Ok(())
    }
    
    /// 删除给定时间以前的数据
    pub fn delete_data_before_time(&self, cutoff_time: DateTime<Utc>) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        
        let sql = "DELETE FROM ts_wide WHERE DateTime < ?";
        let cutoff_str = cutoff_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        
        let deleted_rows = conn.execute(sql, [&cutoff_str])?;
        
        if deleted_rows > 0 {
            info!("删除了 {} 条给定时间前的数据，截止时间: {}", deleted_rows, cutoff_str);
        }
        
        Ok(deleted_rows)
    }
    
    /// 插入宽表数据
    fn insert_wide_data(
        &self,
        grouped_data: &std::collections::HashMap<DateTime<Utc>, std::collections::HashMap<String, f64>>,
        all_tags: &std::collections::HashSet<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        
        for (timestamp, tag_values) in grouped_data {
            // 构建列名和值的列表
            let mut columns = vec!["DateTime".to_string()];
            // 直接使用UTC时间，不进行时区转换
            let mut values = vec![timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string()];
            
            for tag in all_tags {
                let safe_column_name = self.sanitize_column_name(tag);
                columns.push(safe_column_name);
                let value = tag_values.get(tag).unwrap_or(&0.0);
                values.push(value.to_string());
            }
            
            // 构建INSERT OR REPLACE SQL (DuckDB语法)
            let columns_str = columns.join(", ");
            let placeholders = vec!["?"; columns.len()].join(", ");
            
            let sql = format!(
                "INSERT OR REPLACE INTO ts_wide ({}) VALUES ({})",
                columns_str, placeholders
            );
            
            conn.execute(&sql, duckdb::params_from_iter(values.iter()))?;
        }
        
        Ok(())
    }
    
    /// 动态添加列到宽表
    fn add_columns_to_wide_table(&self, tags: &std::collections::HashSet<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        
        // 获取现有列 - 使用DuckDB的DESCRIBE语法
        let mut existing_columns = std::collections::HashSet::new();
        let mut stmt = conn.prepare("DESCRIBE ts_wide")?;
        let rows = stmt.query_map([], |row| {
            let column_name: String = row.get(0)?; // DuckDB的DESCRIBE返回列名在第0列
            Ok(column_name)
        })?;
        
        for row in rows {
            existing_columns.insert(row?);
        }
        
        // 更新已知标签集合
        {
            let mut known_tags = self.known_tags.lock().unwrap();
            for tag in tags {
                known_tags.insert(tag.clone());
            }
        }
        
        // 添加新列
        for tag in tags {
            let safe_column_name = self.sanitize_column_name(tag);
            if !existing_columns.contains(&safe_column_name) {
                let sql = format!("ALTER TABLE ts_wide ADD COLUMN {} DOUBLE", safe_column_name);
                conn.execute(&sql, [])?;
                debug!("添加新列: {}", safe_column_name);
            }
        }
        
        Ok(())
    }
    
    /// 清理列名，确保SQL安全
    fn sanitize_column_name(&self, tag_name: &str) -> String {
        let mut result = tag_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect::<String>()
            .trim_matches('_')
            .to_string();
        
        // 确保列名不以数字开头
        if result.chars().next().map_or(false, |c| c.is_ascii_digit()) {
            result = format!("tag_{}", result);
        }
        
        // 确保列名不为空
        if result.is_empty() {
            result = "unknown_tag".to_string();
        }
        
        result
    }
    

    
    /// 根据标签删除最旧的数据
    pub fn delete_oldest_by_tag(&self, tag_name: &str, keep_count: usize) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        let safe_column_name = self.sanitize_column_name(tag_name);
        
        // 获取该标签的总记录数
        let count_sql = format!(
            "SELECT COUNT(*) FROM ts_wide WHERE {} IS NOT NULL",
            safe_column_name
        );
        let total_count: i64 = conn.query_row(&count_sql, [], |row| row.get(0))?;
        
        if total_count <= keep_count as i64 {
            return Ok(0); // 不需要删除
        }
        
        let delete_count = total_count - keep_count as i64;
        
        // 删除最旧的记录（将对应列设为NULL）
        let delete_sql = format!(
            "UPDATE ts_wide SET {} = NULL WHERE DateTime IN (
                SELECT DateTime FROM ts_wide 
                WHERE {} IS NOT NULL 
                ORDER BY DateTime ASC 
                LIMIT {}
            )",
            safe_column_name, safe_column_name, delete_count
        );
        
        let updated_rows = conn.execute(&delete_sql, [])?;
        
        if updated_rows > 0 {
            info!("标签 {} 删除了 {} 条最旧数据", tag_name, updated_rows);
        }
        
        Ok(updated_rows)
    }
    
    /// 删除指定天数前的数据以维持数据库大小
    pub fn delete_data_older_than_days(&self, days: u32) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        
        // 计算截止时间
        let cutoff_time = Utc::now() - chrono::Duration::days(days as i64);
        let cutoff_str = cutoff_time.format("%Y-%m-%d %H:%M:%S").to_string();
        
        // 删除ts_wide表中的旧数据
        let delete_sql = "DELETE FROM ts_wide WHERE DateTime < ?";
        let deleted_rows = conn.execute(delete_sql, [&cutoff_str])?;
        
        if deleted_rows > 0 {
            info!("删除了{}天前的数据: {}条", days, deleted_rows);
        }
        
        Ok(deleted_rows)
    }
    
    /// 获取数据库中的记录总数
    pub fn get_record_count(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM ts_wide")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }
    
    /// 获取最新的时间戳
    pub fn get_latest_timestamp(&self) -> Result<Option<DateTime<Utc>>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT MAX(DateTime) FROM ts_wide")?;
        
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
    
    /// 获取所有已知的标签名
    pub fn get_known_tags(&self) -> Vec<String> {
        let known_tags = self.known_tags.lock().unwrap();
        known_tags.iter().cloned().collect()
    }
}