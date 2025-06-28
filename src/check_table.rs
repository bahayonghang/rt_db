use anyhow::Result;
use duckdb::Connection;
use tracing::{info, error};
use tracing_subscriber;

fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("开始检查DuckDB数据库...");
    
    // 连接DuckDB数据库
    let conn = Connection::open("./test_realtime_data.duckdb")?;
    
    info!("DuckDB连接成功，开始查询表结构...");
    
    // 查看ts_wide表结构
    info!("=== ts_wide表结构 ===");
    let mut stmt = conn.prepare("DESCRIBE ts_wide")?;
    let rows = stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let type_name: String = row.get(1)?;
        Ok((name, type_name))
    })?;
    
    for row in rows {
        let (name, type_name) = row?;
        info!("列: {} (类型: {})", name, type_name);
    }
    
    // 查询最新的10行数据的DateTime列原始值
        let query = "SELECT DateTime FROM ts_wide ORDER BY DateTime DESC LIMIT 10";
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            let datetime_str: String = row.get(0)?;
            Ok(datetime_str)
        })?;

        info!("=== ts_wide表最新10行数据 ===");
        for (i, row) in rows.enumerate() {
            let datetime_str = row?;
            info!("第{}行DateTime原始值: '{}'", i + 1, datetime_str);
        }

    Ok(())
}