use anyhow::Result;
use log::{error, info};

mod config;
mod database;
mod models;

use config::load_config;
use database::DatabaseClient;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();
    
    info!("启动实时数据查询服务");
    
    // 1. 加载配置
    let config = match load_config("config.toml") {
        Ok(config) => {
            info!("配置加载成功");
            config
        }
        Err(e) => {
            error!("配置加载失败: {}", e);
            return Err(e);
        }
    };
    
    // 2. 连接数据库
    let mut db_client = match DatabaseClient::new(&config.database).await {
        Ok(client) => {
            info!("数据库连接成功");
            client
        }
        Err(e) => {
            error!("数据库连接失败: {}", e);
            return Err(e);
        }
    };
    
    // 3. 测试数据库连接
    if let Err(e) = db_client.test_connection().await {
        error!("数据库连接测试失败: {}", e);
        return Err(e);
    }
    
    // 4. 查询历史表数据
    println!("\n=== 查询历史表最近{}天数据 ===", config.query.days_back);
    match db_client.query_history_data(&config.query.history_table, config.query.days_back).await {
        Ok(history_data) => {
            if history_data.is_empty() {
                println!("未找到历史数据");
            } else {
                println!("找到 {} 条历史记录:", history_data.len());
                for (index, record) in history_data.iter().enumerate() {
                    println!(
                        "[{}] 标签: {}, 时间: {}, 值: {:.2}, 质量: {}",
                        index + 1,
                        record.tag_name,
                        record.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
                        record.value,
                        record.tag_quality.as_deref().unwrap_or("无")
                    );
                    
                    // 限制显示前10条记录，避免输出过多
                    if index >= 9 {
                        println!("... (显示前10条记录，共{}条)", history_data.len());
                        break;
                    }
                }
            }
        }
        Err(e) => {
            error!("查询历史数据失败: {}", e);
            println!("查询历史数据失败: {}", e);
        }
    }
    
    // 5. 查询TagDatabase数据
    println!("\n=== 查询TagDatabase数据 ===");
    match db_client.query_tag_data(&config.query.tag_table).await {
        Ok(tag_data) => {
            if tag_data.is_empty() {
                println!("未找到标签数据");
            } else {
                println!("找到 {} 条标签记录:", tag_data.len());
                for (index, record) in tag_data.iter().enumerate() {
                    println!(
                        "[{}] ID: {}, 标签: {}, OPC名称: {}, 服务器: {}, 单位: {}, 类型: {}, 描述: {}, 当前值: {}, 最小值: {}, 最大值: {}, 记录标志: {}, 输入输出: {}, 质量: {}",
                        index + 1,
                        record.tag_id,
                        record.tag_name,
                        record.tag_opc_name.as_deref().unwrap_or("无"),
                        record.opc_server_name.as_deref().unwrap_or("无"),
                        record.tag_unit.as_deref().unwrap_or("无"),
                        record.tag_type.as_deref().unwrap_or("无"),
                        record.tag_descrip.as_deref().unwrap_or("无"),
                        record.tag_val.map(|v| format!("{:.2}", v)).unwrap_or("无".to_string()),
                        record.tag_min_val.map(|v| format!("{:.2}", v)).unwrap_or("无".to_string()),
                        record.tag_max_val.map(|v| format!("{:.2}", v)).unwrap_or("无".to_string()),
                        record.data_rec_flag.as_deref().unwrap_or("无"),
                        record.in_or_out_flag.as_deref().unwrap_or("无"),
                        record.tag_quality.as_deref().unwrap_or("无")
                    );
                    
                    // 限制显示前10条记录，避免输出过多
                    if index >= 9 {
                        println!("... (显示前10条记录，共{}条)", tag_data.len());
                        break;
                    }
                }
            }
        }
        Err(e) => {
            error!("查询标签数据失败: {}", e);
            println!("查询标签数据失败: {}", e);
        }
    }
    
    info!("程序执行完成");
    Ok(())
}
