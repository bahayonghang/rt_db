mod config;
mod database;
mod data_source;
mod sync_service;

use anyhow::Result;
use std::sync::Arc;
use tracing::{info, error, warn, debug};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use tracing_appender::{rolling, non_blocking};
use std::fs;

use config::AppConfig;
use database::DatabaseManager;
use data_source::SqlServerDataSource;
use sync_service::SyncService;

/// 检查表结构
async fn check_table_structure(data_source: &SqlServerDataSource) -> Result<()> {
    debug!("开始检查表结构...");
    
    let mut client = data_source.create_connection_with_retry().await?;
    
    // 检查TagDatabase表结构
    debug!("检查TagDatabase表结构:");
    let stream = client.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TagDatabase'", &[]).await?;
    let rows = stream.into_first_result().await?;
    
    for row in rows {
        let column_name: &str = row.get(0).unwrap_or("");
        let data_type: &str = row.get(1).unwrap_or("");
        debug!("TagDatabase列名: {}, 类型: {}", column_name, data_type);
    }
    
    // 检查历史表结构
    debug!("检查历史表结构:");
    let stream = client.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '历史表'", &[]).await?;
    let rows = stream.into_first_result().await?;
    
    for row in rows {
        let column_name: &str = row.get(0).unwrap_or("");
        let data_type: &str = row.get(1).unwrap_or("");
        debug!("历史表列名: {}, 类型: {}", column_name, data_type);
    }
    
    debug!("表结构检查完成");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // 检查命令行参数
    let args: Vec<String> = std::env::args().collect();
    
    // 如果参数包含 --test-config，运行配置测试// 检查是否运行测试
    if args.len() > 1 && args[1] == "--test-config" {
        println!("配置测试功能已移除");
        return Ok(());
    }
    
    // 加载配置
    let config = match AppConfig::load("config.toml") {
        Ok(config) => {
            Arc::new(config)
        }
        Err(e) => {
            eprintln!("配置加载失败: {}", e);
            eprintln!("提示: 可以运行 'cargo run -- --test-config' 来测试配置解析功能");
            return Err(e);
        }
    };
    
    // 初始化日志系统
    init_logging(&config);
    
    info!("=== 实时数据缓存服务启动 ===");
    info!("配置加载成功");
    
    // 初始化数据库管理器
    let db_manager = Arc::new(DatabaseManager::new(config.db_file_path.clone()));
    
    // 初始化数据库结构
    if let Err(e) = db_manager.initialize() {
        error!("数据库初始化失败: {}", e);
        return Err(anyhow::anyhow!("数据库初始化失败: {}", e));
    }
    
    // 初始化数据源
    let data_source = Arc::new(SqlServerDataSource::new((*config).clone()));
    
    // 测试数据源连接
    if let Err(e) = data_source.test_connection().await {
        error!("数据源连接测试失败: {}", e);
        return Err(anyhow::anyhow!("数据源连接测试失败: {}", e));
    }
    
    // 检查表结构
    check_table_structure(&data_source).await?;
    
    // 注释掉测试历史数据查询功能，因为已改为在initial_load中查询过去1小时数据
    // debug!("开始测试历史数据查询功能...");
    // match data_source.query_history_data(&config.query.history_table, config.query.days_back).await {
    //     Ok(records) => {
    //         if records.is_empty() {
    //             warn!("未找到历史数据");
    //         } else {
    //             debug!("成功查询到 {} 条历史记录", records.len());
    //         }
    //     }
    //     Err(e) => {
    //         error!("查询历史数据失败: {}", e);
    //         warn!("历史数据查询失败，但程序将继续运行其他功能");
    //     }
    // }
    
    // 创建同步服务
    let mut sync_service = SyncService::new(
        config.clone(),
        db_manager.clone(),
        data_source.clone(),
    );
    
    // 执行初始数据加载
    debug!("开始初始数据加载...");
    if let Err(e) = sync_service.initial_load().await {
        error!("初始数据加载失败: {}", e);
        return Err(anyhow::anyhow!("初始数据加载失败: {}", e));
    }
    
    // 显示初始状态
    if let Ok(status) = sync_service.get_status().await {
        debug!("\n{}", status);
    }
    
    // 启动周期性更新任务
    let update_handle = {
        let mut service = SyncService::new(
            config.clone(),
            db_manager.clone(),
            data_source.clone(),
        );
        
        tokio::spawn(async move {
            if let Err(e) = service.start_periodic_update().await {
                error!("周期性更新任务失败: {}", e);
            }
        })
    };
    
    // 启动状态报告任务
    let status_handle = {
        let service = SyncService::new(
            config.clone(),
            db_manager.clone(),
            data_source.clone(),
        );
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // 5分钟
            interval.tick().await; // 跳过第一个立即触发
            
            loop {
                interval.tick().await;
                if let Ok(status) = service.get_status().await {
                    debug!("定期状态报告:\n{}", status);
                }
            }
        })
    };
    
    info!("服务启动完成，等待终止信号...");
    
    // 等待终止信号
    wait_for_shutdown_signal().await;
    
    info!("收到终止信号，开始停机...");
    
    // 取消任务
    update_handle.abort();
    status_handle.abort();
    
    // 等待任务完成（最多等待5秒）
    let shutdown_timeout = tokio::time::Duration::from_secs(5);
    if let Err(_) = tokio::time::timeout(shutdown_timeout, async {
        let _ = update_handle.await;
        let _ = status_handle.await;
    }).await {
        warn!("任务停止超时，强制退出");
    }
    
    info!("服务已停止");
    Ok(())
}

/// 初始化日志系统
fn init_logging(config: &AppConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&format!("{},tiberius=warn,tokio_util=warn", &config.log_level)));
    
    // 创建logs目录（如果不存在）
    fs::create_dir_all("logs").expect("无法创建logs目录");
    
    // 设置日志文件，按天滚动
    let file_appender = rolling::daily("logs", "rt_db.log");
    let (non_blocking_appender, guard) = non_blocking(file_appender);
    
    // 将guard泄漏以保持文件写入器活跃
    std::mem::forget(guard);
    
    // 创建控制台输出层 - 精简格式，使用北京时间
    let console_layer = fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_timer(fmt::time::OffsetTime::new(
            time::UtcOffset::from_hms(8, 0, 0).unwrap(),
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap()
        ));
    
    // 创建文件输出层 - 精简格式，使用北京时间
    let file_layer = fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_timer(fmt::time::OffsetTime::new(
            time::UtcOffset::from_hms(8, 0, 0).unwrap(),
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap()
        ))
        .with_writer(non_blocking_appender);
    
    // 注册所有层
    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .init();
    
    info!("日志系统初始化完成，日志文件保存在 logs/rt_db.log");
}

/// 等待停机信号
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to create SIGTERM signal");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to create SIGINT signal");
        
        tokio::select! {
            _ = sigterm.recv() => info!("收到 SIGTERM 信号"),
            _ = sigint.recv() => info!("收到 SIGINT 信号"),
        }
    }
    
    #[cfg(windows)]
    {
        let ctrl_c = signal::ctrl_c();
        ctrl_c.await.expect("failed to listen for ctrl+c");
        info!("收到 Ctrl+C 信号");
    }
}
