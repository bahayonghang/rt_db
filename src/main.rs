mod config;
mod database;
mod data_source;
mod sync_service;

use anyhow::Result;
use std::sync::Arc;
use tokio::signal; // 已在wait_for_shutdown_signal函数中按需导入
use tracing::{info, error, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::AppConfig;
use database::DatabaseManager;
use data_source::SqlServerDataSource;
use sync_service::SyncService;

#[tokio::main]
async fn main() -> Result<()> {
    // 加载配置
    let config = match AppConfig::load("config.toml") {
        Ok(config) => {
            Arc::new(config)
        }
        Err(e) => {
            eprintln!("配置加载失败: {}", e);
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
        return Err(e);
    }
    
    // 初始化数据源
    let data_source = Arc::new(SqlServerDataSource::new((*config).clone()));
    
    // 测试数据源连接
    if let Err(e) = data_source.test_connection().await {
        error!("数据源连接测试失败: {}", e);
        return Err(e);
    }
    
    // 创建同步服务
    let mut sync_service = SyncService::new(
        config.clone(),
        db_manager.clone(),
        data_source.clone(),
    );
    
    // 执行初始数据加载
    info!("开始初始数据加载...");
    if let Err(e) = sync_service.initial_load().await {
        error!("初始数据加载失败: {}", e);
        return Err(e);
    }
    
    // 显示初始状态
    if let Ok(status) = sync_service.get_status().await {
        info!("\n{}", status);
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
                    info!("定期状态报告:\n{}", status);
                }
            }
        })
    };
    
    info!("服务启动完成，等待终止信号...");
    
    // 等待终止信号
    wait_for_shutdown_signal().await;
    
    info!("收到终止信号，开始优雅停机...");
    
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
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer()
            .with_target(false)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
        )
        .init();
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
