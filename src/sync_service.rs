use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use tokio::time::{interval, Duration as TokioDuration};
use tracing::{info, debug, error, warn};
use crate::config::AppConfig;
use crate::database::DatabaseManager;
use crate::data_source::SqlServerDataSource;
use std::sync::Arc;

/// 数据同步服务
pub struct SyncService {
    config: Arc<AppConfig>,
    db_manager: Arc<DatabaseManager>,
    data_source: Arc<SqlServerDataSource>,
    last_seen_timestamp: Option<DateTime<Utc>>,
}

impl SyncService {
    /// 创建新的同步服务
    pub fn new(
        config: Arc<AppConfig>,
        db_manager: Arc<DatabaseManager>,
        data_source: Arc<SqlServerDataSource>,
    ) -> Self {
        Self {
            config,
            db_manager,
            data_source,
            last_seen_timestamp: None,
        }
    }
    
    /// 执行初始数据加载
    pub async fn initial_load(&mut self) -> Result<()> {
        info!("开始执行初始数据加载");
        
        // 计算起始时间（当前时间 - 数据窗口天数）
        let now = Utc::now();
        let start_time = now - Duration::days(self.config.data_window_days as i64);
        
        info!("初始数据加载时间范围: {} 到 {}", start_time, now);
        
        // 从SQL Server加载数据
        let records = self.data_source.load_initial_data(start_time).await?;
        
        if records.is_empty() {
            warn!("未找到初始数据");
            self.last_seen_timestamp = Some(now);
            return Ok(());
        }
        
        // 批量插入到本地数据库
        self.db_manager.batch_insert(&records)?;
        
        // 更新最后见到的时间戳
        if let Some(last_record) = records.last() {
            self.last_seen_timestamp = Some(last_record.timestamp);
        }
        
        let record_count = self.db_manager.get_record_count()?;
        info!("初始数据加载完成，共加载 {} 条记录，数据库总记录数: {}", 
              records.len(), record_count);
        
        Ok(())
    }
    
    /// 启动周期性更新任务
    pub async fn start_periodic_update(&mut self) -> Result<()> {
        info!("启动周期性更新任务，更新间隔: {} 秒", self.config.update_interval_secs);
        
        let mut interval_timer = interval(TokioDuration::from_secs(self.config.update_interval_secs));
        
        // 跳过第一个立即触发的tick
        interval_timer.tick().await;
        
        loop {
            interval_timer.tick().await;
            
            if let Err(e) = self.update_cycle().await {
                error!("更新周期执行失败: {}", e);
                // 继续下一个周期，不退出服务
            }
        }
    }
    
    /// 执行一次更新周期
    async fn update_cycle(&mut self) -> Result<()> {
        debug!("开始执行更新周期");
        
        // 1. 拉取增量数据
        let new_records = self.fetch_incremental_data().await?;
        
        // 2. 插入新数据
        if !new_records.is_empty() {
            self.db_manager.batch_insert(&new_records)?;
            
            // 更新最后见到的时间戳
            if let Some(last_record) = new_records.last() {
                self.last_seen_timestamp = Some(last_record.timestamp);
            }
            
            info!("本次更新插入 {} 条新记录", new_records.len());
        }
        
        // 3. 清理过期数据
        self.cleanup_expired_data().await?;
        
        debug!("更新周期完成");
        Ok(())
    }
    
    /// 获取增量数据
    async fn fetch_incremental_data(&self) -> Result<Vec<crate::database::TimeSeriesRecord>> {
        let last_timestamp = self.last_seen_timestamp.unwrap_or_else(|| {
            // 如果没有最后时间戳，使用当前时间减去一个更新间隔
            Utc::now() - Duration::seconds(self.config.update_interval_secs as i64)
        });
        
        debug!("获取增量数据，上次时间戳: {}", last_timestamp);
        
        let records = self.data_source.get_incremental_data(last_timestamp).await?;
        
        if !records.is_empty() {
            debug!("获取到 {} 条增量数据", records.len());
        }
        
        Ok(records)
    }
    
    /// 清理过期数据
    async fn cleanup_expired_data(&self) -> Result<()> {
        let now = Utc::now();
        let cutoff_time = now - Duration::seconds(self.config.data_window_duration_secs());
        
        debug!("清理过期数据，截止时间: {}", cutoff_time);
        
        let deleted_count = self.db_manager.delete_expired_data(cutoff_time)?;
        
        if deleted_count > 0 {
            let total_records = self.db_manager.get_record_count()?;
            info!("清理了 {} 条过期数据，当前总记录数: {}", deleted_count, total_records);
        }
        
        Ok(())
    }
    
    /// 获取服务状态信息
    pub async fn get_status(&self) -> Result<ServiceStatus> {
        let total_records = self.db_manager.get_record_count()?;
        let latest_timestamp = self.db_manager.get_latest_timestamp()?;
        
        Ok(ServiceStatus {
            total_records,
            latest_timestamp,
            last_seen_timestamp: self.last_seen_timestamp,
            data_window_days: self.config.data_window_days,
            update_interval_secs: self.config.update_interval_secs,
        })
    }
}

/// 服务状态信息
#[derive(Debug)]
pub struct ServiceStatus {
    pub total_records: i64,
    pub latest_timestamp: Option<DateTime<Utc>>,
    pub last_seen_timestamp: Option<DateTime<Utc>>,
    pub data_window_days: u32,
    pub update_interval_secs: u64,
}

impl std::fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== 实时数据缓存服务状态 ===")?;
        writeln!(f, "总记录数: {}", self.total_records)?;
        writeln!(f, "最新数据时间: {:?}", self.latest_timestamp)?;
        writeln!(f, "最后同步时间: {:?}", self.last_seen_timestamp)?;
        writeln!(f, "数据窗口: {} 天", self.data_window_days)?;
        writeln!(f, "更新间隔: {} 秒", self.update_interval_secs)?;
        Ok(())
    }
}