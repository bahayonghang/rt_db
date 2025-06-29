use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc, Duration};
use tokio::time::{interval, Duration as TokioDuration};
use tracing::{info, debug, error, warn};
use crate::config::AppConfig;
use crate::database::DatabaseManager;
use crate::data_source::SqlServerDataSource;
use std::sync::Arc;

/// 标签配置信息
#[derive(Debug, Clone)]
pub struct TagConfig {
    pub tag_name: String,
    pub max_records: Option<usize>,
    pub retention_days: Option<u32>,
}

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
    
    /// 初始数据加载 - 查询过去1小时的历史数据
    pub async fn initial_load(&mut self) -> Result<()> {
        info!("开始初始数据加载...");
        
        let now = Utc::now();
        // 固定查询过去1小时的数据
        let one_hour_ago = now - Duration::hours(1);
        
        info!("历史数据时间范围: {} 到 {} (过去1小时)", one_hour_ago, now);
        
        // 查询过去1小时的历史数据
        let history_data = self.data_source.load_data_in_range(one_hour_ago, now).await
            .map_err(|e| anyhow!("加载历史数据失败: {}", e))?;
        
        let mut total_loaded = 0;
        let mut latest_timestamp: Option<DateTime<Utc>> = None;
        
        if !history_data.is_empty() {
            info!("查询到 {} 条历史记录，正在加载...", history_data.len());
            
            // 分批处理数据以避免内存溢出
            let max_memory_records = self.config.batch.max_memory_records;
            for chunk in history_data.chunks(max_memory_records) {
                self.db_manager.convert_and_insert_wide(chunk)
                    .map_err(|e| anyhow!("转换并插入宽表数据失败: {}", e))?;
                
                total_loaded += chunk.len();
                
                // 更新最新时间戳
                if let Some(last_record) = chunk.last() {
                    latest_timestamp = Some(last_record.timestamp);
                }
                
                info!("已加载 {} 条记录，累计: {}", chunk.len(), total_loaded);
            }
        } else {
            info!("过去1小时内无历史数据");
        }
        
        // 查询TagDatabase中的当前数据
        info!("开始查询TagDatabase中的当前数据...");
        let tagdb_data = self.data_source.get_latest_tagdb_data().await
            .map_err(|e| anyhow!("获取TagDatabase数据失败: {}", e))?;
        
        if !tagdb_data.is_empty() {
            info!("查询到 {} 条TagDatabase记录，正在加载...", tagdb_data.len());
            
            // 分批处理TagDatabase数据
            let max_memory_records = self.config.batch.max_memory_records;
            for chunk in tagdb_data.chunks(max_memory_records) {
                self.db_manager.convert_and_insert_wide(chunk)
                    .map_err(|e| anyhow!("转换并插入TagDatabase数据失败: {}", e))?;
                
                total_loaded += chunk.len();
                
                // 更新最新时间戳
                if let Some(last_record) = chunk.last() {
                    latest_timestamp = Some(last_record.timestamp);
                }
                
                info!("已加载 {} 条TagDatabase记录，累计: {}", chunk.len(), total_loaded);
            }
        } else {
            info!("TagDatabase中无数据");
        }
        
        // 更新最后见到的时间戳
        if let Some(timestamp) = latest_timestamp {
            self.last_seen_timestamp = Some(timestamp);
        } else {
            self.last_seen_timestamp = Some(now);
        }
        
        // 初始化标签变化检测（建立基线）
        info!("建立标签变化检测基线...");
        let known_tags = self.db_manager.get_known_tags();
        let tag_changes = self.data_source.detect_tag_changes(&known_tags).await
            .map_err(|e| anyhow!("初始标签检测失败: {}", e))?;
        
        // 处理初始标签变化（主要是新增标签）
        if !tag_changes.added_tags.is_empty() {
            info!("初始化时发现新标签: {:?}", tag_changes.added_tags);
            self.db_manager.handle_tag_changes(&tag_changes)
                .map_err(|e| anyhow!("处理初始标签变化失败: {}", e))?;
        }
        
        // 清理超过3天的旧数据
        info!("开始清理超过3天的旧数据...");
        self.cleanup_old_data().await
            .map_err(|e| anyhow!("清理旧数据失败: {}", e))?;
        
        let record_count = self.db_manager.get_record_count()
            .map_err(|e| anyhow::anyhow!("获取记录总数失败: {}", e))?;
        
        if total_loaded > 0 {
            info!("初始数据加载完成，共加载 {} 条记录，数据库总记录数: {}，已转换为宽表格式", 
                  total_loaded, record_count);
        } else {
            warn!("未找到初始数据");
        }
        
        Ok(())
    }
    
    /// 启动周期性更新任务
    pub async fn start_periodic_update(&mut self) -> Result<()> {
        debug!("启动周期性更新任务，更新间隔: {} 秒", self.config.update_interval_secs);
        
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
        
        // 1. 检测标签变化（加点/少点）
        let known_tags = self.db_manager.get_known_tags();
        debug!("当前已知标签数量: {}", known_tags.len());
        
        let tag_changes = self.data_source.detect_tag_changes(&known_tags).await
            .map_err(|e| anyhow!("检测标签变化失败: {}", e))?;
        
        info!("标签变化检测结果: 新增 {} 个, 删除 {} 个, 当前总数 {}", 
              tag_changes.added_tags.len(), 
              tag_changes.removed_tags.len(), 
              tag_changes.current_tags.len());
        
        // 2. 处理标签变化
        if !tag_changes.added_tags.is_empty() || !tag_changes.removed_tags.is_empty() {
            info!("处理标签变化: 新增标签 {:?}, 删除标签 {:?}", 
                  tag_changes.added_tags, tag_changes.removed_tags);
            
            self.db_manager.handle_tag_changes(&tag_changes)
                .map_err(|e| anyhow!("处理标签变化失败: {}", e))?;
            
            // 如果有删除的标签，可选择清理其数据
            if !tag_changes.removed_tags.is_empty() {
                let cleaned_count = self.db_manager.cleanup_removed_tag_data(&tag_changes.removed_tags)
                    .map_err(|e| anyhow!("清理已删除标签数据失败: {}", e))?;
                if cleaned_count > 0 {
                    info!("已清理 {} 条已删除标签的数据记录", cleaned_count);
                }
            }
        }
        
        // 3. 获取TagDatabase的最新数据并拼接到宽表
        let latest_data = self.fetch_incremental_data().await?;
        
        if !latest_data.is_empty() {
            self.db_manager.append_latest_tagdb_data(&latest_data)
                .map_err(|e| anyhow!("拼接最新TagDB数据失败: {}", e))?;
            
            // 更新最后见到的时间戳为当前时间
            self.last_seen_timestamp = Some(Utc::now());
            
            info!("更新成功: {} 条记录", latest_data.len());
        } else {
            debug!("TagDatabase表中没有数据");
        }
        
        // 4. 清理3天前的数据以维持数据库大小
        self.cleanup_old_data().await
            .map_err(|e| anyhow!("清理旧数据失败: {}", e))?;
        
        debug!("更新周期完成");
        Ok(())
    }
    
    /// 从TagDatabase获取最新数据
    async fn fetch_incremental_data(&mut self) -> Result<Vec<crate::database::TimeSeriesRecord>> {
        debug!("开始获取TagDatabase最新数据...");
        
        // 获取TagDatabase的最新数据
        let latest_data = self.data_source.get_latest_tagdb_data().await
            .map_err(|e| anyhow!("获取TagDatabase数据失败: {}", e))?;
        
        if !latest_data.is_empty() {
            info!("从TagDatabase获取到 {} 条最新数据", latest_data.len());
            debug!("TagDatabase数据更新完成");
        } else {
            debug!("TagDatabase中没有新数据");
        }
        
        Ok(latest_data)
    }
    
    /// 清理3天前的数据以维持数据库大小
    pub async fn cleanup_old_data(&self) -> Result<()> {
        info!("开始清理3天前的数据...");
        
        let deleted_count = self.db_manager.delete_data_older_than_days(3)
            .map_err(|e| anyhow!("删除旧数据失败: {}", e))?;
        
        if deleted_count > 0 {
            let total_records = self.db_manager.get_record_count()
                .map_err(|e| anyhow!("获取记录总数失败: {}", e))?;
            info!("清理完成，删除了 {} 条旧数据，当前总记录数: {}", deleted_count, total_records);
        } else {
            debug!("没有需要清理的旧数据");
        }
        
        Ok(())
    }
    
    /// 删除给定时间以前的数据
    pub async fn delete_data_before_time(&self, cutoff_time: DateTime<Utc>) -> Result<()> {
        info!("开始删除{}以前的数据...", cutoff_time);
        
        let deleted_count = self.db_manager.delete_data_before_time(cutoff_time)
            .map_err(|e| anyhow!("删除指定时间前数据失败: {}", e))?;
        
        if deleted_count > 0 {
            info!("删除完成，删除了 {} 条数据", deleted_count);
        } else {
            debug!("没有需要删除的数据");
        }
        
        Ok(())
    }
    
    /// 管理标签数据 - 已简化为按时间清理数据
    #[allow(dead_code)]
    async fn manage_tag_data(&self, _new_records: &[crate::database::TimeSeriesRecord]) -> Result<()> {
        // 此方法已被简化的时间清理策略替代
        Ok(())
    }
    
    /// 查询TagDatabase获取标签配置 - 已废弃
    #[allow(dead_code)]
    async fn query_tag_database(&self, tag_name: &str) -> Result<TagConfig> {
        // 此方法已被简化的时间清理策略替代
        Ok(TagConfig {
            tag_name: tag_name.to_string(),
            max_records: Some(8000),
            retention_days: Some(30),
        })
    }
    
    /// 获取服务状态信息
    pub async fn get_status(&self) -> Result<ServiceStatus> {
        let total_records = self.db_manager.get_record_count()
            .map_err(|e| anyhow!("获取记录总数失败: {}", e))?;
        let latest_timestamp = self.db_manager.get_latest_timestamp()
            .map_err(|e| anyhow!("获取最新时间戳失败: {}", e))?;
        
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