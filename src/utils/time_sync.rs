//! 时间同步模块
//!
//! 用于与交易所服务器同步时间，减少时间戳错误

use crate::core::{error::ExchangeError, exchange::Exchange};
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 时间同步管理器
pub struct TimeSyncManager {
    /// 时间偏移量（毫秒）
    time_offset_ms: Arc<RwLock<i64>>,
    /// 最后同步时间
    last_sync: Arc<RwLock<DateTime<Utc>>>,
    /// 同步间隔（秒）
    sync_interval_secs: u64,
}

impl TimeSyncManager {
    /// 创建时间同步管理器
    pub fn new(sync_interval_secs: u64) -> Self {
        Self {
            time_offset_ms: Arc::new(RwLock::new(0)),
            last_sync: Arc::new(RwLock::new(Utc::now())),
            sync_interval_secs,
        }
    }

    /// 同步时间
    pub async fn sync_with_exchange(&self, exchange: &dyn Exchange) -> Result<(), ExchangeError> {
        let start_time = Utc::now();

        // 获取服务器时间
        let server_time = exchange.get_server_time().await?;

        // 假设网络延迟对称，取中点
        let end_time = Utc::now();
        let local_time = start_time + (end_time - start_time) / 2;

        // 计算时间偏移（服务器时间 - 本地时间）
        let offset_ms = (server_time.timestamp_millis() - local_time.timestamp_millis()) as i64;

        // 更新偏移量
        *self.time_offset_ms.write().await = offset_ms;
        *self.last_sync.write().await = Utc::now();

        log::info!(
            "⏰ 时间同步完成: 服务器时间 {}, 本地时间 {}, 偏移 {}ms",
            server_time.format("%Y-%m-%d %H:%M:%S%.3f"),
            local_time.format("%Y-%m-%d %H:%M:%S%.3f"),
            offset_ms
        );

        // 如果时间偏差超过1秒，发出警告
        if offset_ms.abs() > 1000 {
            log::warn!("⚠️ 系统时间与服务器相差 {}ms，建议同步系统时间", offset_ms);
        }

        Ok(())
    }

    /// 获取调整后的时间戳（毫秒）
    pub async fn get_adjusted_timestamp(&self) -> i64 {
        let offset = *self.time_offset_ms.read().await;
        Utc::now().timestamp_millis() + offset
    }

    /// 检查是否需要重新同步
    pub async fn needs_sync(&self) -> bool {
        let last_sync = *self.last_sync.read().await;
        let elapsed = Utc::now() - last_sync;
        elapsed.num_seconds() as u64 > self.sync_interval_secs
    }

    /// 启动自动同步任务
    pub async fn start_auto_sync(self: Arc<Self>, exchange: Arc<dyn Exchange>) {
        let sync_manager = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                sync_manager.sync_interval_secs,
            ));

            loop {
                interval.tick().await;

                if let Err(e) = sync_manager.sync_with_exchange(exchange.as_ref()).await {
                    log::error!("时间同步失败: {}", e);
                }
            }
        });

        log::info!("🔄 启动自动时间同步，间隔 {} 秒", self.sync_interval_secs);
    }

    /// 获取时间偏移量
    pub async fn get_offset_ms(&self) -> i64 {
        *self.time_offset_ms.read().await
    }
}

/// 全局时间同步管理器
static mut GLOBAL_TIME_SYNC: Option<Arc<TimeSyncManager>> = None;
static INIT: std::sync::Once = std::sync::Once::new();

/// 初始化全局时间同步
pub fn init_global_time_sync() -> Arc<TimeSyncManager> {
    unsafe {
        INIT.call_once(|| {
            GLOBAL_TIME_SYNC = Some(Arc::new(TimeSyncManager::new(300))); // 5分钟同步一次
        });
        GLOBAL_TIME_SYNC.as_ref().unwrap().clone()
    }
}

/// 获取全局时间同步管理器
pub fn get_time_sync() -> Option<Arc<TimeSyncManager>> {
    unsafe { GLOBAL_TIME_SYNC.as_ref().map(|s| s.clone()) }
}

/// 获取同步后的时间戳
pub async fn get_synced_timestamp() -> i64 {
    if let Some(sync) = get_time_sync() {
        sync.get_adjusted_timestamp().await
    } else {
        Utc::now().timestamp_millis()
    }
}

/// 日志辅助函数 - 带毫秒的时间戳
pub fn log_with_millis(level: log::Level, message: &str) {
    let timestamp = Utc::now();
    let formatted = format!(
        "[{}] {}",
        timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
        message
    );

    match level {
        log::Level::Error => log::error!("{}", formatted),
        log::Level::Warn => log::warn!("{}", formatted),
        log::Level::Info => log::info!("{}", formatted),
        log::Level::Debug => log::debug!("{}", formatted),
        log::Level::Trace => log::trace!("{}", formatted),
    }
}
