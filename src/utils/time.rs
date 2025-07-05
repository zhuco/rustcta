//! 时间处理工具模块
//! 提供与交易所时间同步功能

use crate::error::AppError;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// 全局时间差异（本地时间 - 服务器时间）
static TIME_OFFSET: AtomicI64 = AtomicI64::new(0);

/// 同步本地时间与服务器时间
/// 计算时间差异并存储，后续所有时间戳都会使用这个差异进行调整
pub async fn sync_time_with_server<F, Fut>(get_server_time: F) -> Result<(), AppError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<i64, AppError>>,
{
    let local_time = get_current_timestamp();
    let server_time = get_server_time().await?;
    let offset = local_time - server_time;

    TIME_OFFSET.store(offset, Ordering::SeqCst);

    println!("时间同步完成:");
    println!("  本地时间: {}", local_time);
    println!("  服务器时间: {}", server_time);
    println!("  时间差异: {} ms", offset);

    Ok(())
}

/// 获取当前本地时间戳（毫秒）
pub fn get_current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// 获取调整后的时间戳（用于API请求）
/// 返回本地时间减去时间差异后的时间戳
pub fn get_adjusted_timestamp() -> i64 {
    let local_time = get_current_timestamp();
    let offset = TIME_OFFSET.load(Ordering::SeqCst);
    local_time - offset
}

/// 获取当前时间差异
pub fn get_time_offset() -> i64 {
    TIME_OFFSET.load(Ordering::SeqCst)
}
