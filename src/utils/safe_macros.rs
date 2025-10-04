/// 安全的锁获取宏
#[macro_export]
macro_rules! safe_lock {
    ($mutex:expr) => {
        $mutex.lock().map_err(|e| {
            log::error!("Failed to acquire lock: {}", e);
            crate::core::error::ExchangeError::Other(format!("Lock poisoned: {}", e))
        })
    };
}

/// 安全的读锁获取
#[macro_export]
macro_rules! safe_read {
    ($rwlock:expr) => {
        $rwlock.read().map_err(|e| {
            log::error!("Failed to acquire read lock: {}", e);
            crate::core::error::ExchangeError::Other(format!("Read lock poisoned: {}", e))
        })
    };
}

/// 安全的写锁获取
#[macro_export]
macro_rules! safe_write {
    ($rwlock:expr) => {
        $rwlock.write().map_err(|e| {
            log::error!("Failed to acquire write lock: {}", e);
            crate::core::error::ExchangeError::Other(format!("Write lock poisoned: {}", e))
        })
    };
}

/// 带上下文的错误转换
#[macro_export]
macro_rules! context {
    ($result:expr, $msg:expr) => {
        $result.map_err(|e| {
            log::error!("{}: {}", $msg, e);
            crate::core::error::ExchangeError::Other(format!("{}: {}", $msg, e))
        })
    };
}
