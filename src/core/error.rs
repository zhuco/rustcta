use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExchangeError {
    #[error("网络请求错误: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("JSON序列化错误: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("YAML配置错误: {0}")]
    YamlError(#[from] serde_yaml::Error),

    #[error("环境变量错误: {0}")]
    EnvError(#[from] dotenv::Error),

    #[error("API错误: {code} - {message}")]
    ApiError { code: i32, message: String },

    #[error("认证错误: {0}")]
    AuthError(String),

    #[error("交易对格式错误: {0}")]
    SymbolError(String),

    #[error("订单错误: {0}")]
    OrderError(String),

    #[error("余额不足: 需要 {required}, 可用 {available}")]
    InsufficientBalance { required: f64, available: f64 },

    #[error("WebSocket错误: {0}")]
    WebSocketError(String),

    #[error("速率限制: {0}")]
    RateLimitError(String, Option<u64>),

    #[error("不支持的交易所: {0}")]
    UnsupportedExchange(String),

    #[error("不支持的市场类型: {market_type:?} (交易所: {exchange})")]
    UnsupportedMarketType {
        market_type: crate::core::types::MarketType,
        exchange: String,
    },

    #[error("不支持的订单类型: {order_type:?} (交易所: {exchange})")]
    UnsupportedOrderType {
        order_type: crate::core::types::OrderType,
        exchange: String,
    },

    #[error("不支持的时间间隔: {interval} (交易所: {exchange})")]
    UnsupportedInterval { interval: String, exchange: String },

    #[error("配置错误: {0}")]
    ConfigError(String),

    #[error("参数验证错误: {field} - {reason}")]
    ValidationError { field: String, reason: String },

    #[error("权限不足: {0}")]
    PermissionError(String, String),

    #[error("数据解析错误: {0}")]
    ParseError(String),

    #[error("超时错误: 操作 '{operation}' 超时 ({timeout_seconds}秒)")]
    TimeoutError {
        operation: String,
        timeout_seconds: u64,
    },

    #[error("维护中: {0}")]
    MaintenanceError(String, Option<chrono::DateTime<chrono::Utc>>),

    #[error("订单未找到: ID {order_id} (交易对: {symbol})")]
    OrderNotFound { order_id: String, symbol: String },

    #[error("交易对未找到: {symbol} (市场: {market_type:?})")]
    SymbolNotFound {
        symbol: String,
        market_type: crate::core::types::MarketType,
    },

    #[error("功能未实现: {0}")]
    NotImplemented(String),

    #[error("不支持的功能: {0}")]
    NotSupported(String),

    #[error("其他错误: {0}")]
    Other(String),
}

impl ExchangeError {
    /// 判断错误是否可以重试
    pub fn is_retryable(&self) -> bool {
        match self {
            ExchangeError::NetworkError(_) => true,
            ExchangeError::TimeoutError { .. } => true,
            ExchangeError::RateLimitError(_, _) => true,
            ExchangeError::MaintenanceError(_, _) => true,
            ExchangeError::ApiError { code, .. } => {
                // HTTP 5xx 错误通常可以重试
                *code >= 500 && *code < 600
            }
            _ => false,
        }
    }

    /// 获取建议的重试等待时间(秒)
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            ExchangeError::RateLimitError(_, retry_after) => *retry_after,
            ExchangeError::NetworkError(_) => Some(1),
            ExchangeError::TimeoutError { .. } => Some(2),
            ExchangeError::ApiError { code, .. } if *code >= 500 => Some(5),
            _ => None,
        }
    }

    /// 获取错误的严重程度
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            ExchangeError::NetworkError(_) => ErrorSeverity::Warning,
            ExchangeError::TimeoutError { .. } => ErrorSeverity::Warning,
            ExchangeError::RateLimitError(_, _) => ErrorSeverity::Warning,
            ExchangeError::MaintenanceError(_, _) => ErrorSeverity::Info,
            ExchangeError::ValidationError { .. } => ErrorSeverity::Error,
            ExchangeError::AuthError(_) => ErrorSeverity::Critical,
            ExchangeError::PermissionError(_, _) => ErrorSeverity::Critical,
            ExchangeError::ConfigError(_) => ErrorSeverity::Critical,
            ExchangeError::UnsupportedExchange(_) => ErrorSeverity::Critical,
            _ => ErrorSeverity::Error,
        }
    }

    /// 获取用户友好的错误描述
    pub fn user_friendly_message(&self) -> String {
        match self {
            ExchangeError::NetworkError(_) => "网络连接问题，请检查网络状态".to_string(),
            ExchangeError::AuthError(_) => "API认证失败，请检查密钥配置".to_string(),
            ExchangeError::RateLimitError(_, retry_after) => {
                if let Some(seconds) = retry_after {
                    format!("请求过于频繁，请等待{}秒后重试", seconds)
                } else {
                    "请求过于频繁，请稍后重试".to_string()
                }
            }
            ExchangeError::InsufficientBalance {
                required,
                available,
            } => {
                format!("余额不足，需要{:.8}，可用{:.8}", required, available)
            }
            ExchangeError::OrderNotFound { order_id, .. } => {
                format!("订单{}不存在或已过期", order_id)
            }
            ExchangeError::SymbolNotFound { symbol, .. } => {
                format!("交易对{}不存在或未开放交易", symbol)
            }
            ExchangeError::MaintenanceError(msg, recovery_time) => {
                if let Some(time) = recovery_time {
                    format!(
                        "系统维护中：{}，预计{}恢复",
                        msg,
                        time.format("%Y-%m-%d %H:%M UTC")
                    )
                } else {
                    format!("系统维护中：{}", msg)
                }
            }
            _ => self.to_string(),
        }
    }
}

/// 错误严重程度
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorSeverity {
    Info,     // 信息性错误，通常不影响操作
    Warning,  // 警告性错误，可能影响性能但可以重试
    Error,    // 一般错误，需要用户处理
    Critical, // 严重错误，需要立即处理
}
