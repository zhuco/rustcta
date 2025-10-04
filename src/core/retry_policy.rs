use crate::core::error::ExchangeError;
use async_trait::async_trait;
/// 智能重试策略
use std::time::Duration;
use tokio::time::sleep;

/// 重试策略配置
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 最大重试次数
    pub max_retries: u32,
    /// 初始延迟（毫秒）
    pub initial_delay_ms: u64,
    /// 最大延迟（毫秒）
    pub max_delay_ms: u64,
    /// 指数退避因子
    pub backoff_factor: f64,
    /// 是否添加抖动
    pub jitter: bool,
    /// 可重试的错误类型
    pub retryable_errors: Vec<RetryableError>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 10000,
            backoff_factor: 2.0,
            jitter: true,
            retryable_errors: vec![
                RetryableError::NetworkError,
                RetryableError::Timeout,
                RetryableError::RateLimit,
                RetryableError::ServerError,
            ],
        }
    }
}

/// 可重试的错误类型
#[derive(Debug, Clone, PartialEq)]
pub enum RetryableError {
    NetworkError,
    Timeout,
    RateLimit,
    ServerError,
    TemporaryError,
    MaintenanceError,
}

/// 重试策略trait
#[async_trait]
pub trait RetryPolicy: Send + Sync {
    /// 判断是否应该重试
    fn should_retry(&self, error: &ExchangeError, attempt: u32) -> bool;

    /// 计算重试延迟
    fn calculate_delay(&self, attempt: u32) -> Duration;

    /// 执行带重试的操作
    async fn execute_with_retry<F, T, Fut>(&self, operation: F) -> Result<T, ExchangeError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, ExchangeError>> + Send,
        T: Send;
}

/// 指数退避重试策略
pub struct ExponentialBackoffRetry {
    config: RetryConfig,
}

impl ExponentialBackoffRetry {
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    pub fn with_initial_delay(mut self, delay_ms: u64) -> Self {
        self.config.initial_delay_ms = delay_ms;
        self
    }

    /// 判断错误是否可重试
    fn is_retryable_error(&self, error: &ExchangeError) -> bool {
        match error {
            ExchangeError::NetworkError(_) => self
                .config
                .retryable_errors
                .contains(&RetryableError::NetworkError),
            ExchangeError::RateLimitError(_, _) => self
                .config
                .retryable_errors
                .contains(&RetryableError::RateLimit),
            ExchangeError::ApiError { message, .. } => {
                // 检查是否是临时错误
                if message.contains("timeout") || message.contains("Timeout") {
                    return self
                        .config
                        .retryable_errors
                        .contains(&RetryableError::Timeout);
                }
                if message.contains("503") || message.contains("502") || message.contains("500") {
                    return self
                        .config
                        .retryable_errors
                        .contains(&RetryableError::ServerError);
                }
                if message.contains("maintenance") || message.contains("Maintenance") {
                    return self
                        .config
                        .retryable_errors
                        .contains(&RetryableError::MaintenanceError);
                }
                false
            }
            _ => false,
        }
    }
}

#[async_trait]
impl RetryPolicy for ExponentialBackoffRetry {
    fn should_retry(&self, error: &ExchangeError, attempt: u32) -> bool {
        if attempt >= self.config.max_retries {
            return false;
        }

        self.is_retryable_error(error)
    }

    fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_delay =
            self.config.initial_delay_ms as f64 * self.config.backoff_factor.powi(attempt as i32);

        let mut delay_ms = base_delay.min(self.config.max_delay_ms as f64) as u64;

        // 添加抖动以避免雷同重试
        if self.config.jitter {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let jitter = rng.gen_range(0..=delay_ms / 4);
            delay_ms = delay_ms + jitter;
        }

        Duration::from_millis(delay_ms)
    }

    async fn execute_with_retry<F, T, Fut>(&self, operation: F) -> Result<T, ExchangeError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, ExchangeError>> + Send,
        T: Send,
    {
        let mut attempt = 0;

        loop {
            match operation().await {
                Ok(result) => {
                    if attempt > 0 {
                        log::info!("✅ 操作在第{}次尝试后成功", attempt + 1);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    if !self.should_retry(&error, attempt) {
                        log::error!("❌ 操作失败且不可重试: {:?}", error);
                        return Err(error);
                    }

                    let delay = self.calculate_delay(attempt);
                    log::warn!(
                        "⚠️ 操作失败，将在{:.2}秒后重试 (尝试 {}/{}): {:?}",
                        delay.as_secs_f64(),
                        attempt + 1,
                        self.config.max_retries,
                        error
                    );

                    attempt += 1;

                    sleep(delay).await;
                }
            }
        }
    }
}

/// 自适应重试策略（根据错误类型调整策略）
pub struct AdaptiveRetryPolicy {
    network_retry: ExponentialBackoffRetry,
    rate_limit_retry: ExponentialBackoffRetry,
    server_error_retry: ExponentialBackoffRetry,
}

impl Default for AdaptiveRetryPolicy {
    fn default() -> Self {
        // 网络错误：快速重试
        let network_config = RetryConfig {
            max_retries: 5,
            initial_delay_ms: 50,
            max_delay_ms: 5000,
            backoff_factor: 1.5,
            jitter: true,
            retryable_errors: vec![RetryableError::NetworkError, RetryableError::Timeout],
        };

        // 限流错误：慢速重试
        let rate_limit_config = RetryConfig {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_factor: 3.0,
            jitter: false,
            retryable_errors: vec![RetryableError::RateLimit],
        };

        // 服务器错误：中速重试
        let server_error_config = RetryConfig {
            max_retries: 4,
            initial_delay_ms: 500,
            max_delay_ms: 15000,
            backoff_factor: 2.0,
            jitter: true,
            retryable_errors: vec![
                RetryableError::ServerError,
                RetryableError::MaintenanceError,
            ],
        };

        Self {
            network_retry: ExponentialBackoffRetry::new(network_config),
            rate_limit_retry: ExponentialBackoffRetry::new(rate_limit_config),
            server_error_retry: ExponentialBackoffRetry::new(server_error_config),
        }
    }
}

#[async_trait]
impl RetryPolicy for AdaptiveRetryPolicy {
    fn should_retry(&self, error: &ExchangeError, attempt: u32) -> bool {
        match error {
            ExchangeError::NetworkError(_) => self.network_retry.should_retry(error, attempt),
            ExchangeError::RateLimitError(_, _) => {
                self.rate_limit_retry.should_retry(error, attempt)
            }
            ExchangeError::ApiError { .. } => self.server_error_retry.should_retry(error, attempt),
            _ => false,
        }
    }

    fn calculate_delay(&self, attempt: u32) -> Duration {
        // 使用网络重试的默认延迟计算
        self.network_retry.calculate_delay(attempt)
    }

    async fn execute_with_retry<F, T, Fut>(&self, operation: F) -> Result<T, ExchangeError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, ExchangeError>> + Send,
        T: Send,
    {
        let mut attempt = 0;

        loop {
            match operation().await {
                Ok(result) => {
                    if attempt > 0 {
                        log::info!("✅ 操作在第{}次尝试后成功", attempt + 1);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    // 根据错误类型选择重试策略
                    let (should_retry, delay) = match &error {
                        ExchangeError::NetworkError(_) => {
                            let should = self.network_retry.should_retry(&error, attempt);
                            let delay = self.network_retry.calculate_delay(attempt);
                            (should, delay)
                        }
                        ExchangeError::RateLimitError(_, _) => {
                            let should = self.rate_limit_retry.should_retry(&error, attempt);
                            let delay = self.rate_limit_retry.calculate_delay(attempt);
                            (should, delay)
                        }
                        ExchangeError::ApiError { .. } => {
                            let should = self.server_error_retry.should_retry(&error, attempt);
                            let delay = self.server_error_retry.calculate_delay(attempt);
                            (should, delay)
                        }
                        _ => (false, Duration::from_secs(0)),
                    };

                    if !should_retry {
                        log::error!("❌ 操作失败且不可重试: {:?}", error);
                        return Err(error);
                    }

                    log::warn!(
                        "⚠️ 操作失败，将在{:.2}秒后重试 (尝试 {}): {:?}",
                        delay.as_secs_f64(),
                        attempt + 1,
                        error
                    );

                    attempt += 1;

                    sleep(delay).await;
                }
            }
        }
    }
}

/// 重试助手函数
pub async fn retry_async<F, T, Fut>(operation: F, max_retries: u32) -> Result<T, ExchangeError>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T, ExchangeError>> + Send,
    T: Send,
{
    let policy = ExponentialBackoffRetry::new(RetryConfig {
        max_retries,
        ..Default::default()
    });

    policy.execute_with_retry(operation).await
}

/// 使用自适应策略重试
pub async fn adaptive_retry<F, T, Fut>(operation: F) -> Result<T, ExchangeError>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T, ExchangeError>> + Send,
    T: Send,
{
    let policy = AdaptiveRetryPolicy::default();
    policy.execute_with_retry(operation).await
}
