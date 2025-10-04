use crate::core::error::ExchangeError;
use crate::utils::webhook;
use lru::LruCache;
use reqwest::{Client, ClientBuilder};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::sleep;

/// 缓存条目
#[derive(Clone, Debug)]
struct CacheEntry {
    data: Vec<u8>,
    timestamp: Instant,
    ttl: Duration,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > self.ttl
    }
}

/// 限流器（带自适应降速）
pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    base_requests_per_second: u32,
    current_delay_ms: Arc<AtomicU64>,
    request_times: Arc<RwLock<Vec<Instant>>>,
    last_error_time: Arc<RwLock<Option<Instant>>>,
    error_count: Arc<AtomicU64>,
}

impl RateLimiter {
    pub fn new(requests_per_second: u32) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(requests_per_second as usize)),
            base_requests_per_second: requests_per_second,
            current_delay_ms: Arc::new(AtomicU64::new(0)),
            request_times: Arc::new(RwLock::new(Vec::new())),
            last_error_time: Arc::new(RwLock::new(None)),
            error_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 报告请求错误（触发自适应降速）
    pub async fn report_error(&self) {
        let error_count = self.error_count.fetch_add(1, Ordering::Relaxed);
        let mut last_error = self.last_error_time.write().await;
        *last_error = Some(Instant::now());

        // 指数级增加延迟
        let current_delay = self.current_delay_ms.load(Ordering::Relaxed);
        let new_delay = if current_delay == 0 {
            100 // 初始延迟100ms
        } else {
            (current_delay * 2).min(5000) // 最大延迟5秒
        };
        self.current_delay_ms.store(new_delay, Ordering::Relaxed);

        log::warn!(
            "请求错误，增加延迟到 {}ms (错误次数: {})",
            new_delay,
            error_count + 1
        );
    }

    /// 检查是否可以恢复正常速度
    pub async fn try_recover(&self) {
        let last_error = self.last_error_time.read().await;
        if let Some(time) = *last_error {
            // 30秒无错误则尝试恢复
            if time.elapsed() > Duration::from_secs(30) {
                let current_delay = self.current_delay_ms.load(Ordering::Relaxed);
                if current_delay > 0 {
                    let new_delay = (current_delay / 2).max(0);
                    self.current_delay_ms.store(new_delay, Ordering::Relaxed);
                    if new_delay == 0 {
                        self.error_count.store(0, Ordering::Relaxed);
                        log::info!("请求限流已恢复正常");
                    } else {
                        log::info!("减少请求延迟到 {}ms", new_delay);
                    }
                }
            }
        }
    }

    /// 等待直到可以发送请求
    pub async fn acquire(&self) -> Result<(), ExchangeError> {
        // 先检查是否需要额外延迟
        let delay_ms = self.current_delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }

        // 定期尝试恢复
        self.try_recover().await;

        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| ExchangeError::Other(format!("限流器错误: {}", e)))?;

        // 清理旧的请求记录
        let mut times = self.request_times.write().await;
        let now = Instant::now();
        times.retain(|t| now.duration_since(*t) < Duration::from_secs(1));

        // 如果请求数超过限制，等待
        while times.len() >= self.base_requests_per_second as usize {
            sleep(Duration::from_millis(100)).await;
            times.retain(|t| now.duration_since(*t) < Duration::from_secs(1));
        }

        times.push(now);
        Ok(())
    }
}

/// 高性能请求管理器
pub struct RequestManager {
    client: Client,
    cache: Arc<RwLock<LruCache<String, CacheEntry>>>,
    rate_limiters: Arc<RwLock<HashMap<String, Arc<RateLimiter>>>>,
    default_retry_count: u32,
    default_timeout: Duration,
}

impl RequestManager {
    /// 创建新的请求管理器
    pub fn new() -> Result<Self, ExchangeError> {
        let client = ClientBuilder::new()
            .pool_max_idle_per_host(50) // 增加连接池大小
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true) // 禁用Nagle算法，减少延迟
            .gzip(true) // 启用gzip压缩
            .brotli(true) // 启用brotli压缩
            .build()
            .map_err(|e| ExchangeError::Other(format!("创建HTTP客户端失败: {}", e)))?;

        Ok(Self {
            client,
            cache: Arc::new(RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(1000).unwrap(),
            ))),
            rate_limiters: Arc::new(RwLock::new(HashMap::new())),
            default_retry_count: 3,
            default_timeout: Duration::from_secs(30),
        })
    }

    /// 设置交易所的限流规则
    pub async fn set_rate_limit(&self, exchange: String, requests_per_second: u32) {
        let mut limiters = self.rate_limiters.write().await;
        limiters.insert(exchange, Arc::new(RateLimiter::new(requests_per_second)));
    }

    /// 获取缓存的数据
    async fn get_cached_raw(&self, key: &str) -> Option<Vec<u8>> {
        let mut cache = self.cache.write().await;
        if let Some(entry) = cache.get(key) {
            if !entry.is_expired() {
                return Some(entry.data.clone());
            } else {
                cache.pop(key);
            }
        }
        None
    }

    /// 设置缓存
    async fn set_cache(&self, key: String, data: Vec<u8>, ttl: Duration) {
        let mut cache = self.cache.write().await;
        cache.put(
            key,
            CacheEntry {
                data,
                timestamp: Instant::now(),
                ttl,
            },
        );
    }

    /// 发送GET请求（带缓存）
    pub async fn get_cached<T>(
        &self,
        url: &str,
        exchange: &str,
        cache_ttl: Option<Duration>,
    ) -> Result<T, ExchangeError>
    where
        T: DeserializeOwned,
    {
        // 检查缓存
        let cache_key = format!("{}:{}", exchange, url);
        if let Some(cached_data) = self.get_cached_raw(&cache_key).await {
            if let Ok(result) = serde_json::from_slice(&cached_data) {
                return Ok(result);
            }
        }

        // 获取限流器
        if let Some(limiter) = self.rate_limiters.read().await.get(exchange) {
            limiter.acquire().await?;
        }

        // 发送请求
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| ExchangeError::NetworkError(e))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| ExchangeError::NetworkError(e))?;

            // 设置缓存
            if let Some(ttl) = cache_ttl {
                self.set_cache(cache_key, bytes.to_vec(), ttl).await;
            }

            // 解析响应
            serde_json::from_slice(&bytes)
                .map_err(|e| ExchangeError::ParseError(format!("JSON解析错误: {}", e)))
        } else {
            let status = response.status().as_u16() as i32;
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "未知错误".to_string());
            Err(ExchangeError::ApiError {
                code: status,
                message: error_text,
            })
        }
    }

    /// 发送请求（带重试）
    pub async fn request_with_retry<T>(
        &self,
        method: &str,
        url: &str,
        body: Option<String>,
        headers: Option<HashMap<String, String>>,
        exchange: &str,
        retry_count: Option<u32>,
    ) -> Result<T, ExchangeError>
    where
        T: DeserializeOwned,
    {
        let max_retries = retry_count.unwrap_or(self.default_retry_count);
        let mut last_error = None;

        for attempt in 0..=max_retries {
            // 获取限流器
            if let Some(limiter) = self.rate_limiters.read().await.get(exchange) {
                limiter.acquire().await?;
            }

            // 指数退避
            if attempt > 0 {
                let backoff = Duration::from_millis(100 * 2_u64.pow(attempt - 1));
                sleep(backoff).await;
            }

            // 构建请求
            let mut request = match method.to_uppercase().as_str() {
                "GET" => self.client.get(url),
                "POST" => self.client.post(url),
                "PUT" => self.client.put(url),
                "DELETE" => self.client.delete(url),
                _ => {
                    return Err(ExchangeError::Other(format!(
                        "不支持的HTTP方法: {}",
                        method
                    )))
                }
            };

            // 添加headers
            if let Some(headers) = &headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }

            // 添加body
            if let Some(body) = &body {
                request = request.body(body.clone());
            }

            // 发送请求
            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        let data = response.json::<T>().await.map_err(|e| {
                            ExchangeError::ParseError(format!("JSON解析错误: {}", e))
                        })?;
                        return Ok(data);
                    } else {
                        let status = response.status().as_u16() as i32;

                        // 检查是否是可重试的错误
                        if status >= 500 || status == 429 {
                            last_error = Some(ExchangeError::ApiError {
                                code: status,
                                message: response
                                    .text()
                                    .await
                                    .unwrap_or_else(|_| "服务器错误".to_string()),
                            });
                            continue;
                        } else {
                            // 客户端错误，不重试
                            return Err(ExchangeError::ApiError {
                                code: status,
                                message: response
                                    .text()
                                    .await
                                    .unwrap_or_else(|_| "请求错误".to_string()),
                            });
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(ExchangeError::NetworkError(e));
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| ExchangeError::Other("请求失败".to_string())))
    }

    /// 批量请求（并发优化）
    pub async fn batch_request<T>(
        self: Arc<Self>,
        requests: Vec<(String, String)>, // (url, exchange)
        cache_ttl: Option<Duration>,
    ) -> Vec<Result<T, ExchangeError>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        use futures_util::future::join_all;

        let futures = requests.into_iter().map(|(url, exchange)| {
            let self_clone = self.clone();
            async move { self_clone.get_cached(&url, &exchange, cache_ttl).await }
        });

        join_all(futures).await
    }

    /// 清理过期缓存
    pub async fn clear_expired_cache(&self) {
        let mut cache = self.cache.write().await;
        let keys_to_remove: Vec<String> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_remove {
            cache.pop(&key);
        }
    }

    /// 获取缓存统计信息
    pub async fn get_cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        let total = cache.len();
        let expired = cache.iter().filter(|(_, entry)| entry.is_expired()).count();
        (total, expired)
    }
}

/// 连接池管理器
pub struct ConnectionPoolManager {
    pools: HashMap<String, Arc<Client>>,
    default_config: ConnectionConfig,
}

#[derive(Clone)]
pub struct ConnectionConfig {
    pub max_idle_per_host: usize,
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub tcp_keepalive: Option<Duration>,
    pub tcp_nodelay: bool,
    pub enable_compression: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 50,
            idle_timeout: Duration::from_secs(90),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            tcp_keepalive: Some(Duration::from_secs(60)),
            tcp_nodelay: true,
            enable_compression: true,
        }
    }
}

impl ConnectionPoolManager {
    pub fn new() -> Self {
        Self {
            pools: HashMap::new(),
            default_config: ConnectionConfig::default(),
        }
    }

    /// 获取或创建特定交易所的连接池
    pub fn get_or_create_pool(&mut self, exchange: &str) -> Arc<Client> {
        if let Some(pool) = self.pools.get(exchange) {
            return pool.clone();
        }

        let config = self.default_config.clone();
        let mut builder = ClientBuilder::new()
            .pool_max_idle_per_host(config.max_idle_per_host)
            .pool_idle_timeout(config.idle_timeout)
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .tcp_nodelay(config.tcp_nodelay);

        if let Some(keepalive) = config.tcp_keepalive {
            builder = builder.tcp_keepalive(keepalive);
        }

        if config.enable_compression {
            builder = builder.gzip(true).brotli(true);
        }

        let client = Arc::new(builder.build().expect("Failed to build HTTP client"));
        self.pools.insert(exchange.to_string(), client.clone());
        client
    }

    /// 自定义特定交易所的连接配置
    pub fn set_pool_config(&mut self, exchange: &str, config: ConnectionConfig) {
        let mut builder = ClientBuilder::new()
            .pool_max_idle_per_host(config.max_idle_per_host)
            .pool_idle_timeout(config.idle_timeout)
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .tcp_nodelay(config.tcp_nodelay);

        if let Some(keepalive) = config.tcp_keepalive {
            builder = builder.tcp_keepalive(keepalive);
        }

        if config.enable_compression {
            builder = builder.gzip(true).brotli(true);
        }

        let client = Arc::new(builder.build().expect("Failed to build HTTP client"));
        self.pools.insert(exchange.to_string(), client);
    }
}
