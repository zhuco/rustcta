/// HTTP/2 连接管理优化
use reqwest::{Client, ClientBuilder};
use std::time::Duration;

/// 创建优化的 HTTP 客户端，避免 HTTP/2 流耗尽问题
pub fn create_optimized_client() -> Client {
    ClientBuilder::new()
        // 连接池配置
        .pool_idle_timeout(Duration::from_secs(90))  // 空闲连接90秒后关闭
        .pool_max_idle_per_host(10)                  // 每个主机最多10个空闲连接
        
        // HTTP/2 配置
        .http2_keep_alive_interval(Duration::from_secs(30))  // HTTP/2 心跳
        .http2_keep_alive_timeout(Duration::from_secs(10))   // 心跳超时
        .http2_keep_alive_while_idle(true)                   // 空闲时保持心跳
        
        // 超时配置
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        
        // 重试配置
        .tcp_nodelay(true)  // 禁用 Nagle 算法，减少延迟
        .tcp_keepalive(Some(Duration::from_secs(60)))  // TCP 层心跳
        
        .build()
        .expect("创建 HTTP 客户端失败")
}

/// 连接池管理器
pub struct ConnectionPoolManager {
    client: Client,
    last_reset: std::time::Instant,
    reset_interval: Duration,
}

impl ConnectionPoolManager {
    pub fn new() -> Self {
        Self {
            client: create_optimized_client(),
            last_reset: std::time::Instant::now(),
            reset_interval: Duration::from_secs(300), // 5分钟重置一次连接池
        }
    }
    
    /// 获取客户端，必要时重置
    pub fn get_client(&mut self) -> &Client {
        // 定期重建客户端以避免连接积累
        if self.last_reset.elapsed() > self.reset_interval {
            self.client = create_optimized_client();
            self.last_reset = std::time::Instant::now();
            log::info!("HTTP 客户端连接池已重置");
        }
        &self.client
    }
}

/// 请求速率控制器
pub struct RequestThrottler {
    last_request: tokio::sync::Mutex<std::time::Instant>,
    min_interval: Duration,
}

impl RequestThrottler {
    pub fn new(requests_per_second: u32) -> Self {
        let min_interval = Duration::from_millis(1000 / requests_per_second as u64);
        Self {
            last_request: tokio::sync::Mutex::new(std::time::Instant::now()),
            min_interval,
        }
    }
    
    /// 等待直到可以发送下一个请求
    pub async fn wait_if_needed(&self) {
        let mut last = self.last_request.lock().await;
        let elapsed = last.elapsed();
        
        if elapsed < self.min_interval {
            let wait_time = self.min_interval - elapsed;
            tokio::time::sleep(wait_time).await;
        }
        
        *last = std::time::Instant::now();
    }
}