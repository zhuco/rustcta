use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 性能指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    pub request_count: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub total_latency_ms: u64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub p99_latency_ms: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub rate_limit_hits: u64,
    pub retry_count: u64,
    pub last_updated: DateTime<Utc>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            request_count: 0,
            success_count: 0,
            error_count: 0,
            total_latency_ms: 0,
            min_latency_ms: u64::MAX,
            max_latency_ms: 0,
            avg_latency_ms: 0.0,
            p50_latency_ms: 0,
            p95_latency_ms: 0,
            p99_latency_ms: 0,
            cache_hits: 0,
            cache_misses: 0,
            rate_limit_hits: 0,
            retry_count: 0,
            last_updated: Utc::now(),
        }
    }
}

/// 性能监控器
pub struct PerformanceMonitor {
    metrics: Arc<RwLock<HashMap<String, Metrics>>>,
    latency_samples: Arc<RwLock<HashMap<String, Vec<u64>>>>,
    start_time: Instant,
    enabled: Arc<RwLock<bool>>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            latency_samples: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            enabled: Arc::new(RwLock::new(true)),
        }
    }

    /// 启用/禁用监控
    pub fn set_enabled(&self, enabled: bool) {
        *self.enabled.write() = enabled;
    }

    /// 记录请求开始
    pub fn record_request_start(&self, endpoint: &str) -> RequestTimer {
        if !*self.enabled.read() {
            return RequestTimer::disabled();
        }

        RequestTimer::new(endpoint.to_string(), self.clone())
    }

    /// 记录请求完成
    pub fn record_request_complete(&self, endpoint: &str, latency_ms: u64, success: bool) {
        if !*self.enabled.read() {
            return;
        }

        let mut metrics = self.metrics.write();
        let metric = metrics
            .entry(endpoint.to_string())
            .or_insert_with(Metrics::default);

        metric.request_count += 1;
        if success {
            metric.success_count += 1;
        } else {
            metric.error_count += 1;
        }

        metric.total_latency_ms += latency_ms;
        metric.min_latency_ms = metric.min_latency_ms.min(latency_ms);
        metric.max_latency_ms = metric.max_latency_ms.max(latency_ms);
        metric.avg_latency_ms = metric.total_latency_ms as f64 / metric.request_count as f64;
        metric.last_updated = Utc::now();

        // 更新延迟样本
        let mut samples = self.latency_samples.write();
        let endpoint_samples = samples.entry(endpoint.to_string()).or_insert_with(Vec::new);
        endpoint_samples.push(latency_ms);

        // 保留最近1000个样本
        if endpoint_samples.len() > 1000 {
            endpoint_samples.remove(0);
        }

        // 计算百分位数
        if endpoint_samples.len() >= 10 {
            let mut sorted = endpoint_samples.clone();
            sorted.sort_unstable();

            let p50_idx = sorted.len() / 2;
            let p95_idx = (sorted.len() as f64 * 0.95) as usize;
            let p99_idx = (sorted.len() as f64 * 0.99) as usize;

            metric.p50_latency_ms = sorted[p50_idx];
            metric.p95_latency_ms = sorted[p95_idx.min(sorted.len() - 1)];
            metric.p99_latency_ms = sorted[p99_idx.min(sorted.len() - 1)];
        }
    }

    /// 记录缓存命中
    pub fn record_cache_hit(&self, endpoint: &str) {
        if !*self.enabled.read() {
            return;
        }

        let mut metrics = self.metrics.write();
        let metric = metrics
            .entry(endpoint.to_string())
            .or_insert_with(Metrics::default);
        metric.cache_hits += 1;
    }

    /// 记录缓存未命中
    pub fn record_cache_miss(&self, endpoint: &str) {
        if !*self.enabled.read() {
            return;
        }

        let mut metrics = self.metrics.write();
        let metric = metrics
            .entry(endpoint.to_string())
            .or_insert_with(Metrics::default);
        metric.cache_misses += 1;
    }

    /// 记录限流触发
    pub fn record_rate_limit(&self, endpoint: &str) {
        if !*self.enabled.read() {
            return;
        }

        let mut metrics = self.metrics.write();
        let metric = metrics
            .entry(endpoint.to_string())
            .or_insert_with(Metrics::default);
        metric.rate_limit_hits += 1;
    }

    /// 记录重试
    pub fn record_retry(&self, endpoint: &str) {
        if !*self.enabled.read() {
            return;
        }

        let mut metrics = self.metrics.write();
        let metric = metrics
            .entry(endpoint.to_string())
            .or_insert_with(Metrics::default);
        metric.retry_count += 1;
    }

    /// 获取指定端点的指标
    pub fn get_metrics(&self, endpoint: &str) -> Option<Metrics> {
        self.metrics.read().get(endpoint).cloned()
    }

    /// 获取所有指标
    pub fn get_all_metrics(&self) -> HashMap<String, Metrics> {
        self.metrics.read().clone()
    }

    /// 重置指标
    pub fn reset(&self) {
        self.metrics.write().clear();
        self.latency_samples.write().clear();
    }

    /// 获取运行时间
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// 生成报告
    pub fn generate_report(&self) -> PerformanceReport {
        let metrics = self.get_all_metrics();
        let mut total_requests = 0u64;
        let mut total_success = 0u64;
        let mut total_errors = 0u64;
        let mut total_latency = 0u64;
        let mut total_cache_hits = 0u64;
        let mut total_cache_misses = 0u64;

        for metric in metrics.values() {
            total_requests += metric.request_count;
            total_success += metric.success_count;
            total_errors += metric.error_count;
            total_latency += metric.total_latency_ms;
            total_cache_hits += metric.cache_hits;
            total_cache_misses += metric.cache_misses;
        }

        let avg_latency = if total_requests > 0 {
            total_latency as f64 / total_requests as f64
        } else {
            0.0
        };

        let cache_hit_rate = if total_cache_hits + total_cache_misses > 0 {
            total_cache_hits as f64 / (total_cache_hits + total_cache_misses) as f64
        } else {
            0.0
        };

        let success_rate = if total_requests > 0 {
            total_success as f64 / total_requests as f64
        } else {
            0.0
        };

        PerformanceReport {
            uptime_seconds: self.uptime().as_secs(),
            total_requests,
            total_success,
            total_errors,
            avg_latency_ms: avg_latency,
            cache_hit_rate,
            success_rate,
            endpoint_metrics: metrics,
            generated_at: Utc::now(),
        }
    }
}

impl Clone for PerformanceMonitor {
    fn clone(&self) -> Self {
        Self {
            metrics: self.metrics.clone(),
            latency_samples: self.latency_samples.clone(),
            start_time: self.start_time,
            enabled: self.enabled.clone(),
        }
    }
}

/// 请求计时器
pub struct RequestTimer {
    endpoint: Option<String>,
    start_time: Option<Instant>,
    monitor: Option<PerformanceMonitor>,
}

impl RequestTimer {
    fn new(endpoint: String, monitor: PerformanceMonitor) -> Self {
        Self {
            endpoint: Some(endpoint),
            start_time: Some(Instant::now()),
            monitor: Some(monitor),
        }
    }

    fn disabled() -> Self {
        Self {
            endpoint: None,
            start_time: None,
            monitor: None,
        }
    }

    /// 完成计时
    pub fn complete(self, success: bool) {
        if let (Some(endpoint), Some(start_time), Some(monitor)) =
            (self.endpoint, self.start_time, self.monitor)
        {
            let latency_ms = start_time.elapsed().as_millis() as u64;
            monitor.record_request_complete(&endpoint, latency_ms, success);
        }
    }

    /// 获取已用时间
    pub fn elapsed(&self) -> Option<Duration> {
        self.start_time.map(|t| t.elapsed())
    }
}

/// 性能报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub uptime_seconds: u64,
    pub total_requests: u64,
    pub total_success: u64,
    pub total_errors: u64,
    pub avg_latency_ms: f64,
    pub cache_hit_rate: f64,
    pub success_rate: f64,
    pub endpoint_metrics: HashMap<String, Metrics>,
    pub generated_at: DateTime<Utc>,
}

impl PerformanceReport {
    /// 打印报告
    pub fn print(&self) {
        println!("\n========== 性能报告 ==========");
        println!(
            "生成时间: {}",
            self.generated_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!("运行时间: {} 秒", self.uptime_seconds);
        println!("\n--- 总体统计 ---");
        println!("总请求数: {}", self.total_requests);
        println!(
            "成功请求: {} ({:.2}%)",
            self.total_success,
            self.success_rate * 100.0
        );
        println!("失败请求: {}", self.total_errors);
        println!("平均延迟: {:.2} ms", self.avg_latency_ms);
        println!("缓存命中率: {:.2}%", self.cache_hit_rate * 100.0);

        println!("\n--- 端点统计 (Top 10) ---");
        let mut endpoints: Vec<_> = self.endpoint_metrics.iter().collect();
        endpoints.sort_by(|a, b| b.1.request_count.cmp(&a.1.request_count));

        for (endpoint, metric) in endpoints.iter().take(10) {
            println!("\n端点: {}", endpoint);
            println!("  请求数: {}", metric.request_count);
            println!(
                "  成功率: {:.2}%",
                (metric.success_count as f64 / metric.request_count as f64) * 100.0
            );
            println!("  平均延迟: {:.2} ms", metric.avg_latency_ms);
            println!(
                "  P50: {} ms, P95: {} ms, P99: {} ms",
                metric.p50_latency_ms, metric.p95_latency_ms, metric.p99_latency_ms
            );

            if metric.cache_hits + metric.cache_misses > 0 {
                let hit_rate =
                    metric.cache_hits as f64 / (metric.cache_hits + metric.cache_misses) as f64;
                println!("  缓存命中率: {:.2}%", hit_rate * 100.0);
            }
        }
        println!("\n==============================\n");
    }

    /// 导出为JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// 日志级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

/// 结构化日志器
pub struct Logger {
    level: Arc<RwLock<LogLevel>>,
    outputs: Arc<RwLock<Vec<Box<dyn LogOutput + Send + Sync>>>>,
}

impl Logger {
    pub fn new(level: LogLevel) -> Self {
        Self {
            level: Arc::new(RwLock::new(level)),
            outputs: Arc::new(RwLock::new(vec![Box::new(ConsoleOutput)])),
        }
    }

    /// 设置日志级别
    pub fn set_level(&self, level: LogLevel) {
        *self.level.write() = level;
    }

    /// 添加输出目标
    pub fn add_output(&self, output: Box<dyn LogOutput + Send + Sync>) {
        self.outputs.write().push(output);
    }

    /// 记录日志
    pub fn log(&self, level: LogLevel, message: &str, fields: Option<HashMap<String, String>>) {
        if level < *self.level.read() {
            return;
        }

        let entry = LogEntry {
            timestamp: Utc::now(),
            level,
            message: message.to_string(),
            fields: fields.unwrap_or_default(),
        };

        for output in self.outputs.read().iter() {
            output.write(&entry);
        }
    }

    pub fn trace(&self, message: &str) {
        self.log(LogLevel::Trace, message, None);
    }

    pub fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message, None);
    }

    pub fn info(&self, message: &str) {
        self.log(LogLevel::Info, message, None);
    }

    pub fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message, None);
    }

    pub fn error(&self, message: &str) {
        self.log(LogLevel::Error, message, None);
    }

    pub fn with_fields(&self, level: LogLevel, message: &str, fields: HashMap<String, String>) {
        self.log(level, message, Some(fields));
    }
}

/// 日志条目
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
    pub fields: HashMap<String, String>,
}

/// 日志输出接口
pub trait LogOutput {
    fn write(&self, entry: &LogEntry);
}

/// 控制台输出
pub struct ConsoleOutput;

impl LogOutput for ConsoleOutput {
    fn write(&self, entry: &LogEntry) {
        let level_str = match entry.level {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO ",
            LogLevel::Warn => "WARN ",
            LogLevel::Error => "ERROR",
        };

        let mut output = format!(
            "[{}] {} - {}",
            entry.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
            level_str,
            entry.message
        );

        if !entry.fields.is_empty() {
            output.push_str(" | ");
            for (key, value) in &entry.fields {
                output.push_str(&format!("{}={} ", key, value));
            }
        }

        println!("{}", output);
    }
}
