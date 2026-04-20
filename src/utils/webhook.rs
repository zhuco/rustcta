//! Webhook通知模块
//! 用于发送错误和告警通知到企业微信等平台

use chrono::{DateTime, Duration, Utc};
use log::{debug, error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

/// Webhook配置
#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    /// 企业微信webhook地址
    pub wechat_work: String,
    /// 是否启用
    pub enabled: bool,
    /// 最小推送级别
    pub min_level: String,
    /// 推送间隔限制（秒）
    pub rate_limit_seconds: u64,
}

/// 消息级别
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessageLevel {
    Debug = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
    Critical = 4,
}

impl MessageLevel {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "debug" => Self::Debug,
            "info" => Self::Info,
            "warning" | "warn" => Self::Warning,
            "error" => Self::Error,
            "critical" => Self::Critical,
            _ => Self::Info,
        }
    }

    pub fn emoji(&self) -> &str {
        match self {
            Self::Debug => "🔍",
            Self::Info => "ℹ️",
            Self::Warning => "⚠️",
            Self::Error => "❌",
            Self::Critical => "🚨",
        }
    }
}

/// 企业微信消息格式
#[derive(Debug, Serialize)]
struct WeChatWorkMessage {
    msgtype: String,
    markdown: MarkdownContent,
}

#[derive(Debug, Serialize)]
struct MarkdownContent {
    content: String,
}

/// Webhook通知器
pub struct WebhookNotifier {
    config: WebhookConfig,
    client: Client,
    /// 上次发送时间记录（用于限流）
    last_send_times: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
}

impl WebhookNotifier {
    /// 创建新的通知器
    pub fn new(config: WebhookConfig) -> Self {
        Self {
            config,
            client: Client::new(),
            last_send_times: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 发送通用事件通知
    pub async fn send_event(
        &self,
        strategy: &str,
        title: &str,
        body: &str,
        level: MessageLevel,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.enabled {
            return Ok(());
        }

        let min_level = MessageLevel::from_str(&self.config.min_level);
        if level < min_level {
            return Ok(());
        }

        let key = format!("{}::{}", strategy, title);
        if !self.check_rate_limit(&key).await {
            debug!("跳过推送，限流中: {}", key);
            return Ok(());
        }

        let content = self.build_event_message(strategy, title, body, level);
        self.send_to_wechat_work(content).await
    }

    /// 从配置文件加载
    pub fn from_config_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: serde_yaml::Value = serde_yaml::from_str(&config_str)?;

        let webhook_config = config["webhook"].clone();
        let config: WebhookConfig = serde_yaml::from_value(webhook_config)?;

        Ok(Self::new(config))
    }

    /// 发送错误通知
    pub async fn send_error(
        &self,
        strategy: &str,
        error: &str,
        level: MessageLevel,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 检查是否启用
        if !self.config.enabled {
            return Ok(());
        }

        // 检查级别
        let min_level = MessageLevel::from_str(&self.config.min_level);
        if level < min_level {
            return Ok(());
        }

        // 检查限流
        if !self.check_rate_limit(strategy).await {
            debug!("跳过推送，限流中: {}", strategy);
            return Ok(());
        }

        // 构建消息
        let content = self.build_error_message(strategy, error, level);

        // 发送到企业微信
        self.send_to_wechat_work(content).await
    }

    /// 检查限流
    async fn check_rate_limit(&self, key: &str) -> bool {
        let mut times = self.last_send_times.write().await;
        let now = Utc::now();

        if let Some(last_time) = times.get(key) {
            let elapsed = now.signed_duration_since(*last_time);
            if elapsed < Duration::seconds(self.config.rate_limit_seconds as i64) {
                return false;
            }
        }

        times.insert(key.to_string(), now);
        true
    }

    /// 构建错误消息
    fn build_error_message(&self, strategy: &str, error: &str, level: MessageLevel) -> String {
        self.build_event_message(strategy, "策略告警", &format!("```\n{}\n```", error), level)
    }

    fn build_event_message(
        &self,
        strategy: &str,
        title: &str,
        body: &str,
        level: MessageLevel,
    ) -> String {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

        format!(
            r#"## {} {}

**策略**: {}
**级别**: {:?}
**时间**: {}

{}

---
_自动推送 by RustCTA_"#,
            level.emoji(),
            title,
            strategy,
            level,
            now,
            body
        )
    }

    /// 发送到企业微信
    async fn send_to_wechat_work(&self, content: String) -> Result<(), Box<dyn std::error::Error>> {
        let message = WeChatWorkMessage {
            msgtype: "markdown".to_string(),
            markdown: MarkdownContent { content },
        };

        let response = self
            .client
            .post(&self.config.wechat_work)
            .json(&message)
            .send()
            .await?;

        if response.status().is_success() {
            info!("成功发送Webhook通知");
        } else {
            warn!("Webhook发送失败: {}", response.status());
        }

        Ok(())
    }
}

/// 全局通知器实例
static GLOBAL_NOTIFIER: OnceLock<Arc<WebhookNotifier>> = OnceLock::new();

/// 初始化全局通知器
pub fn init_global_notifier(config_path: &str) {
    match WebhookNotifier::from_config_file(config_path) {
        Ok(notifier) => {
            if GLOBAL_NOTIFIER.set(Arc::new(notifier)).is_ok() {
                info!("全局Webhook通知器已初始化");
            } else {
                info!("全局Webhook通知器已存在，忽略重复初始化");
            }
        }
        Err(_) => {
            warn!("无法初始化Webhook通知器");
        }
    }
}

/// 获取全局通知器
pub fn get_global_notifier() -> Option<Arc<WebhookNotifier>> {
    GLOBAL_NOTIFIER.get().cloned()
}

/// 快捷函数：发送错误通知
pub async fn notify_error(strategy: &str, error: &str) {
    if let Some(notifier) = get_global_notifier() {
        let _ = notifier
            .send_error(strategy, error, MessageLevel::Error)
            .await;
    }
}

pub async fn notify_info(strategy: &str, message: &str) {
    notify_event(strategy, "策略提示", message, MessageLevel::Info).await;
}

pub async fn notify_event(strategy: &str, title: &str, body: &str, level: MessageLevel) {
    if let Some(notifier) = get_global_notifier() {
        let _ = notifier.send_event(strategy, title, body, level).await;
    }
}

/// 快捷函数：发送严重错误通知
pub async fn notify_critical(strategy: &str, error: &str) {
    if let Some(notifier) = get_global_notifier() {
        let _ = notifier
            .send_error(strategy, error, MessageLevel::Critical)
            .await;
    }
}
