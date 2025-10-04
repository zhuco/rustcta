use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::json;
use std::sync::Arc;

use crate::strategies::range_grid::domain::config::{NotificationConfig, WeComConfig};

#[derive(Clone)]
pub struct RangeGridNotifier {
    inner: Option<Arc<dyn NotificationChannel + Send + Sync>>,
}

impl RangeGridNotifier {
    pub fn new(config: &Option<NotificationConfig>) -> Self {
        let inner = config.as_ref().and_then(|cfg| cfg.wecom.clone()).map(|wc| {
            Arc::new(WeComNotifier::new(wc)) as Arc<dyn NotificationChannel + Send + Sync>
        });
        Self { inner }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub async fn send_text(&self, title: &str, body: &str) {
        if let Some(channel) = &self.inner {
            if let Err(err) = channel.send_text(title, body).await {
                log::warn!("企业微信通知发送失败: {}", err);
            }
        }
    }
}

#[async_trait::async_trait]
pub trait NotificationChannel {
    async fn send_text(&self, title: &str, body: &str) -> Result<()>;
}

pub struct WeComNotifier {
    client: Client,
    cfg: WeComConfig,
}

impl WeComNotifier {
    pub fn new(cfg: WeComConfig) -> Self {
        Self {
            client: Client::new(),
            cfg,
        }
    }
}

#[async_trait::async_trait]
impl NotificationChannel for WeComNotifier {
    async fn send_text(&self, title: &str, body: &str) -> Result<()> {
        let content = format!("【{}】\n{}", title, body);
        let payload = json!({
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_list": self.cfg.mentioned_list,
                "mentioned_mobile_list": self.cfg.mentioned_mobile_list,
            }
        });

        let resp = self
            .client
            .post(&self.cfg.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| anyhow!("WeCom webhook 请求失败: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("WeCom webhook 返回异常: {status} - {text}"));
        }

        Ok(())
    }
}
