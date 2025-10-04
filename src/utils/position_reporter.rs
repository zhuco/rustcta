use crate::cta::account_manager::AccountManager;
use chrono::{Timelike, Utc};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 仓位报告服务
pub struct PositionReporter {
    account_manager: Arc<AccountManager>,
    webhook_url: String,
    last_report_hour: Arc<RwLock<Option<u32>>>,
}

impl PositionReporter {
    pub fn new(account_manager: Arc<AccountManager>, webhook_url: String) -> Self {
        Self {
            account_manager,
            webhook_url,
            last_report_hour: Arc::new(RwLock::new(None)),
        }
    }

    /// 启动定期仓位报告任务 - 每小时发送一次
    pub async fn start(self: Arc<Self>) {
        log::info!("📊 启动仓位报告服务（每小时发送一次）...");

        let reporter = self.clone();
        tokio::spawn(async move {
            // 每30秒检查一次时间
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                let now = Utc::now();
                let current_hour = now.hour();
                let current_minute = now.minute();

                // 每小时的第0分钟发送报告
                if current_minute == 0 {
                    let mut last_hour = reporter.last_report_hour.write().await;

                    // 确保每小时只发送一次
                    if Some(current_hour) != *last_hour {
                        *last_hour = Some(current_hour);
                        drop(last_hour);

                        log::info!("⏰ 触发整点仓位报告 ({}:00 UTC)", current_hour);
                        if let Err(e) = reporter.send_position_report().await {
                            log::error!("发送仓位报告失败: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// 发送仓位报告到企业微信
    pub async fn send_position_report(&self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("📊 收集仓位数据...");

        let mut all_positions = Vec::new();

        // 获取所有账户的仓位
        let accounts = self.account_manager.get_all_accounts();

        for account in accounts {
            // 跳过Bitmart账户
            if account.id.contains("bitmart") {
                continue;
            }

            match account.exchange.get_positions(None).await {
                Ok(positions) => {
                    for pos in positions {
                        // 计算仓位价值（USDT）
                        let position_value = pos.amount.abs() * pos.mark_price;

                        if position_value.abs() > 10.0 {
                            // 只报告价值超过10 USDT的仓位
                            all_positions.push((
                                account.id.clone(),
                                pos.symbol.clone(),
                                pos.amount,
                                pos.entry_price,
                                pos.mark_price,
                                pos.unrealized_pnl,
                                position_value,
                            ));
                        }
                    }
                }
                Err(e) => {
                    log::warn!("获取账户 {} 仓位失败: {}", account.id, e);
                }
            }
        }

        // 按仓位价值排序（降序）
        all_positions.sort_by(|a, b| b.6.abs().partial_cmp(&a.6.abs()).unwrap());

        // 构建简洁的报告内容
        let mut content = format!(
            "📊 **整点仓位TOP10**\n时间: {}\n\n",
            Utc::now().format("%H:%M UTC")
        );

        if all_positions.is_empty() {
            content.push_str("当前无持仓");
        } else {
            // 只显示前10个最大仓位
            for (i, (_, symbol, amount, entry_price, _, pnl, value)) in
                all_positions.iter().take(10).enumerate()
            {
                let side = if *amount > 0.0 { "多" } else { "空" };
                let pnl_str = if *pnl >= 0.0 {
                    format!("+{:.1}", pnl)
                } else {
                    format!("{:.1}", pnl)
                };

                content.push_str(&format!(
                    "{}. {} {} {:.3}@{:.2} PNL:{}\n",
                    i + 1,
                    symbol,
                    side,
                    amount.abs(),
                    entry_price,
                    pnl_str
                ));
            }
        }

        // 发送到企业微信
        self.send_to_wechat(&content).await?;

        log::info!("✅ 仓位报告已发送");
        Ok(())
    }

    /// 发送消息到企业微信
    async fn send_to_wechat(&self, content: &str) -> Result<(), Box<dyn std::error::Error>> {
        let message = json!({
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        });

        let client = reqwest::Client::new();
        let response = client.post(&self.webhook_url).json(&message).send().await?;

        if !response.status().is_success() {
            return Err(format!("Webhook发送失败: {}", response.status()).into());
        }

        Ok(())
    }
}

/// 启动全局仓位报告服务
pub async fn start_position_reporter(
    account_manager: Arc<AccountManager>,
    webhook_url: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let reporter = Arc::new(PositionReporter::new(account_manager, webhook_url));
    reporter.start().await;
    Ok(())
}
