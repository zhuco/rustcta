use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::{FundingRateArbitrageConfig, FundingScanReport};

#[derive(Debug, Serialize)]
struct WecomMessage {
    msgtype: String,
    markdown: WecomMarkdown,
}

#[derive(Debug, Serialize)]
struct WecomMarkdown {
    content: String,
}

#[derive(Debug, Deserialize)]
struct WecomResponse {
    errcode: i64,
    errmsg: String,
}

pub fn build_startup_markdown(report: &FundingScanReport) -> String {
    let generated_at = report.generated_at.format("%Y-%m-%d %H:%M:%S UTC");
    let mut content = format!(
        "### 资金费率套利启动观察\n\
         > 时间：{}\n\
         > 模式：仅观察，不下单\n\
         > 执行阈值：`{:.4}%`\n\n",
        generated_at, report.threshold_pct
    );

    if report.selections.is_empty() {
        content.push_str("当前没有完成扫描的交易所。\n");
    } else {
        content.push_str("### 每交易所候选\n\n");
        for selection in &report.selections {
            match &selection.selected {
                Some(candidate) => {
                    let next = candidate
                        .next_funding_time
                        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "未知".to_string());
                    let settle = candidate
                        .seconds_to_settlement
                        .map(|seconds| format!("{}s", seconds))
                        .unwrap_or_else(|| "未知".to_string());
                    let mark = candidate
                        .mark_price
                        .map(|price| format!("{:.8}", price))
                        .unwrap_or_else(|| "-".to_string());
                    content.push_str(&format!(
                        "- `{}` `{}` 费率 <font color=\"warning\">{:+.4}%</font>，结算 `{}`，剩余 `{}`，标记价 `{}`，快照 `{}`ms\n",
                        selection.exchange,
                        candidate.canonical_symbol,
                        candidate.funding_rate_pct,
                        next,
                        settle,
                        mark,
                        candidate.snapshot_age_ms,
                    ));
                }
                None => {
                    content.push_str(&format!(
                        "- `{}` 未命中：{}，已扫 symbols={} funding_snapshots={}\n",
                        selection.exchange,
                        selection
                            .skipped_reason
                            .as_deref()
                            .unwrap_or("unknown reason"),
                        selection.scanned_symbols,
                        selection.funding_snapshots,
                    ));
                }
            }
        }
    }

    if !report.errors.is_empty() {
        content.push_str("\n### 扫描错误\n\n");
        for error in &report.errors {
            content.push_str(&format!(
                "- `{}` `{}`：{}\n",
                error.exchange, error.stage, error.message
            ));
        }
    }

    content.push_str("\n> 当前版本不会提交任何订单。\n");
    content
}

pub async fn send_startup_notification(
    config: &FundingRateArbitrageConfig,
    report: &FundingScanReport,
) -> Result<()> {
    send_markdown_notification(config, build_startup_markdown(report)).await
}

pub async fn send_markdown_notification(
    config: &FundingRateArbitrageConfig,
    content: String,
) -> Result<()> {
    if !config.notifications.enabled {
        return Ok(());
    }

    let webhook_url = resolve_webhook_url(config)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .context("build wecom webhook client")?;
    send_markdown(&client, &webhook_url, content).await
}

pub fn resolve_webhook_url(config: &FundingRateArbitrageConfig) -> Result<String> {
    if let Some(url) = config
        .notifications
        .wecom_webhook_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
    {
        return Ok(url.to_string());
    }

    let env_name = config.notifications.wecom_webhook_env.trim();
    std::env::var(env_name)
        .with_context(|| format!("missing WeCom webhook env var {env_name}"))
        .map(|url| url.trim().to_string())
        .and_then(|url| {
            if url.is_empty() {
                Err(anyhow!("WeCom webhook env var {env_name} is empty"))
            } else {
                Ok(url)
            }
        })
}

async fn send_markdown(client: &Client, webhook_url: &str, content: String) -> Result<()> {
    let payload = WecomMessage {
        msgtype: "markdown".to_string(),
        markdown: WecomMarkdown { content },
    };

    let response = client
        .post(webhook_url)
        .json(&payload)
        .send()
        .await
        .context("send WeCom webhook request")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read WeCom webhook response body")?;

    if !status.is_success() {
        return Err(anyhow!("WeCom webhook http status={status} body={body}"));
    }

    let parsed: WecomResponse =
        serde_json::from_str(&body).context("parse WeCom webhook response json")?;
    if parsed.errcode != 0 {
        return Err(anyhow!(
            "WeCom webhook returned errcode={} errmsg={}",
            parsed.errcode,
            parsed.errmsg
        ));
    }

    log::info!(
        "funding arbitrage startup notification sent at {}",
        Utc::now()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::market::{CanonicalSymbol, ExchangeId};

    use super::super::scanner::{ExchangeFundingSelection, FundingCandidate, FundingScanReport};
    use super::*;

    #[test]
    fn startup_markdown_should_include_no_order_notice() {
        let report = FundingScanReport {
            generated_at: Utc::now(),
            threshold: -0.005,
            threshold_pct: -0.5,
            selections: vec![ExchangeFundingSelection {
                exchange: ExchangeId::Binance,
                selected: Some(FundingCandidate {
                    exchange: ExchangeId::Binance,
                    canonical_symbol: CanonicalSymbol::new("btc", "usdt"),
                    exchange_symbol: Some("BTCUSDT".to_string()),
                    funding_rate: -0.006,
                    funding_rate_pct: -0.6,
                    predicted_funding_rate: None,
                    mark_price: Some(100_000.0),
                    index_price: None,
                    next_funding_time: None,
                    seconds_to_settlement: None,
                    snapshot_age_ms: 10,
                    qualifies: true,
                }),
                scanned_symbols: 1,
                funding_snapshots: 1,
                eligible_candidates: 1,
                skipped_reason: None,
            }],
            errors: Vec::new(),
        };

        let markdown = build_startup_markdown(&report);

        assert!(markdown.contains("仅观察，不下单"));
        assert!(markdown.contains("BTC/USDT"));
        assert!(markdown.contains("-0.6000%"));
    }
}
