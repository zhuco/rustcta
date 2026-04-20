use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{sleep, Duration};

use rustcta::core::types::Kline;
use rustcta::utils::indicators::{calculate_adx, calculate_ema, calculate_macd, calculate_rsi};
use rustcta::utils::webhook::{MessageLevel, WebhookConfig, WebhookNotifier};

const CONFIG_PATH: &str = "config/trend_report.yml";
const STRATEGY_NAME: &str = "ETH_AI_TREND";

#[derive(Debug, Clone, Deserialize)]
struct TrendReporterConfig {
    symbol: String,
    #[serde(default)]
    display_symbol: Option<String>,
    #[serde(default = "default_base_url")]
    base_url: String,
    #[serde(default = "default_poll_interval")]
    poll_interval_secs: u64,
    intervals: Vec<TimeframeConfig>,
    webhook: WebhookConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct TimeframeConfig {
    label: String,
    interval: String,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_base_url() -> String {
    "https://api.binance.com".to_string()
}

fn default_poll_interval() -> u64 {
    60
}

fn default_limit() -> u32 {
    200
}

impl TrendReporterConfig {
    fn from_file(path: &str) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("无法读取配置文件: {}", path))?;
        let mut config: TrendReporterConfig =
            serde_yaml::from_str(&raw).with_context(|| format!("解析配置文件失败: {}", path))?;

        if config.intervals.is_empty() {
            return Err(anyhow!("配置文件中未指定任何时间框架"));
        }

        config.base_url = config.base_url.trim_end_matches('/').to_string();
        Ok(config)
    }

    fn display_symbol(&self) -> String {
        self.display_symbol
            .clone()
            .unwrap_or_else(|| format_symbol(&self.symbol))
    }
}

fn format_symbol(symbol: &str) -> String {
    if symbol.len() > 4 {
        let (base, quote) = symbol.split_at(symbol.len() - 4);
        format!("{}/{}", base, quote)
    } else {
        symbol.to_string()
    }
}

#[derive(Debug, Clone, Copy)]
enum TrendBias {
    StrongBullish,
    Bullish,
    Neutral,
    Bearish,
    StrongBearish,
}

impl TrendBias {
    fn emoji(&self) -> &'static str {
        match self {
            TrendBias::StrongBullish => "🚀",
            TrendBias::Bullish => "📈",
            TrendBias::Neutral => "⚖️",
            TrendBias::Bearish => "📉",
            TrendBias::StrongBearish => "🧊",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            TrendBias::StrongBullish => "强势多头",
            TrendBias::Bullish => "多头",
            TrendBias::Neutral => "震荡",
            TrendBias::Bearish => "空头",
            TrendBias::StrongBearish => "强势空头",
        }
    }
}

#[derive(Debug, Clone)]
struct TimeframeSummary {
    label: String,
    interval: String,
    last_close: f64,
    change_pct: f64,
    rsi: f64,
    adx: f64,
    macd_hist: f64,
    ema_fast: f64,
    ema_slow: f64,
    slope_pct: f64,
    score: f64,
    bias: TrendBias,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = TrendReporterConfig::from_file(CONFIG_PATH)?;
    let notifier = Arc::new(WebhookNotifier::new(config.webhook.clone()));
    let client = Client::builder()
        .timeout(StdDuration::from_secs(10))
        .build()
        .context("初始化HTTP客户端失败")?;

    log::info!(
        "启动ETH趋势报告，标的:{}，时间框架:{}",
        config.display_symbol(),
        config
            .intervals
            .iter()
            .map(|tf| tf.interval.clone())
            .collect::<Vec<_>>()
            .join(", ")
    );

    loop {
        if let Err(err) = analyze_once(&config, &client, notifier.clone()).await {
            log::error!("趋势分析失败: {err:?}");
        }
        sleep(Duration::from_secs(config.poll_interval_secs)).await;
    }
}

async fn analyze_once(
    config: &TrendReporterConfig,
    client: &Client,
    notifier: Arc<WebhookNotifier>,
) -> Result<()> {
    let mut summaries = Vec::new();
    let mut hourly_klines: Option<Vec<Kline>> = None;
    let mut reference_price = None;

    for tf in &config.intervals {
        let klines = fetch_klines(client, &config.base_url, &config.symbol, tf).await?;
        if tf.interval == "1h" {
            hourly_klines = Some(klines.clone());
        }

        if let Some(summary) = analyze_timeframe(tf, &klines) {
            if reference_price.is_none() {
                reference_price = Some(summary.last_close);
            }
            summaries.push(summary);
        } else {
            log::warn!("{}({}) 数据不足，跳过该时间框架", tf.label, tf.interval);
        }
    }

    if summaries.is_empty() {
        return Err(anyhow!("所有时间框架数据不足"));
    }

    let latest_price = reference_price.unwrap_or_else(|| summaries[0].last_close);
    let daily_change = hourly_klines
        .as_ref()
        .and_then(|klines| change_over_hours(klines, 24));

    let report = build_report(
        &summaries,
        &config.display_symbol(),
        latest_price,
        daily_change,
    );

    notifier
        .send_event(
            STRATEGY_NAME,
            "ETH/USDC 多周期趋势",
            &report,
            MessageLevel::Info,
        )
        .await
        .map_err(|err| anyhow!("发送企业微信通知失败: {err}"))?;

    Ok(())
}

fn change_over_hours(klines: &[Kline], hours: usize) -> Option<f64> {
    if klines.len() <= hours {
        return None;
    }
    let last = klines.last()?.close;
    let prev = klines[klines.len() - hours - 1].close;
    Some(((last - prev) / prev) * 100.0)
}

async fn fetch_klines(
    client: &Client,
    base_url: &str,
    symbol: &str,
    tf: &TimeframeConfig,
) -> Result<Vec<Kline>> {
    let url = format!("{base_url}/api/v3/klines");
    let response = client
        .get(url)
        .query(&[
            ("symbol", symbol),
            ("interval", tf.interval.as_str()),
            ("limit", &tf.limit.to_string()),
        ])
        .send()
        .await
        .with_context(|| format!("请求{}-{}K线失败", symbol, tf.interval))?
        .error_for_status()
        .with_context(|| format!("{}-{}K线接口返回错误", symbol, tf.interval))?;

    let raw: Vec<Vec<serde_json::Value>> = response
        .json()
        .await
        .with_context(|| format!("解析{}-{}K线JSON失败", symbol, tf.interval))?;

    let mut klines = Vec::with_capacity(raw.len());
    for entry in raw {
        if entry.len() < 9 {
            continue;
        }
        let open_time = entry[0]
            .as_i64()
            .and_then(DateTime::<Utc>::from_timestamp_millis);
        let close_time = entry[6]
            .as_i64()
            .and_then(DateTime::<Utc>::from_timestamp_millis);
        let parse_f64 = |value: &serde_json::Value| -> Option<f64> {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
        };

        let open_time = match open_time {
            Some(t) => t,
            None => continue,
        };
        let close_time = match close_time {
            Some(t) => t,
            None => continue,
        };

        let open = parse_f64(&entry[1]).unwrap_or(0.0);
        let high = parse_f64(&entry[2]).unwrap_or(0.0);
        let low = parse_f64(&entry[3]).unwrap_or(0.0);
        let close = parse_f64(&entry[4]).unwrap_or(0.0);
        let volume = parse_f64(&entry[5]).unwrap_or(0.0);
        let quote_volume = parse_f64(&entry[7]).unwrap_or(0.0);
        let trade_count = entry[8].as_u64().unwrap_or(0);

        klines.push(Kline {
            symbol: symbol.to_string(),
            interval: tf.interval.clone(),
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
        });
    }

    if klines.is_empty() {
        return Err(anyhow!("{}-{} K线为空", symbol, tf.interval));
    }

    Ok(klines)
}

fn analyze_timeframe(tf: &TimeframeConfig, klines: &[Kline]) -> Option<TimeframeSummary> {
    if klines.len() < 60 {
        return None;
    }

    let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();
    let highs: Vec<f64> = klines.iter().map(|k| k.high).collect();
    let lows: Vec<f64> = klines.iter().map(|k| k.low).collect();

    let ema_fast = calculate_ema(&closes, 12)?;
    let ema_slow = calculate_ema(&closes, 26)?;
    let (macd_line, macd_signal, macd_hist) = calculate_macd(&closes).unwrap_or((0.0, 0.0, 0.0));
    let rsi = calculate_rsi(&closes, 14).unwrap_or(50.0);
    let adx = calculate_adx(&highs, &lows, &closes, 14).unwrap_or(0.0);

    let last_close = *closes.last()?;
    let prev_close = closes
        .get(closes.len().saturating_sub(2))
        .copied()
        .unwrap_or(last_close);
    let change_pct = if prev_close > 0.0 {
        (last_close - prev_close) / prev_close * 100.0
    } else {
        0.0
    };

    let slope_ref = closes
        .get(closes.len().saturating_sub(6))
        .copied()
        .unwrap_or(last_close);
    let slope_pct = if slope_ref > 0.0 {
        (last_close - slope_ref) / slope_ref * 100.0
    } else {
        0.0
    };

    let ema_spread = if ema_slow.abs() > f64::EPSILON {
        (ema_fast - ema_slow) / ema_slow * 100.0
    } else {
        0.0
    };

    let mut score = 0.0;
    score += ema_spread * 2.0;
    score += slope_pct * 1.5;
    score += (rsi - 50.0) * 0.6;
    score += macd_hist * 80.0;
    if macd_line > macd_signal {
        score += 8.0;
    } else {
        score -= 8.0;
    }
    if adx > 25.0 {
        score *= 1.1;
    }

    let bias = classify_bias(score);

    Some(TimeframeSummary {
        label: tf.label.clone(),
        interval: tf.interval.clone(),
        last_close,
        change_pct,
        rsi,
        adx,
        macd_hist,
        ema_fast,
        ema_slow,
        slope_pct,
        score,
        bias,
    })
}

fn classify_bias(score: f64) -> TrendBias {
    if score >= 45.0 {
        TrendBias::StrongBullish
    } else if score >= 15.0 {
        TrendBias::Bullish
    } else if score <= -45.0 {
        TrendBias::StrongBearish
    } else if score <= -15.0 {
        TrendBias::Bearish
    } else {
        TrendBias::Neutral
    }
}

fn build_report(
    summaries: &[TimeframeSummary],
    display_symbol: &str,
    latest_price: f64,
    daily_change: Option<f64>,
) -> String {
    let mut body = String::new();
    let overall_score: f64 =
        summaries.iter().map(|s| s.score).sum::<f64>() / summaries.len() as f64;
    let headline_bias = classify_bias(overall_score);

    let _ = writeln!(
        body,
        "**{}** | {} | 最新价: {:.2} USDC",
        headline_bias.label(),
        display_symbol,
        latest_price
    );

    if let Some(change) = daily_change {
        let _ = writeln!(body, "24h 变化: {:+.2}%", change);
    }

    let _ = writeln!(body, "总体动量评分: {:+.1}", overall_score);
    let _ = writeln!(body, "---");

    for summary in summaries {
        let ema_spread_pct = if summary.ema_slow.abs() > f64::EPSILON {
            (summary.ema_fast - summary.ema_slow) / summary.ema_slow * 100.0
        } else {
            0.0
        };
        let _ = writeln!(
            body,
            "{} **{} ({})** | 分数 {:+.1} | Δ1K {:+.2}% | 斜率 {:+.2}% | EMA差 {:+.2}% | RSI {:.1} | MACD {:.3} | ADX {:.1}",
            summary.bias.emoji(),
            summary.label,
            summary.interval,
            summary.score,
            summary.change_pct,
            summary.slope_pct,
            ema_spread_pct,
            summary.rsi,
            summary.macd_hist,
            summary.adx
        );
    }

    body
}
