use std::fmt::Write as _;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Args;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

const DEFAULT_CONFIG_PATH: &str = "config/trend_report.yml";
const STRATEGY_NAME: &str = "ETH_AI_TREND";

#[derive(Debug, Clone, Args)]
pub struct TrendReportArgs {
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    pub config: String,
}

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

fn calculate_ema(prices: &[f64], period: usize) -> Option<f64> {
    if prices.is_empty() || period == 0 {
        return None;
    }

    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut ema = prices[0];
    for price in prices.iter().skip(1) {
        ema = (price - ema) * multiplier + ema;
    }
    Some(ema)
}

fn calculate_rsi(prices: &[f64], period: usize) -> Option<f64> {
    if prices.len() < period + 1 || period == 0 {
        return None;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;
    for i in prices.len() - period..prices.len() {
        let change = prices[i] - prices[i - 1];
        if change > 0.0 {
            gains += change;
        } else {
            losses += change.abs();
        }
    }

    let avg_gain = gains / period as f64;
    let avg_loss = losses / period as f64;
    if avg_loss == 0.0 {
        return Some(100.0);
    }

    let rs = avg_gain / avg_loss;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

fn calculate_macd(prices: &[f64]) -> Option<(f64, f64, f64)> {
    if prices.len() < 26 {
        return None;
    }

    let ema12 = calculate_ema(prices, 12)?;
    let ema26 = calculate_ema(prices, 26)?;
    let macd_line = ema12 - ema26;
    let signal = macd_line * 0.9;
    let histogram = macd_line - signal;
    Some((macd_line, signal, histogram))
}

fn calculate_adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    if highs.len() < period + 1 || lows.len() < period + 1 || closes.len() < period + 1 {
        return None;
    }

    let mut tr_values = Vec::new();
    let mut plus_dm = Vec::new();
    let mut minus_dm = Vec::new();

    for i in 1..highs.len() {
        let high_diff = highs[i] - highs[i - 1];
        let low_diff = lows[i - 1] - lows[i];

        plus_dm.push(if high_diff > low_diff && high_diff > 0.0 {
            high_diff
        } else {
            0.0
        });
        minus_dm.push(if low_diff > high_diff && low_diff > 0.0 {
            low_diff
        } else {
            0.0
        });

        let high_low = highs[i] - lows[i];
        let high_close = (highs[i] - closes[i - 1]).abs();
        let low_close = (lows[i] - closes[i - 1]).abs();
        tr_values.push(high_low.max(high_close).max(low_close));
    }

    if tr_values.len() < period {
        return None;
    }

    let tr_sum: f64 = tr_values[tr_values.len() - period..].iter().sum();
    if tr_sum == 0.0 {
        return Some(0.0);
    }

    let plus_di = 100.0 * plus_dm[plus_dm.len() - period..].iter().sum::<f64>() / tr_sum;
    let minus_di = 100.0 * minus_dm[minus_dm.len() - period..].iter().sum::<f64>() / tr_sum;
    let dx = if plus_di + minus_di == 0.0 {
        0.0
    } else {
        100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    };

    Some(dx)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Kline {
    symbol: String,
    interval: String,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    quote_volume: f64,
    trade_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct WebhookConfig {
    wechat_work: String,
    enabled: bool,
    min_level: String,
    rate_limit_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MessageLevel {
    Info = 1,
}

impl MessageLevel {
    fn from_str(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "debug" | "info" | "warning" | "warn" | "error" | "critical" => Self::Info,
            _ => Self::Info,
        }
    }

    fn emoji(self) -> &'static str {
        "ℹ️"
    }
}

#[derive(Debug, Serialize)]
struct WeChatWorkMessage {
    msgtype: String,
    markdown: MarkdownContent,
}

#[derive(Debug, Serialize)]
struct MarkdownContent {
    content: String,
}

struct WebhookNotifier {
    config: WebhookConfig,
    client: Client,
    last_send_times: Arc<tokio::sync::RwLock<std::collections::HashMap<String, DateTime<Utc>>>>,
}

impl WebhookNotifier {
    fn new(config: WebhookConfig, client: Client) -> Self {
        Self {
            config,
            client,
            last_send_times: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    async fn send_event(
        &self,
        strategy: &str,
        title: &str,
        body: &str,
        level: MessageLevel,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let min_level = MessageLevel::from_str(&self.config.min_level);
        if level < min_level {
            return Ok(());
        }

        let key = format!("{strategy}::{title}");
        if !self.check_rate_limit(&key).await {
            log::debug!("跳过推送，限流中: {key}");
            return Ok(());
        }

        let content = self.build_event_message(strategy, title, body, level);
        self.send_to_wechat_work(content).await
    }

    async fn check_rate_limit(&self, key: &str) -> bool {
        let mut times = self.last_send_times.write().await;
        let now = Utc::now();

        if let Some(last_time) = times.get(key) {
            let elapsed = now.signed_duration_since(*last_time);
            if elapsed < chrono::Duration::seconds(self.config.rate_limit_seconds as i64) {
                return false;
            }
        }

        times.insert(key.to_string(), now);
        true
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

    async fn send_to_wechat_work(&self, content: String) -> Result<()> {
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
            log::info!("成功发送Webhook通知");
        } else {
            log::warn!("Webhook发送失败: {}", response.status());
        }

        Ok(())
    }
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
        format!("{base}/{quote}")
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

pub async fn run_trend_report(args: TrendReportArgs) -> Result<()> {
    let config = TrendReporterConfig::from_file(&args.config)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .context("初始化HTTP客户端失败")?;
    let notifier = Arc::new(WebhookNotifier::new(config.webhook.clone(), client.clone()));

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
