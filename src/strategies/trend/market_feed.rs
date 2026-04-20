use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use log::{error, info, warn};
use serde::Deserialize;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::core::types::{Interval, Kline, MarketType};
use crate::core::websocket::{BaseWebSocketClient, WebSocketClient};
use crate::Exchange;

use super::config::MarketDataConfig;

const WS_BASE_URL: &str = "wss://fstream.binance.com/stream";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandleInterval {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
}

impl CandleInterval {
    pub const ALL: [CandleInterval; 4] = [
        CandleInterval::OneMinute,
        CandleInterval::FiveMinutes,
        CandleInterval::FifteenMinutes,
        CandleInterval::OneHour,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            CandleInterval::OneMinute => "1m",
            CandleInterval::FiveMinutes => "5m",
            CandleInterval::FifteenMinutes => "15m",
            CandleInterval::OneHour => "1h",
        }
    }

    pub fn as_interval(&self) -> Interval {
        match self {
            CandleInterval::OneMinute => Interval::OneMinute,
            CandleInterval::FiveMinutes => Interval::FiveMinutes,
            CandleInterval::FifteenMinutes => Interval::FifteenMinutes,
            CandleInterval::OneHour => Interval::OneHour,
        }
    }

    pub fn stream_suffix(&self) -> &'static str {
        match self {
            CandleInterval::OneMinute => "kline_1m",
            CandleInterval::FiveMinutes => "kline_5m",
            CandleInterval::FifteenMinutes => "kline_15m",
            CandleInterval::OneHour => "kline_1h",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "1m" => Some(CandleInterval::OneMinute),
            "5m" => Some(CandleInterval::FiveMinutes),
            "15m" => Some(CandleInterval::FifteenMinutes),
            "1h" => Some(CandleInterval::OneHour),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CandleEvent {
    pub symbol: String,
    pub interval: CandleInterval,
    pub kline: Kline,
    pub is_final: bool,
    pub latency_ms: i64,
}

#[derive(Debug, Clone)]
pub struct CandleSnapshot {
    pub one_minute: Vec<Kline>,
    pub five_minutes: Vec<Kline>,
    pub fifteen_minutes: Vec<Kline>,
    pub one_hour: Vec<Kline>,
}

impl CandleSnapshot {
    pub fn len(&self, interval: CandleInterval) -> usize {
        match interval {
            CandleInterval::OneMinute => self.one_minute.len(),
            CandleInterval::FiveMinutes => self.five_minutes.len(),
            CandleInterval::FifteenMinutes => self.fifteen_minutes.len(),
            CandleInterval::OneHour => self.one_hour.len(),
        }
    }
}

#[derive(Default)]
struct SymbolCandles {
    buffers: HashMap<CandleInterval, VecDeque<Kline>>,
}

impl SymbolCandles {
    fn buffer_mut(&mut self, interval: CandleInterval, capacity: usize) -> &mut VecDeque<Kline> {
        self.buffers
            .entry(interval)
            .or_insert_with(|| VecDeque::with_capacity(capacity))
    }

    fn buffer_clone(&self, interval: CandleInterval) -> Vec<Kline> {
        self.buffers
            .get(&interval)
            .map(|deque| deque.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Clone)]
pub struct CandleCache {
    capacity: usize,
    store: Arc<RwLock<HashMap<String, SymbolCandles>>>,
    broadcaster: broadcast::Sender<CandleEvent>,
}

impl CandleCache {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(512);
        Self {
            capacity: capacity.max(1),
            store: Arc::new(RwLock::new(HashMap::new())),
            broadcaster: tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CandleEvent> {
        self.broadcaster.subscribe()
    }

    pub async fn apply_event(&self, event: CandleEvent) {
        {
            let mut guard = self.store.write().await;
            let entry = guard.entry(event.symbol.clone()).or_default();
            let buffer = entry.buffer_mut(event.interval, self.capacity);

            if let Some(last) = buffer.back_mut() {
                if last.open_time == event.kline.open_time {
                    *last = event.kline.clone();
                } else if last.open_time < event.kline.open_time {
                    buffer.push_back(event.kline.clone());
                    if buffer.len() > self.capacity {
                        buffer.pop_front();
                    }
                } else {
                    // 过时K线，忽略
                    return;
                }
            } else {
                buffer.push_back(event.kline.clone());
            }
        }

        let _ = self.broadcaster.send(event);
    }

    pub async fn seed(&self, symbol: &str, interval: CandleInterval, candles: Vec<Kline>) {
        if candles.is_empty() {
            return;
        }

        let mut ordered = candles;
        ordered.sort_by_key(|k| k.open_time);
        if ordered.len() > self.capacity {
            let drain = ordered.len() - self.capacity;
            ordered.drain(0..drain);
        }

        let last = ordered.last().cloned();

        {
            let mut guard = self.store.write().await;
            let entry = guard.entry(symbol.to_string()).or_default();
            let buffer = entry.buffer_mut(interval, self.capacity);
            buffer.clear();
            for candle in ordered.iter() {
                buffer.push_back(candle.clone());
            }
        }

        if let Some(kline) = last {
            let event = CandleEvent {
                symbol: symbol.to_string(),
                interval,
                kline,
                is_final: true,
                latency_ms: 0,
            };
            let _ = self.broadcaster.send(event);
        }
    }

    pub async fn snapshot(&self, symbol: &str) -> Option<CandleSnapshot> {
        let guard = self.store.read().await;
        let entry = guard.get(symbol)?;
        Some(CandleSnapshot {
            one_minute: entry.buffer_clone(CandleInterval::OneMinute),
            five_minutes: entry.buffer_clone(CandleInterval::FiveMinutes),
            fifteen_minutes: entry.buffer_clone(CandleInterval::FifteenMinutes),
            one_hour: entry.buffer_clone(CandleInterval::OneHour),
        })
    }
}

#[derive(Clone)]
pub struct TrendMarketFeed {
    symbols: Arc<Vec<String>>,
    symbol_map: Arc<HashMap<String, String>>,
    exchange: Arc<Box<dyn Exchange>>,
    market_type: MarketType,
    cache: Arc<CandleCache>,
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    running: Arc<AtomicBool>,
    config: MarketDataConfig,
    offline_mode: bool,
    last_event_at_ms: Arc<AtomicI64>,
}

impl TrendMarketFeed {
    pub fn new(
        symbols: Vec<String>,
        exchange: Arc<Box<dyn Exchange>>,
        market_data: MarketDataConfig,
    ) -> Self {
        let mut symbol_map = HashMap::new();
        for symbol in &symbols {
            symbol_map.insert(Self::normalize_symbol(symbol), symbol.clone());
        }

        let offline_mode = std::env::var("RUSTCTA_OFFLINE")
            .map(|val| {
                matches!(
                    val.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        Self {
            symbols: Arc::new(symbols),
            symbol_map: Arc::new(symbol_map),
            exchange,
            market_type: MarketType::Futures,
            cache: Arc::new(CandleCache::new(market_data.cache_depth)),
            handles: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            config: market_data,
            offline_mode,
            last_event_at_ms: Arc::new(AtomicI64::new(Utc::now().timestamp_millis())),
        }
    }

    pub fn candle_cache(&self) -> Arc<CandleCache> {
        self.cache.clone()
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.bootstrap_history().await?;

        if self.offline_mode {
            info!("TrendMarketFeed: 离线模式，跳过实时行情启动");
            return Ok(());
        }

        let feed = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                if !feed.running.load(Ordering::SeqCst) {
                    break;
                }

                match feed.run_stream().await {
                    Ok(_) => break,
                    Err(err) => {
                        warn!("TrendMarketFeed: WebSocket循环错误: {}", err);
                        if let Err(e) = feed.bootstrap_history().await {
                            warn!("TrendMarketFeed: 重建缓存失败: {}", e);
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        self.handles.lock().await.push(handle);
        Ok(())
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
    }

    async fn bootstrap_history(&self) -> Result<()> {
        if self.symbols.is_empty() {
            warn!("TrendMarketFeed: 未配置交易对，跳过bootstrap");
            return Ok(());
        }

        let limit = Some(self.config.bootstrap_bars as u32);
        for symbol in self.symbols.iter() {
            for interval in CandleInterval::ALL.iter() {
                match self
                    .exchange
                    .as_ref()
                    .get_klines(symbol, interval.as_interval(), self.market_type, limit)
                    .await
                {
                    Ok(candles) => {
                        self.cache.seed(symbol, *interval, candles).await;
                    }
                    Err(err) => {
                        warn!(
                            "TrendMarketFeed: 获取 {} {} K线失败: {}",
                            symbol,
                            interval.label(),
                            err
                        );
                    }
                }
            }
        }

        info!("TrendMarketFeed: 已完成历史K线预热");
        Ok(())
    }

    async fn run_stream(&self) -> Result<()> {
        if self.symbols.is_empty() {
            return Ok(());
        }

        let stream_url = self.build_stream_url();
        info!("TrendMarketFeed: 连接 {}", stream_url);
        let mut client = BaseWebSocketClient::new(stream_url, "binance".to_string());
        client.connect().await?;
        self.last_event_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::SeqCst);

        while self.running.load(Ordering::SeqCst) {
            let now_ms = Utc::now().timestamp_millis();
            let last_ms = self.last_event_at_ms.load(Ordering::SeqCst);
            if now_ms - last_ms > 180_000 {
                return Err(anyhow!(
                    "行情流超过 {}s 未收到事件",
                    (now_ms - last_ms) / 1000
                ));
            }

            match client.receive().await {
                Ok(Some(message)) => {
                    if let Err(err) = self.handle_message(&message).await {
                        warn!("TrendMarketFeed: 处理消息失败: {}", err);
                    }
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(err) => {
                    warn!("TrendMarketFeed: 接收失败: {}", err);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&self, payload: &str) -> Result<()> {
        let envelope: WsEnvelope = serde_json::from_str(payload)?;
        if let Some(mut event) = envelope.into_event() {
            if let Some(canonical) = self
                .symbol_map
                .get(&Self::normalize_symbol(&event.symbol))
                .cloned()
            {
                event.symbol = canonical.clone();
                event.kline.symbol = canonical;
            } else {
                return Ok(());
            }

            self.last_event_at_ms
                .store(Utc::now().timestamp_millis(), Ordering::SeqCst);
            self.cache.apply_event(event).await;
        }
        Ok(())
    }

    fn build_stream_url(&self) -> String {
        let streams: Vec<String> = self
            .symbols
            .iter()
            .flat_map(|symbol| {
                CandleInterval::ALL.iter().map(move |interval| {
                    format!(
                        "{}@{}",
                        Self::normalize_symbol(symbol),
                        interval.stream_suffix()
                    )
                })
            })
            .collect();

        if streams.is_empty() {
            return WS_BASE_URL.to_string();
        }

        format!("{}?streams={}", WS_BASE_URL, streams.join("/"))
    }

    fn normalize_symbol(symbol: &str) -> String {
        symbol.replace('/', "").to_lowercase()
    }
}

#[derive(Debug, Deserialize)]
struct WsEnvelope {
    stream: String,
    data: WsPayload,
}

impl WsEnvelope {
    fn into_event(self) -> Option<CandleEvent> {
        self.data.into_event()
    }
}

#[derive(Debug, Deserialize)]
struct WsPayload {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "k")]
    kline: WsKline,
}

impl WsPayload {
    fn into_event(self) -> Option<CandleEvent> {
        let WsPayload {
            symbol,
            event_time,
            kline,
        } = self;

        let interval = CandleInterval::from_label(&kline.interval)?;
        let is_final = kline.is_final;
        let parsed_kline = kline.into_kline(&symbol.to_uppercase())?;
        let event_time = Utc.timestamp_millis_opt(event_time).single()?;
        let latency_ms = Utc::now().timestamp_millis() - event_time.timestamp_millis();

        Some(CandleEvent {
            symbol: parsed_kline.symbol.clone(),
            interval,
            kline: parsed_kline,
            is_final,
            latency_ms,
        })
    }
}

#[derive(Debug, Deserialize)]
struct WsKline {
    #[serde(rename = "t")]
    open_time: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "n")]
    trade_count: u64,
    #[serde(rename = "x")]
    is_final: bool,
}

impl WsKline {
    fn into_kline(self, symbol: &str) -> Option<Kline> {
        Some(Kline {
            symbol: symbol.to_string(),
            interval: self.interval.clone(),
            open_time: Utc.timestamp_millis_opt(self.open_time).single()?,
            close_time: Utc.timestamp_millis_opt(self.close_time).single()?,
            open: self.open.parse().ok()?,
            high: self.high.parse().ok()?,
            low: self.low.parse().ok()?,
            close: self.close.parse().ok()?,
            volume: self.volume.parse().ok()?,
            quote_volume: self.quote_volume.parse().ok()?,
            trade_count: self.trade_count,
        })
    }
}
