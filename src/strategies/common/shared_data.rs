use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Duration, Utc};
use once_cell::sync::Lazy;

use crate::strategies::mean_reversion::indicators::IndicatorOutputs;
use crate::strategies::mean_reversion::model::SymbolSnapshot;

/// 市场状态分类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    Trending,
    Ranging,
    Extreme,
}

/// 斐波那契锚点
#[derive(Debug, Clone)]
pub struct TrendFibAnchors {
    pub swing_high: f64,
    pub swing_low: f64,
    pub level_382: f64,
    pub level_500: f64,
    pub level_618: f64,
}

impl Default for TrendFibAnchors {
    fn default() -> Self {
        Self {
            swing_high: 0.0,
            swing_low: 0.0,
            level_382: 0.0,
            level_500: 0.0,
            level_618: 0.0,
        }
    }
}

/// 趋势指标快照
#[derive(Debug, Clone)]
pub struct TrendIndicatorSnapshot {
    pub ema_fast: f64,
    pub ema_slow: f64,
    pub ema_slope_1m: f64,
    pub ema_slope_5m: f64,
    pub atr_5m: f64,
    pub atr_15m: f64,
    pub boll_mid: f64,
    pub boll_bandwidth: f64,
    pub fib_anchors: TrendFibAnchors,
    pub ofi: f64,
    pub orderbook_imbalance: f64,
    pub data_quality_score: f64,
    pub regime: MarketRegime,
    pub updated_at: DateTime<Utc>,
}

impl Default for TrendIndicatorSnapshot {
    fn default() -> Self {
        Self {
            ema_fast: 0.0,
            ema_slow: 0.0,
            ema_slope_1m: 0.0,
            ema_slope_5m: 0.0,
            atr_5m: 0.0,
            atr_15m: 0.0,
            boll_mid: 0.0,
            boll_bandwidth: 0.0,
            fib_anchors: TrendFibAnchors::default(),
            ofi: 0.0,
            orderbook_imbalance: 0.0,
            data_quality_score: 100.0,
            regime: MarketRegime::Ranging,
            updated_at: Utc::now(),
        }
    }
}

/// 共享给趋势策略使用的行情与指标快照
#[derive(Debug, Clone)]
pub struct SharedSymbolData {
    pub snapshot: SymbolSnapshot,
    pub indicators: IndicatorOutputs,
    pub updated_at: DateTime<Utc>,
    pub trend_snapshot: Option<TrendIndicatorSnapshot>,
}

impl SharedSymbolData {
    /// 判断数据是否过期
    pub fn is_stale(&self, max_age: Duration) -> bool {
        Utc::now() - self.updated_at > max_age
    }

    /// 返回当前数据质量分数
    pub fn data_quality(&self) -> f64 {
        self.trend_snapshot
            .as_ref()
            .map(|snapshot| snapshot.data_quality_score)
            .unwrap_or(100.0)
    }

    /// 返回推断的市场状态
    pub fn regime(&self) -> MarketRegime {
        self.trend_snapshot
            .as_ref()
            .map(|snapshot| snapshot.regime)
            .unwrap_or(MarketRegime::Ranging)
    }
}

static SHARED_TREND_DATA: Lazy<RwLock<HashMap<String, SharedSymbolData>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// 发布均值回归策略的快照供趋势策略复用
pub fn publish_trend_inputs(
    symbol: &str,
    snapshot: &SymbolSnapshot,
    indicators: &IndicatorOutputs,
    trend_snapshot: Option<TrendIndicatorSnapshot>,
) {
    publish_trend_inputs_with_timestamp(symbol, snapshot, indicators, trend_snapshot, Utc::now());
}

pub fn publish_trend_inputs_with_timestamp(
    symbol: &str,
    snapshot: &SymbolSnapshot,
    indicators: &IndicatorOutputs,
    trend_snapshot: Option<TrendIndicatorSnapshot>,
    updated_at: DateTime<Utc>,
) {
    if let Ok(mut map) = SHARED_TREND_DATA.write() {
        map.insert(
            symbol.to_string(),
            SharedSymbolData {
                snapshot: snapshot.clone(),
                indicators: indicators.clone(),
                updated_at,
                trend_snapshot,
            },
        );
    }
}

/// 获取共享快照
pub fn get_trend_inputs(symbol: &str) -> Option<SharedSymbolData> {
    SHARED_TREND_DATA
        .read()
        .ok()
        .and_then(|map| map.get(symbol).cloned())
}

/// 清理共享数据
pub fn remove_trend_inputs(symbol: &str) {
    if let Ok(mut map) = SHARED_TREND_DATA.write() {
        map.remove(symbol);
    }
}

/// 返回当前已发布的交易对列表
pub fn list_shared_symbols() -> Vec<String> {
    SHARED_TREND_DATA
        .read()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}
