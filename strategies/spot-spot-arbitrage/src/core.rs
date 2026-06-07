use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const SYMBOL_BLACKLIST_FAILURE_THRESHOLD: u32 = 20;
const SYMBOL_BLACKLIST_TTL_MS: i64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpotVenue {
    Mexc,
    CoinEx,
    GateIo,
    Bitget,
}

impl SpotVenue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mexc => "mexc",
            Self::CoinEx => "coinex",
            Self::GateIo => "gateio",
            Self::Bitget => "bitget",
        }
    }

    pub fn other(self) -> Self {
        match self {
            Self::Mexc => Self::CoinEx,
            Self::CoinEx => Self::Mexc,
            Self::GateIo => Self::Bitget,
            Self::Bitget => Self::GateIo,
        }
    }
}

pub fn configured_spot_pair(exchanges: &[String]) -> (SpotVenue, SpotVenue) {
    let has_gateio = exchanges.iter().any(|exchange| {
        matches!(
            exchange.trim().to_ascii_lowercase().as_str(),
            "gate" | "gateio" | "gate.io"
        )
    });
    let has_bitget = exchanges
        .iter()
        .any(|exchange| exchange.trim().eq_ignore_ascii_case("bitget"));
    if has_gateio && has_bitget {
        (SpotVenue::GateIo, SpotVenue::Bitget)
    } else {
        (SpotVenue::Mexc, SpotVenue::CoinEx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    StaleBook,
    InsufficientDepth,
    MinNotional,
    SymbolRule,
    InsufficientQuoteBalance,
    InsufficientBaseBalance,
    NetSpreadBelowThreshold,
    AbnormalSpread,
    NotionalLimit,
    Cooldown,
    ExchangeHealth,
    DailyLossLimit,
    TradeLossLimit,
    ConsecutiveRejections,
    SymbolBlacklisted,
    DisabledSymbol,
    DisabledExchange,
    DisabledExchangeSymbol,
    ControlPlaneBlocked,
    PaperExecutionRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSource {
    Websocket,
    Rest,
    Replay,
}

impl BookSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::Rest => "rest",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFeeSource {
    Config,
    Exchange,
    Default,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpotOrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

impl SpotOrderBookLevel {
    pub fn notional(&self) -> f64 {
        self.price * self.quantity
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedBook {
    pub exchange: String,
    pub symbol: String,
    pub bids: Vec<SpotOrderBookLevel>,
    pub asks: Vec<SpotOrderBookLevel>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    pub sequence: Option<u64>,
    pub source: BookSource,
    pub is_stale: bool,
}

impl CachedBook {
    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.local_timestamp)
            .num_milliseconds()
            .max(0)
    }

    pub fn is_fresh(&self, now: DateTime<Utc>, max_age_ms: i64) -> bool {
        !self.is_stale && self.age_ms(now) <= max_age_ms
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub raw_spread_bps: f64,
    pub buy_fee_bps: f64,
    pub sell_fee_bps: f64,
    pub fee_source_buy: SpotFeeSource,
    pub fee_source_sell: SpotFeeSource,
    pub platform_discount_applied: bool,
    pub estimated_fee_bps: f64,
    pub estimated_slippage_bps: f64,
    pub safety_buffer_bps: f64,
    pub estimated_net_spread_bps: f64,
    pub estimated_total_fee: f64,
    pub estimated_gross_pnl: f64,
    pub estimated_net_pnl: f64,
    pub executable_notional: f64,
    pub quantity: f64,
    pub accepted: bool,
    pub rejection_reason: Option<RejectionReason>,
    pub rejection_detail: Option<String>,
    pub buy_book_age_ms: i64,
    pub sell_book_age_ms: i64,
    pub buy_book_source: BookSource,
    pub sell_book_source: BookSource,
    pub buy_latency_ms: Option<i64>,
    pub sell_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimulatedTradeRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_avg_price: f64,
    pub sell_avg_price: f64,
    pub quantity: f64,
    pub notional: f64,
    pub buy_fee: f64,
    pub sell_fee: f64,
    pub gross_pnl: f64,
    pub net_pnl: f64,
    pub latency_ms: i64,
    pub order_book_age_ms: i64,
    pub execution_mode: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SpotRiskLimits {
    pub max_notional_per_symbol: f64,
    pub max_total_notional: f64,
    pub max_daily_loss: f64,
    pub max_trade_loss: f64,
    pub max_consecutive_rejections: u32,
}

impl Default for SpotRiskLimits {
    fn default() -> Self {
        Self {
            max_notional_per_symbol: 500.0,
            max_total_notional: 1_000.0,
            max_daily_loss: 100.0,
            max_trade_loss: 10.0,
            max_consecutive_rejections: 20,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpotRiskState {
    symbol_notional: HashMap<String, f64>,
    total_notional: f64,
    consecutive_rejections: u32,
    symbol_failures: HashMap<String, u32>,
    cooldown_until: HashMap<String, DateTime<Utc>>,
    blacklisted_until: HashMap<String, DateTime<Utc>>,
    daily_pnl: f64,
}

impl SpotRiskState {
    pub fn is_in_cooldown(&self, symbol: &str, now: DateTime<Utc>) -> bool {
        self.cooldown_until
            .get(symbol)
            .is_some_and(|until| *until > now)
    }

    pub fn is_symbol_blacklisted(&self, symbol: &str, now: DateTime<Utc>) -> bool {
        self.blacklisted_until
            .get(symbol)
            .is_some_and(|until| *until > now)
    }

    pub fn daily_loss_limit_hit(&self, limits: &SpotRiskLimits) -> bool {
        self.daily_pnl < -limits.max_daily_loss
    }

    pub fn consecutive_rejection_limit_hit(&self, limits: &SpotRiskLimits) -> bool {
        self.consecutive_rejections >= limits.max_consecutive_rejections
    }

    pub fn trade_loss_limit_hit(
        &self,
        limits: &SpotRiskLimits,
        trade: &SimulatedTradeRecord,
    ) -> bool {
        trade.net_pnl < -limits.max_trade_loss
    }

    pub fn notional_limit_hit(&self, limits: &SpotRiskLimits, symbol: &str, notional: f64) -> bool {
        self.total_notional + notional > limits.max_total_notional + 1e-12
            || self
                .symbol_notional
                .get(symbol)
                .copied()
                .unwrap_or_default()
                + notional
                > limits.max_notional_per_symbol + 1e-12
    }

    pub fn record_rejection(&mut self, symbol: &str, reason: RejectionReason, now: DateTime<Utc>) {
        if spot_rejection_counts_toward_consecutive(reason) {
            self.consecutive_rejections += 1;
        }
        if matches!(reason, RejectionReason::ExchangeHealth) {
            let failures = self.symbol_failures.entry(symbol.to_string()).or_default();
            *failures += 1;
            if *failures >= SYMBOL_BLACKLIST_FAILURE_THRESHOLD {
                self.blacklisted_until.insert(
                    symbol.to_string(),
                    now + chrono::Duration::milliseconds(SYMBOL_BLACKLIST_TTL_MS),
                );
                *failures = 0;
            }
        }
    }

    pub fn record_trade(&mut self, trade: &SimulatedTradeRecord, now: DateTime<Utc>) {
        self.consecutive_rejections = 0;
        self.total_notional += trade.notional;
        *self
            .symbol_notional
            .entry(trade.symbol.clone())
            .or_default() += trade.notional;
        self.daily_pnl += trade.net_pnl;
        self.symbol_failures.remove(&trade.symbol);
        self.blacklisted_until.remove(&trade.symbol);
        self.cooldown_until.insert(trade.symbol.clone(), now);
    }

    pub fn apply_trade_cooldown(&mut self, symbol: &str, cooldown_ms: u64, now: DateTime<Utc>) {
        self.cooldown_until.insert(
            symbol.to_string(),
            now + chrono::Duration::milliseconds(cooldown_ms as i64),
        );
    }

    pub fn consecutive_rejections(&self) -> u32 {
        self.consecutive_rejections
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    pub fn total_notional(&self) -> f64 {
        self.total_notional
    }

    pub fn symbol_notional(&self, symbol: &str) -> f64 {
        self.symbol_notional
            .get(symbol)
            .copied()
            .unwrap_or_default()
    }
}

pub fn spot_rejection_counts_toward_consecutive(reason: RejectionReason) -> bool {
    matches!(
        reason,
        RejectionReason::InsufficientQuoteBalance
            | RejectionReason::InsufficientBaseBalance
            | RejectionReason::NotionalLimit
            | RejectionReason::DailyLossLimit
            | RejectionReason::TradeLossLimit
            | RejectionReason::ControlPlaneBlocked
            | RejectionReason::PaperExecutionRejected
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadEstimate {
    pub raw_spread: f64,
    pub raw_spread_bps: f64,
    pub estimated_cost_bps: f64,
    pub net_spread_bps: f64,
}

pub fn calculate_spread(
    buy_price: f64,
    sell_price: f64,
    buy_taker_fee_bps: f64,
    sell_taker_fee_bps: f64,
    slippage_bps: f64,
    safety_buffer_bps: f64,
) -> SpreadEstimate {
    let raw_spread = sell_price - buy_price;
    let raw_spread_bps = if buy_price > 0.0 {
        raw_spread / buy_price * 10_000.0
    } else {
        0.0
    };
    let estimated_cost_bps =
        buy_taker_fee_bps + sell_taker_fee_bps + slippage_bps + safety_buffer_bps;
    SpreadEstimate {
        raw_spread,
        raw_spread_bps,
        estimated_cost_bps,
        net_spread_bps: raw_spread_bps - estimated_cost_bps,
    }
}

pub fn depth_notional(levels: &[SpotOrderBookLevel], target_notional: f64) -> f64 {
    if target_notional <= 0.0 {
        return 0.0;
    }
    let mut remaining = target_notional;
    let mut consumed = 0.0;
    for level in levels {
        let level_notional = level.notional().max(0.0);
        let take = level_notional.min(remaining);
        consumed += take;
        remaining -= take;
        if remaining <= 0.0 {
            break;
        }
    }
    consumed
}

#[derive(Debug, Clone, Default)]
pub struct SummaryReport {
    pub symbols_scanned: u64,
    pub opportunities_detected: u64,
    pub opportunities_accepted: u64,
    pub opportunities_rejected: u64,
    pub rejection_reasons: HashMap<RejectionReason, u64>,
    pub total_simulated_trades: u64,
    pub total_gross_pnl: f64,
    pub total_net_pnl: f64,
    pub total_fees: f64,
    raw_spread_sum: f64,
    net_spread_sum: f64,
    book_age_sum: i64,
    book_age_count: u64,
    symbol_net_pnl: HashMap<String, f64>,
}

impl SummaryReport {
    pub fn record_opportunity(&mut self, opportunity: &OpportunityRecord) {
        self.opportunities_detected += 1;
        self.raw_spread_sum += opportunity.raw_spread_bps;
        self.net_spread_sum += opportunity.estimated_net_spread_bps;
        self.book_age_sum += opportunity
            .buy_book_age_ms
            .max(opportunity.sell_book_age_ms);
        self.book_age_count += 1;
        if opportunity.accepted {
            self.opportunities_accepted += 1;
        } else {
            self.opportunities_rejected += 1;
            if let Some(reason) = opportunity.rejection_reason {
                self.record_rejection(reason);
            }
        }
    }

    pub fn record_rejection(&mut self, reason: RejectionReason) {
        *self.rejection_reasons.entry(reason).or_default() += 1;
    }

    pub fn record_trade(&mut self, trade: &SimulatedTradeRecord) {
        self.total_simulated_trades += 1;
        self.total_gross_pnl += trade.gross_pnl;
        self.total_net_pnl += trade.net_pnl;
        self.total_fees += trade.buy_fee + trade.sell_fee;
        *self.symbol_net_pnl.entry(trade.symbol.clone()).or_default() += trade.net_pnl;
    }

    pub fn symbol_net_pnl(&self, symbol: &str) -> f64 {
        self.symbol_net_pnl.get(symbol).copied().unwrap_or_default()
    }

    pub fn avg_raw_spread_bps(&self) -> f64 {
        average(self.raw_spread_sum, self.opportunities_detected)
    }

    pub fn avg_net_spread_bps(&self) -> f64 {
        average(self.net_spread_sum, self.opportunities_detected)
    }

    pub fn avg_book_age_ms(&self) -> f64 {
        if self.book_age_count > 0 {
            self.book_age_sum as f64 / self.book_age_count as f64
        } else {
            0.0
        }
    }

    pub fn render(&self) -> String {
        format!(
            "spot_spot_taker_arbitrage report symbols_scanned={} opportunities={} accepted={} rejected={} trades={} gross_pnl={:.6} net_pnl={:.6} fees={:.6} avg_raw_bps={:.3} avg_net_bps={:.3} avg_book_age_ms={:.1} rejections={:?}",
            self.symbols_scanned,
            self.opportunities_detected,
            self.opportunities_accepted,
            self.opportunities_rejected,
            self.total_simulated_trades,
            self.total_gross_pnl,
            self.total_net_pnl,
            self.total_fees,
            self.avg_raw_spread_bps(),
            self.avg_net_spread_bps(),
            self.avg_book_age_ms(),
            self.rejection_reasons
        )
    }
}

fn average(sum: f64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}
