use std::collections::{BTreeSet, HashMap};

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
    #[serde(default)]
    pub capital_cost_bps: f64,
    #[serde(default)]
    pub transfer_cost_bps: f64,
    #[serde(default)]
    pub transfer_delay_penalty_bps: f64,
    #[serde(default)]
    pub inventory_rebalance_cost_bps: f64,
    #[serde(default)]
    pub latency_penalty_bps: f64,
    #[serde(default)]
    pub effective_min_net_spread_bps: f64,
    #[serde(default)]
    pub estimated_slippage_cost: f64,
    #[serde(default)]
    pub estimated_capital_cost: f64,
    #[serde(default)]
    pub estimated_transfer_cost: f64,
    #[serde(default)]
    pub estimated_inventory_rebalance_cost: f64,
    #[serde(default)]
    pub estimated_latency_penalty_cost: f64,
    #[serde(default)]
    pub estimated_total_cost: f64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TradePnlCategory {
    #[default]
    Arbitrage,
    InventoryDrift,
    OneSidedExposure,
    InventoryRecovery,
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
    #[serde(default)]
    pub pnl_category: TradePnlCategory,
    #[serde(default)]
    pub slippage_cost: f64,
    #[serde(default)]
    pub capital_cost: f64,
    #[serde(default)]
    pub transfer_cost: f64,
    #[serde(default)]
    pub inventory_rebalance_cost: f64,
    #[serde(default)]
    pub latency_penalty_cost: f64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpotBookEventKind {
    Snapshot,
    Delta,
    TopOfBook,
    Heartbeat,
    Stale,
    Gap,
    ChecksumMismatch,
    Reconnect,
    Error,
}

impl SpotBookEventKind {
    pub fn is_tradeable_update(self) -> bool {
        matches!(self, Self::Snapshot | Self::Delta | Self::TopOfBook)
    }

    pub fn is_stale_signal(self) -> bool {
        matches!(
            self,
            Self::Stale | Self::Gap | Self::ChecksumMismatch | Self::Reconnect | Self::Error
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpotBookEvent {
    pub exchange: String,
    pub symbol: String,
    pub event_kind: SpotBookEventKind,
    pub bids: Vec<SpotOrderBookLevel>,
    pub asks: Vec<SpotOrderBookLevel>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    pub sequence: Option<u64>,
    pub source: BookSource,
    pub is_tradeable: bool,
    pub stale_reason: Option<String>,
}

impl SpotBookEvent {
    pub fn snapshot(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        bid: f64,
        ask: f64,
        quantity: f64,
        local_timestamp: DateTime<Utc>,
    ) -> Self {
        let bid_price = bid;
        let ask_price = ask;
        let bid = SpotOrderBookLevel {
            price: bid,
            quantity,
        };
        let ask = SpotOrderBookLevel {
            price: ask,
            quantity,
        };
        Self {
            exchange: normalize_exchange(&exchange.into()),
            symbol: normalize_symbol(&symbol.into()),
            event_kind: SpotBookEventKind::Snapshot,
            bids: vec![bid],
            asks: vec![ask],
            best_bid: Some(bid_price),
            best_ask: Some(ask_price),
            exchange_timestamp: Some(local_timestamp),
            local_timestamp,
            latency_ms: None,
            sequence: None,
            source: BookSource::Websocket,
            is_tradeable: true,
            stale_reason: None,
        }
    }

    pub fn stale(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        reason: impl Into<String>,
        local_timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            exchange: normalize_exchange(&exchange.into()),
            symbol: normalize_symbol(&symbol.into()),
            event_kind: SpotBookEventKind::Stale,
            bids: Vec::new(),
            asks: Vec::new(),
            best_bid: None,
            best_ask: None,
            exchange_timestamp: None,
            local_timestamp,
            latency_ms: None,
            sequence: None,
            source: BookSource::Websocket,
            is_tradeable: false,
            stale_reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectedVenuePair {
    pub buy_exchange: String,
    pub sell_exchange: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventDrivenSpreadResult {
    pub symbol: String,
    pub updated_exchange: String,
    pub recomputed_pairs: Vec<DirectedVenuePair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventDrivenSpreadEngineConfig {
    pub exchanges: Vec<String>,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EventDrivenSpreadEngine {
    allowed_exchanges: BTreeSet<String>,
    allowed_symbols: BTreeSet<String>,
    books: HashMap<(String, String), CachedBook>,
}

impl EventDrivenSpreadEngine {
    pub fn new(config: EventDrivenSpreadEngineConfig) -> Self {
        Self {
            allowed_exchanges: config
                .exchanges
                .into_iter()
                .map(|exchange| normalize_exchange(&exchange))
                .collect(),
            allowed_symbols: config
                .symbols
                .into_iter()
                .map(|symbol| normalize_symbol(&symbol))
                .collect(),
            books: HashMap::new(),
        }
    }

    pub fn on_book_event(&mut self, event: SpotBookEvent) -> EventDrivenSpreadResult {
        let exchange = normalize_exchange(&event.exchange);
        let symbol = normalize_symbol(&event.symbol);
        if !self.exchange_allowed(&exchange) || !self.symbol_allowed(&symbol) {
            return EventDrivenSpreadResult {
                symbol,
                updated_exchange: exchange,
                recomputed_pairs: Vec::new(),
            };
        }

        if event.event_kind.is_stale_signal() || !event.is_tradeable {
            if let Some(book) = self.books.get_mut(&(exchange.clone(), symbol.clone())) {
                book.is_stale = true;
                book.local_timestamp = event.local_timestamp;
            } else {
                self.books.insert(
                    (exchange.clone(), symbol.clone()),
                    CachedBook {
                        exchange: exchange.clone(),
                        symbol: symbol.clone(),
                        bids: Vec::new(),
                        asks: Vec::new(),
                        best_bid: None,
                        best_ask: None,
                        exchange_timestamp: None,
                        local_timestamp: event.local_timestamp,
                        latency_ms: event.latency_ms,
                        sequence: event.sequence,
                        source: event.source,
                        is_stale: true,
                    },
                );
            }
        } else if event.event_kind.is_tradeable_update() {
            self.books.insert(
                (exchange.clone(), symbol.clone()),
                CachedBook {
                    exchange: exchange.clone(),
                    symbol: symbol.clone(),
                    bids: event.bids,
                    asks: event.asks,
                    best_bid: event.best_bid,
                    best_ask: event.best_ask,
                    exchange_timestamp: event.exchange_timestamp,
                    local_timestamp: event.local_timestamp,
                    latency_ms: event.latency_ms,
                    sequence: event.sequence,
                    source: event.source,
                    is_stale: false,
                },
            );
        }

        EventDrivenSpreadResult {
            recomputed_pairs: self.recomputable_pairs_for_update(&exchange, &symbol),
            symbol,
            updated_exchange: exchange,
        }
    }

    pub fn book(&self, exchange: &str, symbol: &str) -> Option<&CachedBook> {
        self.books
            .get(&(normalize_exchange(exchange), normalize_symbol(symbol)))
    }

    pub fn book_pair(
        &self,
        left_exchange: SpotVenue,
        right_exchange: SpotVenue,
        symbol: &str,
    ) -> Option<(&CachedBook, &CachedBook)> {
        let symbol = normalize_symbol(symbol);
        Some((
            self.books
                .get(&(left_exchange.as_str().to_string(), symbol.clone()))?,
            self.books
                .get(&(right_exchange.as_str().to_string(), symbol))?,
        ))
    }

    fn recomputable_pairs_for_update(
        &self,
        updated_exchange: &str,
        symbol: &str,
    ) -> Vec<DirectedVenuePair> {
        let Some(updated_book) = self
            .books
            .get(&(updated_exchange.to_string(), symbol.to_string()))
        else {
            return Vec::new();
        };
        if !book_has_tradeable_top(updated_book) {
            return Vec::new();
        }

        let mut pairs = Vec::new();
        for ((other_exchange, other_symbol), other_book) in &self.books {
            if other_symbol != symbol || other_exchange == updated_exchange {
                continue;
            }
            if !book_has_tradeable_top(other_book) {
                continue;
            }
            pairs.push(DirectedVenuePair {
                buy_exchange: updated_exchange.to_string(),
                sell_exchange: other_exchange.clone(),
            });
            pairs.push(DirectedVenuePair {
                buy_exchange: other_exchange.clone(),
                sell_exchange: updated_exchange.to_string(),
            });
        }
        pairs.sort_by(|left, right| {
            (left.buy_exchange.as_str(), left.sell_exchange.as_str())
                .cmp(&(right.buy_exchange.as_str(), right.sell_exchange.as_str()))
        });
        pairs
    }

    fn exchange_allowed(&self, exchange: &str) -> bool {
        self.allowed_exchanges.is_empty() || self.allowed_exchanges.contains(exchange)
    }

    fn symbol_allowed(&self, symbol: &str) -> bool {
        self.allowed_symbols.is_empty() || self.allowed_symbols.contains(symbol)
    }
}

fn book_has_tradeable_top(book: &CachedBook) -> bool {
    !book.is_stale && book.best_bid.is_some() && book.best_ask.is_some()
}

fn normalize_exchange(exchange: &str) -> String {
    let normalized = exchange.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
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
    pub total_arbitrage_net_pnl: f64,
    pub total_inventory_recovery_pnl: f64,
    pub total_inventory_drift_pnl: f64,
    pub total_one_sided_exposure_pnl: f64,
    pub total_fees: f64,
    pub total_slippage_cost: f64,
    pub total_capital_cost: f64,
    pub total_transfer_cost: f64,
    pub total_inventory_rebalance_cost: f64,
    pub total_latency_penalty_cost: f64,
    raw_spread_sum: f64,
    net_spread_sum: f64,
    book_age_sum: i64,
    book_age_count: u64,
    symbol_net_pnl: HashMap<String, f64>,
}

pub trait OpportunitySummaryRecord {
    fn raw_spread_bps(&self) -> f64;
    fn estimated_net_spread_bps(&self) -> f64;
    fn accepted(&self) -> bool;
    fn rejection_reason(&self) -> Option<RejectionReason>;
    fn buy_book_age_ms(&self) -> i64;
    fn sell_book_age_ms(&self) -> i64;
}

impl OpportunitySummaryRecord for OpportunityRecord {
    fn raw_spread_bps(&self) -> f64 {
        self.raw_spread_bps
    }

    fn estimated_net_spread_bps(&self) -> f64 {
        self.estimated_net_spread_bps
    }

    fn accepted(&self) -> bool {
        self.accepted
    }

    fn rejection_reason(&self) -> Option<RejectionReason> {
        self.rejection_reason
    }

    fn buy_book_age_ms(&self) -> i64 {
        self.buy_book_age_ms
    }

    fn sell_book_age_ms(&self) -> i64 {
        self.sell_book_age_ms
    }
}

pub trait TradeSummaryRecord {
    fn symbol(&self) -> &str;
    fn buy_fee(&self) -> f64;
    fn sell_fee(&self) -> f64;
    fn gross_pnl(&self) -> f64;
    fn net_pnl(&self) -> f64;
    fn pnl_category(&self) -> TradePnlCategory {
        TradePnlCategory::Arbitrage
    }
    fn slippage_cost(&self) -> f64 {
        0.0
    }
    fn capital_cost(&self) -> f64 {
        0.0
    }
    fn transfer_cost(&self) -> f64 {
        0.0
    }
    fn inventory_rebalance_cost(&self) -> f64 {
        0.0
    }
    fn latency_penalty_cost(&self) -> f64 {
        0.0
    }
}

impl TradeSummaryRecord for SimulatedTradeRecord {
    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn buy_fee(&self) -> f64 {
        self.buy_fee
    }

    fn sell_fee(&self) -> f64 {
        self.sell_fee
    }

    fn gross_pnl(&self) -> f64 {
        self.gross_pnl
    }

    fn net_pnl(&self) -> f64 {
        self.net_pnl
    }

    fn pnl_category(&self) -> TradePnlCategory {
        self.pnl_category
    }

    fn slippage_cost(&self) -> f64 {
        self.slippage_cost
    }

    fn capital_cost(&self) -> f64 {
        self.capital_cost
    }

    fn transfer_cost(&self) -> f64 {
        self.transfer_cost
    }

    fn inventory_rebalance_cost(&self) -> f64 {
        self.inventory_rebalance_cost
    }

    fn latency_penalty_cost(&self) -> f64 {
        self.latency_penalty_cost
    }
}

impl SummaryReport {
    pub fn record_opportunity(&mut self, opportunity: &(impl OpportunitySummaryRecord + ?Sized)) {
        self.opportunities_detected += 1;
        self.raw_spread_sum += opportunity.raw_spread_bps();
        self.net_spread_sum += opportunity.estimated_net_spread_bps();
        self.book_age_sum += opportunity
            .buy_book_age_ms()
            .max(opportunity.sell_book_age_ms());
        self.book_age_count += 1;
        if opportunity.accepted() {
            self.opportunities_accepted += 1;
        } else {
            self.opportunities_rejected += 1;
            if let Some(reason) = opportunity.rejection_reason() {
                self.record_rejection(reason);
            }
        }
    }

    pub fn record_rejection(&mut self, reason: RejectionReason) {
        *self.rejection_reasons.entry(reason).or_default() += 1;
    }

    pub fn record_trade(&mut self, trade: &(impl TradeSummaryRecord + ?Sized)) {
        self.total_simulated_trades += 1;
        self.total_gross_pnl += trade.gross_pnl();
        self.total_net_pnl += trade.net_pnl();
        self.total_fees += trade.buy_fee() + trade.sell_fee();
        match trade.pnl_category() {
            TradePnlCategory::Arbitrage => {
                self.total_arbitrage_net_pnl += trade.net_pnl();
            }
            TradePnlCategory::InventoryDrift => {
                self.total_inventory_drift_pnl += trade.net_pnl();
                self.total_inventory_recovery_pnl += trade.net_pnl();
            }
            TradePnlCategory::OneSidedExposure => {
                self.total_one_sided_exposure_pnl += trade.net_pnl();
                self.total_inventory_recovery_pnl += trade.net_pnl();
            }
            TradePnlCategory::InventoryRecovery => {
                self.total_inventory_recovery_pnl += trade.net_pnl();
            }
        }
        self.total_slippage_cost += trade.slippage_cost();
        self.total_capital_cost += trade.capital_cost();
        self.total_transfer_cost += trade.transfer_cost();
        self.total_inventory_rebalance_cost += trade.inventory_rebalance_cost();
        self.total_latency_penalty_cost += trade.latency_penalty_cost();
        *self
            .symbol_net_pnl
            .entry(trade.symbol().to_string())
            .or_default() += trade.net_pnl();
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
            "spot_spot_taker_arbitrage report symbols_scanned={} opportunities={} accepted={} rejected={} trades={} gross_pnl={:.6} net_pnl={:.6} arbitrage_net_pnl={:.6} inventory_recovery_pnl={:.6} inventory_drift_pnl={:.6} one_sided_exposure_pnl={:.6} fees={:.6} slippage_cost={:.6} capital_cost={:.6} transfer_cost={:.6} inventory_rebalance_cost={:.6} latency_penalty_cost={:.6} avg_raw_bps={:.3} avg_net_bps={:.3} avg_book_age_ms={:.1} rejections={:?}",
            self.symbols_scanned,
            self.opportunities_detected,
            self.opportunities_accepted,
            self.opportunities_rejected,
            self.total_simulated_trades,
            self.total_gross_pnl,
            self.total_net_pnl,
            self.total_arbitrage_net_pnl,
            self.total_inventory_recovery_pnl,
            self.total_inventory_drift_pnl,
            self.total_one_sided_exposure_pnl,
            self.total_fees,
            self.total_slippage_cost,
            self.total_capital_cost,
            self.total_transfer_cost,
            self.total_inventory_rebalance_cost,
            self.total_latency_penalty_cost,
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
