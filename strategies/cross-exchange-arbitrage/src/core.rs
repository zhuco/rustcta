use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExchangeId(pub String);

impl ExchangeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into().trim().to_ascii_lowercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ExchangeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CanonicalSymbol {
    pub base: String,
    pub quote: String,
}

impl CanonicalSymbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into().trim().to_ascii_uppercase(),
            quote: quote.into().trim().to_ascii_uppercase(),
        }
    }

    pub fn as_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

impl std::fmt::Display for CanonicalSymbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.as_pair())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MakerLegKind {
    LongMakerBuy,
    ShortMakerSell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FillInferenceType {
    RealTrade,
    BookInferredFill,
    NotFilled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyRoute {
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_side: OrderSide,
    pub taker_side: OrderSide,
    pub maker_leg_kind: MakerLegKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SimulatedBundleStatus {
    Observing,
    MakerPending,
    MakerFilled,
    Hedging,
    OpenSimulated,
    ClosingSimulated,
    Closed,
    OrphanLeg,
    RiskStopped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimulatedBundleState {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub status: SimulatedBundleStatus,
    pub route: StrategyRoute,
    pub target_notional_usdt: f64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeeRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeFeeRates {
    pub maker: f64,
    pub taker: f64,
}

impl ExchangeFeeRates {
    pub fn rate(self, role: FeeRole) -> f64 {
        match role {
            FeeRole::Maker => self.maker,
            FeeRole::Taker => self.taker,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeBreakdown {
    pub maker_entry_fee: f64,
    pub taker_hedge_fee: f64,
    pub maker_close_fee: f64,
    pub taker_close_fee: f64,
    pub emergency_close_fee: f64,
}

impl FeeBreakdown {
    pub fn open_fee(&self) -> f64 {
        self.maker_entry_fee + self.taker_hedge_fee
    }

    pub fn normal_close_fee(&self) -> f64 {
        self.maker_close_fee + self.taker_close_fee
    }

    pub fn total_normal_fee(&self) -> f64 {
        self.open_fee() + self.normal_close_fee()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModel {
    default: ExchangeFeeRates,
    per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
}

impl FeeModel {
    pub fn new(
        default: ExchangeFeeRates,
        per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
    ) -> Self {
        Self {
            default,
            per_exchange,
        }
    }

    pub fn rates_for(&self, exchange: &ExchangeId) -> ExchangeFeeRates {
        self.per_exchange
            .get(exchange)
            .copied()
            .unwrap_or(self.default)
    }

    pub fn rate(&self, exchange: &ExchangeId, role: FeeRole) -> f64 {
        self.rates_for(exchange).rate(role)
    }

    pub fn fee_amount(&self, exchange: &ExchangeId, role: FeeRole, notional_usdt: f64) -> f64 {
        notional_usdt * self.rate(exchange, role)
    }

    pub fn estimate_maker_taker_round_trip(
        &self,
        maker_exchange: &ExchangeId,
        taker_exchange: &ExchangeId,
        notional_usdt: f64,
    ) -> FeeBreakdown {
        FeeBreakdown {
            maker_entry_fee: self.fee_amount(maker_exchange, FeeRole::Maker, notional_usdt),
            taker_hedge_fee: self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
            maker_close_fee: self.fee_amount(maker_exchange, FeeRole::Maker, notional_usdt),
            taker_close_fee: self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
            emergency_close_fee: self.fee_amount(maker_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(taker_exchange, FeeRole::Taker, notional_usdt),
        }
    }

    pub fn estimate_dual_taker_round_trip(
        &self,
        long_exchange: &ExchangeId,
        short_exchange: &ExchangeId,
        notional_usdt: f64,
    ) -> FeeBreakdown {
        FeeBreakdown {
            maker_entry_fee: 0.0,
            taker_hedge_fee: self.fee_amount(long_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(short_exchange, FeeRole::Taker, notional_usdt),
            maker_close_fee: 0.0,
            taker_close_fee: self.fee_amount(long_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(short_exchange, FeeRole::Taker, notional_usdt),
            emergency_close_fee: self.fee_amount(long_exchange, FeeRole::Taker, notional_usdt)
                + self.fee_amount(short_exchange, FeeRole::Taker, notional_usdt),
        }
    }
}

impl Default for FeeModel {
    fn default() -> Self {
        Self::new(
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            HashMap::new(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingEstimate {
    pub long_leg_funding: f64,
    pub short_leg_funding: f64,
    pub net_funding: f64,
    pub net_funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub minutes_to_funding: Option<i64>,
    pub dangerous: bool,
    pub near_negative_settlement: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingModel {
    pub max_adverse_funding_rate: f64,
}

impl FundingModel {
    pub fn new(max_adverse_funding_rate: f64) -> Self {
        Self {
            max_adverse_funding_rate,
        }
    }

    pub fn leg_funding(position_side: PositionSide, notional_usdt: f64, funding_rate: f64) -> f64 {
        let funding = notional_usdt.abs() * funding_rate;
        match position_side {
            PositionSide::Long => -funding,
            PositionSide::Short => funding,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn estimate_pair_with_timing(
        &self,
        long_notional_usdt: f64,
        long_funding_rate: f64,
        long_next_funding_time: Option<DateTime<Utc>>,
        short_notional_usdt: f64,
        short_funding_rate: f64,
        short_next_funding_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        no_open_before_funding_mins: i64,
    ) -> FundingEstimate {
        let long_leg_funding =
            Self::leg_funding(PositionSide::Long, long_notional_usdt, long_funding_rate);
        let short_leg_funding =
            Self::leg_funding(PositionSide::Short, short_notional_usdt, short_funding_rate);
        let net_funding = long_leg_funding + short_leg_funding;
        let base_notional = long_notional_usdt
            .abs()
            .max(short_notional_usdt.abs())
            .max(1.0);
        let net_funding_rate = net_funding / base_notional;
        let next_funding_time =
            earliest_future(long_next_funding_time, short_next_funding_time, now);
        let minutes_to_funding =
            next_funding_time.map(|time| time.signed_duration_since(now).num_minutes().max(0));
        let near_negative_settlement = net_funding < 0.0
            && minutes_to_funding
                .map(|mins| mins <= no_open_before_funding_mins.max(0))
                .unwrap_or(false);

        FundingEstimate {
            long_leg_funding,
            short_leg_funding,
            net_funding,
            net_funding_rate,
            next_funding_time,
            minutes_to_funding,
            dangerous: net_funding_rate < -self.max_adverse_funding_rate,
            near_negative_settlement,
        }
    }

    pub fn estimate_pair(
        &self,
        long_notional_usdt: f64,
        long_funding_rate: f64,
        short_notional_usdt: f64,
        short_funding_rate: f64,
    ) -> FundingEstimate {
        self.estimate_pair_with_timing(
            long_notional_usdt,
            long_funding_rate,
            None,
            short_notional_usdt,
            short_funding_rate,
            None,
            Utc::now(),
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn settle_leg(
        bundle_id: impl Into<String>,
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        notional_usdt: f64,
        funding_rate: f64,
        mark_price: Option<f64>,
        settled_at: DateTime<Utc>,
    ) -> FundingSettlement {
        FundingSettlement {
            bundle_id: bundle_id.into(),
            exchange,
            canonical_symbol,
            position_side,
            notional_usdt: notional_usdt.abs(),
            funding_rate,
            funding_pnl_usdt: Self::leg_funding(position_side, notional_usdt, funding_rate),
            mark_price,
            settled_at,
        }
    }
}

impl Default for FundingModel {
    fn default() -> Self {
        Self::new(0.001)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSettlement {
    pub bundle_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub position_side: PositionSide,
    pub notional_usdt: f64,
    pub funding_rate: f64,
    pub funding_pnl_usdt: f64,
    pub mark_price: Option<f64>,
    pub settled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FundingSettlementLedger {
    settlements: Vec<FundingSettlement>,
}

impl FundingSettlementLedger {
    pub fn record(&mut self, settlement: FundingSettlement) {
        self.settlements.push(settlement);
    }

    pub fn settlements(&self) -> &[FundingSettlement] {
        &self.settlements
    }

    pub fn total_pnl_usdt(&self) -> f64 {
        self.settlements
            .iter()
            .map(|settlement| settlement.funding_pnl_usdt)
            .sum()
    }

    pub fn total_pnl_for_bundle(&self, bundle_id: &str) -> f64 {
        self.settlements
            .iter()
            .filter(|settlement| settlement.bundle_id == bundle_id)
            .map(|settlement| settlement.funding_pnl_usdt)
            .sum()
    }
}

fn earliest_future(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    [left, right]
        .into_iter()
        .flatten()
        .filter(|time| *time >= now)
        .min()
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DualTakerArbitrageConfig {
    pub target_notional_usdt: f64,
    pub min_open_spread_pct: f64,
    pub max_open_spread_pct: f64,
    pub taker_slippage_pct: f64,
    pub close_min_net_profit_pct: f64,
    pub expected_close_spread_pct: f64,
    pub top_of_book_capacity_ratio: f64,
    pub orderbook_stale_ms: u64,
    pub min_orderbook_levels: usize,
    pub single_leg_timeout_ms: u64,
    pub max_consecutive_single_leg_fills: u32,
    pub max_positions_per_exchange: usize,
    pub max_active_bundles_per_symbol: usize,
    pub symbol_cooldown_secs: i64,
    pub max_hold_secs: i64,
}

impl Default for DualTakerArbitrageConfig {
    fn default() -> Self {
        Self {
            target_notional_usdt: 5.5,
            min_open_spread_pct: 0.005,
            max_open_spread_pct: 0.05,
            taker_slippage_pct: 0.0005,
            close_min_net_profit_pct: 0.0005,
            expected_close_spread_pct: 0.001,
            top_of_book_capacity_ratio: 0.8,
            orderbook_stale_ms: 500,
            min_orderbook_levels: 1,
            single_leg_timeout_ms: 600,
            max_consecutive_single_leg_fills: 3,
            max_positions_per_exchange: 10,
            max_active_bundles_per_symbol: 1,
            symbol_cooldown_secs: 300,
            max_hold_secs: 86_400,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantityUnit {
    #[default]
    Base,
    Contracts,
}

fn default_contract_size() -> f64 {
    1.0
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolPrecision {
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_quantity: f64,
    pub min_notional_usdt: f64,
    #[serde(default)]
    pub quantity_unit: QuantityUnit,
    #[serde(default = "default_contract_size")]
    pub contract_size: f64,
}

impl Default for SymbolPrecision {
    fn default() -> Self {
        Self {
            price_tick: 0.0,
            quantity_step: 0.0,
            min_quantity: 0.0,
            min_notional_usdt: 0.0,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
        }
    }
}

impl SymbolPrecision {
    pub fn effective_contract_size(self) -> f64 {
        match self.quantity_unit {
            QuantityUnit::Base => 1.0,
            QuantityUnit::Contracts => self.contract_size.max(0.0),
        }
    }

    pub fn base_quantity_from_order_quantity(self, order_quantity: f64) -> f64 {
        match self.quantity_unit {
            QuantityUnit::Base => order_quantity.max(0.0),
            QuantityUnit::Contracts => order_quantity.max(0.0) * self.effective_contract_size(),
        }
    }

    pub fn order_quantity_from_base_quantity(self, base_quantity: f64) -> f64 {
        match self.quantity_unit {
            QuantityUnit::Base => base_quantity.max(0.0),
            QuantityUnit::Contracts => {
                let contract_size = self.effective_contract_size();
                if contract_size <= 0.0 {
                    0.0
                } else {
                    base_quantity.max(0.0) / contract_size
                }
            }
        }
    }

    pub fn normalized_order_quantity_from_base(self, base_quantity: f64) -> f64 {
        floor_to_step(
            self.order_quantity_from_base_quantity(base_quantity),
            self.quantity_step,
        )
    }

    pub fn normalized_base_quantity(self, base_quantity: f64) -> f64 {
        self.base_quantity_from_order_quantity(
            self.normalized_order_quantity_from_base(base_quantity),
        )
    }

    pub fn min_base_quantity(self) -> f64 {
        self.base_quantity_from_order_quantity(self.min_quantity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrecisionKey {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
}

impl PrecisionKey {
    pub fn new(exchange: ExchangeId, canonical_symbol: CanonicalSymbol) -> Self {
        Self {
            exchange,
            canonical_symbol,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PrecisionRegistry {
    rules: HashMap<PrecisionKey, SymbolPrecision>,
}

impl PrecisionRegistry {
    pub fn insert(
        &mut self,
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        precision: SymbolPrecision,
    ) {
        self.rules
            .insert(PrecisionKey::new(exchange, canonical_symbol), precision);
    }

    pub fn get(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> SymbolPrecision {
        self.rules
            .get(&PrecisionKey::new(
                exchange.clone(),
                canonical_symbol.clone(),
            ))
            .copied()
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookTop {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub best_bid_price: f64,
    pub best_bid_quantity: f64,
    pub best_ask_price: f64,
    pub best_ask_quantity: f64,
    pub levels: usize,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub latency_ms: Option<u64>,
}

impl OrderBookTop {
    pub fn is_valid(&self, min_levels: usize) -> bool {
        self.levels >= min_levels.max(1)
            && self.best_bid_price > 0.0
            && self.best_ask_price > 0.0
            && self.best_bid_price < self.best_ask_price
            && self.best_bid_quantity > 0.0
            && self.best_ask_quantity > 0.0
    }

    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.received_at)
            .num_milliseconds()
    }

    pub fn is_fresh(&self, now: DateTime<Utc>, stale_after_ms: u64) -> bool {
        self.age_ms(now) <= stale_after_ms as i64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeStartupReadiness {
    pub exchange: ExchangeId,
    pub expected_symbols: usize,
    pub market_data_subscribed_symbols: usize,
    pub user_stream_subscribed: bool,
    pub server_time_synced: bool,
    pub symbol_rules_loaded: bool,
}

impl ExchangeStartupReadiness {
    pub fn ready(&self) -> bool {
        self.expected_symbols > 0
            && self.market_data_subscribed_symbols >= self.expected_symbols
            && self.user_stream_subscribed
            && self.server_time_synced
            && self.symbol_rules_loaded
    }

    pub fn missing_gates(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.expected_symbols == 0 || self.market_data_subscribed_symbols < self.expected_symbols
        {
            missing.push("market_data_subscription");
        }
        if !self.user_stream_subscribed {
            missing.push("user_stream_subscription");
        }
        if !self.server_time_synced {
            missing.push("server_time_sync");
        }
        if !self.symbol_rules_loaded {
            missing.push("symbol_rules");
        }
        missing
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupReadiness {
    pub exchanges: Vec<ExchangeStartupReadiness>,
}

impl StartupReadiness {
    pub fn can_evaluate_opportunities(&self) -> bool {
        !self.exchanges.is_empty() && self.exchanges.iter().all(ExchangeStartupReadiness::ready)
    }

    pub fn blocked_reasons(&self) -> BTreeMap<String, Vec<&'static str>> {
        self.exchanges
            .iter()
            .filter_map(|exchange| {
                let missing = exchange.missing_gates();
                if missing.is_empty() {
                    None
                } else {
                    Some((exchange.exchange.to_string(), missing))
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolatilityRankDirection {
    Gainer,
    Loser,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolatilityRankTicker {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub direction: VolatilityRankDirection,
    pub change_pct: f64,
    pub quote_volume_usdt: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct VolatilityUniverseConfig {
    pub enabled: bool,
    pub top_gainers_per_exchange: usize,
    pub top_losers_per_exchange: usize,
    pub min_abs_change_pct: f64,
    pub min_quote_volume_usdt: f64,
    pub refresh_secs: u64,
    pub max_dynamic_symbols: usize,
    pub monitor_orderbook: bool,
}

impl Default for VolatilityUniverseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            top_gainers_per_exchange: 20,
            top_losers_per_exchange: 20,
            min_abs_change_pct: 0.05,
            min_quote_volume_usdt: 1_000_000.0,
            refresh_secs: 60,
            max_dynamic_symbols: 120,
            monitor_orderbook: true,
        }
    }
}

pub fn select_high_volatility_symbols(
    tickers: &[VolatilityRankTicker],
    config: &VolatilityUniverseConfig,
    excluded_bases: &[String],
    excluded_symbols: &[String],
) -> Vec<String> {
    if !config.enabled || config.max_dynamic_symbols == 0 {
        return Vec::new();
    }

    let excluded_bases: Vec<_> = excluded_bases
        .iter()
        .map(|base| base.trim().to_ascii_uppercase())
        .collect();
    let excluded_symbols: Vec<_> = excluded_symbols
        .iter()
        .map(|symbol| normalize_symbol_pair(symbol))
        .collect();
    let mut selected = Vec::new();
    let mut by_exchange_direction: BTreeMap<(String, VolatilityRankDirection), Vec<_>> =
        BTreeMap::new();

    for ticker in tickers.iter().filter(|ticker| {
        ticker.change_pct.abs() >= config.min_abs_change_pct
            && ticker.quote_volume_usdt >= config.min_quote_volume_usdt
    }) {
        by_exchange_direction
            .entry((ticker.exchange.to_string(), ticker.direction))
            .or_default()
            .push(ticker);
    }

    for ((_, direction), mut rows) in by_exchange_direction {
        rows.sort_by(|left, right| {
            right
                .change_pct
                .abs()
                .partial_cmp(&left.change_pct.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let limit = match direction {
            VolatilityRankDirection::Gainer => config.top_gainers_per_exchange,
            VolatilityRankDirection::Loser => config.top_losers_per_exchange,
        };
        let mut accepted_for_group = 0;
        for ticker in rows {
            let symbol = ticker.canonical_symbol.as_pair();
            let base = ticker.canonical_symbol.base.to_ascii_uppercase();
            if excluded_bases.contains(&base) || excluded_symbols.contains(&symbol) {
                continue;
            }
            if !selected.contains(&symbol) {
                selected.push(symbol);
            }
            if selected.len() >= config.max_dynamic_symbols {
                return selected;
            }
            accepted_for_group += 1;
            if accepted_for_group >= limit {
                break;
            }
        }
    }

    selected
}

fn normalize_symbol_pair(symbol: &str) -> String {
    let symbol = symbol.trim().to_ascii_uppercase().replace(['-', '_'], "/");
    if symbol.is_empty() || symbol.contains('/') {
        return symbol;
    }
    for quote in ["USDT", "USDC", "USD"] {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            let base = &symbol[..symbol.len() - quote.len()];
            return format!("{base}/{quote}");
        }
    }
    symbol
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TakerOrderRole {
    OpenLong,
    OpenShort,
    CloseLong,
    CloseShort,
    EmergencyCloseLong,
    EmergencyCloseShort,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakerOrderDraft {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub side: OrderSide,
    pub base_quantity: f64,
    pub quantity: f64,
    pub quantity_unit: QuantityUnit,
    pub contract_size: f64,
    pub reference_price: f64,
    pub worst_acceptable_price: f64,
    pub reduce_only: bool,
    pub role: TakerOrderRole,
}

impl TakerOrderDraft {
    pub fn planned_notional_usdt(&self) -> f64 {
        self.base_quantity.abs() * self.reference_price
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakerFillAudit {
    pub bundle_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub role: TakerOrderRole,
    pub side: OrderSide,
    pub quantity_unit: QuantityUnit,
    pub contract_size: f64,
    pub planned_price: f64,
    pub actual_fill_price: f64,
    pub planned_base_quantity: f64,
    pub actual_base_quantity: f64,
    pub planned_order_quantity: f64,
    pub actual_order_quantity: f64,
    pub planned_notional_usdt: f64,
    pub actual_notional_usdt: f64,
    pub slippage_pct: f64,
    pub fee_usdt: f64,
}

impl TakerFillAudit {
    pub fn from_order_fill(
        bundle_id: impl Into<String>,
        order: &TakerOrderDraft,
        actual_order_quantity: f64,
        actual_fill_price: f64,
        taker_fee_rate: f64,
    ) -> Self {
        let precision = SymbolPrecision {
            price_tick: 0.0,
            quantity_step: 0.0,
            min_quantity: 0.0,
            min_notional_usdt: 0.0,
            quantity_unit: order.quantity_unit,
            contract_size: order.contract_size,
        };
        let actual_base_quantity =
            precision.base_quantity_from_order_quantity(actual_order_quantity.abs());
        let planned_notional_usdt = order.planned_notional_usdt();
        let actual_notional_usdt = actual_base_quantity * actual_fill_price;
        let slippage_pct = match order.side {
            OrderSide::Buy => {
                (actual_fill_price - order.reference_price) / order.reference_price.max(1.0)
            }
            OrderSide::Sell => {
                (order.reference_price - actual_fill_price) / order.reference_price.max(1.0)
            }
        };

        Self {
            bundle_id: bundle_id.into(),
            exchange: order.exchange.clone(),
            canonical_symbol: order.canonical_symbol.clone(),
            role: order.role,
            side: order.side,
            quantity_unit: order.quantity_unit,
            contract_size: order.contract_size,
            planned_price: order.reference_price,
            actual_fill_price,
            planned_base_quantity: order.base_quantity,
            actual_base_quantity,
            planned_order_quantity: order.quantity,
            actual_order_quantity,
            planned_notional_usdt,
            actual_notional_usdt,
            slippage_pct,
            fee_usdt: actual_notional_usdt * taker_fee_rate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DualTakerFeeEstimate {
    pub open_long_fee_usdt: f64,
    pub open_short_fee_usdt: f64,
    pub close_long_fee_usdt: f64,
    pub close_short_fee_usdt: f64,
}

impl DualTakerFeeEstimate {
    pub fn open_fee_usdt(&self) -> f64 {
        self.open_long_fee_usdt + self.open_short_fee_usdt
    }

    pub fn close_fee_usdt(&self) -> f64 {
        self.close_long_fee_usdt + self.close_short_fee_usdt
    }

    pub fn total_fee_usdt(&self) -> f64 {
        self.open_fee_usdt() + self.close_fee_usdt()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DualTakerOpenOpportunity {
    pub opportunity_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub long_entry_price: f64,
    pub short_entry_price: f64,
    pub spread_pct: f64,
    pub quantity: f64,
    pub long_notional_usdt: f64,
    pub short_notional_usdt: f64,
    pub executable_top_depth_usdt: f64,
    pub top_of_book_capacity_ratio: f64,
    pub estimated_open_fee_usdt: f64,
    pub estimated_round_trip_fee_usdt: f64,
    pub expected_close_spread_pct: f64,
    pub expected_gross_pnl_usdt: f64,
    pub expected_net_pnl_usdt: f64,
    pub expected_net_profit_pct: f64,
    pub submit_parallel: bool,
    pub orders: Vec<TakerOrderDraft>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenArbitragePosition {
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub quantity: f64,
    pub long_entry_price: f64,
    pub short_entry_price: f64,
    pub opened_at: DateTime<Utc>,
}

impl OpenArbitragePosition {
    pub fn open_long_notional_usdt(&self) -> f64 {
        self.quantity.abs() * self.long_entry_price
    }

    pub fn open_short_notional_usdt(&self) -> f64 {
        self.quantity.abs() * self.short_entry_price
    }

    pub fn base_notional_usdt(&self) -> f64 {
        self.open_long_notional_usdt()
            .max(self.open_short_notional_usdt())
            .max(1.0)
    }

    pub fn held_secs(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.opened_at).num_seconds()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    ProfitTarget,
    MaxHoldTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DualTakerCloseEvaluation {
    pub bundle_id: String,
    pub should_close: bool,
    pub reason: Option<CloseReason>,
    pub long_close_price: f64,
    pub short_close_price: f64,
    pub gross_pnl_usdt: f64,
    pub total_fee_usdt: f64,
    pub net_pnl_usdt: f64,
    pub net_profit_pct: f64,
    pub submit_parallel: bool,
    pub orders: Vec<TakerOrderDraft>,
}

pub fn evaluate_dual_taker_open_opportunities(
    books: &[OrderBookTop],
    precisions: &PrecisionRegistry,
    fee_model: &FeeModel,
    config: &DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> Vec<DualTakerOpenOpportunity> {
    let fresh_books: Vec<_> = books
        .iter()
        .filter(|book| {
            book.is_valid(config.min_orderbook_levels)
                && book.is_fresh(now, config.orderbook_stale_ms)
        })
        .collect();
    let mut opportunities = Vec::new();

    for long_book in &fresh_books {
        for short_book in &fresh_books {
            if long_book.exchange == short_book.exchange
                || long_book.canonical_symbol != short_book.canonical_symbol
            {
                continue;
            }
            if short_book.best_bid_price <= long_book.best_ask_price {
                continue;
            }

            let spread_pct =
                (short_book.best_bid_price - long_book.best_ask_price) / long_book.best_ask_price;
            if spread_pct < config.min_open_spread_pct || spread_pct > config.max_open_spread_pct {
                continue;
            }

            let long_precision = precisions.get(&long_book.exchange, &long_book.canonical_symbol);
            let short_precision =
                precisions.get(&short_book.exchange, &short_book.canonical_symbol);
            let Some(quantity_plan) = shared_open_quantity(
                long_book,
                short_book,
                long_precision,
                short_precision,
                config.target_notional_usdt,
                config.top_of_book_capacity_ratio,
            ) else {
                continue;
            };
            let quantity = quantity_plan.base_quantity;

            let long_notional_usdt = quantity * long_book.best_ask_price;
            let short_notional_usdt = quantity * short_book.best_bid_price;
            let estimated_open_fee_usdt =
                fee_model.fee_amount(&long_book.exchange, FeeRole::Taker, long_notional_usdt)
                    + fee_model.fee_amount(
                        &short_book.exchange,
                        FeeRole::Taker,
                        short_notional_usdt,
                    );
            let expected_close_spread_pct = config.expected_close_spread_pct.max(0.0);
            let expected_spread_capture_pct = (spread_pct - expected_close_spread_pct).max(0.0);
            let expected_gross_pnl_usdt =
                quantity * long_book.best_ask_price * expected_spread_capture_pct;
            let estimated_round_trip_fee_usdt = estimated_open_fee_usdt
                + fee_model.fee_amount(&long_book.exchange, FeeRole::Taker, long_notional_usdt)
                + fee_model.fee_amount(&short_book.exchange, FeeRole::Taker, short_notional_usdt);
            let expected_net_pnl_usdt = expected_gross_pnl_usdt - estimated_round_trip_fee_usdt;
            let expected_net_profit_pct =
                expected_net_pnl_usdt / long_notional_usdt.max(short_notional_usdt).max(1.0);
            let symbol = long_book.canonical_symbol.clone();
            let opportunity_id = format!(
                "{}:{}:{}:{}",
                symbol.as_pair(),
                long_book.exchange,
                short_book.exchange,
                long_book
                    .received_at
                    .timestamp_millis()
                    .max(short_book.received_at.timestamp_millis())
            );
            let orders = vec![
                taker_order_draft(
                    long_book.exchange.clone(),
                    symbol.clone(),
                    OrderSide::Buy,
                    quantity,
                    long_book.best_ask_price,
                    false,
                    TakerOrderRole::OpenLong,
                    config.taker_slippage_pct,
                    long_precision,
                ),
                taker_order_draft(
                    short_book.exchange.clone(),
                    symbol.clone(),
                    OrderSide::Sell,
                    quantity,
                    short_book.best_bid_price,
                    false,
                    TakerOrderRole::OpenShort,
                    config.taker_slippage_pct,
                    short_precision,
                ),
            ];

            opportunities.push(DualTakerOpenOpportunity {
                opportunity_id,
                canonical_symbol: symbol,
                long_exchange: long_book.exchange.clone(),
                short_exchange: short_book.exchange.clone(),
                long_entry_price: long_book.best_ask_price,
                short_entry_price: short_book.best_bid_price,
                spread_pct,
                quantity,
                long_notional_usdt,
                short_notional_usdt,
                executable_top_depth_usdt: quantity_plan.executable_depth_usdt,
                top_of_book_capacity_ratio: config.top_of_book_capacity_ratio,
                estimated_open_fee_usdt,
                estimated_round_trip_fee_usdt,
                expected_close_spread_pct,
                expected_gross_pnl_usdt,
                expected_net_pnl_usdt,
                expected_net_profit_pct,
                submit_parallel: true,
                orders,
            });
        }
    }

    opportunities.sort_by(|left, right| {
        right
            .spread_pct
            .partial_cmp(&left.spread_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    opportunities
}

pub fn evaluate_ready_dual_taker_open_opportunities(
    readiness: &StartupReadiness,
    books: &[OrderBookTop],
    precisions: &PrecisionRegistry,
    fee_model: &FeeModel,
    config: &DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> Vec<DualTakerOpenOpportunity> {
    if !readiness.can_evaluate_opportunities() {
        return Vec::new();
    }
    evaluate_dual_taker_open_opportunities(books, precisions, fee_model, config, now)
}

pub fn filter_open_opportunities_by_risk(
    opportunities: Vec<DualTakerOpenOpportunity>,
    risk_state: &ArbitrageRiskState,
    config: &DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> Vec<DualTakerOpenOpportunity> {
    opportunities
        .into_iter()
        .filter(|opportunity| {
            risk_state
                .can_open(
                    &opportunity.canonical_symbol,
                    &opportunity.long_exchange,
                    &opportunity.short_exchange,
                    config,
                    now,
                )
                .is_ok()
        })
        .collect()
}

pub fn evaluate_dual_taker_close(
    position: &OpenArbitragePosition,
    long_book: &OrderBookTop,
    short_book: &OrderBookTop,
    precisions: &PrecisionRegistry,
    fee_model: &FeeModel,
    config: &DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> Option<DualTakerCloseEvaluation> {
    if long_book.exchange != position.long_exchange
        || short_book.exchange != position.short_exchange
        || long_book.canonical_symbol != position.canonical_symbol
        || short_book.canonical_symbol != position.canonical_symbol
        || !long_book.is_valid(config.min_orderbook_levels)
        || !short_book.is_valid(config.min_orderbook_levels)
        || !long_book.is_fresh(now, config.orderbook_stale_ms)
        || !short_book.is_fresh(now, config.orderbook_stale_ms)
    {
        return None;
    }

    let quantity = position.quantity.abs();
    let long_close_price = long_book.best_bid_price;
    let short_close_price = short_book.best_ask_price;
    let gross_pnl_usdt = quantity * (long_close_price - position.long_entry_price)
        + quantity * (position.short_entry_price - short_close_price);

    let fees = DualTakerFeeEstimate {
        open_long_fee_usdt: fee_model.fee_amount(
            &position.long_exchange,
            FeeRole::Taker,
            position.open_long_notional_usdt(),
        ),
        open_short_fee_usdt: fee_model.fee_amount(
            &position.short_exchange,
            FeeRole::Taker,
            position.open_short_notional_usdt(),
        ),
        close_long_fee_usdt: fee_model.fee_amount(
            &position.long_exchange,
            FeeRole::Taker,
            quantity * long_close_price,
        ),
        close_short_fee_usdt: fee_model.fee_amount(
            &position.short_exchange,
            FeeRole::Taker,
            quantity * short_close_price,
        ),
    };
    let total_fee_usdt = fees.total_fee_usdt();
    let net_pnl_usdt = gross_pnl_usdt - total_fee_usdt;
    let net_profit_pct = net_pnl_usdt / position.base_notional_usdt();
    let max_hold_expired = position.held_secs(now) >= config.max_hold_secs;
    let profit_target_met = net_profit_pct >= config.close_min_net_profit_pct;
    let reason = if profit_target_met {
        Some(CloseReason::ProfitTarget)
    } else if max_hold_expired {
        Some(CloseReason::MaxHoldTime)
    } else {
        None
    };

    let long_precision = precisions.get(&position.long_exchange, &position.canonical_symbol);
    let short_precision = precisions.get(&position.short_exchange, &position.canonical_symbol);
    let orders = vec![
        taker_order_draft(
            position.long_exchange.clone(),
            position.canonical_symbol.clone(),
            OrderSide::Sell,
            quantity,
            long_close_price,
            true,
            TakerOrderRole::CloseLong,
            config.taker_slippage_pct,
            long_precision,
        ),
        taker_order_draft(
            position.short_exchange.clone(),
            position.canonical_symbol.clone(),
            OrderSide::Buy,
            quantity,
            short_close_price,
            true,
            TakerOrderRole::CloseShort,
            config.taker_slippage_pct,
            short_precision,
        ),
    ];

    Some(DualTakerCloseEvaluation {
        bundle_id: position.bundle_id.clone(),
        should_close: reason.is_some(),
        reason,
        long_close_price,
        short_close_price,
        gross_pnl_usdt,
        total_fee_usdt,
        net_pnl_usdt,
        net_profit_pct,
        submit_parallel: true,
        orders,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairedTakerFillState {
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub quantity: f64,
    pub submitted_at: DateTime<Utc>,
    pub long_filled: bool,
    pub short_filled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SingleLegDecision {
    pub emergency_close_required: bool,
    pub stop_strategy: bool,
    pub consecutive_single_leg_fills: u32,
    pub close_order: Option<TakerOrderDraft>,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingleLegGuard {
    pub consecutive_single_leg_fills: u32,
}

impl SingleLegGuard {
    pub fn evaluate(
        &mut self,
        fill_state: &PairedTakerFillState,
        config: &DualTakerArbitrageConfig,
        reference_price: f64,
        precision: SymbolPrecision,
        now: DateTime<Utc>,
    ) -> SingleLegDecision {
        let exactly_one_filled = fill_state.long_filled ^ fill_state.short_filled;
        if fill_state.long_filled && fill_state.short_filled {
            self.consecutive_single_leg_fills = 0;
            return SingleLegDecision {
                emergency_close_required: false,
                stop_strategy: false,
                consecutive_single_leg_fills: 0,
                close_order: None,
                warning: None,
            };
        }
        if !exactly_one_filled
            || now
                .signed_duration_since(fill_state.submitted_at)
                .num_milliseconds()
                < config.single_leg_timeout_ms as i64
        {
            return SingleLegDecision {
                emergency_close_required: false,
                stop_strategy: false,
                consecutive_single_leg_fills: self.consecutive_single_leg_fills,
                close_order: None,
                warning: None,
            };
        }

        self.consecutive_single_leg_fills += 1;
        let close_order = if fill_state.long_filled {
            taker_order_draft(
                fill_state.long_exchange.clone(),
                fill_state.canonical_symbol.clone(),
                OrderSide::Sell,
                fill_state.quantity,
                reference_price,
                true,
                TakerOrderRole::EmergencyCloseLong,
                config.taker_slippage_pct,
                precision,
            )
        } else {
            taker_order_draft(
                fill_state.short_exchange.clone(),
                fill_state.canonical_symbol.clone(),
                OrderSide::Buy,
                fill_state.quantity,
                reference_price,
                true,
                TakerOrderRole::EmergencyCloseShort,
                config.taker_slippage_pct,
                precision,
            )
        };

        let stop_strategy =
            self.consecutive_single_leg_fills >= config.max_consecutive_single_leg_fills;
        SingleLegDecision {
            emergency_close_required: true,
            stop_strategy,
            consecutive_single_leg_fills: self.consecutive_single_leg_fills,
            close_order: Some(close_order),
            warning: Some(format!(
                "single leg fill detected for bundle {}; emergency close required",
                fill_state.bundle_id
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenBlockReason {
    StrategyHalted,
    SymbolAlreadyActive,
    SymbolCoolingDown,
    ExchangePositionLimit,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageRiskState {
    pub open_positions: BTreeMap<String, OpenArbitragePosition>,
    pub symbol_cooldowns: BTreeMap<String, DateTime<Utc>>,
    pub strategy_halted: bool,
}

impl ArbitrageRiskState {
    pub fn can_open(
        &self,
        symbol: &CanonicalSymbol,
        long_exchange: &ExchangeId,
        short_exchange: &ExchangeId,
        config: &DualTakerArbitrageConfig,
        now: DateTime<Utc>,
    ) -> Result<(), OpenBlockReason> {
        if self.strategy_halted {
            return Err(OpenBlockReason::StrategyHalted);
        }
        let symbol_key = symbol.as_pair();
        if self
            .open_positions
            .values()
            .filter(|position| position.canonical_symbol == *symbol)
            .count()
            >= config.max_active_bundles_per_symbol
        {
            return Err(OpenBlockReason::SymbolAlreadyActive);
        }
        if self
            .symbol_cooldowns
            .get(&symbol_key)
            .is_some_and(|cooldown_until| *cooldown_until > now)
        {
            return Err(OpenBlockReason::SymbolCoolingDown);
        }
        if self.exchange_position_count(long_exchange) >= config.max_positions_per_exchange
            || self.exchange_position_count(short_exchange) >= config.max_positions_per_exchange
        {
            return Err(OpenBlockReason::ExchangePositionLimit);
        }
        Ok(())
    }

    pub fn record_open(&mut self, position: OpenArbitragePosition) {
        self.open_positions
            .insert(position.bundle_id.clone(), position);
    }

    pub fn record_close(
        &mut self,
        bundle_id: &str,
        closed_at: DateTime<Utc>,
        cooldown_secs: i64,
    ) -> Option<OpenArbitragePosition> {
        let position = self.open_positions.remove(bundle_id)?;
        self.symbol_cooldowns.insert(
            position.canonical_symbol.as_pair(),
            closed_at + chrono::Duration::seconds(cooldown_secs.max(0)),
        );
        Some(position)
    }

    pub fn exchange_position_count(&self, exchange: &ExchangeId) -> usize {
        self.open_positions
            .values()
            .filter(|position| {
                &position.long_exchange == exchange || &position.short_exchange == exchange
            })
            .count()
    }

    pub fn positions_exceeding_hold(
        &self,
        now: DateTime<Utc>,
        max_hold_secs: i64,
    ) -> Vec<&OpenArbitragePosition> {
        self.open_positions
            .values()
            .filter(|position| position.held_secs(now) >= max_hold_secs)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetPosition {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub quantity: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetPositionWarning {
    pub canonical_symbol: CanonicalSymbol,
    pub message: String,
    pub positions: Vec<NetPosition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartupUsdtPosition {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub position_side: PositionSide,
    pub base_quantity: f64,
    pub entry_price: f64,
    pub opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartupSingleLegResolution {
    pub position: StartupUsdtPosition,
    pub close_order: Option<TakerOrderDraft>,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StartupPositionTakeoverPlan {
    pub adopted_positions: Vec<OpenArbitragePosition>,
    pub single_leg_resolutions: Vec<StartupSingleLegResolution>,
}

impl StartupPositionTakeoverPlan {
    pub fn requires_emergency_close(&self) -> bool {
        !self.single_leg_resolutions.is_empty()
    }

    pub fn ready_for_new_opens(&self) -> bool {
        self.single_leg_resolutions.is_empty()
    }
}

pub fn plan_startup_usdt_position_takeover(
    positions: &[StartupUsdtPosition],
    books: &[OrderBookTop],
    precisions: &PrecisionRegistry,
    config: &DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> StartupPositionTakeoverPlan {
    let mut grouped: BTreeMap<String, Vec<StartupUsdtPosition>> = BTreeMap::new();
    for position in positions
        .iter()
        .filter(|position| position.base_quantity.abs() > 0.0)
    {
        grouped
            .entry(position.canonical_symbol.as_pair())
            .or_default()
            .push(position.clone());
    }

    let mut plan = StartupPositionTakeoverPlan::default();
    for (symbol_key, positions) in grouped {
        let longs: Vec<_> = positions
            .iter()
            .filter(|position| position.position_side == PositionSide::Long)
            .collect();
        let shorts: Vec<_> = positions
            .iter()
            .filter(|position| position.position_side == PositionSide::Short)
            .collect();

        if longs.len() == 1
            && shorts.len() == 1
            && (longs[0].base_quantity.abs() - shorts[0].base_quantity.abs()).abs() <= 1e-10
            && longs[0].exchange != shorts[0].exchange
        {
            plan.adopted_positions.push(OpenArbitragePosition {
                bundle_id: format!(
                    "startup:{}:{}:{}",
                    symbol_key, longs[0].exchange, shorts[0].exchange
                ),
                canonical_symbol: longs[0].canonical_symbol.clone(),
                long_exchange: longs[0].exchange.clone(),
                short_exchange: shorts[0].exchange.clone(),
                quantity: longs[0].base_quantity.abs(),
                long_entry_price: longs[0].entry_price,
                short_entry_price: shorts[0].entry_price,
                opened_at: longs[0].opened_at.min(shorts[0].opened_at),
            });
            continue;
        }

        for position in positions {
            let close_order =
                startup_single_leg_close_order(&position, books, precisions, config, now);
            let reason = if close_order.is_some() {
                "single_leg_startup_position_requires_reduce_only_close"
            } else {
                "single_leg_startup_position_missing_fresh_orderbook"
            };
            plan.single_leg_resolutions
                .push(StartupSingleLegResolution {
                    position,
                    close_order,
                    reason: reason.to_string(),
                });
        }
    }

    plan
}

fn startup_single_leg_close_order(
    position: &StartupUsdtPosition,
    books: &[OrderBookTop],
    precisions: &PrecisionRegistry,
    config: &DualTakerArbitrageConfig,
    now: DateTime<Utc>,
) -> Option<TakerOrderDraft> {
    let book = books.iter().find(|book| {
        book.exchange == position.exchange
            && book.canonical_symbol == position.canonical_symbol
            && book.is_valid(config.min_orderbook_levels)
            && book.is_fresh(now, config.orderbook_stale_ms)
    })?;
    let precision = precisions.get(&position.exchange, &position.canonical_symbol);
    match position.position_side {
        PositionSide::Long => Some(taker_order_draft(
            position.exchange.clone(),
            position.canonical_symbol.clone(),
            OrderSide::Sell,
            position.base_quantity.abs(),
            book.best_bid_price,
            true,
            TakerOrderRole::EmergencyCloseLong,
            config.taker_slippage_pct,
            precision,
        )),
        PositionSide::Short => Some(taker_order_draft(
            position.exchange.clone(),
            position.canonical_symbol.clone(),
            OrderSide::Buy,
            position.base_quantity.abs(),
            book.best_ask_price,
            true,
            TakerOrderRole::EmergencyCloseShort,
            config.taker_slippage_pct,
            precision,
        )),
    }
}

pub fn inspect_single_leg_net_positions(
    positions: &[NetPosition],
    tolerance: f64,
) -> Vec<NetPositionWarning> {
    let mut grouped: BTreeMap<String, Vec<NetPosition>> = BTreeMap::new();
    for position in positions
        .iter()
        .filter(|position| position.quantity.abs() > tolerance)
    {
        grouped
            .entry(position.canonical_symbol.as_pair())
            .or_default()
            .push(position.clone());
    }

    grouped
        .into_values()
        .filter_map(|positions| {
            let has_long = positions
                .iter()
                .any(|position| position.quantity > tolerance);
            let has_short = positions
                .iter()
                .any(|position| position.quantity < -tolerance);
            if has_long && has_short {
                None
            } else {
                let symbol = positions[0].canonical_symbol.clone();
                Some(NetPositionWarning {
                    canonical_symbol: symbol.clone(),
                    message: format!("single leg net position detected for {}", symbol.as_pair()),
                    positions,
                })
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeRuntimeStatus {
    pub exchange: ExchangeId,
    pub subscribed_symbols: usize,
    pub message_count: u64,
    pub last_latency_ms: Option<u64>,
    pub max_latency_ms: Option<u64>,
    pub server_time_offset_ms: Option<i64>,
    pub disconnect_count: u64,
    pub last_message_at: Option<DateTime<Utc>>,
}

impl ExchangeRuntimeStatus {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            subscribed_symbols: 0,
            message_count: 0,
            last_latency_ms: None,
            max_latency_ms: None,
            server_time_offset_ms: None,
            disconnect_count: 0,
            last_message_at: None,
        }
    }

    pub fn set_subscribed_symbols(&mut self, count: usize) {
        self.subscribed_symbols = count;
    }

    pub fn record_message(
        &mut self,
        latency_ms: Option<u64>,
        server_time_offset_ms: Option<i64>,
        received_at: DateTime<Utc>,
    ) {
        self.message_count += 1;
        self.last_message_at = Some(received_at);
        if let Some(latency_ms) = latency_ms {
            self.last_latency_ms = Some(latency_ms);
            self.max_latency_ms = Some(self.max_latency_ms.unwrap_or(0).max(latency_ms));
        }
        if server_time_offset_ms.is_some() {
            self.server_time_offset_ms = server_time_offset_ms;
        }
    }

    pub fn record_disconnect(&mut self) {
        self.disconnect_count += 1;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeStatusRegistry {
    statuses: BTreeMap<String, ExchangeRuntimeStatus>,
}

impl ExchangeStatusRegistry {
    pub fn status_mut(&mut self, exchange: ExchangeId) -> &mut ExchangeRuntimeStatus {
        let key = exchange.to_string();
        self.statuses
            .entry(key)
            .or_insert_with(|| ExchangeRuntimeStatus::new(exchange))
    }

    pub fn statuses(&self) -> Vec<ExchangeRuntimeStatus> {
        self.statuses.values().cloned().collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyLogEventKind {
    StartupReady,
    StartupBlocked,
    OpportunityOpened,
    PositionClosed,
    SingleLegWarning,
    EmergencyClose,
    StrategyStopped,
    ExchangeDisconnected,
    NetPositionWarning,
    MarketDataHeartbeat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyLogRotationConfig {
    pub persist_only_key_events: bool,
    pub max_file_bytes: u64,
    pub retained_files: usize,
}

impl Default for StrategyLogRotationConfig {
    fn default() -> Self {
        Self {
            persist_only_key_events: true,
            max_file_bytes: 100 * 1024 * 1024,
            retained_files: 5,
        }
    }
}

impl StrategyLogRotationConfig {
    pub fn should_persist(&self, event_kind: StrategyLogEventKind) -> bool {
        if !self.persist_only_key_events {
            return true;
        }
        !matches!(event_kind, StrategyLogEventKind::MarketDataHeartbeat)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SharedOpenQuantity {
    base_quantity: f64,
    executable_depth_usdt: f64,
}

fn shared_open_quantity(
    long_book: &OrderBookTop,
    short_book: &OrderBookTop,
    long_precision: SymbolPrecision,
    short_precision: SymbolPrecision,
    target_notional_usdt: f64,
    top_of_book_capacity_ratio: f64,
) -> Option<SharedOpenQuantity> {
    shared_open_quantity_with_capacity(
        long_book,
        short_book,
        long_precision,
        short_precision,
        target_notional_usdt,
        top_of_book_capacity_ratio,
    )
}

fn shared_open_quantity_with_capacity(
    long_book: &OrderBookTop,
    short_book: &OrderBookTop,
    long_precision: SymbolPrecision,
    short_precision: SymbolPrecision,
    target_notional_usdt: f64,
    top_of_book_capacity_ratio: f64,
) -> Option<SharedOpenQuantity> {
    if target_notional_usdt <= 0.0 || top_of_book_capacity_ratio <= 0.0 {
        return None;
    }
    let capacity_ratio = top_of_book_capacity_ratio.min(1.0);
    let long_top_base_quantity =
        long_precision.base_quantity_from_order_quantity(long_book.best_ask_quantity);
    let short_top_base_quantity =
        short_precision.base_quantity_from_order_quantity(short_book.best_bid_quantity);
    let long_top_depth_usdt = long_top_base_quantity * long_book.best_ask_price;
    let short_top_depth_usdt = short_top_base_quantity * short_book.best_bid_price;
    let executable_depth_usdt = long_top_depth_usdt.min(short_top_depth_usdt) * capacity_ratio;
    if executable_depth_usdt < target_notional_usdt {
        return None;
    }

    let long_available_base = long_top_base_quantity * capacity_ratio;
    let short_available_base = short_top_base_quantity * capacity_ratio;
    let mut quantity = (target_notional_usdt / long_book.best_ask_price)
        .min(long_available_base)
        .min(short_available_base);
    quantity = normalize_shared_base_quantity(quantity, long_precision, short_precision);
    if quantity <= 0.0
        || quantity < long_precision.min_base_quantity()
        || quantity < short_precision.min_base_quantity()
    {
        return None;
    }
    if long_precision.min_notional_usdt > 0.0
        && quantity * long_book.best_ask_price < long_precision.min_notional_usdt
    {
        return None;
    }
    if short_precision.min_notional_usdt > 0.0
        && quantity * short_book.best_bid_price < short_precision.min_notional_usdt
    {
        return None;
    }
    if quantity * long_book.best_ask_price < target_notional_usdt {
        return None;
    }
    Some(SharedOpenQuantity {
        base_quantity: quantity,
        executable_depth_usdt,
    })
}

fn normalize_shared_base_quantity(
    base_quantity: f64,
    long_precision: SymbolPrecision,
    short_precision: SymbolPrecision,
) -> f64 {
    let mut quantity = base_quantity.max(0.0);
    for _ in 0..3 {
        let long_quantity = long_precision.normalized_base_quantity(quantity);
        let short_quantity = short_precision.normalized_base_quantity(quantity);
        let next_quantity = long_quantity.min(short_quantity);
        if (next_quantity - quantity).abs() <= 1e-12 {
            return next_quantity;
        }
        quantity = next_quantity;
    }
    quantity
}

#[allow(clippy::too_many_arguments)]
fn taker_order_draft(
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    side: OrderSide,
    base_quantity: f64,
    reference_price: f64,
    reduce_only: bool,
    role: TakerOrderRole,
    slippage_pct: f64,
    precision: SymbolPrecision,
) -> TakerOrderDraft {
    let quantity = precision.normalized_order_quantity_from_base(base_quantity);
    let base_quantity = precision.base_quantity_from_order_quantity(quantity);
    TakerOrderDraft {
        exchange,
        canonical_symbol,
        side,
        base_quantity,
        quantity,
        quantity_unit: precision.quantity_unit,
        contract_size: precision.effective_contract_size(),
        reference_price,
        worst_acceptable_price: taker_limit_price(side, reference_price, slippage_pct, precision),
        reduce_only,
        role,
    }
}

fn taker_limit_price(
    side: OrderSide,
    reference_price: f64,
    slippage_pct: f64,
    precision: SymbolPrecision,
) -> f64 {
    match side {
        OrderSide::Buy => {
            ceil_to_step(reference_price * (1.0 + slippage_pct), precision.price_tick)
        }
        OrderSide::Sell => {
            floor_to_step(reference_price * (1.0 - slippage_pct), precision.price_tick)
        }
    }
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value.max(0.0);
    }
    (((value / step) + 1e-12).floor() * step).max(0.0)
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value.max(0.0);
    }
    ((value / step).ceil() * step).max(0.0)
}
