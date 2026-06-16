use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::MarketType;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CanonicalSymbol {
    pub base: String,
    pub quote: String,
}

impl CanonicalSymbol {
    pub fn parse(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_uppercase().replace(['-', '_'], "/");
        if let Some((base, quote)) = normalized.split_once('/') {
            return (!base.is_empty() && !quote.is_empty()).then(|| Self {
                base: base.to_string(),
                quote: quote.to_string(),
            });
        }
        for quote in ["USDT", "USDC", "USD"] {
            if normalized.ends_with(quote) && normalized.len() > quote.len() {
                return Some(Self {
                    base: normalized[..normalized.len() - quote.len()].to_string(),
                    quote: quote.to_string(),
                });
            }
        }
        None
    }

    pub fn as_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentKey {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: CanonicalSymbol,
}

impl InstrumentKey {
    pub fn new(
        exchange: impl Into<String>,
        market_type: MarketType,
        symbol: CanonicalSymbol,
    ) -> Self {
        Self {
            exchange: exchange.into().trim().to_ascii_lowercase(),
            market_type,
            symbol,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteKind {
    SpotPerpBasis,
    PerpPerpSpread,
    SingleExchangeSettlement,
}

impl RouteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SpotPerpBasis => "spot_perp_basis",
            Self::PerpPerpSpread => "perp_perp_spread",
            Self::SingleExchangeSettlement => "single_exchange_settlement",
        }
    }
}

impl fmt::Display for RouteKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSidePolicy {
    ReceiveFunding,
    PayFunding,
    FixedLong,
    FixedShort,
}

impl PositionSidePolicy {
    pub fn position_side(self, funding_rate: f64) -> PositionSide {
        match self {
            Self::ReceiveFunding if funding_rate >= 0.0 => PositionSide::Short,
            Self::ReceiveFunding => PositionSide::Long,
            Self::PayFunding if funding_rate >= 0.0 => PositionSide::Long,
            Self::PayFunding => PositionSide::Short,
            Self::FixedLong => PositionSide::Long,
            Self::FixedShort => PositionSide::Short,
        }
    }

    pub fn open_side(self, funding_rate: f64) -> OrderSide {
        match self.position_side(funding_rate) {
            PositionSide::Long => OrderSide::Buy,
            PositionSide::Short => OrderSide::Sell,
            PositionSide::None => OrderSide::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStyle {
    DualTaker,
    MakerTaker,
    DualMaker,
    DualTakerReduceOnly,
    MakerTakerReduceOnly,
    MakerMakerReduceOnly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UnifiedArbitrageConfig {
    pub strategy_kind: String,
    pub mode: String,
    pub trading_mode: String,
    pub enable_live_trading: bool,
    pub account: AccountConfig,
    pub market: MarketConfig,
    pub defaults: DefaultsConfig,
    pub routes: Vec<RouteConfig>,
    pub alerts: AlertConfig,
    pub persistence: PersistenceConfig,
    pub control: ControlConfig,
    pub enabled_exchanges: Vec<String>,
    pub exchanges: BTreeMap<String, Value>,
}

impl Default for UnifiedArbitrageConfig {
    fn default() -> Self {
        Self {
            strategy_kind: "unified_arbitrage".to_string(),
            mode: "observe".to_string(),
            trading_mode: "paper".to_string(),
            enable_live_trading: false,
            account: AccountConfig::default(),
            market: MarketConfig::default(),
            defaults: DefaultsConfig::default(),
            routes: Vec::new(),
            alerts: AlertConfig::default(),
            persistence: PersistenceConfig::default(),
            control: ControlConfig::default(),
            enabled_exchanges: Vec::new(),
            exchanges: BTreeMap::new(),
        }
    }
}

impl UnifiedArbitrageConfig {
    pub fn active_routes(&self) -> Vec<&RouteConfig> {
        self.routes.iter().filter(|route| route.enabled).collect()
    }

    pub fn active_symbols(&self) -> Vec<String> {
        let mut symbols = Vec::new();
        for route in self.active_routes() {
            if !symbols.contains(&route.symbol) {
                symbols.push(route.symbol.clone());
            }
        }
        symbols
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.strategy_kind != "unified_arbitrage" {
            return Err(ConfigError::InvalidStrategyKind);
        }
        if self.routes.is_empty() {
            return Err(ConfigError::NoRoutes);
        }
        let mut route_ids = BTreeSet::new();
        for route in &self.routes {
            route.validate(&self.defaults)?;
            if !route_ids.insert(route.route_id.clone()) {
                return Err(ConfigError::DuplicateRouteId(route.route_id.clone()));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccountConfig {
    pub tenant_id: String,
    pub account_id: String,
    pub require_private_ws: bool,
    pub require_position_readback: bool,
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            tenant_id: "local".to_string(),
            account_id: "unified_arbitrage".to_string(),
            require_private_ws: true,
            require_position_readback: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MarketConfig {
    pub quote_asset: String,
    pub stale_book_ms: u64,
    pub depth_levels: usize,
    pub public_book_speed: String,
    pub top_of_book_capacity_ratio: f64,
    pub funding_snapshot_max_age_ms: u64,
}

impl Default for MarketConfig {
    fn default() -> Self {
        Self {
            quote_asset: "USDT".to_string(),
            stale_book_ms: 1000,
            depth_levels: 5,
            public_book_speed: "fastest".to_string(),
            top_of_book_capacity_ratio: 0.8,
            funding_snapshot_max_age_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DefaultsConfig {
    pub sizing: SizingConfig,
    pub leverage: LeverageConfig,
    pub thresholds: ThresholdConfig,
    pub funding: FundingConfig,
    pub execution: ExecutionConfig,
    pub split_execution: SplitExecutionConfig,
    pub hedge_tolerance: HedgeToleranceConfig,
    pub risk: RiskConfig,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            sizing: SizingConfig::default(),
            leverage: LeverageConfig::default(),
            thresholds: ThresholdConfig::default(),
            funding: FundingConfig::default(),
            execution: ExecutionConfig::default(),
            split_execution: SplitExecutionConfig::default(),
            hedge_tolerance: HedgeToleranceConfig::default(),
            risk: RiskConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SizingConfig {
    pub min_order_notional_usdt: f64,
    pub max_order_notional_usdt: f64,
    pub max_position_notional_usdt: f64,
    pub max_route_notional_usdt: f64,
    pub max_symbol_notional_usdt: f64,
    pub max_exchange_notional_usdt: f64,
    pub max_total_notional_usdt: f64,
}

impl Default for SizingConfig {
    fn default() -> Self {
        Self {
            min_order_notional_usdt: 5.0,
            max_order_notional_usdt: 20.0,
            max_position_notional_usdt: 100.0,
            max_route_notional_usdt: 100.0,
            max_symbol_notional_usdt: 200.0,
            max_exchange_notional_usdt: 300.0,
            max_total_notional_usdt: 500.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LeverageConfig {
    pub default_perp_leverage: u32,
    pub max_perp_leverage: u32,
    pub margin_mode: String,
}

impl Default for LeverageConfig {
    fn default() -> Self {
        Self {
            default_perp_leverage: 3,
            max_perp_leverage: 10,
            margin_mode: "isolated".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThresholdConfig {
    pub min_open_spread_bps: f64,
    pub max_open_spread_bps: f64,
    pub min_open_net_edge_bps: f64,
    pub close_spread_bps: f64,
    pub take_profit_bps: f64,
    pub stop_loss_bps: f64,
    pub alert_spread_above_bps: f64,
    pub alert_spread_below_bps: f64,
}

impl Default for ThresholdConfig {
    fn default() -> Self {
        Self {
            min_open_spread_bps: 20.0,
            max_open_spread_bps: 1000.0,
            min_open_net_edge_bps: 5.0,
            close_spread_bps: 5.0,
            take_profit_bps: 10.0,
            stop_loss_bps: 30.0,
            alert_spread_above_bps: 100.0,
            alert_spread_below_bps: -20.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FundingConfig {
    pub expected_holding_hours: f64,
    pub use_predicted_funding: bool,
    pub funding_uncertainty_buffer_bps: f64,
    pub close_if_funding_edge_below_bps: f64,
}

impl Default for FundingConfig {
    fn default() -> Self {
        Self {
            expected_holding_hours: 8.0,
            use_predicted_funding: true,
            funding_uncertainty_buffer_bps: 5.0,
            close_if_funding_edge_below_bps: -5.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    pub preferred_open_style: ExecutionStyle,
    pub allowed_open_styles: Vec<ExecutionStyle>,
    pub preferred_close_style: ExecutionStyle,
    pub allowed_close_styles: Vec<ExecutionStyle>,
    pub taker_slippage_bps: f64,
    pub maker_price_offset_bps: f64,
    pub maker_order_timeout_ms: u64,
    pub allow_market_order: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            preferred_open_style: ExecutionStyle::DualTaker,
            allowed_open_styles: vec![ExecutionStyle::DualTaker, ExecutionStyle::MakerTaker],
            preferred_close_style: ExecutionStyle::DualTakerReduceOnly,
            allowed_close_styles: vec![
                ExecutionStyle::DualTakerReduceOnly,
                ExecutionStyle::MakerTakerReduceOnly,
            ],
            taker_slippage_bps: 10.0,
            maker_price_offset_bps: 5.0,
            maker_order_timeout_ms: 3000,
            allow_market_order: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SplitExecutionConfig {
    pub enabled: bool,
    pub min_child_notional_usdt: f64,
    pub max_child_notional_usdt: f64,
    pub max_child_orders: usize,
    pub child_delay_ms: u64,
    pub child_delay_jitter_ms: u64,
    pub reprice_before_each_child: bool,
    pub stop_slicing_if_edge_below_bps: f64,
    pub allow_partial_parent_completion: bool,
    pub min_parent_fill_ratio: f64,
}

impl Default for SplitExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_child_notional_usdt: 5.0,
            max_child_notional_usdt: 20.0,
            max_child_orders: 10,
            child_delay_ms: 250,
            child_delay_jitter_ms: 100,
            reprice_before_each_child: true,
            stop_slicing_if_edge_below_bps: 3.0,
            allow_partial_parent_completion: true,
            min_parent_fill_ratio: 0.8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HedgeToleranceConfig {
    pub max_leg_imbalance_usdt: f64,
    pub repair_above_imbalance_usdt: f64,
    pub block_new_entries_above_imbalance_usdt: f64,
    pub emergency_close_above_imbalance_usdt: f64,
}

impl Default for HedgeToleranceConfig {
    fn default() -> Self {
        Self {
            max_leg_imbalance_usdt: 1.0,
            repair_above_imbalance_usdt: 2.0,
            block_new_entries_above_imbalance_usdt: 5.0,
            emergency_close_above_imbalance_usdt: 10.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    pub start_close_only: bool,
    pub max_open_bundles: usize,
    pub max_active_bundles_per_symbol: usize,
    pub min_liquidation_distance_pct: f64,
    pub max_hold_secs: i64,
    pub max_daily_loss_usdt: f64,
    pub block_on_external_account_exposure: bool,
    pub orphan_exposure_blocks_new_entries: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            start_close_only: false,
            max_open_bundles: 20,
            max_active_bundles_per_symbol: 3,
            min_liquidation_distance_pct: 25.0,
            max_hold_secs: 86_400,
            max_daily_loss_usdt: 100.0,
            block_on_external_account_exposure: true,
            orphan_exposure_blocks_new_entries: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RouteConfig {
    pub route_id: String,
    pub enabled: bool,
    pub kind: RouteKind,
    pub symbol: String,
    pub sizing: Option<SizingConfig>,
    pub thresholds: Option<ThresholdConfig>,
    pub funding: Option<FundingConfig>,
    pub execution: Option<ExecutionConfig>,
    pub split_execution: Option<SplitExecutionConfig>,
    pub risk: Option<RiskConfig>,
    pub settlement: Option<SettlementConfig>,
    pub legs: BTreeMap<String, LegConfig>,
}

impl Default for RouteConfig {
    fn default() -> Self {
        Self {
            route_id: String::new(),
            enabled: true,
            kind: RouteKind::PerpPerpSpread,
            symbol: String::new(),
            sizing: None,
            thresholds: None,
            funding: None,
            execution: None,
            split_execution: None,
            risk: None,
            settlement: None,
            legs: BTreeMap::new(),
        }
    }
}

impl RouteConfig {
    pub fn validate(&self, defaults: &DefaultsConfig) -> Result<(), ConfigError> {
        if self.route_id.trim().is_empty() {
            return Err(ConfigError::InvalidRoute(
                "route_id is required".to_string(),
            ));
        }
        if CanonicalSymbol::parse(&self.symbol).is_none() {
            return Err(ConfigError::InvalidRoute(format!(
                "{} has invalid symbol {}",
                self.route_id, self.symbol
            )));
        }
        match self.kind {
            RouteKind::SpotPerpBasis => {
                let spot_count = self
                    .legs
                    .values()
                    .filter(|leg| leg.market_type == MarketType::Spot)
                    .count();
                let perp_count = self
                    .legs
                    .values()
                    .filter(|leg| leg.market_type == MarketType::Perpetual)
                    .count();
                if spot_count != 1 || perp_count != 1 {
                    return Err(ConfigError::InvalidRoute(format!(
                        "{} requires exactly one spot leg and one perpetual leg",
                        self.route_id
                    )));
                }
            }
            RouteKind::PerpPerpSpread => {
                if self.legs.len() != 2
                    || self
                        .legs
                        .values()
                        .any(|leg| leg.market_type != MarketType::Perpetual)
                {
                    return Err(ConfigError::InvalidRoute(format!(
                        "{} requires exactly two perpetual legs",
                        self.route_id
                    )));
                }
            }
            RouteKind::SingleExchangeSettlement => {
                if self.legs.len() != 1 || self.settlement.is_none() {
                    return Err(ConfigError::InvalidRoute(format!(
                        "{} requires one settlement leg and settlement config",
                        self.route_id
                    )));
                }
                let settlement = self.settlement.unwrap();
                let risk = self.risk.unwrap_or(defaults.risk);
                if settlement.max_settlement_position_notional_usdt
                    > self
                        .sizing
                        .unwrap_or(defaults.sizing)
                        .max_position_notional_usdt
                    || risk.min_liquidation_distance_pct <= 0.0
                {
                    return Err(ConfigError::InvalidRoute(format!(
                        "{} has unsafe settlement sizing or liquidation distance",
                        self.route_id
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn threshold_config(&self, defaults: &DefaultsConfig) -> ThresholdConfig {
        self.thresholds.unwrap_or(defaults.thresholds)
    }

    pub fn funding_config(&self, defaults: &DefaultsConfig) -> FundingConfig {
        self.funding.unwrap_or(defaults.funding)
    }

    pub fn execution_config(&self, defaults: &DefaultsConfig) -> ExecutionConfig {
        self.execution
            .clone()
            .unwrap_or_else(|| defaults.execution.clone())
    }

    pub fn split_config(&self, defaults: &DefaultsConfig) -> SplitExecutionConfig {
        self.split_execution.unwrap_or(defaults.split_execution)
    }

    pub fn sizing_config(&self, defaults: &DefaultsConfig) -> SizingConfig {
        self.sizing.unwrap_or(defaults.sizing)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LegConfig {
    pub exchange: String,
    pub market_type: MarketType,
    pub side_on_open: Option<OrderSide>,
    pub position_side: Option<PositionSide>,
    pub position_side_policy: Option<PositionSidePolicy>,
    pub leverage: Option<u32>,
}

impl Default for LegConfig {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            market_type: MarketType::Perpetual,
            side_on_open: None,
            position_side: None,
            position_side_policy: None,
            leverage: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementConfig {
    pub open_before_settlement_secs: i64,
    pub min_submit_before_settlement_secs: i64,
    pub close_after_settlement_secs: i64,
    pub min_abs_funding_rate_bps: f64,
    pub min_settlement_net_edge_bps: f64,
    pub max_hold_secs: i64,
    pub max_settlement_position_notional_usdt: f64,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            open_before_settlement_secs: 20,
            min_submit_before_settlement_secs: 3,
            close_after_settlement_secs: 5,
            min_abs_funding_rate_bps: 20.0,
            min_settlement_net_edge_bps: 5.0,
            max_hold_secs: 120,
            max_settlement_position_notional_usdt: 10.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AlertConfig {
    pub enabled: bool,
    pub spread_above_bps: f64,
    pub spread_below_bps: f64,
    pub liquidation_distance_below_pct: f64,
    pub pnl_loss_below_usdt: f64,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            spread_above_bps: 100.0,
            spread_below_bps: -20.0,
            liquidation_distance_below_pct: 25.0,
            pnl_loss_below_usdt: -20.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceConfig {
    pub jsonl_dir: String,
    pub trade_ledger_path: String,
    pub dashboard_snapshot_path: String,
    pub clickhouse_enabled: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            jsonl_dir: "logs/unified_arbitrage".to_string(),
            trade_ledger_path: "logs/unified_arbitrage/trade_events.jsonl".to_string(),
            dashboard_snapshot_path: "logs/unified_arbitrage/dashboard.json".to_string(),
            clickhouse_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlConfig {
    pub enable_manual_open: bool,
    pub enable_manual_close: bool,
    pub manual_commands_require_confirmation: bool,
    pub manual_open_requires_current_signal: bool,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            enable_manual_open: true,
            enable_manual_close: true,
            manual_commands_require_confirmation: true,
            manual_open_requires_current_signal: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    InvalidStrategyKind,
    NoRoutes,
    DuplicateRouteId(String),
    InvalidRoute(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStrategyKind => {
                formatter.write_str("strategy_kind must be unified_arbitrage")
            }
            Self::NoRoutes => formatter.write_str("unified_arbitrage requires at least one route"),
            Self::DuplicateRouteId(route_id) => write!(formatter, "duplicate route_id {route_id}"),
            Self::InvalidRoute(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookTop {
    pub instrument: InstrumentKey,
    pub best_bid_price: f64,
    pub best_bid_quantity: f64,
    pub best_ask_price: f64,
    pub best_ask_quantity: f64,
    pub levels: usize,
    pub received_at: DateTime<Utc>,
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub instrument: InstrumentKey,
    pub funding_rate: f64,
    pub predicted_funding_rate: Option<f64>,
    pub funding_interval_hours: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl FundingSnapshot {
    pub fn effective_rate(self, config: FundingConfig) -> f64 {
        if config.use_predicted_funding {
            self.predicted_funding_rate.unwrap_or(self.funding_rate)
        } else {
            self.funding_rate
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeRates {
    pub maker: f64,
    pub taker: f64,
}

impl Default for FeeRates {
    fn default() -> Self {
        Self {
            maker: 0.0002,
            taker: 0.0005,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteMarketInput {
    pub route_id: String,
    pub books: BTreeMap<String, OrderBookTop>,
    pub funding: BTreeMap<String, FundingSnapshot>,
    pub fees: BTreeMap<String, FeeRates>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteOpportunity {
    pub route_id: String,
    pub kind: RouteKind,
    pub symbol: String,
    pub spread_bps: f64,
    pub funding_edge_bps: f64,
    pub expected_net_edge_bps: f64,
    pub expected_funding_pnl_usdt: f64,
    pub accepted: bool,
    pub reject_reason: Option<String>,
    pub open_style: ExecutionStyle,
    pub close_style: ExecutionStyle,
    pub split_plan: SplitPlan,
}

pub fn evaluate_route_opportunity(
    route: &RouteConfig,
    defaults: &DefaultsConfig,
    input: &RouteMarketInput,
) -> RouteOpportunity {
    let thresholds = route.threshold_config(defaults);
    let funding_config = route.funding_config(defaults);
    let execution = route.execution_config(defaults);
    let sizing = route.sizing_config(defaults);
    let split = route.split_config(defaults);
    let notional = sizing
        .max_order_notional_usdt
        .min(sizing.max_position_notional_usdt);
    let (spread_bps, funding_edge_bps, reject_reason) = match route.kind {
        RouteKind::SpotPerpBasis => spot_perp_edges(route, input, funding_config),
        RouteKind::PerpPerpSpread => perp_perp_edges(route, input, funding_config),
        RouteKind::SingleExchangeSettlement => settlement_edges(route, input, funding_config),
    };

    let open_fee_bps = route
        .legs
        .keys()
        .map(|leg_id| input.fees.get(leg_id).cloned().unwrap_or_default().taker * 10_000.0)
        .sum::<f64>();
    let close_fee_bps = open_fee_bps;
    let expected_net_edge_bps = spread_bps + funding_edge_bps
        - open_fee_bps
        - close_fee_bps
        - execution.taker_slippage_bps
        - thresholds.min_open_net_edge_bps
        - funding_config.funding_uncertainty_buffer_bps;
    let accepted = reject_reason.is_none()
        && spread_bps >= thresholds.min_open_spread_bps
        && spread_bps <= thresholds.max_open_spread_bps
        && expected_net_edge_bps >= 0.0;

    RouteOpportunity {
        route_id: route.route_id.clone(),
        kind: route.kind,
        symbol: route.symbol.clone(),
        spread_bps,
        funding_edge_bps,
        expected_net_edge_bps,
        expected_funding_pnl_usdt: notional * funding_edge_bps / 10_000.0,
        accepted,
        reject_reason: if accepted {
            None
        } else {
            reject_reason.or_else(|| Some("edge_below_threshold".to_string()))
        },
        open_style: execution.preferred_open_style,
        close_style: execution.preferred_close_style,
        split_plan: build_split_plan(notional, split),
    }
}

fn spot_perp_edges(
    route: &RouteConfig,
    input: &RouteMarketInput,
    funding_config: FundingConfig,
) -> (f64, f64, Option<String>) {
    let Some((spot_id, _spot_leg)) = route
        .legs
        .iter()
        .find(|(_, leg)| leg.market_type == MarketType::Spot)
    else {
        return (0.0, 0.0, Some("missing_spot_leg".to_string()));
    };
    let Some((perp_id, _perp_leg)) = route
        .legs
        .iter()
        .find(|(_, leg)| leg.market_type == MarketType::Perpetual)
    else {
        return (0.0, 0.0, Some("missing_perp_leg".to_string()));
    };
    let (Some(spot_book), Some(perp_book)) = (input.books.get(spot_id), input.books.get(perp_id))
    else {
        return (0.0, 0.0, Some("missing_books".to_string()));
    };
    if spot_book.best_ask_price <= 0.0 {
        return (0.0, 0.0, Some("invalid_spot_book".to_string()));
    }
    let spread_bps =
        (perp_book.best_bid_price - spot_book.best_ask_price) / spot_book.best_ask_price * 10_000.0;
    let funding_bps = input
        .funding
        .get(perp_id)
        .map(|funding| {
            funding.clone().effective_rate(funding_config)
                * expected_funding_windows(funding, funding_config)
                * 10_000.0
        })
        .unwrap_or(0.0);
    (spread_bps, funding_bps, None)
}

fn perp_perp_edges(
    route: &RouteConfig,
    input: &RouteMarketInput,
    funding_config: FundingConfig,
) -> (f64, f64, Option<String>) {
    let Some((long_id, _)) = route.legs.iter().find(|(_, leg)| {
        leg.position_side == Some(PositionSide::Long) || leg.side_on_open == Some(OrderSide::Buy)
    }) else {
        return (0.0, 0.0, Some("missing_long_leg".to_string()));
    };
    let Some((short_id, _)) = route.legs.iter().find(|(_, leg)| {
        leg.position_side == Some(PositionSide::Short) || leg.side_on_open == Some(OrderSide::Sell)
    }) else {
        return (0.0, 0.0, Some("missing_short_leg".to_string()));
    };
    let (Some(long_book), Some(short_book)) = (input.books.get(long_id), input.books.get(short_id))
    else {
        return (0.0, 0.0, Some("missing_books".to_string()));
    };
    if long_book.best_ask_price <= 0.0 {
        return (0.0, 0.0, Some("invalid_long_book".to_string()));
    }
    let spread_bps = (short_book.best_bid_price - long_book.best_ask_price)
        / long_book.best_ask_price
        * 10_000.0;
    let long_funding = input
        .funding
        .get(long_id)
        .map(|funding| funding.clone().effective_rate(funding_config))
        .unwrap_or(0.0);
    let short_funding = input
        .funding
        .get(short_id)
        .map(|funding| funding.clone().effective_rate(funding_config))
        .unwrap_or(0.0);
    let funding_edge_bps = (short_funding - long_funding) * 10_000.0;
    (spread_bps, funding_edge_bps, None)
}

fn settlement_edges(
    route: &RouteConfig,
    input: &RouteMarketInput,
    funding_config: FundingConfig,
) -> (f64, f64, Option<String>) {
    let Some((leg_id, _)) = route.legs.iter().next() else {
        return (0.0, 0.0, Some("missing_settlement_leg".to_string()));
    };
    let Some(funding) = input.funding.get(leg_id) else {
        return (0.0, 0.0, Some("missing_funding".to_string()));
    };
    let funding_bps = funding.clone().effective_rate(funding_config).abs() * 10_000.0;
    (funding_bps, funding_bps, None)
}

fn expected_funding_windows(funding: &FundingSnapshot, config: FundingConfig) -> f64 {
    config.expected_holding_hours / funding.funding_interval_hours.max(1.0)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SplitPlan {
    pub enabled: bool,
    pub parent_notional_usdt: f64,
    pub child_notional_usdt: f64,
    pub child_count: usize,
    pub child_delay_ms: u64,
    pub reprice_before_each_child: bool,
}

pub fn build_split_plan(parent_notional_usdt: f64, config: SplitExecutionConfig) -> SplitPlan {
    if !config.enabled || parent_notional_usdt <= config.max_child_notional_usdt {
        return SplitPlan {
            enabled: false,
            parent_notional_usdt,
            child_notional_usdt: parent_notional_usdt.max(0.0),
            child_count: 1,
            child_delay_ms: 0,
            reprice_before_each_child: false,
        };
    }
    let child_count = ((parent_notional_usdt / config.max_child_notional_usdt).ceil() as usize)
        .min(config.max_child_orders.max(1));
    let child_notional_usdt =
        (parent_notional_usdt / child_count as f64).max(config.min_child_notional_usdt);
    SplitPlan {
        enabled: true,
        parent_notional_usdt,
        child_notional_usdt,
        child_count,
        child_delay_ms: config.child_delay_ms,
        reprice_before_each_child: config.reprice_before_each_child,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgeStatus {
    Matched,
    Residual,
    Repairing,
    RouteCloseOnly,
    EmergencyCloseRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HedgeEvaluation {
    pub matched_base_qty: f64,
    pub residual_base_qty: f64,
    pub residual_notional_usdt: f64,
    pub status: HedgeStatus,
}

pub fn evaluate_hedge_residual(
    left_base_qty: f64,
    right_base_qty: f64,
    reference_price: f64,
    tolerance: HedgeToleranceConfig,
) -> HedgeEvaluation {
    let matched = left_base_qty.abs().min(right_base_qty.abs());
    let residual_base_qty = (left_base_qty.abs() - right_base_qty.abs()).abs();
    let residual_notional_usdt = residual_base_qty * reference_price.max(0.0);
    let status = if residual_notional_usdt <= tolerance.max_leg_imbalance_usdt {
        HedgeStatus::Matched
    } else if residual_notional_usdt >= tolerance.emergency_close_above_imbalance_usdt {
        HedgeStatus::EmergencyCloseRequired
    } else if residual_notional_usdt >= tolerance.block_new_entries_above_imbalance_usdt {
        HedgeStatus::RouteCloseOnly
    } else if residual_notional_usdt >= tolerance.repair_above_imbalance_usdt {
        HedgeStatus::Repairing
    } else {
        HedgeStatus::Residual
    };
    HedgeEvaluation {
        matched_base_qty: matched,
        residual_base_qty,
        residual_notional_usdt,
        status,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegPositionSnapshot {
    pub leg_id: String,
    pub exchange: String,
    pub market_type: MarketType,
    pub position_side: PositionSide,
    pub leverage: Option<u32>,
    pub quantity_base: f64,
    pub quantity_contracts: f64,
    pub position_value_usdt: f64,
    pub avg_entry_price: f64,
    pub mark_price: f64,
    pub liquidation_price: Option<f64>,
    pub liquidation_distance_pct: Option<f64>,
    pub unrealized_pnl_usdt: f64,
    pub realized_pnl_usdt: f64,
    pub funding_rate: Option<f64>,
    pub predicted_funding_rate: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub margin_used_usdt: Option<f64>,
    pub maintenance_margin_usdt: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutePositionSnapshot {
    pub route_id: String,
    pub kind: RouteKind,
    pub symbol: String,
    pub status: String,
    pub spread_bps: f64,
    pub funding_diff_bps: f64,
    pub expected_funding_pnl_usdt: f64,
    pub position_value_usdt: f64,
    pub quantity_base: f64,
    pub avg_entry_cost_usdt: f64,
    pub unrealized_pnl_usdt: f64,
    pub realized_pnl_usdt: f64,
    pub funding_pnl_usdt: f64,
    pub fee_pnl_usdt: f64,
    pub net_pnl_usdt: f64,
    pub pnl_bps: f64,
    pub nearest_liquidation_distance_pct: Option<f64>,
    pub hedge: HedgeEvaluation,
    pub legs: Vec<LegPositionSnapshot>,
}

pub fn build_route_position_snapshot(
    route: &RouteConfig,
    opportunity: &RouteOpportunity,
    legs: Vec<LegPositionSnapshot>,
    hedge_tolerance: HedgeToleranceConfig,
) -> RoutePositionSnapshot {
    let position_value_usdt = legs
        .iter()
        .map(|leg| leg.position_value_usdt.abs())
        .sum::<f64>();
    let quantity_base = legs
        .iter()
        .map(|leg| leg.quantity_base.abs())
        .fold(f64::INFINITY, f64::min);
    let quantity_base = if quantity_base.is_finite() {
        quantity_base
    } else {
        0.0
    };
    let unrealized_pnl_usdt = legs.iter().map(|leg| leg.unrealized_pnl_usdt).sum();
    let realized_pnl_usdt = legs.iter().map(|leg| leg.realized_pnl_usdt).sum();
    let nearest_liquidation_distance_pct = legs
        .iter()
        .filter_map(|leg| leg.liquidation_distance_pct)
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let reference_price = legs.first().map(|leg| leg.mark_price).unwrap_or(0.0);
    let hedge = if legs.len() >= 2 {
        evaluate_hedge_residual(
            legs[0].quantity_base,
            legs[1].quantity_base,
            reference_price,
            hedge_tolerance,
        )
    } else {
        evaluate_hedge_residual(
            quantity_base,
            quantity_base,
            reference_price,
            hedge_tolerance,
        )
    };
    let net_pnl_usdt = unrealized_pnl_usdt + realized_pnl_usdt;
    RoutePositionSnapshot {
        route_id: route.route_id.clone(),
        kind: route.kind,
        symbol: route.symbol.clone(),
        status: if matches!(hedge.status, HedgeStatus::Matched) {
            "open".to_string()
        } else {
            format!("{:?}", hedge.status).to_ascii_lowercase()
        },
        spread_bps: opportunity.spread_bps,
        funding_diff_bps: opportunity.funding_edge_bps,
        expected_funding_pnl_usdt: opportunity.expected_funding_pnl_usdt,
        position_value_usdt,
        quantity_base,
        avg_entry_cost_usdt: legs.iter().map(|leg| leg.avg_entry_price).sum::<f64>()
            / legs.len().max(1) as f64,
        unrealized_pnl_usdt,
        realized_pnl_usdt,
        funding_pnl_usdt: 0.0,
        fee_pnl_usdt: 0.0,
        net_pnl_usdt,
        pnl_bps: if position_value_usdt > 0.0 {
            net_pnl_usdt / position_value_usdt * 10_000.0
        } else {
            0.0
        },
        nearest_liquidation_distance_pct,
        hedge,
        legs,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManualOpenCommand {
    pub route_id: String,
    pub notional_usdt: f64,
    pub execution_style: Option<ExecutionStyle>,
    pub use_split_execution: bool,
    pub max_slippage_bps: f64,
    pub require_current_signal: bool,
    pub dry_run_preview: bool,
    pub operator_confirmation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManualCloseCommand {
    pub scope: CloseScope,
    pub route_id: Option<String>,
    pub bundle_id: Option<String>,
    pub symbol: Option<String>,
    pub reason: String,
    pub execution_style: Option<ExecutionStyle>,
    pub reduce_only: bool,
    pub close_residual_only: bool,
    pub max_slippage_bps: f64,
    pub dry_run_preview: bool,
    pub operator_confirmation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseScope {
    Bundle,
    Route,
    Symbol,
    All,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManualCommandValidation {
    pub accepted: bool,
    pub reason: Option<String>,
}

pub fn validate_manual_open(
    command: &ManualOpenCommand,
    config: &UnifiedArbitrageConfig,
    current_signal: Option<&RouteOpportunity>,
) -> ManualCommandValidation {
    if !config.control.enable_manual_open {
        return rejected("manual open is disabled");
    }
    if config.control.manual_commands_require_confirmation
        && command
            .operator_confirmation_id
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        return rejected("operator confirmation is required");
    }
    let Some(route) = config
        .routes
        .iter()
        .find(|route| route.route_id == command.route_id)
    else {
        return rejected("route not found");
    };
    if command.notional_usdt <= 0.0
        || command.notional_usdt
            > route
                .sizing_config(&config.defaults)
                .max_position_notional_usdt
    {
        return rejected("requested notional exceeds route limit");
    }
    if command.require_current_signal
        && config.control.manual_open_requires_current_signal
        && !current_signal.is_some_and(|signal| signal.accepted)
    {
        return rejected("current signal is required and not accepted");
    }
    ManualCommandValidation {
        accepted: true,
        reason: None,
    }
}

pub fn validate_manual_close(
    command: &ManualCloseCommand,
    config: &UnifiedArbitrageConfig,
) -> ManualCommandValidation {
    if !config.control.enable_manual_close {
        return rejected("manual close is disabled");
    }
    if !command.reduce_only {
        return rejected("manual close requires reduce_only=true");
    }
    if config.control.manual_commands_require_confirmation
        && command
            .operator_confirmation_id
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        return rejected("operator confirmation is required");
    }
    if matches!(command.scope, CloseScope::Route) && command.route_id.is_none() {
        return rejected("route close requires route_id");
    }
    ManualCommandValidation {
        accepted: true,
        reason: None,
    }
}

fn rejected(reason: impl Into<String>) -> ManualCommandValidation {
    ManualCommandValidation {
        accepted: false,
        reason: Some(reason.into()),
    }
}
