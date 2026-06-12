use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExchangeId(pub String);

impl ExchangeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into().trim().to_ascii_lowercase())
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
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpreadTargetDirection {
    Decrease,
    Increase,
}

impl SpreadTargetDirection {
    pub fn open_threshold_met(self, spread_pct: f64, open_spread_pct: f64) -> bool {
        match self {
            Self::Decrease => spread_pct <= open_spread_pct,
            Self::Increase => spread_pct >= open_spread_pct,
        }
    }

    pub fn target_close_met(self, spread_pct: f64, target_close_spread_pct: f64) -> bool {
        match self {
            Self::Decrease => spread_pct <= target_close_spread_pct,
            Self::Increase => spread_pct >= target_close_spread_pct,
        }
    }

    pub fn directional_move_from_open(self, weighted_open_spread_pct: f64, spread_pct: f64) -> f64 {
        match self {
            Self::Decrease => weighted_open_spread_pct - spread_pct,
            Self::Increase => spread_pct - weighted_open_spread_pct,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectionPolicy {
    FundingFirst,
    SpreadFirst,
    Balanced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenExecutionStyle {
    MakerTaker,
    DualMaker,
    DualTaker,
    AdaptiveOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseExecutionStyle {
    MakerMakerReduceOnly,
    MakerTakerReduceOnly,
    DualTakerReduceOnly,
    AdaptiveClose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderIntentKind {
    OpenMaker,
    OpenHedgeTaker,
    OpenTaker,
    CloseMaker,
    CloseTaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    PostOnly,
    ImmediateOrCancel,
    FillOrKill,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderDraft {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub base_quantity: f64,
    pub order_quantity: f64,
    pub reference_price: f64,
    pub limit_price: f64,
    pub post_only: bool,
    pub reduce_only: bool,
    pub time_in_force: TimeInForce,
    pub intent_kind: OrderIntentKind,
}

impl OrderDraft {
    pub fn notional_usdt(&self) -> f64 {
        self.base_quantity.abs() * self.reference_price.max(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SymbolPrecision {
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_quantity: f64,
    pub min_notional_usdt: f64,
}

impl Default for SymbolPrecision {
    fn default() -> Self {
        Self {
            price_tick: 0.0,
            quantity_step: 0.0,
            min_quantity: 0.0,
            min_notional_usdt: 0.0,
        }
    }
}

impl SymbolPrecision {
    pub fn normalize_quantity_down(self, quantity: f64) -> f64 {
        floor_to_step(quantity.max(0.0), self.quantity_step)
    }

    pub fn normalize_quantity_up(self, quantity: f64) -> f64 {
        ceil_to_step(quantity.max(0.0), self.quantity_step)
    }

    pub fn buy_limit(self, price: f64) -> f64 {
        ceil_to_step(price.max(0.0), self.price_tick)
    }

    pub fn sell_limit(self, price: f64) -> f64 {
        floor_to_step(price.max(0.0), self.price_tick)
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

    pub fn is_fresh(&self, now: DateTime<Utc>, max_book_age_ms: i64) -> bool {
        now.signed_duration_since(self.received_at)
            .num_milliseconds()
            <= max_book_age_ms
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub funding_rate: f64,
    pub predicted_funding_rate: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub open_interest: Option<f64>,
    pub turnover_24h: Option<f64>,
    pub volume_24h: Option<f64>,
    pub funding_interval_hours: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl FundingSnapshot {
    pub fn is_fresh(&self, now: DateTime<Utc>, max_age_ms: i64) -> bool {
        now.signed_duration_since(self.updated_at)
            .num_milliseconds()
            <= max_age_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
pub struct RouteConfig {
    pub route_id: String,
    pub leg_a_exchange: String,
    pub leg_b_exchange: String,
    pub symbol: String,
    pub target_direction: SpreadTargetDirection,
    pub direction_policy: DirectionPolicy,
    pub enabled: bool,
}

impl Default for RouteConfig {
    fn default() -> Self {
        Self {
            route_id: "default_route".to_string(),
            leg_a_exchange: "mexc".to_string(),
            leg_b_exchange: "bybit".to_string(),
            symbol: "HUSDT/USDT".to_string(),
            target_direction: SpreadTargetDirection::Decrease,
            direction_policy: DirectionPolicy::Balanced,
            enabled: true,
        }
    }
}

impl RouteConfig {
    pub fn canonical_symbol(&self) -> CanonicalSymbol {
        parse_symbol_pair(&self.symbol)
    }

    pub fn leg_a(&self) -> ExchangeId {
        ExchangeId::new(&self.leg_a_exchange)
    }

    pub fn leg_b(&self) -> ExchangeId {
        ExchangeId::new(&self.leg_b_exchange)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThresholdsConfig {
    pub open_spread_pct: f64,
    pub target_close_spread_pct: f64,
    pub allow_negative_immediate_edge: bool,
    pub min_immediate_open_edge_pct: f64,
    pub min_expected_total_edge_pct: f64,
    pub min_net_funding_rate: f64,
    pub max_adverse_funding_rate: f64,
    pub funding_uncertainty_buffer_pct: f64,
    pub repair_buffer_pct: f64,
}

impl Default for ThresholdsConfig {
    fn default() -> Self {
        Self {
            open_spread_pct: -0.03,
            target_close_spread_pct: -0.08,
            allow_negative_immediate_edge: true,
            min_immediate_open_edge_pct: -0.005,
            min_expected_total_edge_pct: 0.001,
            min_net_funding_rate: -0.0005,
            max_adverse_funding_rate: 0.001,
            funding_uncertainty_buffer_pct: 0.0005,
            repair_buffer_pct: 0.0005,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FundingConfig {
    pub require_funding_snapshot: bool,
    pub require_mark_price: bool,
    pub require_next_funding_time: bool,
    pub max_funding_snapshot_age_ms: i64,
    pub funding_rate_basis_hours: f64,
    pub expected_funding_windows: u32,
    pub expected_funding_horizon_hours: f64,
    pub funding_decay_haircut_pct: f64,
}

impl Default for FundingConfig {
    fn default() -> Self {
        Self {
            require_funding_snapshot: true,
            require_mark_price: true,
            require_next_funding_time: true,
            max_funding_snapshot_age_ms: 5_000,
            funding_rate_basis_hours: 8.0,
            expected_funding_windows: 2,
            expected_funding_horizon_hours: 16.0,
            funding_decay_haircut_pct: 0.5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SizingConfig {
    pub min_order_notional_usdt: f64,
    pub max_order_notional_usdt: f64,
    pub max_position_notional_usdt: f64,
    pub max_leg_imbalance_usdt: f64,
}

impl Default for SizingConfig {
    fn default() -> Self {
        Self {
            min_order_notional_usdt: 8.0,
            max_order_notional_usdt: 10.0,
            max_position_notional_usdt: 20.0,
            max_leg_imbalance_usdt: 0.5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddMode {
    LadderOnSpread,
    SteadyFunding,
    Hybrid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AddingConfig {
    pub add_mode: AddMode,
    pub add_step_pct: f64,
    pub min_add_interval_secs: i64,
    pub max_open_slices_per_route: usize,
    pub pause_add_before_target_pct: f64,
}

impl Default for AddingConfig {
    fn default() -> Self {
        Self {
            add_mode: AddMode::LadderOnSpread,
            add_step_pct: 0.005,
            min_add_interval_secs: 30,
            max_open_slices_per_route: 5,
            pause_add_before_target_pct: 0.002,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    pub preferred_open_style: OpenExecutionStyle,
    pub allowed_open_styles: Vec<OpenExecutionStyle>,
    pub preferred_close_style: CloseExecutionStyle,
    pub allowed_close_styles: Vec<CloseExecutionStyle>,
    pub risk_close_style: CloseExecutionStyle,
    pub fallback_close_style: CloseExecutionStyle,
    pub allow_dual_maker_open: bool,
    pub allow_dual_maker_close: bool,
    pub maker_price_offset_pct: f64,
    pub maker_order_timeout_ms: u64,
    pub close_maker_price_offset_pct: f64,
    pub close_maker_order_timeout_ms: u64,
    pub max_close_single_leg_exposure_ms: u64,
    pub hedge_taker_slippage_pct: f64,
    pub close_taker_slippage_pct: f64,
    pub cancel_if_signal_invalid: bool,
    pub cancel_if_close_signal_invalid: bool,
    pub allow_market_order: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            preferred_open_style: OpenExecutionStyle::MakerTaker,
            allowed_open_styles: vec![
                OpenExecutionStyle::MakerTaker,
                OpenExecutionStyle::DualMaker,
            ],
            preferred_close_style: CloseExecutionStyle::MakerMakerReduceOnly,
            allowed_close_styles: vec![
                CloseExecutionStyle::MakerMakerReduceOnly,
                CloseExecutionStyle::MakerTakerReduceOnly,
                CloseExecutionStyle::DualTakerReduceOnly,
            ],
            risk_close_style: CloseExecutionStyle::DualTakerReduceOnly,
            fallback_close_style: CloseExecutionStyle::DualTakerReduceOnly,
            allow_dual_maker_open: true,
            allow_dual_maker_close: true,
            maker_price_offset_pct: 0.0005,
            maker_order_timeout_ms: 3_000,
            close_maker_price_offset_pct: 0.0005,
            close_maker_order_timeout_ms: 3_000,
            max_close_single_leg_exposure_ms: 1_000,
            hedge_taker_slippage_pct: 0.003,
            close_taker_slippage_pct: 0.003,
            cancel_if_signal_invalid: true,
            cancel_if_close_signal_invalid: true,
            allow_market_order: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    pub max_mmr_pct: f64,
    pub max_adl_pct: f64,
    pub min_liquidation_buffer_pct: f64,
    pub max_book_age_ms: i64,
    pub min_orderbook_levels: usize,
    pub single_leg_timeout_ms: u64,
    pub unknown_order_timeout_ms: u64,
    pub max_open_routes: usize,
    pub max_active_routes_per_symbol: usize,
    pub max_positions_per_exchange: usize,
    pub symbol_cooldown_secs: i64,
    pub close_on_unrealized_loss: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_mmr_pct: 20.0,
            max_adl_pct: 300.0,
            min_liquidation_buffer_pct: 30.0,
            max_book_age_ms: 500,
            min_orderbook_levels: 1,
            single_leg_timeout_ms: 1_000,
            unknown_order_timeout_ms: 3_000,
            max_open_routes: 3,
            max_active_routes_per_symbol: 1,
            max_positions_per_exchange: 10,
            symbol_cooldown_secs: 300,
            close_on_unrealized_loss: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FundingSpreadExpansionMakerConfig {
    pub strategy_kind: Option<String>,
    pub display_name: Option<String>,
    pub mode: String,
    pub dry_run: bool,
    pub routes: Vec<RouteConfig>,
    pub thresholds: ThresholdsConfig,
    pub funding: FundingConfig,
    pub sizing: SizingConfig,
    pub adding: AddingConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
    pub runtime_contract: serde_json::Value,
}

impl Default for FundingSpreadExpansionMakerConfig {
    fn default() -> Self {
        Self {
            strategy_kind: Some("funding_spread_expansion_maker".to_string()),
            display_name: Some("Funding Spread Expansion Maker".to_string()),
            mode: "observe".to_string(),
            dry_run: true,
            routes: vec![RouteConfig::default()],
            thresholds: ThresholdsConfig::default(),
            funding: FundingConfig::default(),
            sizing: SizingConfig::default(),
            adding: AddingConfig::default(),
            execution: ExecutionConfig::default(),
            risk: RiskConfig::default(),
            runtime_contract: serde_json::Value::Null,
        }
    }
}

impl FundingSpreadExpansionMakerConfig {
    pub fn active_symbols(&self) -> Vec<String> {
        self.routes
            .iter()
            .filter(|route| route.enabled)
            .map(|route| route.canonical_symbol().as_pair())
            .fold(Vec::new(), |mut symbols, symbol| {
                if !symbols.contains(&symbol) {
                    symbols.push(symbol);
                }
                symbols
            })
    }

    pub fn active_exchanges(&self) -> Vec<String> {
        self.routes
            .iter()
            .filter(|route| route.enabled)
            .flat_map(|route| [route.leg_a_exchange.clone(), route.leg_b_exchange.clone()])
            .map(|exchange| exchange.trim().to_ascii_lowercase())
            .filter(|exchange| !exchange.is_empty())
            .fold(Vec::new(), |mut exchanges, exchange| {
                if !exchanges.contains(&exchange) {
                    exchanges.push(exchange);
                }
                exchanges
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteState {
    Observing,
    MakerOpenPending,
    HedgePending,
    Open,
    AddPending,
    TargetCloseReady,
    Closing,
    Closed,
    RepairingSingleLeg,
    CloseOnly,
    RiskStopped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutePosition {
    pub route_id: String,
    pub state: RouteState,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub long_base_quantity: f64,
    pub short_base_quantity: f64,
    pub long_avg_entry_price: f64,
    pub short_avg_entry_price: f64,
    pub weighted_open_spread_pct: f64,
    pub current_notional_usdt: f64,
    pub cumulative_funding_pnl_usdt: f64,
    pub cumulative_fee_usdt: f64,
    pub opened_at: DateTime<Utc>,
    pub last_add_at: Option<DateTime<Utc>>,
    pub last_add_spread_pct: Option<f64>,
    pub open_slices: usize,
    pub pending_maker_orders: usize,
    pub pending_hedge_orders: usize,
    pub unknown_order_state: bool,
    pub pending_repair: bool,
}

impl RoutePosition {
    pub fn leg_imbalance_usdt(&self) -> f64 {
        (self.long_base_quantity * self.long_avg_entry_price
            - self.short_base_quantity * self.short_avg_entry_price)
            .abs()
    }

    pub fn hedged_open(route_id: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            route_id: route_id.into(),
            state: RouteState::Open,
            long_exchange: ExchangeId::new("mexc"),
            short_exchange: ExchangeId::new("bybit"),
            canonical_symbol: CanonicalSymbol::new("HUSDT", "USDT"),
            long_base_quantity: 0.0,
            short_base_quantity: 0.0,
            long_avg_entry_price: 0.0,
            short_avg_entry_price: 0.0,
            weighted_open_spread_pct: 0.0,
            current_notional_usdt: 0.0,
            cumulative_funding_pnl_usdt: 0.0,
            cumulative_fee_usdt: 0.0,
            opened_at: now,
            last_add_at: None,
            last_add_spread_pct: None,
            open_slices: 0,
            pending_maker_orders: 0,
            pending_hedge_orders: 0,
            unknown_order_state: false,
            pending_repair: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskSnapshot {
    pub private_stream_ready: bool,
    pub precision_ready: bool,
    pub account_position_ready: bool,
    pub no_unmanaged_position: bool,
    pub symbol_cooling_down: bool,
    pub pending_repair: bool,
    pub unknown_order_state: bool,
    pub mmr_pct: f64,
    pub adl_pct: f64,
    pub liquidation_buffer_pct: f64,
    pub single_leg_exposure_ms: u64,
}

impl RiskSnapshot {
    pub fn ready_for_open(self, risk: &RiskConfig) -> Result<(), EvaluationRejectReason> {
        if !self.private_stream_ready {
            return Err(EvaluationRejectReason::PrivateStreamNotReady);
        }
        if !self.precision_ready {
            return Err(EvaluationRejectReason::PrecisionNotReady);
        }
        if !self.account_position_ready {
            return Err(EvaluationRejectReason::AccountPositionNotReady);
        }
        if !self.no_unmanaged_position {
            return Err(EvaluationRejectReason::UnmanagedPosition);
        }
        if self.symbol_cooling_down {
            return Err(EvaluationRejectReason::SymbolCoolingDown);
        }
        if self.pending_repair {
            return Err(EvaluationRejectReason::PendingRepair);
        }
        if self.unknown_order_state {
            return Err(EvaluationRejectReason::UnknownOrderState);
        }
        if self.mmr_pct > risk.max_mmr_pct {
            return Err(EvaluationRejectReason::MmrTooHigh);
        }
        if self.adl_pct > risk.max_adl_pct {
            return Err(EvaluationRejectReason::AdlTooHigh);
        }
        if self.liquidation_buffer_pct < risk.min_liquidation_buffer_pct {
            return Err(EvaluationRejectReason::LiquidationBufferTooLow);
        }
        Ok(())
    }

    pub fn risk_close_reason(
        self,
        thresholds: &ThresholdsConfig,
        net_funding_rate: f64,
    ) -> Option<CloseReason> {
        if self.pending_repair || self.single_leg_exposure_ms > 0 {
            return Some(CloseReason::SingleLegExposure);
        }
        if self.unknown_order_state {
            return Some(CloseReason::UnknownOrderState);
        }
        if net_funding_rate < -thresholds.max_adverse_funding_rate {
            return Some(CloseReason::FundingReversed);
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationDecision {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationRejectReason {
    RouteDisabled,
    InvalidBook,
    StaleBook,
    FundingMissing,
    FundingStale,
    FundingMarkPriceMissing,
    FundingNextTimeMissing,
    SpreadThresholdNotMet,
    TargetAlreadyReached,
    FundingBelowThreshold,
    ImmediateEdgeTooLow,
    ExpectedTotalEdgeTooLow,
    PositionFull,
    NextOrderBelowMinimum,
    PendingMakerOrder,
    PendingHedgeOrder,
    PendingRepair,
    UnknownOrderState,
    LegImbalanceTooHigh,
    AddStepNotReached,
    AddIntervalNotReached,
    MaxSlicesReached,
    PrivateStreamNotReady,
    PrecisionNotReady,
    AccountPositionNotReady,
    UnmanagedPosition,
    SymbolCoolingDown,
    MmrTooHigh,
    AdlTooHigh,
    LiquidationBufferTooLow,
    ExecutionStyleNotAllowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    TargetSpreadReached,
    FundingReversed,
    SingleLegExposure,
    UnknownOrderState,
    ManualKillSwitch,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeBreakdown {
    pub spread_pct: f64,
    pub immediate_open_edge_pct: f64,
    pub expected_target_spread_edge_pct: f64,
    pub expected_funding_edge_pct: f64,
    pub expected_fee_pct: f64,
    pub expected_slippage_pct: f64,
    pub expected_total_edge_pct: f64,
    pub net_funding_rate: f64,
    pub net_funding_rate_per_hour: f64,
    pub funding_rate_basis_hours: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenEvaluation {
    pub decision: EvaluationDecision,
    pub reject_reason: Option<EvaluationRejectReason>,
    pub route_id: String,
    pub selected_open_style: Option<OpenExecutionStyle>,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub target_notional_usdt: f64,
    pub edge: Option<EdgeBreakdown>,
    pub orders: Vec<OrderDraft>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CloseEvaluation {
    pub should_close: bool,
    pub reason: Option<CloseReason>,
    pub selected_close_style: CloseExecutionStyle,
    pub spread_pct: f64,
    pub estimated_spread_pnl_usdt: f64,
    pub orders: Vec<OrderDraft>,
    pub observed_at: DateTime<Utc>,
}

pub fn spread_pct(leg_a_price: f64, leg_b_price: f64) -> f64 {
    (leg_b_price - leg_a_price) / leg_a_price.max(1e-12)
}

pub fn leg_funding(position_side: PositionSide, notional_usdt: f64, funding_rate: f64) -> f64 {
    let funding = notional_usdt.abs() * funding_rate;
    match position_side {
        PositionSide::Long => -funding,
        PositionSide::Short => funding,
    }
}

pub fn net_funding_rate(long_funding_rate: f64, short_funding_rate: f64) -> f64 {
    -long_funding_rate + short_funding_rate
}

pub fn funding_rate_per_hour(funding_rate: f64, funding_interval_hours: f64) -> f64 {
    funding_rate / funding_interval_hours.max(1e-12)
}

pub fn net_funding_rate_per_hour(
    long_funding_rate: f64,
    long_funding_interval_hours: f64,
    short_funding_rate: f64,
    short_funding_interval_hours: f64,
) -> f64 {
    -funding_rate_per_hour(long_funding_rate, long_funding_interval_hours)
        + funding_rate_per_hour(short_funding_rate, short_funding_interval_hours)
}

pub fn net_funding_rate_for_basis(
    long_funding_rate: f64,
    long_funding_interval_hours: f64,
    short_funding_rate: f64,
    short_funding_interval_hours: f64,
    basis_hours: f64,
) -> f64 {
    net_funding_rate_per_hour(
        long_funding_rate,
        long_funding_interval_hours,
        short_funding_rate,
        short_funding_interval_hours,
    ) * basis_hours.max(1e-12)
}

pub fn route_sides(route: &RouteConfig) -> (ExchangeId, ExchangeId) {
    match route.target_direction {
        SpreadTargetDirection::Decrease => (route.leg_a(), route.leg_b()),
        SpreadTargetDirection::Increase => (route.leg_b(), route.leg_a()),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_open(
    route: &RouteConfig,
    leg_a_book: &OrderBookTop,
    leg_b_book: &OrderBookTop,
    leg_a_precision: SymbolPrecision,
    leg_b_precision: SymbolPrecision,
    leg_a_fees: FeeRates,
    leg_b_fees: FeeRates,
    leg_a_funding: Option<&FundingSnapshot>,
    leg_b_funding: Option<&FundingSnapshot>,
    current_position_notional: f64,
    risk_snapshot: RiskSnapshot,
    config: &FundingSpreadExpansionMakerConfig,
    now: DateTime<Utc>,
) -> OpenEvaluation {
    let (long_exchange, short_exchange) = route_sides(route);
    let rejected = |reason| OpenEvaluation {
        decision: EvaluationDecision::Rejected,
        reject_reason: Some(reason),
        route_id: route.route_id.clone(),
        selected_open_style: None,
        long_exchange: long_exchange.clone(),
        short_exchange: short_exchange.clone(),
        target_notional_usdt: 0.0,
        edge: None,
        orders: Vec::new(),
        observed_at: now,
    };

    if !route.enabled {
        return rejected(EvaluationRejectReason::RouteDisabled);
    }
    if let Err(reason) = risk_snapshot.ready_for_open(&config.risk) {
        return rejected(reason);
    }
    if !leg_a_book.is_valid(config.risk.min_orderbook_levels)
        || !leg_b_book.is_valid(config.risk.min_orderbook_levels)
    {
        return rejected(EvaluationRejectReason::InvalidBook);
    }
    if !leg_a_book.is_fresh(now, config.risk.max_book_age_ms)
        || !leg_b_book.is_fresh(now, config.risk.max_book_age_ms)
    {
        return rejected(EvaluationRejectReason::StaleBook);
    }

    let (Some(leg_a_funding), Some(leg_b_funding)) = (leg_a_funding, leg_b_funding) else {
        return rejected(EvaluationRejectReason::FundingMissing);
    };
    if config.funding.require_funding_snapshot
        && (!leg_a_funding.is_fresh(now, config.funding.max_funding_snapshot_age_ms)
            || !leg_b_funding.is_fresh(now, config.funding.max_funding_snapshot_age_ms))
    {
        return rejected(EvaluationRejectReason::FundingStale);
    }
    if config.funding.require_mark_price
        && (leg_a_funding.mark_price.is_none() || leg_b_funding.mark_price.is_none())
    {
        return rejected(EvaluationRejectReason::FundingMarkPriceMissing);
    }
    if config.funding.require_next_funding_time
        && (leg_a_funding.next_funding_time.is_none() || leg_b_funding.next_funding_time.is_none())
    {
        return rejected(EvaluationRejectReason::FundingNextTimeMissing);
    }

    let leg_a_mid = mid_price(leg_a_book);
    let leg_b_mid = mid_price(leg_b_book);
    let spread = spread_pct(leg_a_mid, leg_b_mid);
    if !route
        .target_direction
        .open_threshold_met(spread, config.thresholds.open_spread_pct)
    {
        return rejected(EvaluationRejectReason::SpreadThresholdNotMet);
    }
    if route
        .target_direction
        .target_close_met(spread, config.thresholds.target_close_spread_pct)
    {
        return rejected(EvaluationRejectReason::TargetAlreadyReached);
    }
    if current_position_notional >= config.sizing.max_position_notional_usdt {
        return rejected(EvaluationRejectReason::PositionFull);
    }

    let target_notional =
        next_order_notional(current_position_notional, &config.sizing).unwrap_or(0.0);
    if target_notional <= 0.0 {
        return rejected(EvaluationRejectReason::NextOrderBelowMinimum);
    }

    let (long_funding_rate, long_interval_hours, short_funding_rate, short_interval_hours) =
        if long_exchange == route.leg_a() {
            (
                leg_a_funding.funding_rate,
                leg_a_funding.funding_interval_hours,
                leg_b_funding.funding_rate,
                leg_b_funding.funding_interval_hours,
            )
        } else {
            (
                leg_b_funding.funding_rate,
                leg_b_funding.funding_interval_hours,
                leg_a_funding.funding_rate,
                leg_a_funding.funding_interval_hours,
            )
        };
    let net_funding = net_funding_rate_for_basis(
        long_funding_rate,
        long_interval_hours,
        short_funding_rate,
        short_interval_hours,
        config.funding.funding_rate_basis_hours,
    );
    let net_funding_hourly = net_funding_rate_per_hour(
        long_funding_rate,
        long_interval_hours,
        short_funding_rate,
        short_interval_hours,
    );
    if net_funding < config.thresholds.min_net_funding_rate {
        return rejected(EvaluationRejectReason::FundingBelowThreshold);
    }

    let style = select_open_style(&config.execution).unwrap_or(OpenExecutionStyle::MakerTaker);
    let edge = estimate_edge(
        route,
        spread,
        target_notional,
        net_funding,
        net_funding_hourly,
        style,
        leg_a_fees,
        leg_b_fees,
        &config.thresholds,
        &config.funding,
        &config.execution,
    );
    if !config.thresholds.allow_negative_immediate_edge && edge.immediate_open_edge_pct < 0.0 {
        return rejected(EvaluationRejectReason::ImmediateEdgeTooLow);
    }
    if edge.immediate_open_edge_pct < config.thresholds.min_immediate_open_edge_pct {
        return rejected(EvaluationRejectReason::ImmediateEdgeTooLow);
    }
    if edge.expected_total_edge_pct < config.thresholds.min_expected_total_edge_pct {
        return rejected(EvaluationRejectReason::ExpectedTotalEdgeTooLow);
    }

    let orders = build_open_orders(
        route,
        leg_a_book,
        leg_b_book,
        leg_a_precision,
        leg_b_precision,
        target_notional,
        style,
        &config.execution,
    );
    OpenEvaluation {
        decision: EvaluationDecision::Accepted,
        reject_reason: None,
        route_id: route.route_id.clone(),
        selected_open_style: Some(style),
        long_exchange,
        short_exchange,
        target_notional_usdt: target_notional,
        edge: Some(edge),
        orders,
        observed_at: now,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_add(
    route: &RouteConfig,
    position: &RoutePosition,
    leg_a_book: &OrderBookTop,
    leg_b_book: &OrderBookTop,
    leg_a_precision: SymbolPrecision,
    leg_b_precision: SymbolPrecision,
    leg_a_fees: FeeRates,
    leg_b_fees: FeeRates,
    leg_a_funding: Option<&FundingSnapshot>,
    leg_b_funding: Option<&FundingSnapshot>,
    risk_snapshot: RiskSnapshot,
    config: &FundingSpreadExpansionMakerConfig,
    now: DateTime<Utc>,
) -> OpenEvaluation {
    let (long_exchange, short_exchange) = route_sides(route);
    let rejected = |reason| OpenEvaluation {
        decision: EvaluationDecision::Rejected,
        reject_reason: Some(reason),
        route_id: route.route_id.clone(),
        selected_open_style: None,
        long_exchange: long_exchange.clone(),
        short_exchange: short_exchange.clone(),
        target_notional_usdt: 0.0,
        edge: None,
        orders: Vec::new(),
        observed_at: now,
    };

    if position.pending_maker_orders > 0 {
        return rejected(EvaluationRejectReason::PendingMakerOrder);
    }
    if position.pending_hedge_orders > 0 {
        return rejected(EvaluationRejectReason::PendingHedgeOrder);
    }
    if position.pending_repair || risk_snapshot.pending_repair {
        return rejected(EvaluationRejectReason::PendingRepair);
    }
    if position.unknown_order_state || risk_snapshot.unknown_order_state {
        return rejected(EvaluationRejectReason::UnknownOrderState);
    }
    if position.leg_imbalance_usdt() > config.sizing.max_leg_imbalance_usdt {
        return rejected(EvaluationRejectReason::LegImbalanceTooHigh);
    }
    if position.open_slices >= config.adding.max_open_slices_per_route {
        return rejected(EvaluationRejectReason::MaxSlicesReached);
    }

    let spread = spread_pct(mid_price(leg_a_book), mid_price(leg_b_book));
    if route
        .target_direction
        .target_close_met(spread, config.thresholds.target_close_spread_pct)
    {
        return rejected(EvaluationRejectReason::TargetAlreadyReached);
    }
    if !add_trigger_met(
        route.target_direction,
        spread,
        position,
        &config.adding,
        now,
    ) {
        return rejected(match config.adding.add_mode {
            AddMode::LadderOnSpread => EvaluationRejectReason::AddStepNotReached,
            AddMode::SteadyFunding | AddMode::Hybrid => {
                EvaluationRejectReason::AddIntervalNotReached
            }
        });
    }

    evaluate_open(
        route,
        leg_a_book,
        leg_b_book,
        leg_a_precision,
        leg_b_precision,
        leg_a_fees,
        leg_b_fees,
        leg_a_funding,
        leg_b_funding,
        position.current_notional_usdt,
        risk_snapshot,
        config,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_close(
    route: &RouteConfig,
    position: &RoutePosition,
    leg_a_book: &OrderBookTop,
    leg_b_book: &OrderBookTop,
    leg_a_precision: SymbolPrecision,
    leg_b_precision: SymbolPrecision,
    risk_snapshot: RiskSnapshot,
    net_funding_rate: f64,
    config: &FundingSpreadExpansionMakerConfig,
    now: DateTime<Utc>,
) -> CloseEvaluation {
    let spread = spread_pct(mid_price(leg_a_book), mid_price(leg_b_book));
    let risk_reason = risk_snapshot.risk_close_reason(&config.thresholds, net_funding_rate);
    let target_met = route
        .target_direction
        .target_close_met(spread, config.thresholds.target_close_spread_pct);
    let reason = risk_reason.or_else(|| target_met.then_some(CloseReason::TargetSpreadReached));
    let should_close = reason.is_some();
    let selected_close_style = if risk_reason.is_some() {
        config.execution.risk_close_style
    } else if target_met {
        select_close_style(&config.execution, risk_snapshot)
    } else {
        config.execution.fallback_close_style
    };
    let estimated_spread_pnl_usdt = position.current_notional_usdt
        * route
            .target_direction
            .directional_move_from_open(position.weighted_open_spread_pct, spread);
    let orders = if should_close {
        build_close_orders(
            route,
            position,
            leg_a_book,
            leg_b_book,
            leg_a_precision,
            leg_b_precision,
            selected_close_style,
            reason,
            &config.execution,
        )
    } else {
        Vec::new()
    };
    CloseEvaluation {
        should_close,
        reason,
        selected_close_style,
        spread_pct: spread,
        estimated_spread_pnl_usdt,
        orders,
        observed_at: now,
    }
}

pub fn next_order_notional(current_position_notional: f64, sizing: &SizingConfig) -> Option<f64> {
    let remaining = sizing.max_position_notional_usdt - current_position_notional.max(0.0);
    let next = sizing.max_order_notional_usdt.min(remaining);
    (next >= sizing.min_order_notional_usdt).then_some(next)
}

fn estimate_edge(
    route: &RouteConfig,
    spread: f64,
    target_notional: f64,
    net_funding: f64,
    net_funding_hourly: f64,
    style: OpenExecutionStyle,
    leg_a_fees: FeeRates,
    leg_b_fees: FeeRates,
    thresholds: &ThresholdsConfig,
    funding: &FundingConfig,
    execution: &ExecutionConfig,
) -> EdgeBreakdown {
    let target_spread_edge = route
        .target_direction
        .directional_move_from_open(spread, thresholds.target_close_spread_pct)
        .max(0.0);
    let expected_horizon_hours = if funding.expected_funding_horizon_hours > 0.0 {
        funding.expected_funding_horizon_hours
    } else {
        funding.funding_rate_basis_hours * funding.expected_funding_windows as f64
    };
    let funding_edge = net_funding_hourly
        * expected_horizon_hours
        * (1.0 - funding.funding_decay_haircut_pct.clamp(0.0, 1.0));
    let fee_pct = match style {
        OpenExecutionStyle::DualMaker => {
            leg_a_fees.maker + leg_b_fees.maker + leg_a_fees.maker + leg_b_fees.maker
        }
        OpenExecutionStyle::MakerTaker | OpenExecutionStyle::AdaptiveOpen => {
            leg_a_fees.maker + leg_b_fees.taker + leg_a_fees.taker + leg_b_fees.taker
        }
        OpenExecutionStyle::DualTaker => {
            leg_a_fees.taker + leg_b_fees.taker + leg_a_fees.taker + leg_b_fees.taker
        }
    };
    let expected_slippage = execution.hedge_taker_slippage_pct + execution.close_taker_slippage_pct;
    let immediate_open_edge = -fee_pct / 2.0 - execution.hedge_taker_slippage_pct;
    let expected_total = target_spread_edge + funding_edge
        - fee_pct
        - expected_slippage
        - thresholds.funding_uncertainty_buffer_pct
        - thresholds.repair_buffer_pct;
    EdgeBreakdown {
        spread_pct: spread,
        immediate_open_edge_pct: immediate_open_edge,
        expected_target_spread_edge_pct: target_spread_edge,
        expected_funding_edge_pct: funding_edge,
        expected_fee_pct: target_notional.max(1.0) * fee_pct / target_notional.max(1.0),
        expected_slippage_pct: expected_slippage,
        expected_total_edge_pct: expected_total,
        net_funding_rate: net_funding,
        net_funding_rate_per_hour: net_funding_hourly,
        funding_rate_basis_hours: funding.funding_rate_basis_hours,
    }
}

fn add_trigger_met(
    target_direction: SpreadTargetDirection,
    spread: f64,
    position: &RoutePosition,
    adding: &AddingConfig,
    now: DateTime<Utc>,
) -> bool {
    match adding.add_mode {
        AddMode::LadderOnSpread => position
            .last_add_spread_pct
            .or(Some(position.weighted_open_spread_pct))
            .is_some_and(|last| match target_direction {
                SpreadTargetDirection::Decrease => spread <= last - adding.add_step_pct,
                SpreadTargetDirection::Increase => spread >= last + adding.add_step_pct,
            }),
        AddMode::SteadyFunding => position
            .last_add_at
            .map(|last| {
                now.signed_duration_since(last).num_seconds() >= adding.min_add_interval_secs
            })
            .unwrap_or(true),
        AddMode::Hybrid => {
            add_trigger_met(
                target_direction,
                spread,
                position,
                &AddingConfig {
                    add_mode: AddMode::LadderOnSpread,
                    ..adding.clone()
                },
                now,
            ) || add_trigger_met(
                target_direction,
                spread,
                position,
                &AddingConfig {
                    add_mode: AddMode::SteadyFunding,
                    ..adding.clone()
                },
                now,
            )
        }
    }
}

fn select_open_style(execution: &ExecutionConfig) -> Option<OpenExecutionStyle> {
    let preferred = execution.preferred_open_style;
    if execution.allowed_open_styles.contains(&preferred)
        && (preferred != OpenExecutionStyle::DualMaker || execution.allow_dual_maker_open)
    {
        return Some(preferred);
    }
    execution
        .allowed_open_styles
        .iter()
        .copied()
        .find(|style| *style != OpenExecutionStyle::DualMaker || execution.allow_dual_maker_open)
}

fn select_close_style(
    execution: &ExecutionConfig,
    risk_snapshot: RiskSnapshot,
) -> CloseExecutionStyle {
    if risk_snapshot.pending_repair || risk_snapshot.unknown_order_state {
        return execution.fallback_close_style;
    }
    let preferred = execution.preferred_close_style;
    if execution.allowed_close_styles.contains(&preferred)
        && (preferred != CloseExecutionStyle::MakerMakerReduceOnly
            || execution.allow_dual_maker_close)
    {
        preferred
    } else {
        execution.fallback_close_style
    }
}

fn build_open_orders(
    route: &RouteConfig,
    leg_a_book: &OrderBookTop,
    leg_b_book: &OrderBookTop,
    leg_a_precision: SymbolPrecision,
    leg_b_precision: SymbolPrecision,
    target_notional: f64,
    style: OpenExecutionStyle,
    execution: &ExecutionConfig,
) -> Vec<OrderDraft> {
    let (long_exchange, short_exchange) = route_sides(route);
    let long_on_a = long_exchange == route.leg_a();
    let (long_book, short_book, long_precision, short_precision) = if long_on_a {
        (leg_a_book, leg_b_book, leg_a_precision, leg_b_precision)
    } else {
        (leg_b_book, leg_a_book, leg_b_precision, leg_a_precision)
    };
    let quantity = shared_quantity(
        target_notional,
        long_book.best_ask_price,
        long_precision,
        short_precision,
    );
    let maker_long = OrderDraft {
        exchange: long_exchange.clone(),
        canonical_symbol: route.canonical_symbol(),
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: long_book.best_ask_price,
        limit_price: long_precision
            .buy_limit(long_book.best_ask_price * (1.0 - execution.maker_price_offset_pct)),
        post_only: true,
        reduce_only: false,
        time_in_force: TimeInForce::PostOnly,
        intent_kind: OrderIntentKind::OpenMaker,
    };
    let taker_short = OrderDraft {
        exchange: short_exchange.clone(),
        canonical_symbol: route.canonical_symbol(),
        side: OrderSide::Sell,
        position_side: PositionSide::Short,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: short_book.best_bid_price,
        limit_price: short_precision
            .sell_limit(short_book.best_bid_price * (1.0 - execution.hedge_taker_slippage_pct)),
        post_only: false,
        reduce_only: false,
        time_in_force: TimeInForce::ImmediateOrCancel,
        intent_kind: match style {
            OpenExecutionStyle::DualTaker => OrderIntentKind::OpenTaker,
            _ => OrderIntentKind::OpenHedgeTaker,
        },
    };
    let maker_short = OrderDraft {
        exchange: short_exchange,
        canonical_symbol: route.canonical_symbol(),
        side: OrderSide::Sell,
        position_side: PositionSide::Short,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: short_book.best_bid_price,
        limit_price: short_precision
            .sell_limit(short_book.best_bid_price * (1.0 + execution.maker_price_offset_pct)),
        post_only: true,
        reduce_only: false,
        time_in_force: TimeInForce::PostOnly,
        intent_kind: OrderIntentKind::OpenMaker,
    };
    let taker_long = OrderDraft {
        exchange: long_exchange,
        canonical_symbol: route.canonical_symbol(),
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: long_book.best_ask_price,
        limit_price: long_precision
            .buy_limit(long_book.best_ask_price * (1.0 + execution.hedge_taker_slippage_pct)),
        post_only: false,
        reduce_only: false,
        time_in_force: TimeInForce::ImmediateOrCancel,
        intent_kind: OrderIntentKind::OpenTaker,
    };
    match style {
        OpenExecutionStyle::DualMaker => vec![maker_long, maker_short],
        OpenExecutionStyle::DualTaker => vec![taker_long, taker_short],
        OpenExecutionStyle::MakerTaker | OpenExecutionStyle::AdaptiveOpen => {
            vec![maker_long, taker_short]
        }
    }
}

fn build_close_orders(
    route: &RouteConfig,
    position: &RoutePosition,
    leg_a_book: &OrderBookTop,
    leg_b_book: &OrderBookTop,
    leg_a_precision: SymbolPrecision,
    leg_b_precision: SymbolPrecision,
    style: CloseExecutionStyle,
    reason: Option<CloseReason>,
    execution: &ExecutionConfig,
) -> Vec<OrderDraft> {
    let long_on_a = position.long_exchange == route.leg_a();
    let (long_book, short_book, long_precision, short_precision) = if long_on_a {
        (leg_a_book, leg_b_book, leg_a_precision, leg_b_precision)
    } else {
        (leg_b_book, leg_a_book, leg_b_precision, leg_a_precision)
    };
    if reason == Some(CloseReason::SingleLegExposure)
        && (position.long_base_quantity - position.short_base_quantity).abs() > 0.0
    {
        return build_single_leg_repair_close_orders(
            position,
            long_book,
            short_book,
            long_precision,
            short_precision,
            execution,
        );
    }
    let quantity = position
        .long_base_quantity
        .min(position.short_base_quantity)
        .max(0.0);
    let maker_long_close = OrderDraft {
        exchange: position.long_exchange.clone(),
        canonical_symbol: position.canonical_symbol.clone(),
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: long_book.best_bid_price,
        limit_price: long_precision
            .sell_limit(long_book.best_bid_price * (1.0 + execution.close_maker_price_offset_pct)),
        post_only: true,
        reduce_only: true,
        time_in_force: TimeInForce::PostOnly,
        intent_kind: OrderIntentKind::CloseMaker,
    };
    let maker_short_close = OrderDraft {
        exchange: position.short_exchange.clone(),
        canonical_symbol: position.canonical_symbol.clone(),
        side: OrderSide::Buy,
        position_side: PositionSide::Short,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: short_book.best_ask_price,
        limit_price: short_precision
            .buy_limit(short_book.best_ask_price * (1.0 - execution.close_maker_price_offset_pct)),
        post_only: true,
        reduce_only: true,
        time_in_force: TimeInForce::PostOnly,
        intent_kind: OrderIntentKind::CloseMaker,
    };
    let taker_long_close = OrderDraft {
        exchange: position.long_exchange.clone(),
        canonical_symbol: position.canonical_symbol.clone(),
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: long_book.best_bid_price,
        limit_price: long_precision
            .sell_limit(long_book.best_bid_price * (1.0 - execution.close_taker_slippage_pct)),
        post_only: false,
        reduce_only: true,
        time_in_force: TimeInForce::ImmediateOrCancel,
        intent_kind: OrderIntentKind::CloseTaker,
    };
    let taker_short_close = OrderDraft {
        exchange: position.short_exchange.clone(),
        canonical_symbol: position.canonical_symbol.clone(),
        side: OrderSide::Buy,
        position_side: PositionSide::Short,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: short_book.best_ask_price,
        limit_price: short_precision
            .buy_limit(short_book.best_ask_price * (1.0 + execution.close_taker_slippage_pct)),
        post_only: false,
        reduce_only: true,
        time_in_force: TimeInForce::ImmediateOrCancel,
        intent_kind: OrderIntentKind::CloseTaker,
    };
    match style {
        CloseExecutionStyle::MakerMakerReduceOnly | CloseExecutionStyle::AdaptiveClose => {
            vec![maker_long_close, maker_short_close]
        }
        CloseExecutionStyle::MakerTakerReduceOnly => vec![maker_long_close, taker_short_close],
        CloseExecutionStyle::DualTakerReduceOnly => vec![taker_long_close, taker_short_close],
    }
}

fn build_single_leg_repair_close_orders(
    position: &RoutePosition,
    long_book: &OrderBookTop,
    short_book: &OrderBookTop,
    long_precision: SymbolPrecision,
    short_precision: SymbolPrecision,
    execution: &ExecutionConfig,
) -> Vec<OrderDraft> {
    if position.long_base_quantity > position.short_base_quantity {
        let quantity = (position.long_base_quantity - position.short_base_quantity).max(0.0);
        return vec![OrderDraft {
            exchange: position.long_exchange.clone(),
            canonical_symbol: position.canonical_symbol.clone(),
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            base_quantity: quantity,
            order_quantity: quantity,
            reference_price: long_book.best_bid_price,
            limit_price: long_precision
                .sell_limit(long_book.best_bid_price * (1.0 - execution.close_taker_slippage_pct)),
            post_only: false,
            reduce_only: true,
            time_in_force: TimeInForce::ImmediateOrCancel,
            intent_kind: OrderIntentKind::CloseTaker,
        }];
    }
    let quantity = (position.short_base_quantity - position.long_base_quantity).max(0.0);
    vec![OrderDraft {
        exchange: position.short_exchange.clone(),
        canonical_symbol: position.canonical_symbol.clone(),
        side: OrderSide::Buy,
        position_side: PositionSide::Short,
        base_quantity: quantity,
        order_quantity: quantity,
        reference_price: short_book.best_ask_price,
        limit_price: short_precision
            .buy_limit(short_book.best_ask_price * (1.0 + execution.close_taker_slippage_pct)),
        post_only: false,
        reduce_only: true,
        time_in_force: TimeInForce::ImmediateOrCancel,
        intent_kind: OrderIntentKind::CloseTaker,
    }]
}

fn shared_quantity(
    target_notional: f64,
    reference_price: f64,
    first_precision: SymbolPrecision,
    second_precision: SymbolPrecision,
) -> f64 {
    let raw = target_notional / reference_price.max(1e-12);
    let first = first_precision.normalize_quantity_down(raw);
    let second = second_precision.normalize_quantity_down(raw);
    first
        .min(second)
        .max(first_precision.min_quantity)
        .max(second_precision.min_quantity)
}

fn mid_price(book: &OrderBookTop) -> f64 {
    (book.best_bid_price + book.best_ask_price) / 2.0
}

fn parse_symbol_pair(symbol: &str) -> CanonicalSymbol {
    let normalized = symbol.trim().to_ascii_uppercase().replace(['-', '_'], "/");
    if let Some((base, quote)) = normalized.split_once('/') {
        return CanonicalSymbol::new(base, quote);
    }
    for quote in ["USDT", "USDC", "USD"] {
        if normalized.ends_with(quote) && normalized.len() > quote.len() {
            return CanonicalSymbol::new(&normalized[..normalized.len() - quote.len()], quote);
        }
    }
    CanonicalSymbol::new(normalized, "USDT")
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 || !step.is_finite() {
        return value;
    }
    (value / step).floor() * step
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 || !step.is_finite() {
        return value;
    }
    (value / step).ceil() * step
}

pub fn default_snapshot_payload() -> BTreeMap<String, serde_json::Value> {
    BTreeMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-12T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn book(exchange: &str, bid: f64, ask: f64) -> OrderBookTop {
        OrderBookTop {
            exchange: ExchangeId::new(exchange),
            canonical_symbol: CanonicalSymbol::new("ESPORTS", "USDT"),
            best_bid_price: bid,
            best_bid_quantity: 1000.0,
            best_ask_price: ask,
            best_ask_quantity: 1000.0,
            levels: 1,
            received_at: now(),
        }
    }

    fn funding_with_interval(exchange: &str, rate: f64, interval_hours: f64) -> FundingSnapshot {
        FundingSnapshot {
            exchange: ExchangeId::new(exchange),
            canonical_symbol: CanonicalSymbol::new("ESPORTS", "USDT"),
            funding_rate: rate,
            predicted_funding_rate: None,
            mark_price: Some(0.15),
            index_price: Some(0.15),
            open_interest: None,
            turnover_24h: None,
            volume_24h: None,
            funding_interval_hours: interval_hours,
            next_funding_time: Some(now() + Duration::hours(4)),
            updated_at: now(),
        }
    }

    fn funding(exchange: &str, rate: f64) -> FundingSnapshot {
        funding_with_interval(exchange, rate, 8.0)
    }

    fn ready_risk() -> RiskSnapshot {
        RiskSnapshot {
            private_stream_ready: true,
            precision_ready: true,
            account_position_ready: true,
            no_unmanaged_position: true,
            symbol_cooling_down: false,
            pending_repair: false,
            unknown_order_state: false,
            mmr_pct: 5.0,
            adl_pct: 10.0,
            liquidation_buffer_pct: 100.0,
            single_leg_exposure_ms: 0,
        }
    }

    fn config() -> FundingSpreadExpansionMakerConfig {
        let mut config = FundingSpreadExpansionMakerConfig::default();
        config.routes[0].symbol = "ESPORTS/USDT".to_string();
        config.thresholds.min_expected_total_edge_pct = -0.01;
        config
    }

    #[test]
    fn esports_example_spread_should_match_negative_open_direction() {
        let spread = spread_pct(0.1558, 0.1486);
        assert!((spread - -0.046213).abs() < 0.0001);
        assert!(SpreadTargetDirection::Decrease.open_threshold_met(spread, -0.03));
    }

    #[test]
    fn target_close_should_trigger_at_minus_eight_percent() {
        let direction = SpreadTargetDirection::Decrease;
        assert!(direction.target_close_met(-0.0801, -0.08));
        assert!(!direction.target_close_met(-0.06, -0.08));
    }

    #[test]
    fn funding_direction_should_charge_long_and_pay_short() {
        assert_eq!(leg_funding(PositionSide::Long, 100.0, 0.01), -1.0);
        assert_eq!(leg_funding(PositionSide::Short, 100.0, 0.01), 1.0);
        assert!((net_funding_rate(-0.01, 0.02) - 0.03).abs() < 1e-12);
    }

    #[test]
    fn funding_rate_should_normalize_mixed_settlement_intervals() {
        let net_hourly = net_funding_rate_per_hour(-0.001, 1.0, 0.008, 8.0);
        let net_basis = net_funding_rate_for_basis(-0.001, 1.0, 0.008, 8.0, 8.0);
        assert!((net_hourly - 0.002).abs() < 1e-12);
        assert!((net_basis - 0.016).abs() < 1e-12);
    }

    #[test]
    fn open_should_reject_when_normalized_funding_is_negative_even_if_raw_sum_looks_positive() {
        let mut config = config();
        config.thresholds.min_net_funding_rate = 0.0;
        config.thresholds.min_expected_total_edge_pct = -1.0;
        let eval = evaluate_open(
            &config.routes[0],
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&funding_with_interval("bitget", 0.004, 1.0)),
            Some(&funding_with_interval("binance", 0.008, 8.0)),
            0.0,
            ready_risk(),
            &config,
            now(),
        );
        assert_eq!(
            eval.reject_reason,
            Some(EvaluationRejectReason::FundingBelowThreshold)
        );
    }

    #[test]
    fn expected_funding_edge_should_use_hourly_rate_times_horizon() {
        let mut config = config();
        config.funding.funding_rate_basis_hours = 8.0;
        config.funding.expected_funding_horizon_hours = 16.0;
        config.funding.funding_decay_haircut_pct = 0.5;
        let eval = evaluate_open(
            &config.routes[0],
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&funding_with_interval("bitget", -0.001, 1.0)),
            Some(&funding_with_interval("binance", 0.008, 8.0)),
            0.0,
            ready_risk(),
            &config,
            now(),
        );
        assert_eq!(eval.decision, EvaluationDecision::Accepted);
        let edge = eval.edge.unwrap();
        assert!((edge.net_funding_rate_per_hour - 0.002).abs() < 1e-12);
        assert!((edge.net_funding_rate - 0.016).abs() < 1e-12);
        assert!((edge.expected_funding_edge_pct - 0.016).abs() < 1e-12);
        assert_eq!(edge.funding_rate_basis_hours, 8.0);
    }

    #[test]
    fn open_should_reject_missing_mark_price_when_funding_config_requires_it() {
        let config = config();
        let mut leg_a_funding = funding("bitget", -0.015);
        leg_a_funding.mark_price = None;
        let eval = evaluate_open(
            &config.routes[0],
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&leg_a_funding),
            Some(&funding("binance", 0.015)),
            0.0,
            ready_risk(),
            &config,
            now(),
        );

        assert_eq!(
            eval.reject_reason,
            Some(EvaluationRejectReason::FundingMarkPriceMissing)
        );
    }

    #[test]
    fn open_should_reject_missing_next_funding_time_when_funding_config_requires_it() {
        let config = config();
        let mut leg_b_funding = funding("binance", 0.015);
        leg_b_funding.next_funding_time = None;
        let eval = evaluate_open(
            &config.routes[0],
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&funding("bitget", -0.015)),
            Some(&leg_b_funding),
            0.0,
            ready_risk(),
            &config,
            now(),
        );

        assert_eq!(
            eval.reject_reason,
            Some(EvaluationRejectReason::FundingNextTimeMissing)
        );
    }

    #[test]
    fn next_order_notional_should_respect_range_and_position_cap() {
        let sizing = SizingConfig::default();
        assert_eq!(next_order_notional(8.0, &sizing), Some(10.0));
        assert_eq!(next_order_notional(18.0, &sizing), None);
    }

    #[test]
    fn open_should_allow_negative_immediate_edge_when_total_edge_passes() {
        let config = config();
        let eval = evaluate_open(
            &config.routes[0],
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&funding("bitget", -0.015)),
            Some(&funding("binance", 0.015)),
            0.0,
            ready_risk(),
            &config,
            now(),
        );
        assert_eq!(eval.decision, EvaluationDecision::Accepted);
        assert!(eval.edge.unwrap().immediate_open_edge_pct < 0.0);
    }

    #[test]
    fn add_should_wait_for_ladder_step_and_allow_minus_six_before_target() {
        let config = config();
        let mut position = RoutePosition::hedged_open("route", now());
        position.long_base_quantity = 54.0;
        position.short_base_quantity = 54.0;
        position.long_avg_entry_price = 0.1558;
        position.short_avg_entry_price = 0.1486;
        position.weighted_open_spread_pct = -0.0469;
        position.current_notional_usdt = 8.0;
        position.last_add_spread_pct = Some(-0.0469);
        let eval = evaluate_add(
            &config.routes[0],
            &position,
            &book("bitget", 0.1599, 0.1601),
            &book("binance", 0.1503, 0.1505),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&funding("bitget", -0.015)),
            Some(&funding("binance", 0.015)),
            ready_risk(),
            &config,
            now(),
        );
        assert_eq!(eval.decision, EvaluationDecision::Accepted);
        assert_eq!(eval.target_notional_usdt, 10.0);
    }

    #[test]
    fn add_should_block_unknown_order_state() {
        let config = config();
        let mut position = RoutePosition::hedged_open("route", now());
        position.unknown_order_state = true;
        let eval = evaluate_add(
            &config.routes[0],
            &position,
            &book("bitget", 0.1599, 0.1601),
            &book("binance", 0.1503, 0.1505),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            Some(&funding("bitget", -0.015)),
            Some(&funding("binance", 0.015)),
            ready_risk(),
            &config,
            now(),
        );
        assert_eq!(
            eval.reject_reason,
            Some(EvaluationRejectReason::UnknownOrderState)
        );
    }

    #[test]
    fn close_should_choose_maker_maker_for_target_and_dual_taker_for_risk() {
        let config = config();
        let mut position = RoutePosition::hedged_open("route", now());
        position.long_exchange = ExchangeId::new("bitget");
        position.short_exchange = ExchangeId::new("binance");
        position.long_base_quantity = 54.0;
        position.short_base_quantity = 54.0;
        position.weighted_open_spread_pct = -0.0469;
        position.current_notional_usdt = 8.0;
        let target = evaluate_close(
            &config.routes[0],
            &position,
            &book("bitget", 0.1600, 0.1602),
            &book("binance", 0.1470, 0.1472),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            ready_risk(),
            0.02,
            &config,
            now(),
        );
        assert!(target.should_close);
        assert_eq!(target.reason, Some(CloseReason::TargetSpreadReached));
        assert_eq!(
            target.selected_close_style,
            CloseExecutionStyle::MakerMakerReduceOnly
        );
        assert!(target.orders.iter().all(|order| order.reduce_only));

        let mut risk = ready_risk();
        risk.unknown_order_state = true;
        let risk_close = evaluate_close(
            &config.routes[0],
            &position,
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            risk,
            0.02,
            &config,
            now(),
        );
        assert_eq!(risk_close.reason, Some(CloseReason::UnknownOrderState));
        assert_eq!(
            risk_close.selected_close_style,
            CloseExecutionStyle::DualTakerReduceOnly
        );
    }

    #[test]
    fn single_leg_risk_close_should_reduce_only_close_exposed_leg() {
        let config = config();
        let mut position = RoutePosition::hedged_open("route", now());
        position.long_exchange = ExchangeId::new("bitget");
        position.short_exchange = ExchangeId::new("binance");
        position.long_base_quantity = 54.0;
        position.short_base_quantity = 0.0;
        position.pending_repair = true;
        position.current_notional_usdt = 8.0;
        let mut risk = ready_risk();
        risk.pending_repair = true;
        let close = evaluate_close(
            &config.routes[0],
            &position,
            &book("bitget", 0.1557, 0.1559),
            &book("binance", 0.1485, 0.1487),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            risk,
            0.02,
            &config,
            now(),
        );
        assert_eq!(close.reason, Some(CloseReason::SingleLegExposure));
        assert_eq!(close.orders.len(), 1);
        assert_eq!(close.orders[0].side, OrderSide::Sell);
        assert_eq!(close.orders[0].position_side, PositionSide::Long);
        assert!(close.orders[0].reduce_only);
        assert!(!close.orders[0].post_only);
        assert_eq!(close.orders[0].base_quantity, 54.0);
    }

    #[test]
    fn unrealized_loss_should_not_trigger_close_without_target_or_risk() {
        let config = config();
        let mut position = RoutePosition::hedged_open("route", now());
        position.long_exchange = ExchangeId::new("bitget");
        position.short_exchange = ExchangeId::new("binance");
        position.long_base_quantity = 54.0;
        position.short_base_quantity = 54.0;
        position.weighted_open_spread_pct = -0.0469;
        position.current_notional_usdt = 8.0;
        let close = evaluate_close(
            &config.routes[0],
            &position,
            &book("bitget", 0.1500, 0.1502),
            &book("binance", 0.1470, 0.1472),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            ready_risk(),
            0.02,
            &config,
            now(),
        );
        assert!(!close.should_close);
    }
}
