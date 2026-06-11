use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    pub fn parse(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_uppercase().replace(['-', '_'], "/");
        if let Some((base, quote)) = normalized.split_once('/') {
            return (!base.is_empty() && !quote.is_empty()).then(|| Self::new(base, quote));
        }
        for quote in ["USDT", "USDC", "USD"] {
            if normalized.ends_with(quote) && normalized.len() > quote.len() {
                return Some(Self::new(
                    &normalized[..normalized.len() - quote.len()],
                    quote,
                ));
            }
        }
        None
    }

    pub fn as_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFuturesMarketType {
    Spot,
    Perpetual,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstrumentKey {
    pub exchange: String,
    pub market_type: SpotFuturesMarketType,
    pub canonical_symbol: CanonicalSymbol,
}

impl InstrumentKey {
    pub fn new(
        exchange: impl Into<String>,
        market_type: SpotFuturesMarketType,
        canonical_symbol: CanonicalSymbol,
    ) -> Self {
        Self {
            exchange: exchange.into().trim().to_ascii_lowercase(),
            market_type,
            canonical_symbol,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SymbolPrecision {
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_base_quantity: f64,
    pub min_notional_usdt: f64,
    #[serde(default = "default_contract_size")]
    pub contract_size: f64,
}

impl Default for SymbolPrecision {
    fn default() -> Self {
        Self {
            price_tick: 0.0,
            quantity_step: 0.0,
            min_base_quantity: 0.0,
            min_notional_usdt: 0.0,
            contract_size: 1.0,
        }
    }
}

impl SymbolPrecision {
    pub fn floor_base_quantity(self, quantity: f64) -> f64 {
        floor_to_step(quantity.max(0.0), self.quantity_step)
    }

    pub fn ceil_base_quantity(self, quantity: f64) -> f64 {
        ceil_to_step(quantity.max(0.0), self.quantity_step)
    }

    pub fn buy_limit_price(self, price: f64) -> f64 {
        ceil_to_step(price.max(0.0), self.price_tick)
    }

    pub fn sell_limit_price(self, price: f64) -> f64 {
        floor_to_step(price.max(0.0), self.price_tick)
    }
}

fn default_contract_size() -> f64 {
    1.0
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PrecisionRegistry {
    rules: BTreeMap<InstrumentKey, SymbolPrecision>,
}

impl PrecisionRegistry {
    pub fn insert(&mut self, key: InstrumentKey, precision: SymbolPrecision) {
        self.rules.insert(key, precision);
    }

    pub fn get(&self, key: &InstrumentKey) -> SymbolPrecision {
        self.rules.get(key).copied().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookTop {
    pub instrument: InstrumentKey,
    pub best_bid_price: f64,
    pub best_bid_quantity: f64,
    pub best_ask_price: f64,
    pub best_ask_quantity: f64,
    pub levels: usize,
    pub received_at: DateTime<Utc>,
    pub sequence_gap: bool,
}

impl OrderBookTop {
    pub fn is_valid(&self, min_levels: usize) -> bool {
        self.levels >= min_levels.max(1)
            && !self.sequence_gap
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub instrument: InstrumentKey,
    pub funding_rate: f64,
    pub predicted_funding_rate: Option<f64>,
    pub funding_interval_hours: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

impl FundingSnapshot {
    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        now.signed_duration_since(self.observed_at)
            .num_milliseconds()
    }

    pub fn effective_rate_for_short(&self, use_predicted: bool) -> f64 {
        if use_predicted {
            self.predicted_funding_rate
                .map(|predicted| predicted.min(self.funding_rate))
                .unwrap_or(self.funding_rate)
        } else {
            self.funding_rate
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FeeRates {
    pub maker: f64,
    pub taker: f64,
}

impl Default for FeeRates {
    fn default() -> Self {
        Self {
            maker: 0.001,
            taker: 0.001,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesRoute {
    pub route_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub spot_exchange: String,
    pub perp_exchange: String,
}

impl SpotFuturesRoute {
    pub fn spot_key(&self) -> InstrumentKey {
        InstrumentKey::new(
            &self.spot_exchange,
            SpotFuturesMarketType::Spot,
            self.canonical_symbol.clone(),
        )
    }

    pub fn perp_key(&self) -> InstrumentKey {
        InstrumentKey::new(
            &self.perp_exchange,
            SpotFuturesMarketType::Perpetual,
            self.canonical_symbol.clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesSelectionConfig {
    pub min_open_basis_bps: f64,
    pub max_open_basis_bps: f64,
    pub min_open_net_edge_bps: f64,
    pub min_spot_top_depth_usdt: f64,
    pub min_perp_top_depth_usdt: f64,
    pub top_of_book_capacity_ratio: f64,
}

impl Default for SpotFuturesSelectionConfig {
    fn default() -> Self {
        Self {
            min_open_basis_bps: 10.0,
            max_open_basis_bps: 300.0,
            min_open_net_edge_bps: 20.0,
            min_spot_top_depth_usdt: 50.0,
            min_perp_top_depth_usdt: 50.0,
            top_of_book_capacity_ratio: 0.5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesFundingConfig {
    pub enabled: bool,
    pub require_next_funding_time: bool,
    pub max_funding_snapshot_age_ms: i64,
    pub expected_holding_hours: f64,
    pub no_open_before_adverse_funding_mins: i64,
    pub min_expected_funding_bps: f64,
    pub close_if_next_funding_bps_below: f64,
    pub use_predicted_funding_when_available: bool,
}

impl Default for SpotFuturesFundingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_next_funding_time: true,
            max_funding_snapshot_age_ms: 5_000,
            expected_holding_hours: 8.0,
            no_open_before_adverse_funding_mins: 20,
            min_expected_funding_bps: -5.0,
            close_if_next_funding_bps_below: -3.0,
            use_predicted_funding_when_available: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesExecutionConfig {
    pub allow_live_order_submission: bool,
    pub enable_spot_fill_readback_hedge: bool,
    pub spot_maker_order_ttl_ms: u64,
    pub spot_maker_price_offset_ticks: u32,
    pub spot_post_only_required: bool,
    pub perp_hedge_slippage_pct: f64,
    pub close_slippage_buffer_bps: f64,
    pub latency_buffer_bps: f64,
    pub safety_buffer_bps: f64,
    pub min_hedge_base_qty: f64,
}

impl Default for SpotFuturesExecutionConfig {
    fn default() -> Self {
        Self {
            allow_live_order_submission: false,
            enable_spot_fill_readback_hedge: false,
            spot_maker_order_ttl_ms: 3_000,
            spot_maker_price_offset_ticks: 1,
            spot_post_only_required: true,
            perp_hedge_slippage_pct: 0.003,
            close_slippage_buffer_bps: 5.0,
            latency_buffer_bps: 3.0,
            safety_buffer_bps: 5.0,
            min_hedge_base_qty: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFuturesSizingConfig {
    pub min_notional_usdt: f64,
    pub target_notional_usdt: f64,
    pub max_notional_usdt: f64,
    pub perp_leverage: f64,
}

impl Default for SpotFuturesSizingConfig {
    fn default() -> Self {
        Self {
            min_notional_usdt: 10.0,
            target_notional_usdt: 50.0,
            max_notional_usdt: 100.0,
            perp_leverage: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFuturesRejectReason {
    ExcludedBase,
    InvalidSpotBook,
    InvalidPerpBook,
    StaleSpotBook,
    StalePerpBook,
    FundingMissing,
    FundingStale,
    FundingTimeMissing,
    NearAdverseFundingSettlement,
    ExpectedFundingTooAdverse,
    BasisTooSmall,
    BasisTooLarge,
    InsufficientSpotDepth,
    InsufficientPerpDepth,
    QuantityTooSmall,
    NotionalTooSmall,
    BelowMinNetEdge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFuturesBundleStatus {
    Idle,
    SignalAccepted,
    SpotMakerPlaced,
    SpotMakerPartiallyFilled,
    SpotMakerFilled,
    PerpHedgeSubmitting,
    HedgedOpen,
    Closing,
    Closed,
    SpotMakerCanceledNoFill,
    HedgeFailedOneSidedSpotLong,
    UnknownOrderState,
    CloseOnly,
    RiskStopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFuturesOrderRole {
    SpotMakerBuy,
    PerpTakerShortHedge,
    SpotTakerCloseSell,
    PerpReduceOnlyCloseBuy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFuturesOrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesOrderDraft {
    pub instrument: InstrumentKey,
    pub role: SpotFuturesOrderRole,
    pub side: SpotFuturesOrderSide,
    pub base_quantity: f64,
    pub limit_price: f64,
    pub post_only: bool,
    pub reduce_only: bool,
    pub time_in_force: String,
}

impl SpotFuturesOrderDraft {
    pub fn notional_usdt(&self) -> f64 {
        self.base_quantity.abs() * self.limit_price
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesHedgePlan {
    pub trigger: String,
    pub unhedged_spot_base_qty: f64,
    pub hedge_order: SpotFuturesOrderDraft,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesOpportunity {
    pub opportunity_id: String,
    pub route: SpotFuturesRoute,
    pub spot_maker_price: f64,
    pub perp_hedge_price: f64,
    pub basis_edge_bps: f64,
    pub expected_funding_bps: f64,
    pub open_fee_bps: f64,
    pub close_fee_bps: f64,
    pub slippage_buffer_bps: f64,
    pub expected_net_edge_bps: f64,
    pub target_base_qty: f64,
    pub target_notional_usdt: f64,
    pub spot_depth_usdt: f64,
    pub perp_depth_usdt: f64,
    pub spot_maker_order: SpotFuturesOrderDraft,
    pub hedge_after_fill: SpotFuturesHedgePlan,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesOpportunityAudit {
    pub opportunity_id: String,
    pub route: SpotFuturesRoute,
    pub accepted: bool,
    pub reject_reason: Option<SpotFuturesRejectReason>,
    pub basis_edge_bps: Option<f64>,
    pub expected_funding_bps: Option<f64>,
    pub expected_net_edge_bps: Option<f64>,
    pub spot_depth_usdt: f64,
    pub perp_depth_usdt: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesBundle {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub route: SpotFuturesRoute,
    pub status: SpotFuturesBundleStatus,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub target_base_qty: f64,
    pub spot_filled_base_qty: f64,
    pub perp_hedged_base_qty: f64,
    pub spot_open_client_order_id: Option<String>,
    pub perp_hedge_client_order_id: Option<String>,
    pub close_only: bool,
    pub last_error: Option<String>,
}

impl SpotFuturesBundle {
    pub fn from_opportunity(
        bundle_id: impl Into<String>,
        opportunity: &SpotFuturesOpportunity,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            bundle_id: bundle_id.into(),
            opportunity_id: opportunity.opportunity_id.clone(),
            route: opportunity.route.clone(),
            status: SpotFuturesBundleStatus::SignalAccepted,
            opened_at: now,
            updated_at: now,
            target_base_qty: opportunity.target_base_qty,
            spot_filled_base_qty: 0.0,
            perp_hedged_base_qty: 0.0,
            spot_open_client_order_id: None,
            perp_hedge_client_order_id: None,
            close_only: false,
            last_error: None,
        }
    }

    pub fn unhedged_spot_base_qty(&self) -> f64 {
        (self.spot_filled_base_qty - self.perp_hedged_base_qty).max(0.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpotFuturesBundleEvent {
    SpotMakerSubmitted {
        client_order_id: String,
        occurred_at: DateTime<Utc>,
    },
    SpotMakerRejected {
        reason: String,
        occurred_at: DateTime<Utc>,
    },
    SpotMakerFill {
        filled_base_qty: f64,
        cumulative_base_qty: f64,
        final_fill: bool,
        occurred_at: DateTime<Utc>,
    },
    SpotMakerCancelSubmitted {
        occurred_at: DateTime<Utc>,
    },
    SpotMakerCanceledNoFill {
        occurred_at: DateTime<Utc>,
    },
    PerpHedgeSubmitted {
        client_order_id: String,
        occurred_at: DateTime<Utc>,
    },
    PerpHedgeRejected {
        reason: String,
        occurred_at: DateTime<Utc>,
    },
    PerpHedgeFill {
        filled_base_qty: f64,
        cumulative_base_qty: f64,
        final_fill: bool,
        occurred_at: DateTime<Utc>,
    },
    CloseRequested {
        occurred_at: DateTime<Utc>,
    },
    Closed {
        occurred_at: DateTime<Utc>,
    },
    UnknownOrderState {
        reason: String,
        occurred_at: DateTime<Utc>,
    },
    RiskStopped {
        reason: String,
        occurred_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpotFuturesBundleAction {
    SubmitPerpHedge {
        order: SpotFuturesOrderDraft,
        unhedged_spot_base_qty: f64,
    },
    CancelSpotMaker {
        client_order_id: Option<String>,
    },
    EnterCloseOnly {
        reason: String,
    },
    SubmitCloseOrders {
        spot_close: SpotFuturesOrderDraft,
        perp_close: SpotFuturesOrderDraft,
    },
    StartReadback {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesClosePricing {
    pub spot_sell_limit_price: f64,
    pub perp_buy_limit_price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotFuturesClosePlan {
    pub spot_close_order: SpotFuturesOrderDraft,
    pub perp_close_order: SpotFuturesOrderDraft,
}

pub fn apply_spot_futures_bundle_event(
    bundle: &mut SpotFuturesBundle,
    event: SpotFuturesBundleEvent,
    hedge_template: &SpotFuturesOrderDraft,
    min_hedge_base_qty: f64,
    close_plan: Option<&SpotFuturesClosePlan>,
) -> Vec<SpotFuturesBundleAction> {
    let mut actions = Vec::new();
    match event {
        SpotFuturesBundleEvent::SpotMakerSubmitted {
            client_order_id,
            occurred_at,
        } => {
            bundle.spot_open_client_order_id = Some(client_order_id);
            bundle.status = SpotFuturesBundleStatus::SpotMakerPlaced;
            bundle.updated_at = occurred_at;
        }
        SpotFuturesBundleEvent::SpotMakerRejected {
            reason,
            occurred_at,
        } => {
            bundle.last_error = Some(reason);
            bundle.status = SpotFuturesBundleStatus::Idle;
            bundle.updated_at = occurred_at;
        }
        SpotFuturesBundleEvent::SpotMakerFill {
            filled_base_qty: _,
            cumulative_base_qty,
            final_fill,
            occurred_at,
        } => {
            bundle.spot_filled_base_qty = cumulative_base_qty.max(bundle.spot_filled_base_qty);
            bundle.status = if final_fill {
                SpotFuturesBundleStatus::SpotMakerFilled
            } else {
                SpotFuturesBundleStatus::SpotMakerPartiallyFilled
            };
            bundle.updated_at = occurred_at;
            let unhedged = bundle.unhedged_spot_base_qty();
            if unhedged >= min_hedge_base_qty.max(0.0) && unhedged > 0.0 {
                let mut hedge_order = hedge_template.clone();
                hedge_order.base_quantity = unhedged;
                actions.push(SpotFuturesBundleAction::SubmitPerpHedge {
                    order: hedge_order,
                    unhedged_spot_base_qty: unhedged,
                });
                bundle.status = SpotFuturesBundleStatus::PerpHedgeSubmitting;
            }
        }
        SpotFuturesBundleEvent::SpotMakerCancelSubmitted { occurred_at } => {
            bundle.updated_at = occurred_at;
            actions.push(SpotFuturesBundleAction::CancelSpotMaker {
                client_order_id: bundle.spot_open_client_order_id.clone(),
            });
        }
        SpotFuturesBundleEvent::SpotMakerCanceledNoFill { occurred_at } => {
            if bundle.spot_filled_base_qty <= 0.0 {
                bundle.status = SpotFuturesBundleStatus::SpotMakerCanceledNoFill;
            }
            bundle.updated_at = occurred_at;
        }
        SpotFuturesBundleEvent::PerpHedgeSubmitted {
            client_order_id,
            occurred_at,
        } => {
            bundle.perp_hedge_client_order_id = Some(client_order_id);
            bundle.status = SpotFuturesBundleStatus::PerpHedgeSubmitting;
            bundle.updated_at = occurred_at;
        }
        SpotFuturesBundleEvent::PerpHedgeRejected {
            reason,
            occurred_at,
        } => {
            bundle.last_error = Some(reason.clone());
            bundle.status = SpotFuturesBundleStatus::HedgeFailedOneSidedSpotLong;
            bundle.close_only = true;
            bundle.updated_at = occurred_at;
            actions.push(SpotFuturesBundleAction::EnterCloseOnly { reason });
        }
        SpotFuturesBundleEvent::PerpHedgeFill {
            filled_base_qty: _,
            cumulative_base_qty,
            final_fill,
            occurred_at,
        } => {
            bundle.perp_hedged_base_qty = cumulative_base_qty.max(bundle.perp_hedged_base_qty);
            bundle.updated_at = occurred_at;
            if final_fill && bundle.unhedged_spot_base_qty() <= 0.0 {
                bundle.status = SpotFuturesBundleStatus::HedgedOpen;
            } else if bundle.unhedged_spot_base_qty() > min_hedge_base_qty.max(0.0) {
                let mut hedge_order = hedge_template.clone();
                hedge_order.base_quantity = bundle.unhedged_spot_base_qty();
                actions.push(SpotFuturesBundleAction::SubmitPerpHedge {
                    order: hedge_order,
                    unhedged_spot_base_qty: bundle.unhedged_spot_base_qty(),
                });
            }
        }
        SpotFuturesBundleEvent::CloseRequested { occurred_at } => {
            bundle.status = SpotFuturesBundleStatus::Closing;
            bundle.close_only = true;
            bundle.updated_at = occurred_at;
            if let Some(close_plan) = close_plan {
                actions.push(SpotFuturesBundleAction::SubmitCloseOrders {
                    spot_close: close_plan.spot_close_order.clone(),
                    perp_close: close_plan.perp_close_order.clone(),
                });
            }
        }
        SpotFuturesBundleEvent::Closed { occurred_at } => {
            bundle.status = SpotFuturesBundleStatus::Closed;
            bundle.updated_at = occurred_at;
        }
        SpotFuturesBundleEvent::UnknownOrderState {
            reason,
            occurred_at,
        } => {
            bundle.last_error = Some(reason.clone());
            bundle.status = SpotFuturesBundleStatus::UnknownOrderState;
            bundle.close_only = true;
            bundle.updated_at = occurred_at;
            actions.push(SpotFuturesBundleAction::StartReadback { reason });
        }
        SpotFuturesBundleEvent::RiskStopped {
            reason,
            occurred_at,
        } => {
            bundle.last_error = Some(reason);
            bundle.status = SpotFuturesBundleStatus::RiskStopped;
            bundle.close_only = true;
            bundle.updated_at = occurred_at;
        }
    }
    actions
}

pub fn build_spot_futures_close_plan(
    route: &SpotFuturesRoute,
    spot_precision: SymbolPrecision,
    perp_precision: SymbolPrecision,
    pricing: SpotFuturesClosePricing,
    base_quantity: f64,
) -> Option<SpotFuturesClosePlan> {
    let spot_qty = spot_precision.floor_base_quantity(base_quantity);
    let perp_qty = perp_precision.floor_base_quantity(base_quantity);
    let close_qty = spot_qty.min(perp_qty);
    if close_qty <= 0.0 {
        return None;
    }
    Some(SpotFuturesClosePlan {
        spot_close_order: SpotFuturesOrderDraft {
            instrument: route.spot_key(),
            role: SpotFuturesOrderRole::SpotTakerCloseSell,
            side: SpotFuturesOrderSide::Sell,
            base_quantity: close_qty,
            limit_price: spot_precision.sell_limit_price(pricing.spot_sell_limit_price),
            post_only: false,
            reduce_only: false,
            time_in_force: "ioc".to_string(),
        },
        perp_close_order: SpotFuturesOrderDraft {
            instrument: route.perp_key(),
            role: SpotFuturesOrderRole::PerpReduceOnlyCloseBuy,
            side: SpotFuturesOrderSide::Buy,
            base_quantity: close_qty,
            limit_price: perp_precision.buy_limit_price(pricing.perp_buy_limit_price),
            post_only: false,
            reduce_only: true,
            time_in_force: "ioc".to_string(),
        },
    })
}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_spot_futures_opportunity(
    route: &SpotFuturesRoute,
    spot_book: &OrderBookTop,
    perp_book: &OrderBookTop,
    funding: Option<&FundingSnapshot>,
    spot_precision: SymbolPrecision,
    perp_precision: SymbolPrecision,
    spot_fee: FeeRates,
    perp_fee: FeeRates,
    selection: &SpotFuturesSelectionConfig,
    funding_config: &SpotFuturesFundingConfig,
    execution: &SpotFuturesExecutionConfig,
    sizing: &SpotFuturesSizingConfig,
    excluded_bases: &[String],
    now: DateTime<Utc>,
    stale_quote_ms: u64,
    min_levels: usize,
) -> (Option<SpotFuturesOpportunity>, SpotFuturesOpportunityAudit) {
    let opportunity_id = format!(
        "{}:{}:{}:{}",
        route.canonical_symbol.as_pair(),
        route.spot_exchange,
        route.perp_exchange,
        now.timestamp_millis()
    );
    let mut audit = SpotFuturesOpportunityAudit {
        opportunity_id: opportunity_id.clone(),
        route: route.clone(),
        accepted: false,
        reject_reason: None,
        basis_edge_bps: None,
        expected_funding_bps: None,
        expected_net_edge_bps: None,
        spot_depth_usdt: spot_book.best_bid_quantity * spot_book.best_bid_price,
        perp_depth_usdt: perp_book.best_bid_quantity * perp_book.best_bid_price,
        observed_at: now,
    };

    let excluded: BTreeSet<_> = excluded_bases
        .iter()
        .map(|base| base.trim().to_ascii_uppercase())
        .collect();
    if excluded.contains(&route.canonical_symbol.base) {
        audit.reject_reason = Some(SpotFuturesRejectReason::ExcludedBase);
        return (None, audit);
    }
    if !spot_book.is_valid(min_levels) {
        audit.reject_reason = Some(SpotFuturesRejectReason::InvalidSpotBook);
        return (None, audit);
    }
    if !perp_book.is_valid(min_levels) {
        audit.reject_reason = Some(SpotFuturesRejectReason::InvalidPerpBook);
        return (None, audit);
    }
    if !spot_book.is_fresh(now, stale_quote_ms) {
        audit.reject_reason = Some(SpotFuturesRejectReason::StaleSpotBook);
        return (None, audit);
    }
    if !perp_book.is_fresh(now, stale_quote_ms) {
        audit.reject_reason = Some(SpotFuturesRejectReason::StalePerpBook);
        return (None, audit);
    }

    let expected_funding_bps = if funding_config.enabled {
        let Some(snapshot) = funding else {
            audit.reject_reason = Some(SpotFuturesRejectReason::FundingMissing);
            return (None, audit);
        };
        if snapshot.age_ms(now) > funding_config.max_funding_snapshot_age_ms {
            audit.reject_reason = Some(SpotFuturesRejectReason::FundingStale);
            return (None, audit);
        }
        if funding_config.require_next_funding_time && snapshot.next_funding_time.is_none() {
            audit.reject_reason = Some(SpotFuturesRejectReason::FundingTimeMissing);
            return (None, audit);
        }
        let effective_rate =
            snapshot.effective_rate_for_short(funding_config.use_predicted_funding_when_available);
        if effective_rate < 0.0 {
            let near_adverse_settlement = snapshot
                .next_funding_time
                .map(|time| {
                    time.signed_duration_since(now).num_minutes()
                        <= funding_config.no_open_before_adverse_funding_mins
                })
                .unwrap_or(false);
            if near_adverse_settlement {
                audit.reject_reason = Some(SpotFuturesRejectReason::NearAdverseFundingSettlement);
                return (None, audit);
            }
        }
        let expected_settlements =
            funding_config.expected_holding_hours / snapshot.funding_interval_hours.max(1.0);
        let expected = effective_rate * expected_settlements * 10_000.0;
        if expected < funding_config.min_expected_funding_bps {
            audit.reject_reason = Some(SpotFuturesRejectReason::ExpectedFundingTooAdverse);
            audit.expected_funding_bps = Some(expected);
            return (None, audit);
        }
        expected
    } else {
        0.0
    };
    audit.expected_funding_bps = Some(expected_funding_bps);

    let spot_maker_price = spot_precision.sell_limit_price(
        spot_book.best_ask_price
            - execution.spot_maker_price_offset_ticks as f64 * spot_precision.price_tick,
    );
    let perp_hedge_price = perp_precision
        .sell_limit_price(perp_book.best_bid_price * (1.0 - execution.perp_hedge_slippage_pct));
    if spot_maker_price <= 0.0 || perp_hedge_price <= spot_maker_price {
        audit.reject_reason = Some(SpotFuturesRejectReason::BasisTooSmall);
        return (None, audit);
    }
    let basis_edge_bps = (perp_hedge_price - spot_maker_price) / spot_maker_price * 10_000.0;
    audit.basis_edge_bps = Some(basis_edge_bps);
    if basis_edge_bps < selection.min_open_basis_bps {
        audit.reject_reason = Some(SpotFuturesRejectReason::BasisTooSmall);
        return (None, audit);
    }
    if basis_edge_bps > selection.max_open_basis_bps {
        audit.reject_reason = Some(SpotFuturesRejectReason::BasisTooLarge);
        return (None, audit);
    }

    let spot_depth_usdt = spot_book.best_ask_quantity * spot_maker_price;
    let perp_depth_usdt = perp_book.best_bid_quantity * perp_hedge_price;
    audit.spot_depth_usdt = spot_depth_usdt;
    audit.perp_depth_usdt = perp_depth_usdt;
    if spot_depth_usdt < selection.min_spot_top_depth_usdt {
        audit.reject_reason = Some(SpotFuturesRejectReason::InsufficientSpotDepth);
        return (None, audit);
    }
    if perp_depth_usdt < selection.min_perp_top_depth_usdt {
        audit.reject_reason = Some(SpotFuturesRejectReason::InsufficientPerpDepth);
        return (None, audit);
    }

    let target_notional_usdt = sizing
        .target_notional_usdt
        .min(sizing.max_notional_usdt)
        .max(sizing.min_notional_usdt);
    let depth_capacity_usdt =
        spot_depth_usdt.min(perp_depth_usdt) * selection.top_of_book_capacity_ratio.clamp(0.0, 1.0);
    let planned_notional_usdt = target_notional_usdt.min(depth_capacity_usdt);
    let mut target_base_qty = planned_notional_usdt / spot_maker_price;
    target_base_qty = spot_precision.floor_base_quantity(target_base_qty);
    target_base_qty = perp_precision.floor_base_quantity(target_base_qty);
    if target_base_qty <= 0.0
        || target_base_qty < spot_precision.min_base_quantity
        || target_base_qty < perp_precision.min_base_quantity
    {
        audit.reject_reason = Some(SpotFuturesRejectReason::QuantityTooSmall);
        return (None, audit);
    }
    let planned_notional_usdt = target_base_qty * spot_maker_price;
    if planned_notional_usdt < sizing.min_notional_usdt
        || planned_notional_usdt < spot_precision.min_notional_usdt
        || planned_notional_usdt < perp_precision.min_notional_usdt
    {
        audit.reject_reason = Some(SpotFuturesRejectReason::NotionalTooSmall);
        return (None, audit);
    }

    let open_fee_bps = spot_fee.maker * 10_000.0 + perp_fee.taker * 10_000.0;
    let close_fee_bps = spot_fee.taker * 10_000.0 + perp_fee.taker * 10_000.0;
    let slippage_buffer_bps = execution.perp_hedge_slippage_pct * 10_000.0
        + execution.close_slippage_buffer_bps
        + execution.latency_buffer_bps
        + execution.safety_buffer_bps;
    let expected_net_edge_bps =
        basis_edge_bps + expected_funding_bps - open_fee_bps - close_fee_bps - slippage_buffer_bps;
    audit.expected_net_edge_bps = Some(expected_net_edge_bps);
    if expected_net_edge_bps < selection.min_open_net_edge_bps {
        audit.reject_reason = Some(SpotFuturesRejectReason::BelowMinNetEdge);
        return (None, audit);
    }

    let spot_maker_order = SpotFuturesOrderDraft {
        instrument: route.spot_key(),
        role: SpotFuturesOrderRole::SpotMakerBuy,
        side: SpotFuturesOrderSide::Buy,
        base_quantity: target_base_qty,
        limit_price: spot_maker_price,
        post_only: execution.spot_post_only_required,
        reduce_only: false,
        time_in_force: "gtc".to_string(),
    };
    let hedge_order = SpotFuturesOrderDraft {
        instrument: route.perp_key(),
        role: SpotFuturesOrderRole::PerpTakerShortHedge,
        side: SpotFuturesOrderSide::Sell,
        base_quantity: target_base_qty,
        limit_price: perp_hedge_price,
        post_only: false,
        reduce_only: false,
        time_in_force: "ioc".to_string(),
    };
    audit.accepted = true;
    (
        Some(SpotFuturesOpportunity {
            opportunity_id,
            route: route.clone(),
            spot_maker_price,
            perp_hedge_price,
            basis_edge_bps,
            expected_funding_bps,
            open_fee_bps,
            close_fee_bps,
            slippage_buffer_bps,
            expected_net_edge_bps,
            target_base_qty,
            target_notional_usdt: planned_notional_usdt,
            spot_depth_usdt,
            perp_depth_usdt,
            spot_maker_order,
            hedge_after_fill: SpotFuturesHedgePlan {
                trigger: "spot_maker_fill_private_stream_or_rest_readback".to_string(),
                unhedged_spot_base_qty: target_base_qty,
                hedge_order,
            },
        }),
        audit,
    )
}

pub fn default_excluded_bases() -> Vec<String> {
    vec![
        "BTC".to_string(),
        "ETH".to_string(),
        "BNB".to_string(),
        "SOL".to_string(),
    ]
}

pub fn normalize_symbols(symbols: &[String], excluded_bases: &[String]) -> Vec<String> {
    let excluded: BTreeSet<_> = excluded_bases
        .iter()
        .map(|base| base.trim().to_ascii_uppercase())
        .collect();
    symbols
        .iter()
        .filter_map(|symbol| CanonicalSymbol::parse(symbol))
        .filter(|symbol| !excluded.contains(&symbol.base))
        .map(|symbol| symbol.as_pair())
        .fold(Vec::new(), |mut acc, symbol| {
            if !acc.contains(&symbol) {
                acc.push(symbol);
            }
            acc
        })
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        value
    } else {
        (value / step).floor() * step
    }
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        value
    } else {
        (value / step).ceil() * step
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn route(symbol: &str) -> SpotFuturesRoute {
        SpotFuturesRoute {
            route_id: "gate-bitget-ordi".to_string(),
            canonical_symbol: CanonicalSymbol::parse(symbol).unwrap(),
            spot_exchange: "gate".to_string(),
            perp_exchange: "bitget".to_string(),
        }
    }

    fn book(key: InstrumentKey, bid: f64, ask: f64, now: DateTime<Utc>) -> OrderBookTop {
        OrderBookTop {
            instrument: key,
            best_bid_price: bid,
            best_bid_quantity: 100.0,
            best_ask_price: ask,
            best_ask_quantity: 100.0,
            levels: 5,
            received_at: now,
            sequence_gap: false,
        }
    }

    #[test]
    fn default_universe_should_exclude_major_bases() {
        let symbols = vec![
            "BTC/USDT".to_string(),
            "ETH/USDT".to_string(),
            "ORDI/USDT".to_string(),
            "SOLUSDT".to_string(),
        ];
        assert_eq!(
            normalize_symbols(&symbols, &default_excluded_bases()),
            vec!["ORDI/USDT"]
        );
    }

    #[test]
    fn positive_funding_should_add_edge_for_spot_long_perp_short() {
        let now = Utc::now();
        let route = route("ORDI/USDT");
        let (opportunity, audit) = evaluate_spot_futures_opportunity(
            &route,
            &book(route.spot_key(), 9.99, 10.0, now),
            &book(route.perp_key(), 10.12, 10.13, now),
            Some(&FundingSnapshot {
                instrument: route.perp_key(),
                funding_rate: 0.0003,
                predicted_funding_rate: None,
                funding_interval_hours: 8.0,
                next_funding_time: Some(now + Duration::hours(1)),
                observed_at: now,
            }),
            SymbolPrecision {
                price_tick: 0.001,
                quantity_step: 0.001,
                min_base_quantity: 0.001,
                min_notional_usdt: 5.0,
                contract_size: 1.0,
            },
            SymbolPrecision {
                price_tick: 0.001,
                quantity_step: 0.001,
                min_base_quantity: 0.001,
                min_notional_usdt: 5.0,
                contract_size: 1.0,
            },
            FeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            FeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            &SpotFuturesSelectionConfig {
                min_open_net_edge_bps: 1.0,
                ..SpotFuturesSelectionConfig::default()
            },
            &SpotFuturesFundingConfig::default(),
            &SpotFuturesExecutionConfig {
                perp_hedge_slippage_pct: 0.0005,
                ..SpotFuturesExecutionConfig::default()
            },
            &SpotFuturesSizingConfig::default(),
            &[],
            now,
            1000,
            1,
        );
        assert!(opportunity.is_some(), "{audit:?}");
        assert!(audit.expected_funding_bps.unwrap() > 0.0);
    }

    #[test]
    fn negative_funding_near_settlement_should_reject() {
        let now = Utc::now();
        let route = route("ORDI/USDT");
        let (_, audit) = evaluate_spot_futures_opportunity(
            &route,
            &book(route.spot_key(), 9.99, 10.0, now),
            &book(route.perp_key(), 10.12, 10.13, now),
            Some(&FundingSnapshot {
                instrument: route.perp_key(),
                funding_rate: -0.0003,
                predicted_funding_rate: None,
                funding_interval_hours: 8.0,
                next_funding_time: Some(now + Duration::minutes(5)),
                observed_at: now,
            }),
            SymbolPrecision::default(),
            SymbolPrecision::default(),
            FeeRates::default(),
            FeeRates::default(),
            &SpotFuturesSelectionConfig::default(),
            &SpotFuturesFundingConfig::default(),
            &SpotFuturesExecutionConfig::default(),
            &SpotFuturesSizingConfig::default(),
            &[],
            now,
            1000,
            1,
        );
        assert_eq!(
            audit.reject_reason,
            Some(SpotFuturesRejectReason::NearAdverseFundingSettlement)
        );
    }

    #[test]
    fn instrument_key_should_keep_spot_and_perp_precision_separate() {
        let symbol = CanonicalSymbol::parse("ORDI/USDT").unwrap();
        let mut registry = PrecisionRegistry::default();
        registry.insert(
            InstrumentKey::new("gate", SpotFuturesMarketType::Spot, symbol.clone()),
            SymbolPrecision {
                quantity_step: 0.01,
                ..SymbolPrecision::default()
            },
        );
        registry.insert(
            InstrumentKey::new("gate", SpotFuturesMarketType::Perpetual, symbol.clone()),
            SymbolPrecision {
                quantity_step: 1.0,
                ..SymbolPrecision::default()
            },
        );
        assert_eq!(
            registry
                .get(&InstrumentKey::new(
                    "gate",
                    SpotFuturesMarketType::Spot,
                    symbol.clone()
                ))
                .quantity_step,
            0.01
        );
        assert_eq!(
            registry
                .get(&InstrumentKey::new(
                    "gate",
                    SpotFuturesMarketType::Perpetual,
                    symbol
                ))
                .quantity_step,
            1.0
        );
    }

    #[test]
    fn partial_spot_fill_should_emit_perp_hedge_for_unhedged_quantity() {
        let now = Utc::now();
        let route = route("ORDI/USDT");
        let spot_order = SpotFuturesOrderDraft {
            instrument: route.spot_key(),
            role: SpotFuturesOrderRole::SpotMakerBuy,
            side: SpotFuturesOrderSide::Buy,
            base_quantity: 10.0,
            limit_price: 10.0,
            post_only: true,
            reduce_only: false,
            time_in_force: "gtc".to_string(),
        };
        let hedge_order = SpotFuturesOrderDraft {
            instrument: route.perp_key(),
            role: SpotFuturesOrderRole::PerpTakerShortHedge,
            side: SpotFuturesOrderSide::Sell,
            base_quantity: 10.0,
            limit_price: 10.1,
            post_only: false,
            reduce_only: false,
            time_in_force: "ioc".to_string(),
        };
        let opportunity = SpotFuturesOpportunity {
            opportunity_id: "opp-1".to_string(),
            route: route.clone(),
            spot_maker_price: 10.0,
            perp_hedge_price: 10.1,
            basis_edge_bps: 100.0,
            expected_funding_bps: 2.0,
            open_fee_bps: 4.0,
            close_fee_bps: 6.0,
            slippage_buffer_bps: 3.0,
            expected_net_edge_bps: 89.0,
            target_base_qty: 10.0,
            target_notional_usdt: 100.0,
            spot_depth_usdt: 1000.0,
            perp_depth_usdt: 1000.0,
            spot_maker_order: spot_order,
            hedge_after_fill: SpotFuturesHedgePlan {
                trigger: "private_fill".to_string(),
                unhedged_spot_base_qty: 10.0,
                hedge_order: hedge_order.clone(),
            },
        };
        let mut bundle = SpotFuturesBundle::from_opportunity("bundle-1", &opportunity, now);
        let actions = apply_spot_futures_bundle_event(
            &mut bundle,
            SpotFuturesBundleEvent::SpotMakerFill {
                filled_base_qty: 3.0,
                cumulative_base_qty: 3.0,
                final_fill: false,
                occurred_at: now,
            },
            &hedge_order,
            1.0,
            None,
        );

        assert_eq!(bundle.status, SpotFuturesBundleStatus::PerpHedgeSubmitting);
        assert_eq!(bundle.unhedged_spot_base_qty(), 3.0);
        assert_eq!(
            actions,
            vec![SpotFuturesBundleAction::SubmitPerpHedge {
                order: SpotFuturesOrderDraft {
                    base_quantity: 3.0,
                    ..hedge_order
                },
                unhedged_spot_base_qty: 3.0,
            }]
        );
    }

    #[test]
    fn hedge_reject_should_enter_close_only_one_sided_spot_long() {
        let now = Utc::now();
        let route = route("ORDI/USDT");
        let opportunity = SpotFuturesOpportunity {
            opportunity_id: "opp-1".to_string(),
            route: route.clone(),
            spot_maker_price: 10.0,
            perp_hedge_price: 10.1,
            basis_edge_bps: 100.0,
            expected_funding_bps: 2.0,
            open_fee_bps: 4.0,
            close_fee_bps: 6.0,
            slippage_buffer_bps: 3.0,
            expected_net_edge_bps: 89.0,
            target_base_qty: 10.0,
            target_notional_usdt: 100.0,
            spot_depth_usdt: 1000.0,
            perp_depth_usdt: 1000.0,
            spot_maker_order: SpotFuturesOrderDraft {
                instrument: route.spot_key(),
                role: SpotFuturesOrderRole::SpotMakerBuy,
                side: SpotFuturesOrderSide::Buy,
                base_quantity: 10.0,
                limit_price: 10.0,
                post_only: true,
                reduce_only: false,
                time_in_force: "gtc".to_string(),
            },
            hedge_after_fill: SpotFuturesHedgePlan {
                trigger: "private_fill".to_string(),
                unhedged_spot_base_qty: 10.0,
                hedge_order: SpotFuturesOrderDraft {
                    instrument: route.perp_key(),
                    role: SpotFuturesOrderRole::PerpTakerShortHedge,
                    side: SpotFuturesOrderSide::Sell,
                    base_quantity: 10.0,
                    limit_price: 10.1,
                    post_only: false,
                    reduce_only: false,
                    time_in_force: "ioc".to_string(),
                },
            },
        };
        let mut bundle = SpotFuturesBundle::from_opportunity("bundle-1", &opportunity, now);
        bundle.spot_filled_base_qty = 10.0;
        let actions = apply_spot_futures_bundle_event(
            &mut bundle,
            SpotFuturesBundleEvent::PerpHedgeRejected {
                reason: "insufficient margin".to_string(),
                occurred_at: now,
            },
            &opportunity.hedge_after_fill.hedge_order,
            1.0,
            None,
        );

        assert_eq!(
            bundle.status,
            SpotFuturesBundleStatus::HedgeFailedOneSidedSpotLong
        );
        assert!(bundle.close_only);
        assert_eq!(
            actions,
            vec![SpotFuturesBundleAction::EnterCloseOnly {
                reason: "insufficient margin".to_string()
            }]
        );
    }

    #[test]
    fn close_plan_should_use_spot_taker_and_perp_reduce_only_buy() {
        let route = route("ORDI/USDT");
        let plan = build_spot_futures_close_plan(
            &route,
            SymbolPrecision {
                price_tick: 0.01,
                quantity_step: 0.01,
                min_base_quantity: 0.01,
                min_notional_usdt: 5.0,
                contract_size: 1.0,
            },
            SymbolPrecision {
                price_tick: 0.001,
                quantity_step: 0.001,
                min_base_quantity: 0.001,
                min_notional_usdt: 5.0,
                contract_size: 1.0,
            },
            SpotFuturesClosePricing {
                spot_sell_limit_price: 9.991,
                perp_buy_limit_price: 10.1121,
            },
            1.23456,
        )
        .expect("close plan");

        assert_eq!(
            plan.spot_close_order.role,
            SpotFuturesOrderRole::SpotTakerCloseSell
        );
        assert_eq!(plan.spot_close_order.side, SpotFuturesOrderSide::Sell);
        assert!(!plan.spot_close_order.reduce_only);
        assert_eq!(plan.spot_close_order.time_in_force, "ioc");
        assert_eq!(
            plan.perp_close_order.role,
            SpotFuturesOrderRole::PerpReduceOnlyCloseBuy
        );
        assert_eq!(plan.perp_close_order.side, SpotFuturesOrderSide::Buy);
        assert!(plan.perp_close_order.reduce_only);
        assert_eq!(plan.perp_close_order.time_in_force, "ioc");
        assert_eq!(plan.perp_close_order.base_quantity, 1.23);
    }
}
