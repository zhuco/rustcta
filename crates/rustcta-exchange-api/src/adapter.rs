use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    AmendOrderRequest, AmendOrderResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiResult, ExchangeClient, ExchangeId, MarketType, OrderBookRequest, OrderBookResponse,
    OrderStatus, OrderType, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, RecentFillsRequest, RecentFillsResponse, SetLeverageRequest,
    SetLeverageResponse, SymbolRules, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
};

/// Strategy-facing normalized adapter contract.
///
/// Concrete venue adapters may support richer exchange-specific APIs, but those
/// details must be hidden behind this contract before strategy code consumes
/// orders, fills, positions, symbols, or stream recovery behavior.
#[async_trait]
pub trait ExchangeAdapter: ExchangeClient {
    fn adapter_profile(&self) -> ExchangeAdapterProfile {
        ExchangeAdapterProfile::from_capabilities(self.capabilities(), None)
    }

    fn normalize_order_status(&self, status: OrderStatus) -> NormalizedOrderState {
        NormalizedOrderState::from(status)
    }

    fn normalize_order_request(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderRequest> {
        Ok(request)
    }

    async fn place_normalized_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order(self.normalize_order_request(request)?)
            .await
    }

    async fn cancel_normalized_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order(request).await
    }

    async fn cancel_replace_order(
        &self,
        cancel: CancelOrderRequest,
        replacement: PlaceOrderRequest,
    ) -> ExchangeApiResult<CancelReplaceResponse> {
        let cancel_response = self.cancel_normalized_order(cancel).await?;
        let place_response = self.place_normalized_order(replacement).await?;
        Ok(CancelReplaceResponse {
            mode: CancelReplaceMode::CancelThenPlace,
            cancel: cancel_response,
            replacement: place_response,
        })
    }

    async fn resync_after_stream_gap(
        &self,
        request: StreamRecoveryRequest,
    ) -> ExchangeApiResult<StreamRecoveryPlan> {
        Ok(StreamRecoveryPlan::rest_snapshot_then_private_readback(
            request,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NormalizedOrderState {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl NormalizedOrderState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }
}

impl From<OrderStatus> for NormalizedOrderState {
    fn from(status: OrderStatus) -> Self {
        match status {
            OrderStatus::New | OrderStatus::Open | OrderStatus::PendingCancel => Self::New,
            OrderStatus::PartiallyFilled => Self::PartiallyFilled,
            OrderStatus::Filled => Self::Filled,
            OrderStatus::Cancelled | OrderStatus::Expired => Self::Canceled,
            OrderStatus::Rejected | OrderStatus::Unknown => Self::Rejected,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReplaceMode {
    NativeAtomic,
    NativeNonAtomic,
    AmendInPlace,
    CancelThenPlace,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelReplaceResponse {
    pub mode: CancelReplaceMode,
    pub cancel: CancelOrderResponse,
    pub replacement: PlaceOrderResponse,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeAdapterProfile {
    pub exchange: ExchangeId,
    pub spot_futures_unified: bool,
    pub spot_supported: bool,
    pub futures_supported: bool,
    pub perpetual_supported: bool,
    pub order_types: OrderTypeProfile,
    pub order_state_machine: OrderStateMachineProfile,
    pub partial_fill_support: AdapterSupport,
    pub cancel_replace: AdapterSupport,
    pub position_sync: AdapterSupport,
    pub funding_rate: AdapterSupport,
    pub leverage_control: AdapterSupport,
    pub position_mode_control: AdapterSupport,
    pub websocket_reliability: WebSocketReliabilityProfile,
    pub normalization: NormalizationProfile,
    pub rate_limits: RateLimitAdapterProfile,
    pub symbol_mapping: SymbolMappingProfile,
    pub hedge_execution: HedgeExecutionProfile,
}

impl ExchangeAdapterProfile {
    pub fn from_capabilities(
        capabilities: crate::ExchangeClientCapabilities,
        account_control: Option<crate::AccountControlCapabilities>,
    ) -> Self {
        let spot_supported = capabilities.market_types.contains(&MarketType::Spot);
        let futures_supported = capabilities.market_types.contains(&MarketType::Futures);
        let perpetual_supported = capabilities.market_types.contains(&MarketType::Perpetual);
        let has_derivatives = futures_supported || perpetual_supported;
        let leverage_control = account_control
            .as_ref()
            .is_some_and(|capability| capability.supports_leverage);
        let position_mode_control = account_control
            .as_ref()
            .is_some_and(|capability| capability.supports_position_mode_change);

        Self {
            exchange: capabilities.exchange.clone(),
            spot_futures_unified: spot_supported && has_derivatives,
            spot_supported,
            futures_supported,
            perpetual_supported,
            order_types: OrderTypeProfile {
                limit: capabilities
                    .supports_order_types
                    .contains(&OrderType::Limit),
                market: capabilities
                    .supports_order_types
                    .contains(&OrderType::Market),
                ioc: capabilities.supports_order_types.contains(&OrderType::IOC)
                    || capabilities
                        .supports_time_in_force
                        .contains(&TimeInForce::IOC),
                post_only: capabilities.supports_post_only
                    || capabilities
                        .supports_order_types
                        .contains(&OrderType::PostOnly)
                    || capabilities
                        .supports_time_in_force
                        .contains(&TimeInForce::GTX),
            },
            order_state_machine: OrderStateMachineProfile::default(),
            partial_fill_support: support_from_bool(
                capabilities.supports_recent_fills || capabilities.supports_query_order,
                "requires recent fills or query-order support",
            ),
            cancel_replace: cancel_replace_support(&capabilities),
            position_sync: support_from_bool(
                capabilities.supports_positions,
                "get_positions not declared",
            ),
            funding_rate: support_from_bool(
                capabilities.supports_funding_rates
                    || capabilities.capabilities_v2.funding_rates.is_supported(),
                "funding-rate support is not declared in ExchangeClientCapabilities",
            ),
            leverage_control: support_from_bool(leverage_control, "set_leverage not declared"),
            position_mode_control: support_from_bool(
                position_mode_control,
                "set_position_mode not declared",
            ),
            websocket_reliability: WebSocketReliabilityProfile::from_capabilities(&capabilities),
            normalization: NormalizationProfile::default(),
            rate_limits: RateLimitAdapterProfile::default(),
            symbol_mapping: SymbolMappingProfile::default(),
            hedge_execution: HedgeExecutionProfile::default(),
        }
    }

    pub fn missing_features(&self) -> Vec<AdapterMissingFeature> {
        let mut missing = Vec::new();
        if !self.spot_futures_unified {
            missing.push(AdapterMissingFeature::new(
                self.exchange.clone(),
                "spot_futures_unified",
                "spot and derivatives are not exposed through one normalized adapter profile",
            ));
        }
        for (feature, supported) in [
            ("limit_order", self.order_types.limit),
            ("market_order", self.order_types.market),
            ("ioc_order", self.order_types.ioc),
            ("post_only_order", self.order_types.post_only),
        ] {
            if !supported {
                missing.push(AdapterMissingFeature::new(
                    self.exchange.clone(),
                    feature,
                    "order type not declared",
                ));
            }
        }
        for (feature, support) in [
            ("partial_fill_support", &self.partial_fill_support),
            ("cancel_replace", &self.cancel_replace),
            ("position_sync", &self.position_sync),
            ("funding_rate", &self.funding_rate),
            ("leverage_control", &self.leverage_control),
            ("position_mode_control", &self.position_mode_control),
        ] {
            if !support.is_supported() {
                missing.push(AdapterMissingFeature::new(
                    self.exchange.clone(),
                    feature,
                    support.reason(),
                ));
            }
        }
        if !self.websocket_reliability.private_rest_fallback {
            missing.push(AdapterMissingFeature::new(
                self.exchange.clone(),
                "private_ws_rest_fallback",
                "private stream recovery cannot fall back to private REST readback",
            ));
        }
        missing
    }

    pub fn inconsistent_behaviors(&self) -> Vec<AdapterBehaviorInconsistency> {
        let mut items = Vec::new();
        if self.order_state_machine.rejected_maps_unknown_to_rejected {
            items.push(AdapterBehaviorInconsistency::new(
                self.exchange.clone(),
                "unknown_order_state",
                "UNKNOWN is normalized to REJECTED and must trigger reconciliation before strategy-level rejection accounting",
            ));
        }
        if matches!(self.cancel_replace, AdapterSupport::Composed { .. }) {
            items.push(AdapterBehaviorInconsistency::new(
                self.exchange.clone(),
                "cancel_replace_atomicity",
                "cancel-replace is composed and not venue-atomic",
            ));
        }
        if self.websocket_reliability.public_order_book_mode != OrderBookRecoveryMode::StrictDelta {
            items.push(AdapterBehaviorInconsistency::new(
                self.exchange.clone(),
                "order_book_recovery",
                "public order book is not strict sequence-delta; snapshot REST recovery must be treated as mandatory",
            ));
        }
        items
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderTypeProfile {
    pub limit: bool,
    pub market: bool,
    pub ioc: bool,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderStateMachineProfile {
    pub states: Vec<NormalizedOrderState>,
    pub terminal_states: Vec<NormalizedOrderState>,
    pub maps_open_to_new: bool,
    pub maps_pending_cancel_to_new: bool,
    pub maps_expired_to_canceled: bool,
    pub rejected_maps_unknown_to_rejected: bool,
}

impl Default for OrderStateMachineProfile {
    fn default() -> Self {
        Self {
            states: vec![
                NormalizedOrderState::New,
                NormalizedOrderState::PartiallyFilled,
                NormalizedOrderState::Filled,
                NormalizedOrderState::Canceled,
                NormalizedOrderState::Rejected,
            ],
            terminal_states: vec![
                NormalizedOrderState::Filled,
                NormalizedOrderState::Canceled,
                NormalizedOrderState::Rejected,
            ],
            maps_open_to_new: true,
            maps_pending_cancel_to_new: true,
            maps_expired_to_canceled: true,
            rejected_maps_unknown_to_rejected: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "support")]
pub enum AdapterSupport {
    Native,
    Composed { reason: String },
    RestFallback { reason: String },
    Missing { reason: String },
}

impl AdapterSupport {
    pub fn is_supported(&self) -> bool {
        !matches!(self, Self::Missing { .. })
    }

    pub fn reason(&self) -> &str {
        match self {
            Self::Native => "native",
            Self::Composed { reason }
            | Self::RestFallback { reason }
            | Self::Missing { reason } => reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBookRecoveryMode {
    SnapshotOnly,
    BestEffortDelta,
    StrictDelta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketReliabilityProfile {
    pub reconnect_strategy: ReconnectStrategy,
    pub public_order_book_mode: OrderBookRecoveryMode,
    pub snapshot_diff_recovery_required: bool,
    pub sequence_gap_detection: bool,
    pub public_rest_fallback: bool,
    pub private_rest_fallback: bool,
    pub resubscribe_after_reconnect: bool,
}

impl WebSocketReliabilityProfile {
    pub fn from_capabilities(capabilities: &crate::ExchangeClientCapabilities) -> Self {
        let runtime = &capabilities.capabilities_v2.stream_runtime;
        Self {
            reconnect_strategy: ReconnectStrategy::default(),
            public_order_book_mode: match capabilities.order_book.strictness {
                crate::OrderBookStrictness::SnapshotOnly => OrderBookRecoveryMode::SnapshotOnly,
                crate::OrderBookStrictness::BestEffortDelta => {
                    OrderBookRecoveryMode::BestEffortDelta
                }
                crate::OrderBookStrictness::StrictDelta => OrderBookRecoveryMode::StrictDelta,
            },
            snapshot_diff_recovery_required: true,
            sequence_gap_detection: capabilities.order_book.supports_sequence,
            public_rest_fallback: capabilities.supports_order_book_snapshot,
            private_rest_fallback: capabilities.supports_query_order
                || capabilities.supports_open_orders
                || capabilities.supports_recent_fills
                || capabilities.supports_positions,
            resubscribe_after_reconnect: runtime.reconnect_requires_resubscribe
                || runtime.reconnect.requires_resubscribe,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconnectStrategy {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: u32,
    pub jitter_ms: u64,
    pub max_attempts: Option<u32>,
    pub stale_message_timeout_ms: u64,
}

impl Default for ReconnectStrategy {
    fn default() -> Self {
        Self {
            initial_delay_ms: 250,
            max_delay_ms: 15_000,
            backoff_multiplier: 2,
            jitter_ms: 250,
            max_attempts: None,
            stale_message_timeout_ms: 3_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecoveryRequest {
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub exchange_symbol: String,
    pub detected_at: DateTime<Utc>,
    pub reason: crate::ResyncReason,
    pub expected_sequence: Option<u64>,
    pub received_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecoveryPlan {
    pub request: StreamRecoveryRequest,
    pub reconnect: bool,
    pub resubscribe: bool,
    pub fetch_order_book_snapshot: bool,
    pub fetch_open_orders: bool,
    pub fetch_recent_fills: bool,
    pub fetch_positions: bool,
    pub block_strategy_until_recovered: bool,
}

impl StreamRecoveryPlan {
    pub fn rest_snapshot_then_private_readback(request: StreamRecoveryRequest) -> Self {
        Self {
            request,
            reconnect: true,
            resubscribe: true,
            fetch_order_book_snapshot: true,
            fetch_open_orders: true,
            fetch_recent_fills: true,
            fetch_positions: true,
            block_strategy_until_recovered: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationProfile {
    pub price_precision: PrecisionNormalizationMode,
    pub quantity_precision: PrecisionNormalizationMode,
    pub min_notional: MinNotionalMode,
    pub reject_when_below_min_notional: bool,
}

impl Default for NormalizationProfile {
    fn default() -> Self {
        Self {
            price_precision: PrecisionNormalizationMode::FloorSellCeilBuy,
            quantity_precision: PrecisionNormalizationMode::Floor,
            min_notional: MinNotionalMode::RejectBeforeSubmit,
            reject_when_below_min_notional: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrecisionNormalizationMode {
    Floor,
    Ceil,
    RoundNearest,
    FloorSellCeilBuy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MinNotionalMode {
    RejectBeforeSubmit,
    IncreaseQuantityIfRiskAllows,
    VenueRejects,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedOrderSizing {
    pub rules: SymbolRules,
    pub requested_price: Option<String>,
    pub requested_quantity: String,
    pub normalized_price: Option<String>,
    pub normalized_quantity: String,
    pub notional: Option<String>,
    pub below_min_notional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitAdapterProfile {
    pub bucket_key: String,
    pub quote_read_weight: u32,
    pub order_write_weight: u32,
    pub cancel_weight: u32,
    pub throttle_before_submit: bool,
}

impl Default for RateLimitAdapterProfile {
    fn default() -> Self {
        Self {
            bucket_key: "exchange_account_market".to_string(),
            quote_read_weight: 1,
            order_write_weight: 1,
            cancel_weight: 1,
            throttle_before_submit: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolMappingProfile {
    pub canonical_to_exchange_required: bool,
    pub preserves_case: bool,
    pub market_type_scoped: bool,
    pub reload_on_symbol_rules_refresh: bool,
}

impl Default for SymbolMappingProfile {
    fn default() -> Self {
        Self {
            canonical_to_exchange_required: true,
            preserves_case: false,
            market_type_scoped: true,
            reload_on_symbol_rules_refresh: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HedgeExecutionProfile {
    pub requires_position_mode_hedge: bool,
    pub open_failure_retry: FailureRetryPolicy,
    pub close_failure_retry: FailureRetryPolicy,
    pub slippage: SlippageModel,
    pub prefer_private_ws_fills: bool,
    pub rest_fill_fallback_after_ms: u64,
}

impl Default for HedgeExecutionProfile {
    fn default() -> Self {
        Self {
            requires_position_mode_hedge: true,
            open_failure_retry: FailureRetryPolicy::default(),
            close_failure_retry: FailureRetryPolicy {
                max_attempts: 5,
                initial_delay_ms: 100,
                max_delay_ms: 1_500,
                backoff_multiplier: 2,
                retry_on_unknown_ack: true,
                query_before_retry: true,
            },
            slippage: SlippageModel::default(),
            prefer_private_ws_fills: true,
            rest_fill_fallback_after_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailureRetryPolicy {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: u32,
    pub retry_on_unknown_ack: bool,
    pub query_before_retry: bool,
}

impl Default for FailureRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 150,
            max_delay_ms: 2_000,
            backoff_multiplier: 2,
            retry_on_unknown_ack: true,
            query_before_retry: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SlippageModel {
    pub buy_limit_add_pct: f64,
    pub sell_limit_subtract_pct: f64,
    pub short_open_sell1_add_pct: f64,
    pub long_open_buy1_subtract_pct: f64,
}

impl Default for SlippageModel {
    fn default() -> Self {
        Self {
            buy_limit_add_pct: 0.0005,
            sell_limit_subtract_pct: 0.0005,
            short_open_sell1_add_pct: 0.0005,
            long_open_buy1_subtract_pct: 0.0005,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterMissingFeature {
    pub exchange: ExchangeId,
    pub feature: String,
    pub reason: String,
}

impl AdapterMissingFeature {
    pub fn new(
        exchange: ExchangeId,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            exchange,
            feature: feature.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterBehaviorInconsistency {
    pub exchange: ExchangeId,
    pub behavior: String,
    pub impact: String,
}

impl AdapterBehaviorInconsistency {
    pub fn new(
        exchange: ExchangeId,
        behavior: impl Into<String>,
        impact: impl Into<String>,
    ) -> Self {
        Self {
            exchange,
            behavior: behavior.into(),
            impact: impact.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeApiAuditReport {
    pub generated_at: DateTime<Utc>,
    pub exchanges: Vec<ExchangeAdapterProfile>,
    pub missing_features: Vec<AdapterMissingFeature>,
    pub inconsistent_behaviors: Vec<AdapterBehaviorInconsistency>,
    pub implementation_plan: Vec<AdapterImplementationPlanItem>,
}

impl ExchangeApiAuditReport {
    pub fn from_profiles(
        generated_at: DateTime<Utc>,
        exchanges: Vec<ExchangeAdapterProfile>,
    ) -> Self {
        let missing_features = exchanges
            .iter()
            .flat_map(ExchangeAdapterProfile::missing_features)
            .collect();
        let inconsistent_behaviors = exchanges
            .iter()
            .flat_map(ExchangeAdapterProfile::inconsistent_behaviors)
            .collect();
        let implementation_plan = default_adapter_implementation_plan();
        Self {
            generated_at,
            exchanges,
            missing_features,
            inconsistent_behaviors,
            implementation_plan,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterImplementationPlanItem {
    pub phase: u8,
    pub title: String,
    pub details: Vec<String>,
}

pub fn default_adapter_implementation_plan() -> Vec<AdapterImplementationPlanItem> {
    vec![
        AdapterImplementationPlanItem {
            phase: 1,
            title: "freeze unified order contract".to_string(),
            details: vec![
                "strategy code consumes ExchangeAdapter only".to_string(),
                "all venue statuses map to NEW/PARTIALLY_FILLED/FILLED/CANCELED/REJECTED"
                    .to_string(),
                "client_order_id is mandatory for live order replay and reconciliation".to_string(),
            ],
        },
        AdapterImplementationPlanItem {
            phase: 2,
            title: "complete per-venue capability matrix".to_string(),
            details: vec![
                "declare spot/perpetual/futures scope per adapter".to_string(),
                "declare native vs composed cancel-replace".to_string(),
                "add funding-rate and leverage capability to derivatives adapters".to_string(),
            ],
        },
        AdapterImplementationPlanItem {
            phase: 3,
            title: "wire reliable streams".to_string(),
            details: vec![
                "public books use snapshot plus diff recovery".to_string(),
                "sequence gaps block strategy evaluation until REST resync succeeds".to_string(),
                "private fills/orders fall back to query_order/open_orders/recent_fills"
                    .to_string(),
            ],
        },
        AdapterImplementationPlanItem {
            phase: 4,
            title: "enforce normalization layer".to_string(),
            details: vec![
                "price/quantity rounded by symbol rules before submit".to_string(),
                "min notional checked before submit".to_string(),
                "rate-limit buckets throttle before order writes".to_string(),
                "symbol mapping is market-type scoped".to_string(),
            ],
        },
        AdapterImplementationPlanItem {
            phase: 5,
            title: "standardize hedge execution".to_string(),
            details: vec![
                "shared slippage model for maker-open/taker-hedge and dual-taker close".to_string(),
                "common retry policy queries state before replay".to_string(),
                "single-leg exposure is reduced before new opens resume".to_string(),
            ],
        },
    ]
}

pub async fn adapter_smoke_readback<A: ExchangeAdapter + ?Sized>(
    adapter: &A,
    symbol_rules: SymbolRulesRequest,
    order_book: OrderBookRequest,
    positions: PositionsRequest,
    recent_fills: RecentFillsRequest,
) -> ExchangeApiResult<AdapterSmokeReadback> {
    let symbol_rules = adapter.get_symbol_rules(symbol_rules).await?;
    let order_book = adapter.get_order_book(order_book).await?;
    let positions = adapter.get_positions(positions).await?;
    let recent_fills = adapter.get_recent_fills(recent_fills).await?;
    Ok(AdapterSmokeReadback {
        symbol_rules,
        order_book,
        positions,
        recent_fills,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdapterSmokeReadback {
    pub symbol_rules: SymbolRulesResponse,
    pub order_book: OrderBookResponse,
    pub positions: PositionsResponse,
    pub recent_fills: RecentFillsResponse,
}

pub async fn amend_as_cancel_replace<A: ExchangeAdapter + ?Sized>(
    adapter: &A,
    cancel: CancelOrderRequest,
    replacement: PlaceOrderRequest,
    amend: Option<AmendOrderRequest>,
) -> ExchangeApiResult<CancelReplaceResponse> {
    if let Some(amend) = amend {
        let amended: AmendOrderResponse = adapter.amend_order(amend).await?;
        let cancel = CancelOrderResponse {
            schema_version: amended.schema_version,
            metadata: amended.metadata.clone(),
            order: amended.order.clone(),
            cancelled: false,
        };
        let replacement = PlaceOrderResponse {
            schema_version: amended.schema_version,
            metadata: amended.metadata,
            order: amended.order,
        };
        return Ok(CancelReplaceResponse {
            mode: CancelReplaceMode::AmendInPlace,
            cancel,
            replacement,
        });
    }
    adapter.cancel_replace_order(cancel, replacement).await
}

pub async fn set_leverage_if_supported<A>(
    adapter: &A,
    request: SetLeverageRequest,
) -> ExchangeApiResult<SetLeverageResponse>
where
    A: ExchangeAdapter + crate::PerpAccountControlProvider + ?Sized,
{
    adapter.set_leverage(request).await
}

fn support_from_bool(supported: bool, reason: impl Into<String>) -> AdapterSupport {
    if supported {
        AdapterSupport::Native
    } else {
        AdapterSupport::Missing {
            reason: reason.into(),
        }
    }
}

fn cancel_replace_support(capabilities: &crate::ExchangeClientCapabilities) -> AdapterSupport {
    if capabilities.supports_amend_order {
        AdapterSupport::Native
    } else if capabilities.supports_cancel_order && capabilities.supports_place_order {
        AdapterSupport::Composed {
            reason: "composed from cancel_order followed by place_order".to_string(),
        }
    } else {
        AdapterSupport::Missing {
            reason: "requires amend_order or cancel+place".to_string(),
        }
    }
}
