use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    CanonicalSymbol, ExchangeError, ExchangeId, ExchangeSymbol, Fill, MarketType, OrderSide,
    OrderStatus, OrderType, PageRequest, PositionSide, RequestContext, ResponseMetadata,
    SymbolScope, TimeInForce,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: Option<PositionSide>,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub quantity: String,
    pub price: Option<String>,
    pub quote_quantity: Option<String>,
    pub reduce_only: bool,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: OrderState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteMarketOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub quote_quantity: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmendOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub new_client_order_id: Option<String>,
    pub new_quantity: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmendOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: OrderState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderListKind {
    Oco,
    Oto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderListLegType {
    Market,
    Limit,
    LimitMaker,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListConditionalLeg {
    pub order_type: OrderListLegType,
    pub price: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListOrderLeg {
    pub side: OrderSide,
    pub order_type: OrderListLegType,
    pub quantity: String,
    pub price: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderListRequest {
    Oco {
        schema_version: u16,
        context: RequestContext,
        symbol: SymbolScope,
        list_client_order_id: Option<String>,
        side: OrderSide,
        quantity: String,
        above: OrderListConditionalLeg,
        below: OrderListConditionalLeg,
    },
    Oto {
        schema_version: u16,
        context: RequestContext,
        symbol: SymbolScope,
        list_client_order_id: Option<String>,
        working: OrderListOrderLeg,
        pending: OrderListOrderLeg,
    },
}

impl OrderListRequest {
    pub fn schema_version(&self) -> u16 {
        match self {
            Self::Oco { schema_version, .. } | Self::Oto { schema_version, .. } => *schema_version,
        }
    }

    pub fn context_request_id(&self) -> Option<String> {
        match self {
            Self::Oco { context, .. } | Self::Oto { context, .. } => context.request_id.clone(),
        }
    }

    pub fn symbol(&self) -> &SymbolScope {
        match self {
            Self::Oco { symbol, .. } | Self::Oto { symbol, .. } => symbol,
        }
    }

    pub fn list_client_order_id(&self) -> Option<String> {
        match self {
            Self::Oco {
                list_client_order_id,
                ..
            }
            | Self::Oto {
                list_client_order_id,
                ..
            } => list_client_order_id.clone(),
        }
    }

    pub fn kind(&self) -> OrderListKind {
        match self {
            Self::Oco { .. } => OrderListKind::Oco,
            Self::Oto { .. } => OrderListKind::Oto,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub symbol: SymbolScope,
    pub kind: OrderListKind,
    pub order_list_id: Option<String>,
    pub list_client_order_id: Option<String>,
    pub list_status_type: Option<String>,
    pub list_order_status: Option<String>,
    pub orders: Vec<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: OrderState,
    pub cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlaceOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub orders: Vec<PlaceOrderRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlaceOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<BatchOperationReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchCancelOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub cancels: Vec<CancelOrderRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchCancelOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
    pub cancelled_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<BatchOperationReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbol: Option<SymbolScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
    pub cancelled_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: Option<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbol: Option<SymbolScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<PageRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecentFillsRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbol: Option<SymbolScope>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub from_trade_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<PageRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecentFillsResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub fills: Vec<Fill>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileTrigger {
    PlaceOrderTimeout,
    CancelOrderTimeout,
    BatchPlacePartialFailure,
    BatchCancelPartialFailure,
    BatchResponseMissingItem,
    PrivateStreamDisconnected,
    OrderBookSequenceGap,
    LocalExchangeStateConflict,
    DuplicateClientOrderId,
    UnknownOrderState,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownOrderPolicy {
    QueryByClientOrderId,
    QueryByExchangeOrderId,
    CheckOpenOrders,
    CheckRecentFills,
    TreatAsRejected,
    ManualReview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryReconcilePolicy {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: u32,
    pub query_before_retry: bool,
    pub retry_only_with_client_order_id: bool,
}

impl Default for RetryReconcilePolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 250,
            max_delay_ms: 5_000,
            backoff_multiplier: 2,
            query_before_retry: true,
            retry_only_with_client_order_id: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOrderIdPolicy {
    pub required_for_place: bool,
    pub required_for_batch_place: bool,
    pub accepted_on_cancel: bool,
    pub accepted_on_query: bool,
    pub duplicate_requires_reconcile: bool,
    pub max_length: Option<u32>,
    pub allowed_pattern: Option<String>,
}

impl Default for ClientOrderIdPolicy {
    fn default() -> Self {
        Self {
            required_for_place: false,
            required_for_batch_place: false,
            accepted_on_cancel: false,
            accepted_on_query: false,
            duplicate_requires_reconcile: true,
            max_length: None,
            allowed_pattern: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyKey {
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub request_id: Option<String>,
}

impl IdempotencyKey {
    pub fn from_place_request(request: &PlaceOrderRequest) -> Self {
        Self {
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: None,
            request_id: request.context.request_id.clone(),
        }
    }

    pub fn from_cancel_request(request: &CancelOrderRequest) -> Self {
        Self {
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: request.exchange_order_id.clone(),
            request_id: request.context.request_id.clone(),
        }
    }

    pub fn has_order_identifier(&self) -> bool {
        self.client_order_id.is_some() || self.exchange_order_id.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderReconcileState {
    PendingQuery,
    FoundOpen,
    FoundFilled,
    FoundCancelled,
    FoundRejected,
    MissingNeedsOpenOrders,
    MissingNeedsFills,
    UnknownNeedsRetry,
    ManualReview,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconcilePlan {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub trigger: ReconcileTrigger,
    pub state: OrderReconcileState,
    pub symbol: Option<SymbolScope>,
    pub idempotency_key: Option<IdempotencyKey>,
    pub unknown_order_policy: UnknownOrderPolicy,
    pub retry_policy: RetryReconcilePolicy,
    pub requires_query_order: bool,
    pub requires_open_orders: bool,
    pub requires_recent_fills: bool,
    pub allow_order_replay: bool,
    pub reason: String,
}

impl ReconcilePlan {
    pub fn for_place_request(
        exchange: ExchangeId,
        trigger: ReconcileTrigger,
        request: &PlaceOrderRequest,
        retry_policy: RetryReconcilePolicy,
        reason: impl Into<String>,
    ) -> Self {
        let key = IdempotencyKey::from_place_request(request);
        Self::from_key(
            exchange,
            trigger,
            Some(request.symbol.clone()),
            Some(key),
            retry_policy,
            reason,
        )
    }

    pub fn for_cancel_request(
        exchange: ExchangeId,
        trigger: ReconcileTrigger,
        request: &CancelOrderRequest,
        retry_policy: RetryReconcilePolicy,
        reason: impl Into<String>,
    ) -> Self {
        let key = IdempotencyKey::from_cancel_request(request);
        Self::from_key(
            exchange,
            trigger,
            Some(request.symbol.clone()),
            Some(key),
            retry_policy,
            reason,
        )
    }

    pub fn from_key(
        exchange: ExchangeId,
        trigger: ReconcileTrigger,
        symbol: Option<SymbolScope>,
        idempotency_key: Option<IdempotencyKey>,
        retry_policy: RetryReconcilePolicy,
        reason: impl Into<String>,
    ) -> Self {
        let unknown_order_policy = idempotency_key
            .as_ref()
            .and_then(|key| {
                if key.client_order_id.is_some() {
                    Some(UnknownOrderPolicy::QueryByClientOrderId)
                } else if key.exchange_order_id.is_some() {
                    Some(UnknownOrderPolicy::QueryByExchangeOrderId)
                } else {
                    None
                }
            })
            .unwrap_or(UnknownOrderPolicy::CheckOpenOrders);
        let has_order_identifier = idempotency_key
            .as_ref()
            .is_some_and(IdempotencyKey::has_order_identifier);

        Self {
            schema_version: crate::EXCHANGE_API_SCHEMA_VERSION,
            exchange,
            trigger,
            state: OrderReconcileState::PendingQuery,
            symbol,
            idempotency_key,
            unknown_order_policy,
            retry_policy,
            requires_query_order: has_order_identifier,
            requires_open_orders: true,
            requires_recent_fills: false,
            allow_order_replay: false,
            reason: reason.into(),
        }
    }

    pub fn query_first(&self) -> bool {
        self.requires_query_order || self.requires_open_orders || self.requires_recent_fills
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchItemResult {
    pub index: usize,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub order: Option<OrderState>,
    pub error: Option<ExchangeError>,
    pub reconcile_plan: Option<ReconcilePlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchOperationReport {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub total_items: usize,
    pub results: Vec<BatchItemResult>,
}

impl BatchOperationReport {
    pub fn succeeded_count(&self) -> usize {
        self.results
            .iter()
            .filter(|item| item.order.is_some())
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.results
            .iter()
            .filter(|item| item.error.is_some())
            .count()
    }

    pub fn requires_reconciliation(&self) -> bool {
        self.results
            .iter()
            .any(BatchItemResult::requires_reconciliation)
    }
}

impl BatchItemResult {
    pub fn success(index: usize, order: OrderState) -> Self {
        Self {
            index,
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
            order: Some(order),
            error: None,
            reconcile_plan: None,
        }
    }

    pub fn failed(
        index: usize,
        client_order_id: Option<String>,
        exchange_order_id: Option<String>,
        error: ExchangeError,
        reconcile_plan: Option<ReconcilePlan>,
    ) -> Self {
        Self {
            index,
            client_order_id,
            exchange_order_id,
            order: None,
            error: Some(error),
            reconcile_plan,
        }
    }

    pub fn requires_reconciliation(&self) -> bool {
        self.reconcile_plan.is_some()
            || self
                .error
                .as_ref()
                .is_some_and(ExchangeError::requires_reconciliation)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderState {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: Option<PositionSide>,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub status: OrderStatus,
    pub quantity: String,
    pub price: Option<String>,
    pub filled_quantity: String,
    pub average_fill_price: Option<String>,
    pub reduce_only: bool,
    pub post_only: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
