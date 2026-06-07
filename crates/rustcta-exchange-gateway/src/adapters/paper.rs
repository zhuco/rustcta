use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderState,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, MarketType, OrderBookSnapshot,
    OrderStatus, OrderType,
};

use super::{
    ensure_exchange_api_schema, missing_order_identity, response_metadata, GatewayAdapter,
};
use crate::{GatewayError, GatewayExchangeStatus};

#[derive(Debug, Clone)]
pub struct PaperGatewayConfig {
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub public_stream_connected: bool,
    pub private_stream_connected: bool,
}

impl PaperGatewayConfig {
    pub fn new(exchange_id: ExchangeId) -> Self {
        Self {
            exchange_id,
            market_type: MarketType::Spot,
            public_stream_connected: true,
            private_stream_connected: true,
        }
    }
}

#[derive(Clone)]
pub struct PaperGatewayAdapter {
    exchange_id: ExchangeId,
    state: Arc<RwLock<PaperGatewayState>>,
}

#[derive(Debug)]
struct PaperGatewayState {
    config: PaperGatewayConfig,
    started_at: DateTime<Utc>,
    balances: Vec<ExchangeBalance>,
    order_books: HashMap<String, OrderBookSnapshot>,
    orders: Vec<OrderState>,
    public_subscriptions: Vec<String>,
    next_order_sequence: u64,
    next_subscription_sequence: u64,
}

impl PaperGatewayAdapter {
    pub fn new(config: PaperGatewayConfig) -> Self {
        Self {
            exchange_id: config.exchange_id.clone(),
            state: Arc::new(RwLock::new(PaperGatewayState {
                config,
                started_at: Utc::now(),
                balances: Vec::new(),
                order_books: HashMap::new(),
                orders: Vec::new(),
                public_subscriptions: Vec::new(),
                next_order_sequence: 1,
                next_subscription_sequence: 1,
            })),
        }
    }

    pub fn default_paper() -> Result<Self, GatewayError> {
        let exchange_id =
            ExchangeId::new("paper").map_err(|error| GatewayError::Rejected(error.to_string()))?;
        Ok(Self::new(PaperGatewayConfig::new(exchange_id)))
    }

    #[cfg(test)]
    pub fn insert_balance_snapshot(&self, balance: ExchangeBalance) -> Result<(), GatewayError> {
        self.ensure_exchange(&balance.exchange_id)
            .map_err(super::exchange_api_error_to_gateway)?;
        self.write_state()?.balances.push(balance);
        Ok(())
    }

    #[cfg(test)]
    pub fn insert_order_book(&self, order_book: OrderBookSnapshot) -> Result<(), GatewayError> {
        self.ensure_exchange(&order_book.exchange_id)
            .map_err(super::exchange_api_error_to_gateway)?;
        let key = order_book_key(
            &order_book.exchange_id,
            order_book.market_type,
            order_book.exchange_symbol.as_ref(),
            Some(&order_book.canonical_symbol),
        );
        self.write_state()?.order_books.insert(key, order_book);
        Ok(())
    }

    #[cfg(test)]
    pub fn recorded_public_subscriptions(&self) -> Result<Vec<String>, GatewayError> {
        Ok(self.read_state()?.public_subscriptions.clone())
    }

    fn read_state(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, PaperGatewayState>, GatewayError> {
        self.state
            .read()
            .map_err(|_| GatewayError::Rejected("paper adapter state lock poisoned".to_string()))
    }

    fn write_state(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, PaperGatewayState>, GatewayError> {
        self.state
            .write()
            .map_err(|_| GatewayError::Rejected("paper adapter state lock poisoned".to_string()))
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "paper adapter for {} cannot serve request for {}",
                    self.exchange_id, exchange
                ),
            });
        }
        Ok(())
    }

    fn ensure_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        let state = self
            .read_state()
            .map_err(gateway_lock_error_to_exchange_api)?;
        if state.config.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "paper adapter market {:?} cannot serve request for {:?}",
                    state.config.market_type, market_type
                ),
            });
        }
        Ok(())
    }

    fn status(&self) -> GatewayExchangeStatus {
        match self.read_state() {
            Ok(state) => GatewayExchangeStatus {
                exchange: self.exchange_id.to_string(),
                enabled: true,
                public_stream_connected: state.config.public_stream_connected,
                private_stream_connected: state.config.private_stream_connected,
                last_heartbeat_at: Some(Utc::now()),
                rate_limit_used: Some(0.0),
                message: Some(format!(
                    "paper gateway adapter started_at={}",
                    state.started_at.to_rfc3339()
                )),
            },
            Err(error) => GatewayExchangeStatus {
                exchange: self.exchange_id.to_string(),
                enabled: false,
                public_stream_connected: false,
                private_stream_connected: false,
                last_heartbeat_at: None,
                rate_limit_used: None,
                message: Some(error.to_string()),
            },
        }
    }
}

#[async_trait]
impl GatewayAdapter for PaperGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        self.status()
    }
}

#[async_trait]
impl ExchangeClient for PaperGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let market_type = self
            .read_state()
            .map(|state| state.config.market_type)
            .unwrap_or(MarketType::Spot);
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![market_type];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = true;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = true;
        capabilities.private_stream_capabilities =
            Some(rustcta_exchange_api::PrivateStreamCapabilities {
                schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
                supports_orders: true,
                supports_fills: true,
                supports_balances: true,
                supports_positions: false,
                supports_account: true,
                order_event_kinds: vec![
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Ack,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::New,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::PartialFill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Fill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Cancel,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Reject,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Expired,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::BalanceUpdate,
                ],
                supports_client_order_id: true,
                supports_exchange_order_id: true,
            });
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = true;
        capabilities.supports_place_order = true;
        capabilities.supports_cancel_order = true;
        capabilities.supports_query_order = true;
        capabilities.supports_open_orders = true;
        capabilities.supports_batch_place_order = true;
        capabilities.supports_batch_cancel_order = true;
        capabilities.supports_cancel_all_orders = true;
        capabilities.supports_client_order_id = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::strict_delta(Some(200));
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_market_type(market_type)?;
        }

        let requested_assets = request
            .assets
            .iter()
            .map(|asset| asset.trim().to_ascii_uppercase())
            .collect::<Vec<_>>();
        let balances = self
            .read_state()
            .map_err(gateway_lock_error_to_exchange_api)?
            .balances
            .iter()
            .filter(|balance| {
                request
                    .market_type
                    .map_or(true, |market_type| balance.market_type == market_type)
            })
            .filter(|balance| {
                request
                    .context
                    .account_id
                    .as_ref()
                    .map_or(true, |account_id| &balance.account_id == account_id)
            })
            .cloned()
            .map(|mut balance| {
                if !requested_assets.is_empty() {
                    balance
                        .balances
                        .retain(|asset| requested_assets.contains(&asset.asset));
                }
                balance
            })
            .collect();

        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: Vec::new(),
        })
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .unwrap_or_else(|| self.exchange_id.clone());
        self.ensure_exchange(&exchange)?;
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: Vec::new(),
        })
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;

        let key = order_book_key(
            &request.symbol.exchange,
            request.symbol.market_type,
            Some(&request.symbol.exchange_symbol),
            request.symbol.canonical_symbol.as_ref(),
        );
        let mut order_book = self
            .read_state()
            .map_err(gateway_lock_error_to_exchange_api)?
            .order_books
            .get(&key)
            .cloned()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!(
                    "paper adapter has no order book for {}",
                    request.symbol.exchange_symbol.symbol
                ),
            })?;

        if let Some(depth) = request.depth {
            let depth = depth as usize;
            order_book.bids.truncate(depth);
            order_book.asks.truncate(depth);
        }

        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order_book,
        })
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .unwrap_or_else(|| self.exchange_id.clone());
        self.ensure_exchange(&exchange)?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            fees: Vec::new(),
        })
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        parse_positive_decimal(&request.quantity, "quantity")?;
        if request.order_type.requires_limit_price() {
            let price =
                request
                    .price
                    .as_deref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "limit-like paper order requires price".to_string(),
                    })?;
            parse_positive_decimal(price, "price")?;
        } else if let Some(price) = request.price.as_deref() {
            parse_positive_decimal(price, "price")?;
        }

        let now = Utc::now();
        let mut state = self
            .write_state()
            .map_err(gateway_lock_error_to_exchange_api)?;
        if let Some(client_order_id) = &request.client_order_id {
            if state
                .orders
                .iter()
                .any(|order| order.client_order_id.as_ref() == Some(client_order_id))
            {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!("duplicate paper client_order_id {client_order_id}"),
                });
            }
        }
        let exchange_order_id = format!("paper-order-{}", state.next_order_sequence);
        state.next_order_sequence += 1;
        let order = OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: request.symbol.exchange.clone(),
            market_type: request.symbol.market_type,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: Some(exchange_order_id),
            side: request.side,
            position_side: request.position_side,
            order_type: request.order_type,
            time_in_force: request.time_in_force,
            status: OrderStatus::Open,
            quantity: request.quantity.clone(),
            price: request.price.clone(),
            filled_quantity: "0".to_string(),
            average_fill_price: None,
            reduce_only: request.reduce_only,
            post_only: request.post_only,
            created_at: Some(now),
            updated_at: now,
        };
        state.orders.push(order.clone());

        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cancel_order requires client_order_id or exchange_order_id".to_string(),
            });
        }

        let now = Utc::now();
        let mut state = self
            .write_state()
            .map_err(gateway_lock_error_to_exchange_api)?;
        let order = state
            .orders
            .iter_mut()
            .find(|order| order_matches_cancel(order, &request))
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "paper order not found".to_string(),
            })?;
        order.status = OrderStatus::Cancelled;
        order.updated_at = now;
        let order = order.clone();

        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;

        let request_id = request.context.request_id.clone();
        let exchange = request.exchange.clone();
        let mut orders = Vec::with_capacity(request.orders.len());
        for order_request in request.orders {
            orders.push(self.place_order(order_request).await?.order);
        }

        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            orders,
        })
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;

        let request_id = request.context.request_id.clone();
        let exchange = request.exchange.clone();
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel_request in request.cancels {
            orders.push(self.cancel_order(cancel_request).await?.order);
        }
        let cancelled_count = orders.len() as u32;

        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            orders,
            cancelled_count,
        })
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_market_type(market_type)?;
        }
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            if symbol.exchange != request.exchange {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "cancel_all_orders symbol exchange must match request exchange"
                        .to_string(),
                });
            }
            if request
                .market_type
                .is_some_and(|market_type| symbol.market_type != market_type)
            {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "cancel_all_orders symbol market_type must match request market_type"
                        .to_string(),
                });
            }
        }

        let now = Utc::now();
        let mut state = self
            .write_state()
            .map_err(gateway_lock_error_to_exchange_api)?;
        let mut cancelled = Vec::new();
        for order in &mut state.orders {
            if order.exchange != request.exchange {
                continue;
            }
            if order.status.is_terminal() {
                continue;
            }
            if request
                .market_type
                .is_some_and(|market_type| order.market_type != market_type)
            {
                continue;
            }
            if request
                .symbol
                .as_ref()
                .is_some_and(|symbol| order.exchange_symbol != symbol.exchange_symbol)
            {
                continue;
            }

            order.status = OrderStatus::Cancelled;
            order.updated_at = now;
            cancelled.push(order.clone());
        }
        let cancelled_count = cancelled.len() as u32;

        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: cancelled,
            cancelled_count,
        })
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_none() && request.exchange_order_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "query_order requires client_order_id or exchange_order_id".to_string(),
            });
        }
        let order = self
            .read_state()
            .map_err(gateway_lock_error_to_exchange_api)?
            .orders
            .iter()
            .find(|order| order_matches_query(order, &request))
            .cloned();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_market_type(market_type)?;
        }
        let orders = self
            .read_state()
            .map_err(gateway_lock_error_to_exchange_api)?
            .orders
            .iter()
            .filter(|order| !order.status.is_terminal())
            .filter(|order| {
                request
                    .market_type
                    .map_or(true, |market_type| order.market_type == market_type)
            })
            .filter(|order| {
                request.symbol.as_ref().map_or(true, |symbol| {
                    order.exchange_symbol == symbol.exchange_symbol
                })
            })
            .cloned()
            .collect();
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: Vec::new(),
        })
    }

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        if !matches!(
            subscription.kind,
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta
        ) {
            return Err(ExchangeApiError::Unsupported {
                operation: "paper.subscribe_public_stream.non_order_book",
            });
        }
        let mut state = self
            .write_state()
            .map_err(gateway_lock_error_to_exchange_api)?;
        let subscription_id = format!("paper-book-sub-{}", state.next_subscription_sequence);
        state.next_subscription_sequence += 1;
        state.public_subscriptions.push(subscription_id.clone());
        Ok(subscription_id)
    }

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let mut state = self
            .write_state()
            .map_err(gateway_lock_error_to_exchange_api)?;
        let subscription_id = format!("paper-private-sub-{}", state.next_subscription_sequence);
        state.next_subscription_sequence += 1;
        Ok(subscription_id)
    }
}

fn parse_positive_decimal(value: &str, field: &'static str) -> ExchangeApiResult<f64> {
    let value = value
        .trim()
        .parse::<f64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("{field} must be a decimal number"),
        })?;
    if !value.is_finite() || value <= 0.0 {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("{field} must be greater than 0"),
        });
    }
    Ok(value)
}

fn order_matches_cancel(order: &OrderState, request: &CancelOrderRequest) -> bool {
    order.exchange_symbol == request.symbol.exchange_symbol
        && (request
            .client_order_id
            .as_ref()
            .map_or(false, |id| order.client_order_id.as_ref() == Some(id))
            || request
                .exchange_order_id
                .as_ref()
                .map_or(false, |id| order.exchange_order_id.as_ref() == Some(id)))
}

fn order_matches_query(order: &OrderState, request: &QueryOrderRequest) -> bool {
    order.exchange_symbol == request.symbol.exchange_symbol
        && (request
            .client_order_id
            .as_ref()
            .map_or(false, |id| order.client_order_id.as_ref() == Some(id))
            || request
                .exchange_order_id
                .as_ref()
                .map_or(false, |id| order.exchange_order_id.as_ref() == Some(id)))
}

fn order_book_key(
    exchange: &ExchangeId,
    market_type: MarketType,
    exchange_symbol: Option<&ExchangeSymbol>,
    canonical_symbol: Option<&CanonicalSymbol>,
) -> String {
    let symbol = exchange_symbol
        .map(|symbol| symbol.symbol.as_str())
        .or_else(|| canonical_symbol.map(CanonicalSymbol::as_str))
        .unwrap_or("*");
    format!("{exchange}:{market_type:?}:{symbol}")
}

fn gateway_lock_error_to_exchange_api(error: GatewayError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
