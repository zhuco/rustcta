use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::*;

fn exchange_id() -> ExchangeId {
    serde_json::from_str("\"binance\"").expect("exchange id should deserialize")
}

fn market_type() -> MarketType {
    serde_json::from_str("\"spot\"").expect("market type should deserialize")
}

fn exchange_symbol() -> ExchangeSymbol {
    ExchangeSymbol::new(exchange_id(), market_type(), "BTCUSDT")
        .expect("exchange symbol should construct")
}

fn order_side() -> OrderSide {
    serde_json::from_str("\"buy\"").expect("order side should deserialize")
}

fn order_type() -> OrderType {
    serde_json::from_str("\"limit\"").expect("order type should deserialize")
}

fn time_in_force() -> TimeInForce {
    serde_json::from_str("\"gtc\"").expect("time in force should deserialize")
}

fn order_status() -> OrderStatus {
    serde_json::from_str("\"open\"").expect("order status should deserialize")
}

fn context(now: DateTime<Utc>) -> RequestContext {
    RequestContext {
        request_id: Some("req-1".to_string()),
        ..RequestContext::new(now)
    }
}

fn symbol(now: DateTime<Utc>) -> SymbolScope {
    let _ = now;
    SymbolScope {
        exchange: exchange_id(),
        market_type: market_type(),
        canonical_symbol: None,
        exchange_symbol: exchange_symbol(),
    }
}

fn order_state(now: DateTime<Utc>) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id(),
        market_type: market_type(),
        canonical_symbol: None,
        exchange_symbol: exchange_symbol(),
        client_order_id: Some("cli-1".to_string()),
        exchange_order_id: Some("ex-1".to_string()),
        side: order_side(),
        position_side: None,
        order_type: order_type(),
        time_in_force: Some(time_in_force()),
        status: order_status(),
        quantity: "0.01000000".to_string(),
        price: Some("50000.00".to_string()),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: true,
        created_at: Some(now),
        updated_at: now,
    }
}

struct MockExchangeClient {
    exchange: ExchangeId,
    now: DateTime<Utc>,
}

#[async_trait]
impl ExchangeClient for MockExchangeClient {
    fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange.clone());
        capabilities.market_types.push(market_type());
        capabilities.supports_private_rest = true;
        capabilities.supports_place_order = true;
        capabilities.supports_query_order = true;
        capabilities.supports_client_order_id = true;
        capabilities.supports_time_in_force.push(time_in_force());
        capabilities.supports_order_types.push(order_type());
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(self.exchange.clone(), self.now)
            },
            balances: Vec::new(),
        })
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(self.exchange.clone(), self.now)
            },
            positions: Vec::new(),
        })
    }

    async fn get_symbol_rules(
        &self,
        _request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "get_symbol_rules",
        })
    }

    async fn get_order_book(
        &self,
        _request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "get_order_book",
        })
    }

    async fn get_fees(&self, _request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "get_fees",
        })
    }

    async fn place_order(
        &self,
        _request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata::new(self.exchange.clone(), self.now),
            order: order_state(self.now),
        })
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata::new(self.exchange.clone(), self.now),
            order: order_state(self.now),
            cancelled: true,
        })
    }

    async fn query_order(
        &self,
        _request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata::new(self.exchange.clone(), self.now),
            order: Some(order_state(self.now)),
        })
    }

    async fn get_open_orders(
        &self,
        _request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata::new(self.exchange.clone(), self.now),
            orders: vec![order_state(self.now)],
        })
    }

    async fn get_recent_fills(
        &self,
        _request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata::new(self.exchange.clone(), self.now),
            fills: Vec::new(),
        })
    }

    async fn subscribe_public_stream(
        &self,
        _subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        Ok("public-subscription".to_string())
    }

    async fn subscribe_private_stream(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        Ok("private-subscription".to_string())
    }
}

#[tokio::test]
async fn exchange_client_trait_object_should_dispatch_requests() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let client: Box<dyn ExchangeClient> = Box::new(MockExchangeClient {
        exchange: exchange_id(),
        now,
    });

    let request = QueryOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        symbol: symbol(now),
        client_order_id: Some("cli-1".to_string()),
        exchange_order_id: None,
    };

    let response = client.query_order(request).await.unwrap();

    assert_eq!(response.schema_version, EXCHANGE_API_SCHEMA_VERSION);
    assert_eq!(
        response.order.and_then(|order| order.client_order_id),
        Some("cli-1".to_string())
    );
    assert!(client.capabilities().supports_query_order);
}

#[test]
fn gateway_request_should_serialize_with_schema_version() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let request = GatewayRequest::PlaceOrder(PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        symbol: symbol(now),
        client_order_id: Some("cli-1".to_string()),
        side: order_side(),
        position_side: None,
        order_type: order_type(),
        time_in_force: Some(time_in_force()),
        quantity: "0.01000000".to_string(),
        price: Some("50000.00".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    });

    let value = serde_json::to_value(request).unwrap();

    assert_eq!(value["PlaceOrder"]["schema_version"], 1);
    assert_eq!(value["PlaceOrder"]["client_order_id"], "cli-1");
    assert_eq!(value["PlaceOrder"]["quantity"], "0.01000000");
    assert_eq!(value["PlaceOrder"]["post_only"], true);
}

#[test]
fn gateway_request_should_serialize_batch_and_cancel_all_payloads() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        symbol: symbol(now),
        client_order_id: Some("cli-1".to_string()),
        side: order_side(),
        position_side: None,
        order_type: order_type(),
        time_in_force: Some(time_in_force()),
        quantity: "0.01000000".to_string(),
        price: Some("50000.00".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    };
    let batch = GatewayRequest::BatchPlaceOrders(BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        exchange: exchange_id(),
        orders: vec![order],
    });
    let cancel_all = GatewayRequest::CancelAllOrders(CancelAllOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        exchange: exchange_id(),
        market_type: Some(market_type()),
        symbol: Some(symbol(now)),
    });

    let batch = serde_json::to_value(batch).unwrap();
    let cancel_all = serde_json::to_value(cancel_all).unwrap();

    assert_eq!(batch["BatchPlaceOrders"]["schema_version"], 1);
    assert_eq!(
        batch["BatchPlaceOrders"]["orders"][0]["client_order_id"],
        "cli-1"
    );
    assert_eq!(cancel_all["CancelAllOrders"]["schema_version"], 1);
    assert_eq!(
        cancel_all["CancelAllOrders"]["symbol"]["exchange_symbol"]["symbol"],
        "BTCUSDT"
    );
}

#[test]
fn gateway_request_should_serialize_advanced_spot_order_payloads() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let quote = GatewayRequest::PlaceQuoteMarketOrder(QuoteMarketOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        symbol: symbol(now),
        client_order_id: Some("cli-quote".to_string()),
        side: OrderSide::Buy,
        quote_quantity: "25.50".to_string(),
    });
    let amend = GatewayRequest::AmendOrder(AmendOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        symbol: symbol(now),
        client_order_id: None,
        exchange_order_id: Some("ex-1".to_string()),
        new_client_order_id: Some("cli-amend".to_string()),
        new_quantity: "0.00500000".to_string(),
    });
    let order_list = GatewayRequest::PlaceOrderList(OrderListRequest::Oco {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(now),
        symbol: symbol(now),
        list_client_order_id: Some("cli-list".to_string()),
        side: OrderSide::Sell,
        quantity: "0.01000000".to_string(),
        above: OrderListConditionalLeg {
            order_type: OrderListLegType::LimitMaker,
            price: Some("55000.00".to_string()),
            stop_price: None,
            time_in_force: None,
            client_order_id: Some("cli-above".to_string()),
        },
        below: OrderListConditionalLeg {
            order_type: OrderListLegType::StopLossLimit,
            price: Some("45000.00".to_string()),
            stop_price: Some("45500.00".to_string()),
            time_in_force: Some(TimeInForce::GTC),
            client_order_id: Some("cli-below".to_string()),
        },
    });

    let quote = serde_json::to_value(quote).unwrap();
    let amend = serde_json::to_value(amend).unwrap();
    let order_list = serde_json::to_value(order_list).unwrap();

    assert_eq!(quote["PlaceQuoteMarketOrder"]["quote_quantity"], "25.50");
    assert_eq!(amend["AmendOrder"]["new_quantity"], "0.00500000");
    assert_eq!(amend["AmendOrder"]["exchange_order_id"], "ex-1");
    assert_eq!(order_list["PlaceOrderList"]["Oco"]["schema_version"], 1);
    assert_eq!(
        order_list["PlaceOrderList"]["Oco"]["above"]["order_type"],
        "LimitMaker"
    );

    let capabilities = ExchangeClientCapabilities::new(exchange_id());
    assert!(!capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(capabilities.private_stream_capabilities.is_some());
    assert_eq!(
        capabilities.order_book,
        OrderBookCapability::snapshot_only(None)
    );
}

#[test]
fn capabilities_should_default_advanced_spot_flags_for_legacy_payloads() {
    let value = serde_json::json!({
        "schema_version": EXCHANGE_API_SCHEMA_VERSION,
        "exchange": "binance",
        "market_types": ["spot"],
        "supports_public_rest": true,
        "supports_private_rest": true,
        "supports_public_streams": false,
        "supports_private_streams": false,
        "supports_symbol_rules": true,
        "supports_order_book_snapshot": true,
        "supports_balances": true,
        "supports_positions": false,
        "supports_fees": true,
        "supports_place_order": false,
        "supports_cancel_order": false,
        "supports_query_order": true,
        "supports_open_orders": true,
        "supports_recent_fills": true,
        "supports_batch_place_order": false,
        "supports_batch_cancel_order": false,
        "supports_cancel_all_orders": false,
        "supports_client_order_id": true,
        "supports_reduce_only": false,
        "supports_post_only": true,
        "supports_time_in_force": ["gtc"],
        "supports_order_types": ["limit"],
        "max_order_book_depth": 20,
        "max_recent_fill_limit": 1000
    });

    let capabilities: ExchangeClientCapabilities = serde_json::from_value(value).unwrap();

    assert!(!capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(capabilities.private_stream_capabilities.is_none());
}
