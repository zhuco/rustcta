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
fn order_reconciliation_and_batch_report_types_should_serialize_stably() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let request = PlaceOrderRequest {
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
    let plan = ReconcilePlan::for_place_request(
        exchange_id(),
        ReconcileTrigger::PlaceOrderTimeout,
        &request,
        RetryReconcilePolicy::default(),
        "transport timeout",
    );
    let report = BatchOperationReport {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id(),
        total_items: 1,
        results: vec![BatchItemResult::failed(
            0,
            Some("cli-1".to_string()),
            None,
            ExchangeError::new(
                exchange_id(),
                rustcta_types::ExchangeErrorClass::UnknownOrderState,
                "unknown order state",
                now,
            ),
            Some(plan),
        )],
    };

    let value = serde_json::to_value(report).unwrap();

    assert_eq!(value["total_items"], 1);
    assert_eq!(value["results"][0]["index"], 0);
    assert_eq!(
        value["results"][0]["reconcile_plan"]["trigger"],
        "place_order_timeout"
    );
    assert_eq!(
        value["results"][0]["reconcile_plan"]["unknown_order_policy"],
        "query_by_client_order_id"
    );
    assert_eq!(
        value["results"][0]["reconcile_plan"]["allow_order_replay"],
        false
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
    assert!(capabilities.supports_order_book_snapshot);
    assert_eq!(capabilities.max_order_book_depth, Some(20));
    assert_eq!(
        capabilities.order_book,
        OrderBookCapability::snapshot_only(None)
    );
    assert_eq!(
        capabilities.capabilities_v2,
        ExchangeClientCapabilitiesV2::default()
    );
}

#[test]
fn capability_support_should_serialize_with_mode_and_reason() {
    let unsupported = CapabilitySupport::unsupported("official API does not expose batch create");
    let value = serde_json::to_value(&unsupported).unwrap();

    assert_eq!(value["mode"], "unsupported");
    assert_eq!(value["reason"], "official API does not expose batch create");
    assert!(!unsupported.is_supported());
    assert!(CapabilitySupport::native().is_supported());
}

#[test]
fn capability_v2_should_round_trip_structured_fields() {
    let mut capabilities = ExchangeClientCapabilities::new(exchange_id());
    capabilities.market_types = vec![MarketType::Spot];
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::rest_fallback("REST book snapshot is used for resync");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("private stream not verified");
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: CapabilitySupport::composed("sequential single-order fallback"),
        mode: BatchExecutionMode::ComposedSequential,
        atomicity: BatchAtomicity::NonAtomic,
        max_items: Some(5),
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::native(BatchAtomicity::Partial, Some(20));
    capabilities.capabilities_v2.cancel_all_orders = CapabilitySupport::native();
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: true,
        max_limit: Some(1000),
        max_window_ms: Some(86_400_000),
    };
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("closed order history not exposed");
    capabilities.capabilities_v2.endpoints = vec![EndpointCapability {
        operation: "place_order".to_string(),
        support: CapabilitySupport::native(),
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some("POST".to_string()),
        path: Some("/api/v3/order".to_string()),
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("order".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }];
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];

    let value = serde_json::to_value(&capabilities).unwrap();
    assert_eq!(
        value["capabilities_v2"]["batch_place_orders"]["mode"],
        "composed_sequential"
    );
    assert_eq!(
        value["capabilities_v2"]["batch_place_orders"]["support"]["mode"],
        "composed"
    );
    assert_eq!(value["capabilities_v2"]["endpoints"][0]["auth"], "hmac");
    assert_eq!(value["capabilities_v2"]["credential_scopes"][1], "trade");

    let round_trip: ExchangeClientCapabilities = serde_json::from_value(value).unwrap();
    assert_eq!(round_trip.capabilities_v2, capabilities.capabilities_v2);
}

#[test]
fn capability_v2_should_default_missing_nested_fields() {
    let value = serde_json::json!({
        "public_rest": { "mode": "native" },
        "batch_place_orders": {
            "support": {
                "mode": "unsupported",
                "reason": "not verified"
            }
        }
    });

    let capabilities: ExchangeClientCapabilitiesV2 = serde_json::from_value(value).unwrap();

    assert_eq!(capabilities.public_rest, CapabilitySupport::native());
    assert_eq!(
        capabilities.private_rest,
        CapabilitySupport::unsupported("not declared")
    );
    assert_eq!(
        capabilities.batch_place_orders.support,
        CapabilitySupport::unsupported("not verified")
    );
    assert_eq!(
        capabilities.stream_runtime,
        StreamRuntimeCapability::default()
    );
    assert!(capabilities.endpoints.is_empty());
    assert!(capabilities.credential_scopes.is_empty());
}

#[test]
fn capabilities_should_refresh_v2_from_legacy_flags() {
    let mut capabilities = ExchangeClientCapabilities::new(exchange_id());
    capabilities.supports_public_rest = true;
    capabilities.supports_private_rest = true;
    capabilities.supports_private_streams = true;
    capabilities.supports_place_order = true;
    capabilities.supports_cancel_order = true;
    capabilities.supports_batch_place_order = true;
    capabilities.supports_recent_fills = true;
    capabilities.max_recent_fill_limit = Some(500);

    capabilities.refresh_v2_from_legacy_flags();

    assert_eq!(
        capabilities.capabilities_v2.public_rest,
        CapabilitySupport::native()
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(500)
    );
    assert_eq!(
        capabilities.capabilities_v2.credential_scopes,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    );
}

#[test]
fn capabilities_should_apply_v2_to_legacy_flags() {
    let mut capabilities = ExchangeClientCapabilities::new(exchange_id());
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::native(BatchAtomicity::Partial, Some(10));
    capabilities.capabilities_v2.cancel_all_orders = CapabilitySupport::native();
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_since: true,
        supports_until: false,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(10),
        max_window_ms: None,
    };

    capabilities.apply_v2_to_legacy_flags();

    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_recent_fills);
    assert_eq!(capabilities.max_recent_fill_limit, Some(10));
    assert!(!capabilities.supports_batch_cancel_order);
}

#[test]
fn stream_runtime_capability_should_serialize_heartbeat_auth_and_resync() {
    let runtime = StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: CapabilitySupport::ws_only("orders only on private websocket"),
        supports_subscribe: true,
        supports_unsubscribe: true,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(10_000),
            ..Default::default()
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: Some(5),
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: false,
            positions: true,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: true,
            credential_scopes: vec![CredentialScope::ReadOnly],
            renewal_ms: Some(3_300_000),
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    };

    let value = serde_json::to_value(&runtime).unwrap();

    assert_eq!(value["public"]["mode"], "native");
    assert_eq!(value["private"]["mode"], "ws_only");
    assert_eq!(value["heartbeat"]["direction"], "client_ping");
    assert_eq!(value["reconnect"]["requires_resubscribe"], true);
    assert_eq!(value["resync"]["order_book"], true);
    assert_eq!(value["auth"]["credential_scopes"][0], "read_only");
    assert_eq!(value["auth"]["uses_listen_key"], true);

    let round_trip: StreamRuntimeCapability = serde_json::from_value(value).unwrap();
    assert_eq!(round_trip, runtime);
}

#[test]
fn exchange_error_kind_and_mapping_should_preserve_classification() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut error = ExchangeError::new(
        exchange_id(),
        rustcta_types::ExchangeErrorClass::RateLimited,
        "too many requests",
        now,
    );
    error.code = Some("429".to_string());
    error.retry_after_ms = Some(2500);

    let mapping = ExchangeErrorMapping::from(error);
    let value = serde_json::to_value(&mapping).unwrap();
    let decoded: ExchangeErrorMapping = serde_json::from_value(value.clone()).unwrap();

    assert_eq!(value["kind"], "rate_limited");
    assert_eq!(value["retry_after"]["millis"], 2500);
    assert_eq!(decoded.kind, ExchangeErrorKind::RateLimited);
    assert!(decoded.is_retryable());
    assert!(!decoded.requires_reconciliation());
    assert!(ExchangeErrorKind::OrderNotFound.requires_reconciliation());
    assert!(ExchangeErrorKind::Authentication.is_authentication_error());
}

#[test]
fn exchange_error_kind_should_round_trip_all_exchange_error_classes() {
    use rustcta_types::ExchangeErrorClass as Class;

    let classes = [
        Class::Unsupported,
        Class::UnsupportedCapability,
        Class::Authentication,
        Class::Permission,
        Class::RateLimited,
        Class::Network,
        Class::Timeout,
        Class::ExchangeUnavailable,
        Class::Maintenance,
        Class::InvalidRequest,
        Class::InvalidSymbol,
        Class::InvalidPrecision,
        Class::MinNotionalViolation,
        Class::InsufficientBalance,
        Class::InsufficientPosition,
        Class::DuplicateClientOrderId,
        Class::InvalidClientOrderId,
        Class::OrderNotFound,
        Class::OrderRejected,
        Class::RiskRejected,
        Class::StaleMarketData,
        Class::UnknownOrderState,
        Class::Decode,
        Class::Internal,
        Class::Unknown,
    ];

    for class in classes {
        let kind = ExchangeErrorKind::from(class);
        let round_trip = rustcta_types::ExchangeErrorClass::from(kind);

        assert_eq!(round_trip, class);
        assert_eq!(kind.is_retryable(), class.is_retryable());
        assert_eq!(
            kind.requires_reconciliation(),
            class.requires_reconciliation()
        );
    }
}

#[test]
fn order_level_error_should_serialize_snake_case_kind_and_order_ids() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let error = OrderLevelError::new(ExchangeErrorKind::DuplicateClientOrderId, "duplicate")
        .with_order(&order_state(now));

    let value = serde_json::to_value(error).unwrap();

    assert_eq!(value["kind"], "duplicate_client_order_id");
    assert_eq!(value["client_order_id"], "cli-1");
    assert_eq!(value["exchange_order_id"], "ex-1");
}

#[test]
fn rate_limit_plan_should_validate_buckets_and_reasons() {
    let bucket = RateLimitBucket::new("orders", RateLimitScope::Orders, 10, 1_000).with_burst(2);
    let plan = RateLimitPlan::fixed_window([bucket]);
    let sliding = RateLimitPlan::sliding_window([RateLimitBucket::new(
        "rest",
        RateLimitScope::Rest,
        20,
        1_000,
    )]);
    let header = RateLimitPlan::exchange_header(
        [RateLimitBucket::new(
            "headers",
            RateLimitScope::Ip,
            100,
            60_000,
        )],
        ["X-MBX-USED-WEIGHT"],
    );
    let invalid_header = RateLimitPlan::exchange_header(
        [RateLimitBucket::new(
            "headers",
            RateLimitScope::Ip,
            100,
            60_000,
        )],
        [""],
    );
    let missing_header = RateLimitPlan::exchange_header(
        [RateLimitBucket::new(
            "headers",
            RateLimitScope::Ip,
            100,
            60_000,
        )],
        Vec::<String>::new(),
    );
    let value = serde_json::to_value(&plan).unwrap();
    let decoded_bucket: RateLimitBucket = serde_json::from_value(serde_json::json!({
        "name": "legacy-orders",
        "scope": "orders",
        "limit": 10,
        "window_ms": 1000
    }))
    .unwrap();

    assert!(plan.validate().is_ok());
    assert!(sliding.validate().is_ok());
    assert!(header.validate().is_ok());
    assert!(invalid_header.validate().is_err());
    assert!(missing_header.validate().is_err());
    assert_eq!(value["fixed_window"]["buckets"][0]["scope"], "orders");
    assert_eq!(decoded_bucket.cost, 1);
    assert!(decoded_bucket.validate().is_ok());
    assert!(RateLimitPlan::fixed_window(Vec::<RateLimitBucket>::new())
        .validate()
        .is_err());
    assert!(RateLimitPlan::unsupported("").validate().is_err());
    assert!(RateLimitBucket::new("", RateLimitScope::Rest, 1, 1)
        .validate()
        .is_err());
}

#[test]
fn page_request_should_validate_limit_and_cursor() {
    let request = PageRequest::next_page(Some(100), PageCursor::token("next-token"));
    let value = serde_json::to_value(&request).unwrap();
    let cursors = vec![
        PageCursor::Offset { offset: 10 },
        PageCursor::Id {
            id: "fill-1".to_string(),
        },
        PageCursor::Timestamp {
            millis: 1_717_200_000_000,
        },
        PageCursor::Token {
            token: "next-token".to_string(),
        },
        PageCursor::TimeRange {
            start_ms: 1_717_200_000_000,
            end_ms: Some(1_717_286_400_000),
        },
    ];

    assert!(request.validate(Some(500)).is_ok());
    assert_eq!(value["cursor"]["token"]["token"], "next-token");
    for cursor in cursors {
        let value = serde_json::to_value(&cursor).unwrap();
        let decoded: PageCursor = serde_json::from_value(value).unwrap();

        assert_eq!(decoded, cursor);
        assert!(!decoded.is_empty());
    }
    assert!(PageRequest::first_page(0).validate(None).is_err());
    assert!(PageRequest::first_page(501).validate(Some(500)).is_err());
    assert!(PageCursor::token("").is_empty());
    assert!(PageCursor::token("   ").is_empty());
    assert!(PageRequest::next_page(Some(100), PageCursor::token(""))
        .validate(None)
        .is_err());
    assert!(
        PageRequest::next_page(None, PageCursor::Id { id: String::new() })
            .validate(None)
            .is_err()
    );
}

#[test]
fn paged_requests_and_batch_reports_should_deserialize_legacy_payloads() {
    let now = DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let open_orders: OpenOrdersRequest = serde_json::from_value(serde_json::json!({
        "schema_version": EXCHANGE_API_SCHEMA_VERSION,
        "context": context(now),
        "exchange": "binance",
        "market_type": "spot",
        "symbol": null
    }))
    .unwrap();
    let recent: RecentFillsRequest = serde_json::from_value(serde_json::json!({
        "schema_version": EXCHANGE_API_SCHEMA_VERSION,
        "context": context(now),
        "exchange": "binance",
        "market_type": "spot",
        "symbol": null,
        "client_order_id": null,
        "exchange_order_id": null,
        "from_trade_id": null,
        "start_time": null,
        "end_time": null,
        "limit": 100
    }))
    .unwrap();
    let batch: BatchPlaceOrdersResponse = serde_json::from_value(serde_json::json!({
        "schema_version": EXCHANGE_API_SCHEMA_VERSION,
        "metadata": ResponseMetadata::new(exchange_id(), now),
        "orders": []
    }))
    .unwrap();
    let cancel_batch: BatchCancelOrdersResponse = serde_json::from_value(serde_json::json!({
        "schema_version": EXCHANGE_API_SCHEMA_VERSION,
        "metadata": ResponseMetadata::new(exchange_id(), now),
        "orders": [],
        "cancelled_count": 0
    }))
    .unwrap();

    assert!(open_orders.page.is_none());
    assert!(recent.page.is_none());
    assert_eq!(recent.limit, Some(100));
    assert!(batch.report.is_none());
    assert!(cancel_batch.report.is_none());
}
