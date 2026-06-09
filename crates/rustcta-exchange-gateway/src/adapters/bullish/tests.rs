use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, CapabilitySupport, ExchangeApiError, ExchangeClient,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::{
    amend_order_request_spec_fixture, cancel_all_orders_request_spec_fixture,
    create_order_request_spec_fixture,
};
use super::signing::{bullish_hmac_payload, bullish_hmac_signature};
use super::streams::{
    bullish_keepalive_payload, bullish_order_book_stream_spec, bullish_reconnect_policy_ms,
    bullish_subscribe_payload, bullish_unsubscribe_payload, parse_bullish_public_stream_events,
    parse_bullish_public_stream_message, BullishPublicStreamMessage,
};
use super::{BullishGatewayAdapter, BullishGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bullish").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol(market_type: MarketType) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDC").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTCUSDC")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_keep_live_rest_disabled_while_documenting_product_scope() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual, MarketType::Futures]
    );
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
}

#[test]
fn capabilities_v2_should_expose_bullish_advanced_order_boundaries() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(matches!(
        capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.batch_place_orders.support,
        CapabilitySupport::Unsupported { .. }
    ));

    let amend = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "bullish.amend_order")
        .expect("amend endpoint boundary");
    assert_eq!(amend.method.as_deref(), Some("POST"));
    assert_eq!(amend.path.as_deref(), Some("/trading-api/v2/command"));
    assert!(matches!(
        amend.support,
        CapabilitySupport::Unsupported { .. }
    ));

    let cancel_all = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "bullish.cancel_all_orders")
        .expect("cancel-all endpoint boundary");
    assert_eq!(cancel_all.path.as_deref(), Some("/trading-api/v2/command"));

    let batch_place = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "bullish.batch_place_orders")
        .expect("batch-place unsupported boundary");
    assert!(batch_place.path.is_none());

    let batch_cancel = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "bullish.batch_cancel_orders")
        .expect("batch-cancel unsupported boundary");
    assert!(batch_cancel.path.is_none());

    let order_list = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "bullish.place_order_list")
        .expect("order-list unsupported boundary");
    assert!(order_list.path.is_none());
}

#[test]
fn parser_fixtures_should_decode_markets_and_order_book() {
    let markets: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/markets.json"
    ))
    .expect("markets fixture");
    let rules = parse_symbol_rules(&exchange_id(), &[], &markets).expect("symbol rules");
    assert_eq!(rules.len(), 2);
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.market_type == MarketType::Spot));
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.market_type == MarketType::Perpetual));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/orderbook_hybrid.json"
    ))
    .expect("depth fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol(MarketType::Spot), &depth)
        .expect("order book");
    assert_eq!(book.bids[0].price, 64999.5);
    assert_eq!(book.asks[0].price, 65000.0);
    assert_eq!(book.sequence, Some(123456789));
}

#[test]
fn request_spec_and_signing_fixture_should_match_official_hmac_shape() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/trading-api/v2/orders");
    assert_eq!(spec["auth"], "bearer_jwt_plus_bx_signature");

    let amend = amend_order_request_spec_fixture();
    assert_eq!(amend["path"], "/trading-api/v2/command");
    assert_eq!(amend["body"]["commandType"], "V1AmendOrder");

    let cancel_all = cancel_all_orders_request_spec_fixture();
    assert_eq!(cancel_all["path"], "/trading-api/v2/command");
    assert_eq!(
        cancel_all["body"]["commandType"],
        "V1CancelAllOrdersByMarket"
    );

    let payload = bullish_hmac_payload(
        1_700_000_000_000,
        1_700_000_000_001,
        "POST",
        "/trading-api/v2/orders",
        r#"{"commandType":"V3CreateOrder","symbol":"BTCUSDC"}"#,
    );
    let signature = bullish_hmac_signature("test-secret", &payload).expect("signature");
    assert_eq!(signature.len(), 64);

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/signing_vectors/hmac_create_order.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["payload"], payload);
    assert_eq!(fixture["expected_signature"], signature);
}

#[tokio::test]
async fn advanced_order_commands_should_remain_offline_request_spec_only() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig::default()).expect("adapter");

    let amend = AmendOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("amend"),
        symbol: symbol(MarketType::Spot),
        client_order_id: Some("offline-client-order-id".to_string()),
        exchange_order_id: Some("offline-order-id".to_string()),
        new_client_order_id: None,
        new_quantity: "0.01".to_string(),
    };
    let error = adapter.amend_order(amend).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bullish.amend_order_offline_request_spec_only"
        }
    ));

    let cancel_all = CancelAllOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel-all"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        symbol: Some(symbol(MarketType::Spot)),
    };
    let error = adapter
        .cancel_all_orders(cancel_all)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bullish.cancel_all_orders_offline_request_spec_only"
        }
    ));

    let place = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-item"),
        symbol: symbol(MarketType::Spot),
        client_order_id: Some("bullish-batch-place-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![place],
        })
        .await
        .expect_err("unsupported batch place");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bullish.batch_place_orders_unsupported"
        }
    ));

    let error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-item"),
                symbol: symbol(MarketType::Spot),
                client_order_id: Some("bullish-cancel-1".to_string()),
                exchange_order_id: Some("123456789".to_string()),
            }],
        })
        .await
        .expect_err("unsupported batch cancel");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bullish.batch_cancel_orders_unsupported"
        }
    ));

    let error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol(MarketType::Spot),
            list_client_order_id: Some("bullish-oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("66000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("bullish-oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("64000".to_string()),
                stop_price: Some("64200".to_string()),
                time_in_force: None,
                client_order_id: Some("bullish-oco-below".to_string()),
            },
        })
        .await
        .expect_err("unsupported order list");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bullish.order_list_unsupported"
        }
    ));
}

#[tokio::test]
async fn place_order_should_be_explicitly_unsupported_until_transport_is_promoted() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Spot),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bullish.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn websocket_helpers_should_build_json_rpc_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = bullish_subscribe_payload(
        "l2Orderbook",
        Some(&subscription.symbol.exchange_symbol.symbol),
        "1",
    );
    assert_eq!(payload["jsonrpc"], "2.0");
    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["params"]["symbol"], "BTCUSDC");
    assert_eq!(
        bullish_unsubscribe_payload("l2Orderbook", Some("BTCUSDC"), "3")["method"],
        "unsubscribe"
    );
    assert_eq!(bullish_keepalive_payload("2")["method"], "keepalivePing");
    assert_eq!(
        bullish_order_book_stream_spec()["sequence_fields"],
        serde_json::json!(["sequenceNumber", "sequenceNumberRange"])
    );
    assert_eq!(bullish_reconnect_policy_ms(), (240_000, 30_000, 300_000));
}

#[tokio::test]
async fn websocket_subscription_should_return_structured_public_spec_when_enabled() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig {
        enabled_public_streams: true,
        ..BullishGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_public_subscribe
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );

    let spec = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("ws-spec"),
            symbol: symbol(MarketType::Spot),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public ws spec");
    let spec: serde_json::Value = serde_json::from_str(&spec).expect("json");
    assert_eq!(
        spec["url"],
        "wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook"
    );
    assert_eq!(spec["payload"]["params"]["topic"], "l2Orderbook");
    assert_eq!(
        spec["order_book"]["topics"],
        serde_json::json!(["l1Orderbook", "l2Orderbook"])
    );
    assert_eq!(spec["reconnect"]["orderbook_requires_rest_snapshot"], true);
}

#[test]
fn websocket_parser_should_decode_l1_and_l2_order_book_messages() {
    let l1 = serde_json::json!({
        "type": "update",
        "dataType": "V1TABookLevel1",
        "data": {
            "sequenceNumber": "42",
            "symbol": "BTCUSDC",
            "timestamp": "1700000000000",
            "bid": [["64999.50", "1.250000"]],
            "ask": [["65000.00", "0.750000"]]
        }
    });
    let parsed = parse_bullish_public_stream_message(&exchange_id(), symbol(MarketType::Spot), &l1)
        .expect("l1");
    match parsed {
        BullishPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.sequence, Some(42));
            assert_eq!(book.bids[0].price, 64999.5);
            assert_eq!(book.asks[0].quantity, 0.75);
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let l2 = serde_json::json!({
        "type": "update",
        "dataType": "V1TALevel2",
        "data": {
            "symbol": "BTCUSDC",
            "bids": ["64999.50", "1.250000"],
            "asks": ["65000.00", "0.750000"],
            "sequenceNumberRange": [43, 45],
            "datetime": "2026-06-08T00:00:00.000Z"
        }
    });
    let events =
        parse_bullish_public_stream_events(&exchange_id(), symbol(MarketType::Perpetual), &l2)
            .expect("events");
    match events.first() {
        Some(rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot(book)) => {
            assert_eq!(book.order_book.sequence, Some(45));
            assert_eq!(book.order_book.bids[0].price, 64999.5);
        }
        other => panic!("unexpected events: {other:?}"),
    }
}
