use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchAtomicity, BatchCancelOrdersRequest,
    BatchExecutionMode, BatchPlaceOrdersRequest, CancelAllOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, PlaceOrderRequest, PositionsRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, SymbolRulesRequest,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::parser::{
    parse_hibachi_amend_order_ack, parse_hibachi_balances, parse_hibachi_batch_cancel_orders_ack,
    parse_hibachi_batch_place_orders_ack, parse_hibachi_orderbook_snapshot, parse_hibachi_orders,
    parse_hibachi_positions, parse_hibachi_recent_fills, parse_hibachi_symbol_rules,
};
use super::private::{
    hibachi_amend_order_body, hibachi_batch_cancel_orders_body, hibachi_batch_place_orders_body,
    hibachi_cancel_all_orders_body, hibachi_cancel_order_body, hibachi_open_orders_query,
    hibachi_place_limit_order_body, hibachi_query_order_query, hibachi_recent_fills_query,
    AMEND_ORDER_UNSUPPORTED, BATCH_CANCEL_UNSUPPORTED, BATCH_PLACE_UNSUPPORTED,
    ORDER_LIST_UNSUPPORTED, PLACE_ORDER_UNSUPPORTED, PRIVATE_REST_DISABLED,
};
use super::signing::{
    hibachi_cancel_order_payload_hex, hibachi_create_order_payload_hex,
    hibachi_hmac_sha256_signature_hex, HibachiSigningSide,
};
use super::streams::{
    hibachi_keepalive_payload, hibachi_private_account_ws_url, hibachi_public_subscribe_payload,
    hibachi_public_unsubscribe_payload, hibachi_stream_reconnect_policy_ms,
};
use super::test_support::{hibachi_context, hibachi_exchange_id, hibachi_symbol};
use super::{HibachiGatewayAdapter, HibachiGatewayConfig};
use crate::adapters::AdapterBackedGateway;
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "exchange_info" => {
            include_str!("../../../../../tests/fixtures/exchanges/hibachi/exchange_info.json")
        }
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/hibachi/orderbook.json")
        }
        "open_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/hibachi/open_orders.json")
        }
        "order_success" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/parser/order_success.json"
        ),
        "account_trades" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/parser/account_trades_success.json"
        ),
        "account_info_positions" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/parser/account_info_positions.json"
        ),
        "positions_source_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/positions_source_boundary.json"
        ),
        "amend_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/parser/amend_order_ack.json"
        ),
        "batch_place_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/parser/batch_place_orders_ack.json"
        ),
        "batch_cancel_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/parser/batch_cancel_orders_ack.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/unsupported_boundary.json"
        ),
        "signing" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/signing_vectors/exchange_managed_hmac.json"
        ),
        "ws_subscribe_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/ws/subscribe_orderbook.json"
        ),
        "ws_unsubscribe_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/ws/unsubscribe_orderbook.json"
        ),
        _ => panic!("unknown Hibachi fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "exchange_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/exchange_info.json"
        ),
        "orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/orderbook.json"
        ),
        "account_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/account_info_readonly.json"
        ),
        "balances" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/get_balances_account_info.json"
        ),
        "query_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/query_order.json"
        ),
        "open_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/get_open_orders.json"
        ),
        "recent_fills" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/get_recent_fills.json"
        ),
        "positions" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/positions_source_boundary.json"
        ),
        "place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/place_order_limit.json"
        ),
        "cancel_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/cancel_order.json"
        ),
        "cancel_all" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/cancel_all_orders.json"
        ),
        "amend_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/amend_order.json"
        ),
        "batch_place_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/batch_place_orders.json"
        ),
        "batch_cancel_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/batch_cancel_orders.json"
        ),
        _ => panic!("unknown Hibachi request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn named_registration_should_accept_hibachi() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["hibachi"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn capabilities_should_expose_public_perp_and_keep_private_runtime_closed() {
    let adapter = HibachiGatewayAdapter::new(HibachiGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_fees);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_balances);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(!capabilities
        .capabilities_v2
        .batch_place_orders
        .support
        .is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "amend_order"
                && endpoint.method.as_deref() == Some("PATCH")
                && endpoint.path.as_deref() == Some("/trade/order")
                && !endpoint.support.is_supported()
        }));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "batch_place_orders"
                && endpoint.method.as_deref() == Some("POST")
                && endpoint.path.as_deref() == Some("/trade/orders")
                && !endpoint.support.is_supported()
        }));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "batch_cancel_orders"
                && endpoint.method.as_deref() == Some("DELETE")
                && endpoint.path.as_deref() == Some("/trade/orders")
                && !endpoint.support.is_supported()
        }));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write"], "request_spec_only");

    let enabled_private = HibachiGatewayAdapter::new(HibachiGatewayConfig {
        enabled_private_rest: true,
        api_key: Some("test-api-key".to_string()),
        account_id: Some("42".to_string()),
        ..HibachiGatewayConfig::default()
    })
    .expect("private adapter");
    let private_capabilities = enabled_private.capabilities();
    assert!(private_capabilities.supports_positions);
    assert!(private_capabilities.supports_balances);
    assert!(private_capabilities.supports_query_order);
    assert!(private_capabilities.supports_open_orders);
    assert!(private_capabilities.supports_recent_fills);
    assert!(private_capabilities
        .capabilities_v2
        .order_history
        .support
        .is_supported());
    assert!(private_capabilities
        .capabilities_v2
        .fills_history
        .support
        .is_supported());
}

#[test]
fn public_rest_fixtures_should_parse_exchange_info_and_orderbook() {
    let rules = parse_hibachi_symbol_rules(&hibachi_exchange_id(), &fixture("exchange_info"))
        .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC/USDT-P");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("5"));
    assert!(rules[0].supports_reduce_only);

    let snapshot = parse_hibachi_orderbook_snapshot(
        &hibachi_exchange_id(),
        hibachi_symbol(),
        &fixture("orderbook"),
    )
    .expect("snapshot");
    assert_eq!(snapshot.best_bid().expect("bid").price, 64999.5);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65000.0);
    assert_eq!(snapshot.sequence, Some(123456));
}

#[test]
fn private_order_fixture_should_parse_standard_order_state() {
    let orders =
        parse_hibachi_orders(&hibachi_exchange_id(), &fixture("open_orders")).expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].exchange_order_id.as_deref(),
        Some("579183763093760000")
    );
    assert_eq!(orders[0].side, OrderSide::Buy);
    assert_eq!(orders[0].status, rustcta_types::OrderStatus::Open);
    assert_eq!(orders[0].filled_quantity, "0.1");

    let query_order = super::parser::parse_hibachi_order(
        &hibachi_exchange_id(),
        fixture("order_success").get("order").expect("order"),
    )
    .expect("query order");
    assert_eq!(
        query_order.exchange_order_id.as_deref(),
        Some("579183763093760000")
    );
    assert_eq!(query_order.status, rustcta_types::OrderStatus::Open);
}

#[test]
fn account_trades_fixture_should_parse_recent_fills() {
    let fills = parse_hibachi_recent_fills(
        &hibachi_exchange_id(),
        hibachi_context("fills").tenant_id.expect("tenant"),
        hibachi_context("fills").account_id.expect("account"),
        &hibachi_symbol(),
        &fixture("account_trades"),
    )
    .expect("fills");

    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].market_type, MarketType::Perpetual);
    assert_eq!(fills[0].order_id.as_deref(), Some("579183763093760000"));
    assert_eq!(fills[0].fill_id.as_deref(), Some("88112233"));
    assert_eq!(fills[0].side, OrderSide::Buy);
    assert_eq!(fills[0].price, 65000.0);
    assert_eq!(fills[0].quantity, 0.1);
    assert_eq!(fills[0].quote_quantity, Some(6500.0));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills[0].fee_amount, Some(3.25));
}

#[test]
fn account_info_fixture_should_parse_positions() {
    let positions = parse_hibachi_positions(
        &hibachi_exchange_id(),
        hibachi_context("positions").tenant_id.expect("tenant"),
        hibachi_context("positions").account_id.expect("account"),
        &[],
        &fixture("account_info_positions"),
    )
    .expect("positions");

    assert_eq!(positions.len(), 2);
    assert_eq!(
        positions[0].exchange_symbol.as_ref().unwrap().symbol,
        "BTC/USDT-P"
    );
    assert_eq!(positions[0].side, PositionSide::Long);
    assert_eq!(positions[0].quantity, 0.01);
    assert_eq!(positions[0].entry_price, Some(65000.0));
    assert_eq!(positions[0].mark_price, Some(65100.0));
    assert_eq!(positions[0].unrealized_pnl, Some(1.1));
    assert_eq!(positions[1].side, PositionSide::Short);
}

#[test]
fn account_info_fixture_should_parse_balances() {
    let balances = parse_hibachi_balances(
        &hibachi_exchange_id(),
        hibachi_context("balances").tenant_id.expect("tenant"),
        hibachi_context("balances").account_id.expect("account"),
        &[],
        &fixture("account_info_positions"),
    )
    .expect("balances");

    assert_eq!(balances.len(), 1);
    assert_eq!(balances[0].market_type, MarketType::Perpetual);
    assert_eq!(balances[0].balances.len(), 1);
    assert_eq!(balances[0].balances[0].asset, "USDT");
    assert_eq!(balances[0].balances[0].total, 1250.5);
    assert_eq!(balances[0].balances[0].available, 1250.5);
}

#[test]
fn advanced_order_ack_fixtures_should_parse_offline_boundaries() {
    let amend = parse_hibachi_amend_order_ack(&hibachi_exchange_id(), &fixture("amend_ack"))
        .expect("amend ack");
    assert_eq!(
        amend.order.exchange_order_id.as_deref(),
        Some("579183763093760001")
    );
    assert_eq!(
        amend.order.client_order_id.as_deref(),
        Some("hibachi-amend-boundary-1")
    );
    assert_eq!(amend.order.status, OrderStatus::Open);
    assert_eq!(amend.order.quantity, "1");
    assert_eq!(amend.order.price.as_deref(), Some("11"));

    let batch_place =
        parse_hibachi_batch_place_orders_ack(&hibachi_exchange_id(), &fixture("batch_place_ack"))
            .expect("batch place ack");
    assert_eq!(batch_place.orders.len(), 1);
    assert_eq!(
        batch_place.orders[0].client_order_id.as_deref(),
        Some("hibachi-batch-place-1")
    );
    assert_eq!(batch_place.orders[0].price.as_deref(), Some("10"));

    let batch_cancel =
        parse_hibachi_batch_cancel_orders_ack(&hibachi_exchange_id(), &fixture("batch_cancel_ack"))
            .expect("batch cancel ack");
    assert_eq!(batch_cancel.orders.len(), 2);
    assert_eq!(batch_cancel.cancelled_count, 2);
    assert!(batch_cancel
        .orders
        .iter()
        .all(|order| order.status == OrderStatus::Cancelled));
}

#[test]
fn request_specs_should_match_public_and_private_builders() {
    request_spec("exchange_info")
        .assert_matches(&ActualHttpRequest::new("GET", "/market/exchange-info"))
        .expect("exchange-info spec");

    let orderbook = ActualHttpRequest::new("GET", "/market/data/orderbook").with_query([
        ("symbol".to_string(), "BTC/USDT-P".to_string()),
        ("depth".to_string(), "100".to_string()),
        ("granularity".to_string(), "0.01".to_string()),
    ]);
    request_spec("orderbook")
        .assert_matches(&orderbook)
        .expect("orderbook spec");

    let account_info = ActualHttpRequest::new("GET", "/trade/account/info")
        .with_query([("accountId".to_string(), "42".to_string())])
        .with_headers([(
            "Authorization".to_string(),
            "Bearer test-api-key".to_string(),
        )]);
    request_spec("account_info")
        .assert_matches(&account_info)
        .expect("account-info spec");
    request_spec("balances")
        .assert_matches(&account_info)
        .expect("balances source spec");

    let query_order = ActualHttpRequest::new("GET", "/trade/order")
        .with_query(owned_query(hibachi_query_order_query(
            "42",
            "579183763093760000",
        )))
        .with_headers([(
            "Authorization".to_string(),
            "Bearer test-api-key".to_string(),
        )]);
    request_spec("query_order")
        .assert_matches(&query_order)
        .expect("query-order spec");

    let open_orders = ActualHttpRequest::new("GET", "/trade/orders")
        .with_query(owned_query(hibachi_open_orders_query("42", "BTC/USDT-P")))
        .with_headers([(
            "Authorization".to_string(),
            "Bearer test-api-key".to_string(),
        )]);
    request_spec("open_orders")
        .assert_matches(&open_orders)
        .expect("open-orders spec");

    let recent_fills = ActualHttpRequest::new("GET", "/trade/account/trades")
        .with_query(owned_query(hibachi_recent_fills_query(
            "42",
            "BTC/USDT-P",
            Some(100),
        )))
        .with_headers([(
            "Authorization".to_string(),
            "Bearer test-api-key".to_string(),
        )]);
    request_spec("recent_fills")
        .assert_matches(&recent_fills)
        .expect("recent-fills spec");

    let positions_source = fixture("positions_source_boundary");
    assert_eq!(positions_source["sources"][0]["method"], "GET");
    assert_eq!(
        positions_source["sources"][0]["path"],
        "/trade/account/info"
    );

    let place = ActualHttpRequest::new("POST", "/trade/order").with_body(Some(
        hibachi_place_limit_order_body(
            42,
            1_714_701_600_000_000,
            "BTC/USDT-P",
            "ASK",
            "1",
            "10",
            "0.00005",
            "321447ecf3fb215e0bacd04436254a6f28823f86a728de1701d6f7fa66e68ed8",
        ),
    ));
    request_spec("place_order")
        .assert_matches(&place)
        .expect("place-order spec");

    let cancel = ActualHttpRequest::new("DELETE", "/trade/order").with_body(Some(
        hibachi_cancel_order_body(42, 579_183_763_093_760_000, "sig"),
    ));
    request_spec("cancel_order")
        .assert_matches(&cancel)
        .expect("cancel spec");

    let cancel_all = ActualHttpRequest::new("DELETE", "/trade/orders").with_body(Some(
        hibachi_cancel_all_orders_body(42, 1_717_200_000_001, "sig"),
    ));
    request_spec("cancel_all")
        .assert_matches(&cancel_all)
        .expect("cancel-all spec");

    let amend =
        ActualHttpRequest::new("PATCH", "/trade/order").with_body(Some(hibachi_amend_order_body(
            42,
            1_714_701_600_000_001,
            "<redacted-order-id>",
            "BTC/USDT-P",
            "1",
            "11",
            "<redacted-signature>",
        )));
    request_spec("amend_order")
        .assert_matches(&amend)
        .expect("amend spec");

    let batch_place = ActualHttpRequest::new("POST", "/trade/orders").with_body(Some(
        hibachi_batch_place_orders_body(
            42,
            1_714_701_600_000_002,
            "BTC/USDT-P",
            "BID",
            "1",
            "10",
            "<redacted-signature>",
        ),
    ));
    request_spec("batch_place_orders")
        .assert_matches(&batch_place)
        .expect("batch place spec");

    let batch_cancel = ActualHttpRequest::new("DELETE", "/trade/orders").with_body(Some(
        hibachi_batch_cancel_orders_body(
            42,
            1_714_701_600_000_003,
            &["<redacted-order-id-1>", "<redacted-order-id-2>"],
            "<redacted-signature>",
        ),
    ));
    request_spec("batch_cancel_orders")
        .assert_matches(&batch_cancel)
        .expect("batch cancel spec");
}

#[tokio::test]
async fn positions_runtime_should_require_private_read_guard() {
    let adapter = HibachiGatewayAdapter::new(HibachiGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("positions"),
            exchange: hibachi_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![hibachi_symbol().exchange_symbol],
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: PRIVATE_REST_DISABLED
        }
    ));
}

#[tokio::test]
async fn balances_runtime_should_require_private_read_guard() {
    let adapter = HibachiGatewayAdapter::new(HibachiGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("balances"),
            exchange: hibachi_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: PRIVATE_REST_DISABLED
        }
    ));
}

#[tokio::test]
async fn core_readback_runtime_should_require_private_read_guard() {
    let adapter = HibachiGatewayAdapter::new(HibachiGatewayConfig::default()).expect("adapter");

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("query"),
            symbol: hibachi_symbol(),
            client_order_id: None,
            exchange_order_id: Some("579183763093760000".to_string()),
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "hibachi.query_order"
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("open"),
            exchange: hibachi_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(hibachi_symbol()),
            page: None,
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: "hibachi.get_open_orders"
        }
    ));

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("fills"),
            exchange: hibachi_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(hibachi_symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: "hibachi.get_recent_fills"
        }
    ));
}

#[test]
fn signing_vector_should_match_official_byte_packing_shape() {
    let vector = fixture("signing");
    let payload = hibachi_create_order_payload_hex(
        1_714_701_600_000_000,
        2,
        10_000_000_000,
        HibachiSigningSide::Ask,
        Some(42_949_672_960),
        5000,
    );
    assert_eq!(Some(payload.as_str()), vector["payload_hex"].as_str());
    assert_eq!(
        hibachi_hmac_sha256_signature_hex("test-secret", &payload).expect("signature"),
        vector["expected_signature"].as_str().expect("signature")
    );
    assert_eq!(
        hibachi_cancel_order_payload_hex(579_230_295_489_708_032),
        vector["cancel_payload_hex"]
            .as_str()
            .expect("cancel payload")
    );
}

#[tokio::test]
async fn write_methods_should_return_request_spec_boundary() {
    let adapter = HibachiGatewayAdapter::new(HibachiGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: hibachi_context("place"),
        symbol: hibachi_symbol(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.1".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(order).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: PLACE_ORDER_UNSUPPORTED
        }
    ));

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("amend"),
            symbol: hibachi_symbol(),
            client_order_id: None,
            exchange_order_id: Some("579183763093760001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.1".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: AMEND_ORDER_UNSUPPORTED
        }
    ));

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("batch-place"),
            exchange: hibachi_exchange_id(),
            orders: vec![limit_order("batch-place-1")],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: BATCH_PLACE_UNSUPPORTED
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("batch-cancel"),
            exchange: hibachi_exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: hibachi_context("cancel"),
                symbol: hibachi_symbol(),
                client_order_id: None,
                exchange_order_id: Some("579183763093760001".to_string()),
            }],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: BATCH_CANCEL_UNSUPPORTED
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("oco"),
            symbol: hibachi_symbol(),
            list_client_order_id: Some("hibachi-oco".to_string()),
            side: OrderSide::Buy,
            quantity: "0.1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::TakeProfitLimit,
                price: Some("66000".to_string()),
                stop_price: Some("65900".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("64000".to_string()),
                stop_price: Some("64100".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
        })
        .await
        .expect_err("order list unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: ORDER_LIST_UNSUPPORTED
        }
    ));

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: hibachi_context("cancel-all"),
            exchange: hibachi_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(hibachi_symbol()),
        })
        .await
        .expect_err("cancel all unsupported");
    assert!(matches!(
        cancel_all_error,
        ExchangeApiError::Unsupported { .. }
    ));
}

fn limit_order(request_id: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: hibachi_context(request_id),
        symbol: hibachi_symbol(),
        client_order_id: Some(request_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.1".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn owned_query(query: Vec<(&'static str, String)>) -> Vec<(String, String)> {
    query
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

#[test]
fn websocket_helpers_should_build_payload_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: hibachi_context("public-ws"),
        symbol: hibachi_symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        hibachi_public_subscribe_payload(&subscription),
        fixture("ws_subscribe_orderbook")
    );
    assert_eq!(
        hibachi_public_unsubscribe_payload(&subscription),
        fixture("ws_unsubscribe_orderbook")
    );
    assert_eq!(hibachi_keepalive_payload(), json!({ "method": "ping" }));
    assert_eq!(
        hibachi_private_account_ws_url("wss://api.hibachi.xyz/ws/account", "42"),
        "wss://api.hibachi.xyz/ws/account?accountId=42"
    );
    assert_eq!(
        hibachi_stream_reconnect_policy_ms(),
        (30_000, 45_000, 90_000)
    );
}

#[tokio::test]
async fn public_rest_disabled_should_return_clear_boundary() {
    let adapter = HibachiGatewayAdapter::new(HibachiGatewayConfig {
        enabled_public_rest: false,
        ..HibachiGatewayConfig::default()
    })
    .expect("adapter");
    let request = SymbolRulesRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: hibachi_context("rules"),
        symbols: vec![hibachi_symbol()],
    };

    let error = adapter
        .get_symbol_rules(request)
        .await
        .expect_err("public disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "hibachi.public_rest_disabled"
        }
    ));
}
