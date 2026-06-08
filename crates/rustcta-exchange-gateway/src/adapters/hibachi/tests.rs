use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::json;

use super::parser::{
    parse_hibachi_orderbook_snapshot, parse_hibachi_orders, parse_hibachi_symbol_rules,
};
use super::private::{
    hibachi_cancel_all_orders_body, hibachi_cancel_order_body, hibachi_place_limit_order_body,
    PLACE_ORDER_UNSUPPORTED,
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
        "place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/place_order_limit.json"
        ),
        "cancel_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/cancel_order.json"
        ),
        "cancel_all" => include_str!(
            "../../../../../tests/fixtures/exchanges/hibachi/request_specs/cancel_all_orders.json"
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
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write"], "request_spec_only");
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
