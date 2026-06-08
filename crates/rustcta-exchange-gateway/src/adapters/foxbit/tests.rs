use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, TimeInForce};
use serde_json::{json, Value};

use super::parser::{
    foxbit_canonical_pair, foxbit_symbol, parse_order_ack_id, parse_order_book_snapshot,
    parse_symbol_rules,
};
use super::private::{
    create_order_request_spec_fixture, foxbit_order_path, foxbit_place_order_body,
};
use super::private_parser::{parse_balance_assets, parse_fill_ids, parse_open_order_ids};
use super::signing::{
    foxbit_hmac_sha256_hex, foxbit_private_ws_login_prehash, foxbit_rest_prehash,
    foxbit_signed_headers,
};
use super::streams::{
    foxbit_private_login_payload, foxbit_public_ping_payload, foxbit_public_subscribe_payload,
    foxbit_public_unsubscribe_payload, foxbit_reconnect_policy_ms,
    parse_orderbook_snapshot_payload,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{FoxbitGatewayAdapter, FoxbitGatewayConfig};

fn foxbit_fixture(name: &str) -> Value {
    let text = match name {
        "markets_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/markets_success.json")
        }
        "orderbook_btc_brl.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/orderbook_btc_brl.json")
        }
        "order_ack.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/order_ack.json")
        }
        "balances.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/balances.json")
        }
        "open_orders.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/open_orders.json")
        }
        "fills.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/fills.json")
        }
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/foxbit/unsupported_boundary.json")
        }
        "ws/orderbook_snapshot.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/ws/orderbook_snapshot.json"
        ),
        _ => panic!("unknown foxbit fixture {name}"),
    };
    serde_json::from_str(text).expect("foxbit fixture")
}

fn foxbit_request_spec_fixture(name: &str) -> Value {
    let text = match name {
        "balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/balances.json"
        ),
        "cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/cancel_order.json"
        ),
        "create_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/create_order.json"
        ),
        "markets.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/markets.json"
        ),
        "open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/open_orders.json"
        ),
        "order_book.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/order_book.json"
        ),
        "query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/query_order.json"
        ),
        "recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/foxbit/request_specs/recent_fills.json"
        ),
        _ => panic!("unknown foxbit request spec fixture {name}"),
    };
    serde_json::from_str(text).expect("foxbit request spec fixture")
}

#[test]
fn foxbit_symbol_normalization_should_cover_brl_markets() {
    assert_eq!(foxbit_symbol("BTC/BRL"), "btcbrl");
    assert_eq!(foxbit_symbol("eth-brl"), "ethbrl");
    assert_eq!(
        foxbit_canonical_pair("btcbrl").expect("pair"),
        ("BTC".to_string(), "BRL".to_string())
    );
    assert_eq!(
        foxbit_canonical_pair("BTC_USDT").expect("pair"),
        ("BTC".to_string(), "USDT".to_string())
    );
}

#[tokio::test]
async fn foxbit_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![foxbit_fixture("markets_success.json")]).await;
    let adapter = FoxbitGatewayAdapter::new(FoxbitGatewayConfig {
        rest_base_url: base_url,
        ..FoxbitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("BTC/BRL")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "BRL");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00000001")
    );
    assert!(response.rules[0].supports_market_orders);
    assert!(response.rules[0].supports_limit_orders);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/markets");
}

#[tokio::test]
async fn foxbit_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![foxbit_fixture("orderbook_btc_brl.json")]).await;
    let adapter = FoxbitGatewayAdapter::new(FoxbitGatewayConfig {
        rest_base_url: base_url,
        ..FoxbitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC/BRL"),
            depth: Some(1),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.sequence, Some(123456789));
    assert_eq!(response.order_book.bids[0].price, 350000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/markets/btcbrl/orderbook");
    assert_eq!(request.query.get("depth").map(String::as_str), Some("1"));
}

#[test]
fn foxbit_public_parser_fixtures_should_cover_markets_and_orderbook() {
    let rules = parse_symbol_rules(&exchange_id(), &[], &foxbit_fixture("markets_success.json"))
        .expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btcbrl");
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.00002"));

    let book = parse_order_book_snapshot(
        &exchange_id(),
        symbol_scope("BTC/BRL"),
        2,
        &foxbit_fixture("orderbook_btc_brl.json"),
    )
    .expect("book");
    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 2);
    assert_eq!(
        book.exchange_timestamp
            .expect("timestamp")
            .timestamp_millis(),
        1700000000000
    );
}

#[test]
fn foxbit_private_request_specs_and_parsers_should_stay_secret_free() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/orders");
    assert_eq!(spec["headers"]["X-FB-ACCESS-KEY"], "<redacted>");
    assert_eq!(spec["body"]["market_symbol"], "btcbrl");
    assert_eq!(foxbit_order_path("order-1"), "/orders/order-1");

    let order_ack = foxbit_fixture("order_ack.json");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("id"),
        "order-foxbit-1"
    );
    assert_eq!(
        parse_balance_assets(&foxbit_fixture("balances.json")).expect("balances"),
        vec!["BTC", "BRL"]
    );
    assert_eq!(
        parse_open_order_ids(&foxbit_fixture("open_orders.json")).expect("orders"),
        vec!["order-foxbit-1"]
    );
    assert_eq!(
        parse_fill_ids(&foxbit_fixture("fills.json")).expect("fills"),
        vec!["trade-foxbit-1"]
    );
}

#[test]
fn foxbit_required_request_spec_fixtures_should_match_endpoint_mapping() {
    let expected = [
        ("markets.json", "GET", "/markets", "none"),
        (
            "order_book.json",
            "GET",
            "/markets/btcbrl/orderbook",
            "none",
        ),
        ("balances.json", "GET", "/balances", "hmac_sha256"),
        ("open_orders.json", "GET", "/orders", "hmac_sha256"),
        (
            "query_order.json",
            "GET",
            "/orders/order-foxbit-1",
            "hmac_sha256",
        ),
        ("recent_fills.json", "GET", "/trades", "hmac_sha256"),
        ("create_order.json", "POST", "/orders", "hmac_sha256"),
        (
            "cancel_order.json",
            "DELETE",
            "/orders/order-foxbit-1",
            "hmac_sha256",
        ),
    ];

    for (name, method, path, auth) in expected {
        let spec = foxbit_request_spec_fixture(name);
        assert_eq!(spec["method"], method, "{name}");
        assert_eq!(spec["path"], path, "{name}");
        assert_eq!(spec["auth"], auth, "{name}");
        let rendered = spec.to_string();
        assert!(!rendered.contains("fixture-secret"), "{name}");
        assert!(!rendered.contains("foxbit-secret"), "{name}");
        if auth == "hmac_sha256" {
            assert_eq!(spec["headers"]["X-FB-ACCESS-KEY"], "<redacted>", "{name}");
            assert_eq!(
                spec["headers"]["X-FB-ACCESS-SIGNATURE"], "<computed>",
                "{name}"
            );
        }
    }
}

#[test]
fn foxbit_place_order_body_should_map_limit_and_market() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope("BTC/BRL"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("350000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = foxbit_place_order_body(&request).expect("body");
    assert_eq!(body["side"], "BUY");
    assert_eq!(body["type"], "LIMIT");
    assert_eq!(body["market_symbol"], "btcbrl");
    assert_eq!(body["quantity"], "0.01");
    assert_eq!(body["price"], "350000");
    assert!(body.get("post_only").is_none());

    let mut market = request;
    market.order_type = OrderType::Market;
    market.price = None;
    market.quote_quantity = Some("1000".to_string());
    let body = foxbit_place_order_body(&market).expect("market");
    assert_eq!(body["amount"], "1000");
    assert!(body.get("price").is_none());
}

#[test]
fn foxbit_signing_vector_should_match_hmac_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/foxbit/signing_vectors/rest_hmac_sha256.json"
    ))
    .expect("signing fixture");
    let raw_body = r#"{"market_symbol":"btcbrl","price":"350000","quantity":"0.01","side":"BUY","type":"LIMIT"}"#;
    let prehash = foxbit_rest_prehash("1700000000000", "POST", "/orders", "", raw_body);
    let signature = foxbit_hmac_sha256_hex("fixture-secret", &prehash);
    assert_eq!(prehash, fixture["payload"]);
    assert_eq!(signature, fixture["expected_signature"]);
    assert_eq!(
        foxbit_private_ws_login_prehash("1700000000000"),
        "1700000000000login"
    );

    let headers = foxbit_signed_headers(
        "fixture-key",
        "fixture-secret",
        "1700000000000",
        "POST",
        "/orders",
        "",
        raw_body,
        Some(15_000),
    );
    assert_eq!(
        headers
            .iter()
            .find(|(key, _)| key == "X-FB-ACCESS-KEY")
            .map(|(_, value)| value.as_str()),
        Some("fixture-key")
    );
    assert!(!headers
        .iter()
        .any(|(_, value)| value.contains("fixture-secret")));
}

#[test]
fn foxbit_websocket_helpers_should_build_public_private_specs() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope("BTC/BRL"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = foxbit_public_subscribe_payload(&subscription);
    assert_eq!(payload["type"], "subscribe");
    assert_eq!(payload["params"][0]["channel"], "orderbook-1000");
    assert_eq!(payload["params"][0]["market_symbol"], "btcbrl");
    assert_eq!(
        foxbit_public_unsubscribe_payload(&subscription)["type"],
        "unsubscribe"
    );
    assert_eq!(foxbit_public_ping_payload()["params"][0]["channel"], "ping");
    assert_eq!(foxbit_reconnect_policy_ms(), (20_000, 45_000, 60_000));

    let login = foxbit_private_login_payload("fixture-key", "fixture-secret", "1700000000000");
    assert_eq!(login["type"], "login");
    assert_eq!(login["params"]["api_key"], "fixture-key");
    assert_eq!(
        login["params"]["signature"],
        "472015d94e7f4fc787ff74ad30bf4498a577db8f7dee13ee89d0df3480b23353"
    );

    let ws = foxbit_fixture("ws/orderbook_snapshot.json");
    assert_eq!(
        parse_orderbook_snapshot_payload(&ws).expect("ws"),
        (123456789, 1, 1)
    );
}

#[tokio::test]
async fn foxbit_live_private_surfaces_should_remain_explicitly_unsupported() {
    let adapter = FoxbitGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!FoxbitGatewayConfig::default().private_rest_available());

    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope("BTC/BRL"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("350000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "foxbit.place_order_offline_request_spec_only"
        }
    ));

    let boundary = foxbit_fixture("unsupported_boundary.json");
    assert_eq!(boundary["otc_invest_api"], "unsupported");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
    assert_eq!(json!(true), boundary["rest_reconciliation_fallback"]);
}
