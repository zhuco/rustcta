use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, PublicStreamKind, PublicStreamSubscription,
    SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::signing::{cex_rest_signature, cex_ws_signature};
use super::streams::{
    cex_pong_payload, cex_public_subscribe_payload, cex_public_unsubscribe_payload,
    cex_ws_auth_payload,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{CexGatewayAdapter, CexGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn cex_parser_should_map_currency_limits_and_order_book_fixtures() {
    let currency_limits: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/currency_limits.json"
    ))
    .expect("currency limits fixture");
    let rules = parse_symbol_rules(&exchange_id(), &currency_limits).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC/USD");
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("20"));
    assert!(rules[0].supports_market_orders);

    let orderbook: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/orderbook.json"
    ))
    .expect("orderbook fixture");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), Some(1), &orderbook)
        .expect("snapshot");
    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.asks[0].price, 17689.66);
    assert_eq!(snapshot.sequence, None);
}

#[test]
fn cex_parser_should_reject_missing_pairs() {
    let missing: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/missing_required_fields.json"
    ))
    .expect("missing fixture");
    assert!(parse_symbol_rules(&exchange_id(), &missing).is_err());
}

#[tokio::test]
async fn cex_adapter_should_load_symbol_rules_from_public_rest() {
    let response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/currency_limits.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![response]).await;
    let adapter = CexGatewayAdapter::new(CexGatewayConfig {
        rest_base_url: base_url,
        ..CexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USD");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/currency_limits");
}

#[tokio::test]
async fn cex_adapter_should_load_order_book_from_public_rest() {
    let response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/orderbook.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![response]).await;
    let adapter = CexGatewayAdapter::new(CexGatewayConfig {
        rest_base_url: base_url,
        ..CexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 17670.3);
    assert_eq!(response.order_book.asks[0].quantity, 0.01);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/order_book/BTC/USD/");
    assert_eq!(request.query.get("depth").map(String::as_str), Some("2"));
}

#[test]
fn cex_signing_and_ws_payloads_should_be_offline_verifiable() {
    let ws_signature = cex_ws_signature(
        "1448034533",
        "1WZbtMTbMbo2NsW12vOz9IuPM",
        "1IuUeW4IEWatK87zBTENHj1T17s",
    )
    .expect("ws signature");
    assert_eq!(
        ws_signature,
        "7d581adb01ad22f1ed38e1159a7f08ac5d83906ae1a42fe17e7d977786fe9694"
    );
    let rest_signature =
        cex_rest_signature("1700000000000", "user123", "fixture-key", "fixture-secret")
            .expect("rest signature");
    assert_eq!(rest_signature, rest_signature.to_ascii_uppercase());

    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert_eq!(
        cex_public_subscribe_payload(&subscription, "oid-1").expect("subscribe"),
        json!({
            "e": "order-book-subscribe",
            "data": {
                "pair": ["BTC", "USD"],
                "subscribe": true,
                "depth": 25
            },
            "oid": "oid-1"
        })
    );
    assert_eq!(
        cex_public_unsubscribe_payload(&subscription, "oid-2").expect("unsubscribe"),
        json!({
            "e": "order-book-unsubscribe",
            "data": {"pair": ["BTC", "USD"]},
            "oid": "oid-2"
        })
    );
    assert_eq!(cex_pong_payload(), json!({ "e": "pong" }));
    assert_eq!(
        cex_ws_auth_payload("fixture-key", "fixture-secret", "1700000000")
            .expect("auth")
            .get("e")
            .and_then(Value::as_str),
        Some("auth")
    );
}

#[test]
fn cex_named_registration_should_accept_aliases_and_keep_private_disabled() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("cex-test", ["cex.io"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
    let adapter = CexGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
}
