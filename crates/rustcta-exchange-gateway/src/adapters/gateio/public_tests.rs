use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{GateIoGatewayAdapter, GateIoGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/gateio/toolchain/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

#[test]
fn gateio_parser_fixtures_should_cover_success_empty_and_error_shapes() {
    let exchange = super::test_support::exchange_id();
    let rules =
        parse_symbol_rules(&exchange, &fixture("symbol_rules_success.json")).expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].quote_asset, "USDT");

    let empty = parse_symbol_rules(&exchange, &fixture("symbol_rules_empty.json")).expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(&exchange, &fixture("symbol_rules_missing_field.json")).is_err());
    assert!(parse_orderbook_snapshot(
        &exchange,
        symbol_scope("BTC_USDT"),
        &fixture("orderbook_error.json")
    )
    .is_err());
}

#[tokio::test]
async fn gateio_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        rest_base_url: base_url,
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("BTCUSDT")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00001")
    );
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/spot/currency_pairs".to_string()
    );
}

#[tokio::test]
async fn gateio_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "id": 100,
        "current": "1710000000",
        "bids": [["99.5", "1.25"], {"p": "99.0", "s": "2"}],
        "asks": [["100.5", "1.5"], {"price": "101.0", "quantity": "2"}]
    })])
    .await;
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        rest_base_url: base_url,
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC-USDT"),
            depth: Some(21),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(100));
    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[1].quantity, 2.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/spot/order_book");
    assert_eq!(
        request.query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("50"));
}
