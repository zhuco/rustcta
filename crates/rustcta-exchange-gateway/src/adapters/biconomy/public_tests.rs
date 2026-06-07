use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BiconomyGatewayAdapter, BiconomyGatewayConfig};

#[tokio::test]
async fn biconomy_adapter_should_load_spot_symbol_rules() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/biconomy/exchange_info.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        rest_base_url: base_url,
        ..BiconomyGatewayConfig::default()
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
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5"));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v1/exchangeInfo");
}

#[tokio::test]
async fn biconomy_adapter_should_load_order_book_snapshot() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/biconomy/orderbook.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        rest_base_url: base_url,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(200),
        })
        .await
        .expect("book");
    assert_eq!(response.order_book.sequence, Some(42));
    assert_eq!(response.order_book.bids[0].price, 65000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}

#[test]
fn biconomy_parser_should_cover_empty_error_and_missing_fixtures() {
    let exchange = exchange_id();
    let empty = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/biconomy/empty_response.json"
    ))
    .expect("empty fixture");
    let error = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/biconomy/error_response.json"
    ))
    .expect("error fixture");
    let missing = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/biconomy/missing_required_fields.json"
    ))
    .expect("missing fixture");

    assert!(parser::parse_symbol_rules(&exchange, &empty)
        .expect("empty rules")
        .is_empty());
    assert!(parser::parse_order_book(&exchange, symbol_scope(), &error).is_err());
    assert!(parser::parse_symbol_rules(&exchange, &missing).is_err());
}
