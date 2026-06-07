use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeErrorClass;

use super::parser::{parse_order_book, parse_symbol_rules};
use super::test_support::{context, exchange_id, load_fixture, spawn_rest_server, symbol_scope};
use super::{CointrGatewayAdapter, CointrGatewayConfig};

#[tokio::test]
async fn cointr_adapter_should_load_spot_symbol_rules() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_fixture("parser/spot_symbols_success.json")]).await;
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        rest_base_url: base_url,
        ..CointrGatewayConfig::default()
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
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v2/spot/public/symbols");
}

#[tokio::test]
async fn cointr_adapter_should_load_order_book_snapshot() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_fixture("parser/orderbook_success.json")]).await;
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        rest_base_url: base_url,
        ..CointrGatewayConfig::default()
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
    assert_eq!(request.path, "/api/v2/spot/market/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}

#[test]
fn cointr_public_parser_fixtures_should_cover_success_empty_error_and_missing_fields() {
    let rules = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Spot,
        &load_fixture("parser/spot_symbols_success.json")["data"],
    )
    .expect("symbol rules");
    assert_eq!(rules.len(), 1);

    let empty = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Spot,
        &load_fixture("parser/empty_response.json")["data"],
    )
    .expect("empty response");
    assert!(empty.is_empty());

    let missing = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Spot,
        &load_fixture("parser/missing_symbol_field.json")["data"],
    )
    .expect_err("missing symbol field");
    assert!(matches!(
        missing,
        rustcta_exchange_api::ExchangeApiError::Exchange(error)
            if error.class == ExchangeErrorClass::Decode
    ));

    let error = parse_order_book(
        &exchange_id(),
        symbol_scope(),
        &load_fixture("parser/error_response.json"),
    )
    .expect_err("error-shaped response is not a valid order book");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Exchange(error)
            if error.class == ExchangeErrorClass::Decode
    ));
}
