use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser;
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{BitunixGatewayAdapter, BitunixGatewayConfig};

#[tokio::test]
async fn bitunix_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitunix/spot_symbols.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = BitunixGatewayAdapter::new(BitunixGatewayConfig {
        spot_rest_base_url: base_url,
        ..BitunixGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/api/spot/v1/common/coin_pair/list"
    );
}

#[tokio::test]
async fn bitunix_adapter_should_load_perp_order_book_from_public_rest() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitunix/orderbook.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = BitunixGatewayAdapter::new(BitunixGatewayConfig {
        futures_rest_base_url: base_url,
        ..BitunixGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(55),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/futures/market/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("max"));
}

#[test]
fn bitunix_parser_should_cover_empty_error_and_missing_fixtures() {
    let exchange = super::test_support::exchange_id();
    let empty = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitunix/empty_response.json"
    ))
    .expect("empty fixture");
    let error = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitunix/error_response.json"
    ))
    .expect("error fixture");
    let missing = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitunix/missing_required_fields.json"
    ))
    .expect("missing fixture");

    assert!(
        parser::parse_symbol_rules(&exchange, MarketType::Spot, &empty)
            .expect("empty rules")
            .is_empty()
    );
    assert!(parser::parse_orderbook_snapshot(&exchange, spot_symbol_scope(), &error).is_err());
    assert!(parser::parse_symbol_rules(&exchange, MarketType::Spot, &missing).is_err());
}
