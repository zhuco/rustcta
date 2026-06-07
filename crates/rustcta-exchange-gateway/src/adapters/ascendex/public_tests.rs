use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType};

use super::test_support::{
    context, fixture_json, perp_symbol_scope, spawn_rest_server, spot_symbol_scope,
};
use super::{AscendexGatewayAdapter, AscendexGatewayConfig};

#[tokio::test]
async fn ascendex_adapter_should_load_spot_symbol_rules() {
    let (base_url, seen) = spawn_rest_server(vec![fixture_json("spot_products.json")]).await;
    let adapter = AscendexGatewayAdapter::new(AscendexGatewayConfig {
        rest_base_url: base_url,
        ..AscendexGatewayConfig::default()
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
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(response.rules[0].quantity_precision, Some(6));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/pro/v1/cash/products");
}

#[tokio::test]
async fn ascendex_adapter_should_load_futures_symbol_rules() {
    let (base_url, seen) = spawn_rest_server(vec![fixture_json("futures_contracts.json")]).await;
    let adapter = AscendexGatewayAdapter::new(AscendexGatewayConfig {
        rest_base_url: base_url,
        ..AscendexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/pro/v2/futures/contract");
}

#[tokio::test]
async fn ascendex_adapter_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![fixture_json("orderbook.json")]).await;
    let adapter = AscendexGatewayAdapter::new(AscendexGatewayConfig {
        rest_base_url: base_url,
        ..AscendexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(20),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(123));
    assert_eq!(response.order_book.bids[0].quantity, 2.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/pro/v1/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC/USDT")
    );
}

#[tokio::test]
async fn ascendex_adapter_should_classify_exchange_error_responses() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture_json("error.json")]).await;
    let adapter = AscendexGatewayAdapter::new(AscendexGatewayConfig {
        rest_base_url: base_url,
        ..AscendexGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("error"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect_err("exchange error");

    let ExchangeApiError::Exchange(error) = error else {
        panic!("expected classified exchange error");
    };
    assert_eq!(error.class, ExchangeErrorClass::InsufficientBalance);
    assert_eq!(error.code.as_deref(), Some("300011"));
}

#[test]
fn ascendex_parser_fixtures_should_cover_empty_and_missing_field_errors() {
    let empty = super::parser::parse_spot_symbol_rules(
        &super::test_support::exchange_id(),
        &fixture_json("empty_response.json"),
    )
    .expect("empty response should parse");
    assert!(empty.is_empty());

    let error = super::parser::parse_spot_symbol_rules(
        &super::test_support::exchange_id(),
        &fixture_json("missing_symbol.json"),
    )
    .expect_err("missing symbol should fail parsing");
    let ExchangeApiError::Exchange(error) = error else {
        panic!("expected decode exchange error");
    };
    assert_eq!(error.class, ExchangeErrorClass::Decode);
}
