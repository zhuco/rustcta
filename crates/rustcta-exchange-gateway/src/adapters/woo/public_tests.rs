use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::Value;

use super::parser::parse_symbol_rules;
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, symbol_scope};
use super::{WooGatewayAdapter, WooGatewayConfig};

#[tokio::test]
async fn woo_adapter_should_load_spot_and_perp_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("instruments")]).await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope(), perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 2);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Spot);
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(response.rules[1].symbol.market_type, MarketType::Perpetual);
    assert!(response.rules[1].supports_reduce_only);
    assert_eq!(seen.lock().unwrap()[0].path, "/v3/public/instruments");
}

#[tokio::test]
async fn woo_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook")]).await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(50),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[0].quantity, 1.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v3/public/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("SPOT_BTC_USDT")
    );
    assert_eq!(
        request.query.get("maxLevel").map(String::as_str),
        Some("50")
    );
}

#[test]
fn woo_parser_fixtures_should_cover_empty_and_missing_symbol_rules() {
    let exchange = super::test_support::exchange_id();
    let empty = parse_symbol_rules(&exchange, &fixture("instruments_empty"))
        .expect("empty WOO instruments fixture");
    assert!(empty.is_empty());

    let error = parse_symbol_rules(&exchange, &fixture("instruments_missing_symbol"))
        .expect_err("missing WOO symbol should be rejected");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "instruments" => {
            include_str!("../../../../../tests/fixtures/exchanges/woo/instruments.json")
        }
        "instruments_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/woo/instruments_empty.json")
        }
        "instruments_missing_symbol" => include_str!(
            "../../../../../tests/fixtures/exchanges/woo/instruments_missing_symbol.json"
        ),
        "orderbook" => include_str!("../../../../../tests/fixtures/exchanges/woo/orderbook.json"),
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
