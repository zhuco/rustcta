use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perpetual_symbol_scope, spawn_rest_server, symbol_scope};
use super::{BitgetGatewayAdapter, BitgetGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/bitget/toolchain/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

#[test]
fn bitget_parser_fixtures_should_cover_success_empty_and_error_shapes() {
    let exchange = super::test_support::exchange_id();
    let rules = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("symbol_rules_success.json"),
    )
    .expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");

    let empty = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("symbol_rules_empty.json"),
    )
    .expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("symbol_rules_missing_field.json")
    )
    .is_err());
    assert!(
        parse_orderbook_snapshot(&exchange, symbol_scope(), &fixture("orderbook_error.json"))
            .is_err()
    );
}

#[tokio::test]
async fn bitget_adapter_should_load_perpetual_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "00000",
        "requestTime": 1743054548123_i64,
        "data": {
            "bids": [["69999.5", "1.25"]],
            "asks": [["70000.5", "1.5"]],
            "ts": "1743054548123",
            "seq": "200"
        }
    })])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-book"),
            symbol: perpetual_symbol_scope(),
            depth: Some(50),
        })
        .await
        .expect("perpetual book");

    assert_eq!(response.order_book.market_type, MarketType::Perpetual);
    assert_eq!(response.order_book.sequence, Some(200));
    assert_eq!(response.order_book.bids[0].price, 69999.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v2/mix/market/orderbook");
    assert_eq!(
        request.query.get("productType").map(String::as_str),
        Some("USDT-FUTURES")
    );
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("50"));
}

#[tokio::test]
async fn bitget_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        ..BitgetGatewayConfig::default()
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
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("1"));
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/api/v2/spot/public/symbols".to_string()
    );
}

#[tokio::test]
async fn bitget_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "00000",
        "requestTime": 1743054548123_i64,
        "data": {
            "bids": [["99.5", "1.25"], ["99.0", "2"]],
            "asks": [["100.5", "1.5"], ["101.0", "2"]],
            "ts": "1743054548123",
            "seq": "100"
        }
    })])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(100));
    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v2/spot/market/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("type").map(String::as_str), Some("step0"));
    assert_eq!(request.query.get("limit").map(String::as_str), Some("15"));
}
