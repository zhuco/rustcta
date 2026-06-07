use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{CoinExGatewayAdapter, CoinExGatewayConfig};

#[tokio::test]
async fn coinex_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "market": "BTCUSDT",
            "base_ccy": "BTC",
            "quote_ccy": "USDT",
            "price_precision": 2,
            "amount_precision": 6,
            "min_amount": "0.0001",
            "min_notional": "1",
            "max_amount": "100",
            "is_trading_available": true
        }]
    })])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        ..CoinExGatewayConfig::default()
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
    assert_eq!(response.rules[0].min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("1"));
    assert_eq!(seen.lock().unwrap()[0].path, "/spot/market".to_string());
}

#[tokio::test]
async fn coinex_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "last": 100,
            "updated_at": 1743054548123_i64,
            "depth": {
                "bids": [["99.5", "1.25"], ["99.0", "2"]],
                "asks": [["100.5", "1.5"], ["101.0", "2"]]
            }
        }
    })])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        ..CoinExGatewayConfig::default()
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
    assert_eq!(request.path, "/spot/depth");
    assert_eq!(
        request.query.get("market").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
    assert_eq!(request.query.get("interval").map(String::as_str), Some("0"));
}
