use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{KuCoinGatewayAdapter, KuCoinGatewayConfig};

#[tokio::test]
async fn kucoin_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200000",
        "data": [{
            "symbol": "BTC-USDT",
            "baseCurrency": "BTC",
            "quoteCurrency": "USDT",
            "priceIncrement": "0.1",
            "baseIncrement": "0.00000001",
            "baseMinSize": "0.00001",
            "baseMaxSize": "10000000000",
            "quoteMinSize": "0.1",
            "quoteMaxSize": "99999999",
            "enableTrading": true
        }]
    })])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinGatewayConfig::default()
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
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("0.1"));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v2/symbols");
}

#[tokio::test]
async fn kucoin_adapter_should_load_level2_20_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200000",
        "data": {
            "sequence": "100",
            "time": 1710000000000_i64,
            "bids": [["99.5", "1.25"], ["99.0", "2"]],
            "asks": [["100.5", "1.5"], ["101.0", "2"]]
        }
    })])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinGatewayConfig::default()
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
    assert_eq!(request.path, "/api/v1/market/orderbook/level2_20");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
}

#[tokio::test]
async fn kucoin_adapter_should_load_level2_100_order_book_for_deep_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200000",
        "data": {
            "sequence": 101,
            "bids": [["99.5", "1.25"]],
            "asks": [["100.5", "1.5"]]
        }
    })])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book-100"),
            symbol: symbol_scope(),
            depth: Some(80),
        })
        .await
        .expect("book");

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/market/orderbook/level2_100");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
}
