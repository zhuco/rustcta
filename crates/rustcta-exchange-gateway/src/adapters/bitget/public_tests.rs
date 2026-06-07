use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{BitgetGatewayAdapter, BitgetGatewayConfig};

#[tokio::test]
async fn bitget_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "00000",
        "data": [{
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "pricePrecision": "2",
            "quantityPrecision": "6",
            "minTradeAmount": "0.0001",
            "minTradeUSDT": "1",
            "maxTradeAmount": "100",
            "status": "online"
        }]
    })])
    .await;
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
