use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{BinanceGatewayAdapter, BinanceGatewayConfig};

#[tokio::test]
async fn binance_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbols": [{
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "baseAssetPrecision": 8,
            "quoteAssetPrecision": 8,
            "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "5.00000000"}
            ]
        }]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        rest_base_url: base_url,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("BTC/USDT")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(
        response.rules[0].price_increment.as_deref(),
        Some("0.01000000")
    );
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00001000")
    );
    assert_eq!(
        response.rules[0].min_notional.as_deref(),
        Some("5.00000000")
    );
    assert_eq!(response.rules[0].price_precision, Some(2));
    assert_eq!(response.rules[0].quantity_precision, Some(5));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v3/exchangeInfo");
}

#[tokio::test]
async fn binance_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 1027024,
        "bids": [["4.00000000", "431.00000000"]],
        "asks": [["4.00000200", "12.00000000"]]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        rest_base_url: base_url,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC-USDT"),
            depth: Some(11),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(1027024));
    assert_eq!(response.order_book.bids[0].price, 4.0);
    assert_eq!(response.order_book.asks[0].quantity, 12.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("20"));
}
