use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{MexcGatewayAdapter, MexcGatewayConfig};

#[tokio::test]
async fn mexc_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbols": [{
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "9000", "stepSize": "0.00001"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "5"}
            ]
        }]
    })])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        ..MexcGatewayConfig::default()
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
        seen.lock().unwrap()[0].path,
        "/api/v3/exchangeInfo".to_string()
    );
}

#[tokio::test]
async fn mexc_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 100,
        "bids": [["99.5", "1.25"], ["99.0", "2"]],
        "asks": [["100.5", "1.5"], ["101.0", "2"]]
    })])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        ..MexcGatewayConfig::default()
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
    assert_eq!(request.path, "/api/v3/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
}
