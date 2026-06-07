use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{OkxGatewayAdapter, OkxGatewayConfig};

#[tokio::test]
async fn okx_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "baseCcy": "BTC",
            "quoteCcy": "USDT",
            "tickSz": "0.01",
            "lotSz": "0.00000001",
            "minSz": "0.00001",
            "minLmtAmt": "5",
            "maxLmtSz": "100",
            "state": "live"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
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
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v5/public/instruments");
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("SPOT")
    );
}

#[tokio::test]
async fn okx_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "bids": [["99.5", "1.25", "0", "2"], ["99.0", "2", "0", "1"]],
            "asks": [["100.5", "1.5", "0", "3"], ["101.0", "2", "0", "1"]],
            "ts": "1710000000123"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
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

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[1].quantity, 2.0);
    assert!(response.order_book.exchange_timestamp.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v5/market/books");
    assert_eq!(
        request.query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(request.query.get("sz").map(String::as_str), Some("10"));
}
