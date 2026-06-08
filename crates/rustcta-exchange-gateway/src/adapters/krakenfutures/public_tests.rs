use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{KrakenFuturesGatewayAdapter, KrakenFuturesGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[tokio::test]
async fn krakenfutures_adapter_should_reject_spot_public_requests() {
    let adapter = KrakenFuturesGatewayAdapter::default_public().expect("adapter");

    let rules_err = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .unwrap_err();
    assert!(matches!(rules_err, ExchangeApiError::Unsupported { .. }));

    let book_err = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-book"),
            symbol: spot_symbol_scope(),
            depth: Some(10),
        })
        .await
        .unwrap_err();
    assert!(matches!(book_err, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn krakenfutures_adapter_should_load_futures_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "instruments": [{
            "symbol": "PF_XBTUSDT",
            "type": "perpetual",
            "tickSize": "0.1",
            "contractSize": "1",
            "minTradeSize": "0.001"
        }]
    })])
    .await;
    let adapter = KrakenFuturesGatewayAdapter::new(KrakenFuturesGatewayConfig {
        futures_rest_base_url: base_url,
        ..KrakenFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(
        response.rules[0]
            .symbol
            .canonical_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "BTC/USDT"
    );
    assert_eq!(response.rules[0].quantity_increment.as_deref(), Some("1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/instruments");
}

#[tokio::test]
async fn krakenfutures_adapter_should_load_futures_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "orderBook": {
            "symbol": "PF_XBTUSDT",
            "bids": [["37500.0", "2"]],
            "asks": [["37501.0", "3"]],
            "sequence": 42
        }
    })])
    .await;
    let adapter = KrakenFuturesGatewayAdapter::new(KrakenFuturesGatewayConfig {
        futures_rest_base_url: base_url,
        ..KrakenFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-book"),
            symbol: perp_symbol_scope(),
            depth: Some(10),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(42));
    assert_eq!(response.order_book.best_bid().unwrap().price, 37500.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("PF_XBTUSDT")
    );
    assert_eq!(request.query.get("depth").map(String::as_str), Some("10"));
}

#[test]
fn krakenfutures_adapter_should_register_by_name() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("test", ["krakenfutures"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}
