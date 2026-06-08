use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::test_support::{btc_nis_symbol_scope, context, spawn_rest_server};
use super::{Bit2cGatewayAdapter, Bit2cGatewayConfig};

#[tokio::test]
async fn bit2c_adapter_should_return_static_nis_symbol_rules() {
    let adapter =
        Bit2cGatewayAdapter::new(Bit2cGatewayConfig::default()).expect("adapter should construct");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: Vec::new(),
        })
        .await
        .expect("symbol rules");

    assert_eq!(response.rules.len(), 4);
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BtcNis");
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "NIS");
    assert!(response.rules[0].supports_limit_orders);
    assert!(!response.rules[0].supports_market_orders);
}

#[tokio::test]
async fn bit2c_adapter_should_load_order_book_from_public_rest() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bit2c/orderbook_btc_nis.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = Bit2cGatewayAdapter::new(Bit2cGatewayConfig {
        rest_base_url: base_url,
        ..Bit2cGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: btc_nis_symbol_scope(),
            depth: Some(1),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.bids[0].price, 371000.0);
    assert_eq!(response.order_book.asks[0].quantity, 0.18);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/Exchanges/BtcNis/orderbook.json");
    assert!(request.query.is_empty());
    assert!(request.headers.contains_key("host"));
    assert!(request.body.is_none());
}
