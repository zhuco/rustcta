use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{CoinspotGatewayAdapter, CoinspotGatewayConfig};

#[tokio::test]
async fn coinspot_adapter_should_load_aud_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "status": "ok",
        "markets": [{
            "coin": "BTC",
            "market": "AUD",
            "price_tick": "0.01",
            "amount_tick": "0.00000001",
            "minimum_amount": "0.0001",
            "minimum_total": "1",
            "status": "online"
        }]
    })])
    .await;
    let adapter = CoinspotGatewayAdapter::new(CoinspotGatewayConfig {
        public_rest_base_url: base_url,
        ..CoinspotGatewayConfig::default()
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
    assert_eq!(response.rules[0].quote_asset, "AUD");
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("1"));
    assert_eq!(seen.lock().unwrap()[0].path, "/pubapi/v2/markets");
}

#[tokio::test]
async fn coinspot_adapter_should_load_order_book_from_public_open_orders() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "status": "ok",
        "buyorders": [{"rate": "99000.5", "amount": "0.25"}],
        "sellorders": [{"rate": "100000.5", "amount": "0.3"}]
    })])
    .await;
    let adapter = CoinspotGatewayAdapter::new(CoinspotGatewayConfig {
        public_rest_base_url: base_url,
        ..CoinspotGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(5),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99000.5);
    assert_eq!(response.order_book.asks[0].quantity, 0.3);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/pubapi/v2/orders/open");
    assert_eq!(
        request.query.get("cointype").map(String::as_str),
        Some("btc")
    );
    assert_eq!(
        request.query.get("markettype").map(String::as_str),
        Some("aud")
    );
}
