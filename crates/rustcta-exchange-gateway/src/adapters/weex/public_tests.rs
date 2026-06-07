use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{WeexGatewayAdapter, WeexGatewayConfig};

#[tokio::test]
async fn weex_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("spot_exchange_info")]).await;
    let adapter = WeexGatewayAdapter::new(WeexGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        ..WeexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v3/exchangeInfo");
}

#[tokio::test]
async fn weex_adapter_should_load_perp_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook")]).await;
    let adapter = WeexGatewayAdapter::new(WeexGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        ..WeexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(55),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 65000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/capi/v3/market/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("200"));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "spot_exchange_info" => {
            include_str!("../../../../../tests/fixtures/exchanges/weex/spot_exchange_info.json")
        }
        "orderbook" => include_str!("../../../../../tests/fixtures/exchanges/weex/orderbook.json"),
        other => panic!("unknown WEEX fixture {other}"),
    };
    serde_json::from_str(text).expect("WEEX fixture JSON")
}
