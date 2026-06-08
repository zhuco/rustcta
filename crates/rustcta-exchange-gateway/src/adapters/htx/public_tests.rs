use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};

use super::test_support::{context, load_fixture, spawn_rest_server, spot_symbol};
use super::{HtxGatewayAdapter, HtxGatewayConfig};

#[tokio::test]
async fn htx_adapter_should_load_spot_symbol_rules() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_fixture("parser/spot_symbols_success.json")]).await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        spot_rest_base_url: base_url,
        ..HtxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(seen.lock().unwrap()[0].path, "/v1/common/symbols");
}

#[tokio::test]
async fn htx_adapter_should_load_order_book_snapshot() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_fixture("parser/orderbook_success.json")]).await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        spot_rest_base_url: base_url,
        ..HtxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol(),
            depth: Some(20),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(42));
    assert_eq!(response.order_book.bids[0].price, 65000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/market/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("btcusdt")
    );
}
