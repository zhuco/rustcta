use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{CoincheckGatewayAdapter, CoincheckGatewayConfig};

#[tokio::test]
async fn coincheck_public_rest_should_parse_symbol_rules_and_order_book() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "bids": [["10000000", "0.2"], ["9999000", "0.1"]],
        "asks": [["10001000", "0.3"]]
    })])
    .await;
    let adapter = CoincheckGatewayAdapter::new(CoincheckGatewayConfig {
        rest_base_url: base_url,
        ..CoincheckGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");
    assert_eq!(rules.rules[0].base_asset, "BTC");
    assert_eq!(rules.rules[0].quote_asset, "JPY");

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(1),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.bids.len(), 1);
    assert_eq!(book.order_book.bids[0].price, 10000000.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/api/order_books");
    assert_eq!(
        requests[0].query.get("pair").map(String::as_str),
        Some("btc_jpy")
    );
}
