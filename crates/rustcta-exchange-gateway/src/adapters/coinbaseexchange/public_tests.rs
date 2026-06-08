use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{CoinbaseExchangeGatewayAdapter, CoinbaseExchangeGatewayConfig};

#[tokio::test]
async fn coinbaseexchange_public_rest_should_parse_products_and_book() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            {
                "id": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "base_increment": "0.00000001",
                "quote_increment": "0.01",
                "base_min_size": "0.0001",
                "base_max_size": "100",
                "status": "online"
            }
        ]),
        json!({
            "sequence": 10,
            "bids": [["65000.00", "0.4", 1]],
            "asks": [["65010.00", "0.5", 1]]
        }),
    ])
    .await;
    let adapter = CoinbaseExchangeGatewayAdapter::new(CoinbaseExchangeGatewayConfig {
        rest_base_url: base_url,
        ..CoinbaseExchangeGatewayConfig::default()
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
    assert_eq!(rules.rules.len(), 1);
    assert_eq!(rules.rules[0].base_asset, "BTC");
    assert_eq!(rules.rules[0].quote_asset, "USD");
    assert_eq!(rules.rules[0].price_precision, Some(2));

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(50),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.bids[0].price, 65000.0);
    assert_eq!(book.order_book.asks[0].quantity, 0.5);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/products");
    assert_eq!(requests[1].path, "/products/BTC-USD/book");
    assert_eq!(
        requests[1].query.get("level").map(String::as_str),
        Some("2")
    );
}
