use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BitoproGatewayAdapter, BitoproGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "trading_pairs_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/trading_pairs_success.json"
        ),
        "trading_pairs_empty.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/trading_pairs_empty.json"
        ),
        "trading_pairs_missing_required_fields.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/trading_pairs_missing_required_fields.json"
        ),
        "orderbook_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/orderbook_success.json"
        ),
        _ => panic!("unknown bitopro fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[test]
fn bitopro_adapter_should_register_by_name() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("bitopro-test", ["bitopro"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn bitopro_public_parser_fixtures_should_cover_twd_pairs_and_books() {
    let rules =
        parse_symbol_rules(&exchange_id(), &fixture("trading_pairs_success.json")).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc_twd");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "TWD");
    assert_eq!(rules[0].price_increment.as_deref(), Some("1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("100"));

    let empty =
        parse_symbol_rules(&exchange_id(), &fixture("trading_pairs_empty.json")).expect("empty");
    assert!(empty.is_empty());

    let missing = parse_symbol_rules(
        &exchange_id(),
        &fixture("trading_pairs_missing_required_fields.json"),
    )
    .expect_err("missing pair should fail");
    assert!(format!("{missing:?}").contains("missing field pair"));

    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope(),
        &fixture("orderbook_success.json"),
    )
    .expect("book");
    assert_eq!(book.best_bid().unwrap().price, 3_000_000.0);
    assert_eq!(book.best_ask().unwrap().price, 3_010_000.0);
}

#[tokio::test]
async fn bitopro_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("trading_pairs_success.json")]).await;
    let adapter = BitoproGatewayAdapter::new(BitoproGatewayConfig {
        rest_base_url: base_url,
        ..Default::default()
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
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "btc_twd");
    assert_eq!(seen.lock().unwrap()[0].path, "/provisioning/trading-pairs");
}

#[tokio::test]
async fn bitopro_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "bids": [{"price": "3000000", "amount": "0.1", "count": 1, "total": "0.1"}],
        "asks": [{"price": "3010000", "amount": "0.2", "count": 1, "total": "0.2"}],
        "timestamp": 1639552073346_i64
    })])
    .await;
    let adapter = BitoproGatewayAdapter::new(BitoproGatewayConfig {
        rest_base_url: base_url,
        ..Default::default()
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

    assert_eq!(response.order_book.best_bid().unwrap().price, 3_000_000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/order-book/btc_twd");
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
    assert_eq!(request.query.get("scale").map(String::as_str), Some("0"));
}
