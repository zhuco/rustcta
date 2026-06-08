use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use crate::adapters::AdapterBackedGateway;

use super::parser::parse_symbol_rules;
use super::test_support::{
    config_with_base_url, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::HollaexGatewayAdapter;

fn load_parser_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "constants_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/parser/constants_success.json"
        ),
        "orderbook_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/parser/orderbook_success.json"
        ),
        _ => panic!("unknown hollaex parser fixture {name}"),
    };
    serde_json::from_str(text).expect("parser fixture")
}

#[test]
fn hollaex_parser_should_read_constants_fixture() {
    let rules = parse_symbol_rules(
        &exchange_id(),
        &load_parser_fixture("constants_success.json"),
    )
    .expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc-usdt");
    assert_eq!(rules[1].base_asset, "XHT");
    assert_eq!(rules[1].quote_asset, "USDT");
}

#[tokio::test]
async fn hollaex_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_parser_fixture("constants_success.json")]).await;
    let adapter = HollaexGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "XHT");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.0001"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.01")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/constants");
}

#[tokio::test]
async fn hollaex_adapter_should_load_order_book_snapshot_from_public_rest() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_parser_fixture("orderbook_success.json")]).await;
    let adapter = HollaexGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(1),
        })
        .await
        .expect("order book");

    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.bids[0].price, 0.1005);
    assert_eq!(response.order_book.asks[0].quantity, 42.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("xht-usdt")
    );
}

#[tokio::test]
async fn hollaex_adapter_should_reject_non_spot_scope() {
    let adapter = HollaexGatewayAdapter::default_public().expect("adapter");
    let mut symbol = symbol_scope();
    symbol.market_type = MarketType::Perpetual;
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("non-spot"),
            symbol,
            depth: Some(5),
        })
        .await
        .expect_err("non spot unsupported");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[test]
fn hollaex_gateway_should_register_named_adapter() {
    let duplicate = AdapterBackedGateway::with_named_adapters("test", ["hollaex", "hollaex_demo"]);
    assert!(duplicate.is_err());
    assert!(format!("{:?}", duplicate.err()).contains("already registered"));

    let gateway = AdapterBackedGateway::with_named_adapters("test", ["hollaex"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
    assert_eq!(exchange_id().as_str(), "hollaex");
}
