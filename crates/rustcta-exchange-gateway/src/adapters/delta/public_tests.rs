use rustcta_exchange_api::{
    ExchangeClient, FeesRequest, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::Value;

use super::parser::{parse_option_chain, parse_symbol_rules};
use super::test_support::{
    context, exchange_id, option_symbol_scope, perp_symbol_scope, spawn_rest_server,
};
use super::{DeltaGatewayAdapter, DeltaGatewayConfig};

#[tokio::test]
async fn delta_adapter_should_load_perp_and_option_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("products")]).await;
    let adapter = DeltaGatewayAdapter::new(DeltaGatewayConfig {
        rest_base_url: base_url,
        ..DeltaGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![perp_symbol_scope(), option_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 2);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(response.rules[1].symbol.market_type, MarketType::Option);
    assert_eq!(seen.lock().unwrap()[0].path, "/v2/products");
}

#[tokio::test]
async fn delta_adapter_should_load_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook")]).await;
    let adapter = DeltaGatewayAdapter::new(DeltaGatewayConfig {
        rest_base_url: base_url,
        ..DeltaGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(20),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 65000.0);
    assert_eq!(response.order_book.asks[0].quantity, 3.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v2/l2orderbook/BTCUSDT");
    assert_eq!(request.query.get("depth").map(String::as_str), Some("20"));
}

#[tokio::test]
async fn delta_adapter_should_parse_option_chain_and_greeks_fixture() {
    let chain =
        parse_option_chain(&exchange_id(), "BTC", &fixture("option_chain")).expect("option chain");
    assert_eq!(chain.contracts.len(), 2);
    assert_eq!(chain.greeks[0].delta.as_deref(), Some("0.52"));
    assert_eq!(chain.greeks[1].implied_volatility.as_deref(), Some("0.66"));
}

#[tokio::test]
async fn delta_adapter_should_expose_product_fee_rates() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("products")]).await;
    let adapter = DeltaGatewayAdapter::new(DeltaGatewayConfig {
        rest_base_url: base_url,
        ..DeltaGatewayConfig::default()
    })
    .expect("adapter");

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0002");
}

#[test]
fn delta_parser_should_cover_empty_products() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("products_empty")).expect("empty");
    assert!(rules.is_empty());
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "products" => include_str!("../../../../../tests/fixtures/exchanges/delta/products.json"),
        "products_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/delta/products_empty.json")
        }
        "orderbook" => include_str!("../../../../../tests/fixtures/exchanges/delta/orderbook.json"),
        "option_chain" => {
            include_str!("../../../../../tests/fixtures/exchanges/delta/option_chain.json")
        }
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
