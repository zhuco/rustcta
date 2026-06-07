use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, symbol_scope};
use super::{CoinbaseGatewayAdapter, CoinbaseGatewayConfig};

#[tokio::test]
async fn coinbase_adapter_should_load_spot_symbol_rules_from_products() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("products_success")]).await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        ..CoinbaseGatewayConfig::default()
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
    assert_eq!(response.rules[0].quote_asset, "USD");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00000001")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/products");
    assert_eq!(
        request.query.get("product_type").map(String::as_str),
        Some("SPOT")
    );
}

#[tokio::test]
async fn coinbase_adapter_should_load_intx_perpetual_rules_separately() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "products": [{
            "product_id": "BTC-PERP-INTX",
            "base_currency_id": "BTC",
            "quote_currency_id": "USDC",
            "base_increment": "0.0001",
            "price_increment": "0.1",
            "base_min_size": "0.0001",
            "quote_min_size": "1",
            "product_type": "FUTURE",
            "product_venue": "INTX",
            "future_product_details": {
                "perpetual_details": {
                    "funding_rate": "0.0001",
                    "funding_time": "2026-06-07T08:00:00Z"
                }
            }
        }]
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        international_rest_base_url: base_url,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("perp rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(
        response.rules[0].symbol.market_type,
        rustcta_types::MarketType::Perpetual
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/products");
    assert_eq!(
        request.query.get("product_type").map(String::as_str),
        Some("FUTURE")
    );
    assert_eq!(
        request
            .query
            .get("contract_expiry_type")
            .map(String::as_str),
        Some("PERPETUAL")
    );
    assert_eq!(
        request.query.get("product_venue").map(String::as_str),
        Some("INTX")
    );
}

#[tokio::test]
async fn coinbase_adapter_should_load_limited_order_book() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook_success")]).await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        ..CoinbaseGatewayConfig::default()
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

    assert_eq!(response.order_book.sequence, Some(123));
    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/product_book");
    assert_eq!(
        request.query.get("product_id").map(String::as_str),
        Some("BTC-USD")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("7"));
}

#[test]
fn coinbase_parser_fixtures_should_cover_empty_and_error_products() {
    let exchange = super::test_support::exchange_id();
    let empty = parse_symbol_rules(
        &exchange,
        rustcta_types::MarketType::Spot,
        &fixture("products_empty"),
    )
    .expect("empty products fixture");
    assert!(empty.is_empty());

    let error = parse_symbol_rules(
        &exchange,
        rustcta_types::MarketType::Spot,
        &fixture("products_missing_product_id"),
    )
    .expect_err("missing product id should be rejected");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

#[test]
fn coinbase_parser_fixtures_should_cover_orderbook_shape_errors() {
    let exchange = super::test_support::exchange_id();
    let snapshot =
        parse_orderbook_snapshot(&exchange, symbol_scope(), &fixture("orderbook_success"))
            .expect("orderbook fixture");
    assert_eq!(snapshot.sequence, Some(123));
    assert_eq!(snapshot.bids[0].quantity, 1.25);

    let error = parse_orderbook_snapshot(
        &exchange,
        symbol_scope(),
        &fixture("orderbook_missing_bids"),
    )
    .expect_err("missing bids should be rejected");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "products_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinbase/products_success.json")
        }
        "products_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinbase/products_empty.json")
        }
        "products_missing_product_id" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinbase/products_missing_product_id.json"
        ),
        "orderbook_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinbase/orderbook_success.json")
        }
        "orderbook_missing_bids" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinbase/orderbook_missing_bids.json"
        ),
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
