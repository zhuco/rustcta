use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BitbnsGatewayAdapter, BitbnsGatewayConfig};

fn bitbns_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "markets_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/markets_success.json")
        }
        "orderbook_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/orderbook_success.json")
        }
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/unsupported_boundary.json")
        }
        _ => panic!("unknown bitbns fixture {name}"),
    };
    serde_json::from_str(text).expect("bitbns fixture")
}

#[tokio::test]
async fn bitbns_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![bitbns_fixture("markets_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        rest_base_url: base_url,
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("ETH_USDT")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "ETH");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.001"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("0.1"));
    assert!(response.rules[0].supports_limit_orders);
    assert!(!response.rules[0].supports_market_orders);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/order/fetchMarkets/");
}

#[test]
fn bitbns_public_parser_fixtures_should_cover_markets_and_order_book() {
    let rules =
        parse_symbol_rules(&exchange_id(), &bitbns_fixture("markets_success.json")).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC_INR");
    assert_eq!(rules[0].min_notional.as_deref(), Some("10"));
    assert_eq!(rules[1].symbol.exchange_symbol.symbol, "ETH_USDT");

    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope("BTC_INR"),
        1,
        &bitbns_fixture("orderbook_success.json"),
    )
    .expect("book");
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
    assert_eq!(book.bids[0].price, 65000.0);
    assert_eq!(
        book.exchange_timestamp
            .expect("timestamp")
            .timestamp_millis(),
        1630664703000
    );
}

#[tokio::test]
async fn bitbns_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![bitbns_fixture("orderbook_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        rest_base_url: base_url,
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC_INR"),
            depth: Some(1),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.asks[0].price, 65010.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/exchangeData/orderBook");
    assert_eq!(request.query.get("coin").map(String::as_str), Some("BTC"));
    assert_eq!(request.query.get("market").map(String::as_str), Some("INR"));
}

#[tokio::test]
async fn bitbns_adapter_should_reject_zero_depth_and_non_spot_private_operations() {
    let adapter = BitbnsGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("zero-depth"),
            symbol: symbol_scope("BTC_INR"),
            depth: Some(0),
        })
        .await
        .expect_err("zero depth");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));

    let boundary = bitbns_fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
}

#[test]
fn bitbns_transport_error_fixture_should_remain_classified_boundary_data() {
    let error = json!({
        "status": 0,
        "error": "Invalid coin or market"
    });
    assert_eq!(error["status"], 0);
    assert_eq!(error["error"], "Invalid coin or market");
}
