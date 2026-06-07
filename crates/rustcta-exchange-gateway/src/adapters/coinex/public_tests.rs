use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{CoinExGatewayAdapter, CoinExGatewayConfig};

fn coinex_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "spot_markets_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinex/spot_markets_success.json")
        }
        "spot_markets_empty.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinex/spot_markets_empty.json")
        }
        "spot_markets_missing_field.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinex/spot_markets_missing_field.json"
        ),
        "orderbook_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinex/orderbook_success.json")
        }
        _ => panic!("unknown coinex fixture {name}"),
    };
    serde_json::from_str(text).expect("coinex fixture")
}

#[tokio::test]
async fn coinex_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "market": "BTCUSDT",
            "base_ccy": "BTC",
            "quote_ccy": "USDT",
            "price_precision": 2,
            "amount_precision": 6,
            "min_amount": "0.0001",
            "min_notional": "1",
            "max_amount": "100",
            "is_trading_available": true
        }]
    })])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        ..CoinExGatewayConfig::default()
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
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(response.rules[0].min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("1"));
    assert_eq!(seen.lock().unwrap()[0].path, "/spot/market".to_string());
}

#[test]
fn coinex_public_parser_fixtures_should_cover_success_empty_and_missing_field() {
    let rules = parse_symbol_rules(
        &super::test_support::exchange_id(),
        &coinex_fixture("spot_markets_success.json")["data"],
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");

    let empty = parse_symbol_rules(
        &super::test_support::exchange_id(),
        &coinex_fixture("spot_markets_empty.json")["data"],
    )
    .expect("empty");
    assert!(empty.is_empty());

    let missing = parse_symbol_rules(
        &super::test_support::exchange_id(),
        &coinex_fixture("spot_markets_missing_field.json")["data"],
    )
    .expect_err("missing market/name field should fail");
    assert!(format!("{missing:?}").contains("missing field name"));

    let book = parse_orderbook_snapshot(
        &super::test_support::exchange_id(),
        symbol_scope(),
        &coinex_fixture("orderbook_success.json")["data"],
    )
    .expect("orderbook");
    assert_eq!(book.sequence, Some(100));
    assert_eq!(book.bids[0].price, 65000.0);
}

#[tokio::test]
async fn coinex_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "last": 100,
            "updated_at": 1743054548123_i64,
            "depth": {
                "bids": [["99.5", "1.25"], ["99.0", "2"]],
                "asks": [["100.5", "1.5"], ["101.0", "2"]]
            }
        }
    })])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        ..CoinExGatewayConfig::default()
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

    assert_eq!(response.order_book.sequence, Some(100));
    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/spot/depth");
    assert_eq!(
        request.query.get("market").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
    assert_eq!(request.query.get("interval").map(String::as_str), Some("0"));
}
