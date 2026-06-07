use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::parse_symbol_rules;
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{PoloniexGatewayAdapter, PoloniexGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    serde_json::from_str(match path {
        "success" => include_str!(
            "../../../../../tests/fixtures/exchanges/poloniex/parser/symbol_rules_success.json"
        ),
        "empty" => include_str!(
            "../../../../../tests/fixtures/exchanges/poloniex/parser/symbol_rules_empty.json"
        ),
        "error" => include_str!(
            "../../../../../tests/fixtures/exchanges/poloniex/parser/symbol_rules_error.json"
        ),
        "missing_symbol" => include_str!(
            "../../../../../tests/fixtures/exchanges/poloniex/parser/symbol_rules_missing_symbol.json"
        ),
        _ => unreachable!("unknown fixture"),
    })
    .expect("fixture json")
}

#[test]
fn poloniex_parser_should_read_symbol_rule_fixtures() {
    let exchange = super::test_support::exchange_id();
    let success = parse_symbol_rules(&exchange, MarketType::Spot, &fixture("success"))
        .expect("success fixture");
    assert_eq!(success.len(), 1);
    assert_eq!(success[0].symbol.exchange_symbol.symbol, "BTC_USDT");

    let empty =
        parse_symbol_rules(&exchange, MarketType::Spot, &fixture("empty")).expect("empty fixture");
    assert!(empty.is_empty());

    let error = parse_symbol_rules(&exchange, MarketType::Spot, &fixture("error"))
        .expect_err("error fixture should fail");
    assert!(error.to_string().contains("symbols response missing list"));

    let missing = parse_symbol_rules(&exchange, MarketType::Spot, &fixture("missing_symbol"))
        .expect_err("missing symbol should fail");
    assert!(missing.to_string().contains("missing field symbol"));
}

#[tokio::test]
async fn poloniex_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([{
        "symbol": "BTC_USDT",
        "baseCurrencyName": "BTC",
        "quoteCurrencyName": "USDT",
        "state": "NORMAL",
        "symbolTradeLimit": {
            "priceScale": 2,
            "quantityScale": 6,
            "minQuantity": "0.000001",
            "minAmount": "1"
        }
    }])])
    .await;
    let adapter = PoloniexGatewayAdapter::new(PoloniexGatewayConfig {
        rest_base_url: base_url,
        ..PoloniexGatewayConfig::default()
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
    assert_eq!(seen.lock().unwrap()[0].path, "/markets");
}

#[tokio::test]
async fn poloniex_adapter_should_load_perp_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "ts": 1700000000000i64,
            "bids": [["99.5", "1.25"]],
            "asks": [["100.5", "1.5"]]
        }
    })])
    .await;
    let adapter = PoloniexGatewayAdapter::new(PoloniexGatewayConfig {
        rest_base_url: base_url,
        ..PoloniexGatewayConfig::default()
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

    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v3/market/orderBook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC_USDT_PERP")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}

#[tokio::test]
async fn poloniex_adapter_should_parse_spot_flat_order_book() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "time": 1700000000000i64,
        "bids": ["99.5", "1.25"],
        "asks": ["100.5", "1.5"]
    })])
    .await;
    let adapter = PoloniexGatewayAdapter::new(PoloniexGatewayConfig {
        rest_base_url: base_url,
        ..PoloniexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-book"),
            symbol: spot_symbol_scope(),
            depth: Some(55),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.asks[0].quantity, 1.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/markets/BTC_USDT/orderBook");
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}
