use rustcta_exchange_api::{
    ExchangeClient, FundingRatesRequest, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perpetual_symbol_scope, spawn_rest_server, symbol_scope};
use super::{MexcGatewayAdapter, MexcGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/mexc/toolchain/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

#[test]
fn mexc_parser_fixtures_should_cover_success_empty_and_error_shapes() {
    let exchange = super::test_support::exchange_id();
    let rules = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("symbol_rules_success.json"),
    )
    .expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");

    let empty = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("symbol_rules_empty.json"),
    )
    .expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("symbol_rules_missing_field.json")
    )
    .is_err());
    assert!(
        parse_orderbook_snapshot(&exchange, symbol_scope(), &fixture("orderbook_error.json"))
            .is_err()
    );
}

#[tokio::test]
async fn mexc_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        ..MexcGatewayConfig::default()
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
        seen.lock().unwrap()[0].path,
        "/api/v3/exchangeInfo".to_string()
    );
}

#[tokio::test]
async fn mexc_adapter_should_load_perpetual_symbol_rules_from_contract_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "code": 0,
        "data": [{
            "symbol": "BTC_USDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "priceUnit": "0.1",
            "volUnit": "1",
            "minVol": "1",
            "state": 0
        }]
    })])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        contract_rest_base_url: base_url,
        ..MexcGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perpetual_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Perpetual);
    assert!(response.rules[0].supports_reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/contract/detail");
}

#[tokio::test]
async fn mexc_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 100,
        "bids": [["99.5", "1.25"], ["99.0", "2"]],
        "asks": [["100.5", "1.5"], ["101.0", "2"]]
    })])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        ..MexcGatewayConfig::default()
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
    assert_eq!(request.path, "/api/v3/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
}

#[tokio::test]
async fn mexc_adapter_should_load_perpetual_funding_rate_from_contract_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "code": 0,
        "data": {
            "symbol": "BTC_USDT",
            "fundingRate": "0.0001",
            "nextSettleTime": 1700028800000_i64,
            "timestamp": 1700000000000_i64
        }
    })])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        contract_rest_base_url: base_url,
        ..MexcGatewayConfig::default()
    })
    .expect("adapter");

    let response = ExchangeClient::get_funding_rates(
        &adapter,
        FundingRatesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("funding"),
            symbols: vec![perpetual_symbol_scope()],
        },
    )
    .await
    .expect("funding");

    assert_eq!(response.rates.len(), 1);
    assert_eq!(response.rates[0].funding_rate, "0.0001");
    assert!(response.rates[0].funding_time.is_some());
    assert!(response.rates[0].next_funding_time.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/contract/funding_rate/BTC_USDT");
}
