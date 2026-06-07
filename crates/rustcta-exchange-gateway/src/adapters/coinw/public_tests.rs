use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{CoinwGatewayAdapter, CoinwGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[tokio::test]
async fn coinw_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200",
        "data": [
            {
                "currencyBase": "BTC",
                "currencyQuote": "USDT",
                "currencyPair": "BTC_USDT",
                "pricePrecision": 2,
                "countPrecision": 4,
                "minBuyPrice": "0.001",
                "maxBuyPrice": "99999999",
                "minBuyCount": "0.0001",
                "maxBuyCount": "9999999",
                "minBuyAmount": "5",
                "maxBuyAmount": "99999999",
                "state": 1
            }
        ],
        "msg": "SUCCESS",
        "success": true,
        "failed": false
    })])
    .await;
    let adapter = CoinwGatewayAdapter::new(CoinwGatewayConfig {
        rest_base_url: base_url,
        ..CoinwGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.0001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/v1/public");
    assert_eq!(
        request.query.get("command").map(String::as_str),
        Some("returnSymbol")
    );
}

#[tokio::test]
async fn coinw_adapter_should_load_perpetual_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [
            {
                "base": "btc",
                "name": "BTC",
                "quote": "usdt",
                "pricePrecision": 1,
                "minSize": 1,
                "oneLotSize": 0.001,
                "maxPosition": 20000,
                "makerFee": "0.0001",
                "takerFee": "0.0006",
                "status": "online"
            }
        ],
        "msg": ""
    })])
    .await;
    let adapter = CoinwGatewayAdapter::new(CoinwGatewayConfig {
        rest_base_url: base_url,
        ..CoinwGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(
        response.rules[0].symbol.market_type,
        rustcta_types::MarketType::Perpetual
    );
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BTC_USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.001")
    );
    assert!(response.rules[0].supports_reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1/perpum/instruments");
}

#[tokio::test]
async fn coinw_adapter_should_load_spot_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200",
        "data": [
            {
                "pair": "BTC_USDT",
                "asks": [["100.5", "0.2"], ["101.0", "0.1"]],
                "bids": [["100.0", "0.3"], ["99.5", "0.4"]]
            }
        ],
        "msg": "SUCCESS",
        "success": true,
        "failed": false
    })])
    .await;
    let adapter = CoinwGatewayAdapter::new(CoinwGatewayConfig {
        rest_base_url: base_url,
        ..CoinwGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-book"),
            symbol: spot_symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 100.0);
    assert_eq!(response.order_book.asks[0].quantity, 0.2);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/public");
    assert_eq!(
        request.query.get("command").map(String::as_str),
        Some("returnOrderBook")
    );
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(request.query.get("size").map(String::as_str), Some("20"));
}

#[tokio::test]
async fn coinw_adapter_should_load_perpetual_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "asks": [{"p": 100.5, "m": 0.2}, {"p": 101.0, "m": 0.1}],
            "bids": [{"p": 100.0, "m": 0.3}, {"p": 99.5, "m": 0.4}],
            "n": "btc",
            "ts": 1775443378356_u64
        },
        "msg": ""
    })])
    .await;
    let adapter = CoinwGatewayAdapter::new(CoinwGatewayConfig {
        rest_base_url: base_url,
        ..CoinwGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-book"),
            symbol: perp_symbol_scope(),
            depth: Some(5),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 100.0);
    assert_eq!(response.order_book.asks[0].quantity, 0.2);
    assert!(response.order_book.exchange_timestamp.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1/perpumPublic/depth");
    assert_eq!(request.query.get("base").map(String::as_str), Some("BTC"));
}

#[test]
fn coinw_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["coinw"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn coinw_public_parser_fixtures_should_cover_success_empty_and_missing_fields() {
    let exchange_id = super::test_support::exchange_id();
    let spot_rules = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/spot_symbol_rules_success.json"
    ))
    .expect("spot rules fixture");
    let perp_rules = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/perp_symbol_rules_success.json"
    ))
    .expect("perp rules fixture");
    let spot_book = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/spot_order_book_success.json"
    ))
    .expect("spot book fixture");
    let perp_book = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/perp_order_book_success.json"
    ))
    .expect("perp book fixture");
    let empty = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/empty_data_array.json"
    ))
    .expect("empty fixture");
    let missing = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/missing_symbol_field.json"
    ))
    .expect("missing fixture");

    assert_eq!(
        parse_symbol_rules(&exchange_id, MarketType::Spot, &spot_rules)
            .expect("spot rules")
            .len(),
        1
    );
    assert_eq!(
        parse_symbol_rules(&exchange_id, MarketType::Perpetual, &perp_rules)
            .expect("perp rules")
            .len(),
        1
    );
    assert_eq!(
        parse_symbol_rules(&exchange_id, MarketType::Spot, &empty)
            .expect("empty rules")
            .len(),
        0
    );
    assert!(parse_symbol_rules(&exchange_id, MarketType::Spot, &missing).is_err());
    assert_eq!(
        parse_orderbook_snapshot(&exchange_id, spot_symbol_scope(), &spot_book)
            .expect("spot book")
            .bids[0]
            .price,
        100.0
    );
    assert_eq!(
        parse_orderbook_snapshot(&exchange_id, perp_symbol_scope(), &perp_book)
            .expect("perp book")
            .asks[0]
            .quantity,
        0.2
    );
}
