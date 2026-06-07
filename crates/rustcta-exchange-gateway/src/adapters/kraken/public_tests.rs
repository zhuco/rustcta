use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{KrakenGatewayAdapter, KrakenGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "asset_pairs.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/asset_pairs.json")
        }
        "asset_pairs_empty.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/asset_pairs_empty.json")
        }
        "error.json" => include_str!("../../../../../tests/fixtures/exchanges/kraken/error.json"),
        "orderbook.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/orderbook.json")
        }
        "orderbook_missing_asks.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kraken/orderbook_missing_asks.json"
        ),
        _ => panic!("unknown kraken fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[tokio::test]
async fn kraken_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": [],
        "result": fixture("asset_pairs.json")
    })])
    .await;
    let adapter = KrakenGatewayAdapter::new(KrakenGatewayConfig {
        spot_rest_base_url: format!("{base_url}/0"),
        futures_rest_base_url: base_url,
        ..KrakenGatewayConfig::default()
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
    assert_eq!(
        response.rules[0]
            .symbol
            .canonical_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "BTC/USD"
    );
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/0/public/AssetPairs");
}

#[tokio::test]
async fn kraken_adapter_should_load_futures_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "instruments": [{
            "symbol": "PF_XBTUSDT",
            "type": "perpetual",
            "tickSize": "0.1",
            "contractSize": "1",
            "minTradeSize": "0.001"
        }]
    })])
    .await;
    let adapter = KrakenGatewayAdapter::new(KrakenGatewayConfig {
        spot_rest_base_url: format!("{base_url}/0"),
        futures_rest_base_url: base_url,
        ..KrakenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(
        response.rules[0]
            .symbol
            .canonical_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "BTC/USDT"
    );
    assert_eq!(response.rules[0].quantity_increment.as_deref(), Some("1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/instruments");
}

#[tokio::test]
async fn kraken_adapter_should_load_spot_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": [],
        "result": fixture("orderbook.json")
    })])
    .await;
    let adapter = KrakenGatewayAdapter::new(KrakenGatewayConfig {
        spot_rest_base_url: format!("{base_url}/0"),
        futures_rest_base_url: base_url,
        ..KrakenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(501),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 37500.0);
    assert_eq!(response.order_book.asks[0].price, 37501.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/0/public/Depth");
    assert_eq!(
        request.query.get("pair").map(String::as_str),
        Some("XBTUSD")
    );
    assert_eq!(request.query.get("count").map(String::as_str), Some("500"));
}

#[tokio::test]
async fn kraken_public_parser_fixtures_should_cover_empty_error_and_missing_fields() {
    let empty_rules = super::parser::parse_spot_symbol_rules(
        &super::test_support::exchange_id(),
        &fixture("asset_pairs_empty.json"),
    )
    .expect("empty rules");
    assert!(empty_rules.is_empty());

    let error = super::parser::parse_spot_symbol_rules(
        &super::test_support::exchange_id(),
        &fixture("error.json"),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Exchange(_)
    ));

    let missing = super::parser::parse_spot_orderbook_snapshot(
        &super::test_support::exchange_id(),
        spot_symbol_scope(),
        &fixture("orderbook_missing_asks.json"),
    )
    .unwrap_err();
    assert!(matches!(
        missing,
        rustcta_exchange_api::ExchangeApiError::Exchange(_)
    ));
}

#[tokio::test]
async fn kraken_adapter_should_load_futures_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "orderBook": {
            "symbol": "PF_XBTUSDT",
            "bids": [["37500.0", "2"]],
            "asks": [["37501.0", "3"]],
            "sequence": 42
        }
    })])
    .await;
    let adapter = KrakenGatewayAdapter::new(KrakenGatewayConfig {
        spot_rest_base_url: format!("{base_url}/0"),
        futures_rest_base_url: base_url,
        ..KrakenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-book"),
            symbol: perp_symbol_scope(),
            depth: Some(10),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(42));
    assert_eq!(response.order_book.best_bid().unwrap().price, 37500.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("PF_XBTUSDT")
    );
}

#[test]
fn kraken_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["kraken"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}
