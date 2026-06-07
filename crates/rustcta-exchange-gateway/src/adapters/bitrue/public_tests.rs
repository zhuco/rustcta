use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{BitrueGatewayAdapter, BitrueGatewayConfig};

#[tokio::test]
async fn bitrue_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("spot_exchange_info")]).await;
    let adapter = BitrueGatewayAdapter::new(BitrueGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        ..BitrueGatewayConfig::default()
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
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v1/exchangeInfo");
}

#[tokio::test]
async fn bitrue_adapter_should_load_perp_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook")]).await;
    let adapter = BitrueGatewayAdapter::new(BitrueGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        ..BitrueGatewayConfig::default()
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
    assert_eq!(request.path, "/fapi/v1/depth");
    assert_eq!(
        request.query.get("contractName").map(String::as_str),
        Some("E-BTC-USDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}

#[tokio::test]
async fn bitrue_adapter_should_load_perp_symbol_rules_from_fixture() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("futures_contracts")]).await;
    let adapter = BitrueGatewayAdapter::new(BitrueGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        ..BitrueGatewayConfig::default()
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
        response.rules[0].symbol.exchange_symbol.symbol,
        "E-BTC-USDT"
    );
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.001")
    );
    assert_eq!(seen.lock().unwrap()[0].path, "/fapi/v1/contracts");
}

#[tokio::test]
async fn bitrue_parser_fixture_should_cover_missing_required_field() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("missing_symbol")]).await;
    let adapter = BitrueGatewayAdapter::new(BitrueGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        ..BitrueGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("missing-field"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect_err("missing symbol should decode-error");

    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

#[test]
fn bitrue_endpoint_mapping_should_declare_dual_product_surfaces() {
    let mapping = include_str!("endpoint_mapping.yaml");
    assert!(mapping.contains("operation: bitrue.get_symbol_rules"));
    assert!(mapping.contains("path: /api/v1/exchangeInfo"));
    assert!(mapping.contains("path: /fapi/v1/contracts"));
    assert!(mapping.contains("path: /api/v1/order"));
    assert!(mapping.contains("path: /fapi/v2/order"));
    assert!(mapping.contains("mode: composed_sequential"));
    assert!(mapping.contains("spot:"));
    assert!(mapping.contains("futures:"));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "spot_exchange_info" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/spot_exchange_info.json")
        }
        "futures_contracts" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/futures_contracts.json")
        }
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/orderbook.json")
        }
        "missing_symbol" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/missing_symbol.json")
        }
        _ => unreachable!("unknown Bitrue fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
