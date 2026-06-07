use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{BingxGatewayAdapter, BingxGatewayConfig};

#[tokio::test]
async fn bingx_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "symbols": [{
                "symbol": "BTC-USDT",
                "tickSize": "0.01",
                "stepSize": "0.00001",
                "minQty": "0.00001",
                "maxQty": "100",
                "minNotional": "5"
            }]
        }
    })])
    .await;
    let adapter = BingxGatewayAdapter::new(BingxGatewayConfig {
        rest_base_url: base_url,
        ..BingxGatewayConfig::default()
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
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/openApi/spot/v1/common/symbols"
    );
}

#[test]
fn bingx_public_parser_should_cover_fixture_success_and_missing_field() {
    let spot_symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/spot_symbols.json"
    ))
    .expect("spot symbols fixture");
    let rules = parse_symbol_rules(
        &super::test_support::exchange_id(),
        rustcta_types::MarketType::Spot,
        &spot_symbols,
    )
    .expect("spot symbol rules");
    assert_eq!(rules[0].base_asset, "BTC");

    let perp_contracts: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/perp_contracts.json"
    ))
    .expect("perp contracts fixture");
    let rules = parse_symbol_rules(
        &super::test_support::exchange_id(),
        rustcta_types::MarketType::Perpetual,
        &perp_contracts,
    )
    .expect("perp symbol rules");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(
        &super::test_support::exchange_id(),
        perp_symbol_scope(),
        &orderbook,
    )
    .expect("orderbook");
    assert_eq!(book.bids[0].price, 65000.0);

    let missing_symbol = json!({"code": 0, "data": {"symbols": [{"tickSize": "0.01"}]}});
    let error = parse_symbol_rules(
        &super::test_support::exchange_id(),
        rustcta_types::MarketType::Spot,
        &missing_symbol,
    )
    .expect_err("missing symbol should fail");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Exchange(_)
    ));
}

#[test]
fn bingx_adapter_static_toolchain_files_should_declare_required_plans() {
    let endpoint_mapping = include_str!("endpoint_mapping.yaml");
    let capabilities = include_str!("capabilities_v2.yaml");

    for required in [
        "rate_limit_plan:",
        "pagination_plan:",
        "reconciliation_plan:",
        "batch_capability:",
        "websocket_runtime:",
        "operation: bingx.place_order",
        "operation: bingx.batch_cancel_orders",
        "operation: bingx.generate_listen_key",
        "auth_renewal:",
        "rest_reconciliation",
    ] {
        assert!(
            endpoint_mapping.contains(required),
            "endpoint_mapping.yaml missing {required}"
        );
    }

    for required in [
        "schema_version: 2",
        "exchange: bingx",
        "credential_gated",
        "public:",
        "private:",
        "auth_renewal:",
        "reconciliation:",
        "batch:",
        "pagination:",
        "rate_limits:",
    ] {
        assert!(
            capabilities.contains(required),
            "capabilities_v2.yaml missing {required}"
        );
    }
}

#[tokio::test]
async fn bingx_adapter_should_load_perp_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "T": 1700000000000i64,
            "bids": [["99.5", "1.25"]],
            "asks": [["100.5", "1.5"]]
        }
    })])
    .await;
    let adapter = BingxGatewayAdapter::new(BingxGatewayConfig {
        rest_base_url: base_url,
        ..BingxGatewayConfig::default()
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
    assert_eq!(request.path, "/openApi/swap/v2/quote/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}
