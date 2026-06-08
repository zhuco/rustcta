use rustcta_exchange_api::{
    ExchangeClient, FeesRequest, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser;
use super::test_support::{
    context, exchange_id, perp_symbol_scope, spawn_rest_server, spot_symbol_scope,
};
use super::{BydfiGatewayAdapter, BydfiGatewayConfig};

#[tokio::test]
async fn bydfi_adapter_should_load_perp_symbol_rules_from_public_rest() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/instruments.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = BydfiGatewayAdapter::new(BydfiGatewayConfig {
        rest_base_url: base_url,
        ..BydfiGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/v1/fapi/market/exchange_info"
    );
}

#[tokio::test]
async fn bydfi_adapter_should_load_perp_order_book_from_public_rest() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/orderbook.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = BydfiGatewayAdapter::new(BydfiGatewayConfig {
        rest_base_url: base_url,
        ..BydfiGatewayConfig::default()
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

    assert_eq!(response.order_book.bids[0].price, 65000.0);
    assert_eq!(response.order_book.asks[0].quantity, 0.9);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1/fapi/market/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));
}

#[tokio::test]
async fn bydfi_adapter_should_route_funding_and_mark_public_helpers() {
    let (base_url, seen) = spawn_rest_server(vec![
        serde_json::json!({"symbol": "BTC-USDT", "markPrice": "65001", "indexPrice": "65000"}),
        serde_json::json!([{"symbol": "BTC-USDT", "lastFundingRate": "0.0001"}]),
    ])
    .await;
    let adapter = BydfiGatewayAdapter::new(BydfiGatewayConfig {
        rest_base_url: base_url,
        ..BydfiGatewayConfig::default()
    })
    .expect("adapter");

    adapter.get_bydfi_mark_price("BTCUSDT").await.expect("mark");
    adapter
        .get_bydfi_funding_rate(Some("BTC-USDT"))
        .await
        .expect("funding");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/v1/fapi/market/mark_price");
    assert_eq!(requests[1].path, "/v1/fapi/market/funding_rate");
}

#[tokio::test]
async fn bydfi_adapter_should_keep_spot_trading_surface_unsupported() {
    let adapter = BydfiGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect_err("spot should be unsupported");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[tokio::test]
async fn bydfi_adapter_should_load_fee_snapshot_from_exchange_info() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/instruments.json"
    ))
    .expect("fixture");
    let (base_url, _) = spawn_rest_server(vec![fixture]).await;
    let adapter = BydfiGatewayAdapter::new(BydfiGatewayConfig {
        rest_base_url: base_url,
        ..BydfiGatewayConfig::default()
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
fn bydfi_parser_should_cover_empty_error_and_missing_fixtures() {
    let exchange = exchange_id();
    let empty = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/instruments_empty.json"
    ))
    .expect("empty fixture");
    let error = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/error.json"
    ))
    .expect("error fixture");
    let missing = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/orderbook_missing_bids.json"
    ))
    .expect("missing fixture");

    assert!(parser::parse_symbol_rules(&exchange, &empty)
        .expect("empty rules")
        .is_empty());
    assert!(parser::parse_orderbook_snapshot(&exchange, perp_symbol_scope(), &error).is_err());
    assert!(parser::parse_orderbook_snapshot(&exchange, perp_symbol_scope(), &missing).is_err());
    assert_eq!(
        parser::normalize_bydfi_symbol("BTCUSDT").unwrap(),
        "BTC-USDT"
    );
    assert_eq!(perp_symbol_scope().market_type, MarketType::Perpetual);
}
