use rustcta_exchange_api::{
    CapabilitySupport, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{OkxGatewayAdapter, OkxGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/okx/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

#[tokio::test]
async fn okx_adapter_should_declare_task9_toolchain_capabilities() {
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(matches!(
        capabilities.capabilities_v2.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.public_streams,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(100)
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "place_order"
            && endpoint.path.as_deref() == Some("/api/v5/trade/order")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_recent_fills"
            && endpoint.path.as_deref() == Some("/api/v5/trade/fills-history")));
}

#[test]
fn okx_parser_fixtures_should_cover_success_empty_and_error_shapes() {
    let exchange = super::test_support::exchange_id();
    let rules =
        parse_symbol_rules(&exchange, &fixture("parser/instruments_success.json")).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");

    let empty =
        parse_symbol_rules(&exchange, &fixture("parser/instruments_empty.json")).expect("empty");
    assert!(empty.is_empty());
    assert!(
        parse_symbol_rules(&exchange, &fixture("parser/instruments_missing_field.json")).is_err()
    );
    assert!(parse_orderbook_snapshot(
        &exchange,
        symbol_scope(),
        &fixture("parser/orderbook_error.json")
    )
    .is_err());
}

#[tokio::test]
async fn okx_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "baseCcy": "BTC",
            "quoteCcy": "USDT",
            "tickSz": "0.01",
            "lotSz": "0.00000001",
            "minSz": "0.00001",
            "minLmtAmt": "5",
            "maxLmtSz": "100",
            "state": "live"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
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
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v5/public/instruments");
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("SPOT")
    );
}

#[tokio::test]
async fn okx_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "bids": [["99.5", "1.25", "0", "2"], ["99.0", "2", "0", "1"]],
            "asks": [["100.5", "1.5", "0", "3"], ["101.0", "2", "0", "1"]],
            "ts": "1710000000123"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
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

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[1].quantity, 2.0);
    assert!(response.order_book.exchange_timestamp.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v5/market/books");
    assert_eq!(
        request.query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(request.query.get("sz").map(String::as_str), Some("10"));
}
