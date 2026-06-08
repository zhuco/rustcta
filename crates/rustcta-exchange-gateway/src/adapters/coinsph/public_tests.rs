use rustcta_exchange_api::{
    CapabilitySupport, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, spawn_rest_server, symbol_scope, usdt_symbol_scope};
use super::{CoinsPhGatewayAdapter, CoinsPhGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/coinsph/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

#[tokio::test]
async fn coinsph_adapter_should_declare_php_spot_boundaries() {
    let adapter = CoinsPhGatewayAdapter::new(CoinsPhGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinsPhGatewayConfig::default()
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
    assert!(!capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(!capabilities.supports_amend_order);
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/openapi/quote/v1/depth")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "fiat_transfer"
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
}

#[test]
fn coinsph_parser_fixtures_should_cover_php_market_and_errors() {
    let exchange = super::test_support::exchange_id();
    let rules =
        parse_symbol_rules(&exchange, &fixture("parser/symbol_rules_success.json")).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "PHP");

    let empty =
        parse_symbol_rules(&exchange, &fixture("parser/symbol_rules_empty.json")).expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(
        &exchange,
        &fixture("parser/symbol_rules_missing_field.json")
    )
    .is_err());
    assert!(parse_orderbook_snapshot(
        &exchange,
        symbol_scope("BTCPHP"),
        &fixture("parser/orderbook_error.json")
    )
    .is_err());
}

#[tokio::test]
async fn coinsph_adapter_should_load_php_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbols": [{
            "symbol": "BTCPHP",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "PHP",
            "baseAssetPrecision": 8,
            "quoteAssetPrecision": 2,
            "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "1.00", "maxPrice": "10000000.00", "tickSize": "1.00"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "10.00000000", "stepSize": "0.00001000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "100.00"}
            ]
        }, {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": []
        }]
    })])
    .await;
    let adapter = CoinsPhGatewayAdapter::new(CoinsPhGatewayConfig {
        rest_base_url: base_url,
        ..CoinsPhGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("BTC/PHP")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BTCPHP");
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("100.00"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/openapi/v1/exchangeInfo");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCPHP")
    );
}

#[tokio::test]
async fn coinsph_adapter_should_reject_non_php_spot_symbols() {
    let adapter = CoinsPhGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![usdt_symbol_scope("BTCUSDT")],
        })
        .await
        .expect_err("non-PHP market unsupported");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[tokio::test]
async fn coinsph_adapter_should_load_depth_from_quote_api_namespace() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 1027024,
        "bids": [["3500000.00", "0.10000000"]],
        "asks": [["3501000.00", "0.05000000"]]
    })])
    .await;
    let adapter = CoinsPhGatewayAdapter::new(CoinsPhGatewayConfig {
        rest_base_url: base_url,
        ..CoinsPhGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC-PHP"),
            depth: Some(25),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(1027024));
    assert_eq!(response.order_book.bids[0].price, 3_500_000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/openapi/quote/v1/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCPHP")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("50"));
}
