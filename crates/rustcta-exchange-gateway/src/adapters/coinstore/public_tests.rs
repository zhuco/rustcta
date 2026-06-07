use rustcta_exchange_api::{
    ExchangeClient, FeesRequest, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderType, TimeInForce};

use super::test_support::{context, exchange_id, spawn_rest_server, spot_symbol_scope};
use super::{CoinstoreGatewayAdapter, CoinstoreGatewayConfig};

#[tokio::test]
async fn coinstore_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("spot_symbols_success")]).await;
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        spot_rest_base_url: base_url,
        ..CoinstoreGatewayConfig::default()
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
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");
    assert_eq!(response.rules[0].min_price, None);
    assert_eq!(response.rules[0].max_price, None);
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v2/public/config/spot/symbols");
}

#[tokio::test]
async fn coinstore_adapter_should_load_fees_from_public_config() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("spot_symbols_success")]).await;
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        spot_rest_base_url: base_url,
        ..CoinstoreGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "0.001");
    assert_eq!(response.fees[0].taker_rate, "0.0015");
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v2/public/config/spot/symbols");
}

#[tokio::test]
async fn coinstore_adapter_should_load_spot_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook_success")]).await;
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        spot_rest_base_url: base_url,
        ..CoinstoreGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(20),
        })
        .await
        .expect("book");
    assert_eq!(response.order_book.bids[0].price, 100.0);
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/v1/market/depth/BTCUSDT");
    assert_eq!(
        requests[0].query.get("depth").map(String::as_str),
        Some("20")
    );
}

#[tokio::test]
async fn coinstore_adapter_should_load_futures_contract_id_symbol_rules() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("futures_contracts_success")]).await;
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        futures_rest_base_url: base_url,
        ..CoinstoreGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![super::test_support::perp_symbol_scope()],
        })
        .await
        .expect("perp rules");
    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "12345");
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/api/configs/public");
}

#[test]
fn coinstore_public_adapter_should_register_capabilities() {
    let adapter = CoinstoreGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.exchange, exchange_id());
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_rest);
    assert_eq!(
        capabilities.supports_time_in_force,
        vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK]
    );
    assert!(capabilities
        .supports_order_types
        .contains(&OrderType::PostOnly));
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(capabilities.capabilities_v2.public_streams.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat_policy
            .stale_message_ms,
        180_000
    );
}

#[test]
fn coinstore_parser_fixtures_should_cover_success_empty_error_and_missing_fields() {
    let exchange = exchange_id();
    let spot_rules =
        super::parser::parse_spot_symbol_rules(&exchange, &fixture("spot_symbols_success"))
            .expect("spot rules");
    assert_eq!(spot_rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");

    let empty = super::parser::parse_spot_symbol_rules(&exchange, &fixture("empty"))
        .expect("empty symbol list");
    assert!(empty.is_empty());

    let missing = super::parser::parse_spot_symbol_rules(&exchange, &fixture("missing_symbol"))
        .expect_err("missing symbol should fail");
    assert!(format!("{missing:?}").contains("COINSTORE_PARSE"));

    let response_error = super::transport::classify_coinstore_error_for_test(
        Some(401),
        fixture("error")
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default(),
    );
    assert_eq!(
        response_error,
        rustcta_types::ExchangeErrorClass::Authentication
    );
}

fn fixture(name: &str) -> serde_json::Value {
    serde_json::from_str(match name {
        "spot_symbols_success" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/coinstore/spot_symbols_success.json"
            )
        }
        "futures_contracts_success" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/coinstore/futures_contracts_success.json"
            )
        }
        "orderbook_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/orderbook_success.json")
        }
        "empty" => include_str!("../../../../../tests/fixtures/exchanges/coinstore/empty.json"),
        "error" => include_str!("../../../../../tests/fixtures/exchanges/coinstore/error.json"),
        "missing_symbol" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/missing_symbol.json")
        }
        _ => panic!("unknown coinstore fixture {name}"),
    })
    .expect("fixture json")
}
