use rustcta_exchange_api::{
    ExchangeClient, FeesRequest, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{OrangeXGatewayAdapter, OrangeXGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture_result(name: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/orangex/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json");
    value.get("result").cloned().unwrap_or(value)
}

#[tokio::test]
async fn orangex_adapter_should_load_spot_and_perp_symbol_rules_from_json_rpc() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "result": [{
                "instrument_name": "BTC-USDT-SPOT",
                "is_active": true,
                "kind": "spot",
                "maker_commission": "0.001",
                "min_trade_amount": "0.00001",
                "taker_commission": "0.001",
                "tick_size": "0.01",
                "instr_multiple": "0.00001"
            }]
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "2",
            "result": [{
                "instrument_name": "BTC-USDT-PERPETUAL",
                "is_active": true,
                "kind": "perpetual",
                "contract_size": "1",
                "min_trade_amount": "0.001",
                "tick_size": "0.1",
                "instr_multiple": "0.001"
            }]
        }),
    ])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope(), perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 2);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert!(response.rules[1].supports_reduce_only);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["method"],
        "/public/get_instruments"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["currency"],
        "SPOT"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["currency"],
        "USDT"
    );
}

#[test]
fn orangex_public_parsers_should_cover_fixture_success_empty_error_and_missing_field() {
    let exchange_id = super::test_support::exchange_id();

    let spot_rules = parse_symbol_rules(
        &exchange_id,
        MarketType::Spot,
        &fixture_result("spot_symbols.json"),
    )
    .expect("spot rules");
    assert_eq!(spot_rules.len(), 1);
    assert_eq!(spot_rules[0].base_asset, "BTC");

    let book = parse_orderbook_snapshot(
        &exchange_id,
        perp_symbol_scope(),
        &fixture_result("orderbook.json"),
    )
    .expect("orderbook");
    assert_eq!(book.sequence, Some(119365));

    let empty_rules = parse_symbol_rules(
        &exchange_id,
        MarketType::Spot,
        &fixture_result("empty_instruments.json"),
    )
    .expect("empty rules");
    assert!(empty_rules.is_empty());

    let missing_field = parse_symbol_rules(
        &exchange_id,
        MarketType::Spot,
        &fixture_result("missing_field_instrument.json"),
    )
    .expect_err("missing instrument_name");
    assert!(matches!(
        missing_field,
        rustcta_exchange_api::ExchangeApiError::Exchange(_)
    ));

    let error_fixture = fixture_result("error.json");
    assert_eq!(error_fixture["error"]["code"], 10001);
}

#[tokio::test]
async fn orangex_adapter_should_load_orderbook_snapshot_from_json_rpc() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "jsonrpc": "2.0",
        "id": "1",
        "result": {
            "asks": [["100.5", "2"], ["101", "3"]],
            "bids": [["99.5", "4"], ["99", "5"]],
            "timestamp": "1606443462147",
            "version": 119365
        }
    })])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(101),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[0].quantity, 2.0);
    assert_eq!(response.order_book.sequence, Some(119365));
    assert!(response.order_book.exchange_timestamp.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.body.as_ref().unwrap()["method"],
        "/public/get_order_book"
    );
    assert_eq!(
        request.body.as_ref().unwrap()["params"]["instrument_name"],
        "BTC-USDT-PERPETUAL"
    );
    assert_eq!(request.body.as_ref().unwrap()["params"]["depth"], 100);
}

#[test]
fn orangex_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["orangex"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[tokio::test]
async fn orangex_adapter_should_read_fee_rates_from_public_instruments() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "jsonrpc": "2.0",
        "id": "1",
        "result": [{
            "instrument_name": "BTC-USDT-SPOT",
            "kind": "spot",
            "maker_commission": "0.0008",
            "taker_commission": "0.001",
            "tick_size": "0.01",
            "instr_multiple": "0.00001"
        }]
    })])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        ..OrangeXGatewayConfig::default()
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
    assert_eq!(response.fees[0].maker_rate, "0.0008");
    assert_eq!(response.fees[0].taker_rate, "0.001");
    assert_eq!(
        seen.lock().unwrap()[0].body.as_ref().unwrap()["method"],
        "/public/get_instruments"
    );
}
