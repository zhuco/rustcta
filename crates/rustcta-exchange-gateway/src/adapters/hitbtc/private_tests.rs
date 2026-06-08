use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, EXCHANGE_API_SCHEMA_VERSION,
};

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::private::{
    build_cancel_order_request_spec, build_open_orders_request_spec,
    build_place_order_request_spec, build_recent_fills_request_spec,
};
use super::private_parser::parse_spot_order_ack;
use super::test_support::{context, exchange_id, symbol_scope};
use super::HitbtcGatewayAdapter;

fn load_request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "place_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/request_specs/place_order.json"
        ),
        "cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/request_specs/cancel_order.json"
        ),
        "get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/request_specs/get_open_orders.json"
        ),
        "get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/request_specs/get_recent_fills.json"
        ),
        _ => panic!("unknown hitbtc request spec {name}"),
    };
    serde_json::from_str(text).expect("request spec")
}

fn load_signing_vector(name: &str) -> SigningVector {
    let text = match name {
        "place_order_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/signing_vectors/place_order_hs256.json"
        ),
        "cancel_order_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/signing_vectors/cancel_order_hs256.json"
        ),
        "get_open_orders_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/signing_vectors/get_open_orders_hs256.json"
        ),
        "get_recent_fills_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/signing_vectors/get_recent_fills_hs256.json"
        ),
        "ws_login_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hitbtc/signing_vectors/ws_login_hs256.json"
        ),
        _ => panic!("unknown hitbtc signing vector {name}"),
    };
    serde_json::from_str(text).expect("signing vector")
}

fn load_parser_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "spot_order_ack.json" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/hitbtc/parser/spot_order_ack.json"
            )
        }
        _ => panic!("unknown hitbtc parser fixture {name}"),
    };
    serde_json::from_str(text).expect("parser fixture")
}

#[tokio::test]
async fn hitbtc_adapter_should_keep_private_operations_unsupported() {
    let adapter = HitbtcGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn hitbtc_request_specs_should_match_offline_private_builders() {
    let place =
        build_place_order_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("place request");
    load_request_spec("place_order.json")
        .assert_matches(&place.actual_http_request())
        .expect("place request spec");

    let cancel =
        build_cancel_order_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("cancel request");
    load_request_spec("cancel_order.json")
        .assert_matches(&cancel.actual_http_request())
        .expect("cancel request spec");

    let open_orders =
        build_open_orders_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("open orders request");
    load_request_spec("get_open_orders.json")
        .assert_matches(&open_orders.actual_http_request())
        .expect("open orders request spec");

    let recent_fills =
        build_recent_fills_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("recent fills request");
    load_request_spec("get_recent_fills.json")
        .assert_matches(&recent_fills.actual_http_request())
        .expect("recent fills request spec");
}

#[test]
fn hitbtc_signing_vectors_should_verify() {
    for fixture in [
        "place_order_hs256.json",
        "cancel_order_hs256.json",
        "get_open_orders_hs256.json",
        "get_recent_fills_hs256.json",
        "ws_login_hs256.json",
    ] {
        load_signing_vector(fixture).verify().expect(fixture);
    }
}

#[test]
fn hitbtc_private_parser_should_parse_order_ack_fixture() {
    let order = parse_spot_order_ack(
        &exchange_id(),
        symbol_scope(),
        &load_parser_fixture("spot_order_ack.json"),
    )
    .expect("order ack");
    assert_eq!(order.exchange_order_id.as_deref(), Some("840450210"));
    assert_eq!(order.client_order_id.as_deref(), Some("cli-hitbtc-1"));
    assert_eq!(order.quantity, "0.063");
    assert_eq!(order.filled_quantity, "0.000");
}
