use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, EXCHANGE_API_SCHEMA_VERSION,
};

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::private::{
    build_cancel_order_request_spec, build_get_balances_request_spec,
    build_place_order_request_spec,
};
use super::private_parser::parse_order_ack;
use super::test_support::{context, exchange_id, symbol_scope};
use super::HollaexGatewayAdapter;

fn load_request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/get_balances.json"
        ),
        "place_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/place_order.json"
        ),
        "cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/cancel_order.json"
        ),
        _ => panic!("unknown hollaex request spec {name}"),
    };
    serde_json::from_str(text).expect("request spec")
}

fn load_signing_vector(name: &str) -> SigningVector {
    let text = match name {
        "get_balances_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/get_balances_hmac_sha256.json"
        ),
        "place_order_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/place_order_hmac_sha256.json"
        ),
        "cancel_order_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/cancel_order_hmac_sha256.json"
        ),
        "ws_connect_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/ws_connect_hmac_sha256.json"
        ),
        _ => panic!("unknown hollaex signing vector {name}"),
    };
    serde_json::from_str(text).expect("signing vector")
}

fn load_private_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "order_ack.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/hollaex/parser/order_ack.json")
        }
        _ => panic!("unknown hollaex private parser fixture {name}"),
    };
    serde_json::from_str(text).expect("private parser fixture")
}

#[tokio::test]
async fn hollaex_adapter_should_keep_private_operations_unsupported() {
    let adapter = HollaexGatewayAdapter::default_public().expect("adapter");
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
fn hollaex_request_specs_should_match_offline_private_builders() {
    let balances = build_get_balances_request_spec("test-key", "test-secret", 1_710_000_000)
        .expect("balances request");
    load_request_spec("get_balances.json")
        .assert_matches(&balances.actual_http_request())
        .expect("balances request spec");

    let place = build_place_order_request_spec("test-key", "test-secret", 1_710_000_000)
        .expect("place request");
    load_request_spec("place_order.json")
        .assert_matches(&place.actual_http_request())
        .expect("place request spec");

    let cancel = build_cancel_order_request_spec("test-key", "test-secret", 1_710_000_000)
        .expect("cancel request");
    load_request_spec("cancel_order.json")
        .assert_matches(&cancel.actual_http_request())
        .expect("cancel request spec");
}

#[test]
fn hollaex_signing_vectors_should_verify() {
    for fixture in [
        "get_balances_hmac_sha256.json",
        "place_order_hmac_sha256.json",
        "cancel_order_hmac_sha256.json",
        "ws_connect_hmac_sha256.json",
    ] {
        load_signing_vector(fixture).verify().expect(fixture);
    }
}

#[test]
fn hollaex_private_parser_should_parse_order_ack_fixture() {
    let order = parse_order_ack(
        &exchange_id(),
        symbol_scope(),
        &load_private_fixture("order_ack.json"),
    )
    .expect("order ack");
    assert_eq!(order.exchange_order_id.as_deref(), Some("order-fixture-1"));
    assert_eq!(order.quantity, "0.1");
    assert_eq!(order.filled_quantity, "0");
}
