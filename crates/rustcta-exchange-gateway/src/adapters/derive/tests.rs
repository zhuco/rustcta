use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PrivateStreamKind, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{parse_instrument_metadata, parse_subaccount_position_names};
use super::private_parser::{private_event_channel, private_rpc_method};
use super::signing::{
    derive_fixture_hmac, derive_json_rpc_payload, derive_session_canonical_payload,
};
use super::streams::{
    derive_heartbeat_policy_ms, derive_login_payload, derive_ping_payload,
    derive_private_unsubscribe_payload, derive_public_subscribe_payload,
    derive_public_unsubscribe_payload,
};
use super::test_support::{context, exchange_id, symbol};
use super::{DeriveGatewayAdapter, DeriveGatewayConfig};

#[test]
fn derive_capabilities_should_keep_trade_runtime_closed_without_session_keys() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Option, MarketType::Perpetual]
    );
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
}

#[test]
fn derive_session_signing_vector_should_match_fixture() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/signing_vectors/session_order_hmac.json"
    ))
    .expect("vector");
    let payload = derive_session_canonical_payload(
        vector["method"].as_str().unwrap(),
        vector["subaccount_id"].as_str().unwrap(),
        vector["timestamp_ms"].as_str().unwrap(),
        vector["params_json"].as_str().unwrap(),
    );
    assert_eq!(payload, vector["canonical_payload"]);
    assert_eq!(
        derive_fixture_hmac("fixture-session-material", &payload),
        vector["fixture_signature"]
    );
}

#[test]
fn derive_option_and_perp_metadata_should_parse_from_fixture() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/instruments.json"
    ))
    .expect("instruments fixture");
    let parsed = parse_instrument_metadata(&instruments);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].instrument_type, "perp");
    assert_eq!(parsed[1].instrument_type, "option");
    assert_eq!(parsed[1].option_type.as_deref(), Some("call"));
}

#[test]
fn derive_positions_fixture_should_capture_subaccount_model() {
    let positions: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/positions.json"
    ))
    .expect("positions fixture");
    assert_eq!(
        parse_subaccount_position_names(&positions),
        vec!["BTC-PERP"]
    );
}

#[test]
fn derive_json_rpc_and_ws_helpers_should_build_payloads() {
    let rpc = derive_json_rpc_payload("public/get_all_instruments", r#"{"currency":"BTC"}"#, 7);
    assert!(rpc.contains(r#""method":"public/get_all_instruments""#));
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(MarketType::Perpetual, "BTC-PERP"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = derive_public_subscribe_payload(&subscription);
    assert_eq!(payload["params"]["channels"][0], "orderbook.BTC-PERP.100");
    assert_eq!(
        derive_public_unsubscribe_payload(&subscription)["method"],
        "unsubscribe"
    );
    assert_eq!(
        derive_private_unsubscribe_payload(&PrivateStreamKind::Orders).unwrap()["params"]
            ["channels"][0],
        "private.orders"
    );
    assert_eq!(
        derive_login_payload("fixture-wallet", "fixture-session-key", "fixture-signature")
            ["method"],
        "public/login"
    );
    assert_eq!(derive_ping_payload()["method"], "public/ping");
    assert_eq!(derive_heartbeat_policy_ms(), (30_000, 10_000, 60_000));
    let event: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/ws/private_order_update.json"
    ))
    .expect("private order event fixture");
    assert_eq!(private_rpc_method(&event), Some("subscription"));
    assert_eq!(private_event_channel(&event), Some("private.orders"));
}

#[tokio::test]
async fn derive_place_order_should_validate_market_then_return_session_boundary() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Perpetual, "BTC-PERP"),
        client_order_id: Some("fixture-client-order-id".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "derive.place_order_session_signature_offline_only"
        }
    ));
    assert_eq!(exchange_id().as_str(), "derive");
}
