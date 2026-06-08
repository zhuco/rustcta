use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PrivateStreamKind, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{parse_market_metadata, parse_position_symbols};
use super::private_parser::private_event_kind;
use super::signing::{
    bearer_header, fixture_signature_digest, paradex_auth_canonical_payload,
    paradex_order_canonical_payload,
};
use super::streams::{
    paradex_heartbeat_policy_ms, paradex_ping_payload, paradex_private_unsubscribe_payload,
    paradex_public_subscribe_payload, paradex_public_unsubscribe_payload,
};
use super::test_support::{context, exchange_id, symbol};
use super::{ParadexGatewayAdapter, ParadexGatewayConfig};

#[test]
fn paradex_capabilities_should_keep_private_writes_offline_only_without_credentials() {
    let adapter = ParadexGatewayAdapter::new(ParadexGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Perpetual, MarketType::Option]
    );
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_batch_place_order);
}

#[test]
fn paradex_signing_vectors_should_be_secret_free_and_deterministic() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paradex/signing_vectors/auth_stark_fixture.json"
    ))
    .expect("vector");
    let payload = paradex_auth_canonical_payload(
        vector["account"].as_str().unwrap(),
        vector["timestamp_ms"].as_str().unwrap(),
        vector["expiration_ms"].as_str().unwrap(),
    );
    assert_eq!(payload, vector["canonical_payload"]);
    assert_eq!(
        fixture_signature_digest("fixture-stark-key-label", &payload),
        vector["fixture_digest"]
    );
    assert_eq!(bearer_header("fixture.jwt").unwrap(), "Bearer fixture.jwt");
}

#[test]
fn paradex_option_and_perp_market_metadata_should_parse_from_fixture() {
    let markets: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paradex/markets.json"
    ))
    .expect("markets fixture");
    let parsed = parse_market_metadata(&markets);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].market_kind, "PERP");
    assert_eq!(parsed[1].market_kind, "OPTION");
    assert_eq!(parsed[1].option_kind.as_deref(), Some("CALL"));
}

#[test]
fn paradex_private_position_fixture_should_remain_adapter_specific() {
    let positions: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paradex/positions.json"
    ))
    .expect("positions fixture");
    assert_eq!(parse_position_symbols(&positions), vec!["BTC-USD-PERP"]);
}

#[test]
fn paradex_websocket_helpers_should_build_json_rpc_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(MarketType::Perpetual, "BTC-USD-PERP"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = paradex_public_subscribe_payload(&subscription);
    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["params"]["channel"], "order_book.BTC-USD-PERP");
    assert_eq!(
        paradex_public_unsubscribe_payload(&subscription)["method"],
        "unsubscribe"
    );
    assert_eq!(
        paradex_private_unsubscribe_payload(&PrivateStreamKind::Orders).unwrap()["params"]
            ["channel"],
        "user.orders"
    );
    assert_eq!(paradex_ping_payload()["method"], "ping");
    assert_eq!(paradex_heartbeat_policy_ms(), (30_000, 10_000, 60_000));
    let event: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paradex/ws/order_update.json"
    ))
    .expect("order update fixture");
    assert_eq!(private_event_kind(&event), Some("user.orders"));
}

#[tokio::test]
async fn paradex_place_order_should_validate_market_then_return_signing_boundary() {
    let adapter = ParadexGatewayAdapter::new(ParadexGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Perpetual, "BTC-USD-PERP"),
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
    let payload = paradex_order_canonical_payload(
        "BTC-USD-PERP",
        "BUY",
        "LIMIT",
        "0.01",
        Some("65000"),
        "1700000000000000000",
    );
    assert!(payload.contains("paradex.order"));
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "paradex.place_order_stark_signature_offline_only"
        }
    ));
    assert_eq!(exchange_id().as_str(), "paradex");
}
