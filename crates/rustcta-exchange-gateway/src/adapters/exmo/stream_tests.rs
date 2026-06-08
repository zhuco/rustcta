use rustcta_exchange_api::{
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::streams::{
    exmo_pong_response, exmo_private_subscription_spec, exmo_public_subscription_spec,
};
use super::test_support::{context, exchange_id, symbol_scope};

#[test]
fn exmo_public_stream_spec_should_build_subscribe_payload() {
    let spec = exmo_public_subscription_spec(
        "wss://ws-api.exmo.com/v1/public",
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        },
    )
    .expect("public spec");

    assert_eq!(spec.url, "wss://ws-api.exmo.com/v1/public");
    assert_eq!(spec.channel, "spot/order_book_updates:BTC_USD");
    assert_eq!(spec.subscribe_payload["method"], "subscribe");
    assert_eq!(
        spec.subscribe_payload["topics"][0],
        "spot/order_book_updates:BTC_USD"
    );
}

#[test]
fn exmo_private_stream_spec_should_build_login_and_subscription_payloads() {
    let spec = exmo_private_subscription_spec(
        "wss://ws-api.exmo.com/v1/private",
        &PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            account_id: context("private-ws").account_id.unwrap(),
            market_type: Some(MarketType::Spot),
            kind: PrivateStreamKind::Fills,
        },
        "api_key_change_me",
        "api_secret_change_me",
        "1710000000",
    )
    .expect("private spec");

    assert_eq!(spec.channel, "spot/user_trades");
    assert_eq!(spec.auth_payload.as_ref().unwrap()["method"], "login");
    assert_eq!(
        spec.auth_payload.as_ref().unwrap()["sign"],
        "LhRiS3AO7QdKucUIpsV7OqaCd3R58uiA2DkdLmiy7qRtChIStoYeY3z5k8ZJbmzME7AP2gF7vepuyz3h6SZxtw=="
    );
    assert_eq!(spec.subscribe_payload["topics"][0], "spot/user_trades");
}

#[test]
fn exmo_stream_ping_should_generate_pong() {
    let pong = exmo_pong_response(&json!({"id": 9, "method": "ping"})).expect("pong");
    assert_eq!(pong, json!({"id": 9, "method": "pong"}));
}
