use rustcta_exchange_api::{
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::streams::{
    parse_control_message, parse_public_order_book_event, private_subscription_spec,
    public_subscription_spec,
};
use super::test_support::{context, exchange_id, perp_symbol_scope};
use super::BitmartGatewayConfig;

#[test]
fn bitmart_stream_specs_should_build_public_and_private_payloads() {
    let config = BitmartGatewayConfig {
        api_key: Some("bitmart-key".to_string()),
        api_secret: Some("bitmart-secret".to_string()),
        memo: Some("bitmart-memo".to_string()),
        ..BitmartGatewayConfig::default()
    };
    let public = public_subscription_spec(
        &config,
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        },
    )
    .expect("public spec");
    assert_eq!(public.payload["op"], "subscribe");
    assert_eq!(public.heartbeat_payload, json!("ping"));

    let private = private_subscription_spec(
        &config,
        &PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: rustcta_types::AccountId::new("account_a").expect("account"),
            kind: PrivateStreamKind::Orders,
        },
    )
    .expect("private spec");
    assert_eq!(private.payload["op"], "login");
    assert!(private.payload["subscribe"]["args"][0]
        .as_str()
        .expect("channel")
        .contains("order"));
}

#[test]
fn bitmart_stream_parser_should_parse_orderbook_and_control_messages() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmart/ws_public_depth.json"
    ))
    .expect("fixture");
    let snapshot = parse_public_order_book_event(&exchange_id(), MarketType::Perpetual, &fixture)
        .expect("parse")
        .expect("snapshot");
    assert_eq!(snapshot.best_bid().expect("bid").price, 50000.0);
    assert_eq!(parse_control_message(&json!("pong")), Some("pong"));
}
