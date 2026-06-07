use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderStatus};
use serde_json::json;

use super::streams::{
    ascendex_private_stream_capabilities, auth_payload, parse_private_stream_message,
    parse_public_stream_message, ping_payload, pong_payload, private_subscribe_payload,
    public_subscribe_payload, AscendexPrivateStreamMessage, AscendexPublicStreamMessage,
};
use super::test_support::{
    context, exchange_id, fixture_json, perp_symbol_scope, spot_symbol_scope,
};
use super::{AscendexGatewayAdapter, AscendexGatewayConfig};

#[test]
fn ascendex_public_stream_payloads_should_map_standard_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("depth"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = public_subscribe_payload(&subscription, Some("abc123")).expect("payload");
    assert_eq!(payload["op"], "sub");
    assert_eq!(payload["id"], "abc123");
    assert_eq!(payload["ch"], "depth:BTC/USDT");

    let candle = PublicStreamSubscription {
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
        ..subscription
    };
    let payload = public_subscribe_payload(&candle, None).expect("payload");
    assert_eq!(payload["ch"], "bar:1m:BTC/USDT");
}

#[test]
fn ascendex_private_stream_payloads_should_auth_and_map_channels() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let auth = auth_payload("key", "secret", MarketType::Perpetual, Some("auth1")).expect("auth");
    assert_eq!(auth["op"], "auth");
    assert_eq!(auth["id"], "auth1");
    assert_eq!(auth["key"], "key");
    assert!(auth["sig"].as_str().is_some_and(|sig| !sig.is_empty()));

    let payload = private_subscribe_payload(&subscription, MarketType::Perpetual, Some("sub1"))
        .expect("payload");
    assert_eq!(payload["op"], "sub");
    assert_eq!(payload["ch"], "futures-order");
}

#[test]
fn ascendex_stream_heartbeat_payloads_should_match_protocol() {
    assert_eq!(ping_payload(None), json!({ "op": "ping" }));
    assert_eq!(
        ping_payload(Some("p1")),
        json!({ "op": "ping", "id": "p1" })
    );
    assert_eq!(pong_payload(), json!({ "op": "pong" }));
}

#[test]
fn ascendex_private_stream_capabilities_should_match_current_parser_scope() {
    let capabilities = ascendex_private_stream_capabilities(true);
    assert!(capabilities.supports_orders);
    assert!(capabilities.supports_fills);
    assert!(!capabilities.supports_balances);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_account);
}

#[test]
fn ascendex_public_stream_parser_should_parse_depth_and_heartbeat() {
    let heartbeat = parse_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({"m": "ping", "hp": 3}),
    )
    .expect("heartbeat");
    assert_eq!(heartbeat, AscendexPublicStreamMessage::Heartbeat);

    let book = parse_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &fixture_json("ws_public_depth.json"),
    )
    .expect("book");
    let AscendexPublicStreamMessage::OrderBook(book) = book else {
        panic!("expected order book");
    };
    assert_eq!(book.order_book.sequence, Some(99));
    assert_eq!(book.order_book.bids[0].quantity, 2.0);
}

#[test]
fn ascendex_private_stream_parser_should_parse_order_update() {
    let event = parse_private_stream_message(
        &exchange_id(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &fixture_json("ws_private_order.json"),
    )
    .expect("event");
    let AscendexPrivateStreamMessage::Events(events) = event else {
        panic!("expected events");
    };
    let rustcta_exchange_api::ExchangeStreamEvent::OrderUpdate(order) = &events[0] else {
        panic!("expected order update");
    };
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(order.exchange_order_id.as_deref(), Some("OID1"));
}

#[tokio::test]
async fn ascendex_adapter_should_ack_public_and_private_stream_specs() {
    let adapter = AscendexGatewayAdapter::new(AscendexGatewayConfig {
        account_group: Some("42".to_string()),
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..AscendexGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public subscription");
    assert!(public_id.contains("depth:BTC/USDT"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Account,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("futures-account-update"));
}
