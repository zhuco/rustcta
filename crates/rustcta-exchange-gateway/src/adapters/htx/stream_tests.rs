use rustcta_exchange_api::{
    ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};

use super::streams::{
    parse_private_stream_message, parse_public_stream_message, private_subscribe_payload,
    private_ws_auth_payload, private_ws_auth_payload_for_url, public_subscribe_payload,
    HtxPrivateStreamMessage, HtxPublicStreamMessage,
};
use super::test_support::{context, exchange_id, load_fixture, perp_symbol};

#[test]
fn htx_public_stream_payloads_should_map_depth_channels() {
    let payload = public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: perp_symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("payload");

    assert_eq!(payload["sub"], "market.BTC-USDT.depth.size_20.high_freq");
}

#[test]
fn htx_private_ws_login_payload_should_sign_auth_instruction() {
    let payload = private_ws_auth_payload("test-key", "test-secret", Some("2026-06-08T00:00:00"))
        .expect("payload");

    assert_eq!(payload["op"], "auth");
    assert_eq!(payload["AccessKeyId"], "test-key");
    assert!(payload["Signature"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    let subscribe = private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    })
    .expect("subscribe");
    assert_eq!(subscribe["topic"], "orders_cross.*");
}

#[test]
fn htx_spot_private_ws_payloads_should_use_v21_auth_and_spot_topics() {
    let payload = private_ws_auth_payload_for_url(
        "test-key",
        "test-secret",
        "wss://api.huobi.pro/ws/v2",
        MarketType::Spot,
        Some("2026-06-08T00:00:00"),
    )
    .expect("payload");

    assert_eq!(payload["action"], "req");
    assert_eq!(payload["ch"], "auth");
    assert_eq!(payload["params"]["accessKey"], "test-key");
    assert_eq!(payload["params"]["signatureVersion"], "2.1");
    assert!(payload["params"]["signature"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    let subscribe = private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    })
    .expect("subscribe");
    assert_eq!(subscribe["action"], "sub");
    assert_eq!(subscribe["ch"], "orders#*");
}

#[test]
fn htx_stream_parser_should_parse_book_order_and_control_messages() {
    let book = parse_public_stream_message(
        &exchange_id(),
        perp_symbol(),
        &load_fixture("ws/public_book.json"),
    )
    .expect("book");
    assert!(matches!(book, HtxPublicStreamMessage::OrderBook(_)));

    let order = parse_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(perp_symbol()),
        &load_fixture("ws/private_order.json"),
    )
    .expect("order");
    match order {
        HtxPrivateStreamMessage::Events(events) => {
            assert!(matches!(events[0], ExchangeStreamEvent::OrderUpdate(_)));
        }
        other => panic!("expected events, got {other:?}"),
    }

    let pong = parse_public_stream_message(
        &exchange_id(),
        perp_symbol(),
        &serde_json::json!({ "ping": 1700000000000_i64 }),
    )
    .expect("pong");
    assert!(matches!(pong, HtxPublicStreamMessage::Pong(1700000000000)));
}
