use rustcta_exchange_api::{
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};

use super::streams::{
    bitbns_ping_payload, bitbns_public_socket_url, bitbns_public_subscribe_payload,
    bitbns_reconnect_policy_ms, parse_socket_orderbook_news,
};
use super::test_support::{context, symbol_scope};

#[test]
fn bitbns_socket_helpers_should_build_spec_only_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope("BTC_INR"),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = bitbns_public_subscribe_payload(&subscription).expect("payload");
    assert_eq!(payload["event"], "switchRoom");
    assert_eq!(payload["room"], "news_BTC");
    assert_eq!(
        bitbns_public_socket_url("INR", true),
        "https://wsinrmv2.bitbns.com/"
    );
    assert_eq!(
        bitbns_public_socket_url("USDT", false),
        "https://wsusdtmv2.bitbns.com/?withTicker=true&onlyTicker=true"
    );
    assert_eq!(bitbns_ping_payload()["event"], "ping");
    assert_eq!(bitbns_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn bitbns_socket_parser_fixture_should_decode_news_payload() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbns/ws/orderbook_news_sell.json"
    ))
    .expect("ws fixture");
    let parsed = parse_socket_orderbook_news(&fixture)
        .expect("parse")
        .expect("news");
    assert_eq!(parsed.as_array().expect("array")[0]["rate"], 65010.0);
    assert_eq!(parsed.as_array().expect("array")[0]["btc"], 0.4);
}
