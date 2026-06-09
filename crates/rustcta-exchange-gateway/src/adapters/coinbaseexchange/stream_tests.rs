use rustcta_exchange_api::{
    AccountId, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::streams::{
    coinbaseexchange_private_subscription_spec, coinbaseexchange_public_order_book_ws_policy,
    coinbaseexchange_public_orderbook_subscription_spec, coinbaseexchange_public_subscription_spec,
    parse_coinbaseexchange_public_stream_events,
};
use super::test_support::{context, exchange_id, symbol_scope};

#[test]
fn coinbaseexchange_public_stream_spec_should_use_exchange_channels() {
    let spec = coinbaseexchange_public_subscription_spec(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        },
        "wss://ws-feed.exchange.coinbase.com",
    )
    .expect("spec");
    assert_eq!(spec.url, "wss://ws-feed.exchange.coinbase.com");
    assert_eq!(spec.channel, "level2_batch");
    assert_eq!(spec.subscribe_payload["type"], "subscribe");
    assert_eq!(spec.subscribe_payload["channels"][0], "level2_batch");
    assert_eq!(spec.subscribe_payload["product_ids"][0], "BTC-USD");
}

#[test]
fn coinbaseexchange_orderbook_policy_should_document_batch_sequence_and_resync() {
    let policy = coinbaseexchange_public_order_book_ws_policy();
    assert_eq!(policy.public_channel, "level2_batch");
    assert_eq!(policy.public_interval_ms, 50);
    assert_eq!(policy.level2_channel, "level2");
    assert_eq!(policy.full_channel, "full");
    assert_eq!(policy.level3_channel, "level3");
    assert_eq!(
        policy.rest_snapshot_endpoint,
        "GET /products/{product_id}/book"
    );
    assert_eq!(policy.rest_snapshot_levels, [2, 3]);
    assert_eq!(policy.snapshot_sequence_field, "sequence");
    assert!(policy.level2_sequence.contains("do not carry a sequence"));
    assert!(policy
        .full_sequence
        .contains("sequence <= snapshot sequence"));
    assert!(policy.level3_sequence.contains("compact"));
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("/products/{product_id}/book"));
}

#[test]
fn coinbaseexchange_orderbook_payload_should_support_level2_batch_and_sequence_channels() {
    let default = coinbaseexchange_public_orderbook_subscription_spec(
        &symbol_scope(),
        "wss://ws-feed.exchange.coinbase.com",
        None,
    )
    .expect("default orderbook spec");
    assert_eq!(default.channel, "level2_batch");
    assert_eq!(default.subscribe_payload["channels"][0], "level2_batch");
    assert_eq!(default.subscribe_payload["product_ids"][0], "BTC-USD");

    let full = coinbaseexchange_public_orderbook_subscription_spec(
        &symbol_scope(),
        "wss://ws-feed.exchange.coinbase.com",
        Some("full"),
    )
    .expect("full orderbook spec");
    assert_eq!(full.channel, "full");
    assert_eq!(full.subscribe_payload["channels"][0], "full");
}

#[test]
fn coinbaseexchange_public_orderbook_parser_should_cover_level2_batch_fixtures() {
    let snapshot_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinbaseexchange/ws/public_level2_snapshot.json"
    ))
    .expect("snapshot fixture");
    let snapshot = parse_coinbaseexchange_public_stream_events(
        &exchange_id(),
        symbol_scope(),
        &snapshot_fixture,
    )
    .expect("snapshot events");
    match snapshot.first() {
        Some(ExchangeStreamEvent::OrderBookSnapshot(book)) => {
            assert_eq!(book.order_book.bids[0].price, 10101.10);
            assert_eq!(book.order_book.asks[0].quantity, 0.57753524);
            assert_eq!(book.order_book.sequence, Some(13051505638));
        }
        other => panic!("unexpected snapshot event {other:?}"),
    }

    let update_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinbaseexchange/ws/public_level2_batch_update.json"
    ))
    .expect("update fixture");
    let update = parse_coinbaseexchange_public_stream_events(
        &exchange_id(),
        symbol_scope(),
        &update_fixture,
    )
    .expect("update events");
    match update.first() {
        Some(ExchangeStreamEvent::OrderBookSnapshot(book)) => {
            assert_eq!(book.order_book.bids.len(), 1);
            assert_eq!(book.order_book.bids[0].price, 22356.30);
            assert!(book.order_book.asks.is_empty());
            assert!(book.order_book.exchange_timestamp.is_some());
        }
        other => panic!("unexpected update event {other:?}"),
    }
}

#[test]
fn coinbaseexchange_private_stream_spec_should_sign_user_channel() {
    let spec = coinbaseexchange_private_subscription_spec(
        &PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").unwrap(),
            kind: PrivateStreamKind::Orders,
        },
        "wss://ws-feed.exchange.coinbase.com",
        "key",
        "secret",
        "passphrase",
        "1700000000",
    )
    .expect("spec");
    assert_eq!(spec.channel, "user");
    assert_eq!(spec.subscribe_payload["key"], "key");
    assert_eq!(spec.subscribe_payload["passphrase"], "passphrase");
    assert_eq!(spec.subscribe_payload["timestamp"], "1700000000");
    assert!(spec.subscribe_payload["signature"].as_str().unwrap().len() > 20);
}
