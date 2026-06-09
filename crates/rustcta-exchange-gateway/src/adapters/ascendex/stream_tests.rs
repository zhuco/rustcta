use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderStatus};
use serde_json::json;

use super::streams::{
    ascendex_check_seqnum_continuity, ascendex_depth_snapshot_request,
    ascendex_depth_snapshot_top100_request, ascendex_private_stream_capabilities,
    ascendex_public_order_book_ws_policy, auth_payload, parse_private_stream_message,
    parse_public_stream_message, ping_payload, pong_payload, private_subscribe_payload,
    public_subscribe_payload, AscendexPrivateStreamMessage, AscendexPublicStreamMessage,
    AscendexSeqnumContinuity,
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

    let ticker = PublicStreamSubscription {
        kind: PublicStreamKind::Ticker,
        ..subscription.clone()
    };
    let payload = public_subscribe_payload(&ticker, None).expect("payload");
    assert_eq!(payload["ch"], "bbo:BTC/USDT");

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
fn ascendex_public_order_book_policy_should_describe_depth_bbo_and_resync() {
    let policy = ascendex_public_order_book_ws_policy();
    assert_eq!(policy.depth_channel_template, "depth:{symbol}");
    assert_eq!(policy.bbo_channel_template, "bbo:{symbol}");
    assert_eq!(policy.depth_interval_ms, 300);
    assert_eq!(policy.bbo_interval, "on_change");
    assert_eq!(policy.snapshot_action, "depth-snapshot");
    assert_eq!(policy.snapshot_top100_action, "depth-snapshot-top100");
    assert_eq!(policy.snapshot_max_levels, 500);
    assert_eq!(policy.snapshot_top100_levels, 100);
    assert_eq!(policy.sequence_field, "seqnum");
    assert_eq!(policy.checksum, None);
}

#[test]
fn ascendex_public_depth_snapshot_requests_should_encode_snapshot_actions() {
    let snapshot =
        ascendex_depth_snapshot_request(&spot_symbol_scope(), Some("snap1")).expect("snapshot");
    assert_eq!(snapshot["op"], "req");
    assert_eq!(snapshot["id"], "snap1");
    assert_eq!(snapshot["action"], "depth-snapshot");
    assert_eq!(snapshot["args"]["symbol"], "BTC/USDT");

    let top100 =
        ascendex_depth_snapshot_top100_request(&spot_symbol_scope(), None).expect("top100");
    assert_eq!(top100["op"], "req");
    assert_eq!(top100["id"], "depth-snapshot-top100");
    assert_eq!(top100["action"], "depth-snapshot-top100");
    assert_eq!(top100["args"]["symbol"], "BTC/USDT");
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

    let top100 = parse_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({
            "m": "depth-snapshot-top100",
            "symbol": "BTC/USDT",
            "data": {
                "seqnum": 100,
                "ts": 1700000000000_i64,
                "asks": [["100.6", "1.0"]],
                "bids": [["99.4", "0.9"]]
            }
        }),
    )
    .expect("top100 snapshot");
    let AscendexPublicStreamMessage::OrderBook(top100) = top100 else {
        panic!("expected top100 order book");
    };
    assert_eq!(top100.order_book.sequence, Some(100));
}

#[test]
fn ascendex_public_stream_parser_should_parse_bbo() {
    let bbo = parse_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &fixture_json("ws_public_bbo.json"),
    )
    .expect("bbo");
    let AscendexPublicStreamMessage::Bbo(bbo) = bbo else {
        panic!("expected bbo");
    };
    assert_eq!(bbo.symbol.exchange_symbol.symbol, "BTC/USDT");
    assert_eq!(bbo.bid_price, "99.9");
    assert_eq!(bbo.bid_quantity, "0.8");
    assert_eq!(bbo.ask_price, "100.1");
    assert_eq!(bbo.ask_quantity, "1.2");
}

#[test]
fn ascendex_seqnum_continuity_should_detect_gaps_and_regressions() {
    assert_eq!(
        ascendex_check_seqnum_continuity(None, 99),
        AscendexSeqnumContinuity::First
    );
    assert_eq!(
        ascendex_check_seqnum_continuity(Some(99), 100),
        AscendexSeqnumContinuity::Continuous
    );

    let gap = ascendex_check_seqnum_continuity(Some(100), 103);
    assert_eq!(
        gap,
        AscendexSeqnumContinuity::Gap {
            expected: 101,
            actual: 103
        }
    );
    assert!(gap.requires_resync());

    let regression = ascendex_check_seqnum_continuity(Some(100), 100);
    assert_eq!(
        regression,
        AscendexSeqnumContinuity::NotIncreasing {
            previous: 100,
            actual: 100
        }
    );
    assert!(regression.requires_resync());
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
