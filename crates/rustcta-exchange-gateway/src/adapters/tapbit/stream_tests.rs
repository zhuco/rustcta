use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::{json, Value};

use super::streams::{
    parse_tapbit_public_stream_events, parse_tapbit_public_stream_message, tapbit_heartbeat_spec,
    tapbit_order_book_stream_spec, tapbit_public_subscribe_payload,
    tapbit_public_unsubscribe_payload, tapbit_stream_reconnect_policy, tapbit_ws_pong_payload,
    TapbitPublicStreamMessage, TAPBIT_WS_MAX_BOOK_DEPTH, TAPBIT_WS_MAX_MISSED_PINGS,
    TAPBIT_WS_PING_INTERVAL_SECONDS, TAPBIT_WS_PONG_PAYLOAD,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{TapbitGatewayAdapter, TapbitGatewayConfig};

#[tokio::test]
async fn tapbit_adapter_should_build_public_websocket_subscription_specs() {
    let adapter = TapbitGatewayAdapter::default_public().expect("adapter");
    let spot = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-ws-book"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let perp = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-ws-ticker"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    };

    assert!(adapter.capabilities().supports_public_streams);
    assert_eq!(adapter.capabilities().max_order_book_depth, Some(200));
    assert_eq!(
        tapbit_public_subscribe_payload(&spot).expect("spot payload"),
        json!({"op": "subscribe", "args": ["spot/orderBook.BTCUSDT.200"]})
    );
    assert_eq!(
        tapbit_public_unsubscribe_payload(&spot).expect("spot unsubscribe"),
        json!({"op": "unsubscribe", "args": ["spot/orderBook.BTCUSDT.200"]})
    );
    assert_eq!(
        tapbit_public_subscribe_payload(&perp).expect("perp payload"),
        json!({"op": "subscribe", "args": ["usdt/ticker.BTC-SWAP"]})
    );

    let spec = adapter
        .subscribe_public_stream(spot)
        .await
        .expect("public ws spec");
    let spec: Value = serde_json::from_str(&spec).expect("json spec");
    assert_eq!(spec["url"], json!("wss://ws-openapi.tapbit.com/stream/ws"));
    assert_eq!(
        spec["payload"],
        json!({"op": "subscribe", "args": ["spot/orderBook.BTCUSDT.200"]})
    );
    assert_eq!(spec["heartbeat"], tapbit_heartbeat_spec());
    assert_eq!(spec["order_book"], tapbit_order_book_stream_spec());
    assert_eq!(
        spec["order_book"]["supported_depths"],
        json!([5, 10, 50, 100, 200])
    );
    assert_eq!(spec["order_book"]["sequence_field"], json!("version"));
}

#[tokio::test]
async fn tapbit_adapter_should_use_configured_public_ws_url() {
    let adapter = TapbitGatewayAdapter::new(TapbitGatewayConfig {
        public_ws_url: "wss://example.test/tapbit".to_string(),
        ..TapbitGatewayConfig::default()
    })
    .expect("adapter");
    let spec = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-ws-ticker"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public ws spec");
    let spec: Value = serde_json::from_str(&spec).expect("json spec");
    assert_eq!(spec["url"], json!("wss://example.test/tapbit"));
    assert_eq!(
        spec["payload"],
        json!({"op": "subscribe", "args": ["spot/ticker.BTCUSDT"]})
    );
}

#[test]
fn tapbit_adapter_should_document_websocket_heartbeat_contract() {
    let heartbeat = tapbit_heartbeat_spec();
    assert_eq!(TAPBIT_WS_PING_INTERVAL_SECONDS, 5);
    assert_eq!(TAPBIT_WS_MAX_MISSED_PINGS, 2);
    assert_eq!(TAPBIT_WS_MAX_BOOK_DEPTH, 200);
    assert_eq!(TAPBIT_WS_PONG_PAYLOAD, "pong");
    assert_eq!(heartbeat["server_ping_interval_seconds"], json!(5));
    assert_eq!(heartbeat["max_missed_server_pings"], json!(2));
    assert_eq!(heartbeat["client_pong_payload"], json!("pong"));
    assert_eq!(tapbit_ws_pong_payload(), "pong");
    let policy = tapbit_stream_reconnect_policy();
    assert_eq!(policy.ping_interval_ms, 5_000);
    assert_eq!(policy.pong_timeout_ms, 10_000);
    assert_eq!(policy.stale_message_ms, 10_000);
    let capabilities = TapbitGatewayAdapter::default_public()
        .expect("adapter")
        .capabilities();
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_public_subscribe
    );
    assert_eq!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .interval_ms,
        Some(5_000)
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
}

#[tokio::test]
async fn tapbit_adapter_should_reject_unsupported_or_private_websocket_streams() {
    let adapter = TapbitGatewayAdapter::default_public().expect("adapter");
    let trades = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("trades"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect_err("trades unsupported");
    assert!(format!("{trades:?}").contains("tapbit.spot_public_trades_stream"));

    let private = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("acct").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private ws unsupported");
    assert!(format!("{private:?}")
        .contains("tapbit.subscribe_private_stream.no_official_private_ws_topics"));
}

#[test]
fn tapbit_adapter_should_parse_public_websocket_order_book_and_ticker_messages() {
    let book = parse_tapbit_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({
            "topic": "spot/orderBook.BTCUSDT",
            "action": "update",
            "data": [{
                "asks": [["100.5", "1.5"]],
                "bids": [["99.5", "2.0"]],
                "version": "123456",
                "timestamp": 1700000000000i64
            }]
        }),
    )
    .expect("book");
    match book {
        TapbitPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.bids[0].price, 99.5);
            assert_eq!(snapshot.asks[0].quantity, 1.5);
            assert_eq!(snapshot.sequence, Some(123456));
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let ticker = parse_tapbit_public_stream_message(
        &exchange_id(),
        MarketType::Perpetual,
        perp_symbol_scope(),
        &json!({
            "topic": "usdt/ticker.BTC-SWAP",
            "data": [{
                "symbol": "BTC-SWAP",
                "lastPrice": "29926.5",
                "timestamp": 1700000000000i64
            }]
        }),
    )
    .expect("ticker");
    match ticker {
        TapbitPublicStreamMessage::Ticker(value) => {
            assert_eq!(value["topic"], json!("usdt/ticker.BTC-SWAP"));
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn tapbit_adapter_should_parse_public_websocket_heartbeats_and_acks() {
    let ping = parse_tapbit_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!("ping"),
    )
    .expect("ping");
    assert!(matches!(ping, TapbitPublicStreamMessage::Ping));

    let pong = parse_tapbit_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({"event": "pong"}),
    )
    .expect("pong");
    assert!(matches!(pong, TapbitPublicStreamMessage::Pong));

    let ack = parse_tapbit_public_stream_message(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({"event": "subscribe", "arg": "spot/ticker.BTCUSDT"}),
    )
    .expect("ack");
    assert_eq!(
        ack,
        TapbitPublicStreamMessage::SubscriptionAck {
            event: Some("subscribe".to_string())
        }
    );
}

#[test]
fn tapbit_public_stream_events_should_emit_standard_book_and_heartbeat_events() {
    let events = parse_tapbit_public_stream_events(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({
            "topic": "spot/orderBook.BTCUSDT",
            "action": "update",
            "data": [{
                "asks": [["100.5", "1.5"]],
                "bids": [["99.5", "2.0"]],
                "version": 123456,
                "timestamp": 1700000000000i64
            }]
        }),
    )
    .expect("book events");
    match events.first() {
        Some(ExchangeStreamEvent::OrderBookSnapshot(book)) => {
            assert_eq!(book.order_book.bids[0].price, 99.5);
            assert_eq!(book.order_book.asks[0].quantity, 1.5);
            assert_eq!(book.order_book.sequence, Some(123456));
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let events = parse_tapbit_public_stream_events(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!("ping"),
    )
    .expect("heartbeat");
    assert!(matches!(
        events.first(),
        Some(ExchangeStreamEvent::Heartbeat { .. })
    ));

    let ticker_events = parse_tapbit_public_stream_events(
        &exchange_id(),
        MarketType::Spot,
        spot_symbol_scope(),
        &json!({
            "topic": "spot/ticker.BTCUSDT",
            "data": [{"symbol": "BTCUSDT", "lastPrice": "100"}]
        }),
    )
    .expect("ticker");
    assert!(ticker_events.is_empty());
}
