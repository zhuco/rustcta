use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};

use crate::streams::StreamSupervisorAction;

use super::streams::{
    bitmex_ping_payload, bitmex_private_auth_payload, bitmex_private_subscribe_payload,
    bitmex_public_subscribe_payload, bitmex_stream_reconnect_policy, is_bitmex_heartbeat,
    parse_bitmex_private_stream_message, parse_bitmex_public_stream_message,
    BitmexPrivateStreamMessage, BitmexPublicStreamMessage, BitmexWsSessionEvent,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, private_config};
use super::BitmexGatewayAdapter;

#[test]
fn bitmex_public_stream_payload_should_map_order_book_and_candles() {
    let book = bitmex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("book-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("book payload");
    assert_eq!(book["op"], "subscribe");
    assert_eq!(book["args"][0], "orderBookL2:XBTUSD");

    let candle = bitmex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("candle-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
    })
    .expect("candle payload");
    assert_eq!(candle["args"][0], "tradeBin1m:XBTUSD");
}

#[test]
fn bitmex_stream_parser_should_parse_public_book_and_private_order() {
    let public = parse_bitmex_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &serde_json::json!({
            "table": "orderBookL2",
            "action": "partial",
            "data": [
                {"symbol": "XBTUSD", "side": "Buy", "size": 10, "price": 99.5, "timestamp": "2026-06-07T00:00:00.000Z"},
                {"symbol": "XBTUSD", "side": "Sell", "size": 5, "price": 100.5, "timestamp": "2026-06-07T00:00:00.000Z"}
            ]
        }),
    )
    .expect("public stream");
    match public {
        BitmexPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.bids[0].price, 99.5);
            assert_eq!(book.asks[0].quantity, 5.0);
        }
        other => panic!("unexpected public message {other:?}"),
    }

    let private = parse_bitmex_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(perp_symbol_scope()),
        &serde_json::json!({
            "table": "order",
            "action": "update",
            "data": [{
                "orderID": "order-1",
                "clOrdID": "client-1",
                "symbol": "XBTUSD",
                "side": "Buy",
                "ordType": "Limit",
                "ordStatus": "Filled",
                "orderQty": 10,
                "cumQty": 10,
                "price": 99.5
            }]
        }),
    )
    .expect("private stream");
    match private {
        BitmexPrivateStreamMessage::Order(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));
            assert_eq!(order.status, rustcta_types::OrderStatus::Filled);
        }
        other => panic!("unexpected private message {other:?}"),
    }
}

#[test]
fn bitmex_stream_parser_should_parse_trades_balances_and_positions() {
    let trade = parse_bitmex_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &serde_json::json!({
            "table": "trade",
            "action": "insert",
            "data": [{
                "symbol": "XBTUSD",
                "side": "Sell",
                "size": 3,
                "price": 101.5,
                "timestamp": "2026-06-07T00:00:00.000Z"
            }]
        }),
    )
    .expect("trade stream");
    match trade {
        BitmexPublicStreamMessage::Trades(trades) => {
            assert_eq!(trades[0].price, 101.5);
            assert_eq!(trades[0].quantity, 3.0);
        }
        other => panic!("unexpected trade message {other:?}"),
    }

    let balance = parse_bitmex_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        None,
        &serde_json::json!({
            "table": "margin",
            "action": "update",
            "data": [{
                "currency": "XBt",
                "marginBalance": 200000000,
                "availableMargin": 150000000
            }]
        }),
    )
    .expect("balance stream");
    match balance {
        BitmexPrivateStreamMessage::Balance(response) => {
            assert_eq!(response.balances[0].balances[0].asset, "BTC");
            assert_eq!(response.balances[0].balances[0].total, 2.0);
        }
        other => panic!("unexpected balance message {other:?}"),
    }

    let position = parse_bitmex_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        None,
        &serde_json::json!({
            "table": "position",
            "action": "update",
            "data": [{
                "symbol": "XBTUSD",
                "isOpen": true,
                "currentQty": 7,
                "avgEntryPrice": 100,
                "markPrice": 101,
                "liquidationPrice": 50,
                "leverage": 2
            }]
        }),
    )
    .expect("position stream");
    match position {
        BitmexPrivateStreamMessage::Position(response) => {
            assert_eq!(response.positions[0].quantity, 7.0);
        }
        other => panic!("unexpected position message {other:?}"),
    }
}

#[test]
fn bitmex_private_stream_payload_should_map_auth_and_order_channel() {
    let auth = bitmex_private_auth_payload("key", "secret", 1_518_064_237);
    assert_eq!(auth["op"], "authKeyExpires");
    assert_eq!(auth["args"][0], "key");
    assert!(auth["args"][2]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-stream"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let payload = bitmex_private_subscribe_payload(&subscription).expect("private payload");
    assert_eq!(payload["args"][0], "order");
}

#[test]
fn bitmex_heartbeat_should_build_ping_and_detect_pong() {
    let ping = bitmex_ping_payload();
    assert_eq!(ping["op"], "ping");
    assert!(is_bitmex_heartbeat(&serde_json::json!({"op": "pong"})));
    assert!(is_bitmex_heartbeat(&serde_json::json!({"pong": 1})));

    let parsed = parse_bitmex_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &serde_json::json!({"op": "pong"}),
    )
    .expect("heartbeat");
    assert!(matches!(parsed, BitmexPublicStreamMessage::Pong));
}

#[tokio::test]
async fn bitmex_adapter_should_return_stream_subscription_ids() {
    let adapter = BitmexGatewayAdapter::new(private_config("http://127.0.0.1:1".to_string()))
        .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public subscription");
    assert!(public_id.contains("trade:XBTUSD"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Fills,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("execution"));
}

#[test]
fn bitmex_ws_sessions_should_build_runtime_requests_and_events() {
    let adapter = BitmexGatewayAdapter::new(private_config("http://127.0.0.1:1".to_string()))
        .expect("adapter");
    let mut public_session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-session"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .expect("public session");
    assert_eq!(
        public_session.initial_requests()[0]["args"][0],
        "orderBookL2:XBTUSD"
    );

    let now = chrono::Utc::now();
    public_session.on_connected(now);
    assert_eq!(public_session.heartbeat_request(now)["op"], "ping");
    assert_eq!(
        public_session.supervisor_action(now, &bitmex_stream_reconnect_policy()),
        StreamSupervisorAction::None
    );

    let public_events = public_session
        .handle_text_message(
            r#"{"table":"orderBookL2","action":"partial","data":[{"symbol":"XBTUSD","side":"Buy","size":10,"price":99.5},{"symbol":"XBTUSD","side":"Sell","size":5,"price":100.5}]}"#,
        )
        .expect("public events");
    assert!(public_events.iter().any(|event| {
        matches!(
            event,
            BitmexWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(_)))
        )
    }));

    let mut private_session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-session"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .expect("private session")
        .with_symbol_hint(perp_symbol_scope());
    let initial = private_session.initial_requests();
    assert_eq!(initial[0]["op"], "authKeyExpires");
    assert_eq!(initial[1]["args"][0], "order");
    assert_eq!(private_session.heartbeat_request(now)["op"], "ping");

    let private_events = private_session
        .handle_text_message(
            r#"{"table":"order","action":"update","data":[{"orderID":"order-1","clOrdID":"client-1","symbol":"XBTUSD","side":"Buy","ordType":"Limit","ordStatus":"New","orderQty":10,"cumQty":0,"price":99.5}]}"#,
        )
        .expect("private events");
    assert!(private_events.iter().any(|event| {
        matches!(
            event,
            BitmexWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::OrderUpdate(_)))
        )
    }));
}
