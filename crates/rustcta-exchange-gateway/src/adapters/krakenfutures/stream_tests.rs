use rustcta_exchange_api::{
    BatchAtomicity, BatchExecutionMode, ExchangeApiError, ExchangeClient, ExchangeStreamEvent,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    krakenfutures_futures_challenge_payload, krakenfutures_futures_private_subscribe_payload,
    krakenfutures_public_subscribe_payload, krakenfutures_ws_ping_payload,
    parse_krakenfutures_ws_control_message, KrakenWsControlMessage, KrakenWsSessionEvent,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, private_config};
use super::KrakenFuturesGatewayAdapter;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "ws_public_book.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/krakenfutures/ws_public_book.json"
        ),
        "ws_private_fill.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/krakenfutures/ws_private_fill.json"
        ),
        "ws_private_position.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/krakenfutures/ws_private_position.json"
        ),
        _ => panic!("unknown krakenfutures ws fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[test]
fn krakenfutures_public_subscription_should_use_futures_payload() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("futures-ws"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    };

    let payload = krakenfutures_public_subscribe_payload(&subscription).expect("payload");

    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["feed"], "ticker");
    assert_eq!(payload["product_ids"][0], "PF_XBTUSDT");
}

#[test]
fn krakenfutures_public_subscription_should_reject_spot() {
    let mut symbol = perp_symbol_scope();
    symbol.market_type = MarketType::Spot;
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-ws"),
        symbol,
        kind: PublicStreamKind::Ticker,
    };

    let err = krakenfutures_public_subscribe_payload(&subscription).unwrap_err();
    assert!(matches!(err, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn krakenfutures_futures_private_subscription_should_embed_signed_challenge() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("futures-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Positions,
    };

    let payload = krakenfutures_futures_private_subscribe_payload(
        &subscription,
        "futures-key",
        "challenge-text",
        "signed-challenge",
    )
    .expect("payload");

    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["feed"], "open_positions");
    assert_eq!(payload["api_key"], "futures-key");
    assert_eq!(payload["original_challenge"], "challenge-text");
    assert_eq!(payload["signed_challenge"], "signed-challenge");
}

#[test]
fn krakenfutures_public_ws_session_should_subscribe_and_track_heartbeat() {
    let adapter = KrakenFuturesGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-ws-session"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .expect("session");

    assert_eq!(session.url, "wss://futures.kraken.com/ws/v1");
    let initial = session.initial_requests();
    assert_eq!(initial[0]["event"], "subscribe");
    assert_eq!(initial[0]["feed"], "ticker");
    assert_eq!(session.state().subscription_count, 1);

    session.on_connected();
    let ping = session.heartbeat_request();
    assert_eq!(ping, krakenfutures_ws_ping_payload());
    assert!(session.state().last_ping_at.is_some());

    let events = session
        .handle_text_message(&json!({"event": "heartbeat"}).to_string())
        .expect("heartbeat");
    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(items.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
    )));
    assert!(session.state().last_pong_at.is_some());
}

#[test]
fn krakenfutures_public_ws_session_should_emit_futures_order_book_stream_event() {
    let adapter = KrakenFuturesGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-ws-book"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .expect("session");

    let events = session
        .handle_text_message(&fixture("ws_public_book.json").to_string())
        .expect("book");

    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(
                items.first(),
                Some(ExchangeStreamEvent::OrderBookSnapshot(book))
                    if book.order_book.bids[0].quantity == 2.0
                        && book.order_book.sequence == Some(42)
            )
    )));
}

#[test]
fn krakenfutures_private_ws_session_should_challenge_then_subscribe() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");
    let mut session = adapter
        .private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("futures-private-ws-session"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Perpetual),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Orders,
            },
            None,
        )
        .expect("session");

    assert_eq!(session.url, "wss://futures.kraken.com/ws/v1");
    assert_eq!(
        session.initial_requests(),
        vec![krakenfutures_futures_challenge_payload()]
    );

    let events = session
        .handle_text_message(
            &json!({"event": "challenge", "message": "challenge-text"}).to_string(),
        )
        .expect("challenge");
    assert!(matches!(
        events.first(),
        Some(KrakenWsSessionEvent::Outbound(payload))
            if payload["event"] == "subscribe"
                && payload["feed"] == "open_orders"
                && payload["original_challenge"] == "challenge-text"
                && payload["api_key"] == "futures-key"
                && payload["signed_challenge"].as_str().is_some_and(|signature| !signature.is_empty())
    ));
}

#[test]
fn krakenfutures_private_ws_session_should_emit_futures_fill_stream_event() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");
    let mut session = adapter
        .private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("futures-fill-event"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Perpetual),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Fills,
            },
            None,
        )
        .expect("session");

    let events = session
        .handle_text_message(&fixture("ws_private_fill.json").to_string())
        .expect("fill event");

    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(
                items.first(),
                Some(ExchangeStreamEvent::Fill(fill))
                    if fill.fill_id.as_deref() == Some("fill-1")
                        && fill.market_type == MarketType::Perpetual
                        && fill.canonical_symbol.as_str() == "BTC/USDT"
            )
    )));
}

#[test]
fn krakenfutures_private_ws_session_should_emit_futures_position_stream_event() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");
    let mut session = adapter
        .private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("futures-position-event"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Perpetual),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Positions,
            },
            None,
        )
        .expect("session");

    let events = session
        .handle_text_message(&fixture("ws_private_position.json").to_string())
        .expect("position event");

    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(
                items.first(),
                Some(ExchangeStreamEvent::PositionSnapshot(snapshot))
                    if snapshot.positions[0].canonical_symbol.as_str() == "BTC/USDT"
                        && snapshot.positions[0].quantity == 0.4
                        && snapshot.positions[0].side == rustcta_types::PositionSide::Long
            )
    )));
}

#[test]
fn krakenfutures_ws_control_parser_should_accept_futures_acks() {
    assert_eq!(
        parse_krakenfutures_ws_control_message(&json!({
            "event": "subscribed",
            "feed": "ticker",
            "product_ids": ["PF_XBTUSDT"]
        })),
        KrakenWsControlMessage::SubscriptionAck {
            channel: Some("ticker".to_string())
        }
    );
    assert_eq!(
        parse_krakenfutures_ws_control_message(&json!({"event": "heartbeat"})),
        KrakenWsControlMessage::Heartbeat
    );
}

#[tokio::test]
async fn krakenfutures_adapter_should_ack_futures_stream_specs() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect("futures subscription");
    assert!(public_id.contains("wss://futures.kraken.com/ws/v1:book:PF_XBTUSDT"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("futures private stream");
    assert!(private_id.contains("wss://futures.kraken.com/ws/v1:open_orders:account"));
}

#[test]
fn krakenfutures_capabilities_v2_should_declare_ws_policy() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");

    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(capabilities.capabilities_v2.stream_runtime.auth.required);
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert!(!capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_websockets_token"));
}
