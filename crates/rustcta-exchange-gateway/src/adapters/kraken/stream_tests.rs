use rustcta_exchange_api::{
    BatchAtomicity, BatchExecutionMode, ExchangeClient, ExchangeStreamEvent, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    kraken_futures_challenge_payload, kraken_futures_private_subscribe_payload,
    kraken_public_subscribe_payload, kraken_spot_private_subscribe_payload, kraken_ws_ping_payload,
    parse_kraken_ws_control_message, KrakenWsControlMessage, KrakenWsSessionEvent,
};
use super::test_support::{
    context, exchange_id, perp_symbol_scope, private_config, spawn_rest_server, spot_symbol_scope,
};
use super::KrakenGatewayAdapter;

#[test]
fn kraken_spot_public_subscription_should_use_v2_payload() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-ws"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = kraken_public_subscribe_payload(&subscription).expect("payload");

    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["params"]["channel"], "book");
    assert_eq!(payload["params"]["symbol"][0], "BTC/USD");
    assert_eq!(payload["params"]["depth"], 100);
}

#[test]
fn kraken_futures_public_subscription_should_use_futures_payload() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("futures-ws"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    };

    let payload = kraken_public_subscribe_payload(&subscription).expect("payload");

    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["feed"], "ticker");
    assert_eq!(payload["product_ids"][0], "PF_XBTUSDT");
}

#[test]
fn kraken_spot_private_subscription_should_embed_token() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let payload =
        kraken_spot_private_subscribe_payload(&subscription, "ws-token").expect("payload");

    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["params"]["channel"], "executions");
    assert_eq!(payload["params"]["token"], "ws-token");
}

#[test]
fn kraken_futures_private_subscription_should_embed_signed_challenge() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("futures-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Positions,
    };

    let payload = kraken_futures_private_subscribe_payload(
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
fn kraken_public_ws_session_should_subscribe_and_track_heartbeat() {
    let adapter = KrakenGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-ws-session"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .expect("session");

    assert_eq!(session.url, "wss://ws.kraken.com/v2");
    let initial = session.initial_requests();
    assert_eq!(initial[0]["method"], "subscribe");
    assert_eq!(initial[0]["params"]["channel"], "ticker");
    assert_eq!(session.state().subscription_count, 1);

    session.on_connected();
    let ping = session.heartbeat_request();
    assert_eq!(ping, kraken_ws_ping_payload());
    assert!(session.state().last_ping_at.is_some());

    let events = session
        .handle_text_message(&json!({"channel": "heartbeat"}).to_string())
        .expect("heartbeat");
    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(items.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
    )));
    assert!(session.state().last_pong_at.is_some());
}

#[test]
fn kraken_public_ws_session_should_emit_spot_order_book_stream_event() {
    let adapter = KrakenGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-ws-book"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .expect("session");

    let events = session
        .handle_text_message(
            &json!({
                "channel": "book",
                "type": "snapshot",
                "symbol": "BTC/USD",
                "data": [{
                    "bids": [["29900.1", "1.2", "1700000000.100000"]],
                    "asks": [["29901.2", "0.8", "1700000000.100000"]]
                }]
            })
            .to_string(),
        )
        .expect("book");

    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(
                items.first(),
                Some(ExchangeStreamEvent::OrderBookSnapshot(book))
                    if book.order_book.bids[0].price == 29900.1
                        && book.order_book.asks[0].quantity == 0.8
            )
    )));
}

#[test]
fn kraken_public_ws_session_should_emit_futures_order_book_stream_event() {
    let adapter = KrakenGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-ws-book"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .expect("session");

    let events = session
        .handle_text_message(
            &json!({
                "feed": "book",
                "product_id": "PF_XBTUSDT",
                "bids": [{"price": 29900.1, "qty": 2.0}],
                "asks": [{"price": 29901.2, "qty": 1.5}],
                "sequence": 42
            })
            .to_string(),
        )
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
fn kraken_private_ws_session_should_subscribe_and_handle_pong() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");
    let mut session = adapter
        .spot_private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("private-ws-session"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Balances,
            },
            "ws-token",
        )
        .expect("session");

    assert_eq!(session.url, "wss://ws-auth.kraken.com/v2");
    let initial = session.initial_requests();
    assert_eq!(initial[0]["method"], "subscribe");
    assert_eq!(initial[0]["params"]["channel"], "balances");
    assert_eq!(initial[0]["params"]["token"], "ws-token");

    session.on_connected();
    let events = session
        .handle_text_message(&json!({"method": "pong"}).to_string())
        .expect("pong");
    assert!(matches!(
        events.first(),
        Some(KrakenWsSessionEvent::Private(KrakenWsControlMessage::Pong))
    ));
    assert!(session.state().last_pong_at.is_some());
}

#[test]
fn kraken_private_ws_session_should_emit_spot_order_stream_event() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");
    let mut session = adapter
        .spot_private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("spot-order-event"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Orders,
            },
            "ws-token",
        )
        .expect("session");

    let events = session
        .handle_text_message(
            &json!({
                "channel": "executions",
                "type": "update",
                "data": [{
                    "txid": "OABCDEF-12345-67890",
                    "cl_ord_id": "client-1",
                    "pair": "BTC/USD",
                    "side": "buy",
                    "orderType": "limit",
                    "status": "open",
                    "vol": "0.25",
                    "vol_exec": "0.10",
                    "price": "37500.5"
                }]
            })
            .to_string(),
        )
        .expect("order event");

    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(
                items.first(),
                Some(ExchangeStreamEvent::OrderUpdate(order))
                    if order.exchange_order_id.as_deref() == Some("OABCDEF-12345-67890")
                        && order.client_order_id.as_deref() == Some("client-1")
                        && order.market_type == MarketType::Spot
                        && order.filled_quantity == "0.10"
            )
    )));
}

#[test]
fn kraken_private_ws_session_should_emit_spot_balance_stream_event() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");
    let mut session = adapter
        .spot_private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("spot-balance-event"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Balances,
            },
            "ws-token",
        )
        .expect("session");

    let events = session
        .handle_text_message(
            &json!({
                "channel": "balances",
                "type": "update",
                "data": [{
                    "asset": "XBT",
                    "balance": "1.50",
                    "hold_trade": "0.25"
                }]
            })
            .to_string(),
        )
        .expect("balance event");

    assert!(events.iter().any(|event| matches!(
        event,
        KrakenWsSessionEvent::Stream(items)
            if matches!(
                items.first(),
                Some(ExchangeStreamEvent::BalanceSnapshot(snapshot))
                    if snapshot.balances[0].balances[0].asset == "BTC"
                        && snapshot.balances[0].balances[0].available == 1.25
            )
    )));
}

#[test]
fn kraken_futures_private_ws_session_should_challenge_then_subscribe() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
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
        vec![kraken_futures_challenge_payload()]
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
fn kraken_private_ws_session_should_emit_futures_fill_stream_event() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
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
        .handle_text_message(
            &json!({
                "feed": "fills",
                "fills": [{
                    "fill_id": "fill-1",
                    "order_id": "order-1",
                    "symbol": "PF_XBTUSDT",
                    "side": "buy",
                    "price": "37500.5",
                    "size": "0.2",
                    "fee": "0.03",
                    "fillTime": "2024-01-01T00:00:00Z"
                }]
            })
            .to_string(),
        )
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
fn kraken_private_ws_session_should_emit_futures_position_stream_event() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
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
        .handle_text_message(
            &json!({
                "feed": "open_positions",
                "openPositions": [{
                    "symbol": "PF_XBTUSDT",
                    "side": "long",
                    "size": "0.4",
                    "price": "37000",
                    "markPrice": "37600",
                    "pnl": "12.5",
                    "leverage": "5"
                }]
            })
            .to_string(),
        )
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
fn kraken_ws_control_parser_should_accept_spot_and_futures_acks() {
    assert_eq!(
        parse_kraken_ws_control_message(&json!({
            "method": "subscribe",
            "success": true,
            "result": {"channel": "book"}
        })),
        KrakenWsControlMessage::SubscriptionAck {
            channel: Some("book".to_string())
        }
    );
    assert_eq!(
        parse_kraken_ws_control_message(&json!({
            "event": "subscribed",
            "feed": "ticker",
            "product_ids": ["PF_XBTUSDT"]
        })),
        KrakenWsControlMessage::SubscriptionAck {
            channel: Some("ticker".to_string())
        }
    );
    assert_eq!(
        parse_kraken_ws_control_message(&json!({"event": "heartbeat"})),
        KrakenWsControlMessage::Heartbeat
    );
}

#[tokio::test]
async fn kraken_adapter_should_ack_public_stream_specs() {
    let adapter = KrakenGatewayAdapter::default_public().expect("adapter");

    let spot_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("spot subscription");
    assert!(spot_id.contains("wss://ws.kraken.com/v2:trade:BTC/USD"));

    let futures_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect("futures subscription");
    assert!(futures_id.contains("wss://futures.kraken.com/ws/v1:book:PF_XBTUSDT"));
}

#[tokio::test]
async fn kraken_adapter_should_fetch_token_for_spot_private_stream() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": [],
        "result": {
            "token": "ws-token",
            "expires": 900
        }
    })])
    .await;
    let adapter = KrakenGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let subscription_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Fills,
        })
        .await
        .expect("private subscription");

    assert!(subscription_id.contains("executions:account"));
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/0/private/GetWebSocketsToken");
    assert!(requests[0].headers.contains_key("api-key"));
    assert!(requests[0].headers.contains_key("api-sign"));
}

#[tokio::test]
async fn kraken_adapter_should_ack_futures_private_stream_challenge_specs() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");

    let subscription_id = adapter
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

    assert!(subscription_id.contains("wss://futures.kraken.com/ws/v1:open_orders:account"));
}

#[test]
fn kraken_capabilities_v2_should_declare_pagination_batch_and_ws_policy() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
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
    assert!(capabilities.capabilities_v2.fills_history.supports_cursor);
    assert!(capabilities.capabilities_v2.fills_history.supports_from_id);
    assert!(capabilities.capabilities_v2.stream_runtime.auth.required);
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_websockets_token"));
}
