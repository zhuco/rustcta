use chrono::{Duration, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::config::DeepcoinGatewayConfig;
use super::streams::{
    deepcoin_listen_key_extend_body, deepcoin_private_ws_url, deepcoin_public_subscribe_payload,
    deepcoin_public_unsubscribe_all_payload, deepcoin_public_unsubscribe_payload,
    deepcoin_stream_reconnect_policy, deepcoin_ws_heartbeat_payload,
    parse_deepcoin_private_stream_message, parse_deepcoin_public_stream_message, DeepcoinListenKey,
    DeepcoinPrivateStreamMessage, DeepcoinPrivateWsSession, DeepcoinPublicStreamMessage,
    DeepcoinWsSessionEvent,
};
use super::test_support::{
    actual_request, assert_signed_deepcoin_request, context, exchange_id, fixture,
    perp_symbol_scope, request_spec, spawn_rest_server, spot_symbol_scope,
};
use super::DeepcoinGatewayAdapter;
use crate::streams::StreamSupervisorAction;

#[test]
fn deepcoin_public_stream_payload_should_match_v2_docs() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("deepcoin-public-ws"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = deepcoin_public_subscribe_payload(&subscription, 6).unwrap();

    assert_eq!(payload["Action"], "1");
    assert_eq!(payload["Symbol"], "BTC/USDT_0.1");
    assert_eq!(payload["LocalNo"], 6);
    assert_eq!(payload["ResumeNo"], -1);
    assert_eq!(payload["Topic"], "book25");

    let kline = deepcoin_public_subscribe_payload(
        &PublicStreamSubscription {
            kind: PublicStreamKind::Candles {
                interval: "1m".to_string(),
            },
            ..subscription.clone()
        },
        7,
    )
    .unwrap();
    assert_eq!(kline["Topic"], "kline");
    assert_eq!(kline["PeriodID"], "1m");
    assert_eq!(kline["Count"], 100);

    let unsubscribe = deepcoin_public_unsubscribe_payload(
        &PublicStreamSubscription {
            kind: PublicStreamKind::Trades,
            ..subscription.clone()
        },
        8,
    )
    .unwrap();
    assert_eq!(unsubscribe["Action"], "2");
    assert_eq!(unsubscribe["Topic"], "trade");

    let unsubscribe_all = deepcoin_public_unsubscribe_all_payload(9);
    assert_eq!(unsubscribe_all["Action"], "0");
    assert_eq!(unsubscribe_all["LocalNo"], 9);
}

#[test]
fn deepcoin_public_stream_parser_should_parse_book_and_trades() {
    let book = parse_deepcoin_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &fixture("public_ws_book"),
    )
    .unwrap();
    match book {
        DeepcoinPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.asks.len(), 2);
            assert_eq!(snapshot.bids.len(), 1);
            assert_eq!(
                snapshot.exchange_timestamp.unwrap().timestamp_millis(),
                1771924508470
            );
        }
        other => panic!("unexpected book message: {other:?}"),
    }

    let trades = parse_deepcoin_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "a": "PMT",
            "i": "BTCUSDT",
            "tt": 1771924508470_i64,
            "d": [{
                "TradeID": "1000300469088080",
                "D": "1",
                "P": 1826.76,
                "V": 1.0,
                "T": 1771924508
            }]
        }),
    )
    .unwrap();
    match trades {
        DeepcoinPublicStreamMessage::Trades(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].trade_id.as_deref(), Some("1000300469088080"));
            assert_eq!(rows[0].side, OrderSide::Sell);
            assert_eq!(rows[0].price, "1826.76");
        }
        other => panic!("unexpected trades message: {other:?}"),
    }
}

#[test]
fn deepcoin_stream_heartbeat_should_use_ping_pong_contract() {
    assert_eq!(deepcoin_ws_heartbeat_payload(), "ping");
    let policy = deepcoin_stream_reconnect_policy();
    assert_eq!(policy.ping_interval_ms, 15_000);
    assert_eq!(policy.pong_timeout_ms, 25_000);
    assert_eq!(policy.stale_message_ms, 20_000);

    let public_pong =
        parse_deepcoin_public_stream_message(&exchange_id(), spot_symbol_scope(), &json!("pong"))
            .unwrap();
    assert!(matches!(public_pong, DeepcoinPublicStreamMessage::Pong));

    let private_pong = parse_deepcoin_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        None,
        &json!("pong"),
    )
    .unwrap();
    assert!(matches!(private_pong, DeepcoinPrivateStreamMessage::Pong));
}

#[test]
fn deepcoin_public_ws_session_should_subscribe_ping_and_emit_stream_events() {
    let adapter = DeepcoinGatewayAdapter::default_public().unwrap();
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("deepcoin-public-session"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .unwrap();
    let initial = session.initial_requests();
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0]["Topic"], "book25");
    assert_eq!(session.heartbeat_request(), "ping");

    let now = Utc.with_ymd_and_hms(2026, 6, 7, 0, 0, 0).unwrap();
    session.on_connected(now);
    assert_eq!(
        session.supervisor_action(
            now + Duration::milliseconds(15_001),
            &deepcoin_stream_reconnect_policy(),
        ),
        StreamSupervisorAction::SendPing
    );

    let pong = session.handle_text_message("pong").unwrap();
    assert!(matches!(
        pong.first(),
        Some(DeepcoinWsSessionEvent::Public(
            DeepcoinPublicStreamMessage::Pong
        ))
    ));

    let events = session
        .handle_text_message(
            &json!({
                "a": "PMO",
                "t": "i",
                "i": "BTCUSDT",
                "tt": 1771924508470_i64,
                "d": [{
                    "a": [["8476.98", "415"]],
                    "b": [["8476.90", "12"]]
                }]
            })
            .to_string(),
        )
        .unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DeepcoinWsSessionEvent::Stream(items)
            if matches!(items.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(_)))
    )));
}

#[test]
fn deepcoin_private_ws_session_should_track_heartbeat_events_and_listenkey_renewal() {
    let expires_at = Utc.with_ymd_and_hms(2026, 6, 7, 1, 0, 0).unwrap();
    let listen_key = DeepcoinListenKey {
        listen_key: "listen-123".to_string(),
        expires_at: Some(expires_at),
        private_ws_url: deepcoin_private_ws_url(
            "wss://stream.deepcoin.com/v1/private",
            "listen-123",
        ),
    };
    let mut session = DeepcoinPrivateWsSession::new(
        exchange_id(),
        PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("deepcoin-private-session"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: context("account").account_id.unwrap(),
            kind: PrivateStreamKind::Orders,
        },
        listen_key,
        None,
    )
    .unwrap();
    assert!(session.initial_requests().is_empty());
    assert_eq!(session.heartbeat_request(), "ping");
    assert!(!session.listen_key_renewal_due(expires_at - Duration::minutes(10)));
    assert!(session.listen_key_renewal_due(expires_at - Duration::minutes(4)));

    let pong = session.handle_text_message("pong").unwrap();
    assert!(matches!(
        pong.first(),
        Some(DeepcoinWsSessionEvent::Private(
            DeepcoinPrivateStreamMessage::Pong
        ))
    ));

    let events = session
        .handle_text_message(
            &json!({
                "result": [{
                    "table": "Order",
                    "data": {
                        "D": "0",
                        "I": "BTCUSDT",
                        "IT": 1690804738,
                        "L": "local-1",
                        "OS": "1000175061255804",
                        "OT": "0",
                        "Or": "1",
                        "P": 29365,
                        "U": 1690804739,
                        "V": 55,
                        "p": "1",
                        "v": 0
                    }
                }]
            })
            .to_string(),
        )
        .unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DeepcoinWsSessionEvent::Stream(items)
            if matches!(items.first(), Some(ExchangeStreamEvent::OrderUpdate(_)))
    )));

    session.update_listen_key(DeepcoinListenKey {
        listen_key: "listen-456".to_string(),
        expires_at: Some(expires_at + Duration::hours(1)),
        private_ws_url: deepcoin_private_ws_url(
            "wss://stream.deepcoin.com/v1/private",
            "listen-456",
        ),
    });
    assert_eq!(session.listen_key().listen_key, "listen-456");
    assert!(session.url.ends_with("listenKey=listen-456"));
}

#[tokio::test]
async fn deepcoin_private_stream_should_acquire_listen_key_and_mask_url() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "data": {
            "listenkey": "listen-123",
            "expire_time": 1691403285
        }
    })])
    .await;
    let config = DeepcoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("pass".to_string()),
        enabled_private_rest: true,
        ..DeepcoinGatewayConfig::default()
    };
    let adapter = DeepcoinGatewayAdapter::new(config).unwrap();

    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("deepcoin-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: context("account").account_id.unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let descriptor = adapter
        .subscribe_private_stream(subscription)
        .await
        .unwrap();

    assert_eq!(
        descriptor,
        "wss://stream.deepcoin.com/v1/private?listenKey=listen-123"
    );
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_signed_deepcoin_request(&requests[0], "GET", "/deepcoin/listenkey/acquire");
    request_spec("acquire_listen_key")
        .assert_matches(&actual_request(requests[0].clone()))
        .expect("deepcoin listen key acquire request spec");
}

#[tokio::test]
async fn deepcoin_private_stream_should_extend_listen_key_with_signed_body() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "data": {
            "listenkey": "listen-123",
            "expire_time": 1691406885
        }
    })])
    .await;
    let config = DeepcoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("pass".to_string()),
        enabled_private_rest: true,
        ..DeepcoinGatewayConfig::default()
    };
    let adapter = DeepcoinGatewayAdapter::new(config).unwrap();

    let listen_key = adapter
        .extend_private_stream_listen_key("listen-123")
        .await
        .unwrap();

    assert_eq!(listen_key.listen_key, "listen-123");
    assert_eq!(
        listen_key.private_ws_url,
        "wss://stream.deepcoin.com/v1/private?listenKey=listen-123"
    );
    assert_eq!(listen_key.expires_at.unwrap().timestamp(), 1691406885,);
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_signed_deepcoin_request(&requests[0], "GET", "/deepcoin/listenkey/extend");
    assert_eq!(requests[0].body, "listenkey=listen-123");
    assert!(requests[0]
        .headers
        .get("content-type")
        .is_some_and(|content_type| content_type.starts_with("application/x-www-form-urlencoded")));
    request_spec("extend_listen_key")
        .assert_matches(&actual_request(requests[0].clone()))
        .expect("deepcoin listen key extend request spec");
}

#[test]
fn deepcoin_private_ws_url_should_preserve_existing_query() {
    assert_eq!(
        deepcoin_private_ws_url("wss://stream.deepcoin.com/v1/private?x=1", "abc"),
        "wss://stream.deepcoin.com/v1/private?x=1&listenKey=abc"
    );
    assert_eq!(deepcoin_listen_key_extend_body("abc"), "listenkey=abc");
}

#[test]
fn deepcoin_private_stream_parser_should_parse_short_field_pushes() {
    let parsed = parse_deepcoin_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        None,
        &fixture("private_ws_push_short"),
    )
    .unwrap();

    let DeepcoinPrivateStreamMessage::Events(events) = parsed else {
        panic!("expected stream events");
    };
    assert_eq!(events.len(), 3);
    match &events[0] {
        ExchangeStreamEvent::OrderUpdate(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("1000175061255804"));
            assert_eq!(order.client_order_id.as_deref(), Some("local-1"));
            assert_eq!(order.side, OrderSide::Sell);
            assert_eq!(order.status, OrderStatus::PartiallyFilled);
            assert_eq!(order.filled_quantity, "25");
        }
        other => panic!("unexpected first event: {other:?}"),
    }
    match &events[1] {
        ExchangeStreamEvent::Fill(fill) => {
            assert_eq!(fill.fill_id.as_deref(), Some("1000168389225300"));
            assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
        }
        other => panic!("unexpected second event: {other:?}"),
    }
    assert!(matches!(
        &events[2],
        ExchangeStreamEvent::PositionSnapshot(response) if response.positions.len() == 1
    ));
}

#[test]
fn deepcoin_private_stream_parser_should_parse_rest_style_payloads() {
    let parsed = parse_deepcoin_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        None,
        &json!({
            "arg": { "channel": "fills" },
            "data": [{
                "instType": "SWAP",
                "instId": "BTC-USDT-SWAP",
                "tradeId": "trade-1",
                "ordId": "order-1",
                "billId": "bill-1",
                "fillPx": "98230.5",
                "fillSz": "2",
                "side": "sell",
                "posSide": "short",
                "execType": "T",
                "feeCcy": "USDT",
                "fee": "0.1",
                "ts": "1739261250000"
            }]
        }),
    )
    .unwrap();

    let DeepcoinPrivateStreamMessage::Events(events) = parsed else {
        panic!("expected stream events");
    };
    assert!(matches!(
        events.first(),
        Some(ExchangeStreamEvent::Fill(fill)) if fill.fill_id.as_deref() == Some("bill-1")
    ));
}

#[test]
fn deepcoin_stream_parser_should_cover_error_and_missing_field_fixtures() {
    let public_error = parse_deepcoin_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &fixture("error_response"),
    )
    .expect_err("error-shaped public ws payload should be unsupported");
    assert!(matches!(
        public_error,
        rustcta_exchange_api::ExchangeApiError::Exchange(_)
    ));

    let private_missing = parse_deepcoin_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        None,
        &fixture("missing_inst_id_order"),
    )
    .expect_err("missing private stream table/channel should be unsupported");
    assert!(matches!(
        private_missing,
        rustcta_exchange_api::ExchangeApiError::Exchange(_)
    ));
}
