use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    coinw_ping_payload, coinw_private_subscribe_payload, coinw_public_subscribe_payload,
    coinw_stream_reconnect_policy, CoinwPrivateStreamMessage, CoinwPublicStreamMessage,
    CoinwWsSessionEvent,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{CoinwGatewayAdapter, CoinwGatewayConfig};
use crate::streams::StreamSupervisorAction;

fn private_config() -> CoinwGatewayConfig {
    CoinwGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinwGatewayConfig::default()
    }
}

#[test]
fn coinw_stream_payload_should_map_spot_public_channels() {
    let book = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-book-stream"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = coinw_public_subscribe_payload(&book).expect("book payload");
    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["args"], "spot/level2_20:BTC-USDT");

    let candles = PublicStreamSubscription {
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
        ..book
    };
    let payload = coinw_public_subscribe_payload(&candles).expect("candle payload");
    assert_eq!(payload["args"], "spot/candle-1m:BTC-USDT");
}

#[test]
fn coinw_stream_payload_should_map_futures_public_channels() {
    let trades = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-trades-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Trades,
    };
    let payload = coinw_public_subscribe_payload(&trades).expect("trades payload");
    assert_eq!(payload["event"], "sub");
    assert_eq!(payload["params"]["biz"], "futures");
    assert_eq!(payload["params"]["pairCode"], "BTC");
    assert_eq!(payload["params"]["type"], "fills");
}

#[test]
fn coinw_stream_payload_should_map_private_channels() {
    let spot_orders = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-orders-stream"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let payload =
        coinw_private_subscribe_payload(&spot_orders, MarketType::Spot).expect("spot private");
    assert_eq!(payload["params"]["biz"], "exchange");
    assert_eq!(payload["params"]["type"], "order");

    let futures_positions = PrivateStreamSubscription {
        market_type: Some(MarketType::Perpetual),
        kind: PrivateStreamKind::Positions,
        ..spot_orders
    };
    let payload = coinw_private_subscribe_payload(&futures_positions, MarketType::Perpetual)
        .expect("futures private");
    assert_eq!(payload["params"]["biz"], "futures");
    assert_eq!(payload["params"]["type"], "position");
}

#[tokio::test]
async fn coinw_adapter_should_ack_stream_request_specs() {
    let adapter = CoinwGatewayAdapter::new(private_config()).expect("adapter");
    let public_ack = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-stream"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public ack");
    assert!(public_ack.contains("coinw:wss://ws.futurescw.com/perpum:depth"));

    let private_ack = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-stream"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private ack");
    assert!(private_ack.contains("coinw:wss://ws.futurescw.com:assets:account"));
}

#[test]
fn coinw_public_ws_session_should_handle_initial_requests_heartbeat_and_books() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-session"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let mut session = CoinwGatewayAdapter::default_public()
        .expect("adapter")
        .public_ws_session(subscription)
        .expect("session");
    assert_eq!(
        session.initial_requests(),
        vec![json!({
            "event": "subscribe",
            "args": "spot/level2_20:BTC-USDT",
        })]
    );

    let now = Utc::now();
    assert_eq!(session.heartbeat_request(now), coinw_ping_payload());
    assert_eq!(
        session.supervisor_action(now, &coinw_stream_reconnect_policy()),
        StreamSupervisorAction::Connect
    );

    let heartbeat_events = session
        .handle_text_message(r#"{ "event": "ping" }"#)
        .expect("heartbeat");
    assert!(heartbeat_events.iter().any(|event| matches!(
        event,
        CoinwWsSessionEvent::Outbound(value) if value["event"] == "pong"
    )));
    assert!(heartbeat_events.iter().any(|event| matches!(
        event,
        CoinwWsSessionEvent::Stream(stream_events)
            if matches!(stream_events.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
    )));

    let book_events = session
        .handle_text_message(
            r#"{
                "event": "push",
                "args": "spot/level2_20:BTC-USDT",
                "data": {
                    "bids": [["100.0", "1.25"]],
                    "asks": [["101.0", "2.5"]]
                }
            }"#,
        )
        .expect("book");
    assert!(book_events.iter().any(|event| matches!(
        event,
        CoinwWsSessionEvent::Public(CoinwPublicStreamMessage::OrderBook(_))
    )));
    let stream_event = book_events
        .iter()
        .find_map(|event| match event {
            CoinwWsSessionEvent::Stream(events) => events.first(),
            _ => None,
        })
        .expect("stream event");
    match stream_event {
        ExchangeStreamEvent::OrderBookSnapshot(book) => {
            assert_eq!(book.order_book.bids[0].price, 100.0);
            assert_eq!(book.order_book.asks[0].quantity, 2.5);
        }
        other => panic!("unexpected stream event: {other:?}"),
    }
}

#[test]
fn coinw_private_ws_session_should_parse_order_balance_and_position_events() {
    let adapter = CoinwGatewayAdapter::new(private_config()).expect("adapter");
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-session"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let mut session = adapter
        .private_ws_session(subscription, Some(perp_symbol_scope()))
        .expect("session");
    assert_eq!(
        session.initial_requests(),
        vec![json!({
            "event": "sub",
            "params": {
                "biz": "futures",
                "type": "order",
            },
        })]
    );

    let order_events = session
        .handle_text_message(
            r#"{
                "event": "push",
                "params": { "type": "order" },
                "data": {
                    "instrument": "BTC_USDT",
                    "orderId": "o-1",
                    "direction": "buy",
                    "status": "new",
                    "orderType": "limit",
                    "orderPrice": "100.0",
                    "orderVolume": "2"
                }
            }"#,
        )
        .expect("order event");
    assert!(order_events.iter().any(|event| matches!(
        event,
        CoinwWsSessionEvent::Private(CoinwPrivateStreamMessage::Events(_))
    )));
    assert!(order_events.iter().any(|event| matches!(
        event,
        CoinwWsSessionEvent::Stream(stream_events)
            if matches!(stream_events.first(), Some(ExchangeStreamEvent::OrderUpdate(order)) if order.exchange_order_id.as_deref() == Some("o-1"))
    )));

    let balance = super::streams::parse_coinw_private_stream_message(
        &exchange_id(),
        context("balance").tenant_id.expect("tenant"),
        AccountId::new("account").expect("account"),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "event": "push",
            "params": { "type": "assets" },
            "data": {
                "quote": "USDT",
                "availableUsdt": "10",
                "alFreeze": "2",
                "equity": "12"
            }
        }),
    )
    .expect("balance");
    assert!(matches!(
        balance,
        CoinwPrivateStreamMessage::Events(events)
            if matches!(events.first(), Some(ExchangeStreamEvent::BalanceSnapshot(snapshot)) if snapshot.balances[0].balances[0].asset == "USDT")
    ));

    let position = super::streams::parse_coinw_private_stream_message(
        &exchange_id(),
        context("position").tenant_id.expect("tenant"),
        AccountId::new("account").expect("account"),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "event": "push",
            "params": { "type": "position" },
            "data": {
                "instrument": "BTC_USDT",
                "direction": "buy",
                "currentPiece": "3",
                "openPrice": "99.5"
            }
        }),
    )
    .expect("position");
    assert!(matches!(
        position,
        CoinwPrivateStreamMessage::Events(events)
            if matches!(events.first(), Some(ExchangeStreamEvent::PositionSnapshot(snapshot)) if snapshot.positions[0].quantity == 3.0)
    ));
}
