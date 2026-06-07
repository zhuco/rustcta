use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, TenantId};
use serde_json::json;

use crate::streams::StreamSupervisorAction;

use super::streams::{
    parse_poloniex_private_stream_message, parse_poloniex_public_stream_message,
    poloniex_private_subscribe_payload, poloniex_public_subscribe_payload,
    poloniex_stream_reconnect_policy, poloniex_ws_auth_payload, PoloniexPublicStreamMessage,
    PoloniexWsSessionEvent,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{PoloniexGatewayAdapter, PoloniexGatewayConfig};

#[test]
fn poloniex_streams_should_build_public_and_private_payloads() {
    let public = poloniex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("public payload");
    assert_eq!(public["event"], "subscribe");
    assert_eq!(public["channel"][0], "book_lv2");
    assert_eq!(public["symbols"][0], "BTC_USDT");
    assert_eq!(public["depth"], 20);

    let candle = poloniex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-candle-ws"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
    })
    .expect("candle payload");
    assert_eq!(candle["channel"][0], "candles_minute_1");
    assert_eq!(candle["symbols"][0], "BTC_USDT_PERP");

    let private = poloniex_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Positions,
    })
    .expect("private payload");
    assert_eq!(private["channel"][0], "positions");
    assert_eq!(private["symbols"][0], "all");

    let auth = poloniex_ws_auth_payload("key", "secret", 1_631_018_760_000).expect("auth");
    assert_eq!(auth["channel"][0], "auth");
    assert_eq!(auth["params"]["key"], "key");
    assert_eq!(auth["params"]["signatureMethod"], "HmacSHA256");
    assert!(auth["params"]["signature"]
        .as_str()
        .is_some_and(|signature| !signature.is_empty()));
}

#[test]
fn poloniex_stream_parser_should_parse_public_book_trade_ticker_and_candle() {
    let book = parse_poloniex_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "channel": "book_lv2",
            "action": "snapshot",
            "data": [{
                "symbol": "BTC_USDT",
                "asks": [["46100", "2"]],
                "bids": [["46000", "1"]],
                "id": 10410,
                "ts": 1652774727337i64
            }]
        }),
    )
    .expect("book");
    match book {
        PoloniexPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.asks[0].price, 46100.0);
            assert_eq!(snapshot.bids[0].quantity, 1.0);
        }
        other => panic!("unexpected book message: {other:?}"),
    }

    let trades = parse_poloniex_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &json!({
            "channel": "trades",
            "data": [{
                "id": 291,
                "ts": 1718871802553i64,
                "s": "BTC_USDT_PERP",
                "px": "46100",
                "qty": "1",
                "amt": "461",
                "side": "buy",
                "cT": 1718871802534i64
            }]
        }),
    )
    .expect("trades");
    match trades {
        PoloniexPublicStreamMessage::Trades(trades) => {
            assert_eq!(trades[0].price, "46100");
            assert_eq!(trades[0].quote_quantity.as_deref(), Some("461"));
        }
        other => panic!("unexpected trades message: {other:?}"),
    }

    let ticker = parse_poloniex_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &json!({
            "channel": "tickers",
            "data": [{
                "s": "BTC_USDT_PERP",
                "o": "46000",
                "l": "26829.541",
                "h": "46100",
                "c": "46100",
                "qty": "18736",
                "amt": "8556118.81658",
                "dC": "0.0022",
                "bPx": "46000",
                "bSz": "10",
                "aPx": "46100",
                "aSz": "9",
                "ts": 1718872247385i64
            }]
        }),
    )
    .expect("ticker");
    match ticker {
        PoloniexPublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price.as_deref(), Some("46100"));
            assert_eq!(ticker.bid_quantity.as_deref(), Some("10"));
        }
        other => panic!("unexpected ticker message: {other:?}"),
    }

    let candle = parse_poloniex_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &json!({
            "channel": "candles_minute_1",
            "data": [[
                "BTC_USDT_PERP",
                "91883.46",
                "91958.73",
                "91883.46",
                "91958.73",
                "367.68438",
                "4",
                2,
                1741243200000i64,
                1741243259999i64,
                1741243218348i64
            ]]
        }),
    )
    .expect("candle");
    match candle {
        PoloniexPublicStreamMessage::Candle(candle) => {
            assert_eq!(candle.interval, "1m");
            assert_eq!(candle.close, "91958.73");
            assert_eq!(candle.volume, "4");
        }
        other => panic!("unexpected candle message: {other:?}"),
    }
}

#[test]
fn poloniex_stream_parser_should_parse_private_events() {
    let tenant_id = TenantId::new("tenant").expect("tenant");
    let account_id = AccountId::new("account").expect("account");

    let order_events = parse_poloniex_private_stream_message(
        &exchange_id(),
        tenant_id.clone(),
        account_id.clone(),
        Some(spot_symbol_scope()),
        Some(MarketType::Spot),
        &json!({
            "channel": "orders",
            "data": [{
                "symbol": "BTC_USDT",
                "type": "LIMIT",
                "quantity": "1",
                "orderId": "32471407854219264",
                "tradeFee": "0.1",
                "clientOrderId": "client-1",
                "accountType": "SPOT",
                "feeCurrency": "USDT",
                "eventType": "trade",
                "side": "BUY",
                "filledQuantity": "1",
                "state": "FILLED",
                "tradeTime": 1648708186922i64,
                "tradeAmount": "47112.1",
                "price": "47112.1",
                "tradeQty": "1",
                "tradePrice": "47112.1",
                "tradeId": "fill-1",
                "ts": 1648708187469i64
            }]
        }),
    )
    .expect("order events");
    assert!(matches!(
        &order_events[0],
        ExchangeStreamEvent::OrderUpdate(order) if order.exchange_order_id.as_deref() == Some("32471407854219264")
    ));
    assert!(matches!(
        &order_events[1],
        ExchangeStreamEvent::Fill(fill) if fill.fill_id.as_deref() == Some("fill-1")
    ));

    let balances = parse_poloniex_private_stream_message(
        &exchange_id(),
        tenant_id,
        account_id,
        None,
        Some(MarketType::Spot),
        &json!({
            "channel": "balances",
            "data": [{
                "currency": "BTC",
                "available": "0.5",
                "hold": "0.1",
                "ts": 1657312008443i64
            }]
        }),
    )
    .expect("balances");
    assert!(matches!(
        &balances[0],
        ExchangeStreamEvent::BalanceSnapshot(snapshot) if snapshot.balances[0].balances[0].asset == "BTC"
    ));
}

#[tokio::test]
async fn poloniex_adapter_should_return_stream_subscription_specs() {
    let adapter = PoloniexGatewayAdapter::new(PoloniexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..PoloniexGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-sub"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public subscription");
    assert!(public_id.contains("wss://ws.poloniex.com/ws/v3/public:tickers:BTC_USDT_PERP"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-sub"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("wss://ws.poloniex.com/ws/private:orders:account"));
}

#[test]
fn poloniex_ws_sessions_should_subscribe_ping_and_emit_stream_events() {
    let adapter = PoloniexGatewayAdapter::new(PoloniexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..PoloniexGatewayConfig::default()
    })
    .expect("adapter");

    let mut public_session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-session"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .expect("public session");
    assert_eq!(public_session.initial_requests()[0]["channel"][0], "book");
    assert_eq!(
        public_session.initial_requests()[0]["symbols"][0],
        "BTC_USDT"
    );
    assert_eq!(public_session.heartbeat_request(), r#"{"event":"ping"}"#);

    let now = chrono::Utc::now();
    public_session.on_connected(now);
    assert_eq!(
        public_session.supervisor_action(now, &poloniex_stream_reconnect_policy()),
        StreamSupervisorAction::SendPing
    );

    let public_events = public_session
        .handle_text_message(
            r#"{"channel":"book","data":[{"symbol":"BTC_USDT","asks":[["46100","2"]],"bids":[["46000","1"]],"id":10410,"ts":1652774727337}]}"#,
        )
        .expect("public events");
    assert!(public_events.iter().any(|event| {
        matches!(
            event,
            PoloniexWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(_)))
        )
    }));

    let heartbeat_events = public_session
        .handle_text_message(r#"{"event":"pong"}"#)
        .expect("public heartbeat");
    assert!(heartbeat_events.iter().any(|event| {
        matches!(
            event,
            PoloniexWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));

    let mut private_session = adapter
        .private_ws_session(
            PrivateStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("private-session"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Perpetual),
                account_id: AccountId::new("account").expect("account"),
                kind: PrivateStreamKind::Orders,
            },
            Some(perp_symbol_scope()),
        )
        .expect("private session");
    let initial = private_session.initial_requests();
    assert_eq!(initial[0]["channel"][0], "auth");
    assert_eq!(initial[1]["channel"][0], "orders");
    assert_eq!(private_session.heartbeat_request(), r#"{"event":"ping"}"#);

    private_session.on_connected(now);
    let private_heartbeat = private_session
        .handle_text_message("pong")
        .expect("private heartbeat");
    assert!(private_heartbeat.iter().any(|event| {
        matches!(
            event,
            PoloniexWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));
    assert!(private_session.state().last_pong_at.is_some());

    let private_events = private_session
        .handle_text_message(
            r#"{"channel":"orders","data":[{"symbol":"BTC_USDT_PERP","type":"LIMIT","quantity":"1","orderId":"order-1","clientOrderId":"client-1","eventType":"place","side":"BUY","state":"NEW","price":"46100","ts":1648708187469}]}"#,
        )
        .expect("private events");
    assert!(private_events.iter().any(|event| {
        matches!(
            event,
            PoloniexWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::OrderUpdate(_)))
        )
    }));
}
