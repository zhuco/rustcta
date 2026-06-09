use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::streams::{
    parse_woo_private_stream_message, parse_woo_public_stream_message, woo_ping_payload,
    woo_private_subscribe_payload, woo_public_subscribe_payload, WooPublicStreamMessage,
    WooWsSessionEvent,
};
use super::test_support::{
    assert_signed_woo_request, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::{WooGatewayAdapter, WooGatewayConfig};
use crate::request_spec::RequestSpec;

#[test]
fn woo_stream_payloads_should_match_v3_topics() {
    let trade = woo_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("trade"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Trades,
        },
        7,
    )
    .expect("trade payload");
    assert_eq!(trade["cmd"], "SUBSCRIBE");
    assert_eq!(trade["params"][0], "trade@SPOT_BTC_USDT");

    let bbo = woo_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("bbo"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Ticker,
        },
        8,
    )
    .expect("bbo payload");
    assert_eq!(bbo["params"][0], "bbo@SPOT_BTC_USDT");

    let book = woo_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        },
        9,
    )
    .expect("book payload");
    assert_eq!(book["params"][0], "orderbookupdate@SPOT_BTC_USDT@100");

    let snapshot = woo_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("snapshot"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        },
        10,
    )
    .expect("snapshot payload");
    assert_eq!(snapshot["params"][0], "orderbook@SPOT_BTC_USDT@100");

    let private = woo_private_subscribe_payload(
        &PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        },
        11,
    )
    .expect("private payload");
    assert_eq!(private["params"][0], "executionreport");
}

#[test]
fn woo_stream_parser_should_decode_public_messages() {
    let exchange = exchange_id();
    let symbol = symbol_scope();
    let book = parse_woo_public_stream_message(
        &exchange,
        symbol.clone(),
        &json!({
            "topic": "orderbook10@SPOT_BTC_USDT",
            "ts": 1762308368653_i64,
            "data": {
                "s": "SPOT_BTC_USDT",
                "ts": 1762308368647_i64,
                "bids": [["99.5", "2.0"]],
                "asks": [["100.5", "1.5"]]
            }
        }),
    )
    .expect("book");
    match book {
        WooPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.bids[0].price, 99.5);
            assert_eq!(snapshot.asks[0].quantity, 1.5);
        }
        other => panic!("unexpected book message: {other:?}"),
    }

    let trade = parse_woo_public_stream_message(
        &exchange,
        symbol.clone(),
        &json!({
            "topic": "trade@SPOT_BTC_USDT",
            "data": {
                "s": "SPOT_BTC_USDT",
                "px": "42598.27",
                "sx": "0.3",
                "sd": "BUY",
                "src": 1,
                "rpi": false,
                "ts": 1618820361540_i64
            }
        }),
    )
    .expect("trade");
    match trade {
        WooPublicStreamMessage::Trade(trade) => {
            assert_eq!(trade.side, OrderSide::Buy);
            assert_eq!(trade.price, "42598.27");
            assert_eq!(trade.quantity, "0.3");
        }
        other => panic!("unexpected trade message: {other:?}"),
    }

    let candle = parse_woo_public_stream_message(
        &exchange,
        symbol,
        &json!({
            "topic": "kline@SPOT_BTC_USDT@1m",
            "data": {
                "s": "SPOT_BTC_USDT",
                "t": "1m",
                "o": "56948.97",
                "c": "56891.76",
                "h": "56948.97",
                "l": "56889.06",
                "v": "44.00947568",
                "a": "2504584.9",
                "st": 1618822380000_i64,
                "et": 1618822440000_i64
            }
        }),
    )
    .expect("candle");
    assert!(matches!(candle, WooPublicStreamMessage::Candle(_)));
}

#[test]
fn woo_stream_parser_should_decode_public_orderbook_fixtures() {
    let exchange = exchange_id();
    let symbol = symbol_scope();

    let bbo = parse_woo_public_stream_message(
        &exchange,
        symbol.clone(),
        &load_ws_fixture("public_bbo.json"),
    )
    .expect("bbo");
    match bbo {
        WooPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.sequence, Some(1001));
            assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
            assert_eq!(snapshot.best_ask().expect("ask").quantity, 0.75);
        }
        other => panic!("unexpected bbo message: {other:?}"),
    }

    let update = parse_woo_public_stream_message(
        &exchange,
        symbol.clone(),
        &load_ws_fixture("public_orderbookupdate.json"),
    )
    .expect("orderbook update");
    match update {
        WooPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.sequence, Some(1002));
            assert_eq!(snapshot.bids.len(), 1);
            assert_eq!(snapshot.bids[0].price, 64999.9);
            assert_eq!(snapshot.asks[0].price, 65000.3);
        }
        other => panic!("unexpected orderbook update message: {other:?}"),
    }

    let snapshot = parse_woo_public_stream_message(
        &exchange,
        symbol,
        &load_ws_fixture("public_orderbook_100.json"),
    )
    .expect("orderbook snapshot");
    match snapshot {
        WooPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.sequence, Some(1003));
            assert_eq!(snapshot.bids.len(), 2);
            assert_eq!(snapshot.asks.len(), 2);
        }
        other => panic!("unexpected orderbook snapshot message: {other:?}"),
    }
}

#[test]
fn woo_stream_parser_should_decode_private_messages() {
    let exchange = exchange_id();
    let tenant_id = context("private").tenant_id.expect("tenant");
    let account_id = AccountId::new("account").expect("account");
    let symbol = symbol_scope();

    let execution = parse_woo_private_stream_message(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        Some(symbol),
        &json!({
            "topic": "executionreport",
            "ts": 1675406261689_i64,
            "data": {
                "mt": 0,
                "s": "SPOT_BTC_USDT",
                "cid": "client-1",
                "oid": 54774393,
                "t": "LIMIT",
                "sd": "BUY",
                "ps": "BOTH",
                "sx": "1",
                "px": "50000",
                "tid": 56201985,
                "esx": "1",
                "epx": "50000",
                "f": "0.0001",
                "fa": "BTC",
                "tesx": "1",
                "aepx": "50000",
                "ss": "FILLED",
                "ts": 1675406261689_i64,
                "ro": false,
                "mk": true
            }
        }),
    )
    .expect("execution");
    assert!(matches!(
        &execution[0],
        ExchangeStreamEvent::OrderUpdate(order)
            if order.status == OrderStatus::Filled
                && order.exchange_order_id.as_deref() == Some("54774393")
    ));
    assert!(matches!(
        &execution[1],
        ExchangeStreamEvent::Fill(fill)
            if fill.fill_id.as_deref() == Some("56201985")
                && fill.fee_amount == Some(0.0001)
    ));

    let balances = parse_woo_private_stream_message(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        None,
        &json!({
            "topic": "balance",
            "data": {
                "balances": [{
                    "t": "BTC",
                    "h": "100",
                    "f": "0.5",
                    "ts": 1618757713353_i64
                }]
            }
        }),
    )
    .expect("balance");
    assert!(matches!(
        &balances[0],
        ExchangeStreamEvent::BalanceSnapshot(snapshot)
            if snapshot.balances[0].balances[0].asset == "BTC"
                && snapshot.balances[0].balances[0].locked == 0.5
    ));

    let positions = parse_woo_private_stream_message(
        &exchange,
        tenant_id,
        account_id,
        None,
        &json!({
            "topic": "position",
            "data": {
                "positions": [{
                    "s": "PERP_BTC_USDT",
                    "h": "1",
                    "ps": "BOTH",
                    "aop": "50000",
                    "mp": "50100",
                    "pnl": "100",
                    "lv": 10,
                    "ts": 1677814653001_i64
                }]
            }
        }),
    )
    .expect("position");
    assert!(matches!(
        &positions[0],
        ExchangeStreamEvent::PositionSnapshot(snapshot)
            if snapshot.positions[0].quantity == 1.0
    ));
}

#[tokio::test]
async fn woo_adapter_should_subscribe_public_and_private_streams() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "data": {
            "authKey": "listen-key",
            "expiredTime": 123
        }
    })])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        public_ws_url: "wss://public.example/ws".to_string(),
        private_ws_url: "wss://private.example/ws".to_string(),
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        enabled_private_stream: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(
        capabilities
            .private_stream_capabilities
            .as_ref()
            .expect("private stream capabilities")
            .supports_positions
    );

    let public = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public subscribe");
    assert_eq!(public, "woo:wss://public.example/ws:bbo@SPOT_BTC_USDT");

    let private = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private subscribe");
    assert_eq!(
        private,
        "woo:wss://private.example/ws?key=listen-key:balance:account"
    );

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_woo_request(&requests[0], "POST", "/v3/account/listenKey");
    load_request_spec("listen_key.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[0].body.as_ref().expect("body")["type"],
        "WEBSOCKET"
    );
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/woo/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn load_ws_fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/woo/ws/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("ws fixture");
    serde_json::from_str(&text).expect("ws fixture")
}

#[tokio::test]
async fn woo_ws_sessions_should_emit_initial_heartbeat_and_standard_events() {
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        public_ws_url: "wss://public.example/ws".to_string(),
        ..WooGatewayConfig::default()
    })
    .expect("adapter");
    let mut public_session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-session"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .expect("public session");
    assert_eq!(public_session.url, "wss://public.example/ws");
    assert_eq!(
        public_session.initial_requests()[0]["params"][0],
        "orderbook@SPOT_BTC_USDT@100"
    );

    let heartbeat = public_session.heartbeat_request(chrono::Utc::now());
    assert_eq!(heartbeat["cmd"], "PING");
    assert_eq!(
        woo_ping_payload(123456789_i64),
        json!({"cmd": "PING", "ts": 123456789_i64})
    );

    let book_events = public_session
        .handle_text_message(
            &json!({
                "topic": "orderbook10@SPOT_BTC_USDT",
                "data": {
                    "s": "SPOT_BTC_USDT",
                    "ts": 1762308368647_i64,
                    "bids": [["99.5", "2.0"]],
                    "asks": [["100.5", "1.5"]]
                }
            })
            .to_string(),
        )
        .expect("book text");
    assert!(matches!(
        &book_events[0],
        WooWsSessionEvent::Public(WooPublicStreamMessage::OrderBook(_))
    ));
    assert!(matches!(
        &book_events[1],
        WooWsSessionEvent::Stream(events)
            if matches!(&events[0], ExchangeStreamEvent::OrderBookSnapshot(_))
    ));

    let pong_events = public_session
        .handle_text_message(&json!({"cmd": "PONG", "ts": 123456789_i64}).to_string())
        .expect("pong");
    assert!(matches!(
        &pong_events[1],
        WooWsSessionEvent::Stream(events)
            if matches!(&events[0], ExchangeStreamEvent::Heartbeat { .. })
    ));
}

#[tokio::test]
async fn woo_private_ws_session_should_use_listen_key_and_emit_private_events() {
    let (base_url, _seen) = spawn_rest_server(vec![json!({
        "success": true,
        "data": {
            "authKey": "listen-key",
            "expiredTime": 123
        }
    })])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        private_ws_url: "wss://private.example/ws".to_string(),
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        enabled_private_stream: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");
    let mut private_session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-session"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private session")
        .with_symbol_hint(symbol_scope());
    assert_eq!(
        private_session.url,
        "wss://private.example/ws?key=listen-key"
    );
    assert_eq!(
        private_session.initial_requests()[0]["params"][0],
        "executionreport"
    );

    let execution_events = private_session
        .handle_text_message(
            &json!({
                "topic": "executionreport",
                "data": {
                    "mt": 0,
                    "s": "SPOT_BTC_USDT",
                    "cid": "client-1",
                    "oid": 54774393,
                    "t": "LIMIT",
                    "sd": "BUY",
                    "ps": "BOTH",
                    "sx": "1",
                    "px": "50000",
                    "tid": 56201985,
                    "esx": "1",
                    "epx": "50000",
                    "f": "0.0001",
                    "fa": "BTC",
                    "tesx": "1",
                    "aepx": "50000",
                    "ss": "FILLED",
                    "ts": 1675406261689_i64,
                    "ro": false,
                    "mk": false
                }
            })
            .to_string(),
        )
        .expect("private execution");
    assert!(matches!(
        &execution_events[0],
        WooWsSessionEvent::Private(events)
            if matches!(&events[0], ExchangeStreamEvent::OrderUpdate(order)
                if order.status == OrderStatus::Filled)
    ));
    assert!(matches!(
        &execution_events[1],
        WooWsSessionEvent::Stream(events)
            if matches!(&events[1], ExchangeStreamEvent::Fill(fill)
                if fill.fill_id.as_deref() == Some("56201985"))
    ));
}
