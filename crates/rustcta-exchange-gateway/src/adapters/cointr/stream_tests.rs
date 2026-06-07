use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeSymbol, MarketType};
use serde_json::json;

use super::streams::{
    parse_private_stream_message, parse_public_stream_message, ping_payload, pong_payload,
    private_login_payload, private_subscribe_payload, public_subscribe_payload,
    CointrPrivateStreamMessage, CointrPublicStreamMessage, CointrWsSessionEvent,
};
use super::test_support::{context, exchange_id, load_fixture, symbol_scope};
use super::{CointrGatewayAdapter, CointrGatewayConfig};

#[test]
fn cointr_public_stream_payloads_should_map_channels() {
    let book = public_subscription(PublicStreamKind::OrderBookDelta);
    let payload = public_subscribe_payload(&book).expect("payload");
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["args"][0]["instType"], "SPOT");
    assert_eq!(payload["args"][0]["channel"], "books5");
    assert_eq!(payload["args"][0]["instId"], "BTCUSDT");

    let trades = public_subscription(PublicStreamKind::Trades);
    let payload = public_subscribe_payload(&trades).expect("payload");
    assert_eq!(payload["args"][0]["channel"], "trade");

    let futures_ticker = public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-stream"),
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange_id(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
            exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
                .expect("symbol"),
        },
        kind: PublicStreamKind::Ticker,
    })
    .expect("futures ticker");
    assert_eq!(futures_ticker["args"][0]["instType"], "USDT-FUTURES");
    assert_eq!(futures_ticker["args"][0]["channel"], "ticker");
}

#[test]
fn cointr_private_stream_payloads_should_map_channels() {
    let orders = private_subscription(PrivateStreamKind::Orders);
    let payload = private_subscribe_payload(&orders).expect("orders");
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["args"][0]["channel"], "user.orders");
    let balances = private_subscription(PrivateStreamKind::Balances);
    let payload = private_subscribe_payload(&balances).expect("balances");
    assert_eq!(payload["args"][0]["channel"], "user.account");
}

#[test]
fn cointr_private_ws_login_payload_should_sign_verify_instruction() {
    let payload =
        private_login_payload("key", "secret", "passphrase", 1_700_000_000).expect("login");
    assert_eq!(payload["op"], "login");
    assert_eq!(payload["args"][0]["apiKey"], "key");
    assert_eq!(payload["args"][0]["passphrase"], "passphrase");
    assert_eq!(payload["args"][0]["timestamp"], "1700000000");
    assert_eq!(payload["args"][0]["sign"].as_str().unwrap().len(), 44);
}

#[test]
fn cointr_stream_parser_should_parse_book_order_and_control_messages() {
    let ack = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({"event": "subscribe", "arg": {"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"}}),
    )
    .expect("ack");
    assert!(matches!(
        ack,
        CointrPublicStreamMessage::SubscriptionAck { .. }
    ));

    let book = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &load_fixture("ws/public_book.json"),
    )
    .expect("book");
    match book {
        CointrPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.order_book.sequence, None);
            assert!(book.order_book.exchange_timestamp.is_some());
        }
        other => panic!("unexpected {other:?}"),
    }

    let private = parse_private_stream_message(
        &exchange_id(),
        Some(symbol_scope()),
        &load_fixture("ws/private_order.json"),
    )
    .expect("private");
    match private {
        CointrPrivateStreamMessage::Events(events) => {
            assert_eq!(events.len(), 1);
        }
        other => panic!("unexpected {other:?}"),
    }

    assert!(ping_payload().get("ping").is_some());
    assert!(pong_payload().get("pong").is_some());
}

#[test]
fn cointr_public_stream_parser_should_parse_trade_ticker_and_candle_messages() {
    let trade = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "trade", "instId": "BTCUSDT"},
            "data": [{
                "ts": "1695709835822",
                "price": "26293.4",
                "size": "0.0013",
                "side": "buy",
                "tradeId": "1000000000"
            }],
            "ts": "1695711090682"
        }),
    )
    .expect("trade");
    match trade {
        CointrPublicStreamMessage::Trades(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].price, "26293.4");
            assert_eq!(rows[0].quantity, "0.0013");
            assert_eq!(rows[0].trade_id.as_deref(), Some("1000000000"));
        }
        other => panic!("unexpected trade event: {other:?}"),
    }

    let ticker = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [{
                "lastPr": "26293.4",
                "open24h": "26000",
                "high24h": "26300",
                "low24h": "25900",
                "baseVolume": "12.3",
                "quoteVolume": "323000",
                "ts": "1695711090682"
            }]
        }),
    )
    .expect("ticker");
    match ticker {
        CointrPublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price.as_deref(), Some("26293.4"));
            assert_eq!(ticker.volume.as_deref(), Some("12.3"));
        }
        other => panic!("unexpected ticker event: {other:?}"),
    }

    let candle = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "candle1m", "instId": "BTCUSDT"},
            "data": [[
                "1695672780000",
                "2200.1",
                "2210.2",
                "2190.3",
                "2205.4",
                "10",
                "22054",
                "22054",
                "1"
            ]],
            "ts": "1695702747821"
        }),
    )
    .expect("candle");
    match candle {
        CointrPublicStreamMessage::Candle(candle) => {
            assert_eq!(candle.interval.as_deref(), Some("1m"));
            assert_eq!(candle.open, "2200.1");
            assert_eq!(candle.close, "2205.4");
            assert_eq!(candle.quote_volume.as_deref(), Some("22054"));
        }
        other => panic!("unexpected candle event: {other:?}"),
    }
}

#[test]
fn cointr_private_stream_parser_should_parse_fill_balance_and_position_events() {
    let fill = parse_private_stream_message(
        &exchange_id(),
        Some(symbol_scope()),
        &json!({
            "arg": {"channel": "user.fills"},
            "data": {
                "tradeId": "t1",
                "orderId": "1001",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "price": "65000",
                "qty": "0.01",
                "commission": "0.01",
                "commissionAsset": "USDT"
            }
        }),
    )
    .expect("fill");
    assert!(matches!(fill, CointrPrivateStreamMessage::Events(events) if events.len() == 1));

    let balance = parse_private_stream_message(
        &exchange_id(),
        Some(symbol_scope()),
        &json!({
            "arg": {"channel": "user.account"},
            "data": [{"coin": "USDT", "free": "10", "locked": "2"}]
        }),
    )
    .expect("balance");
    assert!(matches!(balance, CointrPrivateStreamMessage::Events(events) if events.len() == 1));

    let position = parse_private_stream_message(
        &exchange_id(),
        Some(symbol_scope()),
        &json!({
            "arg": {"channel": "user.positions"},
            "data": [{
                "symbol": "BTCUSDT",
                "total": "0.02",
                "holdSide": "LONG",
                "entryPrice": "65000",
                "markPrice": "65100"
            }]
        }),
    )
    .expect("position");
    assert!(matches!(position, CointrPrivateStreamMessage::Events(events) if events.len() == 1));
}

#[test]
fn cointr_ws_sessions_should_prepare_requests_and_respond_to_heartbeat() {
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CointrGatewayConfig::default()
    })
    .expect("adapter");

    let mut public = adapter
        .public_ws_session(public_subscription(PublicStreamKind::OrderBookSnapshot))
        .expect("public session");
    assert_eq!(public.initial_requests()[0]["args"][0]["channel"], "books5");
    let events = public
        .handle_text_message(r#"{"ping":1700000000000}"#)
        .expect("ping");
    assert!(matches!(
        &events[0],
        CointrWsSessionEvent::Outbound(payload) if payload.get("pong").is_some()
    ));

    let mut private = adapter
        .private_ws_session(
            private_subscription(PrivateStreamKind::Orders),
            Some(symbol_scope()),
        )
        .expect("private session");
    let initial = private.initial_requests();
    assert_eq!(initial[0]["op"], "login");
    assert_eq!(initial[1]["args"][0]["channel"], "user.orders");
    let events = private
        .handle_text_message(
            r#"{"arg":{"channel":"user.orders"},"data":{"orderId":"1001","symbol":"BTCUSDT","side":"BUY","type":"LIMIT","status":"NEW","origQty":"0.01","executedQty":"0","price":"65000"}}"#,
        )
        .expect("order");
    assert!(matches!(&events[0], CointrWsSessionEvent::Private(_)));
    assert!(matches!(&events[1], CointrWsSessionEvent::Stream(events) if !events.is_empty()));
}

#[tokio::test]
async fn cointr_adapter_should_ack_stream_subscriptions() {
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CointrGatewayConfig::default()
    })
    .expect("adapter");
    let public_id = adapter
        .subscribe_public_stream(public_subscription(PublicStreamKind::OrderBookDelta))
        .await
        .expect("public");
    assert!(public_id.contains("books5"));
    let private_id = adapter
        .subscribe_private_stream(private_subscription(PrivateStreamKind::Orders))
        .await
        .expect("private");
    assert!(private_id.contains("user.orders"));
}

fn public_subscription(kind: PublicStreamKind) -> PublicStreamSubscription {
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-stream"),
        symbol: symbol_scope(),
        kind,
    }
}

fn private_subscription(kind: PrivateStreamKind) -> PrivateStreamSubscription {
    PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-stream"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind,
    }
}
