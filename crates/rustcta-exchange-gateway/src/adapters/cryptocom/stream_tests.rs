use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, OrderSide, OrderStatus};
use serde_json::json;

use super::streams::{
    cryptocom_private_subscribe_payload, cryptocom_public_subscribe_payload,
    cryptocom_ws_auth_payload, parse_cryptocom_private_stream_message,
    parse_cryptocom_public_stream_message, CryptoComPublicStreamMessage, CryptoComWsSessionEvent,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CryptoComGatewayAdapter, CryptoComGatewayConfig};

#[test]
fn cryptocom_public_stream_payloads_should_match_official_channels() {
    let book = public_subscription(PublicStreamKind::OrderBookDelta);
    let book_payload = cryptocom_public_subscribe_payload(&book, 1).expect("book");
    assert_eq!(book_payload["method"], "subscribe");
    assert_eq!(
        book_payload["params"]["channels"],
        json!(["book.BTC_USDT.50"])
    );

    let trades = public_subscription(PublicStreamKind::Trades);
    let trades_payload = cryptocom_public_subscribe_payload(&trades, 2).expect("trades");
    assert_eq!(
        trades_payload["params"]["channels"],
        json!(["trade.BTC_USDT"])
    );

    let candle = public_subscription(PublicStreamKind::Candles {
        interval: "1m".to_string(),
    });
    let candle_payload = cryptocom_public_subscribe_payload(&candle, 3).expect("candle");
    assert_eq!(
        candle_payload["params"]["channels"],
        json!(["candlestick.1m.BTC_USDT"])
    );
}

#[test]
fn cryptocom_private_stream_payloads_and_auth_should_match_official_channels() {
    let auth = cryptocom_ws_auth_payload("key", "secret", 1_700_000_000_000, 1);
    assert_eq!(auth["method"], "public/auth");
    assert_eq!(auth["api_key"], "key");
    assert!(auth["sig"]
        .as_str()
        .is_some_and(|signature| signature.len() == 64));

    let orders = private_subscription(PrivateStreamKind::Orders);
    let orders_payload = cryptocom_private_subscribe_payload(&orders, 2).expect("orders");
    assert_eq!(orders_payload["params"]["channels"], json!(["user.order"]));

    let fills = private_subscription(PrivateStreamKind::Fills);
    let fills_payload = cryptocom_private_subscribe_payload(&fills, 3).expect("fills");
    assert_eq!(fills_payload["params"]["channels"], json!(["user.trade"]));
}

#[test]
fn cryptocom_public_stream_parser_should_parse_book_trade_ticker_and_candle() {
    let exchange = exchange_id();
    let symbol = symbol_scope();

    let book = parse_cryptocom_public_stream_message(
        &exchange,
        symbol.clone(),
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "book.BTC_USDT.50",
                "data": [{
                    "bids": [["43000.1", "1.2"]],
                    "asks": [["43001.2", "0.8"]],
                    "t": 1700000000000_i64,
                    "u": 123_u64
                }]
            }
        }),
    )
    .expect("book");
    match book {
        CryptoComPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.sequence, Some(123));
            assert_eq!(snapshot.bids[0].price, 43000.1);
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let trades = parse_cryptocom_public_stream_message(
        &exchange,
        symbol.clone(),
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "trade.BTC_USDT",
                "data": [{
                    "d": "trade-1",
                    "s": "SELL",
                    "p": "43002.1",
                    "q": "0.03",
                    "t": 1700000000100_i64
                }]
            }
        }),
    )
    .expect("trades");
    match trades {
        CryptoComPublicStreamMessage::Trades(trades) => {
            assert_eq!(trades[0].trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trades[0].side, OrderSide::Sell);
            assert_eq!(trades[0].price, "43002.1");
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let ticker = parse_cryptocom_public_stream_message(
        &exchange,
        symbol.clone(),
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "ticker.BTC_USDT",
                "data": [{
                    "a": "43010.0",
                    "b": "43009.5",
                    "k": "43010.5",
                    "v": "10.5",
                    "vv": "451500",
                    "t": 1700000000200_i64
                }]
            }
        }),
    )
    .expect("ticker");
    match ticker {
        CryptoComPublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price.as_deref(), Some("43010.0"));
            assert_eq!(ticker.ask_price.as_deref(), Some("43010.5"));
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let candle = parse_cryptocom_public_stream_message(
        &exchange,
        symbol,
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "candlestick.1m.BTC_USDT",
                "interval": "1m",
                "data": [{
                    "o": "43000",
                    "h": "43100",
                    "l": "42990",
                    "c": "43050",
                    "v": "2.5",
                    "t": 1700000000000_i64
                }]
            }
        }),
    )
    .expect("candle");
    match candle {
        CryptoComPublicStreamMessage::Candle(candle) => {
            assert_eq!(candle.interval, "1m");
            assert_eq!(candle.close, "43050");
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn cryptocom_private_stream_parser_should_parse_order_fill_and_balance() {
    let exchange = exchange_id();
    let symbol = symbol_scope();
    let tenant_id = context("private-stream").tenant_id.expect("tenant");
    let account_id = AccountId::new("account").expect("account");

    let orders = parse_cryptocom_private_stream_message(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        Some(symbol.clone()),
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "user.order.BTC_USDT",
                "data": [{
                    "instrument_name": "BTC_USDT",
                    "order_id": "order-1",
                    "client_oid": "client-1",
                    "side": "BUY",
                    "type": "LIMIT",
                    "status": "ACTIVE",
                    "quantity": "1",
                    "cumulative_quantity": "0.2",
                    "limit_price": "43000",
                    "create_time": 1700000000000_i64,
                    "update_time": 1700000000100_i64
                }]
            }
        }),
    )
    .expect("orders");
    match &orders[0] {
        ExchangeStreamEvent::OrderUpdate(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));
            assert_eq!(order.status, OrderStatus::PartiallyFilled);
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let fills = parse_cryptocom_private_stream_message(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        Some(symbol),
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "user.trade.BTC_USDT",
                "data": [{
                    "instrument_name": "BTC_USDT",
                    "order_id": "order-1",
                    "trade_id": "trade-1",
                    "side": "BUY",
                    "taker_side": "BUY",
                    "traded_price": "43001",
                    "traded_quantity": "0.2",
                    "fees": "1.0",
                    "fee_instrument_name": "USDT",
                    "create_time": 1700000000200_i64
                }]
            }
        }),
    )
    .expect("fills");
    assert!(matches!(fills[0], ExchangeStreamEvent::Fill(_)));

    let balances = parse_cryptocom_private_stream_message(
        &exchange,
        tenant_id,
        account_id,
        None,
        &json!({
            "id": -1,
            "method": "subscribe",
            "result": {
                "channel": "user.balance",
                "data": [{
                    "position_balances": [{
                        "instrument_name": "USDT",
                        "quantity": "100",
                        "reserved_qty": "10",
                        "max_withdrawal_balance": "90"
                    }]
                }]
            }
        }),
    )
    .expect("balances");
    assert!(matches!(
        balances[0],
        ExchangeStreamEvent::BalanceSnapshot(_)
    ));
}

#[tokio::test]
async fn cryptocom_adapter_should_ack_public_and_private_stream_subscriptions() {
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(public_subscription(PublicStreamKind::Trades))
        .await
        .expect("public stream");
    assert!(public_id.contains("trade.BTC_USDT"));

    let private_id = adapter
        .subscribe_private_stream(private_subscription(PrivateStreamKind::Balances))
        .await
        .expect("private stream");
    assert!(private_id.contains("user.balance"));
}

#[test]
fn cryptocom_public_ws_session_should_prepare_requests_and_parse_stream_events() {
    let adapter = CryptoComGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(public_subscription(PublicStreamKind::OrderBookSnapshot))
        .expect("session");
    assert_eq!(session.url, "wss://stream.crypto.com/exchange/v1/market");
    assert_eq!(
        session.initial_requests()[0]["params"]["channels"],
        json!(["book.BTC_USDT.50"])
    );

    session.on_connected(chrono::Utc::now());
    let events = session
        .handle_text_message(
            &json!({
                "id": -1,
                "method": "subscribe",
                "result": {
                    "channel": "book.BTC_USDT.50",
                    "data": [{
                        "bids": [["43000.1", "1.2"]],
                        "asks": [["43001.2", "0.8"]],
                        "t": 1700000000000_i64,
                        "u": 123_u64
                    }]
                }
            })
            .to_string(),
        )
        .expect("events");
    assert!(events.iter().any(|event| matches!(
        event,
        CryptoComWsSessionEvent::Stream(items)
            if matches!(items.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(_)))
    )));
}

#[test]
fn cryptocom_private_ws_session_should_auth_subscribe_and_respond_to_heartbeat() {
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");
    let mut session = adapter
        .private_ws_session(
            private_subscription(PrivateStreamKind::Orders),
            Some(symbol_scope()),
        )
        .expect("session");
    let initial = session.initial_requests();
    assert_eq!(initial[0]["method"], "public/auth");
    assert_eq!(initial[0]["api_key"], "key");
    assert_eq!(initial[1]["params"]["channels"], json!(["user.order"]));

    let events = session
        .handle_text_message(
            &json!({
                "id": 99,
                "method": "public/heartbeat"
            })
            .to_string(),
        )
        .expect("heartbeat");
    assert!(matches!(
        &events[0],
        CryptoComWsSessionEvent::Outbound(payload)
            if payload["method"] == "public/respond-heartbeat" && payload["id"] == 99
    ));
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
        market_type: Some(rustcta_types::MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind,
    }
}
