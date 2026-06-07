use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, TenantId};
use serde_json::json;

use super::streams::{
    parse_private_stream_message, parse_public_stream_message, ping_payload, pong_payload,
    private_subscribe_payload, public_subscribe_payload, BiconomyPrivateStreamMessage,
    BiconomyPublicStreamMessage, BiconomyWsSessionEvent,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{BiconomyGatewayAdapter, BiconomyGatewayConfig};

#[test]
fn biconomy_public_stream_payloads_should_map_channels() {
    let book = public_subscription(PublicStreamKind::OrderBookDelta);
    let payload = public_subscribe_payload(&book).expect("payload");
    assert_eq!(payload["method"], "depth.subscribe");
    assert_eq!(payload["params"], json!(["BTC_USDT", 50, "0.01"]));

    let trades = public_subscription(PublicStreamKind::Trades);
    let payload = public_subscribe_payload(&trades).expect("payload");
    assert_eq!(payload["method"], "deals.subscribe");
    assert_eq!(payload["params"], json!(["BTC_USDT"]));

    let candles = public_subscription(PublicStreamKind::Candles {
        interval: "15m".to_string(),
    });
    let payload = public_subscribe_payload(&candles).expect("payload");
    assert_eq!(payload["method"], "kline.subscribe");
    assert_eq!(payload["params"], json!(["BTC_USDT", 900]));
}

#[test]
fn biconomy_private_stream_payloads_should_map_channels() {
    let orders = private_subscription(PrivateStreamKind::Orders);
    let payload = private_subscribe_payload(&orders).expect("orders");
    assert_eq!(payload["channel"], "spot/user.order");
    let balances = private_subscription(PrivateStreamKind::Balances);
    let payload = private_subscribe_payload(&balances).expect("balances");
    assert_eq!(payload["channel"], "spot/user.balance");
}

#[test]
fn biconomy_stream_parser_should_parse_book_order_and_control_messages() {
    let ack = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({"error": null, "result": {"status": "success"}, "id": 2066}),
    )
    .expect("ack");
    assert!(matches!(
        ack,
        BiconomyPublicStreamMessage::SubscriptionAck { .. }
    ));

    let book = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "method": "depth.update",
            "params": [
                true,
                {
                    "bids": [["65000", "1"]],
                    "asks": [["65001", "1"]],
                    "lastUpdateId": 10
                },
                "BTC_USDT"
            ],
            "id": null
        }),
    )
    .expect("book");
    match book {
        BiconomyPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.order_book.sequence, Some(10));
        }
        other => panic!("unexpected {other:?}"),
    }

    let private = parse_private_stream_message(
        &exchange_id(),
        tenant_id(),
        account_id(),
        Some(symbol_scope()),
        &json!({
            "channel": "spot/user.order",
            "data": {
                "orderId": "1001",
                "clientOrderId": "CID1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "status": "NEW",
                "origQty": "0.01",
                "executedQty": "0",
                "price": "65000"
            }
        }),
    )
    .expect("private");
    match private {
        BiconomyPrivateStreamMessage::Events(events) => {
            assert_eq!(events.len(), 1);
        }
        other => panic!("unexpected {other:?}"),
    }

    let balances = parse_private_stream_message(
        &exchange_id(),
        tenant_id(),
        account_id(),
        None,
        &json!({
            "channel": "spot/user.balance",
            "data": [
                {"coin": "USDT", "free": "100.5", "locked": "2.5"},
                {"coin": "BTC", "free": "0.25", "locked": "0.05"}
            ]
        }),
    )
    .expect("balance");
    match balances {
        BiconomyPrivateStreamMessage::Events(events) => match &events[0] {
            ExchangeStreamEvent::BalanceSnapshot(snapshot) => {
                assert_eq!(snapshot.balances[0].balances.len(), 2);
                assert_eq!(snapshot.balances[0].tenant_id, tenant_id());
                assert_eq!(snapshot.balances[0].account_id, account_id());
            }
            other => panic!("unexpected {other:?}"),
        },
        other => panic!("unexpected {other:?}"),
    }

    assert_eq!(ping_payload()["method"], "server.ping");
    assert!(pong_payload().get("pong").is_some());
}

#[test]
fn biconomy_public_stream_parser_should_parse_trade_ticker_and_candle_updates() {
    let trades = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "method": "deals.update",
            "params": [
                "BTC_USDT",
                [
                    {
                        "amount": "0.2",
                        "time": 1660924893.1702139,
                        "id": 848000900,
                        "type": "buy",
                        "price": "21446.35"
                    },
                    {
                        "amount": "0.02",
                        "time": 1660924901.0485499,
                        "id": 848002029,
                        "type": "sell",
                        "price": "21445.84"
                    }
                ]
            ],
            "id": null
        }),
    )
    .expect("trades");
    match trades {
        BiconomyPublicStreamMessage::Trades(trades) => {
            assert_eq!(trades.len(), 2);
            assert_eq!(trades[0].trade_id.as_deref(), Some("848000900"));
            assert_eq!(trades[1].side, rustcta_types::OrderSide::Sell);
        }
        other => panic!("unexpected {other:?}"),
    }

    let ticker = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "method": "state.update",
            "params": [
                "BTC_USDT",
                {
                    "period": 86400,
                    "last": "23728.7",
                    "open": "23901.05",
                    "close": "23902.11",
                    "high": "24411.64",
                    "low": "23727.7",
                    "volume": "25664.43",
                    "deal": "614923080.4154"
                }
            ],
            "id": null
        }),
    )
    .expect("ticker");
    match ticker {
        BiconomyPublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price.as_deref(), Some("23728.7"));
            assert_eq!(ticker.quote_volume.as_deref(), Some("614923080.4154"));
        }
        other => panic!("unexpected {other:?}"),
    }

    let candle = parse_public_stream_message(
        &exchange_id(),
        symbol_scope(),
        &json!({
            "method": "kline.update",
            "params": [[
                1660924800,
                "21444.54",
                "21445.84",
                "21447.87",
                "21437.21",
                "21.74",
                "466178.2626",
                "BTC_USDT"
            ]],
            "id": null
        }),
    )
    .expect("candle");
    match candle {
        BiconomyPublicStreamMessage::Candle(candle) => {
            assert_eq!(candle.open, "21444.54");
            assert_eq!(candle.close, "21445.84");
            assert_eq!(candle.quote_volume.as_deref(), Some("466178.2626"));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn biconomy_ws_sessions_should_prepare_requests_and_respond_to_heartbeat() {
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");

    let mut public = adapter
        .public_ws_session(public_subscription(PublicStreamKind::OrderBookSnapshot))
        .expect("public session");
    assert_eq!(public.initial_requests()[0]["method"], "depth.subscribe");
    let events = public
        .handle_text_message(r#"{"method":"server.ping","params":[],"id":5160}"#)
        .expect("ping");
    assert!(matches!(
        &events[0],
        BiconomyWsSessionEvent::Outbound(payload) if payload.get("pong").is_some()
    ));

    let mut private = adapter
        .private_ws_session(
            private_subscription(PrivateStreamKind::Orders),
            Some(symbol_scope()),
        )
        .expect("private session");
    assert_eq!(private.initial_requests()[0]["channel"], "spot/user.order");
    let events = private
        .handle_text_message(
            r#"{"channel":"spot/user.order","data":{"orderId":"1001","symbol":"BTCUSDT","side":"BUY","type":"LIMIT","status":"NEW","origQty":"0.01","executedQty":"0","price":"65000"}}"#,
        )
        .expect("order");
    assert!(matches!(&events[0], BiconomyWsSessionEvent::Private(_)));
    assert!(matches!(&events[1], BiconomyWsSessionEvent::Stream(events) if !events.is_empty()));

    let mut private_balances = adapter
        .private_ws_session(private_subscription(PrivateStreamKind::Balances), None)
        .expect("private balance session");
    assert_eq!(
        private_balances.initial_requests()[0]["channel"],
        "spot/user.balance"
    );
    let events = private_balances
        .handle_text_message(
            r#"{"channel":"spot/user.balance","data":[{"coin":"USDT","free":"100","locked":"1"}]}"#,
        )
        .expect("balance");
    assert!(matches!(
        &events[1],
        BiconomyWsSessionEvent::Stream(events)
            if matches!(events.first(), Some(ExchangeStreamEvent::BalanceSnapshot(_)))
    ));
}

#[tokio::test]
async fn biconomy_adapter_should_ack_stream_subscriptions() {
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");
    let public_id = adapter
        .subscribe_public_stream(public_subscription(PublicStreamKind::OrderBookDelta))
        .await
        .expect("public");
    assert!(public_id.contains("depth.subscribe"));
    let private_id = adapter
        .subscribe_private_stream(private_subscription(PrivateStreamKind::Orders))
        .await
        .expect("private");
    assert!(private_id.contains("spot/user.order"));
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

fn tenant_id() -> TenantId {
    TenantId::new("tenant").expect("tenant")
}

fn account_id() -> AccountId {
    AccountId::new("account").expect("account")
}
