use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide};
use serde_json::json;

use super::streams::{
    parse_xt_private_stream_message, parse_xt_public_stream_message, xt_private_subscribe_payload,
    xt_public_subscribe_payload, xt_stream_reconnect_policy, xt_ws_ping_text,
    XtPrivateStreamMessage, XtPublicStreamMessage,
};
use super::test_support::{
    actual_request, assert_signed_xt_request, context, exchange_id, perp_symbol_scope,
    request_spec, spawn_rest_server, spot_symbol_scope,
};
use super::{XtGatewayAdapter, XtGatewayConfig};

#[test]
fn xt_public_stream_payloads_should_match_spot_and_futures_docs() {
    let spot = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-public-ws"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spot_payload = xt_public_subscribe_payload(&spot, "spot-1").expect("spot payload");
    assert_eq!(spot_payload["method"], "subscribe");
    assert_eq!(spot_payload["params"][0], "depth_update@btc_usdt");
    assert_eq!(spot_payload["id"], "spot-1");

    let futures = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("futures-public-ws"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    };
    let futures_payload = xt_public_subscribe_payload(&futures, "futures-1").expect("payload");
    assert_eq!(futures_payload["method"], "SUBSCRIBE");
    assert_eq!(futures_payload["params"][0], "agg_ticker@btc_usdt");

    let candle = xt_public_subscribe_payload(
        &PublicStreamSubscription {
            kind: PublicStreamKind::Candles {
                interval: "5m".to_string(),
            },
            ..futures
        },
        "kline-1",
    )
    .expect("kline");
    assert_eq!(candle["params"][0], "kline@btc_usdt,5m");
}

#[test]
fn xt_private_stream_payloads_should_match_spot_and_futures_docs() {
    let spot = private_subscription(Some(MarketType::Spot), PrivateStreamKind::Orders);
    let spot_payload =
        xt_private_subscribe_payload(&spot, MarketType::Spot, "spot-token", "1").unwrap();
    assert_eq!(spot_payload["method"], "subscribe");
    assert_eq!(spot_payload["params"][0], "order");
    assert_eq!(spot_payload["listenKey"], "spot-token");

    let futures = private_subscription(Some(MarketType::Perpetual), PrivateStreamKind::Positions);
    let futures_payload =
        xt_private_subscribe_payload(&futures, MarketType::Perpetual, "listen-key", "2").unwrap();
    assert_eq!(futures_payload["method"], "SUBSCRIBE");
    assert_eq!(futures_payload["params"][0], "position@listen-key");
    assert!(futures_payload.get("listenKey").is_none());

    let spot_positions =
        xt_private_subscribe_payload(&spot, MarketType::Spot, "spot-token", "3").unwrap();
    assert_eq!(spot_positions["params"][0], "order");
    assert_eq!(xt_ws_ping_text(), "ping");
    assert_eq!(
        xt_stream_reconnect_policy(MarketType::Spot).ping_interval_ms,
        50_000
    );
    assert_eq!(
        xt_stream_reconnect_policy(MarketType::Perpetual).ping_interval_ms,
        25_000
    );
}

#[test]
fn xt_stream_parsers_should_ack_heartbeat_and_public_market_data() {
    let pong = parse_xt_public_stream_message(&exchange_id(), spot_symbol_scope(), &json!("pong"))
        .expect("pong");
    assert_eq!(pong, XtPublicStreamMessage::Pong);

    let ack = parse_xt_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({"id": "1", "code": 0, "msg": "success"}),
    )
    .expect("ack");
    assert_eq!(
        ack,
        XtPublicStreamMessage::SubscriptionAck {
            id: Some("1".to_string())
        }
    );

    let book = parse_xt_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &json!({
            "topic": "depth_update",
            "event": "depth_update@btc_usdt",
            "data": {
                "s": "btc_usdt",
                "fu": 121,
                "u": 123,
                "a": [["34001", "2.3"]],
                "b": [["32000", "0.2"]],
                "t": 1700000000000_i64
            }
        }),
    )
    .expect("book");
    match book {
        XtPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.sequence, Some(123));
            assert_eq!(snapshot.asks[0].price, 34001.0);
        }
        other => panic!("unexpected public stream message: {other:?}"),
    }

    let trades = parse_xt_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "topic": "trade",
            "event": "trade@btc_usdt",
            "data": {
                "s": "btc_usdt",
                "i": 6316559590087222000_u64,
                "t": 1700000000000_i64,
                "p": "43000",
                "q": "0.21",
                "b": false
            }
        }),
    )
    .expect("trade");
    match trades {
        XtPublicStreamMessage::Trades(rows) => {
            assert_eq!(rows[0].side, OrderSide::Buy);
            assert_eq!(rows[0].quantity, "0.21");
        }
        other => panic!("unexpected public stream message: {other:?}"),
    }
}

#[test]
fn xt_private_stream_parser_should_parse_order_fill_balance_and_position() {
    let order = parse_xt_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        account_id(),
        MarketType::Spot,
        Some(spot_symbol_scope()),
        &json!({
            "topic": "order",
            "event": "order",
            "data": {
                "s": "btc_usdt",
                "i": "6216559590087220004",
                "ci": "client-1",
                "st": "PARTIALLY_FILLED",
                "sd": "BUY",
                "tp": "LIMIT",
                "oq": "4",
                "eq": "2",
                "p": "4000",
                "ap": "30000",
                "t": 1700000000000_i64
            }
        }),
    )
    .expect("order");
    let XtPrivateStreamMessage::Events(order_events) = order else {
        panic!("expected events");
    };
    assert!(matches!(
        &order_events[0],
        ExchangeStreamEvent::OrderUpdate(order)
            if order.exchange_order_id.as_deref() == Some("6216559590087220004")
                && order.filled_quantity == "2"
    ));

    let fill = parse_xt_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        account_id(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "topic": "trade",
            "event": "trade@listen-key",
            "data": {
                "orderId": "12312312",
                "clientOrderId": "client-2",
                "price": "34244",
                "quantity": "123",
                "timestamp": 1700000000000_i64,
                "symbol": "btc_usdt",
                "orderSide": "BUY",
                "positionSide": "LONG",
                "isMaker": true,
                "fee": 0.0002,
                "feeCoin": "USDT"
            }
        }),
    )
    .expect("fill");
    let XtPrivateStreamMessage::Events(fill_events) = fill else {
        panic!("expected fill events");
    };
    assert!(matches!(
        &fill_events[0],
        ExchangeStreamEvent::Fill(fill)
            if fill.order_id.as_deref() == Some("12312312") && fill.fee_asset.as_deref() == Some("USDT")
    ));

    let balance = parse_xt_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        account_id(),
        MarketType::Spot,
        None,
        &json!({
            "topic": "balance",
            "event": "balance",
            "data": {
                "c": "btc",
                "b": "123",
                "f": "11"
            }
        }),
    )
    .expect("balance");
    let XtPrivateStreamMessage::Events(balance_events) = balance else {
        panic!("expected balance events");
    };
    assert!(matches!(
        &balance_events[0],
        ExchangeStreamEvent::BalanceSnapshot(snapshot)
            if snapshot.balances[0].balances[0].asset == "BTC"
    ));

    let position = parse_xt_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        account_id(),
        MarketType::Perpetual,
        None,
        &json!({
            "topic": "position",
            "event": "position@listen-key",
            "data": {
                "symbol": "btc_usdt",
                "positionSide": "LONG",
                "positionSize": "123",
                "entryPrice": "213",
                "leverage": 20
            }
        }),
    )
    .expect("position");
    let XtPrivateStreamMessage::Events(position_events) = position else {
        panic!("expected position events");
    };
    assert!(matches!(
        &position_events[0],
        ExchangeStreamEvent::PositionSnapshot(snapshot) if snapshot.positions[0].quantity == 123.0
    ));
}

#[tokio::test]
async fn xt_adapter_should_ack_public_and_private_stream_subscriptions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "rc": 0,
            "result": {
                "accessToken": "spot-token"
            }
        }),
        json!({
            "returnCode": 0,
            "result": {
                "listenKey": "futures-listen-key"
            }
        }),
    ])
    .await;
    let adapter = XtGatewayAdapter::new(XtGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..XtGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-stream"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect("public stream");
    assert!(public_id.contains("wss://fstream.xt.com/ws/market:depth@btc_usdt,50,1000ms"));

    let spot_private = adapter
        .subscribe_private_stream(private_subscription(
            Some(MarketType::Spot),
            PrivateStreamKind::Orders,
        ))
        .await
        .expect("spot private");
    assert!(spot_private.contains("wss://stream.xt.com/private:order:account"));

    let futures_private = adapter
        .subscribe_private_stream(private_subscription(
            Some(MarketType::Perpetual),
            PrivateStreamKind::Positions,
        ))
        .await
        .expect("futures private");
    assert!(futures_private
        .contains("wss://fstream.xt.com/ws/user:position@futures-listen-key:account"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_xt_request(&requests[0], "POST", "/v4/ws-token");
    assert_signed_xt_request(&requests[1], "GET", "/future/user/v1/user/listen-key");
    request_spec("spot_ws_token")
        .assert_matches(&actual_request(requests[0].clone()))
        .expect("XT spot private stream token request spec");
    request_spec("futures_listen_key")
        .assert_matches(&actual_request(requests[1].clone()))
        .expect("XT futures listen key request spec");
}

fn private_subscription(
    market_type: Option<MarketType>,
    kind: PrivateStreamKind,
) -> PrivateStreamSubscription {
    PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-stream"),
        exchange: exchange_id(),
        market_type,
        account_id: account_id(),
        kind,
    }
}

fn account_id() -> AccountId {
    AccountId::new("account").expect("account")
}
