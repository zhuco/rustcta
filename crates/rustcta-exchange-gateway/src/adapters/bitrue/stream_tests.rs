use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::streams::{
    bitrue_private_listen_key_url, bitrue_private_subscribe_payload,
    bitrue_public_subscribe_payload, bitrue_ws_pong_payload, parse_bitrue_private_stream_message,
    parse_bitrue_public_stream_message, BitruePrivateStreamMessage, BitruePrivateWsSession,
    BitruePublicStreamMessage, BitruePublicWsSession, BitrueWsSessionEvent,
};
use super::test_support::{
    assert_signed_bitrue_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    spot_symbol_scope,
};
use super::{BitrueGatewayAdapter, BitrueGatewayConfig};

#[test]
fn bitrue_public_stream_payloads_should_match_official_channels() {
    let spot = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-ws"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = bitrue_public_subscribe_payload(&spot, "spot-1").expect("spot payload");
    assert_eq!(payload["event"], "sub");
    assert_eq!(
        payload["params"]["channel"],
        "market_btcusdt_simple_depth_step0"
    );
    assert_eq!(payload["params"]["cb_id"], "spot-1");

    let futures = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-ws"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Candles {
            interval: "5m".to_string(),
        },
    };
    let payload = bitrue_public_subscribe_payload(&futures, "perp-1").expect("perp payload");
    assert_eq!(payload["method"], "sub");
    assert_eq!(payload["params"][0], "btc_usdt@kline_5m");
}

#[test]
fn bitrue_private_stream_payloads_and_urls_should_match_docs() {
    let spot = private_subscription(Some(MarketType::Spot), PrivateStreamKind::Orders);
    let payload =
        bitrue_private_subscribe_payload(&spot, MarketType::Spot).expect("spot private payload");
    assert_eq!(payload["event"], "sub");
    assert_eq!(payload["params"]["channel"], "user_order_update");
    assert_eq!(
        bitrue_private_listen_key_url("wss://wsapi.bitrue.com/stream", "listen", MarketType::Spot)
            .unwrap(),
        "wss://wsapi.bitrue.com/stream?listenKey=listen"
    );

    let futures = private_subscription(Some(MarketType::Perpetual), PrivateStreamKind::Positions);
    let payload =
        bitrue_private_subscribe_payload(&futures, MarketType::Perpetual).expect("perp payload");
    assert_eq!(payload["params"]["channel"], "user_account_update");
    assert_eq!(
        bitrue_private_listen_key_url(
            "wss://fapiws.bitrue.com/stream",
            "listen",
            MarketType::Perpetual
        )
        .unwrap(),
        "wss://fapiws.bitrue.com/stream?streams=listen"
    );
    assert_eq!(
        bitrue_ws_pong_payload(Some("1635221621062"))["pong"],
        "1635221621062"
    );
}

#[tokio::test]
async fn bitrue_adapter_should_return_stream_subscription_specs() {
    let adapter = BitrueGatewayAdapter::new(BitrueGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BitrueGatewayConfig::default()
    })
    .expect("adapter");

    let public = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-stream"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public stream");
    assert!(public.contains("wss://ws.bitrue.com/market/ws"));
    assert!(public.contains("market_btcusdt_trade_ticker"));

    let private = adapter
        .subscribe_private_stream(private_subscription(
            Some(MarketType::Perpetual),
            PrivateStreamKind::Account,
        ))
        .await
        .expect("private stream");
    assert!(private.contains("wss://fapiws.bitrue.com/stream"));
    assert!(private.contains("user_account_update"));
}

#[test]
fn bitrue_public_stream_parser_should_parse_book_trade_and_heartbeat() {
    let pong = parse_bitrue_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({"ping": "1663815268584"}),
    )
    .expect("pong");
    assert_eq!(pong, BitruePublicStreamMessage::Pong);

    let book = parse_bitrue_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "channel": "market_btcusdt_simple_depth_step0",
            "tick": {
                "buys": [["32000", "0.2"]],
                "sells": [["34001", "2.3"]],
                "ts": 1700000000000_i64
            }
        }),
    )
    .expect("book");
    match book {
        BitruePublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.bids[0].price, 32000.0);
            assert_eq!(snapshot.asks[0].quantity, 2.3);
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let trade = parse_bitrue_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "stream": "btcusdt@trade",
            "data": {
                "t": 12345,
                "p": "43000",
                "q": "0.21",
                "m": false,
                "T": 1700000000000_i64
            }
        }),
    )
    .expect("trade");
    match trade {
        BitruePublicStreamMessage::Trades(rows) => {
            assert_eq!(rows[0].side, OrderSide::Buy);
            assert_eq!(rows[0].quantity, "0.21");
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn bitrue_private_stream_parser_should_parse_order_fill_balance_and_position() {
    let order = parse_bitrue_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "event": "ORDER_TRADE_UPDATE",
            "o": {
                "s": "E-BTC-USDT",
                "i": 2001,
                "c": "client-1",
                "S": "BUY",
                "o": "LIMIT",
                "q": "0.3",
                "p": "42000",
                "z": "0.1",
                "X": "PARTIALLY_FILLED",
                "T": 1700000000000_i64
            }
        }),
    )
    .expect("order");
    let BitruePrivateStreamMessage::Events(events) = order else {
        panic!("expected events");
    };
    assert!(matches!(
        &events[0],
        ExchangeStreamEvent::OrderUpdate(order)
            if order.exchange_order_id.as_deref() == Some("2001")
                && order.status == OrderStatus::PartiallyFilled
    ));

    let fill = parse_bitrue_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "event": "executionReport",
            "data": {
                "s": "E-BTC-USDT",
                "i": 2001,
                "t": 9001,
                "c": "client-1",
                "S": "SELL",
                "L": "43000",
                "l": "0.2",
                "m": true,
                "N": "USDT",
                "n": "0.01",
                "T": 1700000000000_i64
            }
        }),
    )
    .expect("fill");
    let BitruePrivateStreamMessage::Events(events) = fill else {
        panic!("expected events");
    };
    assert!(matches!(
        &events[0],
        ExchangeStreamEvent::Fill(fill)
            if fill.fill_id.as_deref() == Some("9001") && fill.side == OrderSide::Sell
    ));

    let balance = parse_bitrue_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Spot,
        None,
        &json!({
            "e": "outboundAccountInfo",
            "B": [
                {"a": "BTC", "f": "0.1", "l": "0.02"}
            ]
        }),
    )
    .expect("balance");
    let BitruePrivateStreamMessage::Events(events) = balance else {
        panic!("expected events");
    };
    assert!(matches!(
        &events[0],
        ExchangeStreamEvent::BalanceSnapshot(snapshot)
            if snapshot.balances[0].balances[0].asset == "BTC"
    ));

    let position = parse_bitrue_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        None,
        &json!({
            "event": "user_account_update",
            "data": {
                "contractName": "E-BTC-USDT",
                "side": "BUY",
                "volume": "0.5",
                "openPrice": "42000",
                "leverageLevel": 10
            }
        }),
    )
    .expect("position");
    let BitruePrivateStreamMessage::Events(events) = position else {
        panic!("expected events");
    };
    assert!(matches!(
        &events[0],
        ExchangeStreamEvent::PositionSnapshot(snapshot)
            if snapshot.positions[0].exchange_symbol.as_ref().unwrap().symbol == "E-BTC-USDT"
    ));
}

#[test]
fn bitrue_private_stream_parser_should_parse_official_futures_user_events() {
    let order = parse_bitrue_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1689568979259_i64,
            "o": {
                "s": "E-BTC-USDT",
                "c": "client-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "1",
                "p": "100",
                "ap": "100",
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "i": 123,
                "l": "0.4",
                "z": "0.4",
                "L": "100",
                "N": "USDT",
                "n": "0.01",
                "T": 1689568979259_i64
            }
        }),
    )
    .expect("official order update");
    let BitruePrivateStreamMessage::Events(events) = order else {
        panic!("expected events");
    };
    assert!(matches!(
        &events[0],
        ExchangeStreamEvent::OrderUpdate(order)
            if order.status == OrderStatus::PartiallyFilled
                && order.exchange_order_id.as_deref() == Some("123")
    ));
    assert!(matches!(
        &events[1],
        ExchangeStreamEvent::Fill(fill)
            if fill.quantity == 0.4 && fill.fee_asset.as_deref() == Some("USDT")
    ));

    let account = parse_bitrue_private_stream_message(
        &exchange_id(),
        context("tenant").tenant_id.unwrap(),
        context("account").account_id.unwrap(),
        MarketType::Perpetual,
        None,
        &json!({
            "e": "ACCOUNT_UPDATE",
            "E": 1689568979259_i64,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "100", "cw": "90"}],
                "P": [{"s": "E-BTC-USDT", "pa": "0.5", "ep": "100", "ps": "LONG"}]
            }
        }),
    )
    .expect("official account update");
    let BitruePrivateStreamMessage::Events(events) = account else {
        panic!("expected account events");
    };
    assert!(events
        .iter()
        .any(|event| matches!(event, ExchangeStreamEvent::BalanceSnapshot(_))));
    assert!(events
        .iter()
        .any(|event| matches!(event, ExchangeStreamEvent::PositionSnapshot(_))));
}

#[test]
fn bitrue_ws_sessions_should_emit_initial_requests_heartbeat_and_stream_events() {
    let public_sub = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-session"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let mut public = BitruePublicWsSession::new(
        exchange_id(),
        "wss://ws.bitrue.com/market/ws".to_string(),
        public_sub,
    )
    .expect("public session");
    assert_eq!(public.initial_requests()[0]["event"], "sub");
    let heartbeat = public
        .handle_text_message(r#"{"ping":"1663815268584"}"#)
        .expect("heartbeat");
    assert!(matches!(
        &heartbeat[0],
        BitrueWsSessionEvent::HeartbeatResponse(value)
            if value["pong"] == "1663815268584"
    ));
    let book = public
        .handle_text_message(
            r#"{"channel":"market_btcusdt_simple_depth_step0","tick":{"buys":[["1","2"]],"sells":[["3","4"]]}}"#,
        )
        .expect("book");
    assert!(book.iter().any(|event| matches!(
        event,
        BitrueWsSessionEvent::Stream(ExchangeStreamEvent::OrderBookSnapshot(_))
    )));

    let private_sub = private_subscription(Some(MarketType::Perpetual), PrivateStreamKind::Orders);
    let mut private = BitruePrivateWsSession::new(
        exchange_id(),
        "wss://fapiws.bitrue.com/stream".to_string(),
        private_sub,
        MarketType::Perpetual,
        "listen",
    )
    .expect("private session")
    .with_symbol_hint(perp_symbol_scope());
    assert!(private.url.contains("streams=listen"));
    assert_eq!(
        private.initial_requests()[0]["params"]["channel"],
        "user_order_update"
    );
    let heartbeat = private
        .handle_text_message(r#"{"event":"ping","ts":"1635221621062"}"#)
        .expect("private heartbeat");
    assert!(matches!(
        &heartbeat[0],
        BitrueWsSessionEvent::HeartbeatResponse(value)
            if value["event"] == "pong"
    ));
}

#[tokio::test]
async fn bitrue_private_stream_listen_key_lifecycle_should_call_official_endpoints() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "msg": "succ", "data": {"listenKey": "spot-listen"}}),
        json!({"code": 200, "msg": "succ"}),
        json!({"code": 200, "msg": "succ"}),
        json!({"code": 200, "msg": "succ", "data": {"listenKey": "futures-listen"}}),
        json!({"code": 200, "msg": "succ"}),
        json!({"code": 200, "msg": "succ"}),
    ])
    .await;
    let adapter = BitrueGatewayAdapter::new(BitrueGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url.clone(),
        futures_private_ws_auth_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BitrueGatewayConfig::default()
    })
    .expect("adapter");

    let spot = adapter
        .create_private_stream_listen_key(MarketType::Spot)
        .await
        .expect("spot listen key");
    assert_eq!(spot.listen_key, "spot-listen");
    assert_eq!(spot.expires_after_minutes, 60);
    assert!(spot.websocket_url.ends_with("?listenKey=spot-listen"));
    adapter
        .keepalive_private_stream_listen_key(MarketType::Spot, "spot-listen")
        .await
        .expect("spot keepalive");
    adapter
        .close_private_stream_listen_key(MarketType::Spot, "spot-listen")
        .await
        .expect("spot close");

    let futures = adapter
        .create_private_stream_listen_key(MarketType::Perpetual)
        .await
        .expect("futures listen key");
    assert_eq!(futures.listen_key, "futures-listen");
    assert_eq!(futures.expires_after_minutes, 30);
    assert!(futures.websocket_url.ends_with("?streams=futures-listen"));
    adapter
        .keepalive_private_stream_listen_key(MarketType::Perpetual, "futures-listen")
        .await
        .expect("futures keepalive");
    adapter
        .close_private_stream_listen_key(MarketType::Perpetual, "futures-listen")
        .await
        .expect("futures close");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/poseidon/api/v1/listenKey");
    assert_eq!(
        requests[0].headers.get("x-mbx-apikey").map(String::as_str),
        Some("key")
    );
    assert_eq!(requests[1].method, "PUT");
    assert_eq!(requests[1].path, "/poseidon/api/v1/listenKey/spot-listen");
    assert_eq!(requests[2].method, "DELETE");
    assert_eq!(requests[2].path, "/poseidon/api/v1/listenKey/spot-listen");
    assert_signed_bitrue_request(&requests[3], "POST", "/user_stream/api/v1/listenKey");
    assert_signed_bitrue_request(
        &requests[4],
        "PUT",
        "/user_stream/api/v1/listenKey/futures-listen",
    );
    assert_signed_bitrue_request(
        &requests[5],
        "DELETE",
        "/user_stream/api/v1/listenKey/futures-listen",
    );
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
        account_id: context("private-stream").account_id.unwrap(),
        kind,
    }
}
