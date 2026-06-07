use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderStatus};
use serde_json::{json, Value};

use super::streams::{
    bitunix_futures_private_subscribe_payload, bitunix_futures_public_subscribe_payload,
    bitunix_futures_public_unsubscribe_payload, bitunix_futures_ws_heartbeat_spec,
    bitunix_futures_ws_login_payload, bitunix_futures_ws_ping_payload,
    bitunix_private_stream_capabilities, bitunix_spot_ws_request_payload,
    parse_bitunix_futures_private_stream_message, parse_bitunix_futures_public_stream_message,
    BITUNIX_FUTURES_WS_MAX_MISSED_PINGS, BITUNIX_FUTURES_WS_PING_INTERVAL_SECONDS,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{BitunixGatewayAdapter, BitunixGatewayConfig};

#[tokio::test]
async fn bitunix_adapter_should_build_futures_public_websocket_specs() {
    let adapter = BitunixGatewayAdapter::default_public().expect("adapter");
    let book = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-ws-book"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let ticker = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-ws-ticker"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    };
    assert!(adapter.capabilities().supports_public_streams);
    assert_eq!(
        bitunix_futures_public_subscribe_payload(&book).expect("payload"),
        json!({"op": "subscribe", "args": [{"symbol": "BTCUSDT", "ch": "depth_books"}]})
    );
    assert_eq!(
        bitunix_futures_public_unsubscribe_payload(&book).expect("unsubscribe"),
        json!({"op": "unsubscribe", "args": [{"symbol": "BTCUSDT", "ch": "depth_books"}]})
    );
    assert_eq!(
        bitunix_futures_public_subscribe_payload(&ticker).expect("ticker"),
        json!({"op": "subscribe", "args": [{"symbol": "BTCUSDT", "ch": "ticker"}]})
    );

    let spec = adapter
        .subscribe_public_stream(book)
        .await
        .expect("ws spec");
    let spec: Value = serde_json::from_str(&spec).expect("json");
    assert_eq!(spec["url"], json!("wss://fapi.bitunix.com/public/"));
    assert_eq!(spec["heartbeat"], bitunix_futures_ws_heartbeat_spec());
    assert_eq!(
        spec["payload"],
        json!({"op": "subscribe", "args": [{"symbol": "BTCUSDT", "ch": "depth_books"}]})
    );
}

#[tokio::test]
async fn bitunix_adapter_should_build_private_futures_websocket_specs() {
    let adapter = private_adapter();
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws-orders"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let capabilities = bitunix_private_stream_capabilities();
    assert!(adapter.capabilities().supports_private_streams);
    assert!(capabilities.supports_orders);
    assert!(capabilities.supports_balances);
    assert!(capabilities.supports_positions);
    assert_eq!(
        bitunix_futures_private_subscribe_payload(&subscription).expect("private subscribe"),
        json!({"op": "subscribe", "args": [{"ch": "order"}]})
    );

    let login = bitunix_futures_ws_login_payload("key", "secret", "nonce", "123").expect("login");
    assert_eq!(login["op"], json!("login"));
    assert_eq!(login["args"][0]["apiKey"], json!("key"));
    assert!(login["args"][0]["sign"]
        .as_str()
        .is_some_and(|sign| sign.len() == 64));

    let spec = adapter
        .subscribe_private_stream(subscription)
        .await
        .expect("private ws spec");
    let spec: Value = serde_json::from_str(&spec).expect("json");
    assert_eq!(spec["url"], json!("wss://fapi.bitunix.com/private/"));
    assert_eq!(spec["mode"], json!("login_subscribe"));
    assert_eq!(spec["initial_payloads"][0]["op"], json!("login"));
    assert_eq!(spec["initial_payloads"][1]["args"][0]["ch"], json!("order"));
}

#[test]
fn bitunix_adapter_should_build_spot_ws_request_payloads_with_signature() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-ws-depth"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload =
        bitunix_spot_ws_request_payload(&subscription, "key", "secret", "nonce", "123", 7)
            .expect("spot payload");
    assert_eq!(payload["id"], json!(7));
    assert_eq!(payload["method"], json!("market.depth"));
    assert_eq!(payload["params"]["symbol"], json!("BTCUSDT"));
    assert_eq!(payload["params"]["precision"], json!(5));
    assert_eq!(payload["params"]["apiKey"], json!("key"));
    assert!(payload["params"]["sign"]
        .as_str()
        .is_some_and(|sign| sign.len() == 64));
}

#[test]
fn bitunix_adapter_should_document_futures_heartbeat_contract() {
    assert_eq!(BITUNIX_FUTURES_WS_PING_INTERVAL_SECONDS, 15);
    assert_eq!(BITUNIX_FUTURES_WS_MAX_MISSED_PINGS, 2);
    assert_eq!(
        bitunix_futures_ws_ping_payload(1732519687),
        json!({"op": "ping", "ping": 1732519687})
    );
    let heartbeat = bitunix_futures_ws_heartbeat_spec();
    assert_eq!(heartbeat["client_ping_interval_seconds"], json!(15));
    assert_eq!(
        heartbeat["client_ping_template"],
        json!({"op": "ping", "ping": 0})
    );
}

#[test]
fn bitunix_adapter_should_parse_futures_stream_heartbeat_and_depth() {
    let heartbeat = parse_bitunix_futures_public_stream_message(
        &exchange_id(),
        &perp_symbol_scope(),
        &json!({"op": "ping", "pong": 1732519687, "ping": 1732519690}),
    )
    .expect("heartbeat")
    .expect("event");
    assert!(matches!(heartbeat, ExchangeStreamEvent::Heartbeat { .. }));

    let depth = parse_bitunix_futures_public_stream_message(
        &exchange_id(),
        &perp_symbol_scope(),
        &json!({
            "ch": "depth_book1",
            "symbol": "BTCUSDT",
            "ts": 1775541541009_i64,
            "data": {
                "b": [["7403.89", "0.002"]],
                "a": [["7405.96", "3.340"]]
            }
        }),
    )
    .expect("depth")
    .expect("event");
    let ExchangeStreamEvent::OrderBookSnapshot(book) = depth else {
        panic!("expected order book snapshot");
    };
    assert_eq!(book.order_book.bids[0].price, 7403.89);
    assert_eq!(book.order_book.asks[0].quantity, 3.34);
}

#[test]
fn bitunix_adapter_should_parse_futures_private_order_updates() {
    let events = parse_bitunix_futures_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &json!({
            "ch": "order",
            "ts": 1775541541009_i64,
            "data": {
                "event": "UPDATE",
                "orderId": "9001",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "qty": "0.01",
                "price": "65000",
                "orderStatus": "NEW",
                "clientId": "CID1"
            }
        }),
    )
    .expect("private event");
    assert_eq!(events.len(), 1);
    let ExchangeStreamEvent::OrderUpdate(order) = &events[0] else {
        panic!("expected order update");
    };
    assert_eq!(order.exchange_order_id.as_deref(), Some("9001"));
    assert_eq!(order.status, OrderStatus::New);
}

#[test]
fn bitunix_adapter_should_parse_private_order_push_with_fill() {
    let events = parse_bitunix_futures_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &json!({
            "ch": "order",
            "ts": 1775541541009_i64,
            "data": {
                "event": "TRADE",
                "orderId": "9001",
                "tradeId": "FILL1",
                "symbol": "BTCUSDT",
                "side": "SELL",
                "type": "LIMIT",
                "qty": "0.03",
                "tradeQty": "0.02",
                "price": "65100",
                "orderStatus": "PART_FILLED",
                "clientId": "CID1",
                "fee": "0.013",
                "feeCoin": "USDT",
                "roleType": "MAKER",
                "ctime": 1775541541009_i64
            }
        }),
    )
    .expect("private fill event");
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0],
        ExchangeStreamEvent::OrderUpdate(order)
            if order.exchange_order_id.as_deref() == Some("9001")
                && order.status == OrderStatus::PartiallyFilled
    ));
    let ExchangeStreamEvent::Fill(fill) = &events[1] else {
        panic!("expected fill");
    };
    assert_eq!(fill.fill_id.as_deref(), Some("FILL1"));
    assert_eq!(fill.order_id.as_deref(), Some("9001"));
    assert_eq!(fill.quantity, 0.02);
    assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
}

#[tokio::test]
async fn bitunix_adapter_should_reject_spot_private_streams_and_unsigned_spot_ws() {
    let adapter = BitunixGatewayAdapter::default_public().expect("adapter");
    let unsigned_spot = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-ws"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect_err("spot ws requires credentials");
    assert!(format!("{unsigned_spot:?}").contains("bitunix.spot_ws_public_request"));

    let private_spot = private_adapter()
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("spot private stream unsupported");
    assert!(format!("{private_spot:?}").contains("bitunix.spot_private_stream"));
}

fn private_adapter() -> BitunixGatewayAdapter {
    BitunixGatewayAdapter::new(BitunixGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        enabled_private_streams: true,
        ..BitunixGatewayConfig::default()
    })
    .expect("adapter")
}
