use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderStatus};
use serde_json::{json, Value};

use super::streams::{
    bydfi_private_ws_login_payload, bydfi_public_subscribe_payload,
    bydfi_public_unsubscribe_payload, bydfi_public_ws_heartbeat_spec, bydfi_public_ws_ping_payload,
    parse_bydfi_private_stream_message, parse_bydfi_public_stream_message,
    BYDFI_PUBLIC_WS_MAX_MISSED_PONGS, BYDFI_PUBLIC_WS_PING_INTERVAL_SECONDS,
};
use super::test_support::{context, exchange_id, perp_symbol_scope};
use super::{BydfiGatewayAdapter, BydfiGatewayConfig};

#[tokio::test]
async fn bydfi_adapter_should_build_public_websocket_specs() {
    let adapter = BydfiGatewayAdapter::default_public().expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws-book"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert!(adapter.capabilities().supports_public_streams);
    assert_eq!(
        bydfi_public_subscribe_payload(&subscription, 1).expect("subscribe"),
        json!({"id": 1, "method": "SUBSCRIBE", "params": ["BTC-USDT@depth"]})
    );
    assert_eq!(
        bydfi_public_unsubscribe_payload(&subscription, 2).expect("unsubscribe"),
        json!({"id": 2, "method": "UNSUBSCRIBE", "params": ["BTC-USDT@depth"]})
    );

    let spec = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect("ws spec");
    let spec: Value = serde_json::from_str(&spec).expect("json");
    assert_eq!(spec["url"], "wss://stream.bydfi.com/v1/public/fapi");
    assert_eq!(spec["heartbeat"], bydfi_public_ws_heartbeat_spec());
}

#[tokio::test]
async fn bydfi_adapter_should_keep_private_ws_unsupported_without_documented_url() {
    let adapter = BydfiGatewayAdapter::new(BydfiGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_streams: false,
        ..BydfiGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private ws should stay unsupported");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[test]
fn bydfi_adapter_should_build_private_ws_login_payload() {
    let payload = bydfi_private_ws_login_payload("sample-key", "sample-secret", "1735660800000", 7)
        .expect("login");
    assert_eq!(payload["id"], 7);
    assert_eq!(payload["method"], "LOGIN");
    assert_eq!(payload["params"]["apiKey"], "sample-key");
    assert!(payload["params"]["sign"]
        .as_str()
        .is_some_and(|signature| signature.len() == 64));
}

#[test]
fn bydfi_adapter_should_document_public_heartbeat_contract() {
    assert_eq!(BYDFI_PUBLIC_WS_PING_INTERVAL_SECONDS, 30);
    assert_eq!(BYDFI_PUBLIC_WS_MAX_MISSED_PONGS, 2);
    assert_eq!(
        bydfi_public_ws_ping_payload(3),
        json!({"id": 3, "method": "ping"})
    );
}

#[test]
fn bydfi_adapter_should_parse_public_stream_heartbeat_and_depth() {
    let heartbeat = parse_bydfi_public_stream_message(
        &exchange_id(),
        &perp_symbol_scope(),
        &json!({"id": 1, "result": "pong"}),
    )
    .expect("heartbeat")
    .expect("event");
    assert!(matches!(heartbeat, ExchangeStreamEvent::Heartbeat { .. }));

    let depth = parse_bydfi_public_stream_message(
        &exchange_id(),
        &perp_symbol_scope(),
        &json!({
            "e": "depthUpdate",
            "E": 1775541541009_i64,
            "s": "BTC-USDT",
            "b": [["65000", "0.2"]],
            "a": [["65001", "0.4"]]
        }),
    )
    .expect("depth")
    .expect("event");
    let ExchangeStreamEvent::OrderBookSnapshot(book) = depth else {
        panic!("expected order book snapshot");
    };
    assert_eq!(book.order_book.bids[0].price, 65000.0);
}

#[test]
fn bydfi_adapter_should_parse_private_stream_fixtures() {
    let account: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/ws/account_update.json"
    ))
    .expect("account fixture");
    let account_events = parse_bydfi_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &account,
    )
    .expect("account events");
    assert_eq!(account_events.len(), 2);

    let order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bydfi/ws/order_trade_update.json"
    ))
    .expect("order fixture");
    let events = parse_bydfi_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &order,
    )
    .expect("order events");
    let ExchangeStreamEvent::OrderUpdate(order) = &events[0] else {
        panic!("expected order update");
    };
    assert_eq!(order.exchange_order_id.as_deref(), Some("9001"));
    assert_eq!(order.status, OrderStatus::New);
}
