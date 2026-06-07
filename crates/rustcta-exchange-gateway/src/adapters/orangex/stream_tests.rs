use rustcta_exchange_api::{
    AccountId, ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use serde_json::json;

use super::streams::{
    orangex_private_subscribe_payload, orangex_private_unsubscribe_payload,
    orangex_public_ping_payload, orangex_public_subscribe_payload,
    orangex_public_unsubscribe_payload, parse_orangex_private_stream_event,
    parse_orangex_ws_control_message, OrangeXWsControlMessage, ORANGEX_WS_TEXT_PING,
};
use super::test_support::{context, exchange_id, perp_symbol_scope};
use super::{OrangeXGatewayAdapter, OrangeXGatewayConfig};

#[test]
fn orangex_public_stream_payload_should_map_orderbook_and_trades_channels() {
    let book_subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("book-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = orangex_public_subscribe_payload(&book_subscription, 7).expect("payload");
    assert_eq!(payload["method"], "/public/subscribe");
    assert_eq!(
        payload["params"]["channels"][0],
        "book.BTC-USDT-PERPETUAL.100ms"
    );

    let trades_subscription = PublicStreamSubscription {
        kind: PublicStreamKind::Trades,
        ..book_subscription
    };
    let payload = orangex_public_subscribe_payload(&trades_subscription, 8).expect("payload");
    assert_eq!(
        payload["params"]["channels"][0],
        "trades.BTC-USDT-PERPETUAL.raw"
    );
}

#[test]
fn orangex_public_stream_payload_should_include_unsubscribe_and_heartbeat_specs() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("stream-control"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    };

    let unsubscribe = orangex_public_unsubscribe_payload(&subscription, 9).expect("unsubscribe");
    assert_eq!(unsubscribe["method"], "/public/unsubscribe");
    assert_eq!(
        unsubscribe["params"]["channels"][0],
        "ticker.BTC-USDT-PERPETUAL.raw"
    );

    let ping = orangex_public_ping_payload(10);
    assert_eq!(ORANGEX_WS_TEXT_PING, "PING");
    assert_eq!(ping["method"], "/public/ping");
    assert_eq!(ping["params"], json!({}));
}

#[test]
fn orangex_ws_control_parser_should_read_ping_ack_and_subscription_messages() {
    let ping_ack = json!({
        "jsonrpc": "2.0",
        "id": "16",
        "result": {}
    });
    assert_eq!(
        parse_orangex_ws_control_message(&ping_ack).expect("parse"),
        Some(OrangeXWsControlMessage::Pong)
    );

    let subscribe_ack = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": ["trades.BTC-USDT-PERPETUAL.raw"]
    });
    assert_eq!(
        parse_orangex_ws_control_message(&subscribe_ack).expect("parse"),
        Some(OrangeXWsControlMessage::SubscriptionAck {
            channels: vec!["trades.BTC-USDT-PERPETUAL.raw".to_string()]
        })
    );

    let event = json!({
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "book.BTC-USDT-PERPETUAL.raw",
            "data": {
                "change_id": 1566764
            }
        }
    });
    assert_eq!(
        parse_orangex_ws_control_message(&event).expect("parse"),
        Some(OrangeXWsControlMessage::SubscriptionEvent {
            channel: "book.BTC-USDT-PERPETUAL.raw".to_string(),
            data: json!({ "change_id": 1566764 })
        })
    );
}

#[tokio::test]
async fn orangex_adapter_should_ack_public_stream_request_spec() {
    let adapter = OrangeXGatewayAdapter::default_public().expect("adapter");
    let ack = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("ticker-stream"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("ack");
    assert!(ack.contains("orangex:wss://api.orangex.com/ws/api/v1"));
    assert!(ack.contains("ticker.BTC-USDT-PERPETUAL.raw"));
}

#[test]
fn orangex_private_stream_payload_should_map_account_channels_and_token() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-orders"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let payload =
        orangex_private_subscribe_payload(&subscription, MarketType::Perpetual, "token", 11)
            .expect("subscribe");
    assert_eq!(payload["method"], "/private/subscribe");
    assert_eq!(payload["params"]["access_token"], "token");
    assert_eq!(
        payload["params"]["channels"][0],
        "user.changes.perpetual.PERPETUAL.raw"
    );

    let unsubscribe =
        orangex_private_unsubscribe_payload(&subscription, MarketType::Perpetual, "token", 12)
            .expect("unsubscribe");
    assert_eq!(unsubscribe["method"], "/private/unsubscribe");
    assert_eq!(
        unsubscribe["params"]["channels"][0],
        "user.changes.perpetual.PERPETUAL.raw"
    );

    let balances = PrivateStreamSubscription {
        kind: PrivateStreamKind::Balances,
        market_type: Some(MarketType::Spot),
        ..subscription
    };
    let payload = orangex_private_subscribe_payload(&balances, MarketType::Spot, "token", 13)
        .expect("balances");
    assert_eq!(payload["params"]["channels"][0], "user.asset.SPOT");
}

#[test]
fn orangex_private_stream_parser_should_convert_user_changes_to_standard_events() {
    let data = json!({
        "orders": [{
            "instrument_name": "BTC-USDT-PERPETUAL",
            "order_id": "order-1",
            "custom_order_id": "client-1",
            "direction": "buy",
            "order_state": "open",
            "amount": "0.01",
            "filled_amount": "0",
            "order_type": "limit",
            "price": "44000"
        }],
        "trades": [{
            "instrument_name": "BTC-USDT-PERPETUAL",
            "order_id": "order-1",
            "trade_id": "trade-1",
            "direction": "buy",
            "amount": "0.01",
            "price": "44000"
        }]
    });

    let events = parse_orangex_private_stream_event(
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &exchange_id(),
        MarketType::Perpetual,
        "user.changes.perpetual.PERPETUAL.raw",
        &data,
    )
    .expect("events");

    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], ExchangeStreamEvent::OrderUpdate(_)));
    assert!(matches!(events[1], ExchangeStreamEvent::Fill(_)));
}

#[tokio::test]
async fn orangex_adapter_should_reject_deprecated_private_stream_runtime() {
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        access_token: Some("token".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-stream"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Account,
        })
        .await
        .expect_err("deprecated private websocket must stay unsupported");

    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { operation }
            if operation == "orangex.subscribe_private_stream.deprecated_official_websocket_api"
    ));
}
