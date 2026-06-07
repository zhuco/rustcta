use rustcta_exchange_api::{
    AuthRenewalKind, ExchangeClient, ExchangeStreamEvent, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::Value;

use super::streams::{
    coinstore_futures_socket_io_pong_payload, coinstore_public_subscribe_payload,
    parse_coinstore_private_stream_events, parse_coinstore_private_stream_message,
    parse_coinstore_public_stream_events, parse_coinstore_public_stream_message,
    CoinstoreWsMessage,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{CoinstoreGatewayAdapter, CoinstoreGatewayConfig};

#[test]
fn coinstore_stream_payloads_should_match_official_channels() {
    let payload = coinstore_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-stream"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::Trades,
    })
    .expect("payload");
    assert_eq!(payload["op"], "SUB");
    assert_eq!(payload["channel"][0], "BTCUSDT@trade");

    let futures_payload = coinstore_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("futures-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("payload");
    let text = futures_payload.as_str().expect("socket.io text");
    assert!(text.starts_with("42[\"subscribe\""));
    assert!(text.contains("future_snapshot_depth"));
}

#[tokio::test]
async fn coinstore_private_stream_should_emit_futures_auth_spec() {
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinstoreGatewayConfig::default()
    })
    .expect("adapter");
    let spec = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-stream"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private stream");
    let value: Value = serde_json::from_str(&spec).expect("json");
    assert!(value["payloads"][0]
        .as_str()
        .unwrap()
        .starts_with("42[\"auth\""));
    assert_eq!(coinstore_futures_socket_io_pong_payload(), "3");
}

#[test]
fn coinstore_stream_parser_should_classify_heartbeat_and_book() {
    assert_eq!(
        parse_coinstore_public_stream_message(
            &exchange_id(),
            super::test_support::perp_symbol_scope(),
            &Value::String("2".to_string())
        )
        .expect("ping"),
        CoinstoreWsMessage::Ping
    );
    assert!(matches!(
        parse_coinstore_public_stream_message(
            &exchange_id(),
            spot_symbol_scope(),
            &fixture("public_ws_book")
        )
        .expect("book"),
        CoinstoreWsMessage::OrderBook(_)
    ));
    assert!(matches!(
        parse_coinstore_private_stream_message(&fixture("private_ws_match")),
        CoinstoreWsMessage::Private(_)
    ));
}

#[test]
fn coinstore_public_book_stream_should_emit_standard_snapshot() {
    let events = parse_coinstore_public_stream_events(
        &exchange_id(),
        spot_symbol_scope(),
        &fixture("public_ws_book"),
    )
    .expect("stream events");

    match events.first() {
        Some(ExchangeStreamEvent::OrderBookSnapshot(book)) => {
            assert_eq!(book.order_book.bids[0].price, 100.0);
            assert_eq!(book.order_book.asks[0].quantity, 2.0);
            assert_eq!(
                book.order_book
                    .exchange_symbol
                    .as_ref()
                    .map(|symbol| symbol.symbol.as_str()),
                Some("BTCUSDT")
            );
        }
        other => panic!("expected order book snapshot, got {other:?}"),
    }
}

#[test]
fn coinstore_private_match_stream_should_emit_order_update() {
    let subscription = private_subscription(PrivateStreamKind::Orders);
    let events = parse_coinstore_private_stream_events(
        &exchange_id(),
        subscription.context.tenant_id.clone().expect("tenant"),
        subscription.account_id.clone(),
        &subscription,
        None,
        &fixture("private_ws_match"),
    )
    .expect("stream events");

    match events.first() {
        Some(ExchangeStreamEvent::OrderUpdate(order)) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("fixture-order-1"));
            assert_eq!(order.client_order_id.as_deref(), Some("fixture-client-1"));
        }
        other => panic!("expected order update, got {other:?}"),
    }
}

#[test]
fn coinstore_private_match_stream_should_emit_fill() {
    let subscription = private_subscription(PrivateStreamKind::Fills);
    let events = parse_coinstore_private_stream_events(
        &exchange_id(),
        subscription.context.tenant_id.clone().expect("tenant"),
        subscription.account_id.clone(),
        &subscription,
        None,
        &fixture("ws_private_order"),
    )
    .expect("stream events");

    match events.first() {
        Some(ExchangeStreamEvent::Fill(fill)) => {
            assert_eq!(fill.order_id.as_deref(), Some("1"));
            assert_eq!(fill.client_order_id.as_deref(), Some("cid-1"));
            assert_eq!(fill.price, 100.0);
            assert_eq!(fill.quantity, 0.01);
        }
        other => panic!("expected fill, got {other:?}"),
    }
}

#[tokio::test]
async fn coinstore_private_balance_and_position_streams_should_stay_unsupported() {
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinstoreGatewayConfig::default()
    })
    .expect("adapter");

    let balance_error = adapter
        .subscribe_private_stream(private_subscription(PrivateStreamKind::Balances))
        .await
        .expect_err("balances stream unsupported");
    assert!(format!("{balance_error:?}").contains("coinstore.private_balances_stream"));

    let position_error = adapter
        .subscribe_private_stream(private_subscription(PrivateStreamKind::Positions))
        .await
        .expect_err("positions stream unsupported");
    assert!(format!("{position_error:?}").contains("coinstore.private_positions_stream"));
}

#[test]
fn coinstore_private_capabilities_should_declare_ws_runtime_policy() {
    let adapter = CoinstoreGatewayAdapter::new(CoinstoreGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinstoreGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_streams);
    let private_streams = capabilities
        .private_stream_capabilities
        .as_ref()
        .expect("private stream capabilities");
    assert!(private_streams.supports_orders);
    assert!(private_streams.supports_fills);
    assert!(!private_streams.supports_balances);
    assert!(!private_streams.supports_positions);
    assert!(capabilities
        .capabilities_v2
        .stream_runtime
        .private
        .is_supported());
    assert_eq!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .auth_renewal_policy
            .kind,
        AuthRenewalKind::ReLogin
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .reconnect
            .requires_resubscribe
    );
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
}

fn private_subscription(kind: PrivateStreamKind) -> PrivateStreamSubscription {
    PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-stream"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind,
    }
}

fn fixture(name: &str) -> Value {
    serde_json::from_str(match name {
        "public_ws_book" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/public_ws_book.json")
        }
        "private_ws_match" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/private_ws_match.json")
        }
        "ws_private_order" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/ws_private_order.json")
        }
        _ => panic!("unknown coinstore fixture {name}"),
    })
    .expect("fixture json")
}
