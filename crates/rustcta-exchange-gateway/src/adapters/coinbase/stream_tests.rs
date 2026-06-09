use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::OrderStatus;
use rustcta_types::{AccountId, CanonicalSymbol, MarketType};
use serde_json::json;

use super::streams::{
    coinbase_private_subscribe_payload, coinbase_public_order_book_ws_policy,
    coinbase_public_subscribe_payload, coinbase_sequence_is_contiguous,
    parse_level2_snapshot_event,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CoinbaseGatewayAdapter, CoinbaseGatewayConfig};

#[test]
fn coinbase_public_subscription_should_map_standard_kinds_to_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = coinbase_public_subscribe_payload(&subscription, None).expect("payload");

    assert_eq!(payload["type"], "subscribe");
    assert_eq!(payload["channel"], "level2");
    assert_eq!(payload["product_ids"][0], "BTC-USD");
    assert!(payload.get("jwt").is_none());
}

#[test]
fn coinbase_private_subscription_should_require_jwt_and_use_user_channel() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let payload =
        coinbase_private_subscribe_payload(&subscription, Some("jwt-token")).expect("payload");

    assert_eq!(payload["type"], "subscribe");
    assert_eq!(payload["channel"], "user");
    assert_eq!(payload["jwt"], "jwt-token");
}

#[test]
fn coinbase_level2_snapshot_parser_should_build_sorted_book() {
    let response = parse_level2_snapshot_event(
        &exchange_id(),
        MarketType::Spot,
        CanonicalSymbol::new("BTC", "USD").expect("canonical"),
        &json!({
            "channel": "level2",
            "timestamp": "2026-06-07T00:00:00Z",
            "sequence_num": 42,
            "events": [{
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "bid", "price_level": "99.0", "new_quantity": "1"},
                    {"side": "bid", "price_level": "100.0", "new_quantity": "2"},
                    {"side": "ask", "price_level": "101.0", "new_quantity": "3"},
                    {"side": "ask", "price_level": "100.5", "new_quantity": "4"}
                ]
            }]
        }),
    )
    .expect("book");

    assert_eq!(response.order_book.sequence, Some(42));
    assert_eq!(response.order_book.bids[0].price, 100.0);
    assert_eq!(response.order_book.asks[0].price, 100.5);
}

#[test]
fn coinbase_public_order_book_ws_policy_should_describe_level2_resync() {
    let policy = coinbase_public_order_book_ws_policy();

    assert_eq!(policy.public_channel, "level2");
    assert_eq!(policy.heartbeat_channel, "heartbeats");
    assert_eq!(policy.public_interval_ms, None);
    assert_eq!(policy.sequence_field, "sequence_num");
    assert_eq!(policy.checksum, None);
    assert!(policy.depth.contains("full level2"));
    assert!(policy.update_semantics.contains("quantity 0 removes"));
    assert!(policy.rest_snapshot_endpoint.contains("product_book"));
    assert!(policy.resync.contains("sequence_num gap"));
}

#[test]
fn coinbase_sequence_continuity_should_detect_gaps_and_regressions() {
    assert!(coinbase_sequence_is_contiguous(None, 42));
    assert!(coinbase_sequence_is_contiguous(Some(42), 43));
    assert!(!coinbase_sequence_is_contiguous(Some(42), 44));
    assert!(!coinbase_sequence_is_contiguous(Some(42), 42));
}

#[tokio::test]
async fn coinbase_adapter_should_ack_public_and_private_stream_specs() {
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        bearer_token: "jwt-token".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");
    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public subscription");
    assert!(public_id.contains("ticker"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Fills,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("user"));
}

#[test]
fn coinbase_heartbeat_subscription_should_include_optional_jwt() {
    let public = super::streams::coinbase_heartbeat_subscribe_payload(None);
    assert_eq!(public["channel"], "heartbeats");
    assert!(public.get("jwt").is_none());

    let private = super::streams::coinbase_heartbeat_subscribe_payload(Some("jwt-token"));
    assert_eq!(private["channel"], "heartbeats");
    assert_eq!(private["jwt"], "jwt-token");
}

#[test]
fn coinbase_public_ws_session_should_subscribe_heartbeats_and_parse_book_events() {
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        bearer_token: "jwt-token".to_string(),
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-session"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .expect("session");

    let initial = session.initial_requests();
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0]["channel"], "level2");
    assert_eq!(initial[1]["channel"], "heartbeats");
    assert_eq!(initial[0]["jwt"], "jwt-token");

    let heartbeat = session
        .handle_text_message(
            r#"{"channel":"heartbeats","timestamp":"2026-06-07T00:00:00Z","events":[{"current_time":"2026-06-07T00:00:00Z"}]}"#,
        )
        .expect("heartbeat");
    assert!(heartbeat.iter().any(|event| {
        matches!(
            event,
            super::streams::CoinbaseWsSessionEvent::Stream(events)
                if matches!(events.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));
    assert!(session.state().last_pong_at.is_some());

    let events = session
        .handle_text_message(
            r#"{
              "channel": "level2",
              "timestamp": "2026-06-07T00:00:01Z",
              "sequence_num": 43,
              "events": [{
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                  {"side": "bid", "price_level": "100", "new_quantity": "1"},
                  {"side": "ask", "price_level": "101", "new_quantity": "2"}
                ]
              }]
            }"#,
        )
        .expect("book");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            super::streams::CoinbaseWsSessionEvent::Stream(items)
                if matches!(items.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(_)))
        )
    }));
}

#[test]
fn coinbase_private_ws_session_should_subscribe_user_and_parse_order_events() {
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        bearer_token: "jwt-token".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");
    let mut session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-session"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .expect("session");
    let initial = session.initial_requests();
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0]["channel"], "user");
    assert_eq!(initial[1]["channel"], "heartbeats");

    let events = session
        .handle_text_message(
            r#"{
              "channel": "user",
              "timestamp": "2026-06-07T00:00:00Z",
              "events": [{
                "type": "snapshot",
                "orders": [{
                  "order_id": "order-1",
                  "client_order_id": "client-1",
                  "product_id": "BTC-USD",
                  "side": "BUY",
                  "status": "OPEN",
                  "order_configuration": {
                    "limit_limit_gtc": {
                      "base_size": "0.01",
                      "limit_price": "50000",
                      "post_only": false
                    }
                  },
                  "filled_size": "0"
                }]
              }]
            }"#,
        )
        .expect("private event");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            super::streams::CoinbaseWsSessionEvent::Stream(items)
                if matches!(
                    items.first(),
                    Some(ExchangeStreamEvent::OrderUpdate(order))
                        if order.exchange_order_id.as_deref() == Some("order-1")
                            && order.status == OrderStatus::Open
                )
        )
    }));
}
