use rustcta_exchange_api::{
    CapabilitySupport, ExchangeClient, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::{json, Value};

use super::streams::{
    is_weex_heartbeat, parse_weex_private_stream_message, parse_weex_public_stream_message,
    weex_pong_payload, weex_private_auth_headers, weex_private_subscribe_payload,
    weex_public_subscribe_payload, WeexPrivateStreamMessage, WeexPublicStreamMessage,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{WeexGatewayAdapter, WeexGatewayConfig};

#[test]
fn weex_public_stream_payload_should_map_market_channels() {
    let book = weex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("book-stream"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("book payload");
    assert_eq!(book["method"], "SUBSCRIBE");
    assert_eq!(book["params"][0], "BTCUSDT@depth15");

    let candle = weex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("candle-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
    })
    .expect("candle payload");
    assert_eq!(candle["params"][0], "BTCUSDT@kline_1m");
}

#[test]
fn weex_private_stream_payload_should_map_auth_and_channels() {
    let headers = weex_private_auth_headers("key", "secret", "passphrase", 1_591_089_508_404)
        .expect("auth headers");
    assert_eq!(headers.get("ACCESS-KEY").map(String::as_str), Some("key"));
    assert_eq!(
        headers.get("ACCESS-PASSPHRASE").map(String::as_str),
        Some("passphrase")
    );
    assert!(headers
        .get("ACCESS-SIGN")
        .is_some_and(|value| !value.is_empty()));

    let payload = weex_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-stream"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    })
    .expect("private payload");
    assert_eq!(payload["params"][0], "orders");
}

#[test]
fn weex_heartbeat_should_build_ping_pong_and_detect_server_ping() {
    assert_eq!(weex_pong_payload(Some(9))["id"], 9);
    assert!(is_weex_heartbeat(&json!({"event": "ping", "time": "1"})));
    assert!(is_weex_heartbeat(&json!({"type": "ping", "time": "1"})));
    assert!(is_weex_heartbeat(&json!({"method": "PONG", "id": 1})));
    assert!(is_weex_heartbeat(&json!({"ping": 1700000000000i64})));
}

#[test]
fn weex_stream_parser_should_parse_heartbeat_public_book_and_private_order() {
    let heartbeat =
        parse_weex_public_stream_message(&exchange_id(), spot_symbol_scope(), &json!({"pong": 1}))
            .expect("heartbeat");
    assert!(matches!(heartbeat, WeexPublicStreamMessage::Pong));

    let book = parse_weex_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "e": "depth",
            "data": {
                "b": [["65000", "0.1"]],
                "a": [["65001", "0.2"]],
                "ts": 1700000000000i64
            }
        }),
    )
    .expect("book");
    match book {
        WeexPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.bids[0].price, 65000.0);
            assert_eq!(book.asks[0].quantity, 0.2);
        }
        other => panic!("unexpected public message {other:?}"),
    }

    let private = parse_weex_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(perp_symbol_scope()),
        MarketType::Perpetual,
        &fixture("ws_order"),
    )
    .expect("private");
    match private {
        WeexPrivateStreamMessage::Order(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));
            assert_eq!(order.status, rustcta_types::OrderStatus::Filled);
        }
        other => panic!("unexpected private message {other:?}"),
    }
}

#[tokio::test]
async fn weex_adapter_should_return_stream_subscription_ids() {
    let adapter = WeexGatewayAdapter::new(WeexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..WeexGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_public_streams);
    assert!(adapter.capabilities().supports_private_streams);

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public subscription");
    assert!(public_id.contains("BTCUSDT@trade"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("orders"));
}

#[test]
fn weex_capabilities_v2_and_yaml_artifacts_should_declare_runtime_plans() {
    let adapter = WeexGatewayAdapter::new(WeexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..WeexGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities().capabilities_v2;

    assert!(matches!(
        capabilities.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.private_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.batch_place_orders.mode,
        rustcta_exchange_api::BatchExecutionMode::Native
    ));
    assert_eq!(capabilities.batch_place_orders.max_items, Some(20));
    assert!(capabilities.stream_runtime.resync.order_book);
    assert!(
        capabilities
            .stream_runtime
            .auth
            .requires_relogin_on_reconnect
    );
    assert!(capabilities.fills_history.supports_since);
    assert!(capabilities.fills_history.supports_until);

    let endpoint_mapping = include_str!("endpoint_mapping.yaml");
    assert!(endpoint_mapping.contains("operation: weex_place_order_perpetual"));
    assert!(
        endpoint_mapping.contains("rate_limit_plan") || endpoint_mapping.contains("rate_limits:")
    );
    assert!(endpoint_mapping.contains("reconciliation:"));

    let capability_yaml = include_str!("capabilities_v2.yaml");
    assert!(capability_yaml.contains("stream_runtime:"));
    assert!(capability_yaml.contains("reconciliation_plan:"));
    assert!(capability_yaml.contains("unsupported_gaps:"));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "ws_order" => include_str!("../../../../../tests/fixtures/exchanges/weex/ws_order.json"),
        other => panic!("unknown WEEX fixture {other}"),
    };
    serde_json::from_str(text).expect("WEEX fixture JSON")
}
