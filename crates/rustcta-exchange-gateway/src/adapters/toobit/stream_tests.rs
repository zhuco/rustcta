use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderStatus};
use serde_json::json;

use super::streams::{
    parse_toobit_private_stream_messages, parse_toobit_public_stream_message,
    toobit_public_subscribe_payload, ToobitPrivateStreamMessage, ToobitPublicStreamMessage,
};
use super::test_support::{
    assert_signed_toobit_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    spot_symbol_scope,
};
use super::{ToobitGatewayAdapter, ToobitGatewayConfig};
use crate::request_spec::RequestSpec;

#[test]
fn toobit_public_stream_payload_should_map_channels() {
    let book = toobit_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("book-stream"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("book payload");
    assert_eq!(book["topic"], "depth");
    assert_eq!(book["symbol"], "BTCUSDT");

    let mark = toobit_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mark-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::Ticker,
    })
    .expect("mark payload");
    assert_eq!(mark["topic"], "markPrice");
    assert_eq!(mark["symbol"], "BTC-SWAP-USDT");
}

#[test]
fn toobit_stream_parser_should_parse_heartbeat_public_book_and_private_order() {
    let heartbeat = parse_toobit_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        10_000,
        &json!({"pong": 1}),
    )
    .expect("heartbeat");
    assert!(matches!(heartbeat, ToobitPublicStreamMessage::Pong));

    let book = parse_toobit_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        10_000,
        &json!({
            "topic": "depth",
            "data": [{
                "s": "BTCUSDT",
                "t": 1700000000000i64,
                "b": [["65000", "0.1"]],
                "a": [["65001", "0.2"]]
            }]
        }),
    )
    .expect("book");
    match book {
        ToobitPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.bids[0].price, 65000.0);
            assert_eq!(book.asks[0].quantity, 0.2);
        }
        other => panic!("unexpected public message {other:?}"),
    }

    let private = parse_toobit_private_stream_messages(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        MarketType::Spot,
        Some(&spot_symbol_scope()),
        &json!({
            "e": "executionReport",
            "s": "BTCUSDT",
            "i": "1001",
            "c": "client-1",
            "S": "BUY",
            "o": "LIMIT",
            "X": "FILLED",
            "q": "0.01",
            "z": "0.01",
            "p": "65000",
            "O": 1700000000000i64,
            "E": 1700000001000i64
        }),
    )
    .expect("private");
    match &private[0] {
        ToobitPrivateStreamMessage::Order(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));
            assert_eq!(order.status, OrderStatus::Filled);
        }
        other => panic!("unexpected private message {other:?}"),
    }
}

#[tokio::test]
async fn toobit_adapter_should_return_public_and_private_stream_subscription_ids() {
    let (base_url, seen) =
        spawn_rest_server(vec![json!({"code": 0, "listenKey": "listen-1"})]).await;
    let adapter = ToobitGatewayAdapter::new(ToobitGatewayConfig {
        rest_base_url: base_url,
        public_ws_url: "wss://stream.toobit.com/quote/ws/v1".to_string(),
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..ToobitGatewayConfig::default()
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
    assert!(public_id.contains("toobit:"));
    assert!(public_id.contains("trade"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("/api/v1/ws/listen-1"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_toobit_request(&requests[0], "POST", "/api/v1/userDataStream");
    load_request_spec("create_listen_key.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn toobit_listen_key_keepalive_should_match_request_spec() {
    let (base_url, seen) = spawn_rest_server(vec![json!({"code": 0})]).await;
    let adapter = ToobitGatewayAdapter::new(ToobitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..ToobitGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .keepalive_listen_key("listen-key")
        .await
        .expect("keepalive");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_toobit_request(&requests[0], "PUT", "/api/v1/userDataStream");
    load_request_spec("keepalive_listen_key.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/toobit/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}
