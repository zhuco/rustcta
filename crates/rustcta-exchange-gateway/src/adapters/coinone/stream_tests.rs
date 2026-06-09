use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::parser::parse_orderbook_snapshot;
use super::streams::{
    coinone_orderbook_id, coinone_orderbook_id_is_newer, coinone_pong_response,
    coinone_private_subscription_spec, coinone_public_order_book_ws_policy,
    coinone_public_subscription_spec,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CoinoneGatewayAdapter, CoinoneGatewayConfig};

fn coinone_ws_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "ws_public_orderbook_snapshot.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinone/ws_public_orderbook_snapshot.json"
        ),
        "ws_public_orderbook_update.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinone/ws_public_orderbook_update.json"
        ),
        _ => panic!("unknown coinone websocket fixture {name}"),
    };
    serde_json::from_str(text).expect("coinone websocket fixture")
}

#[test]
fn coinone_public_stream_spec_should_build_krw_subscription_and_ping_pong() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let spec = coinone_public_subscription_spec("wss://stream.coinone.co.kr", &subscription)
        .expect("spec");

    assert_eq!(spec.channel, "ORDERBOOK");
    assert_eq!(spec.subscribe_payload["request_type"], "SUBSCRIBE");
    assert_eq!(spec.subscribe_payload["topic"]["quote_currency"], "KRW");
    assert_eq!(spec.subscribe_payload["topic"]["target_currency"], "BTC");
    assert_eq!(
        coinone_pong_response(&json!({"request_type": "PING"})).expect("pong"),
        json!({"request_type": "PONG"})
    );
}

#[test]
fn coinone_public_orderbook_policy_should_document_snapshot_delta_boundary() {
    let policy = coinone_public_order_book_ws_policy();

    assert_eq!(policy.url, "wss://stream.coinone.co.kr");
    assert_eq!(policy.channel, "ORDERBOOK");
    assert_eq!(policy.topic_fields, &["quote_currency", "target_currency"]);
    assert!(policy.first_message_semantics.contains("last order book"));
    assert!(policy
        .update_semantics
        .contains("only when the order book changes"));
    assert_eq!(policy.depth, None);
    assert_eq!(policy.interval_ms, None);
    assert_eq!(policy.order_book_id_field, "data.id");
    assert!(policy
        .order_book_id_semantics
        .contains("not a documented contiguous sequence"));
    assert_eq!(policy.sequence_field, None);
    assert_eq!(policy.checksum, None);
    assert!(policy.reconnect_resync.contains("/public/v2/orderbook/KRW"));
}

#[test]
fn coinone_orderbook_ws_fixtures_should_parse_id_and_book_levels() {
    let snapshot = coinone_ws_fixture("ws_public_orderbook_snapshot.json");
    let update = coinone_ws_fixture("ws_public_orderbook_update.json");

    assert_eq!(snapshot["channel"], "ORDERBOOK");
    assert_eq!(update["channel"], "ORDERBOOK");

    let snapshot_id = coinone_orderbook_id(&snapshot).expect("snapshot order book id");
    let update_id = coinone_orderbook_id(&update).expect("update order book id");
    assert!(coinone_orderbook_id_is_newer(snapshot_id, update_id));

    let book = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &snapshot)
        .expect("parse coinone ws orderbook");
    assert_eq!(book.sequence, Some(1_693_560_155_038_001));
    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 2);
}

#[test]
fn coinone_private_stream_spec_should_sign_auth_headers() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let spec = coinone_private_subscription_spec(
        "wss://stream.coinone.co.kr/v1/private",
        &subscription,
        "token",
        "secret",
        "nonce-1",
    )
    .expect("spec");

    assert_eq!(spec.channel, "ORDER");
    assert!(spec.payload_header.is_some());
    assert!(spec
        .signature_header
        .as_ref()
        .is_some_and(|signature| signature.len() == 128));
}

#[tokio::test]
async fn coinone_adapter_should_ack_ws_specs_and_expose_private_policy() {
    let adapter = CoinoneGatewayAdapter::new(CoinoneGatewayConfig {
        access_token: Some("token".to_string()),
        secret_key: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinoneGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public stream id");
    assert_eq!(public_id, "coinone:wss://stream.coinone.co.kr:TICKER");

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private stream id");
    assert_eq!(private_id, "coinone:private:BALANCE:account");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(capabilities
        .private_stream_capabilities
        .as_ref()
        .is_some_and(
            |capabilities| capabilities.supports_orders && !capabilities.supports_positions
        ));
}
