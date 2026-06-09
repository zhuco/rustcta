use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeClient, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::Value;

use super::streams::{coincheck_public_order_book_ws_policy, coincheck_public_subscription_spec};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CoincheckGatewayAdapter, CoincheckGatewayConfig};

#[test]
fn coincheck_public_stream_spec_should_use_coincheck_channels() {
    let spec = coincheck_public_subscription_spec(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        },
        "wss://ws-api.coincheck.com/",
    )
    .expect("spec");
    assert_eq!(spec.channel, "btc_jpy-orderbook");
    assert_eq!(spec.subscribe_payload["type"], "subscribe");
    assert_eq!(spec.subscribe_payload["channel"], "btc_jpy-orderbook");
    assert_eq!(spec.unsubscribe_payload["type"], "unsubscribe");
    assert_eq!(spec.unsubscribe_payload["channel"], "btc_jpy-orderbook");
}

#[test]
fn coincheck_public_order_book_policy_should_document_unsequenced_diff_stream() {
    let policy = coincheck_public_order_book_ws_policy();
    assert_eq!(policy.url, "wss://ws-api.coincheck.com/");
    assert_eq!(policy.channel_template, "{pair}-orderbook");
    assert_eq!(policy.subscribe_type, "subscribe");
    assert_eq!(policy.interval_ms, Some(100));
    assert_eq!(policy.depth, None);
    assert_eq!(policy.sequence, None);
    assert_eq!(policy.checksum, None);
    assert!(policy.update_semantics.contains("differences"));
    assert!(policy.resync.contains("/api/order_books"));
}

#[test]
fn coincheck_public_order_book_fixture_should_match_subscription_policy() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coincheck/ws/public_orderbook_diff.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["type"].as_str(), Some("message"));
    assert_eq!(fixture["channel"].as_str(), Some("btc_jpy-orderbook"));
    assert_eq!(fixture["bids"][0][0].as_str(), Some("10000000"));
    assert_eq!(fixture["asks"][1][1].as_str(), Some("0"));
}

#[tokio::test]
async fn coincheck_private_stream_should_be_unsupported() {
    let adapter = CoincheckGatewayAdapter::new(CoincheckGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoincheckGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").unwrap(),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}
