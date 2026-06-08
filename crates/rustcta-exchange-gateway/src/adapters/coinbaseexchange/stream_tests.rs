use rustcta_exchange_api::{
    AccountId, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::streams::{
    coinbaseexchange_private_subscription_spec, coinbaseexchange_public_subscription_spec,
};
use super::test_support::{context, exchange_id, symbol_scope};

#[test]
fn coinbaseexchange_public_stream_spec_should_use_exchange_channels() {
    let spec = coinbaseexchange_public_subscription_spec(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        },
        "wss://ws-feed.exchange.coinbase.com",
    )
    .expect("spec");
    assert_eq!(spec.url, "wss://ws-feed.exchange.coinbase.com");
    assert_eq!(spec.channel, "level2");
    assert_eq!(spec.subscribe_payload["type"], "subscribe");
    assert_eq!(spec.subscribe_payload["channels"][0], "level2");
    assert_eq!(spec.subscribe_payload["product_ids"][0], "BTC-USD");
}

#[test]
fn coinbaseexchange_private_stream_spec_should_sign_user_channel() {
    let spec = coinbaseexchange_private_subscription_spec(
        &PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").unwrap(),
            kind: PrivateStreamKind::Orders,
        },
        "wss://ws-feed.exchange.coinbase.com",
        "key",
        "secret",
        "passphrase",
        "1700000000",
    )
    .expect("spec");
    assert_eq!(spec.channel, "user");
    assert_eq!(spec.subscribe_payload["key"], "key");
    assert_eq!(spec.subscribe_payload["passphrase"], "passphrase");
    assert_eq!(spec.subscribe_payload["timestamp"], "1700000000");
    assert!(spec.subscribe_payload["signature"].as_str().unwrap().len() > 20);
}
