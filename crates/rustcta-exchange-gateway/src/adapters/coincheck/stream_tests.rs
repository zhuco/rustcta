use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeClient, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::streams::coincheck_public_subscription_spec;
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
