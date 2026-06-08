use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    coinone_pong_response, coinone_private_subscription_spec, coinone_public_subscription_spec,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CoinoneGatewayAdapter, CoinoneGatewayConfig};

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
