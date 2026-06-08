use chrono::{Duration, TimeZone, Utc};
use rustcta_exchange_api::{
    AuthRenewalKind, ExchangeClient, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    kucoinfutures_bullet_token_lease, kucoinfutures_pong_response,
    kucoinfutures_private_subscription_spec, kucoinfutures_public_subscription_spec,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{KuCoinFuturesGatewayAdapter, KuCoinFuturesGatewayConfig};

#[test]
fn kucoinfutures_public_stream_spec_should_normalize_symbol_and_ping_pong() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let spec = kucoinfutures_public_subscription_spec(&subscription, "req-1", None).expect("spec");

    assert_eq!(spec.url, "wss://ws-api-futures.kucoin.com/endpoint");
    assert_eq!(spec.topic, "/contractMarket/level2:XBTUSDTM");
    assert_eq!(spec.subscribe_payload["type"], "subscribe");
    assert_eq!(spec.subscribe_payload["privateChannel"], false);
    assert_eq!(
        kucoinfutures_pong_response(&json!({"type": "ping", "id": "42"})).expect("pong")["id"],
        "42"
    );
}

#[test]
fn kucoinfutures_private_stream_spec_should_use_bullet_token_lease_and_renewal() {
    let issued_at = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let lease = kucoinfutures_bullet_token_lease(
        &exchange_id(),
        "connect-1",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{
                "endpoint": "wss://ws-api-futures.kucoin.com/endpoint",
                "pingInterval": 18000,
                "pingTimeout": 10000
            }]
        }),
        issued_at,
    )
    .expect("lease");
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let spec =
        kucoinfutures_private_subscription_spec(&subscription, "req-2", &lease).expect("spec");

    assert_eq!(lease.ping_interval_ms, 18_000);
    assert!(lease
        .websocket_url()
        .contains("token=bullet-token&connectId=connect-1"));
    assert!(!lease.should_renew(issued_at + Duration::hours(1)));
    assert!(lease.should_renew(issued_at + Duration::hours(23)));
    assert_eq!(spec.topic, "/contractMarket/tradeOrders");
    assert_eq!(spec.subscribe_payload["privateChannel"], true);
}

#[tokio::test]
async fn kucoinfutures_adapter_should_ack_ws_specs_and_expose_v2_policy() {
    let adapter = KuCoinFuturesGatewayAdapter::new(KuCoinFuturesGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("kucoin-public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public stream id");
    assert!(public_id
        .contains("wss://ws-api-futures.kucoin.com/endpoint:/contractMarket/ticker:XBTUSDTM"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("kucoin-private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private stream id");
    assert_eq!(
        private_id,
        "kucoinfutures:bullet-private:/contractAccount/wallet:account"
    );

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert_eq!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .interval_ms,
        Some(20_000)
    );
    assert!(capabilities.capabilities_v2.fills_history.supports_cursor);
    assert!(capabilities
        .private_stream_capabilities
        .as_ref()
        .is_some_and(|capabilities| {
            capabilities.supports_orders
                && capabilities.supports_balances
                && capabilities.supports_positions
        }));
    let lease = kucoinfutures_bullet_token_lease(
        &exchange_id(),
        "connect-2",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{"endpoint": "wss://ws-api-futures.kucoin.com/endpoint"}]
        }),
        Utc::now(),
    )
    .expect("lease");
    assert_eq!(lease.renewal_policy.kind, AuthRenewalKind::TokenRefresh);
}
