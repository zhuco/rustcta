use chrono::{Duration, TimeZone, Utc};
use rustcta_exchange_api::{
    AuthRenewalKind, ExchangeClient, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    kucoin_bullet_token_lease, kucoin_pong_response, kucoin_private_subscription_spec,
    kucoin_public_subscription_spec, parse_kucoin_obu_delta, parse_kucoin_obu_snapshot,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{KuCoinGatewayAdapter, KuCoinGatewayConfig};

#[test]
fn kucoin_public_stream_spec_should_normalize_symbol_and_ping_pong() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let spec = kucoin_public_subscription_spec(&subscription, "req-1", None).expect("spec");

    assert_eq!(spec.url, "wss://ws-api-spot.kucoin.com/endpoint");
    assert_eq!(spec.topic, "obu:BTC-USDT:increment");
    assert_eq!(spec.subscribe_payload["channel"], "obu");
    assert_eq!(spec.subscribe_payload["symbol"], "BTC-USDT");
    assert_eq!(spec.subscribe_payload["depth"], "increment");
    assert_eq!(spec.subscribe_payload["type"], "subscribe");
    assert_eq!(spec.subscribe_payload["privateChannel"], false);
    assert_eq!(
        kucoin_pong_response(&json!({"type": "ping", "id": "42"})).expect("pong")["id"],
        "42"
    );
}

#[test]
fn kucoin_public_stream_parser_should_read_obu_sequence_and_depth() {
    let delta_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/kucoin/ws/obu_increment.json"
    ))
    .expect("delta fixture");
    let delta =
        parse_kucoin_obu_delta(&exchange_id(), &symbol_scope(), &delta_fixture).expect("delta");
    assert_eq!(delta.first_sequence, Some(101));
    assert_eq!(delta.last_sequence, Some(102));
    assert_eq!(delta.bids[0].price, 65000.0);
    assert_eq!(delta.asks[0].quantity, 0.8);

    let snapshot_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/kucoin/ws/obu_depth50_snapshot.json"
    ))
    .expect("snapshot fixture");
    let snapshot = parse_kucoin_obu_snapshot(&exchange_id(), symbol_scope(), &snapshot_fixture)
        .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(102));
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.best_ask().unwrap().price, 65010.0);
}

#[test]
fn kucoin_private_stream_spec_should_use_bullet_token_lease_and_renewal() {
    let issued_at = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let lease = kucoin_bullet_token_lease(
        &exchange_id(),
        "connect-1",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{
                "endpoint": "wss://ws-api-spot.kucoin.com/endpoint",
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
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let spec = kucoin_private_subscription_spec(&subscription, "req-2", &lease).expect("spec");

    assert_eq!(lease.ping_interval_ms, 18_000);
    assert!(lease
        .websocket_url()
        .contains("token=bullet-token&connectId=connect-1"));
    assert!(!lease.should_renew(issued_at + Duration::hours(1)));
    assert!(lease.should_renew(issued_at + Duration::hours(23)));
    assert_eq!(spec.topic, "/spotMarket/tradeOrdersV2");
    assert_eq!(spec.subscribe_payload["privateChannel"], true);
}

#[tokio::test]
async fn kucoin_adapter_should_ack_ws_specs_and_expose_v2_policy() {
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinGatewayConfig::default()
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
    assert!(public_id.contains("wss://ws-api-spot.kucoin.com/endpoint:obu:BTC-USDT:1"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("kucoin-private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private stream id");
    assert_eq!(private_id, "kucoin:bullet-private:/account/balance:account");

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
    assert!(capabilities.order_book.supports_sequence);
    assert!(
        capabilities
            .private_stream_capabilities
            .as_ref()
            .is_some_and(
                |capabilities| capabilities.supports_orders && capabilities.supports_balances
            )
    );
    let lease = kucoin_bullet_token_lease(
        &exchange_id(),
        "connect-2",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{"endpoint": "wss://ws-api-spot.kucoin.com/endpoint"}]
        }),
        Utc::now(),
    )
    .expect("lease");
    assert_eq!(lease.renewal_policy.kind, AuthRenewalKind::TokenRefresh);
}
