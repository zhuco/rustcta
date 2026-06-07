use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};

use super::streams::{
    coinex_ping_payload, coinex_private_login_payload, coinex_private_subscribe_payload,
    coinex_public_subscribe_payload, coinex_reconnect_policy_ms, parse_coinex_stream_control,
    CoinExStreamControlMessage, COINEX_PRIVATE_WS_URL, COINEX_PUBLIC_WS_URL,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CoinExGatewayAdapter, CoinExGatewayConfig};

#[test]
fn coinex_public_stream_spec_should_normalize_symbol_and_heartbeat() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("coinex-public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };

    let payload = coinex_public_subscribe_payload(&subscription, 1).expect("payload");

    assert_eq!(payload["method"], "depth.subscribe");
    assert_eq!(payload["params"][0], "BTCUSDT");
    assert_eq!(coinex_ping_payload(2)["method"], "server.ping");
    assert_eq!(coinex_reconnect_policy_ms(), (30_000, 10_000, 45_000));
}

#[test]
fn coinex_private_stream_spec_should_sign_login_and_subscribe() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("coinex-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let login =
        coinex_private_login_payload("key", "secret", 1_700_000_000_000, 1).expect("login payload");
    let subscribe = coinex_private_subscribe_payload(&subscription, 2).expect("subscribe payload");

    assert_eq!(login["method"], "server.sign");
    assert_eq!(login["params"]["access_id"], "key");
    assert!(login["params"]["signed_str"]
        .as_str()
        .is_some_and(|signature| !signature.is_empty()));
    assert_eq!(subscribe["method"], "order.subscribe");
    assert_eq!(subscribe["params"][0], "SPOT");
    assert_eq!(
        parse_coinex_stream_control(&serde_json::json!({"id": 1, "method": "server.pong"})),
        CoinExStreamControlMessage::Pong
    );
}

#[tokio::test]
async fn coinex_adapter_should_ack_ws_specs_and_expose_v2_policy() {
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CoinExGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("coinex-public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public stream id");
    assert_eq!(
        public_id,
        format!("coinex:{COINEX_PUBLIC_WS_URL}:deals.subscribe")
    );

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("coinex-private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private stream id");
    assert_eq!(
        private_id,
        format!("coinex:{COINEX_PRIVATE_WS_URL}:balance.subscribe:account")
    );

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(super::capabilities::COINEX_SPOT_MAX_PAGE_LIMIT)
    );
    assert!(
        capabilities
            .private_stream_capabilities
            .as_ref()
            .is_some_and(
                |capabilities| capabilities.supports_orders && capabilities.supports_balances
            )
    );
}
