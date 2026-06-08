use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};

use super::streams::{heartbeat_policy_ms, private_stream_spec, public_stream_spec};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{BitoproGatewayAdapter, BitoproGatewayConfig};

#[test]
fn bitopro_public_stream_spec_should_build_url_and_heartbeat_policy() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let spec =
        public_stream_spec(&subscription, "wss://stream.bitopro.com:443/ws").expect("public ws");
    assert_eq!(
        spec.url,
        "wss://stream.bitopro.com:443/ws/v1/pub/order-books/BTC_TWD:5"
    );
    assert_eq!(heartbeat_policy_ms(), (20_000, 5_000));

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitopro/ws/public_orderbook.json"
    ))
    .expect("ws fixture");
    let book = super::parser::parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &fixture)
        .expect("book");
    assert_eq!(book.best_bid().unwrap().price, 3_000_000.0);
}

#[test]
fn bitopro_private_stream_spec_should_sign_handshake_headers() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let spec = private_stream_spec(
        &subscription,
        "wss://stream.bitopro.com:443/ws",
        "bitopro-key",
        "bitopro",
        "support@example.test",
        1554380909131,
    )
    .expect("private ws");

    assert_eq!(
        spec.url,
        "wss://stream.bitopro.com:443/ws/v1/pub/auth/orders"
    );
    assert_eq!(spec.headers.api_key, "bitopro-key");
    assert!(!spec.headers.payload_base64.is_empty());
    assert!(!spec.headers.signature.is_empty());
}

#[tokio::test]
async fn bitopro_adapter_should_ack_public_ws_specs_and_gate_private_ws() {
    let adapter = BitoproGatewayAdapter::new(BitoproGatewayConfig::default()).expect("adapter");
    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public stream id");
    assert_eq!(
        public_id,
        "bitopro:wss://stream.bitopro.com:443/ws/v1/pub/trades/BTC_TWD"
    );

    let error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect_err("private stream disabled");
    assert!(format!("{error:?}").contains("rest_reconciliation_fallback"));
}
