use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::streams::{
    bitget_pong_payload, bitget_public_subscribe_payload, bitget_public_ws_spec,
    parse_bitget_control_message, parse_bitget_public_order_book_event,
};
use super::test_support::{context, exchange_id, perpetual_symbol_scope, symbol_scope};
use super::{BitgetGatewayAdapter, BitgetGatewayConfig};

#[test]
fn bitget_public_stream_payload_should_cover_spot_and_perpetual_books_channels() {
    let spot = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("bitget-spot-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = bitget_public_subscribe_payload(&spot, "books5").expect("spot payload");
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["args"][0]["instType"], "SPOT");
    assert_eq!(payload["args"][0]["channel"], "books5");
    assert_eq!(payload["args"][0]["instId"], "BTCUSDT");

    let perpetual = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("bitget-perp-ws"),
        symbol: perpetual_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = bitget_public_subscribe_payload(&perpetual, "books1").expect("perp payload");
    assert_eq!(payload["args"][0]["instType"], "USDT-FUTURES");
    assert_eq!(payload["args"][0]["channel"], "books1");
}

#[test]
fn bitget_public_stream_parser_should_parse_depth_and_control_messages() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitget/ws/public_depth_books5.json"
    ))
    .expect("fixture");
    let snapshot = parse_bitget_public_order_book_event(&exchange_id(), symbol_scope(), &fixture)
        .expect("parse")
        .expect("snapshot");
    assert_eq!(snapshot.market_type, MarketType::Spot);
    assert_eq!(snapshot.sequence, Some(123));
    assert_eq!(snapshot.best_bid().expect("bid").price, 26274.8);
    assert_eq!(snapshot.best_ask().expect("ask").quantity, 0.05);

    assert_eq!(
        parse_bitget_control_message(&json!({"event": "subscribe"})),
        Some("subscribed")
    );
    assert_eq!(parse_bitget_control_message(&json!("ping")), Some("ping"));
    assert_eq!(bitget_pong_payload(), json!("pong"));
}

#[tokio::test]
async fn bitget_adapter_should_ack_public_ws_specs() {
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        enabled_public_streams: true,
        public_order_book_channel: "books1".to_string(),
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("bitget-ws-id"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let spec = bitget_public_ws_spec(&adapter.config, &subscription).expect("spec");
    assert_eq!(spec.channel, "books1");
    assert_eq!(spec.resync_endpoint, "/api/v2/spot/market/orderbook");
    assert_eq!(spec.unsubscribe["op"], "unsubscribe");

    let stream_id = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect("stream id");
    assert_eq!(stream_id, "bitget:wss://ws.bitget.com/v2/ws/public:books1");
}
