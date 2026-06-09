use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::streams::{
    gateio_public_subscribe_payload, gateio_public_ws_spec, parse_gateio_control_message,
    parse_gateio_public_order_book_event,
};
use super::test_support::{context, exchange_id, perpetual_symbol_scope, symbol_scope};
use super::{GateIoGatewayAdapter, GateIoGatewayConfig};

#[test]
fn gateio_public_stream_payload_should_cover_spot_and_perpetual_order_book_update() {
    let spot = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("gateio-spot-ws"),
        symbol: symbol_scope("BTC_USDT"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = gateio_public_subscribe_payload(&spot, "100ms").expect("spot payload");
    assert_eq!(payload["channel"], "spot.order_book_update");
    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["payload"], json!(["BTC_USDT", "100ms"]));

    let perpetual = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("gateio-perp-ws"),
        symbol: perpetual_symbol_scope("BTC_USDT"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = gateio_public_subscribe_payload(&perpetual, "20ms").expect("perp payload");
    assert_eq!(payload["channel"], "futures.order_book_update");
    assert_eq!(payload["payload"], json!(["BTC_USDT", "20ms", "20"]));
}

#[test]
fn gateio_public_stream_parser_should_parse_depth_and_control_messages() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/gateio/ws/public_spot_order_book_update.json"
    ))
    .expect("fixture");
    let snapshot =
        parse_gateio_public_order_book_event(&exchange_id(), symbol_scope("BTC_USDT"), &fixture)
            .expect("parse")
            .expect("snapshot");
    assert_eq!(snapshot.market_type, MarketType::Spot);
    assert_eq!(snapshot.sequence, Some(101));
    assert_eq!(snapshot.best_bid().expect("bid").price, 26274.8);
    assert_eq!(snapshot.best_ask().expect("ask").quantity, 0.05);

    assert_eq!(
        parse_gateio_control_message(&json!({"event": "subscribe"})),
        Some("subscribed")
    );
}

#[tokio::test]
async fn gateio_adapter_should_ack_public_ws_specs() {
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        enabled_public_streams: true,
        public_order_book_interval: "20ms".to_string(),
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("gateio-ws-id"),
        symbol: symbol_scope("BTC_USDT"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let spec = gateio_public_ws_spec(&adapter.config, &subscription).expect("spec");
    assert_eq!(spec.channel, "spot.order_book_update");
    assert_eq!(spec.interval, "20ms");
    assert_eq!(spec.resync_endpoint, "/spot/order_book");

    let stream_id = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect("stream id");
    assert_eq!(
        stream_id,
        "gateio:wss://api.gateio.ws/ws/v4/:spot.order_book_update"
    );
}
