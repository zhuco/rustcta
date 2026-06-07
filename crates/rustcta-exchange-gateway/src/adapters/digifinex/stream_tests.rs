use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::streams::{
    digifinex_private_subscribe_payload, digifinex_public_subscribe_payload,
    digifinex_stream_reconnect_policy, digifinex_ws_ping_payload,
    parse_digifinex_public_stream_message, DigiFinexPublicStreamMessage,
};
use super::test_support::{context, exchange_id, spot_symbol_scope, swap_symbol_scope};
use super::{DigiFinexGatewayAdapter, DigiFinexGatewayConfig};

#[test]
fn digifinex_stream_payloads_should_cover_public_private_and_heartbeat() {
    let public = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = digifinex_public_subscribe_payload(&public, "1").expect("payload");
    assert_eq!(payload["method"], "depth.subscribe");
    assert_eq!(payload["params"][0], "btc_usdt");

    let private = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private"),
        exchange: exchange_id(),
        account_id: rustcta_types::AccountId::new("account").unwrap(),
        market_type: Some(MarketType::Perpetual),
        kind: PrivateStreamKind::Positions,
    };
    let private_payload =
        digifinex_private_subscribe_payload(&private, MarketType::Perpetual, "2").unwrap();
    assert_eq!(private_payload["method"], "position.subscribe");
    assert_eq!(
        digifinex_ws_ping_payload(MarketType::Spot)["method"],
        "server.ping"
    );
    assert!(digifinex_stream_reconnect_policy(MarketType::Perpetual).ping_interval_ms < 30_000);
}

#[test]
fn digifinex_public_stream_parser_should_parse_pong_and_book() {
    let pong = parse_digifinex_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({"method": "server.pong"}),
    )
    .expect("pong");
    assert_eq!(pong, DigiFinexPublicStreamMessage::Pong);

    let book = parse_digifinex_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "method": "depth.update",
            "params": [{
                "bids": [["65000", "1"]],
                "asks": [["65001", "2"]]
            }]
        }),
    )
    .expect("book");
    match book {
        DigiFinexPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.bids[0].price, 65000.0);
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[tokio::test]
async fn digifinex_adapter_should_ack_stream_subscription_specs() {
    let adapter = DigiFinexGatewayAdapter::new(DigiFinexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..DigiFinexGatewayConfig::default()
    })
    .expect("adapter");
    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: swap_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public");
    assert!(public_id.contains("digifinex:"));
    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            account_id: rustcta_types::AccountId::new("account").unwrap(),
            market_type: Some(MarketType::Spot),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private");
    assert!(private_id.contains("order"));
}
