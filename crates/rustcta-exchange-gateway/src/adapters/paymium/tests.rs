use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::Value;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::signing::{paymium_private_headers, paymium_rest_signature};
use super::streams::{paymium_public_socket_descriptor, paymium_user_socket_descriptor};
use super::{PaymiumGatewayAdapter, PaymiumGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn paymium_parser_should_map_btc_eur_ticker_and_order_book_fixtures() {
    let ticker: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/ticker_btc_eur.json"
    ))
    .expect("ticker fixture");
    let rules = parse_symbol_rules(&exchange_id(), &ticker).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "EUR");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00000001"));
    assert!(rules[0].supports_limit_orders);

    let orderbook: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/orderbook_btc_eur.json"
    ))
    .expect("orderbook fixture");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), Some(1), &orderbook)
        .expect("snapshot");
    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.asks.len(), 1);
    assert_eq!(snapshot.bids[0].price, 64100.0);
    assert_eq!(snapshot.asks[0].quantity, 0.27);
}

#[test]
fn paymium_parser_should_reject_missing_order_book_levels() {
    let missing: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/missing_required_fields.json"
    ))
    .expect("missing fixture");
    assert!(parse_orderbook_snapshot(&exchange_id(), symbol_scope(), None, &missing).is_err());
}

#[test]
fn paymium_signing_and_socket_descriptors_should_be_offline_verifiable() {
    let body =
        r#"{"type":"LimitOrder","currency":"EUR","direction":"buy","price":50000,"amount":0.01}"#;
    let signature = paymium_rest_signature(
        "1700000000000",
        "https://paymium.com/api/v1/user/orders",
        body,
        "fixture-secret",
    )
    .expect("signature");
    assert_eq!(
        signature,
        "ff5ed709bbc679919dc37bf549478110e737c0d9be643e7ad43283e057cbbdbe"
    );
    let headers = paymium_private_headers(
        "fixture-key",
        "fixture-secret",
        "1700000000000",
        "https://paymium.com/api/v1/user/orders",
        body,
    )
    .expect("headers");
    assert_eq!(headers[0].0, "Api-Key");
    assert_eq!(headers[1].0, "Api-Signature");
    assert_eq!(headers[2].0, "Api-Nonce");

    assert_eq!(
        paymium_public_socket_descriptor()["path"].as_str(),
        Some("/ws/socket.io")
    );
    assert_eq!(
        paymium_user_socket_descriptor("fixture-channel")["emit"][1].as_str(),
        Some("fixture-channel")
    );
}

#[test]
fn paymium_named_registration_should_keep_private_and_streams_disabled() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("paymium-test", ["paymium"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);

    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_place_order);
}

#[tokio::test]
async fn paymium_public_stream_subscribe_should_stay_runtime_unsupported_by_default() {
    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig::default()).expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            tenant_id: None,
            account_id: None,
            run_id: None,
            request_id: Some("paymium-public-stream".to_string()),
            requested_at: chrono::Utc::now(),
        },
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let error = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect_err("socket.io runtime should be disabled");
    assert!(format!("{error:?}").contains("paymium.public_socketio_runtime_unverified"));
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("paymium").expect("paymium exchange")
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "EUR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC/EUR")
            .expect("symbol"),
    }
}
