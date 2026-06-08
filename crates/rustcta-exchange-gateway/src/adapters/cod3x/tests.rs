use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{parse_cod3x_audit_boundary, parse_cod3x_audit_subjects};
use super::signing::cod3x_routed_signature_boundary;
use super::streams::{
    cod3x_keepalive_payload, cod3x_private_stream_capabilities, cod3x_public_subscribe_payload,
    cod3x_stream_reconnect_policy_ms,
};
use super::test_support::{cod3x_context, cod3x_exchange_id, cod3x_symbol};
use super::{Cod3xGatewayAdapter, Cod3xGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn named_registration_should_accept_cod3x() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["cod3x"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn capabilities_should_keep_cod3x_as_perp_terminal_audit_only() {
    let adapter = Cod3xGatewayAdapter::new(Cod3xGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities.capabilities_v2.credential_scopes.is_empty());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_positions"));

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    let boundary = parse_cod3x_audit_boundary(&boundary).expect("boundary");
    assert_eq!(
        boundary.venue_profiles,
        vec![
            "hyperliquid".to_string(),
            "gmx_v2".to_string(),
            "lighter".to_string()
        ]
    );
    assert!(!boundary.trade_enabled);
    assert!(boundary.scan_only);
    assert!(!boundary.stable_api_verified);
}

#[tokio::test]
async fn cod3x_bulk_orders_should_stay_routing_unverified() {
    let adapter = Cod3xGatewayAdapter::new(Cod3xGatewayConfig::default()).expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: cod3x_context("batch"),
        exchange: cod3x_exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: cod3x_context("order"),
            symbol: cod3x_symbol(MarketType::Perpetual),
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "1".to_string(),
            price: Some("3200".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        }],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "cod3x.batch_place_order_routing_unverified"
        }
    ));
}

#[test]
fn cod3x_audit_fixtures_should_cover_markets_risk_positions_first() {
    let market_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/markets_audit.json"
    ))
    .expect("market audit fixture");
    let risk_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/risk_audit.json"
    ))
    .expect("risk audit fixture");
    let positions_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/positions_audit.json"
    ))
    .expect("positions audit fixture");

    assert_eq!(
        parse_cod3x_audit_subjects(&market_audit).expect("market subjects"),
        vec!["terminal_routes", "venue_profiles", "market_discovery"]
    );
    assert_eq!(
        parse_cod3x_audit_subjects(&risk_audit).expect("risk subjects"),
        vec!["downstream_funding", "venue_risk", "routing_safety"]
    );
    assert_eq!(
        parse_cod3x_audit_subjects(&positions_audit).expect("positions subjects"),
        vec![
            "downstream_positions",
            "wallet_connections",
            "account_permissions"
        ]
    );
}

#[test]
fn cod3x_stream_and_signing_helpers_should_make_unverified_boundary_explicit() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: cod3x_context("public-ws"),
        symbol: cod3x_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = cod3x_public_subscribe_payload(&subscription);
    assert_eq!(payload["support"], "unsupported_unverified");
    assert_eq!(cod3x_keepalive_payload()["type"], "ping");
    assert_eq!(cod3x_stream_reconnect_policy_ms(), (30_000, 45_000, 60_000));
    assert_eq!(
        cod3x_private_stream_capabilities().schema_version,
        EXCHANGE_API_SCHEMA_VERSION
    );
    assert!(cod3x_routed_signature_boundary(None).is_err());
    assert!(cod3x_routed_signature_boundary(Some("hyperliquid")).is_ok());
}
