use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{
    parse_equation_audit_boundary, parse_equation_audit_subjects, EQUATION_ARBITRUM_CHAIN_ID,
};
use super::signing::equation_wallet_signature_boundary;
use super::streams::{
    equation_keepalive_payload, equation_private_stream_capabilities,
    equation_public_subscribe_payload, equation_stream_reconnect_policy_ms,
};
use super::test_support::{equation_context, equation_exchange_id, equation_symbol};
use super::{EquationGatewayAdapter, EquationGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn named_registration_should_accept_equation() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["equation"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn capabilities_should_keep_equation_as_arbitrum_perp_audit_only() {
    let adapter = EquationGatewayAdapter::new(EquationGatewayConfig::default()).expect("adapter");
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
        "../../../../../tests/fixtures/exchanges/equation/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    let boundary = parse_equation_audit_boundary(&boundary).expect("boundary");
    assert_eq!(boundary.chain_id, EQUATION_ARBITRUM_CHAIN_ID);
    assert!(!boundary.trade_enabled);
    assert!(boundary.scan_only);
    assert!(!boundary.stable_api_verified);
}

#[tokio::test]
async fn equation_bulk_orders_should_stay_signing_unverified() {
    let adapter = EquationGatewayAdapter::new(EquationGatewayConfig::default()).expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: equation_context("batch"),
        exchange: equation_exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: equation_context("order"),
            symbol: equation_symbol(MarketType::Perpetual),
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
            operation: "equation.batch_place_order_signing_unverified"
        }
    ));
}

#[test]
fn equation_audit_fixtures_should_cover_markets_risk_positions_first() {
    let market_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/equation/markets_audit.json"
    ))
    .expect("market audit fixture");
    let risk_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/equation/risk_audit.json"
    ))
    .expect("risk audit fixture");
    let positions_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/equation/positions_audit.json"
    ))
    .expect("positions audit fixture");

    assert_eq!(
        parse_equation_audit_subjects(&market_audit).expect("market subjects"),
        vec!["markets", "symbols", "oracle_pricing"]
    );
    assert_eq!(
        parse_equation_audit_subjects(&risk_audit).expect("risk subjects"),
        vec!["funding", "liquidation", "margin_risk"]
    );
    assert_eq!(
        parse_equation_audit_subjects(&positions_audit).expect("positions subjects"),
        vec!["positions", "collateral", "account_state"]
    );
}

#[test]
fn equation_stream_and_signing_helpers_should_make_unverified_boundary_explicit() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: equation_context("public-ws"),
        symbol: equation_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = equation_public_subscribe_payload(&subscription);
    assert_eq!(payload["support"], "unsupported_unverified");
    assert_eq!(equation_keepalive_payload()["type"], "ping");
    assert_eq!(
        equation_stream_reconnect_policy_ms(),
        (30_000, 45_000, 60_000)
    );
    assert_eq!(
        equation_private_stream_capabilities().schema_version,
        EXCHANGE_API_SCHEMA_VERSION
    );
    assert!(equation_wallet_signature_boundary(None).is_err());
}
