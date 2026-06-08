use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{
    parse_derive_chain_perps_audit_boundary, parse_derive_chain_perps_audit_subjects,
    DERIVE_CHAIN_PERPS_CHAIN_ID,
};
use super::signing::derive_chain_perps_wallet_signature_boundary;
use super::streams::{
    derive_chain_perps_keepalive_payload, derive_chain_perps_private_stream_capabilities,
    derive_chain_perps_public_subscribe_payload, derive_chain_perps_stream_reconnect_policy_ms,
};
use super::test_support::{
    derive_chain_perps_context, derive_chain_perps_exchange_id, derive_chain_perps_symbol,
};
use super::{DeriveChainPerpsGatewayAdapter, DeriveChainPerpsGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn named_registration_should_accept_derive_chain_perps() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("test", ["derive_chain_perps"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn config_should_describe_derive_chain_profile_without_replacing_derive_adapter() {
    let config = DeriveChainPerpsGatewayConfig::default();
    assert_eq!(config.docs_base_url, "https://docs.derive.xyz");
    assert_eq!(config.rest_base_url, "https://api.derive.xyz");
    assert_eq!(config.rpc_url, "https://rpc.lyra.finance");
    assert_eq!(config.chain_id, DERIVE_CHAIN_PERPS_CHAIN_ID);
    assert!(!config.enabled_public_rest);
    assert!(!config.enabled_private_rest);
}

#[test]
fn capabilities_should_keep_derive_chain_perps_as_chain_profile_audit_only() {
    let adapter = DeriveChainPerpsGatewayAdapter::new(DeriveChainPerpsGatewayConfig::default())
        .expect("adapter");
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
        "../../../../../tests/fixtures/exchanges/derive_chain_perps/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    let boundary = parse_derive_chain_perps_audit_boundary(&boundary).expect("boundary");
    assert_eq!(boundary.chain_id, DERIVE_CHAIN_PERPS_CHAIN_ID);
    assert!(!boundary.trade_enabled);
    assert!(boundary.scan_only);
    assert!(!boundary.stable_api_verified);
}

#[tokio::test]
async fn derive_chain_perps_bulk_orders_should_stay_signing_unverified() {
    let adapter = DeriveChainPerpsGatewayAdapter::new(DeriveChainPerpsGatewayConfig::default())
        .expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: derive_chain_perps_context("batch"),
        exchange: derive_chain_perps_exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: derive_chain_perps_context("order"),
            symbol: derive_chain_perps_symbol(MarketType::Perpetual),
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
            operation: "derive_chain_perps.batch_place_order_signing_unverified"
        }
    ));
}

#[test]
fn derive_chain_perps_audit_fixtures_should_cover_markets_risk_positions_first() {
    let market_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive_chain_perps/markets_audit.json"
    ))
    .expect("market audit fixture");
    let risk_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive_chain_perps/risk_audit.json"
    ))
    .expect("risk audit fixture");
    let positions_audit: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive_chain_perps/positions_audit.json"
    ))
    .expect("positions audit fixture");

    assert_eq!(
        parse_derive_chain_perps_audit_subjects(&market_audit).expect("market subjects"),
        vec!["markets", "symbols", "oracle_pricing"]
    );
    assert_eq!(
        parse_derive_chain_perps_audit_subjects(&risk_audit).expect("risk subjects"),
        vec!["funding", "liquidation", "margin_risk"]
    );
    assert_eq!(
        parse_derive_chain_perps_audit_subjects(&positions_audit).expect("positions subjects"),
        vec!["positions", "collateral", "account_state"]
    );
}

#[test]
fn derive_chain_perps_stream_and_signing_helpers_should_make_unverified_boundary_explicit() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: derive_chain_perps_context("public-ws"),
        symbol: derive_chain_perps_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = derive_chain_perps_public_subscribe_payload(&subscription);
    assert_eq!(payload["support"], "unsupported_unverified");
    assert_eq!(derive_chain_perps_keepalive_payload()["type"], "ping");
    assert_eq!(
        derive_chain_perps_stream_reconnect_policy_ms(),
        (30_000, 45_000, 60_000)
    );
    assert_eq!(
        derive_chain_perps_private_stream_capabilities().schema_version,
        EXCHANGE_API_SCHEMA_VERSION
    );
    assert!(derive_chain_perps_wallet_signature_boundary(None).is_err());
}
