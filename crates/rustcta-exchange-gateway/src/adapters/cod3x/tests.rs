use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    ExchangeApiError, ExchangeClient, OrderListConditionalLeg, OrderListLegType, OrderListRequest,
    PlaceOrderRequest, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
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
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(!capabilities
        .capabilities_v2
        .batch_place_orders
        .support
        .is_supported());
    assert!(!capabilities
        .capabilities_v2
        .batch_cancel_orders
        .support
        .is_supported());
    assert!(!capabilities
        .capabilities_v2
        .cancel_all_orders
        .is_supported());

    let boundary_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    let boundary = parse_cod3x_audit_boundary(&boundary_value).expect("boundary");
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
    assert_eq!(
        boundary_value["balance_read_boundary"]["support"],
        "spec_only"
    );
}

#[tokio::test]
async fn cod3x_advanced_orders_should_stay_routing_unverified() {
    let adapter = Cod3xGatewayAdapter::new(Cod3xGatewayConfig::default()).expect("adapter");

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: cod3x_context("amend"),
            symbol: cod3x_symbol(MarketType::Perpetual),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: Some("client-1-replace".to_string()),
            new_quantity: "1".to_string(),
        })
        .await
        .expect_err("unsupported");
    assert_unsupported_operation(amend_error, "cod3x.amend_order_routing_unverified");

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: cod3x_context("order-list"),
            symbol: cod3x_symbol(MarketType::Perpetual),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("3300".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("3100".to_string()),
                stop_price: Some("3150".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("unsupported");
    assert_unsupported_operation(order_list_error, "cod3x.order_list_unsupported");

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
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
        })
        .await
        .expect_err("unsupported");
    assert_unsupported_operation(
        batch_place_error,
        "cod3x.batch_place_order_routing_unverified",
    );

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: cod3x_context("batch-cancel"),
            exchange: cod3x_exchange_id(),
            cancels: vec![],
        })
        .await
        .expect_err("unsupported");
    assert_unsupported_operation(
        batch_cancel_error,
        "cod3x.batch_cancel_order_routing_unverified",
    );

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: cod3x_context("cancel-all"),
            exchange: cod3x_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(cod3x_symbol(MarketType::Perpetual)),
        })
        .await
        .expect_err("unsupported");
    assert_unsupported_operation(
        cancel_all_error,
        "cod3x.cancel_all_orders_routing_unverified",
    );

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(
        boundary["advanced_order_boundary"]["support"],
        "unsupported"
    );
    assert_eq!(
        boundary["expected_runtime_errors"]["batch_cancel_orders"],
        "cod3x.batch_cancel_order_routing_unverified"
    );
}

#[test]
fn cod3x_advanced_order_mapping_should_stay_unsupported() {
    let mapping = include_str!("endpoint_mapping.yaml");
    for operation in [
        "amend_order",
        "place_order_list",
        "batch_place_orders",
        "batch_cancel_orders",
        "cancel_all_orders",
    ] {
        assert!(
            mapping.contains(&format!("operation: {operation}")),
            "missing {operation} endpoint"
        );
    }
    assert!(mapping.contains("support: unsupported"));
    assert!(mapping.contains("auth: unsupported"));
    assert!(mapping.contains("native_batch: false"));
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
    let balances_source: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cod3x/request_specs/get_balances_source_boundary.json"
    ))
    .expect("balances source fixture");

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
    assert_eq!(balances_source["support"], "spec_only");
    assert_eq!(
        balances_source["account_model"],
        "downstream_venue_account_or_wallet"
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

fn assert_unsupported_operation(error: ExchangeApiError, expected: &'static str) {
    match error {
        ExchangeApiError::Unsupported { operation } => assert_eq!(operation, expected),
        other => panic!("expected unsupported {expected}, got {other:?}"),
    }
}
