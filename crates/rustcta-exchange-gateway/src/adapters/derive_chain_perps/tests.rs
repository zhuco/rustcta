use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, OrderListConditionalLeg,
    OrderListLegType, OrderListRequest, PlaceOrderRequest, PublicStreamKind,
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
async fn derive_chain_perps_advanced_orders_should_stay_signing_unverified_or_unsupported() {
    let adapter = DeriveChainPerpsGatewayAdapter::new(DeriveChainPerpsGatewayConfig::default())
        .expect("adapter");
    let symbol = derive_chain_perps_symbol(MarketType::Perpetual);
    let place = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: derive_chain_perps_context("order"),
        symbol: symbol.clone(),
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
    };

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: derive_chain_perps_context("amend"),
            symbol: symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "2".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "derive_chain_perps.amend_order_signing_unverified"
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: derive_chain_perps_context("oco"),
            symbol: symbol.clone(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("3600".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("2800".to_string()),
                stop_price: Some("3000".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: "derive_chain_perps.order_list_unsupported"
        }
    ));

    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: derive_chain_perps_context("batch"),
        exchange: derive_chain_perps_exchange_id(),
        orders: vec![place],
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

    let cancel = CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: derive_chain_perps_context("cancel"),
        symbol,
        client_order_id: None,
        exchange_order_id: Some("order-1".to_string()),
    };
    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: derive_chain_perps_context("batch-cancel"),
            exchange: derive_chain_perps_exchange_id(),
            cancels: vec![cancel],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "derive_chain_perps.batch_cancel_order_signing_unverified"
        }
    ));

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: derive_chain_perps_context("cancel-all"),
            exchange: derive_chain_perps_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: None,
        })
        .await
        .expect_err("cancel-all unsupported");
    assert!(matches!(
        cancel_all_error,
        ExchangeApiError::Unsupported {
            operation: "derive_chain_perps.cancel_all_orders_signing_unverified"
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
