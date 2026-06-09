use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CapabilitySupport,
    CredentialScope, EndpointAuth, EndpointTransport, ExchangeApiError, ExchangeClient,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PrivateStreamKind, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};

use super::parser::{parse_instrument_metadata, parse_subaccount_position_names};
use super::private::{
    build_amend_order_replace_request_spec, replace_rpc_params_fixture, AMEND_ORDER_UNSUPPORTED,
};
use super::private_parser::{parse_replace_order_ack, private_event_channel, private_rpc_method};
use super::signing::{
    derive_fixture_hmac, derive_json_rpc_payload, derive_session_canonical_payload,
};
use super::streams::{
    derive_heartbeat_policy_ms, derive_login_payload, derive_ping_payload,
    derive_private_unsubscribe_payload, derive_public_subscribe_payload,
    derive_public_unsubscribe_payload,
};
use super::test_support::{context, exchange_id, symbol};
use super::{DeriveGatewayAdapter, DeriveGatewayConfig};
use crate::request_spec::RequestSpec;

#[test]
fn derive_capabilities_should_keep_trade_runtime_closed_without_session_keys() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Option, MarketType::Perpetual]
    );
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
}

#[test]
fn derive_session_signing_vector_should_match_fixture() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/signing_vectors/session_order_hmac.json"
    ))
    .expect("vector");
    let payload = derive_session_canonical_payload(
        vector["method"].as_str().unwrap(),
        vector["subaccount_id"].as_str().unwrap(),
        vector["timestamp_ms"].as_str().unwrap(),
        vector["params_json"].as_str().unwrap(),
    );
    assert_eq!(payload, vector["canonical_payload"]);
    assert_eq!(
        derive_fixture_hmac("fixture-session-material", &payload),
        vector["fixture_signature"]
    );
}

#[test]
fn derive_replace_signing_vector_should_match_fixture() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/signing_vectors/session_replace_hmac.json"
    ))
    .expect("vector");
    let payload = derive_session_canonical_payload(
        vector["method"].as_str().unwrap(),
        vector["subaccount_id"].as_str().unwrap(),
        vector["timestamp_ms"].as_str().unwrap(),
        vector["params_json"].as_str().unwrap(),
    );
    assert_eq!(payload, vector["canonical_payload"]);
    assert_eq!(
        derive_fixture_hmac("fixture-session-material", &payload),
        vector["fixture_signature"]
    );
}

#[test]
fn derive_option_and_perp_metadata_should_parse_from_fixture() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/instruments.json"
    ))
    .expect("instruments fixture");
    let parsed = parse_instrument_metadata(&instruments);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].instrument_type, "perp");
    assert_eq!(parsed[1].instrument_type, "option");
    assert_eq!(parsed[1].option_type.as_deref(), Some("call"));
}

#[test]
fn derive_positions_fixture_should_capture_subaccount_model() {
    let positions: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/positions.json"
    ))
    .expect("positions fixture");
    assert_eq!(
        parse_subaccount_position_names(&positions),
        vec!["BTC-PERP"]
    );
}

#[test]
fn derive_json_rpc_and_ws_helpers_should_build_payloads() {
    let rpc = derive_json_rpc_payload("public/get_all_instruments", r#"{"currency":"BTC"}"#, 7);
    assert!(rpc.contains(r#""method":"public/get_all_instruments""#));
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(MarketType::Perpetual, "BTC-PERP"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = derive_public_subscribe_payload(&subscription);
    assert_eq!(payload["params"]["channels"][0], "orderbook.BTC-PERP.100");
    assert_eq!(
        derive_public_unsubscribe_payload(&subscription)["method"],
        "unsubscribe"
    );
    assert_eq!(
        derive_private_unsubscribe_payload(&PrivateStreamKind::Orders).unwrap()["params"]
            ["channels"][0],
        "private.orders"
    );
    assert_eq!(
        derive_login_payload("fixture-wallet", "fixture-session-key", "fixture-signature")
            ["method"],
        "public/login"
    );
    assert_eq!(derive_ping_payload()["method"], "public/ping");
    assert_eq!(derive_heartbeat_policy_ms(), (30_000, 10_000, 60_000));
    let event: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/ws/private_order_update.json"
    ))
    .expect("private order event fixture");
    assert_eq!(private_rpc_method(&event), Some("subscription"));
    assert_eq!(private_event_channel(&event), Some("private.orders"));
}

#[test]
fn derive_amend_replace_request_spec_should_match_offline_builder() {
    let request = build_amend_order_replace_request_spec(
        "fixture-session-key",
        "fixture-session-signature",
        1_700_000_000_000,
    )
    .expect("replace request");
    let spec: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/request_specs/amend_order_replace.json"
    ))
    .expect("request spec");
    spec.assert_matches(&request.actual_http_request())
        .expect("replace request spec");
    assert_eq!(request.body["method"], "private/replace");
    assert_eq!(
        request.body["params"],
        serde_json::json!(replace_rpc_params_fixture())
    );

    let source_boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/request_specs/amend_order_replace_source_boundary.json"
    ))
    .expect("source boundary");
    assert_eq!(
        source_boundary["official_capability"]["method"],
        "private/replace"
    );
    assert_eq!(
        source_boundary["unsupported_separation"]["project_unimplemented"],
        serde_json::json!(["amend_order"])
    );
    assert_eq!(
        source_boundary["expected_runtime_errors"]["amend_order"],
        AMEND_ORDER_UNSUPPORTED
    );
}

#[test]
fn derive_private_parser_should_parse_replace_ack_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/derive/parser/amend_order_replace_ack.json"
    ))
    .expect("replace ack");
    let order = parse_replace_order_ack(
        &exchange_id(),
        symbol(MarketType::Perpetual, "BTC-PERP"),
        &fixture,
    )
    .expect("parsed order");
    assert_eq!(
        order.exchange_order_id.as_deref(),
        Some("fixture-replaced-order-id")
    );
    assert_eq!(
        order.client_order_id.as_deref(),
        Some("fixture-client-order-id-replace")
    );
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.price.as_deref(), Some("65100"));
}

#[tokio::test]
async fn derive_place_order_should_validate_market_then_return_session_boundary() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Perpetual, "BTC-PERP"),
        client_order_id: Some("fixture-client-order-id".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "derive.place_order_session_signature_offline_only"
        }
    ));
    assert_eq!(exchange_id().as_str(), "derive");
}

#[tokio::test]
async fn derive_amend_order_should_remain_request_spec_only() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");
    let request = AmendOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("amend"),
        symbol: symbol(MarketType::Perpetual, "BTC-PERP"),
        client_order_id: Some("fixture-client-order-id".to_string()),
        exchange_order_id: Some("fixture-order-id".to_string()),
        new_client_order_id: Some("fixture-client-order-id-replace".to_string()),
        new_quantity: "0.01".to_string(),
    };
    let error = adapter.amend_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: AMEND_ORDER_UNSUPPORTED
        }
    ));
}

#[tokio::test]
async fn derive_batch_and_order_list_should_remain_unsupported() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");

    let batch_place = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place"),
        exchange: exchange_id(),
        orders: vec![],
    };
    let error = adapter
        .batch_place_orders(batch_place)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "derive.batch_place_orders_unsupported"
        }
    ));

    let batch_cancel = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel"),
        exchange: exchange_id(),
        cancels: vec![],
    };
    let error = adapter
        .batch_cancel_orders(batch_cancel)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "derive.batch_cancel_orders_unsupported"
        }
    ));

    let order_list = OrderListRequest::Oco {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("order-list"),
        symbol: symbol(MarketType::Perpetual, "BTC-PERP"),
        list_client_order_id: Some("fixture-oco-list".to_string()),
        side: OrderSide::Sell,
        quantity: "0.01".to_string(),
        above: OrderListConditionalLeg {
            order_type: OrderListLegType::Limit,
            price: Some("66000".to_string()),
            stop_price: None,
            time_in_force: None,
            client_order_id: Some("fixture-oco-above".to_string()),
        },
        below: OrderListConditionalLeg {
            order_type: OrderListLegType::StopLossLimit,
            price: Some("64000".to_string()),
            stop_price: Some("64500".to_string()),
            time_in_force: None,
            client_order_id: Some("fixture-oco-below".to_string()),
        },
    };
    let error = adapter
        .place_order_list(order_list)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "derive.order_list_unsupported"
        }
    ));
}

#[test]
fn derive_capabilities_v2_should_separate_amend_from_unsupported_batch_boundaries() {
    let adapter = DeriveGatewayAdapter::new(DeriveGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    let amend = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "derive.amend_order")
        .expect("amend endpoint");
    assert!(!amend.support.is_supported());
    assert!(matches!(
        amend.support,
        CapabilitySupport::Unsupported { ref reason } if reason == AMEND_ORDER_UNSUPPORTED
    ));
    assert_eq!(amend.transport, EndpointTransport::JsonRpc);
    assert_eq!(amend.method.as_deref(), Some("POST"));
    assert_eq!(amend.path.as_deref(), Some("/"));
    assert_eq!(amend.auth, EndpointAuth::Jwt);
    assert_eq!(amend.credential_scopes, vec![CredentialScope::Trade]);
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .support
            .clone()
            .is_supported()
            == false
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .support
            .clone()
            .is_supported()
            == false
    );
}
