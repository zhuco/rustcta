use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::parser::{parse_depth_event, parse_market_specs};
use super::private_parser::{parse_batch_place_orders_ack, parse_order_ack};
use super::signing::{sign_rest_request, sign_ws_login};
use super::streams::{
    oxfun_ping_payload, oxfun_private_login_payload, oxfun_public_subscribe_payload,
    oxfun_public_unsubscribe_payload, oxfun_reconnect_policy_ms,
};
use super::{OxfunGatewayAdapter, OxfunGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("oxfun").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol(market_type: MarketType) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTC-USD-SWAP-LIN")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_expose_audit_scope_without_live_trading() {
    let adapter = OxfunGatewayAdapter::new(OxfunGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Perpetual, MarketType::Option]
    );
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.method.as_deref() == Some("placeorders")
            && endpoint.path.as_deref() == Some("/v2/websocket")
            && !endpoint.support.is_supported()));

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["api_maturity"], "audit_shell");
    assert_eq!(
        boundary["advanced_order_separation"]["project_unimplemented"],
        serde_json::json!(["batch_place_orders"])
    );
    assert_eq!(
        boundary["advanced_order_separation"]["unsupported"],
        serde_json::json!(["amend_order", "place_order_list", "batch_cancel_orders"])
    );

    for operation in ["amend_order", "place_order_list", "batch_cancel_orders"] {
        assert!(capabilities
            .capabilities_v2
            .endpoints
            .iter()
            .any(|endpoint| endpoint.operation == operation && !endpoint.support.is_supported()));
    }
}

#[test]
fn signing_vectors_should_match_oxfun_docs_shape() {
    let signature = sign_rest_request(
        "secret",
        "2026-06-08T00:00:00Z",
        "nonce-1",
        "GET",
        "api.ox.fun",
        "/v3/account",
        "asset=OX",
    )
    .expect("signature");
    assert_eq!(signature, "8N+Ov1G0potibfVQkbwA5ZaOGPASA7I3xnxqR+F+DTw=");
    assert_eq!(
        sign_ws_login("secret", "1700000000000").expect("ws signature"),
        "qNyCEuPA4GPpD3gYnapr/2/0OOe1C+GUn5QWMIV8GoY="
    );

    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/signing_vectors/rest_hmac.json"
    ))
    .expect("vector");
    assert_eq!(vector["expected_signature"], signature);
}

#[test]
fn websocket_helpers_should_build_login_subscription_and_heartbeat_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = oxfun_public_subscribe_payload(&subscription);
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["args"][0], "depthUpdate:BTC-USD-SWAP-LIN");
    let login = oxfun_private_login_payload("key", "secret", "1700000000000").expect("login");
    assert_eq!(login["op"], "login");
    assert_eq!(
        login["data"]["signature"],
        "qNyCEuPA4GPpD3gYnapr/2/0OOe1C+GUn5QWMIV8GoY="
    );
    let unsubscribe = oxfun_public_unsubscribe_payload(&subscription);
    assert_eq!(unsubscribe["op"], "unsubscribe");
    assert_eq!(unsubscribe["args"][0], "depthUpdate:BTC-USD-SWAP-LIN");
    assert_eq!(
        oxfun_ping_payload(),
        serde_json::Value::String("ping".to_string())
    );
    assert_eq!(oxfun_reconnect_policy_ms(), (20_000, 45_000, 60_000));
}

#[test]
fn parser_fixtures_should_cover_market_depth_and_private_ack() {
    let markets = parse_market_specs(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/market_snapshot.json"
    ))
    .expect("markets");
    assert_eq!(markets[0].market_code, "BTC-USD-SWAP-LIN");
    assert_eq!(markets[0].tick_size.as_deref(), Some("0.5"));

    let depth = parse_depth_event(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/depth_update_snapshot.json"
    ))
    .expect("depth");
    assert_eq!(depth.sequence, Some(101));
    assert_eq!(depth.action.as_deref(), Some("partial"));
    assert_eq!(depth.checksum, Some(7654321));
    assert_eq!(depth.bids[0], ("64000".to_string(), "1.25".to_string()));

    let diff = parse_depth_event(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/depth_update_diff.json"
    ))
    .expect("diff");
    assert_eq!(diff.action.as_deref(), Some("increment"));
    assert_eq!(diff.sequence, Some(102));
    assert_eq!(diff.asks[0], ("64001".to_string(), "0".to_string()));

    let (submitted, order_id) = parse_order_ack(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/private_order_ack.json"
    ))
    .expect("ack");
    assert!(submitted);
    assert_eq!(order_id.as_deref(), Some("1000000700008"));
}

#[test]
fn batch_place_parser_should_preserve_partial_reconciliation_boundary() {
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("order-parser"),
        symbol: symbol(MarketType::Perpetual),
        client_order_id: Some("1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "1".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-parser"),
        exchange: exchange_id(),
        orders: vec![order.clone()],
    };

    let parsed = parse_batch_place_orders_ack(
        &exchange_id(),
        &request,
        include_str!(
            "../../../../../tests/fixtures/exchanges/oxfun/ws/private_batch_place_ack_single.json"
        ),
    )
    .expect("batch place ack");
    assert_eq!(parsed.orders.len(), 1);
    assert_eq!(
        parsed.orders[0].exchange_order_id.as_deref(),
        Some("1000000700008")
    );
    assert_eq!(parsed.orders[0].client_order_id.as_deref(), Some("1"));
    assert_eq!(parsed.report.as_ref().expect("report").failed_count(), 0);

    let partial_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-parser-partial"),
        exchange: exchange_id(),
        orders: vec![
            order,
            PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("order-parser-failed"),
                symbol: symbol(MarketType::Perpetual),
                client_order_id: Some("2".to_string()),
                side: OrderSide::Buy,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: None,
                quantity: "0.7".to_string(),
                price: Some("9420".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            },
        ],
    };
    let partial = parse_batch_place_orders_ack(
        &exchange_id(),
        &partial_request,
        include_str!(
            "../../../../../tests/fixtures/exchanges/oxfun/ws/private_batch_place_ack_partial.json"
        ),
    )
    .expect("partial batch place ack");
    assert_eq!(partial.orders.len(), 1);
    let report = partial.report.as_ref().expect("partial report");
    assert_eq!(report.total_items, 2);
    assert_eq!(report.failed_count(), 1);
    assert!(report.results[1].reconcile_plan.is_some());

    let expected_report: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/private_batch_place_reconciliation_report.json"
    ))
    .expect("reconciliation report");
    assert_eq!(expected_report["runtime_enabled"], false);
    assert_eq!(
        expected_report["partial_failure_policy"],
        "do_not_replay_without_private_stream_or_rest_readback"
    );
    assert_eq!(
        expected_report["expected_failed_count"],
        serde_json::json!(report.failed_count())
    );
}

#[tokio::test]
async fn batch_place_should_be_explicitly_spec_only() {
    let adapter = OxfunGatewayAdapter::new(OxfunGatewayConfig::default()).expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: symbol(MarketType::Perpetual),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "1".to_string(),
            price: Some("64000".to_string()),
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
            operation: "oxfun.batch_place_orders_ws_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn unsupported_advanced_orders_should_keep_explicit_runtime_guards() {
    let adapter = OxfunGatewayAdapter::new(OxfunGatewayConfig::default()).expect("adapter");

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol(MarketType::Perpetual),
            client_order_id: Some("1".to_string()),
            exchange_order_id: Some("1000000700008".to_string()),
            new_client_order_id: Some("1-replace".to_string()),
            new_quantity: "1".to_string(),
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "oxfun.amend_order_unverified"
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol(MarketType::Perpetual),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("66000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("64000".to_string()),
                stop_price: Some("64500".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: "oxfun.order_list_unverified"
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: symbol(MarketType::Perpetual),
                client_order_id: Some("1".to_string()),
                exchange_order_id: Some("1000000700008".to_string()),
            }],
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "oxfun.batch_cancel_orders_ws_request_spec_only"
        }
    ));
}

#[test]
fn account_position_source_boundary_should_stay_offline_and_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/request_specs/get_positions_account_source.json"
    ))
    .expect("positions source boundary");

    assert_eq!(boundary["operation"], "get_positions");
    assert_eq!(boundary["support"], "spec_only");
    assert_eq!(boundary["runtime_enabled"], false);
    assert_eq!(boundary["fixture_policy"]["contains_api_secret"], false);
    assert_eq!(boundary["fixture_policy"]["live_call_allowed"], false);
}
