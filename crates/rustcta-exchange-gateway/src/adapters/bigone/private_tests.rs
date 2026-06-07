use rustcta_exchange_api::{
    BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest, ExchangeClient,
    OpenOrdersRequest, PlaceOrderRequest, QueryOrderRequest, RecentFillsRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_api::{CapabilitySupport, ReconcileTrigger};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::capabilities::{
    bigone_auth_renewal_policy, bigone_rate_limit_plan, bigone_reconcile_plan,
    bigone_stream_runtime_capability, BIGONE_MAX_PAGE_LIMIT,
};
use super::private_parser::{parse_balances, parse_order_state};
use super::signing::bigone_jwt;
use super::test_support::{
    context, exchange_id, perp_symbol_scope, private_config, spawn_rest_server, spot_symbol_scope,
};
use super::{BigOneGatewayAdapter, BigOneGatewayConfig};

#[test]
fn bigone_private_parser_should_parse_balances_and_orders() {
    let balance_fixture = fixture("balance.json");
    let balances = parse_balances(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &balance_fixture,
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "USDT");

    let order = parse_order_state(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("order_ack.json"),
    )
    .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));
}

#[test]
fn bigone_private_parser_should_cover_empty_error_and_missing_field_fixtures() {
    let empty_balances = parse_balances(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &fixture("empty_response.json"),
    )
    .expect("empty balances");
    assert!(empty_balances[0].balances.is_empty());

    let empty_order = parse_order_state(
        &exchange_id(),
        None,
        MarketType::Spot,
        &fixture("empty_response.json"),
    )
    .expect_err("empty order should be invalid");
    assert!(empty_order.to_string().contains("empty"));

    let missing_symbol = parse_order_state(
        &exchange_id(),
        None,
        MarketType::Spot,
        &fixture("missing_fields_order.json"),
    )
    .expect_err("missing symbol should be invalid");
    assert!(missing_symbol.to_string().contains("missing symbol"));

    let error = super::parser::parse_error(
        exchange_id(),
        "BigONE fixture error",
        &fixture("error.json"),
    );
    assert!(error.to_string().contains("invalid order id"));
}

#[tokio::test]
async fn bigone_private_capabilities_should_require_credentials() {
    let public = BigOneGatewayAdapter::new(BigOneGatewayConfig::default()).expect("adapter");
    assert!(!public.capabilities().supports_private_rest);
    assert!(matches!(
        public.capabilities().capabilities_v2.private_rest,
        CapabilitySupport::Unsupported { .. }
    ));

    let private = BigOneGatewayAdapter::new(BigOneGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BigOneGatewayConfig::default()
    })
    .expect("adapter");
    assert!(private.capabilities().supports_private_rest);
    assert!(private.capabilities().supports_batch_place_order);
    let capabilities = private.capabilities();
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(BIGONE_MAX_PAGE_LIMIT)
    );
    assert!(capabilities.capabilities_v2.public_streams.is_supported());
    assert!(capabilities.capabilities_v2.private_streams.is_supported());
    assert!(capabilities
        .capabilities_v2
        .batch_place_orders
        .support
        .is_supported());
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .same_market_type_required
    );

    let stream = bigone_stream_runtime_capability();
    assert!(stream.resync.order_book);
    assert!(stream.reconnect.requires_resubscribe);
    assert!(stream.orderbook_requires_snapshot_after_reconnect);
    assert_eq!(
        bigone_auth_renewal_policy().renewal_interval_ms,
        Some(25 * 60 * 1000)
    );
    assert!(bigone_rate_limit_plan().validate().is_ok());
    assert_eq!(bigone_rate_limit_plan().buckets().len(), 4);
    let reconcile = bigone_reconcile_plan(
        exchange_id(),
        ReconcileTrigger::PlaceOrderTimeout,
        Some(spot_symbol_scope()),
        "request-spec test",
    );
    assert!(reconcile.requires_query_order);
    assert!(reconcile.requires_open_orders);
    assert!(reconcile.requires_recent_fills);
}

#[tokio::test]
async fn bigone_spot_cancel_should_use_documented_cancel_endpoint() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "data": {
            "id": "order-1",
            "asset_pair_name": "BTC-USDT",
            "side": "BUY",
            "type": "LIMIT",
            "state": "CANCELED",
            "amount": "0.1",
            "filled_amount": "0"
        }
    })])
    .await;
    let adapter = BigOneGatewayAdapter::new(private_config(base_url)).expect("adapter");

    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
        })
        .await
        .expect("cancel");

    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    request_spec("cancel_order_spot.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("cancel request spec");
    assert!(requests[0]
        .header("authorization")
        .is_some_and(|value| value.starts_with("Bearer ")));
    assert!(!requests[0].raw_contains_secret());
}

#[tokio::test]
async fn bigone_contract_place_should_use_v2_and_jwt_bearer_auth() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "data": {
            "id": "contract-order-1",
            "instrument_id": "BTC-USDT",
            "side": "BUY",
            "type": "LIMIT",
            "state": "OPEN",
            "amount": "0.1",
            "price": "100",
            "filled_amount": "0"
        }
    })])
    .await;
    let adapter = BigOneGatewayAdapter::new(private_config(base_url)).expect("adapter");

    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.1".to_string(),
            price: Some("100".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("place");

    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    request_spec("place_order_contract.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("contract place request spec");
    assert!(requests[0]
        .header("authorization")
        .is_some_and(|value| value.starts_with("Bearer ")));
    assert!(!requests[0].raw_contains_secret());
    assert!(requests[0].body.contains("\"reduce_only\":true"));
}

#[tokio::test]
async fn bigone_private_request_specs_should_cover_core_read_write_paths() {
    let responses = vec![
        fixture("balance.json"),
        fixture("order_ack.json"),
        fixture("order_ack.json"),
        fixture("open_orders.json"),
        fixture("open_orders.json"),
        fixture("fill.json"),
        fixture("open_orders.json"),
    ];
    let (base_url, seen) = spawn_rest_server(responses).await;
    let adapter = BigOneGatewayAdapter::new(private_config(base_url)).expect("adapter");

    adapter
        .get_balances(rustcta_exchange_api::BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    adapter
        .place_order(PlaceOrderRequest {
            price: Some("65000".to_string()),
            .._place_request()
        })
        .await
        .expect("place");
    adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
        })
        .await
        .expect("query");
    adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                price: Some("65000".to_string()),
                .._place_request()
            }],
        })
        .await
        .expect("batch place");
    adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-contract"),
                symbol: perp_symbol_scope(),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            }],
        })
        .await
        .expect("batch cancel");

    let requests = seen.lock().unwrap();
    let specs = [
        "get_balances_spot.json",
        "place_order_spot.json",
        "query_order_spot.json",
        "batch_place_spot.json",
        "open_orders_spot.json",
        "recent_fills_spot.json",
        "batch_cancel_contract.json",
    ];
    assert_eq!(requests.len(), specs.len());
    for (request, spec) in requests.iter().zip(specs) {
        request_spec(spec)
            .assert_matches(&request.actual_http_request())
            .unwrap_or_else(|error| panic!("{spec}: {error}"));
    }
}

#[test]
fn bigone_signing_vector_should_verify_fixed_jwt() {
    let vector = signing_vector("jwt_openapi.json");
    vector.verify().expect("hmac vector");
    let token = bigone_jwt(
        "test-key",
        "test-secret",
        vector
            .timestamp
            .as_deref()
            .expect("timestamp")
            .parse()
            .expect("timestamp integer"),
    )
    .expect("jwt");
    assert_eq!(
        token,
        format!(
            "{}.{}",
            vector.payload,
            vector
                .expected_signature
                .trim_end_matches('=')
                .replace('+', "-")
                .replace('/', "_")
        )
    );
}

#[allow(dead_code)]
fn _place_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("client-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.1".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn fixture(name: &str) -> serde_json::Value {
    serde_json::from_str(match name {
        "balance.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/balance.json")
        }
        "order_ack.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/order_ack.json")
        }
        "open_orders.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/open_orders.json")
        }
        "fill.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/fill.json"),
        "empty_response.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/empty_response.json")
        }
        "error.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/error.json"),
        "missing_fields_order.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/missing_fields_order.json")
        }
        other => panic!("unknown BigONE fixture {other}"),
    })
    .expect("fixture json")
}

fn request_spec(name: &str) -> RequestSpec {
    serde_json::from_str(match name {
        "get_balances_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/get_balances_spot.json"),
        "place_order_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/place_order_spot.json"),
        "place_order_contract.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/place_order_contract.json"),
        "cancel_order_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/cancel_order_spot.json"),
        "query_order_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/query_order_spot.json"),
        "batch_place_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/batch_place_spot.json"),
        "open_orders_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/open_orders_spot.json"),
        "recent_fills_spot.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/recent_fills_spot.json"),
        "batch_cancel_contract.json" => include_str!("../../../../../tests/fixtures/exchanges/bigone/request_specs/batch_cancel_contract.json"),
        other => panic!("unknown BigONE request spec {other}"),
    })
    .expect("request spec json")
}

fn signing_vector(name: &str) -> SigningVector {
    serde_json::from_str(match name {
        "jwt_openapi.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bigone/signing_vectors/jwt_openapi.json"
        ),
        other => panic!("unknown BigONE signing vector {other}"),
    })
    .expect("signing vector json")
}
