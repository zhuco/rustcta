use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, FeesRequest, OpenOrdersRequest, OrderListLegType, OrderListOrderLeg,
    OrderListRequest, PlaceOrderRequest, QueryOrderRequest, RecentFillsRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType, PositionSide, TimeInForce};

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::private::{
    build_amend_order_request_spec, build_batch_cancel_orders_request_spec,
    build_cancel_order_request_spec, build_get_balances_request_spec,
    build_get_fees_spot_request_spec, build_place_order_list_request_spec,
    build_place_order_request_spec,
};
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_spot_batch_cancel_ack, parse_spot_order_ack,
    parse_spot_order_list_ack,
};
use super::test_support::{
    config_with_base_url, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::FmfwioGatewayAdapter;

fn load_request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "place_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/place_order.json"
        ),
        "cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/cancel_order.json"
        ),
        "get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/get_balances.json"
        ),
        "get_fees_spot.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/get_fees_spot.json"
        ),
        "amend_order_replace.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/amend_order_replace.json"
        ),
        "place_order_list_otoco.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/place_order_list_otoco.json"
        ),
        "batch_cancel_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/request_specs/batch_cancel_orders.json"
        ),
        _ => panic!("unknown fmfwio request spec {name}"),
    };
    serde_json::from_str(text).expect("request spec")
}

fn load_signing_vector(name: &str) -> SigningVector {
    let text = match name {
        "place_order_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/place_order_hs256.json"
        ),
        "cancel_order_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/cancel_order_hs256.json"
        ),
        "get_fees_spot_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/get_fees_spot_hs256.json"
        ),
        "ws_login_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/ws_login_hs256.json"
        ),
        "amend_order_replace_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/amend_order_replace_hs256.json"
        ),
        "place_order_list_otoco_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/place_order_list_otoco_hs256.json"
        ),
        "batch_cancel_orders_hs256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/signing_vectors/batch_cancel_orders_hs256.json"
        ),
        _ => panic!("unknown fmfwio signing vector {name}"),
    };
    serde_json::from_str(text).expect("signing vector")
}

fn load_parser_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "spot_order_ack.json" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/fmfwio/parser/spot_order_ack.json"
            )
        }
        "spot_balances.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/fmfwio/parser/spot_balances.json")
        }
        "spot_fees.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/fmfwio/parser/spot_fees.json")
        }
        "spot_order_list_ack.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/parser/spot_order_list_ack.json"
        ),
        "batch_cancel_orders_ack.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/fmfwio/parser/batch_cancel_orders_ack.json"
        ),
        _ => panic!("unknown fmfwio parser fixture {name}"),
    };
    serde_json::from_str(text).expect("parser fixture")
}

#[tokio::test]
async fn fmfwio_adapter_should_guard_private_operations_without_credentials() {
    let adapter = FmfwioGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_balances);
    assert!(!capabilities.supports_fees);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(matches!(
        capabilities.capabilities_v2.batch_cancel_orders.support,
        CapabilitySupport::Unsupported { .. }
    ));
    for operation in ["amend_order", "place_order_list"] {
        let endpoint = capabilities
            .capabilities_v2
            .endpoints
            .iter()
            .find(|endpoint| endpoint.operation == operation)
            .expect("fmfwio spec-only endpoint");
        assert!(matches!(endpoint.support, CapabilitySupport::Native));
    }
    let batch_cancel_endpoint = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "batch_cancel_orders")
        .expect("batch cancel endpoint");
    assert!(matches!(
        batch_cancel_endpoint.support,
        CapabilitySupport::Unsupported { .. }
    ));

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn fmfwio_fees_should_send_signed_private_rest_request() {
    let (base_url, seen) = spawn_rest_server(vec![load_parser_fixture("spot_fees.json")]).await;
    let adapter = FmfwioGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_fees);

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(response.fees.len(), 2);
    assert_eq!(response.fees[0].maker_rate, "-0.0001");
    assert_eq!(response.fees[0].taker_rate, "0.001");

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/api/3/spot/fee");
    assert!(seen[0].headers.contains_key("authorization"));
}

#[tokio::test]
async fn fmfwio_balances_should_send_signed_private_rest_request() {
    let (base_url, seen) = spawn_rest_server(vec![load_parser_fixture("spot_balances.json")]).await;
    let adapter = FmfwioGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_balances);

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "USDT");
    assert_eq!(response.balances[0].balances[0].available, 100.0);

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/api/3/spot/balance");
    assert!(seen[0].headers.contains_key("authorization"));
}

#[tokio::test]
async fn fmfwio_advanced_orders_should_send_signed_private_rest_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        load_parser_fixture("spot_order_ack.json"),
        load_parser_fixture("spot_order_list_ack.json"),
    ])
    .await;
    let adapter = FmfwioGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_amend_order);
    assert!(capabilities.supports_order_list);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope(),
            client_order_id: Some("cli-fmfwio-1".to_string()),
            exchange_order_id: None,
            new_client_order_id: Some("cli-fmfwio-1-r1".to_string()),
            new_quantity: "0.050".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(
        amended.order.client_order_id.as_deref(),
        Some("cli-fmfwio-1")
    );

    let order_list = adapter
        .place_order_list(OrderListRequest::Oto {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol_scope(),
            list_client_order_id: Some("cli-fmfwio-otoco".to_string()),
            working: OrderListOrderLeg {
                side: OrderSide::Buy,
                order_type: OrderListLegType::Limit,
                quantity: "0.063".to_string(),
                price: Some("0.046016".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("cli-fmfwio-otoco-primary".to_string()),
            },
            pending: OrderListOrderLeg {
                side: OrderSide::Sell,
                order_type: OrderListLegType::Limit,
                quantity: "0.063".to_string(),
                price: Some("0.050000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("cli-fmfwio-otoco-tp".to_string()),
            },
        })
        .await
        .expect("order list");
    assert_eq!(order_list.orders.len(), 3);

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "PATCH");
    assert_eq!(seen[0].path, "/api/3/spot/order/cli-fmfwio-1");
    assert!(seen[0].body.contains("quantity=0.050"));
    assert!(seen[0].headers.contains_key("authorization"));
    assert_eq!(seen[1].method, "POST");
    assert_eq!(seen[1].path, "/api/3/spot/order/list");
    assert!(seen[1].body.contains("oneTriggerOther"));
    assert!(seen[1].headers.contains_key("authorization"));
}

#[tokio::test]
async fn fmfwio_core_trading_should_send_signed_private_rest_requests() {
    let order_ack = load_parser_fixture("spot_order_ack.json");
    let (base_url, seen) = spawn_rest_server(vec![
        order_ack.clone(),
        order_ack.clone(),
        order_ack.clone(),
        serde_json::json!([order_ack.clone()]),
        serde_json::json!([{
            "id": "trade-1",
            "order_id": "840450210",
            "client_order_id": "cli-fmfwio-1",
            "symbol": "ETHBTC",
            "side": "sell",
            "price": "0.046016",
            "quantity": "0.063",
            "fee": "0.00001",
            "fee_currency": "BTC",
            "liquidity": "taker",
            "timestamp": "2024-04-15T17:02:05.092Z"
        }]),
    ])
    .await;
    let adapter = FmfwioGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");

    adapter
        .place_order(place_order_request())
        .await
        .expect("place order");
    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("cli-fmfwio-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("cancel order");
    adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: Some("cli-fmfwio-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("query order");
    assert_eq!(
        adapter
            .get_open_orders(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("open"),
                exchange: exchange_id(),
                market_type: Some(rustcta_types::MarketType::Spot),
                symbol: Some(symbol_scope()),
                page: None,
            })
            .await
            .expect("open orders")
            .orders
            .len(),
        1
    );
    assert_eq!(
        adapter
            .get_recent_fills(RecentFillsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("fills"),
                exchange: exchange_id(),
                market_type: Some(rustcta_types::MarketType::Spot),
                symbol: Some(symbol_scope()),
                client_order_id: None,
                exchange_order_id: None,
                from_trade_id: None,
                start_time: None,
                end_time: None,
                limit: Some(100),
                page: None,
            })
            .await
            .expect("fills")
            .fills
            .len(),
        1
    );

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/api/3/spot/order");
    assert!(seen[0].body.contains("client_order_id=cli-fmfwio-1"));
    assert!(seen[0].headers.contains_key("authorization"));
    assert_eq!(seen[1].method, "DELETE");
    assert_eq!(seen[1].path, "/api/3/spot/order/cli-fmfwio-1");
    assert_eq!(seen[2].method, "GET");
    assert_eq!(seen[2].path, "/api/3/spot/order/cli-fmfwio-1");
    assert_eq!(seen[3].path, "/api/3/spot/order");
    assert_eq!(
        seen[3].query.get("symbol").map(String::as_str),
        Some("ETHBTC")
    );
    assert_eq!(seen[4].path, "/api/3/spot/history/trade");
    assert_eq!(seen[4].query.get("limit").map(String::as_str), Some("100"));
}

#[test]
fn fmfwio_request_specs_should_match_offline_private_builders() {
    let place =
        build_place_order_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("place request");
    load_request_spec("place_order.json")
        .assert_matches(&place.actual_http_request())
        .expect("place request spec");

    let cancel =
        build_cancel_order_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("cancel request");
    load_request_spec("cancel_order.json")
        .assert_matches(&cancel.actual_http_request())
        .expect("cancel request spec");

    let balances =
        build_get_balances_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("balances request");
    load_request_spec("get_balances.json")
        .assert_matches(&balances.actual_http_request())
        .expect("balances request spec");

    let fees =
        build_get_fees_spot_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("fees request");
    load_request_spec("get_fees_spot.json")
        .assert_matches(&fees.actual_http_request())
        .expect("fees request spec");

    let amend =
        build_amend_order_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("amend request");
    load_request_spec("amend_order_replace.json")
        .assert_matches(&amend.actual_http_request())
        .expect("amend request spec");

    let order_list =
        build_place_order_list_request_spec("test-key", "test-secret", 1_710_000_000_000, 10_000)
            .expect("order-list request");
    load_request_spec("place_order_list_otoco.json")
        .assert_matches(&order_list.actual_http_request())
        .expect("order-list request spec");

    let batch_cancel = build_batch_cancel_orders_request_spec(
        "test-key",
        "test-secret",
        1_710_000_000_000,
        10_000,
    )
    .expect("batch-cancel request");
    load_request_spec("batch_cancel_orders.json")
        .assert_matches(&batch_cancel.actual_http_request())
        .expect("batch-cancel request spec");
}

#[test]
fn fmfwio_signing_vectors_should_verify() {
    for fixture in [
        "place_order_hs256.json",
        "cancel_order_hs256.json",
        "get_fees_spot_hs256.json",
        "amend_order_replace_hs256.json",
        "place_order_list_otoco_hs256.json",
        "batch_cancel_orders_hs256.json",
        "ws_login_hs256.json",
    ] {
        load_signing_vector(fixture).verify().expect(fixture);
    }
}

#[test]
fn fmfwio_private_parser_should_parse_order_list_fixture() {
    let balances = parse_account_balances(
        &exchange_id(),
        context("balances").tenant_id.unwrap(),
        context("balances").account_id.unwrap(),
        &["BTC".to_string()],
        &load_parser_fixture("spot_balances.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances.len(), 1);
    assert_eq!(balances[0].balances[0].total, 0.012);

    let fees = parse_fee_snapshots(
        &exchange_id(),
        &[symbol_scope()],
        &load_parser_fixture("spot_fees.json"),
    )
    .expect("fees");
    assert_eq!(fees[0].maker_rate, "-0.0001");
    assert_eq!(fees[0].taker_rate, "0.001");

    let response = parse_spot_order_list_ack(
        &exchange_id(),
        symbol_scope(),
        &load_parser_fixture("spot_order_list_ack.json"),
    )
    .expect("order list ack");
    assert_eq!(response.order_list_id.as_deref(), Some("2100001"));
    assert_eq!(response.orders.len(), 3);
    assert_eq!(
        response.orders[0].client_order_id.as_deref(),
        Some("cli-fmfwio-otoco-primary")
    );
}

#[test]
fn fmfwio_private_parser_should_parse_batch_cancel_fixture() {
    let response = parse_spot_batch_cancel_ack(
        &exchange_id(),
        symbol_scope(),
        &load_parser_fixture("batch_cancel_orders_ack.json"),
    )
    .expect("batch cancel ack");
    assert_eq!(response.cancelled_count, 2);
    assert!(response
        .orders
        .iter()
        .all(|order| order.status == rustcta_types::OrderStatus::Cancelled));
}

fn place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope(),
        client_order_id: Some("cli-fmfwio-1".to_string()),
        side: OrderSide::Sell,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.063".to_string(),
        price: Some("0.046016".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

#[test]
fn fmfwio_private_parser_should_parse_order_ack_fixture() {
    let order = parse_spot_order_ack(
        &exchange_id(),
        symbol_scope(),
        &load_parser_fixture("spot_order_ack.json"),
    )
    .expect("order ack");
    assert_eq!(order.exchange_order_id.as_deref(), Some("840450210"));
    assert_eq!(order.client_order_id.as_deref(), Some("cli-fmfwio-1"));
    assert_eq!(order.quantity, "0.063");
    assert_eq!(order.filled_quantity, "0.000");
}
