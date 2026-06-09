use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListOrderLeg, OrderListRequest, PageRequest,
    PlaceOrderRequest, QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::private::{
    build_cancel_order_request_spec, build_get_balances_request_spec,
    build_get_open_orders_request_spec, build_get_recent_fills_request_spec,
    build_place_order_request_spec, build_query_order_request_spec,
};
use super::private_parser::{parse_open_orders, parse_order_ack, parse_recent_fills};
use super::test_support::{
    context, exchange_id, private_config_with_base_url, spawn_rest_server, symbol_scope,
};
use super::HollaexGatewayAdapter;

fn load_request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/get_balances.json"
        ),
        "place_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/place_order.json"
        ),
        "cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/cancel_order.json"
        ),
        "query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/query_order.json"
        ),
        "get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/get_open_orders.json"
        ),
        "get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/request_specs/get_recent_fills.json"
        ),
        _ => panic!("unknown hollaex request spec {name}"),
    };
    serde_json::from_str(text).expect("request spec")
}

fn load_signing_vector(name: &str) -> SigningVector {
    let text = match name {
        "get_balances_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/get_balances_hmac_sha256.json"
        ),
        "place_order_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/place_order_hmac_sha256.json"
        ),
        "cancel_order_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/cancel_order_hmac_sha256.json"
        ),
        "ws_connect_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/signing_vectors/ws_connect_hmac_sha256.json"
        ),
        _ => panic!("unknown hollaex signing vector {name}"),
    };
    serde_json::from_str(text).expect("signing vector")
}

fn load_private_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "order_ack.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/hollaex/parser/order_ack.json")
        }
        "order_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/hollaex/order_success.json")
        }
        "open_orders_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/hollaex/open_orders_success.json")
        }
        "recent_fills_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/hollaex/recent_fills_success.json"
        ),
        _ => panic!("unknown hollaex private parser fixture {name}"),
    };
    serde_json::from_str(text).expect("private parser fixture")
}

fn load_unsupported_boundary() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/hollaex/unsupported_boundary.json"
    ))
    .expect("unsupported boundary")
}

#[tokio::test]
async fn hollaex_adapter_should_keep_balance_and_writes_unsupported() {
    let adapter = HollaexGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);

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
async fn hollaex_readbacks_should_fail_closed_without_private_guard() {
    let adapter = HollaexGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-fixture-1".to_string()),
        })
        .await
        .expect_err("query guard");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.query_order"
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-disabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect_err("open guard");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.get_open_orders"
        }
    ));
}

#[tokio::test]
async fn hollaex_advanced_order_surfaces_should_remain_explicitly_unsupported() {
    let adapter = HollaexGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
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

    let boundary = load_unsupported_boundary();
    let advanced = &boundary["advanced_order_boundaries"];
    assert_eq!(
        advanced["amend_order"]["runtime_error"],
        "hollaex.amend_order_unsupported"
    );
    assert_eq!(advanced["amend_order"]["status"], "unsupported");
    assert_eq!(
        advanced["place_order_list"]["runtime_error"],
        "hollaex.order_list_unsupported"
    );
    assert_eq!(
        advanced["place_order_list"]["order_list_kinds"],
        serde_json::json!(["oco", "oto"])
    );
    assert_eq!(
        advanced["batch_place_orders"]["runtime_error"],
        "hollaex.batch_place_orders_unsupported"
    );
    assert_eq!(
        advanced["batch_cancel_orders"]["runtime_error"],
        "hollaex.batch_cancel_orders_unsupported"
    );
    assert_eq!(
        advanced["cancel_all_orders"]["runtime_error"],
        "hollaex.cancel_all_orders_request_spec_only"
    );
    assert_eq!(
        advanced["cancel_all_orders"]["request_spec_fixture"],
        "tests/fixtures/exchanges/hollaex/request_specs/cancel_all_orders.json"
    );

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.2".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.amend_order_unsupported"
        }
    ));

    let oco_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol_scope(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("120".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("90".to_string()),
                stop_price: Some("95".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order list unsupported");
    assert!(matches!(
        oco_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.order_list_unsupported"
        }
    ));

    let oto_error = adapter
        .place_order_list(OrderListRequest::Oto {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oto"),
            symbol: symbol_scope(),
            list_client_order_id: Some("oto-1".to_string()),
            working: OrderListOrderLeg {
                side: OrderSide::Buy,
                order_type: OrderListLegType::Limit,
                quantity: "0.1".to_string(),
                price: Some("100".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oto-working".to_string()),
            },
            pending: OrderListOrderLeg {
                side: OrderSide::Sell,
                order_type: OrderListLegType::TakeProfitLimit,
                quantity: "0.1".to_string(),
                price: Some("120".to_string()),
                stop_price: Some("115".to_string()),
                time_in_force: None,
                client_order_id: Some("oto-pending".to_string()),
            },
        })
        .await
        .expect_err("oto order list unsupported");
    assert!(matches!(
        oto_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.order_list_unsupported"
        }
    ));

    let place = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-order"),
        symbol: symbol_scope(),
        client_order_id: Some("batch-place-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.1".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![place],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.batch_place_orders_unsupported"
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
                symbol: symbol_scope(),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            }],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "hollaex.batch_cancel_orders_unsupported"
        }
    ));
}

#[test]
fn hollaex_request_specs_should_match_offline_private_builders() {
    let balances = build_get_balances_request_spec("test-key", "test-secret", 1_710_000_000)
        .expect("balances request");
    load_request_spec("get_balances.json")
        .assert_matches(&balances.actual_http_request())
        .expect("balances request spec");

    let place = build_place_order_request_spec("test-key", "test-secret", 1_710_000_000)
        .expect("place request");
    load_request_spec("place_order.json")
        .assert_matches(&place.actual_http_request())
        .expect("place request spec");

    let cancel = build_cancel_order_request_spec("test-key", "test-secret", 1_710_000_000)
        .expect("cancel request");
    load_request_spec("cancel_order.json")
        .assert_matches(&cancel.actual_http_request())
        .expect("cancel request spec");

    let query =
        build_query_order_request_spec("test-key", "test-secret", 1_710_000_000, "order-fixture-1")
            .expect("query request");
    load_request_spec("query_order.json")
        .assert_matches(&query.actual_http_request())
        .expect("query request spec");

    let open = build_get_open_orders_request_spec(
        "test-key",
        "test-secret",
        1_710_000_000,
        "xht-usdt",
        None,
    )
    .expect("open orders request");
    load_request_spec("get_open_orders.json")
        .assert_matches(&open.actual_http_request())
        .expect("open orders request spec");

    let fills = build_get_recent_fills_request_spec(
        "test-key",
        "test-secret",
        1_710_000_000,
        "xht-usdt",
        None,
    )
    .expect("recent fills request");
    load_request_spec("get_recent_fills.json")
        .assert_matches(&fills.actual_http_request())
        .expect("recent fills request spec");
}

#[test]
fn hollaex_signing_vectors_should_verify() {
    for fixture in [
        "get_balances_hmac_sha256.json",
        "place_order_hmac_sha256.json",
        "cancel_order_hmac_sha256.json",
        "ws_connect_hmac_sha256.json",
    ] {
        load_signing_vector(fixture).verify().expect(fixture);
    }
}

#[test]
fn hollaex_private_parser_should_parse_order_ack_fixture() {
    let order = parse_order_ack(
        &exchange_id(),
        symbol_scope(),
        &load_private_fixture("order_ack.json"),
    )
    .expect("order ack");
    assert_eq!(order.exchange_order_id.as_deref(), Some("order-fixture-1"));
    assert_eq!(order.quantity, "0.1");
    assert_eq!(order.filled_quantity, "0");
}

#[test]
fn hollaex_private_parser_should_parse_readback_fixtures() {
    let open_orders = parse_open_orders(
        &exchange_id(),
        &symbol_scope(),
        &load_private_fixture("open_orders_success.json"),
    )
    .expect("open orders");
    assert_eq!(open_orders.len(), 1);
    assert_eq!(
        open_orders[0].exchange_order_id.as_deref(),
        Some("order-fixture-1")
    );

    let fills = parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.expect("tenant"),
        context("fills").account_id.expect("account"),
        &symbol_scope(),
        &load_private_fixture("recent_fills_success.json"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("trade-fixture-1"));
    assert_eq!(fills[0].order_id.as_deref(), Some("order-fixture-1"));
}

#[tokio::test]
async fn hollaex_should_query_order_with_signed_get() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_private_fixture("order_success.json")]).await;
    let adapter =
        HollaexGatewayAdapter::new(private_config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-fixture-1".to_string()),
        })
        .await
        .expect("query order");

    assert_eq!(
        response
            .order
            .as_ref()
            .and_then(|order| order.exchange_order_id.as_deref()),
        Some("order-fixture-1")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/order");
    assert_eq!(
        request.query.get("order_id").map(String::as_str),
        Some("order-fixture-1")
    );
    assert_eq!(
        request.headers.get("api-key").map(String::as_str),
        Some("test-key")
    );
    assert!(request.headers.contains_key("api-signature"));
    assert!(request.headers.contains_key("api-expires"));
    assert!(!request.body.contains("test-secret"));
}

#[tokio::test]
async fn hollaex_should_get_open_orders_with_signed_get() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_private_fixture("open_orders_success.json")]).await;
    let adapter =
        HollaexGatewayAdapter::new(private_config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: Some(PageRequest::first_page(25)),
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/orders");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("xht-usdt")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("25"));
    assert!(request.headers.contains_key("api-signature"));
    assert!(request.body.is_empty());
}

#[tokio::test]
async fn hollaex_should_get_recent_fills_with_signed_get() {
    let (base_url, seen) =
        spawn_rest_server(vec![load_private_fixture("recent_fills_success.json")]).await;
    let adapter =
        HollaexGatewayAdapter::new(private_config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills.len(), 1);
    assert_eq!(
        response.fills[0].fill_id.as_deref(),
        Some("trade-fixture-1")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/user/trades");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("xht-usdt")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
    assert!(request.headers.contains_key("api-signature"));
    assert!(request.body.is_empty());
}
