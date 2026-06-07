use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use crate::request_spec::{ActualHttpRequest, RequestSpec};
use crate::signing_spec::SigningVector;

use super::test_support::{
    assert_signed_request, context, exchange_id, fixture_json, perp_symbol_scope, private_config,
    spawn_rest_server, spot_symbol_scope,
};
use super::AscendexGatewayAdapter;

#[tokio::test]
async fn ascendex_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = AscendexGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn ascendex_adapter_should_route_signed_private_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture_json("balances.json"),
        fixture_json("positions.json"),
        fixture_json("order_ack.json"),
        json!({
            "code": 0,
            "data": {
                "info": {
                    "id": "CID1",
                    "orderId": "OID1",
                    "symbol": "BTC/USDT",
                    "orderType": "Limit",
                    "side": "Buy",
                    "status": "Canceled",
                    "orderQty": "0.01",
                    "price": "65000"
                }
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "id": "CID1",
                "orderId": "OID1",
                "symbol": "BTC/USDT",
                "orderType": "Limit",
                "side": "Buy",
                "status": "Filled",
                "orderQty": "0.01",
                "cumFilledQty": "0.01",
                "avgPx": "65000",
                "lastExecTime": 1700000000000i64
            }]
        }),
        json!({
            "code": 0,
            "data": [{
                "id": "CID2",
                "orderId": "OID2",
                "symbol": "BTC/USDT",
                "orderType": "Limit",
                "side": "Sell",
                "status": "New",
                "orderQty": "0.02",
                "price": "66000"
            }]
        }),
    ])
    .await;
    let adapter = AscendexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances.len(), 2);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: Vec::new(),
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("CID1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("OID1"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("CID1".to_string()),
            exchange_order_id: Some("OID1".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("OID1".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(queried.order.expect("order").status, OrderStatus::Filled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/42/api/pro/v1/cash/balance");
    assert_signed_request(&requests[1], "GET", "/42/api/pro/v2/futures/position");
    assert_signed_request(&requests[2], "POST", "/42/api/pro/v1/cash/order");
    request_spec("spot_place_order")
        .assert_matches(&actual_request(&requests[2]))
        .expect("spot place request spec");
    assert_eq!(
        requests[2]
            .body
            .get("symbol")
            .and_then(|value| value.as_str()),
        Some("BTC/USDT")
    );
    assert_eq!(
        requests[2].body.get("id").and_then(|value| value.as_str()),
        Some("CID1")
    );
    assert_signed_request(&requests[3], "DELETE", "/42/api/pro/v1/cash/order");
    assert_signed_request(&requests[4], "GET", "/42/api/pro/v1/cash/order/status");
    assert_eq!(
        requests[4].query.get("orderId").map(String::as_str),
        Some("OID1")
    );
    assert_signed_request(&requests[5], "GET", "/42/api/pro/v1/cash/order/open");
}

#[tokio::test]
async fn ascendex_adapter_should_route_batch_place_and_cancel_orders() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": {
                "info": [
                    {
                        "id": "CID1",
                        "orderId": "OID1",
                        "symbol": "BTC/USDT",
                        "orderType": "Limit",
                        "side": "Buy",
                        "status": "New",
                        "orderQty": "0.01",
                        "price": "65000"
                    },
                    {
                        "id": "CID2",
                        "orderId": "OID2",
                        "symbol": "BTC/USDT",
                        "orderType": "Limit",
                        "side": "Sell",
                        "status": "New",
                        "orderQty": "0.02",
                        "price": "66000"
                    }
                ]
            }
        }),
        json!({
            "code": 0,
            "data": {
                "info": [
                    {"id": "CID1", "orderId": "OID1", "symbol": "BTC/USDT", "status": "Canceled"},
                    {"id": "CID2", "orderId": "OID2", "symbol": "BTC/USDT", "status": "Canceled"}
                ]
            }
        }),
    ])
    .await;
    let adapter = AscendexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let order1 = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("CID1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let order2 = PlaceOrderRequest {
        context: context("batch-place-2"),
        client_order_id: Some("CID2".to_string()),
        side: OrderSide::Sell,
        quantity: "0.02".to_string(),
        price: Some("66000".to_string()),
        ..order1.clone()
    };
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order1, order2],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);
    assert_eq!(placed.orders[1].exchange_order_id.as_deref(), Some("OID2"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("CID1".to_string()),
                    exchange_order_id: Some("OID1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("CID2".to_string()),
                    exchange_order_id: Some("OID2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "POST", "/42/api/pro/v1/cash/order/batch");
    assert_eq!(
        requests[0].body["orders"].as_array().expect("orders").len(),
        2
    );
    assert_signed_request(&requests[1], "DELETE", "/42/api/pro/v1/cash/order/batch");
    assert_eq!(
        requests[1].body["orders"].as_array().expect("orders").len(),
        2
    );
}

#[tokio::test]
async fn ascendex_adapter_should_route_fee_cancel_all_and_futures_order_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": {
                "productFee": [{
                    "symbol": "BTC/USDT",
                    "fee": {"maker": "0.001", "taker": "0.002"}
                }]
            }
        }),
        json!({
            "code": 0,
            "data": {
                "collaterals": [
                    {"asset": "USDT", "totalBalance": "1000", "availableBalance": "900"}
                ]
            }
        }),
        json!({
            "code": 0,
            "data": {
                "info": {
                    "id": "PCID1",
                    "orderId": "POID1",
                    "symbol": "BTC-PERP",
                    "orderType": "Limit",
                    "side": "Buy",
                    "status": "New",
                    "orderQty": "0.25",
                    "price": "65000"
                }
            }
        }),
        json!({
            "code": 0,
            "data": {
                "info": [
                    {"id": "PCID1", "orderId": "POID1", "symbol": "BTC-PERP", "status": "New"},
                    {"id": "PCID2", "orderId": "POID2", "symbol": "BTC-PERP", "status": "New"}
                ]
            }
        }),
        json!({
            "code": 0,
            "data": {
                "info": [
                    {"id": "PCID1", "orderId": "POID1", "symbol": "BTC-PERP", "status": "Canceled"},
                    {"id": "PCID2", "orderId": "POID2", "symbol": "BTC-PERP", "status": "Canceled"}
                ]
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "id": "CID1",
                "orderId": "OID1",
                "symbol": "BTC/USDT",
                "status": "Canceled"
            }]
        }),
    ])
    .await;
    let adapter = AscendexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.001");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("perp balances");
    assert_eq!(balances.balances[0].balances[0].available, 900.0);

    let perp_order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("perp-place"),
        symbol: perp_symbol_scope(),
        client_order_id: Some("PCID1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.25".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: true,
        post_only: false,
    };
    let placed = adapter
        .place_order(perp_order.clone())
        .await
        .expect("perp place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("POID1"));

    let placed_batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-batch-place"),
            exchange: exchange_id(),
            orders: vec![
                perp_order.clone(),
                PlaceOrderRequest {
                    context: context("perp-place-2"),
                    client_order_id: Some("PCID2".to_string()),
                    side: OrderSide::Sell,
                    reduce_only: false,
                    ..perp_order
                },
            ],
        })
        .await
        .expect("perp batch place");
    assert_eq!(placed_batch.orders.len(), 2);

    let cancelled_batch = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("perp-cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("PCID1".to_string()),
                    exchange_order_id: Some("POID1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("perp-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("PCID2".to_string()),
                    exchange_order_id: Some("POID2".to_string()),
                },
            ],
        })
        .await
        .expect("perp batch cancel");
    assert_eq!(cancelled_batch.cancelled_count, 2);

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
        })
        .await
        .expect("spot cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/42/api/pro/v1/spot/fee");
    assert_signed_request(&requests[1], "GET", "/42/api/pro/v2/futures/position");
    assert_signed_request(&requests[2], "POST", "/42/api/pro/v2/futures/order");
    assert_eq!(
        requests[2]
            .body
            .get("symbol")
            .and_then(|value| value.as_str()),
        Some("BTC-PERP")
    );
    assert_eq!(
        requests[2]
            .body
            .get("execInst")
            .and_then(|value| value.as_str()),
        Some("ReduceOnly")
    );
    assert_signed_request(&requests[3], "POST", "/42/api/pro/v2/futures/order/batch");
    assert_signed_request(&requests[4], "DELETE", "/42/api/pro/v2/futures/order/batch");
    assert_signed_request(&requests[5], "DELETE", "/42/api/pro/v1/cash/order/all");
}

#[tokio::test]
async fn ascendex_adapter_should_parse_recent_fills_from_order_history() {
    let (base_url, seen) = spawn_rest_server(vec![fixture_json("order_history.json")]).await;
    let adapter = AscendexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");

    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("OID1"));
    assert_eq!(fills.fills[0].quantity, 0.25);
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_request(&request, "GET", "/42/api/pro/v2/futures/order/hist/current");
    assert_eq!(
        request.query.get("executedOnly").map(String::as_str),
        Some("true")
    );
}

#[test]
fn ascendex_order_body_should_reject_unsupported_or_incomplete_orders() {
    let mut request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("order-body"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("CID1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::StopMarket,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: None,
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = super::private::ascendex_place_order_body(&request)
        .expect_err("stop order should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));

    request.order_type = OrderType::Limit;
    let error = super::private::ascendex_place_order_body(&request)
        .expect_err("limit order without price should be invalid");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));

    request.price = Some("65000".to_string());
    request.reduce_only = true;
    let error = super::private::ascendex_place_order_body(&request)
        .expect_err("spot reduce-only should be invalid");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));
}

#[test]
fn ascendex_capabilities_should_not_advertise_stop_orders() {
    let adapter = AscendexGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities
        .supports_order_types
        .contains(&OrderType::StopMarket));
    assert!(!capabilities
        .supports_order_types
        .contains(&OrderType::StopLimit));
}

#[test]
fn ascendex_endpoint_mapping_should_declare_task15_delivery_surface() {
    let text = include_str!("endpoint_mapping.yaml");
    for required in [
        "exchange: ascendex",
        "account_group_required: true",
        "algorithm: hmac_sha256_base64",
        "capabilities_v2:",
        "rate_limit_plan:",
        "pagination:",
        "reconciliation:",
        "batch:",
        "websocket_policy:",
        "unsupported_gaps:",
        "amend order",
        "quote market order placement",
        "private balance stream events are not normalized",
        "private position stream events are not normalized",
    ] {
        assert!(text.contains(required), "missing mapping entry: {required}");
    }
    for required_path in [
        "/{account_group}/api/pro/v1/cash/order",
        "/{account_group}/api/pro/v2/futures/order",
        "/{account_group}/api/pro/v1/cash/order/batch",
        "/{account_group}/api/pro/v2/futures/order/batch",
        "wss://ascendex.com/{account_group}/api/pro/v2/stream",
    ] {
        assert!(
            text.contains(required_path),
            "missing endpoint path: {required_path}"
        );
    }
}

#[test]
fn ascendex_request_spec_fixture_should_cover_private_and_ws_paths() {
    let specs = fixture_json("request_specs.json");
    let specs = specs.as_array().expect("request spec array");
    assert!(specs.len() >= 9);
    for operation in [
        "spot_balance",
        "spot_place_order",
        "spot_query_order",
        "spot_batch_place",
        "futures_position",
        "futures_place_order",
        "futures_batch_cancel",
        "public_ws_depth",
        "private_ws_futures_orders",
    ] {
        assert!(
            specs
                .iter()
                .any(|spec| spec["operation"].as_str() == Some(operation)),
            "missing request spec operation: {operation}"
        );
    }
    let futures_order = specs
        .iter()
        .find(|spec| spec["operation"].as_str() == Some("futures_place_order"))
        .expect("futures order spec");
    assert_eq!(futures_order["method"], "POST");
    assert_eq!(futures_order["path"], "/42/api/pro/v2/futures/order");
    assert_eq!(futures_order["sign_path"], "v2/futures/order");
    let private_ws = specs
        .iter()
        .find(|spec| spec["operation"].as_str() == Some("private_ws_futures_orders"))
        .expect("private ws spec");
    assert_eq!(private_ws["auth_sign_path"], "v2/stream");
    assert_eq!(private_ws["channel"], "futures-order");

    for (name, operation) in [
        ("spot_place_order", "spot_place_order"),
        ("futures_place_order", "futures_place_order"),
    ] {
        let spec = request_spec(name);
        assert_eq!(spec.exchange, "ascendex");
        assert_eq!(spec.operation, operation);
    }

    for text in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/ascendex/signing_vectors/spot_order_rest.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ascendex/signing_vectors/futures_order_rest.json"
        ),
    ] {
        let vector: SigningVector = serde_json::from_str(text).expect("standard signing vector");
        vector.verify().expect("standard signing vector verifies");
    }
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "spot_place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/ascendex/request_specs/spot_place_order.json"
        ),
        "futures_place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/ascendex/request_specs/futures_place_order.json"
        ),
        other => panic!("unknown AscendEX request spec {other}"),
    };
    serde_json::from_str(text).expect("request spec")
}

fn actual_request(request: &super::test_support::SeenRequest) -> ActualHttpRequest {
    ActualHttpRequest::new(request.method.clone(), request.path.clone())
        .with_query(
            request
                .query
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .with_headers(
            request
                .headers
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .with_body(Some(request.body.clone()).filter(|value| !value.is_null()))
}
