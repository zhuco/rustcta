use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::signing::{prehash, sign_tapbit_request};
use super::test_support::{
    actual_request, assert_signed_json_request, context, exchange_id, perp_symbol_scope,
    private_config, request_spec, signing_vector, spawn_rest_server, spot_symbol_scope,
};
use super::TapbitGatewayAdapter;

#[tokio::test]
async fn tapbit_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = TapbitGatewayAdapter::default_public().expect("adapter");
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

#[test]
fn tapbit_signing_should_match_documented_prehash_shape() {
    let value = prehash(
        "2022-02-23T10:10:36.304Z",
        "GET",
        "/api/v1/spot/account/one",
        "asset=USDT",
        "",
    );
    assert_eq!(
        value,
        "2022-02-23T10:10:36.304ZGET/api/v1/spot/account/one?asset=USDT"
    );
    let signature = sign_tapbit_request(
        "secret",
        "2022-02-23T10:10:36.304Z",
        "GET",
        "/api/v1/spot/account/one",
        "asset=USDT",
        "",
    )
    .expect("signature");
    assert_eq!(
        signature,
        "3a6a1dbee37cd8adc388da6516c12602456a07cf8e657e61b44e9fd055795d8a"
    );
    signing_vector("spot_account_one")
        .verify()
        .expect("tapbit signing vector");
}

#[tokio::test]
async fn tapbit_adapter_should_route_signed_spot_order_lifecycle() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": {"order_id": "3001"}}),
        json!({
            "code": 200,
            "data": {
                "order_id": "3001",
                "trade_pair_name": "BTC/USDT",
                "direction": "buy",
                "quantity": "0.02",
                "filled_quantity": "0",
                "price": "65000",
                "status": "unsettled",
                "order_time": 1700000000000i64
            }
        }),
        json!({
            "code": 200,
            "data": [{
                "order_id": "3001",
                "trade_pair_name": "BTC/USDT",
                "direction": "buy",
                "quantity": "0.02",
                "filled_quantity": "0",
                "price": "65000",
                "status": "unsettled"
            }]
        }),
        json!({"code": 200, "data": {"order_id": "3001"}}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(private_config(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_place_order);
    assert!(adapter.capabilities().supports_cancel_order);
    assert!(!adapter.capabilities().supports_client_order_id);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("3001"));

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("3001".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.status, OrderStatus::Open);

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

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("3001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_signed_json_request(&requests[0], "POST", "/api/v1/spot/order");
    let body: serde_json::Value = serde_json::from_str(&requests[0].body).expect("json body");
    assert_eq!(
        body.get("instrument_id").and_then(|v| v.as_str()),
        Some("BTC/USDT")
    );
    assert_eq!(body.get("direction").and_then(|v| v.as_str()), Some("1"));

    assert_signed_json_request(&requests[1], "GET", "/api/v1/spot/order_info");
    assert_eq!(
        requests[1].query.get("order_id").map(String::as_str),
        Some("3001")
    );
    assert_signed_json_request(&requests[2], "GET", "/api/v1/spot/open_order_list");
    assert_eq!(
        requests[2].query.get("instrument_id").map(String::as_str),
        Some("BTC%2FUSDT")
    );
    assert_signed_json_request(&requests[3], "POST", "/api/v1/spot/cancel_order");
    for (request, spec) in requests.into_iter().zip([
        "place_order_spot",
        "query_order_spot",
        "get_open_orders_spot",
        "cancel_order_spot",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("tapbit spot lifecycle request spec");
    }
}

#[tokio::test]
async fn tapbit_adapter_should_parse_balances_fees_and_cancel_all() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 200,
            "data": [
                {"asset": "BTC", "available": "0.10", "frozen_balance": "0.02", "total_balance": "0.12"},
                {"asset": "USDT", "available": "123", "frozen_balance": "2", "total_balance": "125"}
            ]
        }),
        json!({
            "code": 200,
            "data": [{
                "trade_pair_name": "BTC/USDT",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "price_precision": "2",
                "amount_precision": "6",
                "maker_fee_rate": "0.001",
                "taker_fee_rate": "0.002"
            }]
        }),
        json!({
            "code": 200,
            "data": [{
                "order_id": "3001",
                "trade_pair_name": "BTC/USDT",
                "direction": "buy",
                "quantity": "0.02",
                "filled_quantity": "0",
                "price": "65000",
                "status": "unsettled"
            }]
        }),
        json!({"code": 200, "data": [{"order_id": "3001", "code": "200", "message": ""}]}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].available, 0.10);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.001");
    assert_eq!(fees.fees[0].taker_rate, "0.002");

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);
    assert_eq!(cancel_all.orders[0].status, OrderStatus::Cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_signed_json_request(&requests[0], "GET", "/api/v1/spot/account/list");
    assert_eq!(requests[1].path, "/api/spot/instruments/trade_pair_list");
    assert_signed_json_request(&requests[2], "GET", "/api/v1/spot/open_order_list");
    assert_signed_json_request(&requests[3], "POST", "/api/v1/spot/batch_cancel_order");
    let body: serde_json::Value = serde_json::from_str(&requests[3].body).expect("json body");
    assert_eq!(body["orderIds"][0], "3001");
    for (request, spec) in requests.into_iter().zip([
        "get_balances_spot",
        "get_fees_spot_public_rules",
        "cancel_all_open_orders_spot",
        "batch_cancel_orders_spot",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("tapbit balance/cancel-all request spec");
    }
}

#[tokio::test]
async fn tapbit_adapter_should_route_perpetual_private_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 200,
            "data": [{"asset": "USDT", "available": "100", "frozen": "5", "balance": "105"}]
        }),
        json!({
            "code": 200,
            "data": [{
                "contract_code": "BTC-SWAP",
                "side": "long",
                "quantity": "2",
                "entry_price": "65000",
                "mark_price": "65100",
                "unrealized_pnl": "12",
                "leverage": "5"
            }]
        }),
        json!({"code": 200, "data": {"order_id": "P3001"}}),
        json!({
            "code": 200,
            "data": [{
                "order_id": "P3001",
                "contract_code": "BTC-SWAP",
                "direction": "buy",
                "quantity": "2",
                "filled_quantity": "0",
                "price": "65000",
                "status": "unsettled"
            }]
        }),
        json!({
            "code": 200,
            "data": [{
                "fill_id": "F1",
                "order_id": "P3001",
                "contract_code": "BTC-SWAP",
                "direction": "buy",
                "quantity": "1",
                "price": "65000",
                "fee": "0.5",
                "fee_asset": "USDT",
                "trade_time": 1700000000000i64
            }]
        }),
        json!({"code": 200, "data": {"order_id": "P3001"}}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(private_config(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_positions);
    assert!(adapter.capabilities().supports_recent_fills);
    assert!(adapter.capabilities().supports_reduce_only);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(balances.balances[0].balances[0].total, 105.0);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].side, PositionSide::Long);
    assert_eq!(positions.positions[0].quantity, 2.0);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "2".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("P3001"));

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("P3001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("F1"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("P3001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_signed_json_request(&requests[0], "GET", "/api/v1/usdt/account");
    assert_signed_json_request(&requests[1], "GET", "/api/v1/usdt/position_list");
    assert_signed_json_request(&requests[2], "POST", "/api/v1/usdt/order");
    let body: serde_json::Value = serde_json::from_str(&requests[2].body).expect("json body");
    assert_eq!(
        body.get("instrument_id").and_then(|value| value.as_str()),
        Some("BTC-SWAP")
    );
    assert_eq!(
        body.get("position_side").and_then(|value| value.as_str()),
        Some("long")
    );
    assert_signed_json_request(&requests[3], "GET", "/api/v1/usdt/open_order_list");
    assert_signed_json_request(&requests[4], "GET", "/api/v1/usdt/fills");
    assert_eq!(
        requests[4].query.get("order_id").map(String::as_str),
        Some("P3001")
    );
    assert_signed_json_request(&requests[5], "POST", "/api/v1/usdt/cancel_order");
    for (request, spec) in requests.into_iter().zip([
        "get_balances_perp",
        "get_positions_perp",
        "place_order_perp",
        "get_open_orders_perp",
        "get_recent_fills_perp",
        "cancel_order_perp",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("tapbit perpetual request spec");
    }
}

#[tokio::test]
async fn tapbit_adapter_should_route_spot_native_batch_place_and_cancel() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": [
            {"order_id": "B1", "code": "200", "message": ""},
            {"order_id": "B2", "code": "200", "message": ""}
        ]}),
        json!({"code": 200, "data": [
            {"order_id": "B1", "code": "200", "message": ""},
            {"order_id": "B2", "code": "200", "message": ""}
        ]}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(private_config(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_batch_place_order);
    assert!(adapter.capabilities().supports_batch_cancel_order);

    let order = |request_id: &str, quantity: &str| PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol: spot_symbol_scope(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: quantity.to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order("place-1", "0.01"), order("place-2", "0.02")],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("B1"));
    assert_eq!(placed.orders[1].exchange_order_id.as_deref(), Some("B2"));

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
                    client_order_id: None,
                    exchange_order_id: Some("B1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("B2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_signed_json_request(&requests[0], "POST", "/api/v1/spot/batch_order");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[0].body).expect("batch place body"),
        json!([
            {"instrument_id": "BTC/USDT", "direction": "1", "price": "65000", "quantity": "0.01"},
            {"instrument_id": "BTC/USDT", "direction": "1", "price": "65000", "quantity": "0.02"}
        ])
    );
    assert_signed_json_request(&requests[1], "POST", "/api/v1/spot/batch_cancel_order");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[1].body).expect("batch cancel body"),
        json!({"orderIds": ["B1", "B2"]})
    );
    for (request, spec) in requests
        .into_iter()
        .zip(["batch_place_orders_spot", "batch_cancel_orders_spot"])
    {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("tapbit spot batch request spec");
    }
}

#[tokio::test]
async fn tapbit_adapter_should_route_perpetual_batch_place_and_cancel_fallback() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": {"order_id": "PB1"}}),
        json!({"code": 200, "data": {"order_id": "PB2"}}),
        json!({"code": 200, "data": {"order_id": "PB1"}}),
        json!({"code": 200, "data": {"order_id": "PB2"}}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let order = |request_id: &str, quantity: &str| PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol: perp_symbol_scope(),
        client_order_id: None,
        side: OrderSide::Sell,
        position_side: Some(PositionSide::Short),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: quantity.to_string(),
        price: Some("65100".to_string()),
        quote_quantity: None,
        reduce_only: true,
        post_only: false,
    };
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-batch-place"),
            exchange: exchange_id(),
            orders: vec![order("perp-place-1", "1"), order("perp-place-2", "2")],
        })
        .await
        .expect("perp batch place");
    assert_eq!(placed.orders.len(), 2);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("PB1"));
    assert_eq!(placed.orders[1].exchange_order_id.as_deref(), Some("PB2"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("perp-cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("PB1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("perp-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("PB2".to_string()),
                },
            ],
        })
        .await
        .expect("perp batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_signed_json_request(&requests[0], "POST", "/api/v1/usdt/order");
    assert_signed_json_request(&requests[1], "POST", "/api/v1/usdt/order");
    assert_signed_json_request(&requests[2], "POST", "/api/v1/usdt/cancel_order");
    assert_signed_json_request(&requests[3], "POST", "/api/v1/usdt/cancel_order");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[0].body).expect("perp place body"),
        json!({
            "instrument_id": "BTC-SWAP",
            "direction": "2",
            "price": "65100",
            "quantity": "1",
            "reduce_only": true,
            "position_side": "short"
        })
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[2].body).expect("perp cancel body"),
        json!({"order_id": "PB1"})
    );
}
