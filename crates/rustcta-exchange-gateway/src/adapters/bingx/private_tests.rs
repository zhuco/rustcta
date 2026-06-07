use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use crate::signing_spec::SigningVector;

use super::private_parser::{
    parse_balances, parse_fills, parse_order, parse_orders, parse_positions,
};
use super::test_support::{
    assert_request_matches_spec, context, exchange_id, load_request_specs, perp_symbol_scope,
    spawn_rest_server, spot_symbol_scope,
};
use super::{BingxGatewayAdapter, BingxGatewayConfig};

#[tokio::test]
async fn bingx_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BingxGatewayAdapter::default_public().expect("adapter");
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
async fn bingx_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": {"balances": [
                {"asset": "BTC", "free": "0.1", "locked": "0.02"},
                {"asset": "USDT", "free": "10", "locked": "0"}
            ]}
        }),
        json!({
            "code": 0,
            "data": [{
                "symbol": "BTC-USDT",
                "positionSide": "LONG",
                "positionAmt": "0.5",
                "avgPrice": "65000",
                "markPrice": "65100",
                "leverage": "5"
            }]
        }),
        json!({
            "code": 0,
            "data": {"makerCommissionRate": "0.0002", "takerCommissionRate": "0.0005"}
        }),
        json!({
            "code": 0,
            "data": {
                "symbol": "BTC-USDT",
                "orderId": 1001,
                "clientOrderId": "CLIENT1",
                "price": "65000",
                "origQty": "0.01",
                "executedQty": "0.004",
                "status": "PARTIALLY_FILLED",
                "type": "LIMIT",
                "side": "BUY",
                "time": 1700000000000i64,
                "updateTime": 1700000001000i64
            }
        }),
        json!({
            "code": 0,
            "data": {"orders": [{
                "symbol": "BTC-USDT",
                "orderId": 1002,
                "clientOrderId": "CLIENT2",
                "price": "70000",
                "origQty": "0.02",
                "executedQty": "0",
                "status": "NEW",
                "type": "LIMIT",
                "side": "SELL"
            }]}
        }),
        json!({
            "code": 0,
            "data": [{
                "symbol": "BTC-USDT",
                "id": 2001,
                "orderId": 1001,
                "clientOrderId": "CLIENT1",
                "price": "64950",
                "qty": "0.004",
                "commission": "0.2598",
                "commissionAsset": "USDT",
                "side": "BUY",
                "time": 1700000001000i64
            }]
        }),
    ])
    .await;
    let adapter = private_adapter(base_url);

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
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");

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
    assert_eq!(positions.positions[0].side, PositionSide::Long);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0002");

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("CLIENT1".to_string()),
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.status, OrderStatus::PartiallyFilled);

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
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("2001"));

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "bingx.get_balances.spot");
    assert_request_matches_spec(&requests[1], "bingx.get_positions.perpetual");
    assert_request_matches_spec(&requests[2], "bingx.get_fees.perpetual");
    assert_request_matches_spec(&requests[3], "bingx.query_order.spot");
    assert_request_matches_spec(&requests[4], "bingx.get_open_orders.spot");
    assert_request_matches_spec(&requests[5], "bingx.get_recent_fills.spot");
}

#[tokio::test]
async fn bingx_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({"code": 0, "data": {"orders": [
            {"symbol": "BTC-USDT", "orderId": 3001, "clientOrderId": "BATCH1", "status": "NEW", "side": "BUY", "type": "LIMIT", "origQty": "0.01", "price": "65000"},
            {"symbol": "BTC-USDT", "orderId": 3002, "clientOrderId": "BATCH2", "status": "NEW", "side": "SELL", "type": "LIMIT", "origQty": "0.02", "price": "70000"}
        ]}}),
        json!({"code": 0, "data": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({"code": 0, "data": {"success": [
            {"symbol": "BTC-USDT", "orderId": 3001, "clientOrderId": "BATCH1", "status": "CANCELLED", "side": "BUY", "type": "LIMIT", "origQty": "0.01", "price": "65000"},
            {"symbol": "BTC-USDT", "orderId": 3002, "clientOrderId": "BATCH2", "status": "CANCELLED", "side": "SELL", "type": "LIMIT", "origQty": "0.02", "price": "70000"}
        ]}}),
        json!({"code": 0, "data": {"success": [
            {"symbol": "BTC-USDT", "orderId": 2002, "clientOrderId": "OPEN1", "status": "CANCELED", "side": "SELL", "type": "LIMIT"}
        ]}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    side: OrderSide::Buy,
                    position_side: Some(PositionSide::Long),
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("65000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: Some(PositionSide::Short),
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("70000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders.len(), 2);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let batch_cancel = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancel.cancelled_count, 2);

    let cancel_all = adapter
        .cancel_all_orders(rustcta_exchange_api::CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "bingx.place_order.perpetual");
    assert_eq!(
        requests[0].query.get("positionSide").map(String::as_str),
        Some("LONG")
    );
    assert_request_matches_spec(&requests[1], "bingx.batch_place_orders.perpetual");
    assert!(requests[1].query.contains_key("batchOrders"));
    assert_request_matches_spec(&requests[2], "bingx.cancel_order.perpetual");
    assert_request_matches_spec(&requests[3], "bingx.batch_cancel_orders.perpetual");
    assert_eq!(
        requests[3].query.get("orderIdList").map(String::as_str),
        Some("[%223001%22,%223002%22]")
    );
    assert_request_matches_spec(&requests[4], "bingx.cancel_all_orders.perpetual");
}

#[tokio::test]
async fn bingx_adapter_should_advertise_and_route_advanced_private_features() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": 5001, "clientOrderId": "AMEND1", "quantity": "0.03"}}),
        json!({"code": 0, "data": {
            "orderListId": "oco-1",
            "orders": [
                {"symbol": "BTC-USDT", "orderId": 6001, "clientOrderId": "OCO-LIMIT", "status": "NEW", "side": "SELL", "type": "LIMIT", "origQty": "0.01", "price": "71000"},
                {"symbol": "BTC-USDT", "orderId": 6002, "clientOrderId": "OCO-STOP", "status": "NEW", "side": "SELL", "type": "STOP", "origQty": "0.01", "price": "64000"}
            ]
        }}),
        json!({"code": 0, "data": {"symbol": "BTC-USDT", "leverage": 7}}),
        json!({"code": 0, "data": {"dualSidePosition": true}}),
        json!({"code": 0, "data": {"dualSidePosition": true}}),
        json!({"code": 0, "data": {"assetMode": "multiAssetsMode"}}),
        json!({"code": 0, "data": {"assetMode": "multiAssetsMode"}}),
        json!({"code": 0, "data": {"symbol": "BTC-USDT", "marginType": "ISOLATED"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_amend_order);
    assert!(capabilities.supports_order_list);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("AMEND1".to_string()),
            exchange_order_id: Some("5001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.03".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("5001"));

    let order_list = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: spot_symbol_scope(),
            list_client_order_id: Some("OCO-LIST".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("71000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("OCO-LIMIT".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("64000".to_string()),
                stop_price: Some("64500".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("OCO-STOP".to_string()),
            },
        })
        .await
        .expect("oco");
    assert_eq!(order_list.order_list_id.as_deref(), Some("oco-1"));
    assert_eq!(order_list.orders.len(), 2);

    adapter
        .set_perpetual_leverage(perp_symbol_scope(), PositionSide::Long, 7)
        .await
        .expect("set leverage");
    adapter
        .set_position_mode(true)
        .await
        .expect("set position mode");
    adapter
        .query_position_mode()
        .await
        .expect("query position mode");
    adapter
        .switch_multi_assets_mode("multiAssetsMode")
        .await
        .expect("switch assets mode");
    adapter
        .query_multi_assets_mode()
        .await
        .expect("query assets mode");
    adapter
        .change_perpetual_margin_type(perp_symbol_scope(), "isolated")
        .await
        .expect("margin type");

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "bingx.amend_order.perpetual");
    assert_request_matches_spec(&requests[1], "bingx.place_order_list.spot_oco");
    assert_eq!(
        requests[1]
            .query
            .get("listClientOrderId")
            .map(String::as_str),
        Some("OCO-LIST")
    );
    assert_request_matches_spec(&requests[2], "bingx.set_perpetual_leverage");
    assert_request_matches_spec(&requests[3], "bingx.set_position_mode");
    assert_request_matches_spec(&requests[4], "bingx.query_position_mode");
    assert_request_matches_spec(&requests[5], "bingx.switch_multi_assets_mode");
    assert_request_matches_spec(&requests[6], "bingx.query_multi_assets_mode");
    assert_request_matches_spec(&requests[7], "bingx.change_perpetual_margin_type");
}

#[tokio::test]
async fn bingx_adapter_should_route_spot_batch_place_and_cancel() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orders": [
            {"symbol": "BTC-USDT", "orderId": 4001, "clientOrderId": "SPOT-BATCH1", "status": "NEW", "side": "BUY", "type": "LIMIT", "origQty": "0.01", "price": "65000"},
            {"symbol": "BTC-USDT", "orderId": 4002, "clientOrderId": "SPOT-BATCH2", "status": "NEW", "side": "SELL", "type": "LIMIT", "origQty": "0.02", "price": "70000"}
        ]}}),
        json!({"code": 0, "data": {"success": [
            {"symbol": "BTC-USDT", "orderId": 4001, "clientOrderId": "SPOT-BATCH1", "status": "CANCELED", "side": "BUY", "type": "LIMIT", "origQty": "0.01", "price": "65000"},
            {"symbol": "BTC-USDT", "orderId": 4002, "clientOrderId": "SPOT-BATCH2", "status": "CANCELED", "side": "SELL", "type": "LIMIT", "origQty": "0.02", "price": "70000"}
        ]}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-batch-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("SPOT-BATCH1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("65000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-batch-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("SPOT-BATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("70000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("spot batch place");
    assert_eq!(placed.orders.len(), 2);

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("SPOT-BATCH1".to_string()),
                    exchange_order_id: Some("4001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("SPOT-BATCH2".to_string()),
                    exchange_order_id: Some("4002".to_string()),
                },
            ],
        })
        .await
        .expect("spot batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "bingx.batch_place_orders.spot");
    assert!(requests[0].query.contains_key("data"));
    assert!(requests[0]
        .query
        .get("data")
        .is_some_and(|value| value.contains("newClientOrderId")));
    assert_request_matches_spec(&requests[1], "bingx.batch_cancel_orders.spot");
    assert_eq!(
        requests[1].query.get("orderIds").map(String::as_str),
        Some("4001,4002")
    );
    assert_eq!(
        requests[1].query.get("process").map(String::as_str),
        Some("1")
    );
}

#[test]
fn bingx_signing_should_match_fixture_vectors() {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/signing_vectors.json"
    ))
    .expect("signing vectors");
    let vectors = value["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 2);
    for vector in vectors {
        let signature = super::signing::sign_raw_query(
            vector["secret"].as_str().expect("secret"),
            vector["query"].as_str().expect("query"),
        )
        .expect("signature");
        assert_eq!(
            signature,
            vector["signature"].as_str().expect("expected signature"),
            "vector {}",
            vector["name"].as_str().unwrap_or("unnamed")
        );
    }

    for text in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/bingx/signing_vectors/perpetual_limit_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bingx/signing_vectors/recv_window_sorted_query.json"
        ),
    ] {
        let vector: SigningVector = serde_json::from_str(text).expect("standard signing vector");
        vector.verify().expect("standard signing vector verifies");
    }
}

#[test]
fn bingx_private_parser_should_cover_fixture_success_empty_error_and_missing_field() {
    let balances_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/balance.json"
    ))
    .expect("balance fixture");
    let balances = parse_balances(
        &exchange_id(),
        rustcta_exchange_api::TenantId::new("tenant").expect("tenant"),
        rustcta_exchange_api::AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &balances_value,
    )
    .expect("balances");
    assert_eq!(balances[0].balances.len(), 2);

    let position_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/position.json"
    ))
    .expect("position fixture");
    let positions = parse_positions(
        &exchange_id(),
        rustcta_exchange_api::TenantId::new("tenant").expect("tenant"),
        rustcta_exchange_api::AccountId::new("account").expect("account"),
        &position_value,
    )
    .expect("positions");
    assert_eq!(positions[0].side, PositionSide::Long);

    let fill_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/fill.json"
    ))
    .expect("fill fixture");
    let fills = parse_fills(
        &exchange_id(),
        rustcta_exchange_api::TenantId::new("tenant").expect("tenant"),
        rustcta_exchange_api::AccountId::new("account").expect("account"),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fill_value,
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("2001"));

    let empty_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/empty_open_orders.json"
    ))
    .expect("empty open orders fixture");
    let empty_orders = parse_orders(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &empty_value,
    )
    .expect("empty open orders");
    assert!(empty_orders.is_empty());

    let error_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/error.json"
    ))
    .expect("error fixture");
    let error = parse_orders(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &error_value,
    )
    .expect_err("error response is not an order list");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));

    let missing_field_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bingx/missing_order_side.json"
    ))
    .expect("missing field fixture");
    let missing = parse_order(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &missing_field_value,
    )
    .expect_err("missing side should fail");
    assert!(matches!(missing, ExchangeApiError::Exchange(_)));
}

#[test]
fn bingx_request_specs_should_cover_declared_private_rest_operations() {
    let specs = load_request_specs();
    let operations = specs
        .private_rest
        .iter()
        .map(|spec| spec.operation.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        "bingx.get_balances.spot",
        "bingx.get_balances.perpetual",
        "bingx.get_positions.perpetual",
        "bingx.get_fees.spot",
        "bingx.get_fees.perpetual",
        "bingx.place_order.perpetual",
        "bingx.place_order.spot",
        "bingx.place_quote_market_order.spot",
        "bingx.cancel_order.perpetual",
        "bingx.cancel_order.spot",
        "bingx.amend_order.perpetual",
        "bingx.place_order_list.spot_oco",
        "bingx.batch_place_orders.spot",
        "bingx.batch_place_orders.perpetual",
        "bingx.batch_cancel_orders.spot",
        "bingx.batch_cancel_orders.perpetual",
        "bingx.cancel_all_orders.perpetual",
        "bingx.query_order.spot",
        "bingx.get_open_orders.spot",
        "bingx.get_recent_fills.spot",
        "bingx.generate_listen_key",
        "bingx.set_perpetual_leverage",
        "bingx.set_position_mode",
        "bingx.query_position_mode",
        "bingx.switch_multi_assets_mode",
        "bingx.query_multi_assets_mode",
        "bingx.change_perpetual_margin_type",
    ] {
        assert!(
            operations.contains(required),
            "missing request spec {required}"
        );
    }
}

fn private_adapter(base_url: String) -> BingxGatewayAdapter {
    BingxGatewayAdapter::new(BingxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..BingxGatewayConfig::default()
    })
    .expect("adapter")
}
