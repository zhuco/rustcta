use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PositionsRequest, QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::private_parser::{parse_order, parse_orders};
use super::test_support::{
    actual_request, assert_signed_xt_request, context, exchange_id, perp_symbol_scope,
    request_spec, signing_vector, spawn_rest_server, spot_symbol_scope,
};
use super::{XtGatewayAdapter, XtGatewayConfig};

#[tokio::test]
async fn xt_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = XtGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.supports_fees);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_reduce_only);

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
async fn xt_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balance"),
        fixture("positions"),
        fixture("fees"),
        fixture("order"),
        fixture("open_orders"),
        fixture("fills"),
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
    assert_signed_xt_request(&requests[0], "GET", "/v4/balances");
    assert_signed_xt_request(&requests[1], "GET", "/future/user/v1/position/list");
    assert_eq!(requests[2].path, "/future/market/v3/public/symbol/list");
    assert_signed_xt_request(&requests[3], "GET", "/v4/order/1001");
    assert_signed_xt_request(&requests[4], "GET", "/v4/open-order");
    assert_signed_xt_request(&requests[5], "GET", "/v4/trade");
    for (request, spec) in requests.into_iter().zip([
        "get_balances_spot",
        "get_positions_futures",
        "get_fees_futures_public_rules",
        "query_order_spot",
        "get_open_orders_spot",
        "get_recent_fills_spot",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("XT private read request spec");
    }
}

#[test]
fn xt_private_parser_fixtures_should_cover_empty_and_missing_required_fields() {
    let empty = parse_orders(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("empty_orders"),
    )
    .expect("empty order list");
    assert!(empty.is_empty());

    let missing = parse_order(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("missing_order_side"),
    )
    .expect_err("missing side should fail parser");
    assert!(format!("{missing}").contains("order missing side"));
}

#[tokio::test]
async fn xt_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"returnCode": 0, "result": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({
            "returnCode": 0,
            "result": {
                "symbol": "btc_usdt",
                "orderId": 2001,
                "clientOrderId": "LIMIT1",
                "price": "65000",
                "origQty": "2",
                "executedQty": "0",
                "state": "NEW",
                "orderType": "LIMIT",
                "orderSide": "BUY",
                "positionSide": "LONG"
            }
        }),
        json!({"returnCode": 0, "result": {}}),
        json!({"returnCode": 0, "result": "2001"}),
        json!({"returnCode": 0, "result": ""}),
        json!({"returnCode": 0, "result": true}),
        json!({"returnCode": 0, "result": true}),
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

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "1".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.quantity, "1");

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

    let batch_place = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-place-child"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                side: OrderSide::Buy,
                position_side: Some(PositionSide::Long),
                order_type: OrderType::Market,
                time_in_force: None,
                quantity: "1".to_string(),
                price: None,
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("batch place");
    assert_eq!(batch_place.orders.len(), 1);

    let batch_cancel = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-cancel-child"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                exchange_order_id: Some("2002".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancel.cancelled_count, 1);

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
    assert_eq!(cancel_all.cancelled_count, 0);

    let requests = seen.lock().unwrap().clone();
    assert_signed_xt_request(&requests[0], "POST", "/future/trade/v1/order/create");
    assert!(requests[0].body.contains("positionSide=LONG"));
    assert_signed_xt_request(&requests[1], "GET", "/future/trade/v1/order/detail");
    assert_signed_xt_request(&requests[2], "POST", "/future/trade/v1/order/update");
    assert!(requests[2].body.contains("origQty=1"));
    assert_signed_xt_request(&requests[3], "POST", "/future/trade/v1/order/cancel");
    assert_signed_xt_request(
        &requests[4],
        "POST",
        "/future/trade/v2/order/atomic-create-batch",
    );
    assert!(requests[4].body.contains("\"clientOrderId\":\"BATCH1\""));
    assert_signed_xt_request(&requests[5], "POST", "/future/trade/v1/order/cancel-batch");
    assert_signed_xt_request(&requests[6], "POST", "/future/trade/v1/order/cancel-all");
    for (request, spec) in requests.into_iter().zip([
        "place_order_futures",
        "query_order_futures",
        "amend_order_futures",
        "cancel_order_futures",
        "batch_place_orders_futures",
        "batch_cancel_orders_futures",
        "cancel_all_orders_futures",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("XT futures write request spec");
    }
}

#[tokio::test]
async fn xt_adapter_should_route_spot_advanced_order_mutations_and_boundaries() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "rc": 0,
            "result": {
                "symbol": "btc_usdt",
                "orderId": 1001,
                "clientOrderId": "SPOT1",
                "price": "65000",
                "origQty": "0.01",
                "executedQty": "0",
                "state": "NEW",
                "type": "LIMIT",
                "side": "BUY"
            }
        }),
        json!({"rc": 0, "result": {"orderId": 1001, "modifyId": "m1"}}),
        json!({
            "rc": 0,
            "result": {
                "batchId": "b1",
                "items": [{"index": "0", "clientOrderId": "SB1", "orderId": 1002, "reject": "false"}]
            }
        }),
        json!({"rc": 0, "result": {}}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_streams);
    assert!(capabilities
        .private_stream_capabilities
        .as_ref()
        .is_some_and(|capabilities| capabilities.supports_orders
            && capabilities.supports_fills
            && capabilities.supports_balances
            && capabilities.supports_positions));
    assert!(capabilities.supports_amend_order);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_order_list);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-amend"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("SPOT1".to_string()),
            exchange_order_id: Some("1001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.005".to_string(),
        })
        .await
        .expect("spot amend");
    assert_eq!(amended.order.quantity, "0.005");

    let batch_place = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("spot-batch-place-child"),
                symbol: spot_symbol_scope(),
                client_order_id: Some("SB1".to_string()),
                side: OrderSide::Buy,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "0.01".to_string(),
                price: Some("64000".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("spot batch place");
    assert_eq!(
        batch_place.orders[0].exchange_order_id.as_deref(),
        Some("1002")
    );

    let batch_cancel = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("spot-batch-cancel-child"),
                symbol: spot_symbol_scope(),
                client_order_id: None,
                exchange_order_id: Some("1002".to_string()),
            }],
        })
        .await
        .expect("spot batch cancel");
    assert_eq!(batch_cancel.cancelled_count, 1);

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-order-list"),
            symbol: spot_symbol_scope(),
            list_client_order_id: Some("LIST1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("70000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("ABOVE1".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("59000".to_string()),
                stop_price: Some("60000".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("BELOW1".to_string()),
            },
        })
        .await
        .expect_err("XT has no Spot OCO/OTO order-list mapping");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported { .. }
    ));

    let quote_market_error = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-quote-market"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect_err("XT perpetual quote-sized market order is unsupported");
    assert!(matches!(
        quote_market_error,
        ExchangeApiError::Unsupported { .. }
    ));

    let missing_id_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-cancel-missing-id"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("spot-batch-cancel-missing-id-child"),
                symbol: spot_symbol_scope(),
                client_order_id: None,
                exchange_order_id: None,
            }],
        })
        .await
        .expect_err("batch cancel requires every order id");
    assert!(matches!(
        missing_id_error,
        ExchangeApiError::InvalidRequest { .. }
    ));

    let requests = seen.lock().unwrap().clone();
    assert_signed_xt_request(&requests[0], "GET", "/v4/order/1001");
    assert_signed_xt_request(&requests[1], "PUT", "/v4/order/1001");
    assert!(requests[1].body.contains("\"quantity\":\"0.005\""));
    assert_signed_xt_request(&requests[2], "POST", "/v4/batch-order");
    assert!(requests[2].body.contains("\"items\""));
    assert!(requests[2].body.contains("\"clientOrderId\":\"SB1\""));
    assert_signed_xt_request(&requests[3], "DELETE", "/v4/batch-order");
    assert!(requests[3].body.contains("\"orderIds\""));
    for (request, spec) in requests.into_iter().zip([
        "query_order_spot_for_amend",
        "amend_order_spot",
        "batch_place_orders_spot",
        "batch_cancel_orders_spot",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("XT spot advanced write request spec");
    }
}

#[test]
fn xt_signing_should_match_known_hmac() {
    let signature = super::signing::sign_payload("secret", "payload").expect("signature");
    assert_eq!(
        signature,
        "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4"
    );
    signing_vector("raw_payload")
        .verify()
        .expect("XT signing vector");
}

fn private_adapter(base_url: String) -> XtGatewayAdapter {
    XtGatewayAdapter::new(XtGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..XtGatewayConfig::default()
    })
    .expect("adapter")
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "balance" => include_str!("../../../../../tests/fixtures/exchanges/xt/balance.json"),
        "positions" => include_str!("../../../../../tests/fixtures/exchanges/xt/positions.json"),
        "fees" => include_str!("../../../../../tests/fixtures/exchanges/xt/fees.json"),
        "order" => include_str!("../../../../../tests/fixtures/exchanges/xt/order.json"),
        "open_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/xt/open_orders.json")
        }
        "fills" => include_str!("../../../../../tests/fixtures/exchanges/xt/fills.json"),
        "empty_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/xt/empty_orders.json")
        }
        "missing_order_side" => {
            include_str!("../../../../../tests/fixtures/exchanges/xt/missing_order_side.json")
        }
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
