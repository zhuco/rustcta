use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::private_parser::{parse_order, parse_orders};
use super::test_support::{
    actual_request, assert_signed_deepcoin_request, context, exchange_id, fixture,
    perp_symbol_scope, request_spec, signing_vector, spawn_rest_server, spot_symbol_scope,
};
use super::{DeepcoinGatewayAdapter, DeepcoinGatewayConfig};

#[tokio::test]
async fn deepcoin_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = DeepcoinGatewayAdapter::default_public().expect("adapter");
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
async fn deepcoin_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balances_spot"),
        fixture("positions_perp"),
        fixture("fee_perp"),
        fixture("order_partially_filled"),
        fixture("open_orders"),
        fixture("fills_spot"),
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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("20011"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_deepcoin_request(&requests[0], "GET", "/deepcoin/account/all-balances");
    assert_signed_deepcoin_request(&requests[1], "GET", "/deepcoin/account/positions");
    assert_signed_deepcoin_request(&requests[2], "GET", "/deepcoin/account/trade-fee");
    assert_signed_deepcoin_request(&requests[3], "GET", "/deepcoin/trade/order");
    assert_signed_deepcoin_request(&requests[4], "GET", "/deepcoin/trade/v2/orders-pending");
    assert_signed_deepcoin_request(&requests[5], "GET", "/deepcoin/trade/fills");
    for (request, spec) in requests.into_iter().zip([
        "get_balances_spot",
        "get_positions_perp",
        "get_fees_perp",
        "query_order_spot",
        "get_open_orders_spot",
        "get_recent_fills_spot",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("deepcoin private read request spec");
    }
}

#[tokio::test]
async fn deepcoin_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": "0", "data": {"ordId": "2001", "clOrdId": "LIMIT1", "sCode": "0", "sMsg": ""}}),
        json!({"code": "0", "data": {"ordId": "2001", "clOrdId": "LIMIT1", "sCode": "0", "sMsg": ""}}),
        json!({"code": "0", "data": {"errorCode": 0, "errorMsg": "", "ordId": "2001"}}),
        json!({"code": "0", "data": [
            {"ordId": "3001", "clOrdId": "BATCH1", "sCode": "0", "sMsg": ""},
            {"ordId": "3002", "clOrdId": "BATCH2", "sCode": "0", "sMsg": ""}
        ]}),
        json!({"code": "0", "data": {"errorList": [
            {"orderSysId": "3002", "errorCode": 24, "errorMsg": "OrderNotFound:3002"}
        ]}}),
        json!({"code": "0", "data": {"errorList": []}}),
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
            quantity: "2".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

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

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "3".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(amended.order.quantity, "3");

    let batch_placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    side: OrderSide::Buy,
                    position_side: Some(PositionSide::Long),
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "1".to_string(),
                    price: Some("64000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: Some(PositionSide::Short),
                    order_type: OrderType::Market,
                    time_in_force: None,
                    quantity: "2".to_string(),
                    price: None,
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch_placed.orders.len(), 2);
    assert_eq!(
        batch_placed.orders[0].exchange_order_id.as_deref(),
        Some("3001")
    );

    let batch_cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancelled.cancelled_count, 1);

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
    assert_signed_deepcoin_request(&requests[0], "POST", "/deepcoin/trade/order");
    let body: Value = serde_json::from_str(&requests[0].body).expect("json body");
    assert_eq!(body.get("posSide").and_then(Value::as_str), Some("long"));
    assert_eq!(
        body.get("mrgPosition").and_then(Value::as_str),
        Some("merge")
    );
    assert_signed_deepcoin_request(&requests[1], "POST", "/deepcoin/trade/cancel-order");
    assert_signed_deepcoin_request(&requests[2], "POST", "/deepcoin/trade/replace-order");
    let body: Value = serde_json::from_str(&requests[2].body).expect("json body");
    assert_eq!(body.get("OrderSysID").and_then(Value::as_str), Some("2001"));
    assert_eq!(body.get("volume").and_then(Value::as_str), Some("3"));
    assert_signed_deepcoin_request(&requests[3], "POST", "/deepcoin/trade/batch-orders");
    let body: Value = serde_json::from_str(&requests[3].body).expect("json body");
    assert_eq!(
        body.get("orders").and_then(Value::as_array).map(Vec::len),
        Some(2)
    );
    assert_signed_deepcoin_request(&requests[4], "POST", "/deepcoin/trade/batch-cancel-order");
    let body: Value = serde_json::from_str(&requests[4].body).expect("json body");
    assert_eq!(
        body.get("OrderSysIDs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2)
    );
    assert_signed_deepcoin_request(&requests[5], "POST", "/deepcoin/trade/swap/cancel-all");
    for (request, spec) in requests.into_iter().zip([
        "place_order_perp",
        "cancel_order_perp",
        "amend_order_perp",
        "batch_place_orders_perp",
        "batch_cancel_orders_perp",
        "cancel_all_orders_perp",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("deepcoin private write request spec");
    }
}

#[test]
fn deepcoin_signing_should_match_known_hmac_base64() {
    let signature = super::signing::deepcoin_signature(
        "secret",
        "2020-12-08T09:08:57.715Z",
        "GET",
        "/users/self/verify",
        "",
    );
    assert_eq!(signature, "LSLbxnPWM7dl4oMFFC9S1qW7yJS7ab8VgnW7y7FoEiQ=");
    signing_vector("rest_get_verify")
        .verify()
        .expect("signing vector");
}

#[test]
fn deepcoin_private_parser_should_cover_empty_and_missing_field_fixtures() {
    let empty_orders = parse_orders(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("empty_array"),
    )
    .expect("empty orders");
    assert!(empty_orders.is_empty());

    let empty_order = parse_order(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("empty_order"),
    )
    .expect("empty order");
    assert!(empty_order.is_none());

    let missing_symbol = parse_order(
        &exchange_id(),
        None,
        MarketType::Spot,
        &fixture("missing_inst_id_order"),
    )
    .expect_err("missing instId should fail without fallback symbol");
    assert!(matches!(missing_symbol, ExchangeApiError::Exchange(_)));
}

fn private_adapter(base_url: String) -> DeepcoinGatewayAdapter {
    DeepcoinGatewayAdapter::new(DeepcoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("pass".to_string()),
        enabled_private_rest: true,
        ..DeepcoinGatewayConfig::default()
    })
    .expect("adapter")
}
