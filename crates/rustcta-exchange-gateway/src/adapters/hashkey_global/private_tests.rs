use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::test_support::{
    assert_signed_hashkey_global_request, context, exchange_id, perp_symbol_scope,
    spawn_rest_server, spot_symbol_scope,
};
use super::{HashKeyGlobalGatewayAdapter, HashKeyGlobalGatewayConfig};

#[test]
fn hashkey_global_capabilities_v2_should_declare_composed_batch_and_listen_key_policy() {
    let adapter = HashKeyGlobalGatewayAdapter::new(HashKeyGlobalGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..HashKeyGlobalGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(matches!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        rustcta_exchange_api::BatchExecutionMode::ComposedSequential
    ));
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .auth
            .uses_listen_key
    );
    assert!(matches!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .auth_renewal_policy
            .kind,
        rustcta_exchange_api::AuthRenewalKind::ListenKeyKeepAlive
    ));
}

#[tokio::test]
async fn hashkey_global_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = HashKeyGlobalGatewayAdapter::default_public().expect("adapter");
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
async fn hashkey_global_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "makerCommission": 15,
            "takerCommission": 20,
            "balances": [
                {"asset": "BTC", "free": "0.1", "locked": "0.02"},
                {"asset": "USDT", "free": "10", "locked": "0"}
            ]
        }),
        json!({
            "makerCommission": 15,
            "takerCommission": 20,
            "balances": []
        }),
        json!({
            "code": 0,
            "data": {"account": [{
                "marginCoin": "USDT",
                "accountNormal": 10,
                "accountLock": 0,
                "totalEquity": 10,
                "positionVos": [{
                    "symbol": "BTCUSDT",
                    "positions": [{
                        "side": "BUY",
                        "volume": "0.5",
                        "avgPrice": "65000",
                        "indexPrice": "65100",
                        "leverageLevel": "5"
                    }]
                }]
            }]}
        }),
        json!({
            "code": 0,
            "data": {"openMakerFee": "0.0002", "openTakerFee": "0.0005"}
        }),
        json!({
            "code": 0,
            "data": {
                "symbol": "BTCUSDT",
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
            "data": [{
                "symbol": "BTCUSDT",
                "orderId": 1002,
                "clientOrderId": "CLIENT2",
                "price": "70000",
                "origQty": "0.02",
                "executedQty": "0",
                "status": "NEW",
                "type": "LIMIT",
                "side": "SELL"
            }]
        }),
        json!({
            "code": 0,
            "data": [{
                "symbol": "BTCUSDT",
                "id": 2001,
                "orderId": 1001,
                "clientOrderId": "CLIENT1",
                "price": "64950",
                "qty": "0.004",
                "commission": "0.2598",
                "commissionAsset": "USDT",
                "isBuyer": true,
                "isMaker": false,
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

    let spot_fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("spot fees");
    assert_eq!(spot_fees.fees[0].maker_rate, "0.0015");
    assert_eq!(spot_fees.fees[0].taker_rate, "0.002");

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
    assert_eq!(fills.fills[0].side, OrderSide::Buy);

    let requests = seen.lock().unwrap().clone();
    assert_signed_hashkey_global_request(&requests[0], "GET", "/api/v1/account");
    assert_signed_hashkey_global_request(&requests[1], "GET", "/api/v1/account");
    assert!(!requests[1].query.contains_key("symbol"));
    assert_signed_hashkey_global_request(&requests[2], "GET", "/api/v1/futures/account");
    assert_signed_hashkey_global_request(&requests[3], "GET", "/api/v1/futures/commissionRate");
    assert_signed_hashkey_global_request(&requests[4], "GET", "/api/v1/order");
    assert_signed_hashkey_global_request(&requests[5], "GET", "/api/v1/openOrders");
    assert_signed_hashkey_global_request(&requests[6], "GET", "/api/v2/myTrades");
}

#[tokio::test]
async fn hashkey_global_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({"code": 0, "data": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({"code": 0, "data": [{
            "symbol": "BTCUSDT",
            "orderId": 2002,
            "clientOrderId": "OPEN1",
            "price": "66000",
            "origQty": "0.03",
            "executedQty": "0",
            "status": "NEW",
            "type": "LIMIT",
            "side": "SELL"
        }]}),
        json!({"code": 0, "data": {"orderId": 2002, "clientOrderId": "OPEN1"}}),
        json!({"code": 0, "data": {"orderId": 3001, "clientOrderId": "BATCH1"}}),
        json!({"code": 0, "data": {"orderId": 3001, "clientOrderId": "BATCH1"}}),
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

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let batch_placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-place-order"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                side: OrderSide::Sell,
                position_side: Some(PositionSide::Short),
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "0.03".to_string(),
                price: Some("66000".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("batch place");
    assert_eq!(
        batch_placed.orders[0].exchange_order_id.as_deref(),
        Some("3001")
    );

    let batch_cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-cancel-order"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                exchange_order_id: Some("3001".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancelled.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_signed_hashkey_global_request(&requests[0], "POST", "/api/v1/futures/order");
    assert_signed_hashkey_global_request(&requests[1], "POST", "/api/v1/futures/order");
    assert_signed_hashkey_global_request(&requests[2], "GET", "/api/v1/futures/openOrders");
    assert_signed_hashkey_global_request(&requests[3], "POST", "/api/v1/futures/order");
    assert_signed_hashkey_global_request(&requests[4], "POST", "/api/v1/futures/order");
    assert_signed_hashkey_global_request(&requests[5], "POST", "/api/v1/futures/order");
}

#[tokio::test]
async fn hashkey_global_adapter_should_reject_unverified_order_variants_before_rest() {
    let (base_url, seen) = spawn_rest_server(Vec::new()).await;
    let adapter = private_adapter(base_url);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("unsupported-place"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("UNSUPPORTED1".to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "1".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let quote_error = adapter
        .place_order(PlaceOrderRequest {
            quote_quantity: Some("100".to_string()),
            ..request.clone()
        })
        .await
        .expect_err("quote-sized order should be unsupported");
    assert!(matches!(quote_error, ExchangeApiError::Unsupported { .. }));

    let spot_post_only_error = adapter
        .place_order(PlaceOrderRequest {
            order_type: OrderType::PostOnly,
            time_in_force: Some(TimeInForce::GTX),
            post_only: true,
            ..request.clone()
        })
        .await
        .expect_err("spot post-only should be unsupported");
    assert!(matches!(
        spot_post_only_error,
        ExchangeApiError::Unsupported { .. }
    ));

    let stop_error = adapter
        .place_order(PlaceOrderRequest {
            symbol: perp_symbol_scope(),
            position_side: Some(PositionSide::Long),
            order_type: OrderType::StopMarket,
            time_in_force: None,
            price: None,
            ..request
        })
        .await
        .expect_err("stop order should be unsupported");
    assert!(matches!(stop_error, ExchangeApiError::Unsupported { .. }));

    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn hashkey_global_signing_should_match_known_hmac() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/hashkey_global/signing_vectors/query_hmac_sha256.json"
    ))
    .expect("signing vector");
    let query = vector["query"].as_str().expect("query");
    let signature =
        super::signing::sign_raw_query(vector["secret"].as_str().expect("secret"), query)
            .expect("signature");
    assert_eq!(
        signature,
        vector["expected_signature"]
            .as_str()
            .expect("expected signature")
    );
}

fn private_adapter(base_url: String) -> HashKeyGlobalGatewayAdapter {
    HashKeyGlobalGatewayAdapter::new(HashKeyGlobalGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..HashKeyGlobalGatewayConfig::default()
    })
    .expect("adapter")
}
