use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{
    assert_private_request, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::{BiconomyGatewayAdapter, BiconomyGatewayConfig};

#[test]
fn biconomy_capabilities_v2_should_declare_composed_batch_and_rest_reconciliation() {
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(capabilities
        .capabilities_v2
        .batch_cancel_orders
        .support
        .is_supported());
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
    assert!(capabilities
        .capabilities_v2
        .credential_scopes
        .contains(&rustcta_exchange_api::CredentialScope::Trade));
}

#[tokio::test]
async fn biconomy_private_operations_should_require_credentials() {
    let adapter = BiconomyGatewayAdapter::default_public().expect("adapter");
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
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn biconomy_adapter_should_route_order_lifecycle_and_batches() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": "1001", "clientOrderId": "CID1"}}),
        json!({"code": 0, "data": {"orderId": "1001", "clientOrderId": "CID1"}}),
        json!({"code": 0, "data": {"orderId": "1002", "clientOrderId": "B1"}}),
        json!({"code": 0, "data": {"orderId": "1003", "clientOrderId": "B2"}}),
        json!({"code": 0, "data": {"orderId": "1002", "clientOrderId": "B1"}}),
        json!({"code": 0, "data": {"orderId": "1003", "clientOrderId": "B2"}}),
    ])
    .await;
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_batch_place_order);
    assert!(adapter.capabilities().supports_batch_cancel_order);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
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
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1001"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("CID1".to_string()),
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("b1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B1".to_string()),
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
                    context: context("b2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("66000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch");
    assert_eq!(batch.orders.len(), 2);

    let cancel_batch = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("c1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B1".to_string()),
                    exchange_order_id: Some("1002".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("c2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B2".to_string()),
                    exchange_order_id: Some("1003".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancel_batch.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 6);
    assert_private_request(&requests[0], "/api/v2/private/order", "key");
    assert_eq!(requests[0].body.as_ref().unwrap()["symbol"], "BTCUSDT");
    assert_private_request(&requests[1], "/api/v2/private/cancel", "key");
    assert_private_request(&requests[2], "/api/v2/private/order", "key");
    assert_private_request(&requests[5], "/api/v2/private/cancel", "key");
}

#[tokio::test]
async fn biconomy_adapter_should_parse_private_readbacks() {
    let (base_url, _seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"balances": [{"asset": "BTC", "free": "1.2", "locked": "0.3"}]}}),
        json!({"code": 0, "data": {"makerCommission": "0.001", "takerCommission": "0.0015"}}),
        json!({"code": 0, "data": {"orderId": "1001", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "status": "PARTIALLY_FILLED", "origQty": "0.02", "executedQty": "0.01", "price": "65000"}}),
        json!({"code": 0, "data": [{"orderId": "1002", "symbol": "BTCUSDT", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "0.03", "executedQty": "0", "price": "66000"}]}),
        json!({"code": 0, "data": [{"tradeId": "9001", "orderId": "1001", "symbol": "BTCUSDT", "side": "BUY", "price": "65000", "qty": "0.01", "commission": "0.1", "commissionAsset": "USDT", "isMaker": true}]}),
    ])
    .await;
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");

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
    assert_eq!(balances.balances.len(), 1);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.001");

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: symbol_scope(),
            client_order_id: None,
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
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
}

#[tokio::test]
async fn biconomy_cancel_all_should_sweep_symbol_open_orders() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": [
            {"orderId": "1002", "symbol": "BTCUSDT", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "0.03", "executedQty": "0", "price": "66000"},
            {"orderId": "1003", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "status": "NEW", "origQty": "0.01", "executedQty": "0", "price": "64000"}
        ]}),
        json!({"code": 0, "data": {"orderId": "1002"}}),
        json!({"code": 0, "data": {"orderId": "1003"}}),
    ])
    .await;
    let adapter = BiconomyGatewayAdapter::new(BiconomyGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BiconomyGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(response.cancelled_count, 2);
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/api/v2/private/openOrders");
    assert_eq!(requests[1].path, "/api/v2/private/cancel");
    assert_eq!(requests[2].path, "/api/v2/private/cancel");
}
