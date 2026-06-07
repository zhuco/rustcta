use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PositionsRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, PositionSide};
use serde_json::json;

use super::test_support::{
    assert_signed_request, context, exchange_id, spawn_rest_server, spot_symbol_scope,
    swap_symbol_scope,
};
use super::{DigiFinexGatewayAdapter, DigiFinexGatewayConfig};

#[test]
fn digifinex_capabilities_v2_should_declare_native_batch_and_rest_reconciliation() {
    let adapter = DigiFinexGatewayAdapter::new(DigiFinexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..DigiFinexGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(matches!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        rustcta_exchange_api::BatchExecutionMode::Native
    ));
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .supports_partial_failure
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(200)
    );
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
}

#[tokio::test]
async fn digifinex_private_operations_should_require_credentials() {
    let adapter = DigiFinexGatewayAdapter::default_public().expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);
    let error = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
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
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn digifinex_adapter_should_route_orders_and_batches() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"order_id": "1001", "symbol": "btc_usdt", "side": "buy", "amount": "0.01", "price": "65000", "status": "open"}}),
        json!({"code": 0, "data": {"order_id": "1001"}}),
        json!({"code": 0, "data": [{"order_id": "2001", "instrument_id": "btc_usdt", "side": "buy", "amount": "1", "price": "65000"}]}),
        json!({"code": 0, "data": [{"order_id": "2001"}]}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("cid-1".to_string()),
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
    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("child"),
                symbol: swap_symbol_scope(),
                client_order_id: Some("swap-cid".to_string()),
                side: OrderSide::Buy,
                position_side: Some(PositionSide::Long),
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "1".to_string(),
                price: Some("65000".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("batch place");
    adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-child"),
                symbol: swap_symbol_scope(),
                client_order_id: None,
                exchange_order_id: Some("2001".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "POST", "/v3/spot/order/new");
    assert_signed_request(&requests[1], "POST", "/v3/spot/order/cancel");
    assert_signed_request(&requests[2], "POST", "/swap/v2/trade/batch_order");
    assert_signed_request(&requests[3], "POST", "/swap/v2/trade/batch_cancel_order");
}

#[tokio::test]
async fn digifinex_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"USDT": {"available": "10", "frozen": "0", "total": "10"}}}),
        json!({"code": 0, "data": [{"instrument_id": "btc_usdt", "amount": "1", "side": "long", "entry_price": "65000"}]}),
        json!({"code": 0, "data": []}),
        json!({"code": 0, "data": []}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec![],
        })
        .await
        .expect("balances");
    adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![],
        })
        .await
        .expect("positions");
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
            limit: Some(200),
            page: None,
        })
        .await
        .expect("fills");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/v3/spot/assets");
    assert_signed_request(&requests[1], "GET", "/swap/v2/account/positions");
    assert_signed_request(&requests[2], "GET", "/v3/spot/order/current");
    assert_signed_request(&requests[3], "GET", "/v3/spot/my_trades");
}

fn private_adapter(base_url: String) -> DigiFinexGatewayAdapter {
    DigiFinexGatewayAdapter::new(DigiFinexGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..DigiFinexGatewayConfig::default()
    })
    .expect("adapter")
}
