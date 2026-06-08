use rustcta_exchange_api::{
    BalancesRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest,
    PositionsRequest, QueryOrderRequest, QuoteMarketOrderRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::test_support::{
    assert_signed_request, context, exchange_id, private_adapter, spawn_rest_server,
    spot_symbol_scope,
};
use super::{BitmartGatewayAdapter, BitmartGatewayConfig};

#[tokio::test]
async fn bitmart_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BitmartGatewayAdapter::default_public().expect("adapter");
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
        .expect_err("private op unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn bitmart_capabilities_v2_should_declare_ws_rest_fallback_and_boundaries() {
    let adapter = BitmartGatewayAdapter::new(BitmartGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        memo: Some("memo".to_string()),
        enabled_private_rest: true,
        enabled_private_streams: true,
        ..BitmartGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities.capabilities_v2.private_streams.is_supported());
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_batch_place_order);
}

#[tokio::test]
async fn bitmart_adapter_should_sign_private_readbacks_and_order_lifecycle() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 1000,
            "data": {"wallet": [{"id": "USDT", "available": "100", "frozen": "2"}]}
        }),
        json!({
            "code": 1000,
            "data": [{"symbol": "BTCUSDT", "position_amount": "0.5", "side": "long", "entry_price": "65000"}]
        }),
        json!({
            "code": 1000,
            "data": {"symbol": "BTC_USDT", "order_id": "1001", "client_order_id": "CLIENT1", "side": "buy", "type": "limit", "price": "65000", "size": "0.01", "status": "new"}
        }),
        json!({
            "code": 1000,
            "data": {"symbol": "BTC_USDT", "order_id": "1001", "client_order_id": "CLIENT1", "side": "buy", "type": "limit", "price": "65000", "size": "0.01", "status": "canceled"}
        }),
        json!({
            "code": 1000,
            "data": {"symbol": "BTC_USDT", "order_id": "1001", "client_order_id": "CLIENT1", "side": "buy", "type": "limit", "price": "65000", "size": "0.01", "status": "filled"}
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
    assert_eq!(balances.balances[0].balances[0].asset, "USDT");

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
    assert_eq!(positions.positions[0].side, PositionSide::Long);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("CLIENT1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
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
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let queried = adapter
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
    assert_eq!(queried.status, OrderStatus::Filled);

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/spot/v1/wallet");
    assert_signed_request(&requests[2], "POST", "/spot/v2/submit_order");
    assert!(requests[2].body.contains("CLIENT1"));
}

#[tokio::test]
async fn bitmart_adapter_should_support_spot_quote_market_buy() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 1000,
        "data": {"symbol": "BTC_USDT", "order_id": "1002", "client_order_id": "QUOTE1", "side": "buy", "type": "market", "notional": "25", "status": "new"}
    })])
    .await;
    let adapter = private_adapter(base_url);

    let placed = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-market"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect("quote market buy");

    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1002"));
    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "POST", "/spot/v2/submit_order");
    assert!(requests[0].body.contains("\"notional\":\"25\""));
}
