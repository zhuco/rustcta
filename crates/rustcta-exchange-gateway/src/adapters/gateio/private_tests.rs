use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::test_support::{
    assert_signed_request, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::{GateIoGatewayAdapter, GateIoGatewayConfig};

#[tokio::test]
async fn gateio_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = GateIoGatewayAdapter::default_public().expect("adapter");
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
async fn gateio_adapter_should_sign_private_readback_requests_and_parse_responses() {
    let order = json!({
        "id": "1001",
        "text": "t-CLIENT1",
        "currency_pair": "BTC_USDT",
        "side": "buy",
        "type": "limit",
        "time_in_force": "gtc",
        "status": "open",
        "price": "65000",
        "amount": "0.01",
        "left": "0.004",
        "avg_deal_price": "65010",
        "create_time_ms": "1710000000000",
        "update_time_ms": "1710000001000"
    });
    let fill = json!({
        "id": "trade-1",
        "order_id": "1001",
        "text": "t-CLIENT1",
        "currency_pair": "BTC_USDT",
        "side": "buy",
        "price": "65000",
        "amount": "0.006",
        "fee": "0.000006",
        "fee_currency": "BTC",
        "role": "maker",
        "create_time_ms": "1710000002000"
    });
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            {"currency": "BTC", "available": "0.5", "locked": "0.1"},
            {"currency": "ETH", "available": "0", "locked": "0"}
        ]),
        order.clone(),
        json!([{"currency_pair": "BTC_USDT", "orders": [order.clone()]}]),
        json!({"maker_fee": "0.001", "taker_fee": "0.002"}),
        json!([fill]),
    ])
    .await;
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("gate-key".to_string()),
        api_secret: Some("gate-secret".to_string()),
        enabled_private_rest: true,
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);

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
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].total, 0.6);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(queried.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(queried.client_order_id.as_deref(), Some("t-CLIENT1"));
    assert_eq!(queried.side, OrderSide::Buy);
    assert_eq!(queried.status, OrderStatus::New);
    assert_eq!(queried.quantity, "0.01");
    assert_eq!(queried.filled_quantity, "0.006");
    assert_eq!(queried.average_fill_price.as_deref(), Some("65010"));

    let open_orders = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTC_USDT")),
        })
        .await
        .expect("open orders");
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(
        open_orders.orders[0].client_order_id.as_deref(),
        Some("t-CLIENT1")
    );

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTC_USDT")],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "0.001");
    assert_eq!(fees.fees[0].taker_rate, "0.002");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTC-USDT")),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: Some("trade-0".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(50),
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("trade-1"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("BTC"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_request(&requests[0], "/spot/accounts");
    assert_signed_request(&requests[1], "/spot/orders/1001");
    assert_signed_request(&requests[2], "/spot/open_orders");
    assert_signed_request(&requests[3], "/spot/fee");
    assert_signed_request(&requests[4], "/spot/my_trades");
    assert_eq!(
        requests[1].query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        requests[4].query.get("order_id").map(String::as_str),
        Some("1001")
    );
    assert_eq!(
        requests[4].query.get("last_id").map(String::as_str),
        Some("trade-0")
    );
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("50")
    );
}
