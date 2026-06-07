use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::test_support::{
    assert_signed_bitget_request, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::{BitgetGatewayAdapter, BitgetGatewayConfig};

#[tokio::test]
async fn bitget_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        api_key: None,
        api_secret: None,
        passphrase: None,
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");
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
async fn bitget_adapter_should_route_private_rest_readbacks_with_signed_headers() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "00000",
            "data": [
                {"coin": "BTC", "available": "0.5", "frozen": "0.1", "totalAmount": "0.6"},
                {"coin": "ETH", "available": "0", "frozen": "0", "totalAmount": "0"}
            ]
        }),
        json!({
            "code": "00000",
            "data": {
                "symbol": "BTCUSDT",
                "orderId": "1001",
                "clientOid": "CID1001",
                "side": "buy",
                "orderType": "limit",
                "force": "gtc",
                "status": "filled",
                "price": "70000",
                "size": "0.01",
                "filledSize": "0.01",
                "priceAvg": "70001",
                "cTime": "1743054548123",
                "uTime": "1743054549123"
            }
        }),
        json!({
            "code": "00000",
            "data": [{
                "symbol": "BTCUSDT",
                "orderId": "1002",
                "clientOid": "CID1002",
                "side": "sell",
                "orderType": "limit",
                "force": "gtc",
                "status": "live",
                "price": "71000",
                "size": "0.02",
                "filledSize": "0",
                "cTime": "1743054548123",
                "uTime": "1743054549123"
            }]
        }),
        json!({
            "code": "00000",
            "data": {
                "makerFeeRate": "0.0008",
                "takerFeeRate": "0.001"
            }
        }),
        json!({
            "code": "00000",
            "data": {
                "fillList": [{
                    "symbol": "BTCUSDT",
                    "tradeId": "T1001",
                    "orderId": "1001",
                    "clientOid": "CID1001",
                    "side": "buy",
                    "price": "70000",
                    "size": "0.01",
                    "feeCcy": "USDT",
                    "fee": "-0.7",
                    "tradeScope": "taker",
                    "cTime": "1743054548123"
                }]
            }
        }),
    ])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_query_order);
    assert!(adapter.capabilities().supports_open_orders);
    assert!(adapter.capabilities().supports_fees);
    assert!(adapter.capabilities().supports_recent_fills);

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
    assert_eq!(balances.balances[0].balances[0].available, 0.5);
    assert_eq!(balances.balances[0].balances[0].locked, 0.1);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(queried.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(queried.client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(queried.side, OrderSide::Buy);
    assert_eq!(queried.status, OrderStatus::Filled);
    assert_eq!(queried.quantity, "0.01");
    assert_eq!(queried.average_fill_price.as_deref(), Some("70001"));

    let open_orders = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("open orders");
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(
        open_orders.orders[0].exchange_order_id.as_deref(),
        Some("1002")
    );
    assert_eq!(open_orders.orders[0].side, OrderSide::Sell);
    assert_eq!(open_orders.orders[0].status, OrderStatus::New);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "0.0008");
    assert_eq!(fees.fees[0].taker_rate, "0.001");

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
            limit: Some(25),
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("T1001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].price, 70000.0);
    assert_eq!(fills.fills[0].quantity, 0.01);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.7));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_bitget_request(&requests[0], "/api/v2/spot/account/assets");
    assert_signed_bitget_request(&requests[1], "/api/v2/spot/trade/orderInfo");
    assert_eq!(
        requests[1].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    assert_signed_bitget_request(&requests[2], "/api/v2/spot/trade/unfilled-orders");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_bitget_request(&requests[3], "/api/v3/account/fee-rate");
    assert_eq!(
        requests[3].query.get("category").map(String::as_str),
        Some("SPOT")
    );
    assert_signed_bitget_request(&requests[4], "/api/v2/spot/trade/fills");
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("25")
    );
    assert_eq!(
        requests[4].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    assert!(requests
        .iter()
        .all(|request| !request.query.contains_key("secret")));
}
