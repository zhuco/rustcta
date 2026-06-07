use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::signing::sign_raw_query;
use super::test_support::{
    assert_signed_request, context, exchange_id, private_config, spawn_rest_server, symbol_scope,
};
use super::BinanceGatewayAdapter;

#[test]
fn binance_signing_should_match_documented_example() {
    let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
    let signature = sign_raw_query(
        "NhqPtmdSJYVB3mq9F2ZAjgI4VdFOvZyY6XifdlLqbUeX7hY4tH8MGjdPVeQx1ioO",
        query,
    )
    .expect("signature");
    assert_eq!(
        signature,
        "1df63f2e1799dde74ddff3876c338dce38b1cd5da51483e1beffe9c87a8550ba"
    );
}

#[tokio::test]
async fn binance_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BinanceGatewayAdapter::default_public().expect("adapter");
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
async fn binance_adapter_should_load_balances_from_signed_account_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "balances": [
            {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"},
            {"asset": "ETH", "free": "0.00000000", "locked": "0.00000000"}
        ]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "BTC");
    assert_eq!(response.balances[0].balances[0].available, 0.1);
    assert_eq!(response.balances[0].balances[0].locked, 0.02);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/account");
    assert_signed_request(&request);
}

#[tokio::test]
async fn binance_adapter_should_query_order_from_signed_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbol": "BTCUSDT",
        "orderId": 28,
        "clientOrderId": "cli-1",
        "price": "23416.10000000",
        "origQty": "0.00847000",
        "executedQty": "0.00400000",
        "cummulativeQuoteQty": "93.66440000",
        "status": "PARTIALLY_FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "time": 1499827319559_i64,
        "updateTime": 1499827319666_i64
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: Some("cli-1".to_string()),
            exchange_order_id: Some("28".to_string()),
        })
        .await
        .expect("order");

    let order = response.order.expect("order state");
    assert_eq!(order.exchange_order_id.as_deref(), Some("28"));
    assert_eq!(order.client_order_id.as_deref(), Some("cli-1"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.quantity, "0.00847000");
    assert_eq!(order.filled_quantity, "0.00400000");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/order");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("orderId").map(String::as_str), Some("28"));
    assert_eq!(
        request.query.get("origClientOrderId").map(String::as_str),
        Some("cli-1")
    );
    assert_signed_request(&request);
}

#[tokio::test]
async fn binance_adapter_should_load_open_orders_from_signed_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([{
        "symbol": "BTCUSDT",
        "orderId": 29,
        "clientOrderId": "cli-open",
        "price": "25000.00000000",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "SELL",
        "time": 1499827319559_i64,
        "updateTime": 1499827319666_i64
    }])])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTC-USDT")),
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    assert_eq!(response.orders[0].exchange_order_id.as_deref(), Some("29"));
    assert_eq!(response.orders[0].status, OrderStatus::New);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/openOrders");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_request(&request);
}

#[tokio::test]
async fn binance_adapter_should_load_fees_from_account_commission_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbol": "BTCUSDT",
        "standardCommission": {
            "maker": "0.00000040",
            "taker": "0.00000050"
        }
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTCUSDT")],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "0.00000040");
    assert_eq!(response.fees[0].taker_rate, "0.00000050");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/account/commission");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_request(&request);
}

#[tokio::test]
async fn binance_adapter_should_load_recent_fills_from_signed_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([{
        "symbol": "BTCUSDT",
        "id": 28457,
        "orderId": 100234,
        "clientOrderId": "cli-fill",
        "price": "23416.10000000",
        "qty": "0.00400000",
        "quoteQty": "93.66440000",
        "commission": "0.00000010",
        "commissionAsset": "BTC",
        "time": 1499827319559_i64,
        "isBuyer": true,
        "isMaker": false
    }])])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTCUSDT")),
            client_order_id: Some("cli-fill".to_string()),
            exchange_order_id: Some("100234".to_string()),
            from_trade_id: Some("28457".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(50),
        })
        .await
        .expect("fills");

    assert_eq!(response.fills.len(), 1);
    assert_eq!(response.fills[0].fill_id.as_deref(), Some("28457"));
    assert_eq!(response.fills[0].order_id.as_deref(), Some("100234"));
    assert_eq!(
        response.fills[0].client_order_id.as_deref(),
        Some("cli-fill")
    );
    assert_eq!(response.fills[0].side, OrderSide::Buy);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/myTrades");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        request.query.get("orderId").map(String::as_str),
        Some("100234")
    );
    assert_eq!(
        request.query.get("fromId").map(String::as_str),
        Some("28457")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("50"));
    assert_signed_request(&request);
}
