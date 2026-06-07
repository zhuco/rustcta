use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::test_support::{
    assert_signed_request, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::{MexcGatewayAdapter, MexcGatewayConfig};

#[tokio::test]
async fn mexc_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = MexcGatewayAdapter::default_public().expect("adapter");
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

#[test]
fn mexc_signing_should_match_known_hmac() {
    let query = "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=11&recvWindow=5000&timestamp=1644489390087";
    let signature = super::signing::sign_raw_query("secret", query).expect("signature");
    assert_eq!(
        signature,
        "28ba033f359efa9f91364321c923653ac346dfad5fdb95e5b38f41b50c31d4cf"
    );
}

#[tokio::test]
async fn mexc_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "balances": [
                {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"},
                {"asset": "USDT", "free": "123.45", "locked": "1.55"}
            ]
        }),
        json!({
            "symbol": "BTCUSDT",
            "orderId": 1001,
            "clientOrderId": "CLIENT1",
            "price": "65000",
            "origQty": "0.01",
            "executedQty": "0.004",
            "cummulativeQuoteQty": "259.8",
            "status": "PARTIALLY_FILLED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "time": 1700000000000i64,
            "updateTime": 1700000001000i64
        }),
        json!([{
            "symbol": "BTCUSDT",
            "orderId": 1002,
            "clientOrderId": "CLIENT2",
            "price": "70000",
            "origQty": "0.02",
            "executedQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL",
            "time": 1700000002000i64,
            "updateTime": 1700000002000i64
        }]),
        json!([{
            "symbol": "BTCUSDT",
            "makerCommission": "0.0008",
            "takerCommission": "0.001"
        }]),
        json!([{
            "symbol": "BTCUSDT",
            "id": 2001,
            "orderId": 1001,
            "clientOrderId": "CLIENT1",
            "price": "64950",
            "qty": "0.004",
            "commission": "0.2598",
            "commissionAsset": "USDT",
            "isBuyer": true,
            "time": 1700000001000i64
        }]),
    ])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..MexcGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_query_order);

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
    assert_eq!(balances.balances[0].balances.len(), 2);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].available, 0.1);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("order")
        .order
        .expect("order state");
    assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(order.client_order_id.as_deref(), Some("CLIENT1"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.quantity, "0.01");
    assert_eq!(order.filled_quantity, "0.004");
    assert_eq!(order.average_fill_price.as_deref(), Some("64950"));

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
            limit: Some(100),
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("2001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].price, 64_950.0);
    assert_eq!(fills.fills[0].quantity, 0.004);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_request(&requests[0], "/api/v3/account");
    assert!(requests[0].query.get("symbol").is_none());
    assert_signed_request(&requests[1], "/api/v3/order");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[1].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    assert_signed_request(&requests[2], "/api/v3/openOrders");
    assert_signed_request(&requests[3], "/api/v3/tradeFee");
    assert_signed_request(&requests[4], "/api/v3/myTrades");
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("100")
    );
}
