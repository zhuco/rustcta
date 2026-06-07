use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{LiquidityRole, MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::test_support::{
    assert_signed_okx_request, context, exchange_id, private_config, spawn_rest_server,
    symbol_scope,
};
use super::{OkxGatewayAdapter, OkxGatewayConfig};

#[tokio::test]
async fn okx_adapter_should_keep_private_operations_and_streams_unsupported() {
    let adapter = OkxGatewayAdapter::default_public().expect("adapter");
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
async fn okx_adapter_should_sign_private_readback_requests_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "details": [
                    {"ccy": "BTC", "cashBal": "0.12", "availBal": "0.10", "frozenBal": "0.02"},
                    {"ccy": "USDT", "cashBal": "125.5", "availBal": "125.5", "frozenBal": "0"}
                ]
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "1001",
                "clOrdId": "CLIENT1",
                "side": "buy",
                "ordType": "limit",
                "state": "partially_filled",
                "sz": "0.01",
                "px": "65000",
                "accFillSz": "0.004",
                "avgPx": "64950",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "1002",
                "clOrdId": "CLIENT2",
                "side": "sell",
                "ordType": "post_only",
                "state": "live",
                "sz": "0.02",
                "px": "70000",
                "accFillSz": "0",
                "avgPx": "",
                "cTime": "1710000001000",
                "uTime": "1710000001000"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"instId": "BTC-USDT", "maker": "-0.0008", "taker": "-0.001"}]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "fill-1",
                "ordId": "1001",
                "clOrdId": "CLIENT1",
                "side": "buy",
                "fillPx": "64950",
                "fillSz": "0.004",
                "feeCcy": "USDT",
                "fee": "-0.2598",
                "execType": "T",
                "fillTime": "1710000000200"
            }]
        }),
    ])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_query_order);

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
    assert_eq!(balances.balances[0].balances[0].available, 0.10);
    assert_eq!(balances.balances[0].balances[0].locked, 0.02);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
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
            context: context("open-orders"),
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
    assert!(open_orders.orders[0].post_only);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "-0.0008");
    assert_eq!(fees.fees[0].taker_rate, "-0.001");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: Some("CLIENT1".to_string()),
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].client_order_id.as_deref(), Some("CLIENT1"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].liquidity_role, LiquidityRole::Taker);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.2598));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_okx_request(&requests[0], "/api/v5/account/balance");
    assert!(requests[0].query.is_empty());

    assert_signed_okx_request(&requests[1], "/api/v5/trade/order");
    assert_eq!(
        requests[1].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(
        requests[1].query.get("ordId").map(String::as_str),
        Some("1001")
    );

    assert_signed_okx_request(&requests[2], "/api/v5/trade/orders-pending");
    assert_eq!(
        requests[2].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[2].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_okx_request(&requests[3], "/api/v5/account/trade-fee");
    assert_eq!(
        requests[3].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[3].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_okx_request(&requests[4], "/api/v5/trade/fills-history");
    assert_eq!(
        requests[4].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[4].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(
        requests[4].query.get("ordId").map(String::as_str),
        Some("1001")
    );
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("50")
    );
}

#[tokio::test]
async fn okx_adapter_should_return_unsupported_when_private_credentials_are_missing() {
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        enabled_private_rest: true,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("missing-creds"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect_err("missing credentials should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}
