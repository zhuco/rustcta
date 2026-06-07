use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    QueryOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{LiquidityRole, MarketType, OrderSide, OrderStatus};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{KuCoinGatewayAdapter, KuCoinGatewayConfig};

#[tokio::test]
async fn kucoin_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = KuCoinGatewayAdapter::default_public().expect("adapter");
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

fn assert_signed_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("kc-api-key").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("kc-api-sign")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("kc-api-timestamp")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("kc-api-passphrase")
        .is_some_and(|value| !value.is_empty() && value != "passphrase"));
    assert_eq!(
        request
            .headers
            .get("kc-api-key-version")
            .map(String::as_str),
        Some("2")
    );
}

#[test]
fn kucoin_signing_should_base64_encode_hmac_sha256() {
    let signature = super::signing::sign_base64("secret", "1700000000000GET/api/v1/accounts")
        .expect("signature");
    assert_eq!(signature, "ka2jGwVPj+HJ5t7L4fEM4HttekAXENIQpmo8ulfZmV8=");
}

#[tokio::test]
async fn kucoin_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "200000",
            "data": [
                {"type": "trade", "currency": "BTC", "available": "0.5", "holds": "0.1"},
                {"type": "trade", "currency": "USDT", "available": "123.45", "holds": "1.55"}
            ]
        }),
        json!({
            "code": "200000",
            "data": {
                "id": "1001",
                "symbol": "BTC-USDT",
                "clientOid": "CID1001",
                "side": "buy",
                "type": "limit",
                "isActive": true,
                "price": "65000",
                "size": "0.01",
                "dealSize": "0.006",
                "dealFunds": "390.06",
                "createdAt": 1743054548123i64
            }
        }),
        json!({
            "code": "200000",
            "data": {
                "items": [{
                    "id": "1002",
                    "symbol": "BTC-USDT",
                    "clientOid": "CID1002",
                    "side": "sell",
                    "type": "limit",
                    "isActive": true,
                    "price": "70000",
                    "size": "0.02",
                    "dealSize": "0",
                    "createdAt": 1743054549000i64
                }]
            }
        }),
        json!({
            "code": "200000",
            "data": [{
                "symbol": "BTC-USDT",
                "makerFeeRate": "0.001",
                "takerFeeRate": "0.0015"
            }]
        }),
        json!({
            "code": "200000",
            "data": {
                "items": [{
                    "tradeId": "9001",
                    "orderId": "1001",
                    "clientOid": "CID1001",
                    "symbol": "BTC-USDT",
                    "side": "buy",
                    "price": "65010",
                    "size": "0.006",
                    "fee": "0.39",
                    "feeCurrency": "USDT",
                    "liquidity": "taker",
                    "createdAt": 1743054550000i64
                }]
            }
        }),
    ])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinGatewayConfig::default()
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
    assert_eq!(balances.balances[0].balances[0].available, 0.5);

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
    assert_eq!(order.client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.status, OrderStatus::New);
    assert_eq!(order.price.as_deref(), Some("65000"));
    assert_eq!(order.quantity, "0.01");
    assert_eq!(order.filled_quantity, "0.006");
    assert_eq!(order.average_fill_price.as_deref(), Some("65010"));

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
    assert_eq!(fees.fees[0].maker_rate, "0.001");
    assert_eq!(fees.fees[0].taker_rate, "0.0015");

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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].liquidity_role, LiquidityRole::Taker);
    assert_eq!(fills.fills[0].price, 65_010.0);
    assert_eq!(fills.fills[0].quantity, 0.006);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_request(&requests[0], "/api/v1/accounts");
    assert!(requests[0].query.is_empty());
    assert_signed_request(&requests[1], "/api/v1/hf/orders/1001");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_signed_request(&requests[2], "/api/v1/hf/orders/active");
    assert_eq!(
        requests[2].query.get("status").map(String::as_str),
        Some("active")
    );
    assert_signed_request(&requests[3], "/api/v1/trade-fees");
    assert_eq!(
        requests[3].query.get("symbols").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_signed_request(&requests[4], "/api/v1/hf/fills");
    assert_eq!(
        requests[4].query.get("pageSize").map(String::as_str),
        Some("100")
    );
}
