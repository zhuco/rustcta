use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, CancelAllOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest, PlaceOrderRequest,
    QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id, spawn_rest_server,
    symbol_scope,
};
use super::{CoinExGatewayAdapter, CoinExGatewayConfig};

#[tokio::test]
async fn coinex_adapter_should_keep_private_operations_unsupported_until_credentials_move() {
    let adapter = CoinExGatewayAdapter::default_public().expect("adapter");
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
async fn coinex_adapter_should_sign_private_get_requests_with_headers() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "ccy": "BTC",
            "available": "0.5",
            "frozen": "0.1",
            "total": "0.6"
        }]
    })])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        api_key: "coinex-key".to_string(),
        api_secret: "coinex-secret".to_string(),
        enabled_private_rest: true,
        ..CoinExGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("signed-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");

    assert!(adapter.capabilities().supports_private_rest);
    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "BTC");
    assert_eq!(response.balances[0].balances[0].available, 0.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/assets/spot/balance");
    assert_signed_request(&request, "coinex-key", "coinex-secret");
}

#[tokio::test]
async fn coinex_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": {
                "order_id": "2001",
                "market": "BTCUSDT",
                "client_id": "CLIENTLIMIT1",
                "side": "buy",
                "type": "limit",
                "option": "normal",
                "status": "open",
                "price": "65000",
                "amount": "0.02",
                "filled_amount": "0",
                "created_at": 1743054548000_i64
            }
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "2002",
                "market": "BTCUSDT",
                "client_id": "CLIENTQUOTE1",
                "side": "buy",
                "type": "market",
                "status": "open",
                "amount": "25.5",
                "deal_amount": "0",
                "created_at": 1743054549000_i64
            }
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "2001",
                "market": "BTCUSDT",
                "client_id": "CLIENTLIMIT1",
                "status": "cancel"
            }
        }),
        json!({
            "code": 0,
            "data": [{"order_id": "2003"}, {"order_id": "2004"}]
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "2005",
                "market": "BTCUSDT",
                "side": "buy",
                "type": "limit",
                "status": "open",
                "amount": "0.015",
                "deal_amount": "0",
                "created_at": 1743054550000_i64,
                "updated_at": 1743054551000_i64
            }
        }),
    ])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CoinExGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-order"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
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
    assert_eq!(
        placed.order.client_order_id.as_deref(),
        Some("CLIENTLIMIT1")
    );

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTQUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote order");
    assert_eq!(quote.order.exchange_order_id.as_deref(), Some("2002"));
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-order"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel order");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 2);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("2005".to_string()),
            new_client_order_id: None,
            new_quantity: "0.015".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2005"));
    assert_eq!(amended.order.quantity, "0.015");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);

    assert_signed_request_method(&requests[0], "POST", "key", "secret");
    assert_eq!(requests[0].path, "/spot/order");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["market"], "BTCUSDT");
    assert_eq!(body["market_type"], "SPOT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "limit");
    assert_eq!(body["option"], "normal");
    assert_eq!(body["amount"], "0.02");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["client_id"], "CLIENTLIMIT1");

    assert_signed_request_method(&requests[1], "POST", "key", "secret");
    assert_eq!(requests[1].path, "/spot/order");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["type"], "market");
    assert_eq!(body["amount"], "25.5");
    assert_eq!(body["ccy"], "USDT");
    assert_eq!(body["client_id"], "CLIENTQUOTE1");

    assert_signed_request_method(&requests[2], "DELETE", "key", "secret");
    assert_eq!(requests[2].path, "/spot/order");
    let body = requests[2].body.as_ref().expect("cancel body");
    assert_eq!(body["market"], "BTCUSDT");
    assert_eq!(body["order_id"], "2001");
    assert_eq!(body["client_id"], "CLIENTLIMIT1");

    assert_signed_request_method(&requests[3], "POST", "key", "secret");
    assert_eq!(requests[3].path, "/spot/cancel-all-order");
    let body = requests[3].body.as_ref().expect("cancel all body");
    assert_eq!(body["market"], "BTCUSDT");
    assert_eq!(body["market_type"], "SPOT");

    assert_signed_request_method(&requests[4], "POST", "key", "secret");
    assert_eq!(requests[4].path, "/spot/modify-order");
    let body = requests[4].body.as_ref().expect("amend body");
    assert_eq!(body["market"], "BTCUSDT");
    assert_eq!(body["market_type"], "SPOT");
    assert_eq!(body["order_id"], 2005);
    assert_eq!(body["amount"], "0.015");
}

#[tokio::test]
async fn coinex_adapter_should_parse_private_readback_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": [
                {"ccy": "BTC", "available": "0.5", "frozen": "0.1", "total": "0.6"},
                {"ccy": "USDT", "available": "1000", "frozen": "25", "total": "1025"}
            ]
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "1001",
                "client_id": "CID1001",
                "market": "BTCUSDT",
                "side": "buy",
                "type": "limit",
                "option": "normal",
                "status": "part_deal",
                "price": "65000",
                "amount": "0.01",
                "filled_amount": "0.006",
                "avg_price": "65010",
                "created_at": 1743054550000_i64,
                "updated_at": 1743054551000_i64
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "order_id": "1002",
                "client_id": "CID1002",
                "market": "BTCUSDT",
                "side": "sell",
                "type": "limit",
                "status": "open",
                "price": "70000",
                "amount": "0.02",
                "filled_amount": "0"
            }]
        }),
        json!({
            "code": 0,
            "data": {
                "market": "BTCUSDT",
                "maker_fee_rate": "0.001",
                "taker_fee_rate": "0.0015"
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "deal_id": "9001",
                "order_id": "1001",
                "client_id": "CID1001",
                "market": "BTCUSDT",
                "side": "buy",
                "price": "65010",
                "amount": "0.006",
                "fee": "0.39",
                "fee_ccy": "USDT",
                "created_at": 1743054550000_i64
            }]
        }),
    ])
    .await;
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CoinExGatewayConfig::default()
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
    assert_eq!(balances.balances[0].balances.len(), 2);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].locked, 0.1);

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
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
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
    assert_eq!(fills.fills[0].price, 65010.0);
    assert_eq!(fills.fills[0].quantity, 0.006);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.39));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0].path, "/assets/spot/balance");
    assert_eq!(requests[1].path, "/spot/order-status");
    assert_eq!(
        requests[1].query.get("order_id").map(String::as_str),
        Some("1001")
    );
    assert_eq!(requests[2].path, "/spot/pending-order");
    assert_eq!(requests[3].path, "/spot/market");
    assert_eq!(requests[4].path, "/spot/finished-order");
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("100")
    );
}
