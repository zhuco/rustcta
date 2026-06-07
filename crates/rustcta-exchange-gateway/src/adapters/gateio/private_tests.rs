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
async fn gateio_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "id": "2001",
            "currency_pair": "BTC_USDT",
            "text": "t-LIMIT1",
            "side": "buy",
            "type": "limit",
            "time_in_force": "gtc",
            "status": "open",
            "price": "65000",
            "amount": "0.02",
            "left": "0.02",
            "create_time_ms": "1743054548123"
        }),
        json!({
            "id": "2002",
            "currency_pair": "BTC_USDT",
            "text": "t-QUOTE1",
            "side": "buy",
            "type": "market",
            "time_in_force": "ioc",
            "status": "open",
            "price": "0",
            "amount": "25.5",
            "left": "25.5",
            "create_time_ms": "1743054549123"
        }),
        json!({
            "id": "2001",
            "currency_pair": "BTC_USDT",
            "text": "t-LIMIT1",
            "status": "cancelled"
        }),
        json!([
            {"id": "2003", "currency_pair": "BTC_USDT", "text": "t-CANCELALL1", "status": "cancelled"},
            {"id": "2004", "currency_pair": "BTC_USDT", "text": "t-CANCELALL2", "status": "cancelled"}
        ]),
        json!({
            "id": "2005",
            "currency_pair": "BTC_USDT",
            "text": "t-AMEND1",
            "side": "buy",
            "type": "limit",
            "status": "open",
            "price": "65000",
            "amount": "0.015",
            "left": "0.015",
            "create_time_ms": "1743054550123"
        }),
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
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: Some("LIMIT1".to_string()),
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
    assert_eq!(placed.order.client_order_id.as_deref(), Some("t-LIMIT1"));
    assert_eq!(placed.order.status, OrderStatus::New);

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope("BTC_USDT"),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote market order");
    assert_eq!(quote.order.exchange_order_id.as_deref(), Some("2002"));
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-order"),
            symbol: symbol_scope("BTC-USDT"),
            client_order_id: None,
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
            symbol: Some(symbol_scope("BTCUSDT")),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 2);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope("BTC_USDT"),
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

    assert_signed_request_method(&requests[0], "POST", "/spot/orders");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["currency_pair"], "BTC_USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "limit");
    assert_eq!(body["amount"], "0.02");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["time_in_force"], "gtc");
    assert_eq!(body["text"], "t-LIMIT1");

    assert_signed_request_method(&requests[1], "POST", "/spot/orders");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["currency_pair"], "BTC_USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "market");
    assert_eq!(body["amount"], "25.5");
    assert_eq!(body["time_in_force"], "ioc");
    assert_eq!(body["text"], "t-QUOTE1");

    assert_signed_request_method(&requests[2], "DELETE", "/spot/orders/2001");
    assert_eq!(
        requests[2].query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );

    assert_signed_request_method(&requests[3], "DELETE", "/spot/orders");
    assert_eq!(
        requests[3].query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );

    assert_signed_request_method(&requests[4], "PATCH", "/spot/orders/2005");
    let body = requests[4].body.as_ref().expect("amend body");
    assert_eq!(body["currency_pair"], "BTC_USDT");
    assert_eq!(body["account"], "spot");
    assert_eq!(body["amount"], "0.015");
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
