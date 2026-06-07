use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_api::{BatchExecutionMode, CapabilitySupport, ReconcileTrigger};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{
    assert_private_request, context, exchange_id, load_request_spec, spawn_rest_server,
    symbol_scope,
};
use super::{CointrGatewayAdapter, CointrGatewayConfig};

fn private_adapter(base_url: String) -> CointrGatewayAdapter {
    CointrGatewayAdapter::new(CointrGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CointrGatewayConfig::default()
    })
    .expect("adapter")
}

#[tokio::test]
async fn cointr_private_operations_should_require_credentials() {
    let adapter = CointrGatewayAdapter::default_public().expect("adapter");
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
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn cointr_adapter_should_route_order_lifecycle_and_batches() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": "1001", "clientOrderId": "CID1"}}),
        json!({"code": 0, "data": {"orderId": "1001", "clientOrderId": "CID1"}}),
        json!({"code": 0, "data": [
            {"orderId": "1002", "clientOrderId": "B1"},
            {"orderId": "1003", "clientOrderId": "B2"}
        ]}),
        json!({"code": 0, "data": {"successList": [
            {"orderId": "1002", "clientOrderId": "B1"},
            {"orderId": "1003", "clientOrderId": "B2"}
        ]}}),
    ])
    .await;
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CointrGatewayConfig::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_batch_place_order);
    assert!(adapter.capabilities().supports_batch_cancel_order);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("CID1".to_string()),
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
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1001"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("CID1".to_string()),
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("b1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("65000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("b2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("66000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch");
    assert_eq!(batch.orders.len(), 2);

    let cancel_batch = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("c1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B1".to_string()),
                    exchange_order_id: Some("1002".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("c2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("B2".to_string()),
                    exchange_order_id: Some("1003".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancel_batch.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_private_request(&requests[0], "/api/v2/spot/trade/place-order", "key");
    load_request_spec("request_specs/place_order_spot_limit.json")
        .assert_matches(&requests[0].to_actual_request())
        .expect("place order request spec");
    assert_eq!(requests[0].body.as_ref().unwrap()["symbol"], "BTCUSDT");
    assert_eq!(requests[0].body.as_ref().unwrap()["side"], "buy");
    assert_eq!(requests[0].body.as_ref().unwrap()["orderType"], "limit");
    assert_eq!(requests[0].body.as_ref().unwrap()["force"], "gtc");
    assert_eq!(requests[0].body.as_ref().unwrap()["size"], "0.01");
    assert_eq!(requests[0].body.as_ref().unwrap()["clientOid"], "CID1");
    assert!(requests[0].body.as_ref().unwrap().get("quantity").is_none());
    assert!(requests[0]
        .body
        .as_ref()
        .unwrap()
        .get("newClientOrderId")
        .is_none());
    assert_private_request(&requests[1], "/api/v2/spot/trade/cancel-order", "key");
    load_request_spec("request_specs/cancel_order_spot.json")
        .assert_matches(&requests[1].to_actual_request())
        .expect("cancel order request spec");
    assert_eq!(requests[1].body.as_ref().unwrap()["clientOid"], "CID1");
    assert!(requests[1]
        .body
        .as_ref()
        .unwrap()
        .get("origClientOrderId")
        .is_none());
    assert_private_request(&requests[2], "/api/v2/spot/trade/batch-orders", "key");
    load_request_spec("request_specs/batch_place_orders_spot.json")
        .assert_matches(&requests[2].to_actual_request())
        .expect("batch place request spec");
    assert_eq!(
        requests[2].body.as_ref().unwrap()["orderList"][0]["clientOid"],
        "B1"
    );
    assert_private_request(&requests[3], "/api/v2/spot/trade/batch-cancel-order", "key");
    load_request_spec("request_specs/batch_cancel_orders_spot.json")
        .assert_matches(&requests[3].to_actual_request())
        .expect("batch cancel request spec");
    assert_eq!(
        requests[3].body.as_ref().unwrap()["orderList"][0]["clientOid"],
        "B1"
    );
}

#[tokio::test]
async fn cointr_adapter_should_route_spot_buy_quote_market_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {"orderId": "1004", "clientOrderId": "Q1"}
    })])
    .await;
    let adapter = private_adapter(base_url);
    assert!(adapter.capabilities().supports_quote_market_order);

    let placed = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-market"),
            symbol: symbol_scope(),
            client_order_id: Some("Q1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect("quote market");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1004"));

    let requests = seen.lock().unwrap().clone();
    assert_private_request(&requests[0], "/api/v2/spot/trade/place-order", "key");
    load_request_spec("request_specs/place_quote_market_order_spot.json")
        .assert_matches(&requests[0].to_actual_request())
        .expect("quote market request spec");
    assert_eq!(requests[0].body.as_ref().unwrap()["symbol"], "BTCUSDT");
    assert_eq!(requests[0].body.as_ref().unwrap()["side"], "buy");
    assert_eq!(requests[0].body.as_ref().unwrap()["orderType"], "market");
    assert_eq!(requests[0].body.as_ref().unwrap()["size"], "25");
    assert_eq!(requests[0].body.as_ref().unwrap()["clientOid"], "Q1");
    assert!(requests[0].body.as_ref().unwrap().get("force").is_none());

    let sell_error = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-market-sell"),
            symbol: symbol_scope(),
            client_order_id: None,
            side: OrderSide::Sell,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect_err("spot sell quote market unsupported");
    assert!(matches!(sell_error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn cointr_adapter_should_parse_private_readbacks() {
    let (base_url, _seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"balances": [{"asset": "BTC", "free": "1.2", "locked": "0.3"}]}}),
        json!({"code": 0, "data": {"makerCommission": "0.001", "takerCommission": "0.0015"}}),
        json!({"code": 0, "data": {"orderId": "1001", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "status": "PARTIALLY_FILLED", "origQty": "0.02", "executedQty": "0.01", "price": "65000"}}),
        json!({"code": 0, "data": [{"orderId": "1002", "symbol": "BTCUSDT", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "0.03", "executedQty": "0", "price": "66000"}]}),
        json!({"code": 0, "data": [{"tradeId": "9001", "orderId": "1001", "symbol": "BTCUSDT", "side": "BUY", "price": "65000", "qty": "0.01", "commission": "0.1", "commissionAsset": "USDT", "isMaker": true}]}),
    ])
    .await;
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CointrGatewayConfig::default()
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
    assert_eq!(balances.balances.len(), 1);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.001");

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.status, OrderStatus::PartiallyFilled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

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
            limit: Some(10),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
}

#[tokio::test]
async fn cointr_private_get_balance_request_spec_should_match_signed_rest_shape() {
    let (base_url, seen) = spawn_rest_server(vec![serde_json::json!({
        "code": 0,
        "data": {"balances": [{"asset": "BTC", "free": "1.2", "locked": "0.3"}]}
    })])
    .await;
    let adapter = private_adapter(base_url);
    adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-spec"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    load_request_spec("request_specs/get_balances_spot.json")
        .assert_matches(&requests[0].to_actual_request())
        .expect("get balances request spec");
}

#[test]
fn cointr_capabilities_v2_should_declare_task18_toolchain_items() {
    let adapter = private_adapter("http://127.0.0.1:1".to_string());
    let capabilities = adapter.capabilities();
    assert!(matches!(
        capabilities.capabilities_v2.public_streams,
        CapabilitySupport::WsOnly { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert_eq!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat_policy
            .ping_interval_ms,
        15_000
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .auth_renewal_policy
            .resubscribe_after_renewal
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(super::toolchain::COINTR_MAX_BATCH_ITEMS)
    );
    assert!(capabilities.capabilities_v2.fills_history.supports_limit);
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(super::toolchain::COINTR_MAX_RECENT_FILL_LIMIT)
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "cointr.place_order"
            && endpoint.path.as_deref() == Some("/api/v2/spot/trade/place-order")));

    super::toolchain::cointr_rate_limit_plan()
        .validate()
        .expect("rate-limit plan validates");
    let reconcile = super::toolchain::cointr_reconcile_plan(
        exchange_id(),
        ReconcileTrigger::PrivateStreamDisconnected,
        Some(symbol_scope()),
        "test reconnect",
    );
    assert!(reconcile.requires_query_order);
    assert!(reconcile.requires_open_orders);
    assert!(reconcile.requires_recent_fills);
    assert!(!reconcile.allow_order_replay);
}

#[tokio::test]
async fn cointr_cancel_all_should_sweep_symbol_open_orders() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": [
            {"orderId": "1002", "symbol": "BTCUSDT", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "0.03", "executedQty": "0", "price": "66000"},
            {"orderId": "1003", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "status": "NEW", "origQty": "0.01", "executedQty": "0", "price": "64000"}
        ]}),
        json!({"code": 0, "data": {"orderId": "1002"}}),
        json!({"code": 0, "data": {"orderId": "1003"}}),
    ])
    .await;
    let adapter = CointrGatewayAdapter::new(CointrGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CointrGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(response.cancelled_count, 2);
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/api/v2/spot/trade/unfilled-orders");
    assert_eq!(requests[1].path, "/api/v2/spot/trade/cancel-order");
    assert_eq!(requests[2].path, "/api/v2/spot/trade/cancel-order");
}
