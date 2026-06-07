use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::private::{
    WooAlgoOrderAmendRequest, WooAlgoOrderQuery, WooAlgoOrderRequest, WooAlgoOrdersQuery,
};
use super::test_support::{
    assert_signed_woo_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    symbol_scope,
};
use super::{WooGatewayAdapter, WooGatewayConfig};
use crate::request_spec::RequestSpec;

#[tokio::test]
async fn woo_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        api_key: None,
        api_secret: None,
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
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
async fn woo_adapter_should_classify_error_fixture() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("error")]).await;
    let adapter = private_adapter(base_url);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-error"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("woo error fixture");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.class, ExchangeErrorClass::Authentication);
            assert_eq!(error.code.as_deref(), Some("1002"));
            assert_eq!(error.message, "invalid api key");
        }
        other => panic!("expected exchange error, got {other:?}"),
    }
}

#[tokio::test]
async fn woo_adapter_should_route_private_rest_readbacks_with_signed_headers() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balance"),
        fixture("order"),
        fixture("open_orders"),
        fixture("fees"),
        fixture("fills"),
        fixture("positions"),
    ])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_positions);

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
        .expect("query")
        .order
        .expect("order");
    assert_eq!(queried.status, OrderStatus::Filled);
    assert_eq!(queried.client_order_id.as_deref(), Some("1001001"));

    let open_orders = adapter
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
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(open_orders.orders[0].status, OrderStatus::New);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0008");

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
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.7));

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].quantity, 1.25);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 6);
    assert_signed_woo_request(&requests[0], "GET", "/v3/asset/balances");
    load_request_spec("get_balances.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_signed_woo_request(&requests[1], "GET", "/v3/trade/order");
    assert_eq!(
        requests[1].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    assert_signed_woo_request(&requests[2], "GET", "/v3/trade/orders");
    assert_eq!(
        requests[2].query.get("status").map(String::as_str),
        Some("INCOMPLETE")
    );
    assert_signed_woo_request(&requests[3], "GET", "/v3/trade/tradingFee");
    assert_signed_woo_request(&requests[4], "GET", "/v3/trade/transactionHistory");
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("25")
    );
    load_request_spec("recent_fills.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("request spec");
    assert_signed_woo_request(&requests[5], "GET", "/v3/futures/positions");
}

#[tokio::test]
async fn woo_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"success": true, "data": {"orderId": 2001, "clientOrderId": 2001001}}),
        json!({"success": true, "data": {"orderId": 2002, "clientOrderId": 2002002}}),
        json!({"success": true, "data": {"status": "CANCELLED"}}),
        json!({"success": true, "data": {"status": "CANCELLED"}}),
        json!({"success": true, "data": {"orderId": 2004, "clientOrderId": 2004004}}),
    ])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("2001001".to_string()),
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
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: symbol_scope(),
            client_order_id: Some("2002002".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote");
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

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
    assert_eq!(cancel_all.cancelled_count, 1);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope(),
            client_order_id: Some("2004004".to_string()),
            exchange_order_id: Some("2004".to_string()),
            new_client_order_id: None,
            new_quantity: "0.015".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2004"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_woo_request(&requests[0], "POST", "/v3/trade/order");
    load_request_spec("place_order.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["symbol"], "SPOT_BTC_USDT");
    assert_eq!(body["side"], "BUY");
    assert_eq!(body["type"], "LIMIT");
    assert_eq!(body["quantity"], "0.02");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["clientOrderId"], "2001001");

    assert_signed_woo_request(&requests[1], "POST", "/v3/trade/order");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["type"], "MARKET");
    assert_eq!(body["amount"], "25.5");

    assert_signed_woo_request(&requests[2], "DELETE", "/v3/trade/order");
    assert_eq!(
        requests[2].query.get("orderId").map(String::as_str),
        Some("2001")
    );
    load_request_spec("cancel_order.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("request spec");
    assert_signed_woo_request(&requests[3], "DELETE", "/v3/trade/allOrders");
    assert_eq!(
        requests[3].query.get("symbol").map(String::as_str),
        Some("SPOT_BTC_USDT")
    );
    assert_signed_woo_request(&requests[4], "PUT", "/v3/trade/order");
    let body = requests[4].body.as_ref().expect("amend body");
    assert_eq!(body["quantity"], "0.015");
    assert_eq!(body["orderId"], "2004");
}

#[tokio::test]
async fn woo_adapter_should_route_batch_orders_as_composed_v3_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"success": true, "data": {"orderId": 3001, "clientOrderId": 3001001}}),
        json!({"success": true, "data": {"orderId": 3002, "clientOrderId": 3002002}}),
        json!({"success": true, "data": {"status": "CANCELLED", "orderId": 3001}}),
        json!({"success": true, "data": {"status": "CANCELLED", "orderId": 3002}}),
    ])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);

    let first = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: symbol_scope(),
        client_order_id: Some("3001001".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let mut second = first.clone();
    second.context = context("batch-place-2");
    second.client_order_id = Some("3002002".to_string());
    second.side = OrderSide::Sell;
    second.price = Some("66000".to_string());

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![first, second],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("3001"));
    assert_eq!(placed.orders[1].exchange_order_id.as_deref(), Some("3002"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);
    assert_eq!(cancelled.orders.len(), 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_signed_woo_request(&requests[0], "POST", "/v3/trade/order");
    load_request_spec("batch_place_orders.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[0].body.as_ref().expect("first body")["symbol"],
        "SPOT_BTC_USDT"
    );
    assert_signed_woo_request(&requests[1], "POST", "/v3/trade/order");
    assert_signed_woo_request(&requests[2], "DELETE", "/v3/trade/order");
    assert_eq!(
        requests[2].query.get("orderId").map(String::as_str),
        Some("3001")
    );
    load_request_spec("batch_cancel_orders.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("request spec");
    assert_signed_woo_request(&requests[3], "DELETE", "/v3/trade/order");
    assert_eq!(
        requests[3].query.get("orderId").map(String::as_str),
        Some("3002")
    );
}

#[tokio::test]
async fn woo_adapter_should_route_advanced_private_v3_controls() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"success": true, "timestamp": 1, "data": {"expectedTriggerTime": 123456}}),
        json!({"success": true, "timestamp": 2}),
        json!({"success": true, "timestamp": 3}),
        json!({"success": true, "timestamp": 4}),
        json!({"success": true, "timestamp": 5, "data": {"rows": []}}),
    ])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .set_cancel_all_after(60_000)
        .await
        .expect("cancel all after");
    adapter
        .set_futures_leverage(perp_symbol_scope(), "isolated", 5, Some("hedge"))
        .await
        .expect("set leverage");
    adapter
        .set_position_mode("one_way")
        .await
        .expect("set position mode");
    adapter
        .set_account_trading_mode("futures")
        .await
        .expect("set trading mode");
    adapter
        .get_default_margin_mode(Some(perp_symbol_scope()))
        .await
        .expect("get margin mode");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_woo_request(&requests[0], "POST", "/v3/trade/cancelAllAfter");
    assert_eq!(
        requests[0].body.as_ref().expect("cancel all after body")["triggerAfter"],
        60_000
    );
    assert_signed_woo_request(&requests[1], "PUT", "/v3/futures/leverage");
    let leverage = requests[1].body.as_ref().expect("leverage body");
    assert_eq!(leverage["symbol"], "PERP_BTC_USDT");
    assert_eq!(leverage["marginMode"], "ISOLATED");
    assert_eq!(leverage["leverage"], 5);
    assert_eq!(leverage["positionMode"], "HEDGE_MODE");
    assert_signed_woo_request(&requests[2], "PUT", "/v3/futures/positionMode");
    assert_eq!(
        requests[2].body.as_ref().expect("position mode body")["positionMode"],
        "ONE_WAY"
    );
    assert_signed_woo_request(&requests[3], "POST", "/v3/account/tradingMode");
    assert_eq!(
        requests[3].body.as_ref().expect("trading mode body")["tradingMode"],
        "FUTURES"
    );
    assert_signed_woo_request(&requests[4], "GET", "/v3/futures/defaultMarginMode");
    assert_eq!(
        requests[4].query.get("symbol").map(String::as_str),
        Some("PERP_BTC_USDT")
    );
}

#[tokio::test]
async fn woo_adapter_should_route_private_v3_algo_orders() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"success": true, "timestamp": 1, "data": {"algoOrderId": "9001"}}),
        json!({"success": true, "timestamp": 2, "data": {"algoOrderId": "9001"}}),
        json!({"success": true, "timestamp": 3, "data": {"algoOrderId": "9001", "status": "NEW"}}),
        json!({"success": true, "timestamp": 4, "data": {"algoOrderId": "9001", "status": "CANCELLED"}}),
        json!({"success": true, "timestamp": 5, "data": {"rows": []}}),
        json!({"success": true, "timestamp": 6, "data": {"rows": []}}),
    ])
    .await;
    let adapter = WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .place_algo_order(WooAlgoOrderRequest {
            symbol: perp_symbol_scope(),
            side: OrderSide::Sell,
            order_type: OrderType::StopLimit,
            quantity: "0.5".to_string(),
            price: Some("59000".to_string()),
            trigger_price: "59500".to_string(),
            trigger_price_type: Some("mark".to_string()),
            client_order_id: Some("algo-cid-1".to_string()),
            reduce_only: true,
            position_side: Some(rustcta_types::PositionSide::Long),
        })
        .await
        .expect("place algo");
    adapter
        .amend_algo_order(WooAlgoOrderAmendRequest {
            symbol: perp_symbol_scope(),
            algo_order_id: Some("9001".to_string()),
            client_order_id: None,
            new_quantity: Some("0.4".to_string()),
            new_price: Some("58900".to_string()),
            new_trigger_price: Some("59400".to_string()),
        })
        .await
        .expect("amend algo");
    adapter
        .query_algo_order(WooAlgoOrderQuery {
            symbol: perp_symbol_scope(),
            algo_order_id: Some("9001".to_string()),
            client_order_id: None,
        })
        .await
        .expect("query algo");
    adapter
        .cancel_algo_order(WooAlgoOrderQuery {
            symbol: perp_symbol_scope(),
            algo_order_id: None,
            client_order_id: Some("algo-cid-1".to_string()),
        })
        .await
        .expect("cancel algo");
    adapter
        .get_algo_orders(WooAlgoOrdersQuery {
            symbol: Some(perp_symbol_scope()),
            order_type: Some(OrderType::StopLimit),
            status: Some("INCOMPLETE".to_string()),
            limit: Some(10),
        })
        .await
        .expect("get algo orders");
    adapter
        .cancel_algo_orders(WooAlgoOrdersQuery {
            symbol: Some(perp_symbol_scope()),
            order_type: Some(OrderType::StopLimit),
            status: Some("INCOMPLETE".to_string()),
            limit: Some(10),
        })
        .await
        .expect("cancel algo orders");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 6);
    assert_signed_woo_request(&requests[0], "POST", "/v3/trade/algoOrder");
    let place = requests[0].body.as_ref().expect("place algo body");
    assert_eq!(place["symbol"], "PERP_BTC_USDT");
    assert_eq!(place["side"], "SELL");
    assert_eq!(place["type"], "STOP_LIMIT");
    assert_eq!(place["quantity"], "0.5");
    assert_eq!(place["price"], "59000");
    assert_eq!(place["triggerPrice"], "59500");
    assert_eq!(place["triggerPriceType"], "MARK_PRICE");
    assert_eq!(place["clientOrderId"], "algo-cid-1");
    assert_eq!(place["reduceOnly"], true);
    assert_eq!(place["positionSide"], "LONG");

    assert_signed_woo_request(&requests[1], "PUT", "/v3/trade/algoOrder");
    let amend = requests[1].body.as_ref().expect("amend algo body");
    assert_eq!(amend["algoOrderId"], "9001");
    assert_eq!(amend["quantity"], "0.4");
    assert_eq!(amend["price"], "58900");
    assert_eq!(amend["triggerPrice"], "59400");

    assert_signed_woo_request(&requests[2], "GET", "/v3/trade/algoOrder");
    assert_eq!(
        requests[2].query.get("algoOrderId").map(String::as_str),
        Some("9001")
    );
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("PERP_BTC_USDT")
    );
    assert_signed_woo_request(&requests[3], "DELETE", "/v3/trade/algoOrder");
    assert_eq!(
        requests[3].query.get("clientOrderId").map(String::as_str),
        Some("algo-cid-1")
    );
    assert_signed_woo_request(&requests[4], "GET", "/v3/trade/algoOrders");
    assert_eq!(
        requests[4].query.get("type").map(String::as_str),
        Some("STOP_LIMIT")
    );
    assert_eq!(
        requests[4].query.get("status").map(String::as_str),
        Some("INCOMPLETE")
    );
    assert_signed_woo_request(&requests[5], "DELETE", "/v3/trade/algoOrders");
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "balance" => include_str!("../../../../../tests/fixtures/exchanges/woo/balance.json"),
        "order" => include_str!("../../../../../tests/fixtures/exchanges/woo/order.json"),
        "open_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/woo/open_orders.json")
        }
        "fees" => include_str!("../../../../../tests/fixtures/exchanges/woo/fees.json"),
        "fills" => include_str!("../../../../../tests/fixtures/exchanges/woo/fills.json"),
        "positions" => include_str!("../../../../../tests/fixtures/exchanges/woo/positions.json"),
        "error" => include_str!("../../../../../tests/fixtures/exchanges/woo/error.json"),
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}

fn private_adapter(base_url: String) -> WooGatewayAdapter {
    WooGatewayAdapter::new(WooGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..WooGatewayConfig::default()
    })
    .expect("adapter")
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/woo/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}
