use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, CancelAllOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest, OrderListConditionalLeg,
    OrderListLegType, OrderListRequest, PlaceOrderRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::signing::sign_raw_query;
use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id, private_config,
    spawn_rest_server, symbol_scope,
};
use super::BinanceUsGatewayAdapter;

#[test]
fn binanceus_signing_should_match_documented_example() {
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

#[test]
fn binanceus_signing_vector_fixture_should_verify() {
    let vector = load_signing_vector("binanceus/signing_vectors/place_order_limit.json");
    vector.verify().expect("fixture signature");
}

#[tokio::test]
async fn binanceus_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BinanceUsGatewayAdapter::default_public().expect("adapter");
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
async fn binanceus_adapter_should_load_balances_from_signed_account_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "balances": [
            {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"},
            {"asset": "ETH", "free": "0.00000000", "locked": "0.00000000"}
        ]
    })])
    .await;
    let adapter = BinanceUsGatewayAdapter::new(private_config(base_url)).expect("adapter");

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
    load_request_spec("binanceus/request_specs/get_balances.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.path, "/api/v3/account");
    assert_signed_request(&request);
}

#[tokio::test]
async fn binanceus_adapter_should_load_fees_from_signed_trading_fee_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([{
        "symbol": "BTCUSDT",
        "makerCommission": "0.00100000",
        "takerCommission": "0.00100000"
    }])])
    .await;
    let adapter = BinanceUsGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTCUSDT")],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "0.00100000");
    assert_eq!(response.fees[0].taker_rate, "0.00100000");
    assert_eq!(
        response.fees[0].source.as_deref(),
        Some("binanceus.trading_fee")
    );
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("binanceus/request_specs/get_fees.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.path, "/sapi/v1/asset/query/trading-fee");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_request(&request);
}

#[tokio::test]
async fn binanceus_adapter_should_query_order_from_signed_rest() {
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
    let adapter = BinanceUsGatewayAdapter::new(private_config(base_url)).expect("adapter");

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
    load_request_spec("binanceus/request_specs/query_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
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
async fn binanceus_adapter_should_route_supported_private_order_mutations() {
    let order_ack = json!({
        "symbol": "BTCUSDT",
        "orderId": 30,
        "clientOrderId": "cli-place",
        "price": "25000.00000000",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "transactTime": 1499827319559_i64
    });
    let quote_ack = json!({
        "symbol": "BTCUSDT",
        "orderId": 31,
        "clientOrderId": "cli-quote",
        "price": "0.00000000",
        "origQty": "0.00000000",
        "executedQty": "0.00100000",
        "cummulativeQuoteQty": "25.50000000",
        "status": "FILLED",
        "type": "MARKET",
        "side": "BUY",
        "transactTime": 1499827319560_i64
    });
    let cancel_ack = json!({
        "symbol": "BTCUSDT",
        "orderId": 30,
        "clientOrderId": "cli-place",
        "price": "25000.00000000",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "CANCELED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "updateTime": 1499827319561_i64
    });
    let cancel_all_ack = json!([{
        "symbol": "BTCUSDT",
        "orderId": 32,
        "clientOrderId": "cli-open",
        "price": "26000.00000000",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "CANCELED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "SELL",
        "updateTime": 1499827319562_i64
    }]);
    let (base_url, seen) =
        spawn_rest_server(vec![order_ack, quote_ack, cancel_ack, cancel_all_ack]).await;
    let adapter = BinanceUsGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-order"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: Some("cli-place".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01000000".to_string(),
            price: Some("25000.00000000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("30"));

    let quoted = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: Some("cli-quote".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.50".to_string(),
        })
        .await
        .expect("quote order");
    assert_eq!(quoted.order.status, OrderStatus::Filled);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-order"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: Some("cli-place".to_string()),
            exchange_order_id: Some("30".to_string()),
        })
        .await
        .expect("cancel order");
    assert!(cancelled.cancelled);

    let cancelled_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTCUSDT")),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancelled_all.cancelled_count, 1);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/api/v3/order");
    load_request_spec("binanceus/request_specs/place_order.json")
        .assert_matches(&seen[0].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[0], "POST");
    assert_eq!(seen[0].query.get("type").map(String::as_str), Some("LIMIT"));
    assert_eq!(
        seen[0].query.get("timeInForce").map(String::as_str),
        Some("GTC")
    );
    assert_eq!(
        seen[0].query.get("newClientOrderId").map(String::as_str),
        Some("cli-place")
    );

    assert_eq!(seen[1].path, "/api/v3/order");
    load_request_spec("binanceus/request_specs/place_quote_market_order.json")
        .assert_matches(&seen[1].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[1], "POST");
    assert_eq!(
        seen[1].query.get("quoteOrderQty").map(String::as_str),
        Some("25.50")
    );

    assert_eq!(seen[2].path, "/api/v3/order");
    load_request_spec("binanceus/request_specs/cancel_order.json")
        .assert_matches(&seen[2].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[2], "DELETE");
    assert_eq!(seen[2].query.get("orderId").map(String::as_str), Some("30"));
    assert_eq!(
        seen[2].query.get("origClientOrderId").map(String::as_str),
        Some("cli-place")
    );

    assert_eq!(seen[3].path, "/api/v3/openOrders");
    load_request_spec("binanceus/request_specs/cancel_all_orders.json")
        .assert_matches(&seen[3].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[3], "DELETE");

    assert_eq!(seen.len(), 4);
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn load_signing_vector(path: &str) -> SigningVector {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector fixture")
}

#[tokio::test]
async fn binanceus_adapter_should_load_open_orders_from_signed_rest() {
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
    let adapter = BinanceUsGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTC-USDT")),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    assert_eq!(response.orders[0].exchange_order_id.as_deref(), Some("29"));
    assert_eq!(response.orders[0].status, OrderStatus::New);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("binanceus/request_specs/get_open_orders.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.path, "/api/v3/openOrders");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_request(&request);
}

#[tokio::test]
async fn binanceus_adapter_should_keep_unmapped_profile_operations_unsupported() {
    let adapter = BinanceUsGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: None,
            exchange_order_id: Some("33".to_string()),
            new_client_order_id: Some("cli-amend".to_string()),
            new_quantity: "0.00500000".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(amend_error, ExchangeApiError::Unsupported { .. }));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol_scope("BTCUSDT"),
            list_client_order_id: Some("cli-list".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01000000".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("28000.00000000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("cli-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("24000.00000000".to_string()),
                stop_price: Some("24500.00000000".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("cli-below".to_string()),
            },
        })
        .await
        .expect_err("order list unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported { .. }
    ));
}

#[tokio::test]
async fn binanceus_adapter_should_load_recent_fills_from_signed_rest() {
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
    let adapter = BinanceUsGatewayAdapter::new(private_config(base_url)).expect("adapter");

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
            page: None,
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
    load_request_spec("binanceus/request_specs/get_recent_fills.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
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
