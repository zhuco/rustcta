use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType, OrderListRequest,
    PlaceOrderRequest, PositionMode, PositionsRequest, QueryOrderRequest, QuoteMarketOrderRequest,
    RecentFillsRequest, SymbolAccountConfigRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::signing::sign_raw_query;
use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id,
    perpetual_symbol_scope, private_config, spawn_rest_server, symbol_scope,
};
use super::BinanceGatewayAdapter;
use crate::adapters::GatewayAdapter;

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

#[test]
fn binance_signing_vector_fixture_should_verify() {
    let vector = load_signing_vector("binance/signing_vectors/place_order_limit.json");
    vector.verify().expect("fixture signature");
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
    load_request_spec("binance/request_specs/get_balances.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.path, "/api/v3/account");
    assert_signed_request(&request);
}

#[tokio::test]
async fn binance_adapter_should_fallback_to_portfolio_balances_for_unified_account() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            {"asset": "USDT", "balance": "0", "availableBalance": "0"}
        ]),
        json!({
            "totalEquity": "156.70",
            "totalWalletBalance": "156.12"
        }),
    ])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("portfolio-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("portfolio balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(response.balances[0].balances[0].asset, "USDT");
    assert_eq!(response.balances[0].balances[0].total, 156.70);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/fapi/v2/balance");
    assert_eq!(requests[1].path, "/sapi/v1/portfolio/account");
    assert_signed_request(&requests[0]);
    assert_signed_request(&requests[1]);
}

#[tokio::test]
async fn binance_adapter_should_treat_omitted_balance_market_type_as_account_level() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "accountEquity": "42.50",
        "totalAvailableBalance": "40.25"
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("account-level-balances"),
            exchange: exchange_id(),
            market_type: None,
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("account-level balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(response.balances[0].balances[0].asset, "USDT");
    assert_eq!(response.balances[0].balances[0].total, 42.50);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/sapi/v1/portfolio/account");
    assert_signed_request(&requests[0]);
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
    load_request_spec("binance/request_specs/query_order.json")
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
async fn binance_adapter_should_route_private_order_mutations() {
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
    let amend_ack = json!({
        "symbol": "BTCUSDT",
        "orderId": 33,
        "clientOrderId": "cli-amend",
        "price": "27000.00000000",
        "origQty": "0.00500000",
        "executedQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "SELL",
        "updateTime": 1499827319563_i64
    });
    let oco_ack = json!({
        "orderListId": 7,
        "listClientOrderId": "cli-list",
        "listStatusType": "EXEC_STARTED",
        "listOrderStatus": "EXECUTING",
        "symbol": "BTCUSDT",
        "orderReports": [{
            "symbol": "BTCUSDT",
            "orderId": 34,
            "clientOrderId": "cli-above",
            "price": "28000.00000000",
            "origQty": "0.01000000",
            "executedQty": "0.00000000",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT_MAKER",
            "side": "SELL",
            "transactTime": 1499827319564_i64
        }, {
            "symbol": "BTCUSDT",
            "orderId": 35,
            "clientOrderId": "cli-below",
            "price": "24000.00000000",
            "origQty": "0.01000000",
            "executedQty": "0.00000000",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "STOP_LOSS_LIMIT",
            "side": "SELL",
            "transactTime": 1499827319564_i64
        }]
    });
    let (base_url, seen) = spawn_rest_server(vec![
        order_ack,
        quote_ack,
        cancel_ack,
        cancel_all_ack,
        amend_ack,
        oco_ack,
    ])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(capabilities.supports_amend_order);
    assert!(capabilities.supports_order_list);

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

    let amended = adapter
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
        .expect("amend order");
    assert_eq!(amended.order.quantity, "0.00500000");

    let order_list = adapter
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
        .expect("order list");
    assert_eq!(order_list.order_list_id.as_deref(), Some("7"));
    assert_eq!(order_list.orders.len(), 2);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/api/v3/order");
    load_request_spec("binance/request_specs/place_order.json")
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
    load_request_spec("binance/request_specs/place_quote_market_order.json")
        .assert_matches(&seen[1].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[1], "POST");
    assert_eq!(
        seen[1].query.get("quoteOrderQty").map(String::as_str),
        Some("25.50")
    );

    assert_eq!(seen[2].path, "/api/v3/order");
    load_request_spec("binance/request_specs/cancel_order.json")
        .assert_matches(&seen[2].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[2], "DELETE");
    assert_eq!(seen[2].query.get("orderId").map(String::as_str), Some("30"));
    assert_eq!(
        seen[2].query.get("origClientOrderId").map(String::as_str),
        Some("cli-place")
    );

    assert_eq!(seen[3].path, "/api/v3/openOrders");
    load_request_spec("binance/request_specs/cancel_all_orders.json")
        .assert_matches(&seen[3].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[3], "DELETE");

    assert_eq!(seen[4].path, "/api/v3/order/amend/keepPriority");
    load_request_spec("binance/request_specs/amend_order.json")
        .assert_matches(&seen[4].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[4], "PUT");
    assert_eq!(
        seen[4].query.get("quantity").map(String::as_str),
        Some("0.00500000")
    );
    assert_eq!(
        seen[4].query.get("newClientOrderId").map(String::as_str),
        Some("cli-amend")
    );

    assert_eq!(seen[5].path, "/api/v3/orderList/oco");
    load_request_spec("binance/request_specs/place_order_list.json")
        .assert_matches(&seen[5].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[5], "POST");
    assert_eq!(
        seen[5].query.get("aboveType").map(String::as_str),
        Some("LIMIT_MAKER")
    );
    assert_eq!(
        seen[5].query.get("belowStopPrice").map(String::as_str),
        Some("24500.00000000")
    );
}

#[tokio::test]
async fn binance_adapter_should_route_perpetual_place_cancel_and_positions() {
    let place_ack = json!({
        "symbol": "BTCUSDT",
        "orderId": 91,
        "clientOrderId": "perp-place",
        "price": "65000.00000000",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "IOC",
        "type": "LIMIT",
        "side": "SELL",
        "positionSide": "SHORT",
        "reduceOnly": true,
        "updateTime": 1700000000000_i64
    });
    let cancel_ack = json!({
        "symbol": "BTCUSDT",
        "orderId": 91,
        "clientOrderId": "perp-place",
        "price": "65000.00000000",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "CANCELED",
        "timeInForce": "IOC",
        "type": "LIMIT",
        "side": "SELL",
        "positionSide": "SHORT",
        "reduceOnly": true,
        "updateTime": 1700000000100_i64
    });
    let positions = json!([{
        "symbol": "BTCUSDT",
        "positionAmt": "-0.02000000",
        "entryPrice": "64000.00000000",
        "markPrice": "65000.00000000",
        "unRealizedProfit": "20.00000000",
        "positionSide": "SHORT",
        "leverage": "10",
        "updateTime": 1700000000200_i64
    }]);
    let (base_url, seen) = spawn_rest_server(vec![place_ack, cancel_ack, positions]).await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = perpetual_symbol_scope("BTCUSDT");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: symbol.clone(),
            client_order_id: Some("perp-place".to_string()),
            side: OrderSide::Sell,
            position_side: Some(PositionSide::Short),
            order_type: OrderType::IOC,
            time_in_force: None,
            quantity: "0.01000000".to_string(),
            price: Some("65000.00000000".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("place perpetual order");
    assert_eq!(placed.order.market_type, MarketType::Perpetual);
    assert_eq!(placed.order.position_side, Some(PositionSide::Short));
    assert!(placed.order.reduce_only);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-cancel"),
            symbol: symbol.clone(),
            client_order_id: Some("perp-place".to_string()),
            exchange_order_id: Some("91".to_string()),
        })
        .await
        .expect("cancel perpetual order");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.market_type, MarketType::Perpetual);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![symbol.exchange_symbol.clone()],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].market_type, MarketType::Perpetual);
    assert_eq!(positions.positions[0].side, PositionSide::Short);
    assert_eq!(positions.positions[0].quantity, 0.02);
    assert_eq!(positions.positions[0].entry_price, Some(64000.0));

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/fapi/v1/order");
    assert_signed_request_method(&seen[0], "POST");
    assert_eq!(
        seen[0].query.get("positionSide").map(String::as_str),
        Some("SHORT")
    );
    assert_eq!(seen[0].query.get("reduceOnly"), None);
    assert_eq!(seen[1].path, "/fapi/v1/order");
    assert_signed_request_method(&seen[1], "DELETE");
    assert_eq!(
        seen[1].query.get("origClientOrderId").map(String::as_str),
        Some("perp-place")
    );
    assert_eq!(seen[2].path, "/fapi/v2/positionRisk");
    assert_signed_request(&seen[2]);
    assert_eq!(
        seen[2].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
}

#[tokio::test]
async fn binance_adapter_should_route_usdm_batch_place_and_cancel_orders() {
    let batch_place_ack = json!([
        {
            "symbol": "BTCUSDT",
            "orderId": 101,
            "clientOrderId": "batch-a",
            "price": "65000.00000000",
            "origQty": "0.01000000",
            "executedQty": "0.00000000",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "positionSide": "BOTH",
            "reduceOnly": false,
            "updateTime": 1700000000300_i64
        },
        {
            "code": -2022,
            "msg": "ReduceOnly Order is rejected."
        }
    ]);
    let batch_cancel_ack = json!([
        {
            "symbol": "BTCUSDT",
            "orderId": 101,
            "clientOrderId": "batch-a",
            "price": "65000.00000000",
            "origQty": "0.01000000",
            "executedQty": "0.00000000",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "positionSide": "BOTH",
            "reduceOnly": false,
            "updateTime": 1700000000400_i64
        },
        {
            "symbol": "BTCUSDT",
            "orderId": 102,
            "clientOrderId": "batch-b",
            "price": "64000.00000000",
            "origQty": "0.02000000",
            "executedQty": "0.00000000",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL",
            "positionSide": "BOTH",
            "reduceOnly": false,
            "updateTime": 1700000000401_i64
        }
    ]);
    let (base_url, seen) = spawn_rest_server(vec![batch_place_ack, batch_cancel_ack]).await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = perpetual_symbol_scope("BTCUSDT");

    let batch_placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-a"),
                    symbol: symbol.clone(),
                    client_order_id: Some("batch-a".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01000000".to_string(),
                    price: Some("65000.00000000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-b"),
                    symbol: symbol.clone(),
                    client_order_id: Some("batch-b".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02000000".to_string(),
                    price: Some("64000.00000000".to_string()),
                    quote_quantity: None,
                    reduce_only: true,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place orders");
    assert_eq!(batch_placed.orders.len(), 1);
    assert_eq!(
        batch_placed.orders[0].exchange_order_id.as_deref(),
        Some("101")
    );
    let place_report = batch_placed.report.as_ref().expect("batch place report");
    assert_eq!(place_report.total_items, 2);
    assert_eq!(place_report.succeeded_count(), 1);
    assert_eq!(place_report.failed_count(), 1);
    assert_eq!(
        place_report.results[1].client_order_id.as_deref(),
        Some("batch-b")
    );
    assert_eq!(
        place_report.results[1]
            .error
            .as_ref()
            .and_then(|error| error.code.as_deref()),
        Some("-2022")
    );

    let batch_cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-a"),
                    symbol: symbol.clone(),
                    client_order_id: None,
                    exchange_order_id: Some("101".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-b"),
                    symbol: symbol.clone(),
                    client_order_id: None,
                    exchange_order_id: Some("102".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel orders");
    assert_eq!(batch_cancelled.cancelled_count, 2);
    assert_eq!(batch_cancelled.orders.len(), 2);
    assert_eq!(
        batch_cancelled
            .report
            .as_ref()
            .expect("batch cancel report")
            .succeeded_count(),
        2
    );

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/fapi/v1/batchOrders");
    load_request_spec("binance/request_specs/futures_batch_place_orders.json")
        .assert_matches(&seen[0].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[0], "POST");
    let batch_orders = seen[0].query.get("batchOrders").expect("batchOrders");
    let decoded_batch_orders = urlencoding::decode(batch_orders)
        .expect("decode batchOrders")
        .into_owned();
    assert!(decoded_batch_orders.contains("\"symbol\":\"BTCUSDT\""));
    assert!(decoded_batch_orders.contains("\"newClientOrderId\":\"batch-a\""));
    assert!(decoded_batch_orders.contains("\"reduceOnly\":\"true\""));

    assert_eq!(seen[1].path, "/fapi/v1/batchOrders");
    load_request_spec("binance/request_specs/futures_batch_cancel_orders.json")
        .assert_matches(&seen[1].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[1], "DELETE");
    let order_ids = seen[1].query.get("orderIdList").expect("orderIdList");
    assert_eq!(
        urlencoding::decode(order_ids)
            .expect("decode orderIdList")
            .as_ref(),
        "[101,102]"
    );
}

#[tokio::test]
async fn binance_batch_orders_should_reject_unsupported_market_boundaries() {
    let adapter = BinanceGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");
    let spot_order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("spot-batch-place"),
        symbol: symbol_scope("BTCUSDT"),
        client_order_id: Some("spot-batch".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01000000".to_string(),
        price: Some("25000.00000000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-place"),
            exchange: exchange_id(),
            orders: vec![spot_order],
        })
        .await
        .expect_err("spot batch place unsupported");
    assert!(matches!(place_error, ExchangeApiError::Unsupported { .. }));

    let cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("mixed-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-btc"),
                    symbol: perpetual_symbol_scope("BTCUSDT"),
                    client_order_id: None,
                    exchange_order_id: Some("101".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-eth"),
                    symbol: perpetual_symbol_scope("ETHUSDT"),
                    client_order_id: None,
                    exchange_order_id: Some("102".to_string()),
                },
            ],
        })
        .await
        .expect_err("mixed symbol batch cancel rejected");
    assert!(matches!(
        cancel_error,
        ExchangeApiError::InvalidRequest { .. }
    ));
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
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    assert_eq!(response.orders[0].exchange_order_id.as_deref(), Some("29"));
    assert_eq!(response.orders[0].status, OrderStatus::New);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("binance/request_specs/get_open_orders.json")
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
    load_request_spec("binance/request_specs/get_fees.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
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
    load_request_spec("binance/request_specs/get_recent_fills.json")
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

#[tokio::test]
async fn binance_adapter_should_load_perpetual_position_mode_from_signed_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "dualSidePosition": true
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_symbol_account_config(SymbolAccountConfigRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("position-mode"),
            symbol: perpetual_symbol_scope("BTCUSDT"),
        })
        .await
        .expect("position mode");

    assert_eq!(response.config.position_mode, Some(PositionMode::Hedge));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/fapi/v1/positionSide/dual");
    assert_signed_request(&request);
}
