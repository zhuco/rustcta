use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, PlaceOrderRequest,
    PositionsRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderType, PositionSide};
use serde_json::{json, Value};

use super::test_support::{
    assert_signed_toobit_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    spot_symbol_scope,
};
use super::{ToobitGatewayAdapter, ToobitGatewayConfig};
use crate::request_spec::RequestSpec;

#[tokio::test]
async fn toobit_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = ToobitGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);
    assert!(!adapter.capabilities().supports_batch_place_order);

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
async fn toobit_adapter_should_classify_error_fixture() {
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
        .expect_err("toobit error fixture");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.class, ExchangeErrorClass::Authentication);
            assert_eq!(error.code.as_deref(), Some("-1022"));
            assert!(error.message.contains("Signature"));
        }
        other => panic!("expected exchange error, got {other:?}"),
    }
}

#[tokio::test]
async fn toobit_adapter_should_sign_private_reads_and_writes() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"balances": [{"coin": "BTC", "total": "0.2", "free": "0.1", "locked": "0.1"}]}}),
        json!({"code": 0, "data": [{"symbol": "BTC-SWAP-USDT", "positionSide": "LONG", "positionAmt": "0.5", "entryPrice": "64000", "leverage": "5"}]}),
        json!({"code": 0, "data": {"symbol": "BTC-SWAP-USDT", "makerCommission": "0.0002", "takerCommission": "0.0005"}}),
        json!({"code": 0, "data": {"symbol": "BTCUSDT", "orderId": "1001", "clientOrderId": "client-1", "side": "BUY", "type": "LIMIT", "status": "NEW", "origQty": "0.01", "price": "65000"}}),
        json!({"code": 0, "data": {"symbol": "BTCUSDT", "orderId": "1001", "clientOrderId": "client-1", "side": "BUY", "type": "LIMIT", "status": "CANCELED", "origQty": "0.01", "price": "65000"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

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
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");

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
    assert_eq!(positions.positions[0].side, PositionSide::Long);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0002");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("client-1".to_string()),
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
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_signed_toobit_request(&requests[0], "GET", "/api/v1/account");
    load_request_spec("get_balances.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_signed_toobit_request(&requests[1], "GET", "/api/v1/futures/positions");
    assert_signed_toobit_request(&requests[2], "GET", "/api/v1/futures/commissionRate");
    assert_signed_toobit_request(&requests[3], "POST", "/api/v1/spot/order");
    load_request_spec("place_order.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[3]
            .query
            .get("newClientOrderId")
            .map(String::as_str),
        Some("client-1")
    );
    assert_signed_toobit_request(&requests[4], "DELETE", "/api/v1/spot/order");
    load_request_spec("cancel_order.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn toobit_adapter_should_route_native_batch_and_perp_amend() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": [
            {"symbol": "BTC-SWAP-USDT", "orderId": "2001", "clientOrderId": "p1", "side": "BUY", "type": "LIMIT", "status": "NEW", "origQty": "0.01", "price": "65000"},
            {"symbol": "BTC-SWAP-USDT", "orderId": "2002", "clientOrderId": "p2", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "0.02", "price": "66000"}
        ]}),
        json!({"code": 0, "data": [
            {"symbol": "BTC-SWAP-USDT", "orderId": "2001", "side": "BUY", "type": "LIMIT", "status": "CANCELED", "origQty": "0.01"},
            {"symbol": "BTC-SWAP-USDT", "orderId": "2002", "side": "SELL", "type": "LIMIT", "status": "CANCELED", "origQty": "0.02"}
        ]}),
        json!({"code": 0, "data": {"symbol": "BTC-SWAP-USDT", "orderId": "2001", "clientOrderId": "p1", "side": "BUY", "type": "LIMIT", "status": "NEW", "origQty": "0.03", "price": "65000"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let first = perp_order("p1", OrderSide::Buy, "0.01", "65000");
    let second = perp_order("p2", OrderSide::Sell, "0.02", "66000");

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![first, second],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders.len(), 2);

    let cancel_batch = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("2001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("2002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancel_batch.cancelled_count, 2);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.03".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.quantity, "0.03");

    let requests = seen.lock().unwrap().clone();
    assert_signed_toobit_request(&requests[0], "POST", "/api/v2/futures/batch-orders");
    load_request_spec("batch_place_orders.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert!(requests[0]
        .query
        .get("orders")
        .is_some_and(|orders| orders.contains("BTC-SWAP-USDT")));
    assert_signed_toobit_request(&requests[1], "DELETE", "/api/v1/futures/cancelOrderByIds");
    load_request_spec("batch_cancel_orders.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[1].query.get("ids").map(String::as_str),
        Some("2001%2C2002")
    );
    assert_signed_toobit_request(&requests[2], "POST", "/api/v2/futures/order/update");
}

#[tokio::test]
async fn toobit_adapter_should_route_spot_native_batch_with_request_specs() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": [
            {"symbol": "BTCUSDT", "orderId": "1001", "clientOrderId": "s1", "side": "BUY", "type": "LIMIT", "status": "NEW", "origQty": "0.01", "price": "65000"},
            {"symbol": "BTCUSDT", "orderId": "1002", "clientOrderId": "s2", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "0.02", "price": "66000"}
        ]}),
        json!({"code": 0, "data": [
            {"symbol": "BTCUSDT", "orderId": "1001", "side": "BUY", "type": "LIMIT", "status": "CANCELED", "origQty": "0.01"},
            {"symbol": "BTCUSDT", "orderId": "1002", "side": "SELL", "type": "LIMIT", "status": "CANCELED", "origQty": "0.02"}
        ]}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let first = spot_order("s1", OrderSide::Buy, "0.01", "65000");
    let second = spot_order("s2", OrderSide::Sell, "0.02", "66000");

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-place"),
            exchange: exchange_id(),
            orders: vec![first, second],
        })
        .await
        .expect("spot batch place");
    assert_eq!(batch.orders.len(), 2);

    let cancel_batch = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("1001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("1002".to_string()),
                },
            ],
        })
        .await
        .expect("spot batch cancel");
    assert_eq!(cancel_batch.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_signed_toobit_request(&requests[0], "POST", "/api/v1/spot/batchOrders");
    load_request_spec("batch_place_orders_spot.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_signed_toobit_request(&requests[1], "DELETE", "/api/v1/spot/cancelOrderByIds");
    load_request_spec("batch_cancel_orders_spot.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn toobit_adapter_should_route_recent_fills_with_request_spec() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "id": "fill-1",
            "symbol": "BTCUSDT",
            "orderId": "1001",
            "price": "65000",
            "qty": "0.01",
            "commission": "0.5",
            "commissionAsset": "USDT",
            "time": "1700000000000",
            "isBuyer": true,
            "isMaker": false
        }]
    })])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills[0].fill_id.as_deref(), Some("fill-1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_toobit_request(&request, "GET", "/api/v1/account/trades");
    load_request_spec("recent_fills.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

fn private_adapter(base_url: String) -> ToobitGatewayAdapter {
    ToobitGatewayAdapter::new(ToobitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..ToobitGatewayConfig::default()
    })
    .expect("adapter")
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "error" => include_str!("../../../../../tests/fixtures/exchanges/toobit/error.json"),
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/toobit/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn perp_order(
    client_order_id: &str,
    side: OrderSide,
    quantity: &str,
    price: &str,
) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: perp_symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side,
        position_side: Some(if side == OrderSide::Buy {
            PositionSide::Long
        } else {
            PositionSide::Short
        }),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: quantity.to_string(),
        price: Some(price.to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn spot_order(
    client_order_id: &str,
    side: OrderSide,
    quantity: &str,
    price: &str,
) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: spot_symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: quantity.to_string(),
        price: Some(price.to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}
