use rustcta_exchange_api::{
    BalancesRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    PlaceOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{CoincheckGatewayAdapter, CoincheckGatewayConfig};

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coincheck/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coincheck/request_specs/cancel_order.json"
        ),
        "signing_vectors/private_get_balance.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coincheck/signing_vectors/private_get_balance.json"
        ),
        _ => panic!("unknown coincheck fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[tokio::test]
async fn coincheck_private_should_be_unsupported_without_credentials() {
    let adapter = CoincheckGatewayAdapter::default_public().expect("adapter");
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

#[test]
fn coincheck_signing_and_request_specs_should_match_fixtures() {
    let vector = fixture("signing_vectors/private_get_balance.json");
    assert_eq!(
        super::signing::sign_hex(
            vector["secret"].as_str().unwrap(),
            vector["payload"].as_str().unwrap(),
        )
        .unwrap(),
        vector["expected_signature"].as_str().unwrap()
    );

    let place = fixture("request_specs/place_order_limit.json");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/api/exchange/orders");
    assert_eq!(place["body"]["pair"], "btc_jpy");
    assert_eq!(place["body"]["order_type"], "buy");

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], "/api/exchange/orders/2001");
}

#[tokio::test]
async fn coincheck_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "success": true,
            "id": 2001,
            "rate": "10000000",
            "amount": "0.01",
            "order_type": "buy",
            "pair": "btc_jpy"
        }),
        json!({
            "success": true,
            "id": 2002,
            "market_buy_amount": "1000",
            "order_type": "market_buy",
            "pair": "btc_jpy"
        }),
        json!({"success": true, "id": 2001}),
    ])
    .await;
    let adapter = authed_adapter(base_url);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("10000000".to_string()),
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
            client_order_id: None,
            side: OrderSide::Buy,
            quote_quantity: "1000".to_string(),
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
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_signed(&requests[0], "POST", "/api/exchange/orders");
    assert_eq!(requests[0].body.as_ref().unwrap()["pair"], "btc_jpy");
    assert_eq!(requests[0].body.as_ref().unwrap()["order_type"], "buy");
    assert_signed(&requests[1], "POST", "/api/exchange/orders");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["market_buy_amount"],
        "1000"
    );
    assert_signed(&requests[2], "DELETE", "/api/exchange/orders/2001");
}

#[tokio::test]
async fn coincheck_should_route_private_readbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "success": true,
            "jpy": "10000.0",
            "btc": "0.2",
            "jpy_reserved": "500.0",
            "btc_reserved": "0.01"
        }),
        json!({
            "success": true,
            "orders": [
                {
                    "id": 3001,
                    "order_type": "sell",
                    "rate": "10100000",
                    "pair": "btc_jpy",
                    "pending_amount": "0.02",
                    "created_at": "2026-06-08T00:00:00Z"
                }
            ]
        }),
        json!({
            "success": true,
            "transactions": [
                {
                    "id": 9001,
                    "order_id": 3001,
                    "created_at": "2026-06-08T00:00:00Z",
                    "funds": {"btc": "0.02", "jpy": "-202000"},
                    "pair": "btc_jpy",
                    "rate": "10100000",
                    "fee": "0.0",
                    "order_type": "sell"
                }
            ]
        }),
    ])
    .await;
    let adapter = authed_adapter(base_url);

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
    assert_eq!(open.orders[0].side, OrderSide::Sell);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));

    let requests = seen.lock().unwrap().clone();
    assert_signed(&requests[0], "GET", "/api/accounts/balance");
    assert_signed(&requests[1], "GET", "/api/exchange/orders/opens");
    assert_eq!(
        requests[1].query.get("pair").map(String::as_str),
        Some("btc_jpy")
    );
    assert_signed(&requests[2], "GET", "/api/exchange/orders/transactions");
}

fn authed_adapter(base_url: String) -> CoincheckGatewayAdapter {
    CoincheckGatewayAdapter::new(CoincheckGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoincheckGatewayConfig::default()
    })
    .expect("adapter")
}

fn assert_signed(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("access-key").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("access-nonce")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("access-signature")
        .is_some_and(|value| !value.is_empty()));
}
