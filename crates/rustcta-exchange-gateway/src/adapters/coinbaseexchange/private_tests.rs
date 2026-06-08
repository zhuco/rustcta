use rustcta_exchange_api::{
    BalancesRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PageCursor, PageRequest, PlaceOrderRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{CoinbaseExchangeGatewayAdapter, CoinbaseExchangeGatewayConfig};

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinbaseexchange/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinbaseexchange/request_specs/cancel_order.json"
        ),
        "signing_vectors/private_get_accounts.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinbaseexchange/signing_vectors/private_get_accounts.json"
        ),
        _ => panic!("unknown coinbaseexchange fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[tokio::test]
async fn coinbaseexchange_private_should_be_unsupported_without_credentials() {
    let adapter = CoinbaseExchangeGatewayAdapter::default_public().expect("adapter");
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
fn coinbaseexchange_signing_and_request_specs_should_match_fixtures() {
    let vector = fixture("signing_vectors/private_get_accounts.json");
    assert_eq!(
        super::signing::sign_base64_decoded_secret(
            vector["secret"].as_str().unwrap(),
            vector["payload"].as_str().unwrap(),
        )
        .unwrap(),
        vector["expected_signature"].as_str().unwrap()
    );

    let place = fixture("request_specs/place_order_limit.json");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/orders");
    assert_eq!(place["body"]["product_id"], "BTC-USD");
    assert_eq!(place["body"]["client_oid"], "LIMIT1");

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], "/orders/2001");
}

#[tokio::test]
async fn coinbaseexchange_should_route_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "id": "2001",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "size": "0.01",
            "price": "65000",
            "client_oid": "LIMIT1",
            "status": "open"
        }),
        json!({
            "id": "2002",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "market",
            "funds": "125.5",
            "client_oid": "QUOTE1",
            "status": "open"
        }),
        json!({"id": "2001"}),
    ])
    .await;
    let adapter = authed_adapter(base_url);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
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
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "125.5".to_string(),
        })
        .await
        .expect("quote");
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_signed(&requests[0], "POST", "/orders");
    assert_eq!(requests[0].body.as_ref().unwrap()["product_id"], "BTC-USD");
    assert_eq!(requests[0].body.as_ref().unwrap()["client_oid"], "LIMIT1");
    assert_signed(&requests[1], "POST", "/orders");
    assert_eq!(requests[1].body.as_ref().unwrap()["funds"], "125.5");
    assert_signed(&requests[2], "DELETE", "/orders/2001");
}

#[tokio::test]
async fn coinbaseexchange_should_route_private_readbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            {"currency": "BTC", "balance": "0.6", "available": "0.5", "hold": "0.1"},
            {"currency": "USD", "balance": "10.0", "available": "9.0", "hold": "1.0"}
        ]),
        json!({
            "id": "1001",
            "product_id": "BTC-USD",
            "client_oid": "CID1001",
            "side": "buy",
            "type": "limit",
            "status": "open",
            "price": "65000",
            "size": "0.01",
            "filled_size": "0.006",
            "executed_value": "390"
        }),
        json!([
            {
                "id": "1002",
                "product_id": "BTC-USD",
                "side": "sell",
                "type": "limit",
                "status": "open",
                "price": "66000",
                "size": "0.02",
                "filled_size": "0"
            }
        ]),
        json!({"maker_fee_rate": "0.0040", "taker_fee_rate": "0.0060"}),
        json!([
            {
                "trade_id": 9001,
                "order_id": "1001",
                "product_id": "BTC-USD",
                "side": "buy",
                "liquidity": "T",
                "price": "65000",
                "size": "0.006",
                "fee": "1.25",
                "created_at": "2026-06-08T00:00:00Z"
            }
        ]),
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

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        order.order.unwrap().average_fill_price.as_deref(),
        Some("65000")
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: Some(PageRequest {
                limit: Some(50),
                cursor: Some(PageCursor::Token {
                    token: "cursor-1".to_string(),
                }),
            }),
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0040");

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

    let requests = seen.lock().unwrap().clone();
    assert_signed(&requests[0], "GET", "/accounts");
    assert_signed(&requests[1], "GET", "/orders/1001");
    assert_signed(&requests[2], "GET", "/orders");
    assert_eq!(
        requests[2].query.get("status").map(String::as_str),
        Some("open")
    );
    assert_eq!(
        requests[2].query.get("after").map(String::as_str),
        Some("cursor-1")
    );
    assert_signed(&requests[3], "GET", "/fees");
    assert_signed(&requests[4], "GET", "/fills");
    assert_eq!(
        requests[4].query.get("product_id").map(String::as_str),
        Some("BTC-USD")
    );
}

fn authed_adapter(base_url: String) -> CoinbaseExchangeGatewayAdapter {
    CoinbaseExchangeGatewayAdapter::new(CoinbaseExchangeGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..CoinbaseExchangeGatewayConfig::default()
    })
    .expect("adapter")
}

fn assert_signed(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("cb-access-key").map(String::as_str),
        Some("key")
    );
    assert_eq!(
        request
            .headers
            .get("cb-access-passphrase")
            .map(String::as_str),
        Some("passphrase")
    );
    assert!(request
        .headers
        .get("cb-access-sign")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("cb-access-timestamp")
        .is_some_and(|value| !value.is_empty()));
}
