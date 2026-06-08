use rustcta_exchange_api::{
    BalancesRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    PlaceOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{CoinspotGatewayAdapter, CoinspotGatewayConfig};

fn coinspot_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinspot/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinspot/request_specs/get_balances.json"
        ),
        "signing_vectors/private_post.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinspot/signing_vectors/private_post.json"
        ),
        _ => panic!("unknown coinspot fixture {name}"),
    };
    serde_json::from_str(text).expect("coinspot fixture")
}

#[tokio::test]
async fn coinspot_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = CoinspotGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn coinspot_signing_should_hex_encode_hmac_sha512() {
    let vector = coinspot_fixture("signing_vectors/private_post.json");
    let signature = super::signing::sign_hex(
        vector["secret"].as_str().unwrap(),
        vector["payload"].as_str().unwrap(),
    )
    .expect("signature");
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
}

#[test]
fn coinspot_request_spec_fixtures_should_cover_private_writes() {
    let place = coinspot_fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "coinspot.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/api/v2/my/buy");
    assert_eq!(place["body"]["cointype"], "btc");
    assert_eq!(place["body"]["markettype"], "aud");
    assert_eq!(place["body"]["rate"], "99000");

    let balances = coinspot_fixture("request_specs/get_balances.json");
    assert!(
        balances["operation"] == "coinspot.get_balances" || balances["operation"] == "get_balances"
    );
    assert_eq!(balances["path"], "/api/v2/ro/my/balances");
}

#[tokio::test]
async fn coinspot_adapter_should_route_private_reads_and_writes() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "status": "ok",
            "balances": {
                "btc": {"balance": "1.5", "available": "1.2", "reserved": "0.3"},
                "aud": {"balance": "120.0", "available": "100.0", "reserved": "20.0"}
            }
        }),
        json!({
            "status": "ok",
            "id": "9001"
        }),
        json!({
            "status": "ok",
            "buyorders": [{"id": "9001", "amount": "0.01", "rate": "99000", "status": "open"}],
            "sellorders": [{"id": "9002", "amount": "0.02", "rate": "101000", "status": "open"}]
        }),
        json!({
            "status": "ok",
            "transactions": [{
                "txid": "fill-1",
                "orderid": "9001",
                "type": "buy",
                "amount": "0.01",
                "rate": "99000",
                "total": "990",
                "fee": "9.9",
                "created": 1710000000000_i64
            }]
        }),
    ])
    .await;
    let adapter = CoinspotGatewayAdapter::new(CoinspotGatewayConfig {
        public_rest_base_url: base_url.clone(),
        private_rest_base_url: base_url.clone(),
        read_only_rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CoinspotGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_balances);
    assert!(capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_amend_order);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].asset.as_str(), "BTC");
    assert_eq!(balances.balances[0].balances[0].available, 1.2);

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
            price: Some("99000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("9001"));

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 2);
    assert_eq!(open.orders[0].status, OrderStatus::New);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            start_time: None,
            end_time: None,
            limit: None,
            page: None,
            exchange_order_id: None,
            from_trade_id: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills.fills[0].fee_amount, Some(9.9));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_signed_request(&requests[0], "/api/v2/ro/my/balances");
    assert_signed_request(&requests[1], "/api/v2/my/buy");
    assert_eq!(requests[1].body.as_ref().unwrap()["cointype"], "btc");
    assert_eq!(requests[1].body.as_ref().unwrap()["markettype"], "aud");
    assert_eq!(requests[1].body.as_ref().unwrap()["amount"], "0.01");
    assert_eq!(requests[1].body.as_ref().unwrap()["rate"], "99000");
    assert!(requests[1].body.as_ref().unwrap()["nonce"].is_string());
    assert_signed_request(&requests[2], "/api/v2/ro/my/orders/open");
    assert_signed_request(&requests[3], "/api/v2/ro/my/transactions");
}

#[tokio::test]
async fn coinspot_adapter_should_expose_unsupported_cancel_boundary() {
    let adapter = CoinspotGatewayAdapter::new(CoinspotGatewayConfig {
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CoinspotGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("9001".to_string()),
        })
        .await
        .expect_err("cancel unsupported");
    assert!(
        matches!(error, ExchangeApiError::Unsupported { operation } if operation == "coinspot.cancel_order_side_specific")
    );
}

fn assert_signed_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, path);
    assert_eq!(request.headers.get("key").map(String::as_str), Some("key"));
    assert!(request
        .headers
        .get("sign")
        .is_some_and(|value| !value.is_empty()));
}
