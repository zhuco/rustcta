use rustcta_exchange_api::{
    BalancesRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::private_parser::{parse_open_orders, parse_recent_fills};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{CoinoneGatewayAdapter, CoinoneGatewayConfig};

fn coinone_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinone/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinone/request_specs/cancel_order.json"
        ),
        "signing_vectors/private_place_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinone/signing_vectors/private_place_order.json"
        ),
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinone/unsupported_boundary.json")
        }
        _ => panic!("unknown coinone fixture {name}"),
    };
    serde_json::from_str(text).expect("coinone fixture")
}

#[tokio::test]
async fn coinone_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = CoinoneGatewayAdapter::default_public().expect("adapter");
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

#[test]
fn coinone_signing_should_base64_payload_and_hmac_sha512_hex() {
    let vector = coinone_fixture("signing_vectors/private_place_order.json");
    let payload = vector["payload"].clone();
    let encoded = super::signing::encode_payload(&payload).expect("payload");
    let signature = super::signing::sign_payload(vector["secret"].as_str().unwrap(), &encoded)
        .expect("signature");
    assert_eq!(encoded, vector["expected_payload"].as_str().unwrap());
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
}

#[test]
fn coinone_request_specs_and_unsupported_fixture_should_document_boundaries() {
    let place = coinone_fixture("request_specs/place_order_limit.json");
    assert_eq!(place["operation"], "coinone.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/v2.1/order");
    assert_eq!(place["body"]["quote_currency"], "KRW");
    assert_eq!(place["body"]["target_currency"], "BTC");

    let cancel = coinone_fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "coinone.cancel_order");
    assert_eq!(cancel["path"], "/v2.1/order/cancel");

    let unsupported = coinone_fixture("unsupported_boundary.json");
    assert_eq!(unsupported["payment"], "unsupported");
    assert_eq!(unsupported["wallet"], "unsupported");
    assert_eq!(unsupported["fiat_transfer"], "unsupported");
}

#[tokio::test]
async fn coinone_adapter_should_route_private_order_requests_with_signed_headers() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "success",
            "error_code": "0",
            "order_id": "O-1",
            "user_order_id": "C-1"
        }),
        json!({
            "result": "success",
            "error_code": "0",
            "order_id": "O-1"
        }),
    ])
    .await;
    let adapter = CoinoneGatewayAdapter::new(CoinoneGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        secret_key: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinoneGatewayConfig::default()
    })
    .expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("C-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("100000000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("O-1"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("C-1".to_string()),
            exchange_order_id: Some("O-1".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_signed_request(&requests[0], "/v2.1/order");
    assert_eq!(requests[0].body, None);
    assert!(requests[0]
        .headers
        .get("x-coinone-payload")
        .is_some_and(|value| !value.is_empty()));
    assert!(requests[0]
        .headers
        .get("x-coinone-signature")
        .is_some_and(|value| value.len() == 128));
    assert_signed_request(&requests[1], "/v2.1/order/cancel");
}

#[test]
fn coinone_private_parsers_should_cover_orders_and_fills() {
    let orders = parse_open_orders(
        &exchange_id(),
        Some(&symbol_scope()),
        &json!({
            "orders": [{
                "order_id": "O-1",
                "user_order_id": "C-1",
                "side": "BUY",
                "type": "LIMIT",
                "status": "partially_filled",
                "qty": "0.02",
                "executed_qty": "0.01",
                "price": "100000000",
                "timestamp": 1710000000000_i64
            }]
        }),
    )
    .expect("orders");
    assert_eq!(orders[0].status, OrderStatus::PartiallyFilled);
    assert_eq!(orders[0].filled_quantity, "0.01");

    let fills = parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.unwrap(),
        context("fills").account_id.unwrap(),
        &symbol_scope(),
        &json!({
            "fills": [{
                "trade_id": "T-1",
                "order_id": "O-1",
                "user_order_id": "C-1",
                "side": "BUY",
                "qty": "0.01",
                "price": "100000000",
                "fee": "1000",
                "fee_currency": "KRW",
                "timestamp": 1710000000000_i64
            }]
        }),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("T-1"));
    assert_eq!(fills[0].quote_quantity, Some(1_000_000.0));
}

#[tokio::test]
async fn coinone_adapter_should_route_recent_fills_with_context_boundary() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "error_code": "0",
        "fills": [{
            "trade_id": "T-1",
            "order_id": "O-1",
            "side": "SELL",
            "qty": "0.02",
            "price": "99000000"
        }]
    })])
    .await;
    let adapter = CoinoneGatewayAdapter::new(CoinoneGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        secret_key: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinoneGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
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
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");

    assert_eq!(response.fills.len(), 1);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v2.1/order/complete_orders");
    assert_signed_request(&request, "/v2.1/order/complete_orders");
}

fn assert_signed_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, path);
    assert!(request
        .headers
        .get("x-coinone-payload")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("x-coinone-signature")
        .is_some_and(|value| !value.is_empty()));
}
