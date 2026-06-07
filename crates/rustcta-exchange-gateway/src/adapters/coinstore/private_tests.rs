use rustcta_exchange_api::{
    BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest, ExchangeClient,
    PlaceOrderRequest, QuoteMarketOrderRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType, TimeInForce};
use serde_json::json;

use crate::request_spec::{
    ActualHttpRequest, FieldExpectation, FieldMatch, RequestAuth, RequestSpec,
};
use crate::signing_spec::SigningVector;

use super::signing::sign_coinstore_payload;
use super::test_support::{
    assert_signed_request, context, private_config, spawn_rest_server, spot_symbol_scope,
};
use super::CoinstoreGatewayAdapter;

#[tokio::test]
async fn coinstore_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = CoinstoreGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .place_order(place_order_request("cid-1"))
        .await
        .expect_err("private disabled");
    assert!(format!("{error:?}").contains("Unsupported"));
}

#[tokio::test]
async fn coinstore_adapter_should_sign_private_place_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {"orderId": "1", "symbol": "BTCUSDT", "clientOrderId": "cid-1"}
    })])
    .await;
    let adapter = CoinstoreGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let response = adapter
        .place_order(place_order_request("cid-1"))
        .await
        .expect("place");
    assert_eq!(response.order.exchange_order_id.as_deref(), Some("1"));
    let requests = seen.lock().unwrap();
    assert_signed_request(&requests[0], "POST", "/trade/order/place");
    assert!(requests[0].body.contains("BTCUSDT"));
}

#[tokio::test]
async fn coinstore_adapter_should_send_spot_ioc_order_fields() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {"ordId": "2", "symbol": "BTCUSDT", "clOrdId": "cid-ioc"}
    })])
    .await;
    let adapter = CoinstoreGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let mut request = place_order_request("cid-ioc");
    request.order_type = OrderType::IOC;
    request.time_in_force = Some(TimeInForce::IOC);
    adapter.place_order(request).await.expect("ioc place");
    let requests = seen.lock().unwrap();
    assert_signed_request(&requests[0], "POST", "/trade/order/place");
    assert!(requests[0].body.contains("\"ordType\":\"IOC\""));
    assert!(requests[0].body.contains("\"timeInForce\":\"IOC\""));
}

#[tokio::test]
async fn coinstore_adapter_should_send_spot_quote_market_buy() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {"ordId": "3", "symbol": "BTCUSDT", "clOrdId": "cid-quote"}
    })])
    .await;
    let adapter = CoinstoreGatewayAdapter::new(private_config(base_url)).expect("adapter");
    adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("cid-quote".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect("quote market");
    let requests = seen.lock().unwrap();
    assert_signed_request(&requests[0], "POST", "/trade/order/place");
    assert!(requests[0].body.contains("\"ordType\":\"MARKET\""));
    assert!(requests[0].body.contains("\"amount\":\"25\""));
}

#[tokio::test]
async fn coinstore_adapter_should_reject_futures_ioc_before_sending() {
    let (base_url, seen) = spawn_rest_server(vec![]).await;
    let adapter = CoinstoreGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let mut request = place_order_request("cid-futures-ioc");
    request.symbol = super::test_support::perp_symbol_scope();
    request.order_type = OrderType::IOC;
    request.time_in_force = Some(TimeInForce::IOC);
    let error = adapter
        .place_order(request)
        .await
        .expect_err("unsupported futures ioc");
    assert!(format!("{error:?}").contains("coinstore.futures_order_type"));
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn coinstore_adapter_should_route_batch_place_and_cancel() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({ "code": 0, "data": [{"orderId": "1", "symbol": "BTCUSDT"}] }),
        json!({ "code": 0, "data": [{"orderId": "1", "symbol": "BTCUSDT"}] }),
    ])
    .await;
    let adapter = CoinstoreGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: spot_symbol_scope().exchange,
            orders: vec![place_order_request("cid-1")],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 1);
    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: spot_symbol_scope().exchange,
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: spot_symbol_scope(),
                client_order_id: None,
                exchange_order_id: Some("1".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 1);
    let requests = seen.lock().unwrap();
    assert_signed_request(&requests[0], "POST", "/trade/order/placeBatch");
    assert_signed_request(&requests[1], "POST", "/trade/order/cancelBatch");
}

#[tokio::test]
async fn coinstore_request_specs_should_match_public_and_private_http_shapes() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("spot_symbols_success"),
        fixture("order_success"),
        fixture("fills_success"),
    ])
    .await;
    let adapter = CoinstoreGatewayAdapter::new(private_config(base_url)).expect("adapter");
    adapter
        .get_symbol_rules(rustcta_exchange_api::SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spec-rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");
    adapter
        .place_order(place_order_request("fixture-client-1"))
        .await
        .expect("place");
    adapter
        .get_recent_fills(rustcta_exchange_api::RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spec-fills"),
            exchange: spot_symbol_scope().exchange,
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            exchange_order_id: Some("fixture-order-1".to_string()),
            client_order_id: Some("fixture-client-1".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");

    let requests = seen.lock().unwrap();
    request_spec(
        "coinstore.symbol_rules.spot",
        "POST",
        "/v2/public/config/spot/symbols",
        RequestAuth::None,
    )
    .assert_matches(&actual_request(&requests[0]))
    .expect("public rules request spec");
    request_spec_from_fixture("coinstore.place_order.spot")
        .assert_matches(&actual_request(&requests[1]))
        .expect("place order request spec");
    request_spec_from_fixture("coinstore.get_recent_fills.spot")
        .assert_matches(&actual_request(&requests[2]))
        .expect("fills request spec");
}

#[test]
fn coinstore_signing_vector_should_match_two_stage_hmac_fixture() {
    let raw_vectors: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinstore/signing_vectors.json"
    ))
    .expect("signing vectors");
    for raw in raw_vectors {
        let vector: SigningVector = serde_json::from_value(raw.clone()).expect("signing vector");
        vector.verify().expect("second-stage hmac vector verifies");
        let expires = vector
            .timestamp
            .as_deref()
            .expect("timestamp")
            .parse::<i64>()
            .expect("timestamp number");
        let api_secret = raw
            .get("coinstore_api_secret")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("test-secret");
        let signature = sign_coinstore_payload(api_secret, expires, &vector.payload)
            .expect("coinstore signature");
        assert_eq!(signature, vector.expected_signature);
    }
    let vector: SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinstore/signing_vectors/two_stage_hmac_query_plus_body.json"
    ))
    .expect("standard signing vector");
    vector.verify().expect("standard signing vector verifies");
}

#[test]
fn coinstore_request_specs_should_cover_declared_private_rest_operations() {
    let operations = load_private_request_specs()
        .into_iter()
        .map(|spec| spec.operation)
        .collect::<std::collections::BTreeSet<_>>();
    for operation in [
        "coinstore.get_balances.spot",
        "coinstore.place_order.spot",
        "coinstore.cancel_order.spot",
        "coinstore.batch_place_orders.spot",
        "coinstore.batch_cancel_orders.spot",
        "coinstore.get_open_orders.spot",
    ] {
        assert!(operations.contains(operation), "missing {operation}");
    }
}

fn place_order_request(client_order_id: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: spot_symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn request_spec(operation: &str, method: &str, path: &str, auth: RequestAuth) -> RequestSpec {
    let headers = match auth {
        RequestAuth::Signed => [
            (
                "X-CS-APIKEY".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::NonEmpty,
                    value: None,
                },
            ),
            (
                "X-CS-EXPIRES".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::NonEmpty,
                    value: None,
                },
            ),
            (
                "X-CS-SIGN".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::NonEmpty,
                    value: None,
                },
            ),
        ]
        .into_iter()
        .collect(),
        RequestAuth::None | RequestAuth::ApiKey => Default::default(),
    };
    RequestSpec {
        exchange: "coinstore".to_string(),
        operation: operation.to_string(),
        product: None,
        transport: Some("rest".to_string()),
        method: method.to_string(),
        path: path.to_string(),
        auth: Some(auth),
        query: Default::default(),
        headers,
        body: None,
        body_contains: None,
        strict_query: false,
        strict_headers: false,
        strict_body: false,
        forbid_query_values_containing: vec!["secret".to_string(), "token".to_string()],
        forbid_header_values_containing: vec!["secret".to_string(), "token".to_string()],
        signing_vector: Some("two-stage-hmac-query-plus-body".to_string()),
    }
}

fn request_spec_from_fixture(operation: &str) -> RequestSpec {
    load_private_request_specs()
        .into_iter()
        .find(|spec| spec.operation == operation)
        .unwrap_or_else(|| panic!("missing Coinstore request spec {operation}"))
}

fn load_private_request_specs() -> Vec<RequestSpec> {
    serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinstore/request_specs/private_rest.json"
    ))
    .expect("private request specs")
}

fn actual_request(request: &super::test_support::SeenRequest) -> ActualHttpRequest {
    ActualHttpRequest::new(request.method.clone(), request.path.clone())
        .with_query(
            request
                .query
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .with_headers(
            request
                .headers
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .with_body(
            serde_json::from_str::<serde_json::Value>(&request.body)
                .ok()
                .filter(|value| !value.is_null()),
        )
}

fn fixture(name: &str) -> serde_json::Value {
    serde_json::from_str(match name {
        "spot_symbols_success" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/coinstore/spot_symbols_success.json"
            )
        }
        "order_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/order_success.json")
        }
        "fills_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinstore/fills_success.json")
        }
        _ => panic!("unknown coinstore fixture {name}"),
    })
    .expect("fixture json")
}
