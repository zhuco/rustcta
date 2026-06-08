use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest, QueryOrderRequest,
    RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderStatus};
use serde_json::{json, Value};

use super::private::{
    balances_spec, batch_cancel_orders_spec, batch_place_orders_spec, cancel_all_orders_spec,
    cancel_order_spec, open_orders_spec, place_limit_order_spec, query_order_spec,
    recent_fills_spec,
};
use super::test_support::{
    assert_bitopro_signed_request, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::{BitoproGatewayAdapter, BitoproGatewayConfig};

fn fixture(name: &str) -> Value {
    let text = match name {
        "balances_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitopro/balances_success.json")
        }
        "order_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitopro/order_success.json")
        }
        "open_orders_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/open_orders_success.json"
        ),
        "fills_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitopro/fills_success.json")
        }
        "error_response.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitopro/error_response.json")
        }
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/unsupported_boundary.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/get_balances.json"
        ),
        "request_specs/query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/query_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/get_recent_fills.json"
        ),
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/cancel_order.json"
        ),
        "request_specs/cancel_all_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/cancel_all_orders.json"
        ),
        "request_specs/batch_place_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/batch_place_orders.json"
        ),
        "request_specs/batch_cancel_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/request_specs/batch_cancel_orders.json"
        ),
        "signing_vectors/official_get_hmac_sha384.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/signing_vectors/official_get_hmac_sha384.json"
        ),
        "signing_vectors/private_post_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/signing_vectors/private_post_order.json"
        ),
        "ws/private_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitopro/ws/private_open_orders.json"
        ),
        _ => panic!("unknown bitopro fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[test]
fn bitopro_signing_vectors_should_match_hmac_sha384() {
    let official = fixture("signing_vectors/official_get_hmac_sha384.json");
    let signature = super::signing::sign_payload(
        official["api_secret"].as_str().unwrap(),
        official["payload_base64"].as_str().unwrap(),
    )
    .expect("signature");
    assert_eq!(signature, official["expected_signature"].as_str().unwrap());

    let post = fixture("signing_vectors/private_post_order.json");
    let payload = super::signing::payload_for_body(
        &serde_json::from_str(post["body"].as_str().unwrap()).expect("body"),
    )
    .expect("payload");
    assert_eq!(payload, post["payload_base64"].as_str().unwrap());
    let signature = super::signing::sign_payload(
        post["api_secret"].as_str().unwrap(),
        post["payload_base64"].as_str().unwrap(),
    )
    .expect("post signature");
    assert_eq!(signature, post["expected_signature"].as_str().unwrap());
}

#[test]
fn bitopro_request_spec_fixtures_should_cover_private_reads_and_writes() {
    assert_request_spec(&balances_spec(), fixture("request_specs/get_balances.json"));
    assert_request_spec(
        &query_order_spec("btc_twd", "8917255503").expect("query"),
        fixture("request_specs/query_order.json"),
    );
    assert_request_spec(
        &open_orders_spec(Some("btc_twd")).expect("open"),
        fixture("request_specs/get_open_orders.json"),
    );
    assert_request_spec(
        &recent_fills_spec("btc_twd", 100).expect("fills"),
        fixture("request_specs/get_recent_fills.json"),
    );
    assert_request_spec(
        &place_limit_order_spec(
            "btc_twd",
            OrderSide::Buy,
            "3000000",
            "0.01",
            1554380909131,
            true,
            Some(12345),
        )
        .expect("place"),
        fixture("request_specs/place_order_limit.json"),
    );
    assert_request_spec(
        &cancel_order_spec("btc_twd", "8917255503").expect("cancel"),
        fixture("request_specs/cancel_order.json"),
    );
    assert_request_spec(
        &cancel_all_orders_spec(None).expect("cancel all"),
        fixture("request_specs/cancel_all_orders.json"),
    );
    assert_request_spec(
        &batch_place_orders_spec(json!([{
            "pair": "btc_twd",
            "action": "BUY",
            "type": "LIMIT",
            "price": "3000000",
            "amount": "0.01",
            "timestamp": 1554380909131_i64,
            "timeInForce": "GTC",
            "clientId": 12345
        }]))
        .expect("batch place"),
        fixture("request_specs/batch_place_orders.json"),
    );
    assert_request_spec(
        &batch_cancel_orders_spec(json!({
            "BTC_TWD": ["8917255503", "8917255504"]
        }))
        .expect("batch cancel"),
        fixture("request_specs/batch_cancel_orders.json"),
    );
}

#[test]
fn bitopro_private_parser_fixtures_should_cover_success_shapes() {
    let balances = super::private_parser::parse_balances(
        &exchange_id(),
        context("balances").tenant_id.unwrap(),
        context("balances").account_id.unwrap(),
        &[],
        &fixture("balances_success.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");
    assert_eq!(balances[0].balances[0].available, 1.0);

    let order = super::private_parser::parse_order_state(
        &exchange_id(),
        Some(&symbol_scope()),
        &fixture("order_success.json"),
    )
    .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("8917255503"));
    assert_eq!(order.client_order_id.as_deref(), Some("12345"));
    assert_eq!(order.status, OrderStatus::Open);

    let open_orders = super::private_parser::parse_open_orders(
        &exchange_id(),
        Some(&symbol_scope()),
        &fixture("ws/private_open_orders.json"),
    )
    .expect("ws open orders");
    assert_eq!(open_orders.len(), 1);

    let fills = super::private_parser::parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.unwrap(),
        context("fills").account_id.unwrap(),
        &symbol_scope(),
        &fixture("fills_success.json"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("3109362209"));
    assert_eq!(fills[0].price, 3_000_000.0);
}

#[tokio::test]
async fn bitopro_private_reads_should_sign_requests_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balances_success.json"),
        fixture("order_success.json"),
        fixture("open_orders_success.json"),
        fixture("fills_success.json"),
    ])
    .await;
    let adapter = BitoproGatewayAdapter::new(BitoproGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("bitopro-key".to_string()),
        api_secret: Some("bitopro-secret".to_string()),
        api_identity: Some("support@example.test".to_string()),
        enabled_private_rest: true,
        ..Default::default()
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
    assert_eq!(balances.balances[0].balances.len(), 2);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("8917255503".to_string()),
        })
        .await
        .expect("query");
    assert!(order.order.is_some());

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
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/accounts/balance");
    assert_eq!(requests[1].path, "/orders/btc_twd/8917255503");
    assert_eq!(requests[2].path, "/orders/open");
    assert_eq!(requests[3].path, "/orders/trades/btc_twd");
    for request in &requests {
        assert_bitopro_signed_request(request, "bitopro-key");
    }
}

#[tokio::test]
async fn bitopro_private_writes_should_remain_offline_request_spec_only() {
    let adapter = BitoproGatewayAdapter::new(BitoproGatewayConfig::default()).expect("adapter");
    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["trade_enabled"], false);
    assert!(!adapter.capabilities().supports_place_order);

    let error = adapter
        .place_order(rustcta_exchange_api::PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("12345".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: rustcta_types::OrderType::Limit,
            time_in_force: None,
            quantity: "0.01".to_string(),
            price: Some("3000000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn bitopro_adapter_should_classify_error_response_fixture() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("error_response.json")]).await;
    let adapter = BitoproGatewayAdapter::new(BitoproGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("bitopro-key".to_string()),
        api_secret: Some("bitopro-secret".to_string()),
        api_identity: Some("support@example.test".to_string()),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-error"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("error fixture");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.class, ExchangeErrorClass::RateLimited);
            assert_eq!(error.code.as_deref(), Some("TOO_MANY_REQUESTS"));
        }
        other => panic!("expected classified exchange error, got {other:?}"),
    }
}

fn assert_request_spec(spec: &super::private::BitoproRequestSpec, expected: Value) {
    assert_eq!(spec.operation, expected["operation"].as_str().unwrap());
    assert_eq!(spec.method, expected["method"].as_str().unwrap());
    assert_eq!(spec.path, expected["path"].as_str().unwrap());
    assert_eq!(
        serde_json::to_value(&spec.query).unwrap(),
        expected["query"]
    );
    match (&spec.body, &expected["body"]) {
        (Some(body), expected_body) => {
            assert_eq!(serde_json::from_str::<Value>(body).unwrap(), *expected_body);
        }
        (None, expected_body) => assert!(expected_body.is_null()),
    }
}
