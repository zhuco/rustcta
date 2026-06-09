use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest, QueryOrderRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus};
use serde_json::Value;

use super::signing::{canonical_form, signed_form};
use super::test_support::{btc_nis_symbol_scope, context, exchange_id, spawn_rest_server};
use super::{Bit2cGatewayAdapter, Bit2cGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn bit2c_signing_should_match_offline_request_spec_vector() {
    let params = [
        ("Amount", "0.01"),
        ("Price", "99000"),
        ("IsBid", "true"),
        ("Pair", "BtcNis"),
    ];
    let signed = signed_form("secret", &params, 1_700_000_000_000).expect("signed form");

    assert_eq!(
        signed.encoded_body,
        "Amount=0.01&Price=99000&IsBid=true&Pair=BtcNis&nonce=1700000000000"
    );
    assert_eq!(
        signed.signature,
        "eam1//1ne9qUfumUqe8/GAz/kCVxwf+TigdsKWPDdrzpHoWRJ67I+wSHIzzpoUgIq5yQ2L1fZmZO26FIPD6f7w=="
    );
    assert_eq!(
        canonical_form(&[] as &[(&str, &str)], 1_700_000_000_000),
        "nonce=1700000000000"
    );
}

#[test]
fn bit2c_named_registration_should_accept_adapter_id() {
    AdapterBackedGateway::with_named_adapters("bit2c-test", ["bit2c"])
        .expect("bit2c named registration");
}

#[tokio::test]
async fn bit2c_adapter_should_keep_writes_and_balances_disabled_even_with_credentials() {
    let adapter = Bit2cGatewayAdapter::new(Bit2cGatewayConfig {
        api_key: Some("fixture_key".to_string()),
        api_secret: Some("fixture_secret".to_string()),
        enabled_private_rest: true,
        ..Bit2cGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: None,
            assets: Vec::new(),
        })
        .await
        .expect_err("private read must be disabled");

    match error {
        ExchangeApiError::Unsupported { operation } => {
            assert_eq!(operation, "bit2c.balances_private_rest_disabled");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn bit2c_readbacks_should_sign_get_requests_when_enabled() {
    let query_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bit2c/query_order_success.json"
    ))
    .expect("query order response");
    let open_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bit2c/open_orders_success.json"
    ))
    .expect("open orders response");
    let (base_url, seen) = spawn_rest_server(vec![query_response, open_response]).await;
    let adapter = Bit2cGatewayAdapter::new(Bit2cGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("bit2c-key".to_string()),
        api_secret: Some("bit2c-secret".to_string()),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: btc_nis_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(query.exchange_order_id.as_deref(), Some("123456789"));
    assert_eq!(query.status, OrderStatus::Filled);
    assert_eq!(query.side, OrderSide::Buy);
    assert_eq!(query.filled_quantity, "2");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(btc_nis_symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(
        open.orders[0].exchange_order_id.as_deref(),
        Some("123456790")
    );
    assert_eq!(open.orders[0].status, OrderStatus::Open);
    assert_eq!(open.orders[0].side, OrderSide::Buy);
    assert_eq!(open.orders[0].quantity, "2");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/Order/GetById");
    assert_eq!(
        requests[0].query.get("id").map(String::as_str),
        Some("123456789")
    );
    assert!(requests[0].query.get("nonce").is_some());
    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/Order/MyOrders");
    assert_eq!(
        requests[1].query.get("pair").map(String::as_str),
        Some("BtcNis")
    );
    assert!(requests[1].query.get("nonce").is_some());
    for request in &requests {
        assert_eq!(
            request.headers.get("key").map(String::as_str),
            Some("bit2c-key")
        );
        assert!(request.headers.get("sign").is_some());
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/x-www-form-urlencoded")
        );
    }
}
