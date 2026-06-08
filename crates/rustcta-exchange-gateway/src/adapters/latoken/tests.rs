use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OrderBookRequest,
    PlaceOrderRequest, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderType, PositionSide};

use super::private::{
    build_signed_private_request_spec, cancel_order_body, request_spec_body_from_order,
    LATOKEN_BALANCES_PATH, LATOKEN_PLACE_ORDER_PATH,
};
use super::signing::{rest_signature, LatokenDigest};
use super::streams::{
    heartbeat_policy, parse_public_book_message, public_book_destination,
    rest_reconciliation_fallback, stomp_subscribe_frame, stomp_unsubscribe_frame, ws_auth_headers,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{LatokenGatewayAdapter, LatokenGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "pairs_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/latoken/pairs_success.json")
        }
        "currencies_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/currencies_success.json"
        ),
        "orderbook_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/orderbook_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/get_balances.json"
        ),
        "signing_vectors/rest_place_order_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/rest_place_order_hmac_sha256.json"
        ),
        "signing_vectors/ws_auth_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/ws_auth_hmac_sha256.json"
        ),
        "ws/public_orderbook_frame.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/ws/public_orderbook_frame.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/unsupported_boundary.json"
        ),
        _ => panic!("unknown LATOKEN fixture {name}"),
    };
    serde_json::from_str(text).expect("LATOKEN fixture")
}

#[test]
fn latoken_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(
        boundary["public_streams"],
        "stomp_spec_only_runtime_unsupported"
    );
}

#[test]
fn latoken_named_registration_should_accept_adapter_id() {
    AdapterBackedGateway::with_named_adapters("latoken-test", ["latoken"]).expect("latoken");
}

#[tokio::test]
async fn latoken_should_load_symbol_rules_from_pairs_and_currencies() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("pairs_success.json"),
        fixture("currencies_success.json"),
    ])
    .await;
    let adapter = LatokenGatewayAdapter::new(LatokenGatewayConfig {
        rest_base_url: base_url,
        ..LatokenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    let rule = &response.rules[0];
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USDT");
    assert_eq!(rule.symbol.exchange_symbol.symbol, "BTC_USDT");
    assert_eq!(rule.price_increment.as_deref(), Some("0.010000000"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001000"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.00001"));
    assert_eq!(rule.min_notional.as_deref(), Some("5"));

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/v2/pair");
    assert_eq!(seen[1].path, "/v2/currency");
}

#[tokio::test]
async fn latoken_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook_success.json")]).await;
    let adapter = LatokenGatewayAdapter::new(LatokenGatewayConfig {
        rest_base_url: base_url,
        ..LatokenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 2);
    assert_eq!(response.order_book.asks.len(), 2);
    assert_eq!(response.order_book.bids[0].price, 62679.15);
    assert_eq!(response.order_book.asks[0].quantity, 0.1177424);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/book/BTC/USDT");
    assert_eq!(request.query.get("limit").map(String::as_str), Some("2"));
}

#[tokio::test]
async fn latoken_private_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = LatokenGatewayAdapter::new(LatokenGatewayConfig {
        api_key: Some("latoken_public_test_key".to_string()),
        api_secret: Some("latoken_private_test_secret".to_string()),
        enabled_private_rest: true,
        ..LatokenGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
    assert!(adapter.config.private_rest_enabled());
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
        .expect_err("private reads disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "latoken.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn latoken_batch_place_should_be_explicitly_request_spec_only() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![place_order_request()],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "latoken.batch_place_orders_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn latoken_streams_should_deliver_specs_but_keep_runtime_unsupported() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let public_error = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect_err("public ws unsupported");
    assert!(matches!(
        public_error,
        ExchangeApiError::Unsupported {
            operation: "latoken.public_streams_stomp_spec_only"
        }
    ));

    let private_error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private ws unsupported");
    assert!(matches!(
        private_error,
        ExchangeApiError::Unsupported {
            operation: "latoken.private_streams_user_id_auth_unverified"
        }
    ));

    let destination = public_book_destination(&symbol_scope()).expect("destination");
    assert_eq!(destination, "/v1/book/BTC/USDT");
    assert!(stomp_subscribe_frame("sub-1", &destination).starts_with("SUBSCRIBE\nid:sub-1"));
    assert_eq!(
        stomp_unsubscribe_frame("sub-1"),
        "UNSUBSCRIBE\nid:sub-1\n\n\u{0}"
    );
    assert!(heartbeat_policy().contains("nonce sequence gaps"));
    assert!(rest_reconciliation_fallback().contains("/v2/auth/order/active"));
}

#[test]
fn latoken_signing_vectors_should_match_fixtures() {
    let vector = fixture("signing_vectors/rest_place_order_hmac_sha256.json");
    let signature = rest_signature(
        vector["api_secret"].as_str().unwrap(),
        vector["canonical_payload"].as_str().unwrap(),
        LatokenDigest::Sha256,
    )
    .expect("signature");
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());

    let ws_vector = fixture("signing_vectors/ws_auth_hmac_sha256.json");
    let headers = ws_auth_headers(
        ws_vector["api_key"].as_str().unwrap(),
        ws_vector["api_secret"].as_str().unwrap(),
        ws_vector["timestamp_ms"].as_str().unwrap(),
    )
    .expect("ws headers");
    assert_eq!(
        headers["X-LA-SIGNATURE"].as_str().unwrap(),
        ws_vector["expected_signature"].as_str().unwrap()
    );
}

#[test]
fn latoken_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place_fixture = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place_fixture["operation"], "latoken.place_order");
    assert_eq!(place_fixture["method"], "POST");
    assert_eq!(place_fixture["path"], LATOKEN_PLACE_ORDER_PATH);
    assert_eq!(place_fixture["trade_enabled"], false);
    assert_eq!(
        place_fixture["headers"]["X-LA-APIKEY"],
        "<redacted:api-key>"
    );

    let request = place_order_request();
    let (body, body_params) = request_spec_body_from_order(&request).expect("body");
    assert_eq!(body["baseCurrency"], "BTC");
    assert_eq!(body["quoteCurrency"], "USDT");
    let spec = build_signed_private_request_spec(
        "POST",
        LATOKEN_PLACE_ORDER_PATH,
        Vec::new(),
        body_params,
        Some(body),
        "latoken_public_test_key",
        "latoken_private_test_secret",
    )
    .expect("request spec");
    assert_eq!(
        spec.canonical_payload,
        place_fixture["canonical_payload"].as_str().unwrap()
    );
    assert_eq!(
        spec.signature,
        fixture("signing_vectors/rest_place_order_hmac_sha256.json")["expected_signature"]
            .as_str()
            .unwrap()
    );

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "latoken.get_balances");
    assert_eq!(balances["path"], LATOKEN_BALANCES_PATH);
    let (cancel_body, cancel_params) = cancel_order_body("latoken_test_order_id_001");
    assert_eq!(cancel_body["id"], "latoken_test_order_id_001");
    assert_eq!(cancel_params[0].1, "latoken_test_order_id_001");
}

#[test]
fn latoken_ws_public_book_fixture_should_parse_order_book_payload() {
    let value = fixture("ws/public_orderbook_frame.json");
    let order_book =
        parse_public_book_message(&exchange_id(), symbol_scope(), &value).expect("book");
    assert_eq!(order_book.bids.len(), 1);
    assert_eq!(order_book.asks.len(), 1);
    assert_eq!(order_book.bids[0].price, 62679.15);
}

fn place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place-order"),
        symbol: symbol_scope(),
        client_order_id: Some("latoken_test_client_order_001".to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}
