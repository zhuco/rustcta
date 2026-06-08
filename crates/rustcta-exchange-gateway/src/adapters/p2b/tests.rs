use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OrderBookRequest,
    PlaceOrderRequest, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderType};
use serde_json::json;

use super::private::{
    build_private_request_spec, p2b_supports_order_type, request_spec_body_from_order,
    P2B_BALANCE_PATH, P2B_CANCEL_ORDER_PATH, P2B_OPEN_ORDERS_PATH, P2B_PLACE_ORDER_PATH,
    P2B_RECENT_FILLS_PATH,
};
use super::signing::build_private_headers;
use super::streams::p2b_rest_reconciliation_fallback;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{P2bGatewayAdapter, P2bGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbol_rules_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/symbol_rules_success.json")
        }
        "order_book_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/order_book_success.json")
        }
        "order_book_asks_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/order_book_asks_success.json")
        }
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/cancel_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/get_recent_fills.json"
        ),
        "signing_vectors/private_headers.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/signing_vectors/private_headers.json"
        ),
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/unsupported_boundary.json")
        }
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/ws/public_streams_unsupported.json"
        ),
        _ => panic!("unknown p2b fixture {name}"),
    };
    serde_json::from_str(text).expect("p2b fixture")
}

#[test]
fn p2b_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
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
    assert_eq!(capabilities.max_order_book_depth, Some(100));
    assert_eq!(capabilities.supports_order_types, vec![OrderType::Limit]);

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["task"], "A-32");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_rest_default_enabled"], false);
}

#[test]
fn p2b_named_registration_should_accept_aliases() {
    AdapterBackedGateway::with_named_adapters("p2b-test", ["p2b"]).expect("p2b");
    AdapterBackedGateway::with_named_adapters("p2b-alias-test", ["p2pb2b"]).expect("p2pb2b alias");
}

#[tokio::test]
async fn p2b_should_load_symbol_rules_from_public_markets() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        rest_base_url: base_url,
        ..P2bGatewayConfig::default()
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
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.00001"));
    assert_eq!(rule.min_notional.as_deref(), Some("5"));
    assert!(!rule.supports_market_orders);
    assert!(rule.supports_limit_orders);
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v2/public/markets");
}

#[tokio::test]
async fn p2b_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_book_success.json"),
        fixture("order_book_asks_success.json"),
    ])
    .await;
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        rest_base_url: base_url,
        ..P2bGatewayConfig::default()
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
    assert_eq!(response.order_book.bids[0].price, 64250.5);
    assert_eq!(response.order_book.asks[0].quantity, 0.21);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v2/public/book");
    assert_eq!(
        requests[0].query.get("market").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        requests[0].query.get("side").map(String::as_str),
        Some("buy")
    );
    assert_eq!(
        requests[0].query.get("limit").map(String::as_str),
        Some("2")
    );
    assert_eq!(requests[1].path, "/api/v2/public/book");
    assert_eq!(
        requests[1].query.get("side").map(String::as_str),
        Some("sell")
    );
}

#[tokio::test]
async fn p2b_private_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        api_key: "p2b_public_test_key".to_string(),
        api_secret: "p2b_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..P2bGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
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
            operation: "p2b.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn p2b_batch_place_should_be_explicitly_unsupported() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
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
            operation: "p2b.batch_place_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn p2b_streams_should_keep_rest_reconciliation_boundary() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
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
            operation: "p2b.public_streams_unverified"
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
            operation: "p2b.private_streams_unverified"
        }
    ));
    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "unsupported_unverified");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "/api/v2/orders"
    );
    assert!(p2b_rest_reconciliation_fallback().contains("market_deals"));
}

#[test]
fn p2b_private_header_signing_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/private_headers.json");
    let headers = build_private_headers(
        vector["api_key"].as_str().unwrap(),
        vector["api_secret"].as_str().unwrap(),
        &vector["body"],
    )
    .expect("private headers");
    assert_eq!(
        headers.api_key,
        vector["expected_headers"]["X-TXC-APIKEY"].as_str().unwrap()
    );
    assert_eq!(
        headers.payload,
        vector["expected_headers"]["X-TXC-PAYLOAD"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        headers.signature,
        vector["expected_headers"]["X-TXC-SIGNATURE"]
            .as_str()
            .unwrap()
    );
}

#[test]
fn p2b_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "p2b.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], P2B_PLACE_ORDER_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["market"], "BTC_USDT");
    assert_eq!(
        place["headers"]["X-TXC-SIGNATURE"],
        "<redacted:hmac-sha512-hex>"
    );

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "p2b.get_balances");
    assert_eq!(balances["method"], "POST");
    assert_eq!(balances["path"], P2B_BALANCE_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "p2b.cancel_order");
    assert_eq!(cancel["method"], "POST");
    assert_eq!(cancel["path"], P2B_CANCEL_ORDER_PATH);
    assert_eq!(cancel["trade_enabled"], false);
    assert_eq!(
        cancel["headers"]["X-TXC-PAYLOAD"],
        "<redacted:base64-json-payload>"
    );

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "p2b.get_open_orders");
    assert_eq!(open_orders["method"], "POST");
    assert_eq!(open_orders["path"], P2B_OPEN_ORDERS_PATH);
    assert_eq!(open_orders["request_spec_only"], true);
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "p2b.get_recent_fills");
    assert_eq!(recent_fills["method"], "POST");
    assert_eq!(recent_fills["path"], P2B_RECENT_FILLS_PATH);
    assert_eq!(recent_fills["request_spec_only"], true);
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let spec = build_private_request_spec(
        P2B_BALANCE_PATH,
        json!({}),
        1698508469,
        "p2b_public_test_key",
        "p2b_private_test_secret",
    )
    .expect("request spec");
    assert_eq!(spec.method, "POST");
    assert_eq!(spec.body["request"], P2B_BALANCE_PATH);
    assert_eq!(spec.headers.api_key, "p2b_public_test_key");
    assert!(!spec.headers.payload.is_empty());
    assert_eq!(spec.headers.signature.len(), 128);
}

#[test]
fn p2b_private_order_request_body_should_match_official_shape() {
    let request = place_order_request();
    let body = request_spec_body_from_order(&request);
    assert_eq!(
        body,
        json!({
            "market": "BTC_USDT",
            "side": "buy",
            "amount": "0.01",
            "price": "64000"
        })
    );
    assert!(p2b_supports_order_type(OrderType::Limit));
    assert!(!p2b_supports_order_type(OrderType::Market));
}

fn place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}
