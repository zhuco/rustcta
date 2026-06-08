use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderType};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::{
    build_private_request_spec, request_spec_body_from_order, zebpay_supports_order_type,
    ZEBPAY_BALANCE_PATH, ZEBPAY_CANCEL_ALL_ORDERS_PATH, ZEBPAY_CANCEL_ORDER_PATH,
    ZEBPAY_OPEN_ORDERS_PATH, ZEBPAY_PLACE_ORDER_PATH, ZEBPAY_RECENT_FILLS_PATH,
};
use super::public::{market_query, order_book_endpoint, order_book_query};
use super::signing::build_bearer_private_headers;
use super::streams::zebpay_rest_reconciliation_fallback;
use super::test_support::{context, exchange_id, symbol_scope};
use super::{ZebpayGatewayAdapter, ZebpayGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbol_rules_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/symbol_rules_success.json"
        ),
        "order_book_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/order_book_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/cancel_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/get_recent_fills.json"
        ),
        "signing_vectors/bearer_auth.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/signing_vectors/bearer_auth.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/unsupported_boundary.json"
        ),
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/ws/public_streams_unsupported.json"
        ),
        _ => panic!("unknown zebpay fixture {name}"),
    };
    serde_json::from_str(text).expect("zebpay fixture")
}

#[test]
fn zebpay_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
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
    assert_eq!(boundary["private_rest_default_enabled"], false);
    assert_eq!(
        boundary["region_kyc_boundary"],
        "requires_zebpay_api_onboarding"
    );
}

#[test]
fn zebpay_named_registration_should_accept_adapter_id() {
    AdapterBackedGateway::with_named_adapters("zebpay-test", ["zebpay"]).expect("zebpay");
}

#[test]
fn zebpay_should_parse_symbol_rules_from_public_market_fixture() {
    let rules =
        parse_symbol_rules(&exchange_id(), &fixture("symbol_rules_success.json")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-INR")
        .expect("BTC-INR rule");
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "INR");
    assert_eq!(rule.symbol.exchange_symbol.symbol, "BTC-INR");
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001"));
    assert_eq!(rule.min_notional.as_deref(), Some("100"));

    let query = market_query("singapore");
    assert_eq!(query.get("group").map(String::as_str), Some("singapore"));
}

#[test]
fn zebpay_should_parse_order_book_snapshot_fixture() {
    let snapshot = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope(),
        2,
        &fixture("order_book_success.json"),
    )
    .expect("book");

    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.asks.len(), 2);
    assert_eq!(snapshot.bids[0].price, 7_000_000.0);
    assert_eq!(snapshot.asks[0].quantity, 0.12);
    assert_eq!(order_book_endpoint("BTC-INR", 2), "/market/BTC-INR/book");
    assert_eq!(
        order_book_endpoint("BTC-INR", 50),
        "/market/BTC-INR/book_long"
    );
    let query = order_book_query("singapore");
    assert_eq!(query.get("converted").map(String::as_str), Some("0"));
}

#[tokio::test]
async fn zebpay_private_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = ZebpayGatewayAdapter::new(ZebpayGatewayConfig {
        client_id: "zebpay-client-id".to_string(),
        client_secret: "zebpay-client-secret".to_string(),
        access_token: "zebpay-access-token".to_string(),
        enabled_private_rest: true,
        ..ZebpayGatewayConfig::default()
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
            operation: "zebpay.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn zebpay_batch_place_should_be_explicitly_unsupported() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
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
            operation: "zebpay.batch_place_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn zebpay_streams_should_keep_rest_reconciliation_boundary() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
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
            operation: "zebpay.public_streams_unverified"
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
            operation: "zebpay.private_streams_unverified"
        }
    ));
    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "unsupported_unverified");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "/orders?trade_pair=BTC-INR&status=pending&orderid=0&page=1&limit=500"
    );
    assert!(zebpay_rest_reconciliation_fallback().contains("/orders?status=pending"));
}

#[test]
fn zebpay_bearer_auth_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/bearer_auth.json");
    let headers = build_bearer_private_headers(
        vector["client_id"].as_str().unwrap(),
        vector["access_token"].as_str().unwrap(),
        vector["timestamp"].as_str().unwrap(),
        vector["request_id"].as_str().unwrap(),
    )
    .expect("bearer headers");
    assert_eq!(
        headers.client_id,
        vector["expected_headers"]["client_id"].as_str().unwrap()
    );
    assert_eq!(
        headers.timestamp,
        vector["expected_headers"]["timestamp"].as_str().unwrap()
    );
    assert_eq!(
        headers.request_id,
        vector["expected_headers"]["RequestId"].as_str().unwrap()
    );
    assert_eq!(
        headers.authorization,
        vector["expected_headers"]["Authorization"]
            .as_str()
            .unwrap()
    );
}

#[test]
fn zebpay_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "zebpay.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], ZEBPAY_PLACE_ORDER_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["trade_pair"], "BTC-INR");
    assert_eq!(place["headers"]["Authorization"], "<redacted:bearer-token>");

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "zebpay.get_balances");
    assert_eq!(balances["method"], "GET");
    assert_eq!(balances["path"], ZEBPAY_BALANCE_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "zebpay.cancel_order");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], ZEBPAY_CANCEL_ORDER_PATH);
    assert_eq!(cancel["trade_enabled"], false);

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "zebpay.get_open_orders");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], ZEBPAY_OPEN_ORDERS_PATH);
    assert_eq!(open_orders["query"]["limit"], 500);
    assert_eq!(open_orders["request_spec_only"], true);
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "zebpay.get_recent_fills");
    assert_eq!(recent_fills["method"], "GET");
    assert_eq!(recent_fills["path"], ZEBPAY_RECENT_FILLS_PATH);
    assert_eq!(recent_fills["request_spec_only"], true);
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let spec = build_private_request_spec(
        "GET",
        "/wallet/balance?trade_pair=BTC-INR",
        None,
        "zebpay-client-id",
        "zebpay-access-token",
        "1717200000000",
        "00000000-0000-4000-8000-000000000001",
    )
    .expect("request spec");
    assert_eq!(spec.method, "GET");
    assert_eq!(spec.headers.client_id, "zebpay-client-id");
    assert_eq!(spec.headers.authorization, "Bearer zebpay-access-token");
    assert_eq!(ZEBPAY_CANCEL_ALL_ORDERS_PATH, "/orders/CancelAll");
}

#[test]
fn zebpay_private_order_request_body_should_match_official_shape() {
    let request = place_order_request();
    let body = request_spec_body_from_order(&request);
    assert_eq!(
        body,
        json!({
            "trade_pair": "BTC-INR",
            "side": "bid",
            "size": "0.01",
            "price": "7000000",
            "tradeType": 1,
            "platform": "API_Trading"
        })
    );
    assert!(zebpay_supports_order_type(OrderType::Limit));
    assert!(!zebpay_supports_order_type(OrderType::Market));
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
        price: Some("7000000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}
