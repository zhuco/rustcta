use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OrderBookRequest,
    PlaceOrderRequest, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderType};
use serde_json::json;

use super::private::{
    build_basic_private_request_spec, request_spec_body_from_order, BITTEAM_BALANCE_PATH,
    BITTEAM_CANCEL_ORDER_PATH, BITTEAM_OPEN_ORDERS_PATH, BITTEAM_PLACE_ORDER_PATH,
    BITTEAM_RECENT_FILLS_PATH,
};
use super::signing::basic_authorization_header;
use super::streams::bitteam_rest_reconciliation_fallback;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BitteamGatewayAdapter, BitteamGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbol_rules_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/symbol_rules_success.json"
        ),
        "order_book_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/order_book_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/cancel_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/get_recent_fills.json"
        ),
        "signing_vectors/basic_auth.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/signing_vectors/basic_auth.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/unsupported_boundary.json"
        ),
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/ws/public_streams_unsupported.json"
        ),
        _ => panic!("unknown bitteam fixture {name}"),
    };
    serde_json::from_str(text).expect("bitteam fixture")
}

#[test]
fn bitteam_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");
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
}

#[test]
fn bitteam_named_registration_should_accept_aliases() {
    AdapterBackedGateway::with_named_adapters("bitteam-test", ["bitteam"]).expect("bitteam");
    AdapterBackedGateway::with_named_adapters("bitteam-alias-test", ["bit.team"])
        .expect("bit.team alias");
}

#[tokio::test]
async fn bitteam_should_load_symbol_rules_from_public_ccxt_pairs() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        rest_base_url: base_url,
        ..BitteamGatewayConfig::default()
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
    assert_eq!(rule.min_notional.as_deref(), Some("5"));
    assert_eq!(seen.lock().unwrap()[0].path, "/trade/api/ccxt/pairs");
}

#[tokio::test]
async fn bitteam_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("order_book_success.json")]).await;
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        rest_base_url: base_url,
        ..BitteamGatewayConfig::default()
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
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/trade/api/orderbooks/BTC_USDT");
}

#[tokio::test]
async fn bitteam_private_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        api_key: "btm_public_test_key".to_string(),
        api_secret: "btm_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..BitteamGatewayConfig::default()
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
            operation: "bitteam.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn bitteam_batch_place_should_be_explicitly_unsupported() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");
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
            operation: "bitteam.batch_place_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn bitteam_streams_should_keep_rest_reconciliation_boundary() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");
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
            operation: "bitteam.public_streams_unverified"
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
            operation: "bitteam.private_streams_unverified"
        }
    ));
    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "unsupported_unverified");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "/trade/api/ccxt/ordersOfUser"
    );
    assert!(bitteam_rest_reconciliation_fallback().contains("ordersOfUser"));
}

#[test]
fn bitteam_basic_auth_signing_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/basic_auth.json");
    let header = basic_authorization_header(
        vector["api_key"].as_str().unwrap(),
        vector["api_secret"].as_str().unwrap(),
    )
    .expect("basic auth");
    assert_eq!(header, vector["expected_authorization"].as_str().unwrap());
}

#[test]
fn bitteam_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "bitteam.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], BITTEAM_PLACE_ORDER_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["pairId"], 12501);
    assert_eq!(place["headers"]["authorization"], "<redacted:basic-auth>");

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "bitteam.get_balances");
    assert_eq!(balances["method"], "GET");
    assert_eq!(balances["path"], BITTEAM_BALANCE_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "bitteam.cancel_order");
    assert_eq!(cancel["method"], "POST");
    assert_eq!(cancel["path"], BITTEAM_CANCEL_ORDER_PATH);
    assert_eq!(cancel["trade_enabled"], false);
    assert_eq!(cancel["headers"]["authorization"], "<redacted:basic-auth>");

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "bitteam.get_open_orders");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], BITTEAM_OPEN_ORDERS_PATH);
    assert_eq!(open_orders["request_spec_only"], true);
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "bitteam.get_recent_fills");
    assert_eq!(recent_fills["method"], "GET");
    assert_eq!(recent_fills["path"], BITTEAM_RECENT_FILLS_PATH);
    assert_eq!(recent_fills["request_spec_only"], true);
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let spec = build_basic_private_request_spec(
        "GET",
        BITTEAM_BALANCE_PATH,
        None,
        "btm_public_test_key",
        "btm_private_test_secret",
    )
    .expect("request spec");
    assert_eq!(
        spec.authorization,
        "Basic YnRtX3B1YmxpY190ZXN0X2tleTpidG1fcHJpdmF0ZV90ZXN0X3NlY3JldA=="
    );
}

#[test]
fn bitteam_private_order_request_body_should_match_ccxt_shape() {
    let request = place_order_request();
    let body = request_spec_body_from_order(&request, 12501);
    assert_eq!(
        body,
        json!({
            "pairId": 12501,
            "side": "buy",
            "type": "limit",
            "amount": "0.01",
            "price": "64000"
        })
    );
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
