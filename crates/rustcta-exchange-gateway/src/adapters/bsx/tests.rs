use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OrderBookRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    bsx_private_auth_payload, bsx_public_subscribe_payload, bsx_reconnect_policy_ms,
};
use super::transport::BsxRest;
use super::{private, signing, BsxGatewayAdapter, BsxGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bsx").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDC").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC-PERP")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "products" => include_str!("../../../../../tests/fixtures/exchanges/bsx/products.json"),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/orderbook.json")
        }
        "unsupported_boundary" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/unsupported_boundary.json")
        }
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/error_response.json")
        }
        "missing_required_fields" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/missing_required_fields.json")
        }
        "eip712_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/signing_vectors/eip712_order_boundary.json"
        ),
        "ws_auth_signature" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/signing_vectors/ws_auth_hmac_sha256.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/ws/public_orderbook_subscribe.json"
        ),
        "ws_private_auth" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/ws/private_auth_payload.json")
        }
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "products" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/request_specs/products.json")
        }
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/request_specs/orderbook.json")
        }
        "open_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/open_orders_spec_only.json"
        ),
        "cancel_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/cancel_order_unsupported.json"
        ),
        "place_order_unsupported" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/place_order_unsupported.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_bsx_products_as_perpetual_symbol_rules() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("products")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-PERP")
        .expect("btc rule");
    assert_eq!(rule.symbol.market_type, MarketType::Perpetual);
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USDC");
    assert_eq!(rule.price_increment.as_deref(), Some("0.1"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(rule.min_notional.as_deref(), Some("10"));
    assert_eq!(rule.price_precision, Some(1));
    assert_eq!(rule.quantity_precision, Some(4));
}

#[test]
fn parser_should_parse_public_orderbook_snapshot_fixture() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert_eq!(snapshot.sequence, Some(424242));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn parser_should_reject_error_and_missing_required_fields() {
    parse_symbol_rules(&exchange_id(), &fixture("error_response")).expect_err("error response");
    parse_symbol_rules(&exchange_id(), &fixture("missing_required_fields"))
        .expect_err("missing required field");
}

#[test]
fn capabilities_should_expose_public_market_data_but_no_private_runtime() {
    let adapter = BsxGatewayAdapter::new(BsxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_symbol_rules"
            && endpoint.path.as_deref() == Some("/products")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/products/{product_id}/book")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
}

#[tokio::test]
async fn private_writes_should_return_eip712_boundary() {
    let adapter = BsxGatewayAdapter::new(BsxGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let place_error = adapter
        .place_order(order.clone())
        .await
        .expect_err("unsupported place");
    assert!(matches!(
        place_error,
        ExchangeApiError::Unsupported {
            operation: private::PLACE_ORDER_UNSUPPORTED
        }
    ));

    let batch_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect_err("unsupported batch");
    assert!(matches!(
        batch_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_PLACE_UNSUPPORTED
        }
    ));
}

#[test]
fn request_specs_should_match_public_market_data_and_private_boundaries() {
    let products = ActualHttpRequest::new("GET", BsxRest::products_path());
    request_spec("products")
        .assert_matches(&products)
        .expect("products spec");

    let orderbook = ActualHttpRequest::new("GET", BsxRest::orderbook_path("BTC-PERP"));
    request_spec("orderbook")
        .assert_matches(&orderbook)
        .expect("orderbook spec");

    let timestamp_ns = 1_700_000_000_000_000_000_i128;
    let signature = signing::bsx_ws_auth_signature("test-api-key", "test-api-secret", timestamp_ns);
    let open_orders = ActualHttpRequest::new("GET", BsxRest::open_orders_path()).with_headers([
        ("BSX-KEY".to_string(), "test-api-key".to_string()),
        ("BSX-SIGNATURE".to_string(), signature),
        ("BSX-TIMESTAMP".to_string(), timestamp_ns.to_string()),
    ]);
    request_spec("open_orders")
        .assert_matches(&open_orders)
        .expect("open orders spec");

    let cancel_order =
        ActualHttpRequest::new("DELETE", BsxRest::cancel_order_path("test-order-id")).with_headers(
            [
                ("BSX-KEY".to_string(), "test-api-key".to_string()),
                (
                    "BSX-SIGNATURE".to_string(),
                    signing::bsx_ws_auth_signature("test-api-key", "test-api-secret", timestamp_ns),
                ),
                ("BSX-TIMESTAMP".to_string(), timestamp_ns.to_string()),
            ],
        );
    request_spec("cancel_order")
        .assert_matches(&cancel_order)
        .expect("cancel order spec");

    let place_order =
        ActualHttpRequest::new("POST", BsxRest::place_order_path()).with_body(Some(json!({
            "product_id": "BTC-PERP",
            "side": "BUY",
            "order_type": "LIMIT",
            "size": "0.01",
            "price": "65000",
            "signature": "0xredacted_eip712_signature"
        })));
    request_spec("place_order_unsupported")
        .assert_matches(&place_order)
        .expect("place order unsupported spec");
}

#[test]
fn websocket_helpers_should_build_fixture_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let signature_fixture = fixture("ws_auth_signature");
    let timestamp_ns = signature_fixture["timestamp_ns"]
        .as_str()
        .expect("timestamp")
        .parse::<i128>()
        .expect("timestamp ns");
    let signature = signing::bsx_ws_auth_signature("test-api-key", "test-api-secret", timestamp_ns);

    assert_eq!(
        bsx_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        signing::bsx_ws_auth_message("test-api-key", timestamp_ns),
        signature_fixture["canonical_payload"]
    );
    assert_eq!(signature, signature_fixture["expected_signature"]);
    assert_eq!(
        bsx_private_auth_payload("test-api-key", timestamp_ns, &signature),
        fixture("ws_private_auth")
    );
    assert_eq!(
        signing::bsx_eip712_order_boundary(),
        signing::BSX_EIP712_ORDER_BOUNDARY
    );
    assert_eq!(bsx_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let unsupported_spec = request_spec("place_order_unsupported");
    let eip712 = fixture("eip712_boundary");

    assert!(empty["products"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(eip712["expected_error"], private::PLACE_ORDER_UNSUPPORTED);
    assert_eq!(
        unsupported_spec.body_contains.expect("body")["signature"],
        "0xredacted_eip712_signature"
    );
}

#[tokio::test]
async fn public_rest_disabled_should_return_unsupported_for_market_data() {
    let mut config = BsxGatewayConfig::default();
    config.enabled_public_rest = false;
    let adapter = BsxGatewayAdapter::new(config).expect("adapter");

    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol(),
            depth: Some(50),
        })
        .await
        .expect_err("disabled public rest");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bsx.public_rest_disabled"
        }
    ));
}
