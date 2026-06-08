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
    aark_private_auth_payload, aark_public_subscribe_payload, aark_reconnect_policy_ms,
};
use super::transport::AarkRest;
use super::{private, signing, AarkGatewayAdapter, AarkGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("aark").expect("exchange")
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
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "PERP_BTC_USDC")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "public_info" => {
            include_str!("../../../../../tests/fixtures/exchanges/aark/public_info.json")
        }
        "orderbook_snapshot" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/orderbook_snapshot.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/unsupported_boundary.json"
        ),
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/aark/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/aark/error_response.json")
        }
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/signing_vectors/orderly_ed25519_boundary.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/ws/public_orderbook_subscribe.json"
        ),
        "ws_private_auth" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/ws/private_auth_payload.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "public_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/public_info.json"
        ),
        "orderbook_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/orderbook_signed_read.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_orderly_perpetual_public_info() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("public_info")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "PERP_BTC_USDC")
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
fn parser_should_parse_orderly_orderbook_snapshot_fixture() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook_snapshot"))
            .expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert_eq!(snapshot.sequence, Some(424242));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_expose_public_info_but_no_trade_or_orderbook_runtime() {
    let adapter = AarkGatewayAdapter::new(AarkGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(!capabilities.supports_order_book_snapshot);
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
            && endpoint.path.as_deref() == Some("/v1/public/info")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
}

#[tokio::test]
async fn orderbook_and_private_writes_should_return_orderly_boundary() {
    let adapter = AarkGatewayAdapter::new(AarkGatewayConfig::default()).expect("adapter");
    let orderbook_error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol(),
            depth: Some(50),
        })
        .await
        .expect_err("unsupported book");
    assert!(matches!(
        orderbook_error,
        ExchangeApiError::Unsupported {
            operation: private::ORDER_BOOK_UNSUPPORTED
        }
    ));

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
fn request_specs_should_match_public_and_signed_read_boundaries() {
    let public_info = ActualHttpRequest::new("GET", AarkRest::public_info_path());
    request_spec("public_info")
        .assert_matches(&public_info)
        .expect("public info spec");

    let signed_orderbook =
        ActualHttpRequest::new("GET", AarkRest::signed_orderbook_path("PERP_BTC_USDC"))
            .with_headers([
                (
                    "orderly-account-id".to_string(),
                    "acct_test_account_id".to_string(),
                ),
                (
                    "orderly-key".to_string(),
                    "ed25519:test-public-key".to_string(),
                ),
                (
                    "orderly-signature".to_string(),
                    "test-signature-placeholder".to_string(),
                ),
                ("orderly-timestamp".to_string(), "1700000000000".to_string()),
            ]);
    request_spec("orderbook_signed_read")
        .assert_matches(&signed_orderbook)
        .expect("signed orderbook spec");

    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["canonical_payload"],
        signing::aark_orderly_canonical_payload(
            1_700_000_000_000,
            "GET",
            "/v1/orderbook/PERP_BTC_USDC",
            ""
        )
    );
    assert_eq!(
        signing::aark_orderly_signing_boundary(),
        signing::AARK_ORDERLY_SIGNING_BOUNDARY
    );
}

#[test]
fn websocket_helpers_should_build_fixture_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        aark_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        aark_private_auth_payload(
            "ed25519:test-public-key",
            1_700_000_000_000,
            "test-signature-placeholder"
        ),
        fixture("ws_private_auth")
    );
    assert_eq!(aark_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    let unsupported_spec = serde_json::from_str::<Value>(include_str!(
        "../../../../../tests/fixtures/exchanges/aark/request_specs/place_order_unsupported.json"
    ))
    .expect("unsupported request spec");

    assert_eq!(empty["success"], true);
    assert!(empty["data"]["rows"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(error["success"], false);
    assert!(missing.get("symbol").is_some());
    assert_eq!(unsupported_spec["trade_enabled"], false);
    assert_eq!(
        unsupported_spec["expected_error"],
        json!(private::PLACE_ORDER_UNSUPPORTED)
    );
}
