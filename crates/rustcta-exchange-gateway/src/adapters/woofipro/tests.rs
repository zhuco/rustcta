use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OrderBookRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;

use super::parser::{
    parse_balances, parse_orderbook_snapshot, parse_orders, parse_position, parse_recent_fills,
    parse_symbol_rules,
};
use super::streams::{
    woofipro_private_auth_payload, woofipro_public_subscribe_payload, woofipro_reconnect_policy_ms,
};
use super::transport::WoofiproRest;
use super::{signing, WoofiproGatewayAdapter, WoofiproGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("woofipro").expect("exchange")
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
            include_str!("../../../../../tests/fixtures/exchanges/woofipro/public_info.json")
        }
        "orderbook_snapshot" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/orderbook_snapshot.json"
        ),
        "private_holding" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/private_holding.json"
        ),
        "private_position" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/private_position.json"
        ),
        "private_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/private_orders.json"
        ),
        "private_trades" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/private_trades.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/unsupported_boundary.json"
        ),
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/woofipro/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/woofipro/error_response.json")
        }
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/signing_vectors/orderly_ed25519_boundary.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/ws/public_orderbook_subscribe.json"
        ),
        "ws_builder_ticker" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/ws/public_builder_ticker_subscribe.json"
        ),
        "ws_private_auth" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/ws/private_auth_payload.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "public_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/request_specs/public_info.json"
        ),
        "orderbook_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/request_specs/orderbook_signed_read.json"
        ),
        "place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/woofipro/request_specs/place_order.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_woofipro_orderly_perpetual_public_info() {
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
fn parser_should_parse_orderbook_snapshot_fixture_but_runtime_stays_signed_read() {
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
fn parser_should_parse_private_orderly_account_fixtures() {
    let (tenant_id, account_id) = (
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
    );
    let balances = parse_balances(
        &exchange_id(),
        tenant_id.clone(),
        account_id.clone(),
        MarketType::Perpetual,
        &["USDC".to_string()],
        &fixture("private_holding"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "USDC");
    assert_eq!(balances[0].balances[0].available, 975.0);

    let position = parse_position(
        &exchange_id(),
        tenant_id.clone(),
        account_id.clone(),
        &fixture("private_position"),
    )
    .expect("position")
    .expect("non-zero position");
    assert_eq!(position.quantity, 0.25);

    let orders =
        parse_orders(&exchange_id(), Some(&symbol()), &fixture("private_orders")).expect("orders");
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("123456789"));

    let fills = parse_recent_fills(
        &exchange_id(),
        tenant_id,
        account_id,
        Some(&symbol()),
        &fixture("private_trades"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDC"));
}

#[test]
fn capabilities_should_gate_private_runtime_on_orderly_credentials() {
    let adapter = WoofiproGatewayAdapter::new(WoofiproGatewayConfig::default()).expect("adapter");
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
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_symbol_rules"
            && endpoint.path.as_deref() == Some("/v1/public/info")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["full_private_rest"], true);
    assert_eq!(boundary["private_runtime_requires_credentials"], true);
    assert_eq!(boundary["broker_id"], "woofi_pro");
    assert_eq!(boundary["chain_id"], 42161);

    let mut private_config = WoofiproGatewayConfig::default();
    private_config.enabled_private_rest = true;
    private_config.orderly_account_id = Some("acct_test_account_id".to_string());
    private_config.orderly_key = Some("ed25519:test-public-key".to_string());
    private_config.orderly_secret =
        Some("0000000000000000000000000000000000000000000000000000000000000001".to_string());
    let private_adapter = WoofiproGatewayAdapter::new(private_config).expect("private adapter");
    let private_capabilities = private_adapter.capabilities();
    assert!(private_capabilities.supports_private_rest);
    assert!(private_capabilities.supports_place_order);
    assert!(private_capabilities.supports_order_book_snapshot);
}

#[tokio::test]
async fn private_runtime_should_require_credentials_when_disabled() {
    let adapter = WoofiproGatewayAdapter::new(WoofiproGatewayConfig::default()).expect("adapter");
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
            operation: "woofipro.private_rest_disabled"
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
            operation: "woofipro.private_rest_disabled"
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
            operation: "woofipro.private_rest_disabled"
        }
    ));
}

#[test]
fn request_specs_should_match_public_and_signed_read_boundaries() {
    let public_info = ActualHttpRequest::new("GET", WoofiproRest::public_info_path());
    request_spec("public_info")
        .assert_matches(&public_info)
        .expect("public info spec");

    let signed_orderbook =
        ActualHttpRequest::new("GET", WoofiproRest::signed_orderbook_path("PERP_BTC_USDC"))
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

    let place_order = request_spec("place_order");
    assert_eq!(place_order.operation, "place_order");

    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["canonical_payload"],
        signing::woofipro_orderly_canonical_payload(
            1_700_000_000_000,
            "GET",
            "/v1/orderbook/PERP_BTC_USDC",
            ""
        )
    );
    assert_eq!(
        signing::woofipro_orderly_signing_boundary(),
        signing::WOOFIPRO_ORDERLY_SIGNING_BOUNDARY
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
        woofipro_public_subscribe_payload("woofi_pro", &subscription),
        fixture("ws_subscribe")
    );

    let ticker = PublicStreamSubscription {
        kind: PublicStreamKind::Ticker,
        ..subscription
    };
    assert_eq!(
        woofipro_public_subscribe_payload("woofi_pro", &ticker),
        fixture("ws_builder_ticker")
    );
    assert_eq!(
        woofipro_private_auth_payload(
            "ed25519:test-public-key",
            1_700_000_000_000,
            "test-signature-placeholder"
        ),
        fixture("ws_private_auth")
    );
    assert_eq!(woofipro_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_credential_gated() {
    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    let boundary = fixture("unsupported_boundary");

    assert_eq!(empty["success"], true);
    assert!(empty["data"]["rows"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(error["success"], false);
    assert!(missing.get("symbol").is_some());
    assert_eq!(boundary["trade_enabled_with_credentials"], true);
    assert_eq!(boundary["public_ws_runtime_enabled"], false);
}
