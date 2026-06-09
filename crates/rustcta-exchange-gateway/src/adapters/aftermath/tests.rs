use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PositionsRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_position_source_boundary, parse_symbol_rules};
use super::streams::{
    aftermath_public_subscribe_payload, aftermath_public_unsubscribe_payload,
    aftermath_reconnect_policy_ms,
};
use super::{private, signing, AftermathGatewayAdapter, AftermathGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("aftermath").expect("exchange")
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
        exchange_symbol: ExchangeSymbol::new(
            exchange_id(),
            MarketType::Perpetual,
            "aftermath-perp-btc-usdc",
        )
        .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "markets" => include_str!("../../../../../tests/fixtures/exchanges/aftermath/markets.json"),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/aftermath/orderbook.json")
        }
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/unsupported_boundary.json"
        ),
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/aftermath/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/aftermath/error_response.json")
        }
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/signing_vectors/sui_transaction_unsupported.json"
        ),
        "positions_source" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/request_specs/get_positions_account_source.json"
        ),
        "ws_subscribe_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/ws/updates_subscribe_orderbook.json"
        ),
        "ws_unsubscribe_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/ws/updates_unsubscribe_orderbook.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "markets" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/request_specs/markets.json"
        ),
        "orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/aftermath/request_specs/orderbook.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_validate_position_source_boundary_fixture() {
    let audit = parse_position_source_boundary(&exchange_id(), &fixture("positions_source"))
        .expect("audit");

    assert_eq!(audit.boundary, "sdk_account_source_only");
    assert!(audit
        .required_sources
        .iter()
        .any(|source| source == "account_cap_id_or_account_id"));
    assert!(audit
        .position_fields
        .iter()
        .any(|field| field == "position_size"));
    assert!(audit
        .reconciliation_required
        .iter()
        .any(|gap| gap == "Sui checkpoint/epoch freshness"));
}

#[test]
fn parser_should_parse_ccxt_perpetual_markets_and_ignore_spot() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("markets")).expect("rules");

    assert_eq!(rules.len(), 1);
    let rule = &rules[0];
    assert_eq!(rule.symbol.market_type, MarketType::Perpetual);
    assert_eq!(
        rule.symbol.exchange_symbol.symbol,
        "aftermath-perp-btc-usdc"
    );
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USDC");
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(rule.min_notional.as_deref(), Some("5"));
    assert_eq!(rule.price_precision, Some(2));
    assert_eq!(rule.quantity_precision, Some(3));
}

#[test]
fn parser_should_parse_ccxt_orderbook_snapshot() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 64999.5);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.0);
    assert_eq!(snapshot.sequence, Some(42));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_expose_public_perpetual_scan_only_surface() {
    let adapter = AftermathGatewayAdapter::new(AftermathGatewayConfig::default()).expect("adapter");
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
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/api/ccxt/orderbook")));
}

#[tokio::test]
async fn get_positions_should_return_sui_account_source_boundary() {
    let adapter = AftermathGatewayAdapter::new(AftermathGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![symbol().exchange_symbol],
        })
        .await
        .expect_err("positions boundary");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::POSITIONS_UNSUPPORTED
        }
    ));
}

#[tokio::test]
async fn private_writes_should_return_sui_transaction_boundary() {
    let adapter = AftermathGatewayAdapter::new(AftermathGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "1".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter
        .place_order(order.clone())
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
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
        .expect_err("unsupported");
    assert!(matches!(
        batch_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_PLACE_UNSUPPORTED
        }
    ));
}

#[test]
fn request_specs_should_match_public_rest_builders() {
    let markets = ActualHttpRequest::new("GET", "/api/ccxt/markets");
    request_spec("markets")
        .assert_matches(&markets)
        .expect("markets request spec");

    let orderbook = ActualHttpRequest::new("POST", "/api/ccxt/orderbook")
        .with_body(Some(json!({ "chId": "aftermath-perp-btc-usdc" })));
    request_spec("orderbook")
        .assert_matches(&orderbook)
        .expect("orderbook request spec");
}

#[test]
fn websocket_helpers_should_build_offline_payload_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        aftermath_public_subscribe_payload(&subscription),
        fixture("ws_subscribe_orderbook")
    );
    assert_eq!(
        aftermath_public_unsubscribe_payload(&subscription),
        fixture("ws_unsubscribe_orderbook")
    );
    assert_eq!(aftermath_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_keep_after_submit_writes_disabled() {
    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);

    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    assert!(empty.as_array().is_some_and(Vec::is_empty));
    assert_eq!(error["error"], "Invalid request");
    assert!(missing.get("symbol").is_some());

    let signing_vector = fixture("signing_boundary");
    assert_eq!(signing_vector["supported"], false);
    assert_eq!(
        signing_vector["expected_error"],
        private::PLACE_ORDER_UNSUPPORTED
    );
    assert_eq!(
        signing::aftermath_private_write_boundary(),
        signing::AFTERMATH_PRIVATE_WRITE_BOUNDARY
    );
}
