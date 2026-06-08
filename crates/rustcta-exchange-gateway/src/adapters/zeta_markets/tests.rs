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
use super::streams::{zeta_markets_reconnect_policy_ms, zeta_markets_rest_reconciliation_payload};
use super::transport::ZetaMarketsRest;
use super::{private, signing, ZetaMarketsGatewayAdapter, ZetaMarketsGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("zeta_markets").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("SOL", "USDC").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "SOL-PERP")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "public_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/public_info.json"
        ),
        "orderbook_snapshot" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/orderbook_snapshot.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/unsupported_boundary.json"
        ),
        "empty_response" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/empty_response.json"
        ),
        "error_response" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/error_response.json"
        ),
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/signing_vectors/solana_sdk_boundary.json"
        ),
        "ws_reconciliation" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/ws/rest_reconciliation_fallback.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "public_symbols" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/request_specs/public_symbols.json"
        ),
        "public_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/zeta_markets/request_specs/public_orderbook.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_zeta_perpetual_public_symbols() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("public_info")).expect("rules");

    assert_eq!(rules.len(), 3);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "SOL-PERP")
        .expect("sol rule");
    assert_eq!(rule.symbol.market_type, MarketType::Perpetual);
    assert_eq!(rule.base_asset, "SOL");
    assert_eq!(rule.quote_asset, "USDC");
    assert_eq!(rule.price_increment.as_deref(), Some("0.0001"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(rule.price_precision, Some(4));
    assert_eq!(rule.quantity_precision, Some(3));
}

#[test]
fn parser_should_parse_zeta_orderbook_snapshot_fixture() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook_snapshot"))
            .expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 154.25);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 12.5);
    assert_eq!(snapshot.best_ask().expect("ask").price, 154.27);
    assert_eq!(snapshot.sequence, Some(288888888));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_expose_scan_only_public_rest() {
    let adapter =
        ZetaMarketsGatewayAdapter::new(ZetaMarketsGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/v2/orderbook?ticker_id={symbol}")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["venue_ceased_operations"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
}

#[tokio::test]
async fn private_writes_and_streams_should_return_shutdown_boundary() {
    let adapter =
        ZetaMarketsGatewayAdapter::new(ZetaMarketsGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "1".to_string(),
        price: Some("154.25".to_string()),
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

    let stream_error = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-stream"),
            symbol: symbol(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect_err("unsupported stream");
    assert!(matches!(
        stream_error,
        ExchangeApiError::Unsupported {
            operation: "zeta_markets.public_streams_require_solana_sdk_account_mapping"
        }
    ));
}

#[test]
fn request_specs_should_match_public_rest_builders() {
    let public_symbols = ActualHttpRequest::new("GET", ZetaMarketsRest::symbols_path());
    request_spec("public_symbols")
        .assert_matches(&public_symbols)
        .expect("public symbols spec");

    let public_orderbook = ActualHttpRequest::new("GET", "/v2/orderbook?ticker_id=SOL-PERP");
    request_spec("public_orderbook")
        .assert_matches(&public_orderbook)
        .expect("public orderbook spec");
}

#[test]
fn signing_and_stream_boundaries_should_match_fixtures() {
    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["boundary"],
        signing::zeta_markets_signing_boundary()
    );
    assert_eq!(
        signing_fixture["canonical_payload"],
        signing::zeta_markets_solana_transaction_boundary(
            "zeta-v2",
            "place_order",
            "SOL-PERP",
            "wallet-owned"
        )
    );
    assert_eq!(
        zeta_markets_rest_reconciliation_payload("SOL-PERP"),
        fixture("ws_reconciliation")
    );
    assert_eq!(zeta_markets_reconnect_policy_ms(), (30_000, 60_000, 90_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    let unsupported_spec = serde_json::from_str::<Value>(include_str!(
        "../../../../../tests/fixtures/exchanges/zeta_markets/request_specs/place_order_unsupported.json"
    ))
    .expect("unsupported request spec");

    assert_eq!(empty["symbols"].as_array().expect("symbols").len(), 0);
    assert_eq!(error["success"], false);
    assert!(missing.get("symbols").is_some());
    assert_eq!(unsupported_spec["trade_enabled"], false);
    assert_eq!(
        unsupported_spec["expected_error"],
        json!(private::PLACE_ORDER_UNSUPPORTED)
    );
}

#[tokio::test]
async fn orderbook_request_can_be_constructed_for_public_rest_surface() {
    let adapter = ZetaMarketsGatewayAdapter::new(ZetaMarketsGatewayConfig {
        enabled_public_rest: false,
        ..ZetaMarketsGatewayConfig::default()
    })
    .expect("adapter");
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
            operation: "zeta_markets.public_rest_disabled"
        }
    ));
}
