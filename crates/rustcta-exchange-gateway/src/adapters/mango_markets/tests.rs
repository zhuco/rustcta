use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OrderBookRequest, PlaceOrderRequest,
    PositionsRequest, PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{parse_position_source_boundary, parse_symbol_rules};
use super::streams::{
    mango_markets_public_subscribe_payload, mango_markets_public_unsubscribe_payload,
    mango_markets_reconnect_policy_ms,
};
use super::transport::{solana_get_account_info_body, solana_get_account_info_path};
use super::{private, signing, MangoMarketsGatewayAdapter, MangoMarketsGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("mango_markets").expect("exchange")
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
        "group_snapshot" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/group_snapshot.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/unsupported_boundary.json"
        ),
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/signing_vectors/solana_transaction_boundary.json"
        ),
        "positions_source" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/request_specs/get_positions_account_source.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/ws/account_subscribe.json"
        ),
        "ws_unsubscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/ws/account_unsubscribe.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "group_account_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/mango_markets/request_specs/group_account_info.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_validate_position_source_boundary_fixture() {
    let audit = parse_position_source_boundary(&exchange_id(), &fixture("positions_source"))
        .expect("audit");

    assert_eq!(audit.boundary, "solana_mango_account_source_only");
    assert!(audit
        .required_sources
        .iter()
        .any(|source| source == "mango_account_public_key"));
    assert!(audit
        .position_fields
        .iter()
        .any(|field| field == "base_lots"));
    assert!(audit
        .reconciliation_required
        .iter()
        .any(|gap| gap == "slot and commitment freshness"));
}

#[test]
fn parser_should_parse_mango_perp_markets_from_group_snapshot() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("group_snapshot")).expect("rules");

    assert_eq!(rules.len(), 2);
    let sol = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "SOL-PERP")
        .expect("SOL-PERP");
    assert_eq!(sol.symbol.market_type, MarketType::Perpetual);
    assert_eq!(sol.base_asset, "SOL");
    assert_eq!(sol.quote_asset, "USDC");
    assert_eq!(sol.price_increment.as_deref(), Some("0.01"));
    assert_eq!(sol.quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(sol.min_quantity.as_deref(), Some("0.001"));
    assert_eq!(sol.price_precision, Some(2));
    assert_eq!(sol.quantity_precision, Some(3));
    assert!(sol.supports_limit_orders);
    assert!(!sol.supports_market_orders);
}

#[test]
fn parser_should_reject_missing_mango_group_markets() {
    let error = parse_symbol_rules(&exchange_id(), &fixture("missing_required_fields"))
        .expect_err("missing perp markets");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

#[test]
fn capabilities_should_expose_g0_scan_only_boundary() {
    let adapter =
        MangoMarketsGatewayAdapter::new(MangoMarketsGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_symbol_rules);
    assert!(!capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "place_order" && !endpoint.support.is_supported()));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
}

#[tokio::test]
async fn get_positions_should_return_mango_account_source_boundary() {
    let adapter =
        MangoMarketsGatewayAdapter::new(MangoMarketsGatewayConfig::default()).expect("adapter");
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
async fn runtime_requests_should_return_solana_transaction_boundaries() {
    let adapter =
        MangoMarketsGatewayAdapter::new(MangoMarketsGatewayConfig::default()).expect("adapter");

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
        quantity: "1".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: true,
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
fn request_specs_should_match_solana_rpc_scan_boundary() {
    let body = solana_get_account_info_body("78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX");
    let actual =
        ActualHttpRequest::new("POST", solana_get_account_info_path()).with_body(Some(body));
    request_spec("group_account_info")
        .assert_matches(&actual)
        .expect("group account info request spec");

    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["instruction_fingerprint"],
        signing::mango_markets_instruction_fingerprint(
            "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg",
            "78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX",
            "mango-account-public-key-fixture",
            "perp_place_order"
        )
    );
    assert_eq!(
        signing::mango_markets_transaction_boundary(),
        signing::MANGO_MARKETS_SIGNING_BOUNDARY
    );
}

#[test]
fn websocket_helpers_should_build_solana_account_subscribe_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        mango_markets_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        mango_markets_public_unsubscribe_payload(42),
        fixture("ws_unsubscribe")
    );
    assert_eq!(
        mango_markets_reconnect_policy_ms(),
        (30_000, 45_000, 90_000)
    );
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let unsupported_spec = serde_json::from_str::<Value>(include_str!(
        "../../../../../tests/fixtures/exchanges/mango_markets/request_specs/place_order_unsupported.json"
    ))
    .expect("unsupported request spec");

    assert_eq!(unsupported_spec["trade_enabled"], false);
    assert_eq!(
        unsupported_spec["expected_error"],
        json!(private::PLACE_ORDER_UNSUPPORTED)
    );
    assert_eq!(
        fixture("unsupported_boundary")["wallet_signing_enabled"],
        false
    );
}
