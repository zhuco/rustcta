use rustcta_exchange_api::{
    ExchangeApiError, PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

use super::parser::{
    onetrading_symbol, parse_onetrading_order_book, parse_onetrading_symbol_rules,
};
use super::public::{instruments_request_spec_fixture, order_book_request_spec_fixture};
use super::streams::{
    onetrading_ping_payload, onetrading_public_subscribe_payload,
    onetrading_public_unsubscribe_payload, onetrading_reconnect_policy_ms,
};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("onetrading").expect("exchange")
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

fn symbol(raw: &str, base: &str, quote: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, raw).expect("symbol"),
    }
}

#[test]
fn parser_should_normalize_active_spot_eur_markets() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/instruments.json"
    ))
    .expect("fixture");
    let rules = parse_onetrading_symbol_rules(exchange_id(), &[], &instruments).expect("rules");

    assert_eq!(rules.len(), 2);
    let btc_eur = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC_EUR")
        .expect("BTC_EUR");
    assert_eq!(btc_eur.base_asset, "BTC");
    assert_eq!(btc_eur.quote_asset, "EUR");
    assert_eq!(btc_eur.price_increment.as_deref(), Some("0.01"));
    assert_eq!(btc_eur.quantity_increment.as_deref(), Some("0.00001"));
    assert!(btc_eur.supports_limit_orders);
    assert!(btc_eur.supports_post_only);
    assert!(!btc_eur.supports_market_orders);

    assert_eq!(
        onetrading_symbol(&symbol("btc-eur", "BTC", "EUR")),
        "BTC_EUR"
    );
}

#[test]
fn parser_should_reject_missing_required_public_fields() {
    let missing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/missing_required_fields.json"
    ))
    .expect("fixture");
    let error = parse_onetrading_symbol_rules(exchange_id(), &[], &missing).expect_err("missing");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));

    let empty: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/empty_response.json"
    ))
    .expect("fixture");
    let rules = parse_onetrading_symbol_rules(exchange_id(), &[], &empty).expect("empty");
    assert!(rules.is_empty());
}

#[test]
fn parser_should_normalize_rest_and_ws_orderbook_shapes() {
    let rest_book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/orderbook.json"
    ))
    .expect("fixture");
    let rest_snapshot = parse_onetrading_order_book(&symbol("BTC_EUR", "BTC", "EUR"), &rest_book)
        .expect("rest orderbook");
    assert_eq!(rest_snapshot.exchange_id, exchange_id());
    assert_eq!(rest_snapshot.bids[0].price, 10_000.0);
    assert_eq!(rest_snapshot.asks[0].quantity, 4.0);

    let ws_book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/ws_orderbook_snapshot.json"
    ))
    .expect("fixture");
    let ws_snapshot = parse_onetrading_order_book(&symbol("BTC_EUR", "BTC", "EUR"), &ws_book)
        .expect("ws orderbook");
    assert_eq!(ws_snapshot.bids[0].quantity, 6.0);
    assert!(ws_snapshot.exchange_timestamp.is_some());
}

#[test]
fn public_request_specs_and_ws_payloads_should_match_docs() {
    assert_eq!(instruments_request_spec_fixture()["path"], "/instruments");
    assert_eq!(
        order_book_request_spec_fixture()["path"],
        "/order-book/BTC_EUR"
    );

    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC_EUR", "BTC", "EUR"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let subscribe = onetrading_public_subscribe_payload(&subscription);
    assert_eq!(subscribe["type"], "SUBSCRIBE");
    assert_eq!(subscribe["channels"][0]["name"], "ORDER_BOOK");
    assert_eq!(subscribe["channels"][0]["instrument_codes"][0], "BTC_EUR");
    assert_eq!(
        onetrading_public_unsubscribe_payload(&subscription)["type"],
        "UNSUBSCRIBE"
    );
    assert_eq!(onetrading_ping_payload()["type"], "PING");
    assert_eq!(onetrading_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}
