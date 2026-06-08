use rustcta_exchange_api::{
    ExchangeApiError, PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

use super::parser::{arkham_symbol, parse_arkham_order_book, parse_arkham_symbol_rules};
use super::streams::{
    arkham_ping_payload, arkham_public_subscribe_payload, arkham_public_unsubscribe_payload,
    arkham_reconnect_policy_ms,
};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("arkham").expect("exchange")
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

fn symbol(market_type: MarketType, raw: &str, base: &str, quote: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, raw).expect("symbol"),
    }
}

#[test]
fn parser_should_normalize_spot_and_perpetual_markets() {
    let pairs: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/pairs.json"
    ))
    .expect("fixture");
    let rules = parse_arkham_symbol_rules(exchange_id(), &[], &pairs).expect("rules");

    let spot = rules
        .iter()
        .find(|rule| rule.symbol.market_type == MarketType::Spot)
        .expect("spot rule");
    assert_eq!(spot.base_asset, "BTC");
    assert_eq!(spot.quote_asset, "USDT");
    assert_eq!(spot.price_increment.as_deref(), Some("0.01"));
    assert_eq!(spot.quantity_increment.as_deref(), Some("0.00001"));
    assert!(spot.supports_post_only);

    let perp = rules
        .iter()
        .find(|rule| rule.symbol.market_type == MarketType::Perpetual)
        .expect("perp rule");
    assert_eq!(perp.base_asset, "BTC");
    assert_eq!(perp.quote_asset, "USDT");
    assert!(perp.supports_reduce_only);
    assert_eq!(
        arkham_symbol(&symbol(
            MarketType::Perpetual,
            "BTC_USDT_PERP",
            "BTC",
            "USDT"
        )),
        "BTC_USDT_PERP"
    );
}

#[test]
fn parser_should_reject_missing_required_public_fields() {
    let missing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/missing_required_fields.json"
    ))
    .expect("fixture");
    let error = parse_arkham_symbol_rules(exchange_id(), &[], &missing).expect_err("missing");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));

    let empty: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/empty_response.json"
    ))
    .expect("fixture");
    let rules = parse_arkham_symbol_rules(exchange_id(), &[], &empty).expect("empty");
    assert!(rules.is_empty());
}

#[test]
fn parser_should_normalize_orderbook_snapshot() {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/orderbook.json"
    ))
    .expect("fixture");
    let snapshot =
        parse_arkham_order_book(&symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT"), &value)
            .expect("orderbook");

    assert_eq!(snapshot.exchange_id, exchange_id());
    assert_eq!(snapshot.market_type, MarketType::Spot);
    assert_eq!(snapshot.bids[0].price, 64_000.0);
    assert_eq!(snapshot.asks[0].quantity, 0.0243);
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn websocket_public_helpers_should_cover_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(MarketType::Perpetual, "BTC_USDT_PERP", "BTC", "USDT"),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let subscribe = arkham_public_subscribe_payload(&subscription);
    assert_eq!(subscribe["op"], "subscribe");
    assert_eq!(subscribe["channel"], "book");
    assert_eq!(subscribe["symbol"], "BTC_USDT_PERP");
    assert_eq!(
        arkham_public_unsubscribe_payload(&subscription)["op"],
        "unsubscribe"
    );
    assert_eq!(arkham_ping_payload()["op"], "ping");
    assert_eq!(arkham_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}
