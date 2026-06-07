use rustcta_exchange_api::SymbolScope;
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::Value;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};

fn fixture(name: &str) -> Value {
    let raw = match name {
        "instruments" => {
            include_str!("../../../../../tests/fixtures/exchanges/blofin/instruments.json")
        }
        "instruments_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/blofin/instruments_empty.json")
        }
        "error" => include_str!("../../../../../tests/fixtures/exchanges/blofin/error.json"),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/blofin/orderbook.json")
        }
        "orderbook_missing_bids" => include_str!(
            "../../../../../tests/fixtures/exchanges/blofin/orderbook_missing_bids.json"
        ),
        "funding" => include_str!("../../../../../tests/fixtures/exchanges/blofin/funding.json"),
        "open_interest" => {
            include_str!("../../../../../tests/fixtures/exchanges/blofin/open_interest.json")
        }
        other => panic!("unknown blofin fixture {other}"),
    };
    serde_json::from_str(raw).unwrap()
}

fn btc_scope() -> SymbolScope {
    let exchange = ExchangeId::new("blofin").unwrap();
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC-USDT").unwrap(),
    }
}

#[test]
fn parser_fixture_should_load_linear_perp_contract_specs_only() {
    let exchange = ExchangeId::new("blofin").unwrap();
    let rules = parse_symbol_rules(&exchange, &fixture("instruments")).unwrap();

    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC-USDT");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.001"));
    assert_eq!(rules[0].max_quantity.as_deref(), Some("100"));
    assert!(rules[0].supports_reduce_only);
    assert!(!rules.iter().any(|rule| {
        rule.symbol.exchange_symbol.symbol == "BTC-USDT-260626"
            || rule.symbol.market_type == MarketType::Option
    }));

    assert!(parse_symbol_rules(&exchange, &fixture("instruments_empty"))
        .unwrap()
        .is_empty());
    assert!(parse_symbol_rules(&exchange, &fixture("error"))
        .unwrap()
        .is_empty());
}

#[test]
fn parser_fixture_should_load_snapshot_orderbook_with_timestamp() {
    let exchange = ExchangeId::new("blofin").unwrap();
    let book = parse_orderbook_snapshot(&exchange, btc_scope(), &fixture("orderbook")).unwrap();

    assert_eq!(book.market_type, MarketType::Perpetual);
    assert_eq!(
        book.exchange_symbol
            .as_ref()
            .map(|symbol| symbol.symbol.as_str()),
        Some("BTC-USDT")
    );
    assert_eq!(book.bids[0].price, 65000.0);
    assert_eq!(book.bids[0].quantity, 1.25);
    assert_eq!(book.asks[0].price, 65001.0);
    assert_eq!(
        book.exchange_timestamp
            .map(|timestamp| timestamp.timestamp_millis()),
        Some(1_700_000_000_000)
    );
    assert!(book.sequence.is_none());

    assert!(
        parse_orderbook_snapshot(&exchange, btc_scope(), &fixture("orderbook_missing_bids"))
            .is_err()
    );
}

#[test]
fn raw_fixtures_should_document_funding_and_open_interest_boundary() {
    let funding = fixture("funding");
    assert_eq!(funding["data"][0]["instId"], "BTC-USDT");
    assert_eq!(funding["data"][0]["fundingRate"], "0.0001");

    let open_interest = fixture("open_interest");
    assert_eq!(
        open_interest["msg"],
        "documented unsupported by current verified BloFin OpenAPI mapping"
    );
    assert!(open_interest["data"].as_array().unwrap().is_empty());
}

#[test]
fn endpoint_mapping_should_cover_task14_required_sections() {
    let mapping = include_str!("endpoint_mapping.yaml");
    for required in [
        "exchange: blofin",
        "linear perpetual REST/WS only",
        "Spot, dated futures, and options are explicitly unsupported",
        "Contract boundary filters contractType=linear",
        "snapshot-only",
        "operation: batch_place_orders",
        "operation: batch_cancel_orders",
        "native_batch: true",
        "atomicity: partial",
        "operation: funding_rate",
        "operation: open_interest",
        "support: unsupported",
        "auth_renewal:",
        "mode: rest_reconciliation",
        "kill-switch",
        "disabled-symbol",
        "max-notional",
    ] {
        assert!(
            mapping.contains(required),
            "mapping missing required fragment: {required}"
        );
    }
}
