use rustcta_exchange_api::SymbolScope;
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::Value;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("deribit").expect("exchange")
}

fn option_symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Option,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id(),
            MarketType::Option,
            "BTC-28JUN24-70000-C",
        )
        .expect("exchange symbol"),
    }
}

#[test]
fn deribit_symbol_rules_should_parse_options_without_settlement_as_canonical_quote() {
    let value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/instruments_options.json"
    ))
    .expect("fixture");
    let rules = parse_symbol_rules(&exchange_id(), &value).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.market_type, MarketType::Option);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USD");
    assert!(!rules[0].supports_market_orders);
    assert!(rules[0].supports_limit_orders);
}

#[test]
fn deribit_order_book_should_parse_option_snapshot() {
    let value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/orderbook_option.json"
    ))
    .expect("fixture");
    let book =
        parse_orderbook_snapshot(&exchange_id(), option_symbol(), &value).expect("order book");
    assert_eq!(book.bids[0].price, 0.082);
    assert_eq!(book.asks[0].price, 0.083);
    assert_eq!(book.sequence, Some(412345));
}
