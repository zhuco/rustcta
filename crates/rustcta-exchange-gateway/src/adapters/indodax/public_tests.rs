use rustcta_exchange_api::SymbolScope;
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::json;

use super::parser::{normalize_indodax_symbol, parse_orderbook_snapshot, parse_symbol_rules};

#[test]
fn indodax_public_parser_should_keep_idr_pairs_and_parse_rules() {
    let exchange = ExchangeId::new("indodax").expect("exchange");
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/indodax/pairs.json"
    ))
    .expect("fixture");
    let rules = parse_symbol_rules(&exchange, &value).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc_idr");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "IDR");
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.00000001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("10000"));
    assert!(rules.iter().all(|rule| rule.quote_asset == "IDR"));
    assert!(normalize_indodax_symbol("btc_usdt").is_err());
}

#[test]
fn indodax_public_parser_should_parse_depth_snapshot() {
    let exchange = ExchangeId::new("indodax").expect("exchange");
    let symbol = SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "IDR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange.clone(), MarketType::Spot, "btc_idr")
            .expect("symbol"),
    };
    let value = json!({
        "buy": [["100000000", "0.25"], ["99900000", "0.5"]],
        "sell": [["100100000", "0.1"]]
    });
    let book = parse_orderbook_snapshot(&exchange, symbol, &value, 5000).expect("book");
    assert_eq!(book.bids[0].price, 100000000.0);
    assert_eq!(book.bids[0].quantity, 0.25);
    assert_eq!(book.asks[0].price, 100100000.0);
}
