use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeSymbol, MarketType, OrderSide};
use serde_json::json;

use super::parser::parse_symbol_rules;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{CryptoComGatewayAdapter, CryptoComGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    serde_json::from_str(match path {
        "success" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptocom/parser/symbol_rules_success.json"
        ),
        "empty" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptocom/parser/symbol_rules_empty.json"
        ),
        "error" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptocom/parser/symbol_rules_error.json"
        ),
        "missing_symbol" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptocom/parser/symbol_rules_missing_symbol.json"
        ),
        _ => unreachable!("unknown fixture"),
    })
    .expect("fixture json")
}

#[test]
fn cryptocom_parser_should_read_symbol_rule_fixtures() {
    let exchange = exchange_id();
    let success_fixture = fixture("success");
    let success =
        parse_symbol_rules(&exchange, &success_fixture["result"]).expect("success fixture");
    assert_eq!(success.len(), 1);
    assert_eq!(success[0].symbol.exchange_symbol.symbol, "BTC_USDT");

    let empty_fixture = fixture("empty");
    let empty = parse_symbol_rules(&exchange, &empty_fixture["result"]).expect("empty fixture");
    assert!(empty.is_empty());

    let error = parse_symbol_rules(&exchange, &fixture("error")).expect_err("error fixture");
    assert!(error
        .to_string()
        .contains("instrument response is not an array"));

    let missing_fixture = fixture("missing_symbol");
    let missing =
        parse_symbol_rules(&exchange, &missing_fixture["result"]).expect_err("missing symbol");
    assert!(missing
        .to_string()
        .contains("missing field instrument_name"));
}

#[tokio::test]
async fn cryptocom_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "id": 1,
        "method": "public/get-instruments",
        "code": 0,
        "result": {
            "data": [{
                "symbol": "BTC_USDT",
                "inst_type": "SPOT",
                "base_ccy": "BTC",
                "quote_ccy": "USDT",
                "price_tick_size": "0.01",
                "qty_tick_size": "0.000001",
                "min_quantity": "0.00001",
                "max_quantity": "1000",
                "min_notional": "1",
                "quote_decimals": 2,
                "quantity_decimals": 6,
                "tradable": true
            }, {
                "symbol": "BTCUSD-PERP",
                "inst_type": "PERPETUAL_SWAP",
                "base_ccy": "BTC",
                "quote_ccy": "USD",
                "price_tick_size": "0.1",
                "qty_tick_size": "0.001",
                "min_quantity": "0.001",
                "tradable": true
            }]
        }
    })])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope(), perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 2);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(response.rules[1].symbol.market_type, MarketType::Perpetual);
    assert!(response.rules[1].supports_reduce_only);
    assert_eq!(seen.lock().unwrap()[0].path, "/public/get-instruments");
}

#[tokio::test]
async fn cryptocom_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "id": 1,
        "method": "public/get-book",
        "code": 0,
        "result": {
            "data": [{
                "bids": [["65000.0", "1.25", "1"]],
                "asks": [["65001.0", "1.5", "1"]],
                "t": 1743054548123_i64,
                "u": 100
            }]
        }
    })])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(11),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(100));
    assert_eq!(response.order_book.bids[0].price, 65000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/public/get-book");
    assert_eq!(
        request.query.get("instrument_name").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(request.query.get("depth").map(String::as_str), Some("50"));
}

#[tokio::test]
async fn cryptocom_adapter_should_load_public_market_data_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "id": 1,
            "method": "public/get-ticker",
            "code": 0,
            "result": {
                "data": [{
                    "i": "BTCUSD-PERP",
                    "a": "61766.3",
                    "b": "61766.2",
                    "k": "61766.4",
                    "h": "62931.1",
                    "l": "60367.9",
                    "v": "6194.597",
                    "vv": "380074663.9595",
                    "oi": "2552.1189959",
                    "c": "736.8",
                    "t": 1780837531243_i64
                }]
            }
        }),
        json!({
            "id": 1,
            "method": "public/get-trades",
            "code": 0,
            "result": {
                "data": [{
                    "d": "9001",
                    "m": "match-1",
                    "s": "SELL",
                    "p": "61766.1",
                    "q": "0.001",
                    "t": 1780837532240_i64,
                    "tn": "1780837532240681339"
                }]
            }
        }),
        json!({
            "id": 1,
            "method": "public/get-candlestick",
            "code": 0,
            "result": {
                "data": [{
                    "t": 1780837200000_i64,
                    "o": "61700.1",
                    "h": "61800.2",
                    "l": "61600.3",
                    "c": "61750.4",
                    "v": "12.5"
                }]
            }
        }),
        json!({
            "id": 1,
            "method": "public/get-valuations",
            "code": 0,
            "result": {
                "instrument_name": "BTCUSD-PERP",
                "data": [{
                    "t": 1780837200000_i64,
                    "v": "61766.3"
                }]
            }
        }),
    ])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

    let ticker = adapter
        .get_ticker_24h(perp_symbol_scope())
        .await
        .expect("ticker");
    assert_eq!(ticker.last_price.as_deref(), Some("61766.3"));
    assert_eq!(ticker.bid_price.as_deref(), Some("61766.2"));
    assert_eq!(ticker.open_interest.as_deref(), Some("2552.1189959"));

    let trades = adapter
        .get_recent_public_trades(perp_symbol_scope())
        .await
        .expect("trades");
    assert_eq!(trades[0].trade_id.as_deref(), Some("9001"));
    assert_eq!(trades[0].side, OrderSide::Sell);
    assert_eq!(trades[0].price, "61766.1");

    let candles = adapter
        .get_candles(perp_symbol_scope(), "1m", Some(1))
        .await
        .expect("candles");
    assert_eq!(candles[0].interval, "1m");
    assert_eq!(candles[0].close, "61750.4");

    let valuations = adapter
        .get_valuations(perp_symbol_scope(), "mark", Some(1))
        .await
        .expect("valuations");
    assert_eq!(valuations[0].valuation_type, "mark_price");
    assert_eq!(valuations[0].value, "61766.3");

    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/public/get-ticker");
    assert_eq!(
        requests[0].query.get("instrument_name").map(String::as_str),
        Some("BTCUSD-PERP")
    );
    assert_eq!(requests[1].path, "/public/get-trades");
    assert_eq!(requests[2].path, "/public/get-candlestick");
    assert_eq!(
        requests[2].query.get("timeframe").map(String::as_str),
        Some("1m")
    );
    assert_eq!(
        requests[2].query.get("count").map(String::as_str),
        Some("1")
    );
    assert_eq!(requests[3].path, "/public/get-valuations");
    assert_eq!(
        requests[3].query.get("valuation_type").map(String::as_str),
        Some("mark_price")
    );
}

fn perp_symbol_scope() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSD-PERP")
            .expect("symbol"),
    }
}
