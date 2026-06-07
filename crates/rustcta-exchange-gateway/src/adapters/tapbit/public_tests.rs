use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeErrorClass;
use serde_json::json;
use serde_json::Value;

use super::parser::{parse_spot_orderbook_snapshot, parse_spot_symbol_rules};
use super::test_support::{
    context, exchange_id, perp_symbol_scope, spawn_rest_server, spot_symbol_scope,
};
use super::{TapbitGatewayAdapter, TapbitGatewayConfig};

#[tokio::test]
async fn tapbit_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("spot_symbols")]).await;
    let adapter = TapbitGatewayAdapter::new(TapbitGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..TapbitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/spot/instruments/trade_pair_list");
}

#[tokio::test]
async fn tapbit_adapter_should_load_spot_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook")]).await;
    let adapter = TapbitGatewayAdapter::new(TapbitGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..TapbitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[0].quantity, 1.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/spot/instruments/depth");
    assert_eq!(
        request.query.get("instrument_id").map(String::as_str),
        Some("BTC%2FUSDT")
    );
    assert_eq!(request.query.get("depth").map(String::as_str), Some("10"));
}

#[tokio::test]
async fn tapbit_adapter_should_load_perpetual_public_rules_and_book() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 200,
            "data": [{
                "contract_code": "BTC-SWAP",
                "multiplier": "0.001",
                "min_amount": "1",
                "max_amount": "20000",
                "min_price_change": "0.1",
                "price_precision": "1"
            }]
        }),
        json!({
            "code": 200,
            "data": {
                "asks": [["100.5", "1.5"]],
                "bids": [["99.5", "2.0"]],
                "timestamp": 1700000000000i64
            }
        }),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(TapbitGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..TapbitGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");
    assert_eq!(
        rules.rules[0].symbol.market_type,
        rustcta_types::MarketType::Perpetual
    );
    assert_eq!(rules.rules[0].price_increment.as_deref(), Some("0.1"));

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-book"),
            symbol: perp_symbol_scope(),
            depth: Some(50),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.bids[0].quantity, 2.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/api/usdt/instruments/list");
    assert_eq!(requests[1].path, "/api/usdt/instruments/depth");
    assert_eq!(
        requests[1].query.get("instrument_id").map(String::as_str),
        Some("BTC")
    );
    assert_eq!(
        requests[1].query.get("depth").map(String::as_str),
        Some("50")
    );
}

#[tokio::test]
async fn tapbit_adapter_should_expose_spot_public_market_helpers() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": {"timestamp": 1700000000000i64}}),
        json!({
            "code": 200,
            "data": {
                "trade_pair_name": "BTC/USDT",
                "last_price": "65000.1",
                "highest_bid": "65000.0",
                "lowest_ask": "65000.2",
                "highest_price_24h": "66000",
                "lowest_price_24h": "64000",
                "volume24h": "123.4",
                "timestamp": 1700000000001i64
            }
        }),
        json!({"code": 200, "data": [["BTC/USDT", "65000.1", "0.2", "buy", 1700000000002i64]]}),
        json!({"code": 200, "data": [[1700000000000i64, "64000", "66000", "63000", "65000", "42", "2730000"]]}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(TapbitGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..TapbitGatewayConfig::default()
    })
    .expect("adapter");

    assert_eq!(
        adapter.get_spot_server_time().await.expect("time"),
        1700000000000
    );
    let ticker = adapter.get_spot_ticker("BTCUSDT").await.expect("ticker");
    assert_eq!(ticker.last_price, "65000.1");
    assert_eq!(ticker.bid_price.as_deref(), Some("65000.0"));
    let trades = adapter
        .get_spot_public_trades("BTC/USDT")
        .await
        .expect("trades");
    assert_eq!(trades[0].quantity, "0.2");
    assert_eq!(trades[0].side.as_deref(), Some("buy"));
    let klines = adapter
        .get_spot_klines("BTCUSDT", "1d")
        .await
        .expect("klines");
    assert_eq!(klines[0].close, "65000");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/api/spot/instruments/current/timestamp");
    assert_eq!(requests[1].path, "/api/spot/instruments/ticker_one");
    assert_eq!(
        requests[1].query.get("instrument_id").map(String::as_str),
        Some("BTC%2FUSDT")
    );
    assert_eq!(requests[2].path, "/api/spot/instruments/trade_list");
    assert_eq!(requests[3].path, "/api/spot/instruments/candles");
    assert_eq!(
        requests[3].query.get("period").map(String::as_str),
        Some("D")
    );
}

#[tokio::test]
async fn tapbit_adapter_should_expose_perpetual_public_market_helpers() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": {"timestamp": 1700000000000i64}}),
        json!({
            "code": 200,
            "data": {
                "contract_code": "BTC-SWAP",
                "last_price": "65000.1",
                "mark_price": "65001",
                "highest_price_24h": "66000",
                "lowest_price_24h": "64000",
                "volume24h": "123.4"
            }
        }),
        json!({"code": 200, "data": [["65000.1", "buy", "0.2", 1700000000002i64]]}),
        json!({"code": 200, "data": [[1700000000000i64, "64000", "66000", "63000", "65000", "42"]]}),
        json!({"code": 200, "data": {"funding_rate": "0.000236"}}),
    ])
    .await;
    let adapter = TapbitGatewayAdapter::new(TapbitGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..TapbitGatewayConfig::default()
    })
    .expect("adapter");

    assert_eq!(
        adapter.get_perpetual_server_time().await.expect("time"),
        1700000000000
    );
    let ticker = adapter
        .get_perpetual_ticker("BTCUSDT")
        .await
        .expect("ticker");
    assert_eq!(ticker.mark_price.as_deref(), Some("65001"));
    let trades = adapter
        .get_perpetual_public_trades("BTC-SWAP")
        .await
        .expect("trades");
    assert_eq!(trades[0].symbol.as_deref(), Some("BTC-SWAP"));
    let klines = adapter
        .get_perpetual_klines("BTCUSDT", "5m")
        .await
        .expect("klines");
    assert_eq!(klines[0].open, "64000");
    let funding = adapter
        .get_perpetual_funding_rate("BTCUSDT")
        .await
        .expect("funding");
    assert_eq!(funding.funding_rate, "0.000236");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/api/v1/usdt/time");
    assert_eq!(requests[1].path, "/api/usdt/instruments/ticker_one");
    assert_eq!(
        requests[1].query.get("instrument_id").map(String::as_str),
        Some("BTC")
    );
    assert_eq!(requests[2].path, "/api/usdt/instruments/trade_list");
    assert_eq!(requests[3].path, "/api/usdt/instruments/candles");
    assert_eq!(
        requests[3].query.get("period").map(String::as_str),
        Some("5")
    );
    assert_eq!(requests[4].path, "/api/usdt/instruments/funding_rate");
}

#[test]
fn tapbit_parser_fixtures_should_cover_success_empty_error_and_missing_fields() {
    let exchange = exchange_id();

    let symbols = parse_spot_symbol_rules(&exchange, &fixture("spot_symbols")).expect("symbols");
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].base_asset, "BTC");

    let book = parse_spot_orderbook_snapshot(&exchange, spot_symbol_scope(), &fixture("orderbook"))
        .expect("orderbook");
    assert_eq!(book.bids[0].price, 99.5);

    let empty = parse_spot_symbol_rules(&exchange, &fixture("empty")).expect("empty symbols");
    assert!(empty.is_empty());

    let missing = parse_spot_symbol_rules(&exchange, &fixture("missing_required_field"))
        .expect_err("missing base/quote should fail");
    assert!(
        matches!(missing, ExchangeApiError::Exchange(error) if error.class == ExchangeErrorClass::Decode)
    );

    let transport_error =
        super::transport::classify_tapbit_error_for_tests(Some(429), "too frequent requests");
    assert_eq!(transport_error, ExchangeErrorClass::RateLimited);
    assert_eq!(fixture("error")["code"], json!(429));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "spot_symbols" => {
            include_str!("../../../../../tests/fixtures/exchanges/tapbit/spot_symbols.json")
        }
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/tapbit/orderbook.json")
        }
        "empty" => include_str!("../../../../../tests/fixtures/exchanges/tapbit/empty.json"),
        "error" => include_str!("../../../../../tests/fixtures/exchanges/tapbit/error.json"),
        "missing_required_field" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/tapbit/missing_required_field.json"
            )
        }
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
