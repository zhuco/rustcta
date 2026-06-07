use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{ToobitGatewayAdapter, ToobitGatewayConfig};

#[tokio::test]
async fn toobit_adapter_should_load_spot_and_perp_symbol_rules() {
    let exchange_info = json!({
        "code": 0,
        "symbols": [{
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01", "minPrice": "0.01", "maxPrice": "100000"},
                {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001", "maxQty": "100"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "10"}
            ]
        }],
        "contracts": [{
            "symbol": "BTC-SWAP-USDT",
            "status": "TRADING",
            "underlying": "BTC",
            "quoteAsset": "USDT",
            "marginToken": "USDT",
            "inverse": false,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}
            ]
        }]
    });
    let (base_url, seen) = spawn_rest_server(vec![exchange_info.clone(), exchange_info]).await;
    let adapter = ToobitGatewayAdapter::new(ToobitGatewayConfig {
        rest_base_url: base_url,
        ..ToobitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: Vec::new(),
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 2);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(
        response.rules[1].symbol.exchange_symbol.symbol,
        "BTC-SWAP-USDT"
    );
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v1/exchangeInfo");
}

#[tokio::test]
async fn toobit_adapter_should_load_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "t": 1700000000000i64,
        "b": [["65000", "0.1"]],
        "a": [["65001", "0.2"]]
    })])
    .await;
    let adapter = ToobitGatewayAdapter::new(ToobitGatewayConfig {
        rest_base_url: base_url,
        ..ToobitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(55),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 65000.0);
    assert_eq!(response.order_book.asks[0].quantity, 0.2);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/quote/v1/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTC-SWAP-USDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("100"));

    let _ = spot_symbol_scope();
}

#[test]
fn toobit_parser_fixtures_should_cover_empty_and_missing_symbol_rules() {
    let exchange = super::test_support::exchange_id();
    let empty = parse_symbol_rules(
        &exchange,
        rustcta_types::MarketType::Spot,
        &fixture("exchange_info_empty"),
    )
    .expect("empty spot rules");
    assert!(empty.is_empty());

    let error = parse_symbol_rules(
        &exchange,
        rustcta_types::MarketType::Spot,
        &fixture("exchange_info_missing_symbol"),
    )
    .expect_err("missing symbol should be rejected");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

#[test]
fn toobit_parser_fixtures_should_cover_orderbook_success_and_errors() {
    let exchange = super::test_support::exchange_id();
    let snapshot = parse_orderbook_snapshot(
        &exchange,
        spot_symbol_scope(),
        &fixture("orderbook"),
        u64::MAX,
    )
    .expect("orderbook fixture");
    assert_eq!(snapshot.bids[0].price, 3.9);
    assert_eq!(snapshot.asks[0].quantity, 12.0);

    let error = parse_orderbook_snapshot(
        &exchange,
        spot_symbol_scope(),
        &json!({"t": 1672035413265i64, "a": [["4.00000200", "12.00000000"]]}),
        u64::MAX,
    )
    .expect_err("missing bid levels should be rejected");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "exchange_info_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/toobit/exchange_info_empty.json")
        }
        "exchange_info_missing_symbol" => include_str!(
            "../../../../../tests/fixtures/exchanges/toobit/exchange_info_missing_symbol.json"
        ),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/toobit/orderbook.json")
        }
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
