use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{CoinoneGatewayAdapter, CoinoneGatewayConfig};

#[tokio::test]
async fn coinone_adapter_should_load_krw_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "error_code": "0",
        "markets": [{
            "quote_currency": "KRW",
            "target_currency": "BTC",
            "price_unit": "1000",
            "qty_unit": "0.00000001",
            "min_order_amount": "5000",
            "trade_status": "NORMAL"
        }]
    })])
    .await;
    let adapter = CoinoneGatewayAdapter::new(CoinoneGatewayConfig {
        rest_base_url: base_url,
        ..CoinoneGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "KRW");
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5000"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/public/v2/markets/KRW");
    assert_eq!(
        request.query.get("quote_currency").map(String::as_str),
        Some("KRW")
    );
}

#[tokio::test]
async fn coinone_adapter_should_load_krw_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "success",
        "error_code": "0",
        "orderbook": {
            "timestamp": 1710000000000_i64,
            "bids": [{"price": "99900000", "qty": "0.25"}],
            "asks": [{"price": "100100000", "qty": "0.5"}]
        }
    })])
    .await;
    let adapter = CoinoneGatewayAdapter::new(CoinoneGatewayConfig {
        rest_base_url: base_url,
        ..CoinoneGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99_900_000.0);
    assert_eq!(response.order_book.asks[0].quantity, 0.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/public/v2/orderbook/KRW");
    assert_eq!(
        request.query.get("target_currency").map(String::as_str),
        Some("BTC")
    );
    assert_eq!(request.query.get("size").map(String::as_str), Some("15"));
}

#[tokio::test]
async fn coinone_adapter_should_reject_non_krw_symbols() {
    let adapter = CoinoneGatewayAdapter::default_public().expect("adapter");
    let mut symbol = symbol_scope();
    symbol.exchange_symbol =
        ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC-USDT").expect("symbol");
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book-usdt"),
            symbol,
            depth: Some(5),
        })
        .await
        .expect_err("non-KRW symbol should be rejected");
    assert!(
        matches!(error, ExchangeApiError::Unsupported { operation } if operation == "coinone.non_krw_market")
    );
}
