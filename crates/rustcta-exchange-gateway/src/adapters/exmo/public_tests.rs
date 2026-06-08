use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, FeesRequest, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{ExmoGatewayAdapter, ExmoGatewayConfig};

#[tokio::test]
async fn exmo_adapter_should_load_symbol_rules_from_pair_settings() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "BTC_USD": {
            "min_quantity": "0.001",
            "max_quantity": "100",
            "min_price": "1",
            "max_price": "10000",
            "max_amount": "30000",
            "min_amount": "1",
            "price_precision": 2,
            "commission_taker_percent": "0.2",
            "commission_maker_percent": "0.1"
        }
    })])
    .await;
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        rest_base_url: base_url,
        ..ExmoGatewayConfig::default()
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
    assert_eq!(response.rules[0].quote_asset, "USD");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1.1/pair_settings");
}

#[tokio::test]
async fn exmo_adapter_should_load_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "BTC_USD": {
            "ask": [[100, 1, 100], [101, 2, 202]],
            "bid": [[99, 1, 99]]
        }
    })])
    .await;
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        rest_base_url: base_url,
        ..ExmoGatewayConfig::default()
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

    assert_eq!(response.order_book.bids[0].price, 99.0);
    assert_eq!(response.order_book.asks[0].quantity, 1.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1.1/order_book");
    assert_eq!(
        request.body.get("pair").map(String::as_str),
        Some("BTC_USD")
    );
    assert_eq!(request.body.get("limit").map(String::as_str), Some("7"));
}

#[tokio::test]
async fn exmo_adapter_should_parse_public_fee_rates_from_pair_settings() {
    let (base_url, _seen) = spawn_rest_server(vec![json!({
        "BTC_USD": {
            "min_quantity": "0.001",
            "max_quantity": "100",
            "min_price": "1",
            "max_price": "10000",
            "max_amount": "30000",
            "min_amount": "1",
            "price_precision": 2,
            "commission_taker_percent": "0.2",
            "commission_maker_percent": "0.1"
        }
    })])
    .await;
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        rest_base_url: base_url,
        ..ExmoGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees[0].maker_rate, "0.001");
    assert_eq!(response.fees[0].taker_rate, "0.002");
}

#[tokio::test]
async fn exmo_adapter_should_reject_non_spot_market_type() {
    let adapter = ExmoGatewayAdapter::default_public().expect("adapter");
    let mut symbol = symbol_scope();
    symbol.market_type = MarketType::Perpetual;
    symbol.exchange_symbol =
        ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC_USD").expect("symbol");
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book-perp"),
            symbol,
            depth: Some(5),
        })
        .await
        .expect_err("non-spot should be rejected");
    assert!(
        matches!(error, ExchangeApiError::Unsupported { operation } if operation == "exmo.non_spot_market_type")
    );
}
