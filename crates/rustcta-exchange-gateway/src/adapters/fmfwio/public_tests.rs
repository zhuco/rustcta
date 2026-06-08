use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use crate::adapters::AdapterBackedGateway;

use super::test_support::{
    config_with_base_url, context, exchange_id, spawn_rest_server, symbol_scope,
};
use super::FmfwioGatewayAdapter;

#[tokio::test]
async fn fmfwio_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "ETHBTC": {
            "type": "spot",
            "base_currency": "ETH",
            "quote_currency": "BTC",
            "status": "working",
            "quantity_increment": "0.001",
            "tick_size": "0.000001",
            "take_rate": "0.001",
            "make_rate": "-0.0001",
            "fee_currency": "BTC",
            "margin_trading": true,
            "max_initial_leverage": "10.00"
        },
        "BTCUSDT_PERP": {
            "type": "futures",
            "base_currency": "BTC",
            "quote_currency": "USDT",
            "status": "working",
            "quantity_increment": "0.001",
            "tick_size": "0.1"
        }
    })])
    .await;
    let adapter = FmfwioGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "ETH");
    assert_eq!(response.rules[0].quote_asset, "BTC");
    assert_eq!(
        response.rules[0].price_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.001")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/3/public/symbol");
}

#[tokio::test]
async fn fmfwio_adapter_should_load_order_book_snapshot_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "timestamp": "2024-04-11T11:30:38.597950917Z",
        "ask": [["0.046016", "1.250"], ["0.046100", "2.0"]],
        "bid": [["0.046001", "0.500"], ["0.046000", "3.0"]]
    })])
    .await;
    let adapter = FmfwioGatewayAdapter::new(config_with_base_url(base_url)).expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(1),
        })
        .await
        .expect("order book");

    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.bids[0].price, 0.046001);
    assert_eq!(response.order_book.asks[0].quantity, 1.25);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/3/public/orderbook/ETHBTC");
    assert_eq!(request.query.get("depth").map(String::as_str), Some("1"));
}

#[tokio::test]
async fn fmfwio_adapter_should_reject_non_spot_scope() {
    let adapter = FmfwioGatewayAdapter::default_public().expect("adapter");
    let mut symbol = symbol_scope();
    symbol.market_type = MarketType::Perpetual;
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("non-spot"),
            symbol,
            depth: Some(5),
        })
        .await
        .expect_err("non spot unsupported");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[test]
fn fmfwio_gateway_should_register_named_adapter() {
    let duplicate = AdapterBackedGateway::with_named_adapters("test", ["fmfwio", "fmfw.io"]);
    assert!(duplicate.is_err());
    assert!(format!("{:?}", duplicate.err()).contains("already registered"));

    let gateway = AdapterBackedGateway::with_named_adapters("test", ["fmfwio"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
    assert_eq!(exchange_id().as_str(), "fmfwio");
}
