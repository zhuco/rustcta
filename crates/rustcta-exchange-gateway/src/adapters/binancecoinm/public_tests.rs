use rustcta_exchange_api::{
    BatchAtomicity, BatchExecutionMode, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::{BinanceCoinMGatewayAdapter, BinanceCoinMGatewayConfig};
use crate::adapters::binancecoinm::test_support::{
    context, exchange_id, spawn_rest_server, symbol_scope,
};

#[test]
fn binancecoinm_parser_should_preserve_inverse_contract_symbols() {
    let rules = parse_symbol_rules(
        &exchange_id(),
        &json!({
            "symbols": [{
                "symbol": "BTCUSD_PERP",
                "pair": "BTCUSD",
                "contractType": "PERPETUAL",
                "contractStatus": "TRADING",
                "contractSize": 100,
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "marginAsset": "BTC",
                "pricePrecision": 1,
                "quantityPrecision": 0,
                "orderTypes": ["LIMIT", "MARKET"],
                "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": "0.1", "maxPrice": "1000000", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "minQty": "1", "maxQty": "100000", "stepSize": "1"}
                ]
            }, {
                "symbol": "BTCUSD_240628",
                "pair": "BTCUSD",
                "contractType": "CURRENT_QUARTER",
                "contractStatus": "TRADING",
                "contractSize": 100,
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "marginAsset": "BTC",
                "orderTypes": ["LIMIT", "MARKET"],
                "timeInForce": ["GTC"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "minQty": "1", "stepSize": "1"}
                ]
            }]
        }),
    )
    .expect("rules");

    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSD_PERP");
    assert_eq!(rules[0].quote_asset, "USD");
    assert!(rules[0].supports_post_only);
    assert!(rules[0].supports_reduce_only);
    assert_eq!(rules[1].symbol.market_type, MarketType::Futures);
    assert_eq!(rules[1].symbol.exchange_symbol.symbol, "BTCUSD_240628");
}

#[test]
fn binancecoinm_parser_should_parse_orderbook_snapshot() {
    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope("BTCUSD_PERP"),
        &json!({
            "lastUpdateId": 42,
            "E": 1710000000123_i64,
            "bids": [["25000.0", "3"], ["24999.9", "2"]],
            "asks": [["25000.1", "4"]]
        }),
    )
    .expect("orderbook");

    assert_eq!(book.market_type, MarketType::Perpetual);
    assert_eq!(book.exchange_symbol.unwrap().symbol, "BTCUSD_PERP");
    assert_eq!(book.sequence, Some(42));
    assert_eq!(book.bids[0].quantity, 3.0);
}

#[test]
fn binancecoinm_capabilities_should_expose_native_batch_runtime_when_private_rest_enabled() {
    let adapter = BinanceCoinMGatewayAdapter::new(BinanceCoinMGatewayConfig {
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BinanceCoinMGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.max_items,
        Some(10)
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "batch_place_orders"
                && endpoint.path.as_deref() == Some("/dapi/v1/batchOrders")
        }));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "batch_cancel_orders"
                && endpoint.path.as_deref() == Some("/dapi/v1/batchOrders")
        }));
}

#[tokio::test]
async fn binancecoinm_adapter_should_load_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "symbols": [{
                "symbol": "BTCUSD_PERP",
                "contractType": "PERPETUAL",
                "contractStatus": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "orderTypes": ["LIMIT", "MARKET"],
                "timeInForce": ["GTC", "GTX"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "minQty": "1", "stepSize": "1"}
                ]
            }]
        }),
        json!({
            "lastUpdateId": 43,
            "bids": [["25000.0", "1"]],
            "asks": [["25001.0", "1"]]
        }),
    ])
    .await;
    let adapter = BinanceCoinMGatewayAdapter::new(BinanceCoinMGatewayConfig {
        rest_base_url: base_url,
        ..BinanceCoinMGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("symbol-rules"),
            symbols: vec![symbol_scope("BTCUSD_PERP")],
        })
        .await
        .expect("rules");
    assert_eq!(rules.rules[0].symbol.market_type, MarketType::Perpetual);

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-book"),
            symbol: symbol_scope("BTCUSD_PERP"),
            depth: Some(10),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.sequence, Some(43));

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].path, "/dapi/v1/exchangeInfo");
    assert_eq!(seen[1].path, "/dapi/v1/depth");
    assert_eq!(
        seen[1].query.get("symbol").map(String::as_str),
        Some("BTCUSD_PERP")
    );
}
