use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser;
use super::test_support::{context, spawn_rest_server, spot_symbol_scope, swap_symbol_scope};
use super::{DigiFinexGatewayAdapter, DigiFinexGatewayConfig};

#[tokio::test]
async fn digifinex_adapter_should_load_spot_symbol_rules() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/digifinex/spot_markets.json"
    ))
    .expect("fixture");
    let (base_url, _seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = DigiFinexGatewayAdapter::new(DigiFinexGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..DigiFinexGatewayConfig::default()
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
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "btc_usdt");
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5"));
}

#[tokio::test]
async fn digifinex_adapter_should_load_swap_order_book() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/digifinex/swap_orderbook.json"
    ))
    .expect("fixture");
    let (base_url, _seen) = spawn_rest_server(vec![fixture]).await;
    let adapter = DigiFinexGatewayAdapter::new(DigiFinexGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        swap_rest_base_url: base_url,
        ..DigiFinexGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: swap_symbol_scope(),
            depth: Some(50),
        })
        .await
        .expect("book");
    assert_eq!(response.order_book.bids[0].price, 65000.0);
    assert_eq!(response.order_book.sequence, Some(10));
}

#[test]
fn digifinex_parser_should_cover_empty_error_and_missing_fixtures() {
    let exchange = super::test_support::exchange_id();
    let empty = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/digifinex/empty_response.json"
    ))
    .expect("empty fixture");
    let error = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/digifinex/error_response.json"
    ))
    .expect("error fixture");
    let missing = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/digifinex/missing_required_fields.json"
    ))
    .expect("missing fixture");

    assert!(
        parser::parse_symbol_rules(&exchange, MarketType::Spot, &empty)
            .expect("empty rules")
            .is_empty()
    );
    assert!(parser::parse_orderbook_snapshot(&exchange, spot_symbol_scope(), &error).is_err());
    assert!(
        parser::parse_symbol_rules(&exchange, MarketType::Spot, &missing)
            .expect("missing symbol rules")
            .is_empty()
    );
}
