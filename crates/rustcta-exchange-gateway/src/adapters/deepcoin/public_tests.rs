use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{
    context, exchange_id, fixture, perp_symbol_scope, spawn_rest_server, spot_symbol_scope,
};
use super::{DeepcoinGatewayAdapter, DeepcoinGatewayConfig};

#[tokio::test]
async fn deepcoin_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("spot_symbol_rules")]).await;
    let adapter = DeepcoinGatewayAdapter::new(DeepcoinGatewayConfig {
        rest_base_url: base_url,
        ..DeepcoinGatewayConfig::default()
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
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/deepcoin/market/instruments");
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("SPOT")
    );
}

#[tokio::test]
async fn deepcoin_adapter_should_load_perp_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook_perp")]).await;
    let adapter = DeepcoinGatewayAdapter::new(DeepcoinGatewayConfig {
        rest_base_url: base_url,
        ..DeepcoinGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(500),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/deepcoin/market/books");
    assert_eq!(
        request.query.get("instId").map(String::as_str),
        Some("BTC-USDT-SWAP")
    );
    assert_eq!(request.query.get("sz").map(String::as_str), Some("400"));
}

#[test]
fn deepcoin_public_parser_should_cover_empty_and_missing_field_fixtures() {
    let empty_rules = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Spot,
        &fixture("empty_array"),
    )
    .expect("empty symbol rules");
    assert!(empty_rules.is_empty());

    let missing_field = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Spot,
        &fixture("symbol_rules_missing_inst_id"),
    )
    .expect_err("missing instId should fail");
    assert!(matches!(missing_field, ExchangeApiError::Exchange(_)));

    let missing_levels =
        parse_orderbook_snapshot(&exchange_id(), perp_symbol_scope(), &fixture("empty_array"))
            .expect_err("missing orderbook levels should fail");
    assert!(matches!(missing_levels, ExchangeApiError::Exchange(_)));
}
