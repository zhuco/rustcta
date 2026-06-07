use rustcta_exchange_api::{
    ExchangeClient, OrderBookRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_order_book, parse_symbol_rules};
use super::test_support::{context, exchange_id, spawn_rest_server, spot_symbol_scope};
use super::{BigOneGatewayAdapter, BigOneGatewayConfig};

#[test]
fn bigone_parser_should_parse_spot_symbol_rules() {
    let value = fixture("spot_symbol_rules.json");
    let response =
        parse_symbol_rules(&exchange_id(), rustcta_types::MarketType::Spot, &value).expect("rules");
    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BTC-USDT");
    assert!(response.rules[0].supports_market_orders);
}

#[test]
fn bigone_parser_should_parse_order_book_snapshot() {
    let value = fixture("orderbook.json");
    let response = parse_order_book(&exchange_id(), spot_symbol_scope(), &value).expect("book");
    assert_eq!(response.order_book.bids[0].price, 65000.0);
    assert_eq!(response.order_book.sequence, Some(42));
}

#[tokio::test]
async fn bigone_public_rest_should_request_spot_depth_by_asset_pair_path() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "data": {
            "bids": [["100", "1"]],
            "asks": [["101", "2"]]
        }
    })])
    .await;
    let adapter = BigOneGatewayAdapter::new(BigOneGatewayConfig {
        spot_rest_base_url: base_url,
        ..BigOneGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-book"),
            symbol: spot_symbol_scope(),
            depth: Some(50),
        })
        .await
        .expect("order book");

    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v3/asset_pairs/BTC-USDT/depth");
    assert_eq!(
        requests[0].query.get("limit").map(String::as_str),
        Some("50")
    );
}

#[test]
fn bigone_public_parser_should_cover_contract_and_empty_fixtures() {
    let contract = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Perpetual,
        &fixture("contract_symbol_rules.json"),
    )
    .expect("contract rules");
    assert_eq!(contract.rules.len(), 1);
    assert!(contract.rules[0].supports_reduce_only);

    let empty = parse_symbol_rules(
        &exchange_id(),
        rustcta_types::MarketType::Spot,
        &fixture("empty_response.json"),
    )
    .expect("empty rules");
    assert!(empty.rules.is_empty());

    let book = parse_order_book(
        &exchange_id(),
        spot_symbol_scope(),
        &fixture("empty_response.json"),
    )
    .expect("empty book");
    assert!(book.order_book.bids.is_empty());
    assert!(book.order_book.asks.is_empty());
}

#[allow(dead_code)]
fn _request_examples() -> (SymbolRulesRequest, OrderBookRequest) {
    (
        SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope()],
        },
        OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(50),
        },
    )
}

fn fixture(name: &str) -> serde_json::Value {
    serde_json::from_str(match name {
        "spot_symbol_rules.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/spot_symbol_rules.json")
        }
        "contract_symbol_rules.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bigone/contract_symbol_rules.json"
        ),
        "orderbook.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/orderbook.json")
        }
        "empty_response.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/empty_response.json")
        }
        other => panic!("unknown BigONE fixture {other}"),
    })
    .expect("fixture json")
}
