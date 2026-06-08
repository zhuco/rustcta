use chrono::Utc;
use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::Value;

use super::parser::{parse_markets, parse_orderbook};
use super::private_parser::{parse_balances, parse_fills, parse_orders, parse_positions};
use super::signing::unsupported_node_signing_boundary;
use super::streams;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("dydx").unwrap()
}

fn btc_scope() -> SymbolScope {
    let exchange = exchange_id();
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC-USD").unwrap(),
    }
}

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/dydx/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("json")
}

#[test]
fn dydx_parser_should_parse_public_fixtures() {
    let exchange = exchange_id();
    let rules = parse_markets(
        &exchange,
        &[btc_scope()],
        &fixture("perpetual_markets.json"),
    )
    .unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.1"));

    let book = parse_orderbook(&exchange, btc_scope(), &fixture("orderbook.json")).unwrap();
    assert_eq!(book.best_bid().unwrap().price, 65000.0);
    assert_eq!(book.best_ask().unwrap().price, 65001.0);
}

#[test]
fn dydx_parser_should_parse_private_indexer_fixtures() {
    let exchange = exchange_id();
    let tenant = TenantId::new("tenant").unwrap();
    let account = AccountId::new("account").unwrap();
    let balances = parse_balances(
        &exchange,
        tenant.clone(),
        account.clone(),
        &fixture("subaccount.json"),
    )
    .unwrap();
    assert_eq!(balances[0].balances[0].asset, "USDC");

    let positions = parse_positions(
        &exchange,
        tenant.clone(),
        account.clone(),
        &[],
        &fixture("positions.json"),
    )
    .unwrap();
    assert_eq!(positions[0].quantity, 0.02);

    let orders = parse_orders(&exchange, None, &fixture("orders.json")).unwrap();
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("order-1"));

    let fills = parse_fills(&exchange, tenant, account, None, &fixture("fills.json")).unwrap();
    assert_eq!(fills[0].fill_id.as_deref(), Some("fill-1"));
}

#[test]
fn dydx_node_signing_fixture_should_remain_unsupported() {
    let boundary = fixture("signing_vectors/chain_signing_boundary.json");
    assert_eq!(boundary["status"], "unsupported");
    assert_eq!(boundary["secret_material"], "redacted_none");

    let fixture = fixture("signing_vectors/node_write_unsupported.json");
    assert_eq!(fixture["supported"], false);
    let error = unsupported_node_signing_boundary().expect_err("unsupported");
    assert!(error
        .to_string()
        .contains(fixture["expected_error"].as_str().unwrap()));
}

#[test]
fn dydx_request_specs_should_cover_private_indexer_readback() {
    let open_orders = fixture("request_specs/open_orders.json");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], "/v4/orders");
    assert_eq!(open_orders["auth"], "wallet_address_subaccount");
    assert_eq!(open_orders["secret_free"], true);
    assert_eq!(open_orders["query"]["status"], "OPEN");
}

#[test]
fn dydx_ws_fixture_should_match_payload_helper() {
    let expected = fixture("ws/orderbook_subscribe.json");
    let payload = streams::public_subscribe(&rustcta_exchange_api::PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        symbol: btc_scope(),
        kind: rustcta_exchange_api::PublicStreamKind::OrderBookSnapshot,
    });
    assert_eq!(payload, expected);
}
