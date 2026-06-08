use rustcta_exchange_api::{
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::Value;

use super::streams::{
    parse_upbit_stream_message, upbit_heartbeat_policy_ms, upbit_public_subscribe_payload,
    UpbitStreamMessage,
};

fn fixture(name: &str) -> Value {
    let text = match name {
        "markets.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/upbit/markets.json")
        }
        "orderbook.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/upbit/orderbook.json")
        }
        "accounts.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/upbit/accounts.json")
        }
        "order.json" => include_str!("../../../../../tests/fixtures/exchanges/upbit/order.json"),
        "fills.json" => include_str!("../../../../../tests/fixtures/exchanges/upbit/fills.json"),
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/upbit/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/upbit/request_specs/cancel_order.json"
        ),
        "signing_vectors/jwt_query_hash.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/upbit/signing_vectors/jwt_query_hash.json"
        ),
        "ws/orderbook_snapshot.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/upbit/ws/orderbook_snapshot.json")
        }
        _ => panic!("unknown upbit fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture JSON")
}

#[test]
fn upbit_signing_vector_should_cover_query_hash() {
    let vector = fixture("signing_vectors/jwt_query_hash.json");
    let params = vector["params"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            (
                item["key"].as_str().unwrap().to_string(),
                item["value"].as_str().unwrap().to_string(),
            )
        })
        .collect::<Vec<_>>();
    let parts = super::signing::upbit_jwt(
        vector["access_key"].as_str().unwrap(),
        vector["secret_key"].as_str().unwrap(),
        &params,
        vector["nonce"].as_str().unwrap(),
    )
    .expect("jwt");
    assert_eq!(parts.query_string, vector["query_string"]);
    assert_eq!(parts.query_hash.as_deref(), vector["query_hash"].as_str());
    assert_eq!(parts.token, vector["jwt"]);
}

#[test]
fn upbit_request_spec_fixtures_should_cover_private_writes() {
    let place = fixture("request_specs/place_order_limit.json");
    assert_eq!(place["operation"], "upbit.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/v1/orders");
    assert_eq!(place["body"]["market"], "KRW-BTC");

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "upbit.cancel_order");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["query"]["uuid"], "upbit-order-1");
}

#[test]
fn upbit_public_parser_fixtures_should_parse_krw_btc_and_usdt_markets() {
    let exchange = exchange_id();
    let rules = super::parser::parse_symbol_rules(&exchange, &fixture("markets.json")).unwrap();
    assert_eq!(rules.len(), 3);
    assert!(rules.iter().any(|rule| rule.quote_asset == "KRW"));
    assert!(rules.iter().any(|rule| rule.quote_asset == "BTC"));
    assert!(rules.iter().any(|rule| rule.quote_asset == "USDT"));

    let book = super::parser::parse_orderbook_snapshot(
        &exchange,
        symbol_scope(),
        &fixture("orderbook.json"),
    )
    .unwrap();
    assert_eq!(book.best_bid().unwrap().price, 69_990_000.0);
    assert_eq!(book.best_ask().unwrap().price, 70_010_000.0);
}

#[test]
fn upbit_private_parser_fixtures_should_parse_balances_order_and_fills() {
    let balances = super::parser::parse_balances(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").unwrap(),
        rustcta_types::AccountId::new("account").unwrap(),
        &[],
        &fixture("accounts.json"),
    )
    .unwrap();
    assert_eq!(balances[0].balances[0].asset, "BTC");

    let order = super::parser::parse_order(
        &exchange_id(),
        Some(&symbol_scope()),
        &fixture("order.json"),
    )
    .unwrap();
    assert_eq!(order.exchange_order_id.as_deref(), Some("upbit-order-1"));

    let fills = super::parser::parse_fills(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").unwrap(),
        rustcta_types::AccountId::new("account").unwrap(),
        Some(&symbol_scope()),
        &fixture("fills.json"),
    )
    .unwrap();
    assert_eq!(fills[0].fill_id.as_deref(), Some("upbit-fill-1"));
}

#[test]
fn upbit_ws_specs_should_cover_payload_and_parser() {
    let payload = upbit_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        },
        "ticket",
    )
    .unwrap();
    assert_eq!(payload[1]["type"], "orderbook");
    assert_eq!(payload[1]["codes"][0], "KRW-BTC");
    assert_eq!(upbit_heartbeat_policy_ms(), (30_000, 90_000));
    assert!(matches!(
        parse_upbit_stream_message(&fixture("ws/orderbook_snapshot.json")),
        UpbitStreamMessage::Snapshot(_)
    ));
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("upbit").unwrap()
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "KRW").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "KRW-BTC").unwrap(),
    }
}
