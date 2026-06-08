use chrono::Utc;
use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::Value;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private_parser::{parse_balances, parse_fills, parse_open_orders, parse_positions};
use super::signing::{action_hash, sign_l1_action, signing_address_from_private_key};
use super::streams;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("hyperliquid").unwrap()
}

fn btc_scope() -> SymbolScope {
    let exchange = exchange_id();
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDC").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC").unwrap(),
    }
}

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/hyperliquid/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("json")
}

#[test]
fn hyperliquid_parser_should_parse_public_fixtures() {
    let exchange = exchange_id();
    let rules = parse_symbol_rules(&exchange, &[btc_scope()], &fixture("meta.json")).unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00001"));

    let book = parse_orderbook_snapshot(&exchange, btc_scope(), &fixture("l2_book.json")).unwrap();
    assert_eq!(book.best_bid().unwrap().price, 65000.0);
    assert_eq!(book.best_ask().unwrap().price, 65001.0);
}

#[test]
fn hyperliquid_parser_should_parse_private_readback_fixtures() {
    let exchange = exchange_id();
    let tenant = TenantId::new("tenant").unwrap();
    let account = AccountId::new("account").unwrap();
    let balances = parse_balances(
        &exchange,
        tenant.clone(),
        account.clone(),
        &[],
        &fixture("clearinghouse_state.json"),
    )
    .unwrap();
    assert_eq!(balances[0].balances[0].asset, "USDC");

    let positions = parse_positions(
        &exchange,
        tenant.clone(),
        account.clone(),
        &fixture("clearinghouse_state.json"),
    )
    .unwrap();
    assert_eq!(positions[0].quantity, 0.02);

    let orders = parse_open_orders(&exchange, None, &fixture("open_orders.json")).unwrap();
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("123456"));

    let fills = parse_fills(&exchange, tenant, account, None, &fixture("fills.json")).unwrap();
    assert_eq!(fills[0].fill_id.as_deref(), Some("987654"));
}

#[test]
fn hyperliquid_signing_vector_should_build_signature() {
    let vector = fixture("signing_vectors/l1_action_order.json");
    let private_key = vector["private_key"].as_str().unwrap();
    assert_eq!(
        signing_address_from_private_key(private_key).unwrap(),
        vector["expected_signing_address"].as_str().unwrap()
    );
    let signature = sign_l1_action(
        private_key,
        &vector["action"],
        None,
        vector["nonce"].as_u64().unwrap(),
        None,
        vector["is_mainnet"].as_bool().unwrap(),
    )
    .unwrap();
    assert!(signature["r"].as_str().unwrap().starts_with("0x"));
}

#[test]
fn hyperliquid_request_specs_and_digest_vectors_should_be_wired() {
    let spec = fixture("request_specs/place_order_limit.json");
    assert_eq!(spec["method"], "POST");
    assert_eq!(spec["path"], "/exchange");
    assert_eq!(spec["auth"], "l1_action_signature");
    assert_eq!(spec["secret_free"], true);
    assert_eq!(spec["body"]["action"]["type"], "order");

    let vector = fixture("signing_vectors/order_action_digest.json");
    let action = &vector["action"];
    let nonce = vector["nonce"].as_u64().unwrap();
    let vault = vector["vault_address"].as_str();
    let hash = action_hash(action, vault, nonce, None).unwrap();
    assert_eq!(hash.len(), 32);
    let signature = sign_l1_action(
        vector["secret_key"].as_str().unwrap(),
        action,
        vault,
        nonce,
        None,
        vector["is_mainnet"].as_bool().unwrap(),
    )
    .unwrap();
    assert_eq!(signature["r"].as_str().unwrap().len(), 66);
    assert_eq!(signature["s"].as_str().unwrap().len(), 66);
    assert!(matches!(signature["v"].as_u64(), Some(27 | 28)));
}

#[test]
fn hyperliquid_ws_fixture_should_match_payload_helper() {
    let expected = fixture("ws/l2_book_subscribe.json");
    let payload = streams::hyperliquid_public_subscribe_payload(
        &rustcta_exchange_api::PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            symbol: btc_scope(),
            kind: rustcta_exchange_api::PublicStreamKind::OrderBookSnapshot,
        },
    )
    .unwrap();
    assert_eq!(payload, expected);
}
