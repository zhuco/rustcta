use rustcta_exchange_api::{
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::signing::{sign_rest_payload, sign_ws_auth};
use super::streams::{
    bybit_heartbeat_payload, bybit_private_auth_payload, bybit_private_subscribe_payload,
    bybit_public_subscribe_payload, classify_ws_control,
};

#[test]
fn bybit_signing_should_match_fixture_vector() {
    let payload = "{\"category\":\"linear\",\"orderLinkId\":\"cli-place\",\"orderType\":\"Limit\",\"positionIdx\":1,\"price\":\"25000\",\"qty\":\"0.01\",\"side\":\"Buy\",\"symbol\":\"BTCUSDT\"}";
    let signature = sign_rest_payload("test-secret", "1700000000000", "test-key", "5000", payload)
        .expect("signature");
    assert_eq!(
        signature,
        "61ca803aa771613200b534a9e9e2ab4b46c7b7870baa675f3f83991500a9d783"
    );
}

#[test]
fn bybit_parser_should_parse_instruments_and_orderbook() {
    let exchange = exchange_id();
    let rules = parse_symbol_rules(
        &exchange,
        MarketType::Perpetual,
        &json!({
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "status": "Trading",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "priceFilter": {"tickSize": "0.10", "minPrice": "0.10"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"}
                }]
            }
        }),
    )
    .expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");
    assert!(rules[0].supports_reduce_only);

    let book = parse_orderbook_snapshot(
        &exchange,
        symbol_scope("BTCUSDT"),
        &json!({
            "result": {
                "b": [["25000", "1.2"]],
                "a": [["25000.5", "0.8"]],
                "u": 100,
                "ts": 1700000000000_i64
            }
        }),
    )
    .expect("book");
    assert_eq!(book.sequence, Some(100));
    assert_eq!(book.bids[0].quantity, 1.2);
}

#[test]
fn bybit_ws_payloads_should_match_v5_shapes() {
    let public = bybit_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope("BTCUSDT"),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("public payload");
    assert_eq!(public["op"], "subscribe");
    assert_eq!(public["args"][0], "orderbook.50.BTCUSDT");
    assert!(public.get("category").is_none());

    let private = bybit_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    })
    .expect("private payload");
    assert_eq!(private["args"][0], "order");

    let auth =
        bybit_private_auth_payload("test-key", "test-secret", 1700000060000).expect("auth payload");
    assert_eq!(auth["op"], "auth");
    assert_eq!(
        auth["args"][2],
        sign_ws_auth("test-secret", 1700000060000).expect("ws sign")
    );
    assert_eq!(bybit_heartbeat_payload()["op"], "ping");
    assert_eq!(classify_ws_control(&json!({"op": "pong"})).unwrap(), "pong");
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bybit").unwrap()
}

fn symbol_scope(symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, symbol).unwrap(),
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").unwrap()),
        account_id: Some(AccountId::new("account").unwrap()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}
