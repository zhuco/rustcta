use rustcta_exchange_api::{
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::{json, Value};

use super::streams::{
    blofin_private_algo_orders_subscribe_payload, blofin_private_subscribe_payload,
    blofin_public_books5_subscribe_payload, blofin_public_funding_rate_subscribe_payload,
    blofin_public_subscribe_payload, blofin_ws_login_payload, parse_blofin_private_stream_message,
    parse_blofin_public_stream_message,
};

fn fixture(name: &str) -> Value {
    let raw = match name {
        "ws_order" => include_str!("../../../../../tests/fixtures/exchanges/blofin/ws_order.json"),
        other => panic!("unknown blofin stream fixture {other}"),
    };
    serde_json::from_str(raw).unwrap()
}

fn btc_scope() -> rustcta_exchange_api::SymbolScope {
    let exchange = ExchangeId::new("blofin").unwrap();
    rustcta_exchange_api::SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC-USDT").unwrap(),
    }
}

#[test]
fn public_subscribe_payload_should_use_official_channel_shape() {
    let payload = blofin_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(chrono::Utc::now()),
        symbol: btc_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .unwrap();
    assert_eq!(
        payload,
        json!({
            "op": "subscribe",
            "args": [{"channel": "books", "instId": "BTC-USDT"}],
        })
    );
}

#[test]
fn private_subscribe_payload_should_use_orders_channel() {
    let exchange = ExchangeId::new("blofin").unwrap();
    let payload = blofin_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(chrono::Utc::now()),
        exchange,
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("acct").unwrap(),
        kind: PrivateStreamKind::Orders,
    })
    .unwrap();
    assert_eq!(
        payload,
        json!({
            "op": "subscribe",
            "args": [{"channel": "orders"}],
        })
    );
}

#[test]
fn adapter_specific_ws_payloads_should_cover_funding_books5_and_algo_orders() {
    assert_eq!(
        blofin_public_funding_rate_subscribe_payload("btcusdt").unwrap(),
        json!({
            "op": "subscribe",
            "args": [{"channel": "funding-rate", "instId": "BTC-USDT"}],
        })
    );
    assert_eq!(
        blofin_public_books5_subscribe_payload("BTC-USDT").unwrap(),
        json!({
            "op": "subscribe",
            "args": [{"channel": "books5", "instId": "BTC-USDT"}],
        })
    );
    assert_eq!(
        blofin_private_algo_orders_subscribe_payload(Some("ethusdt")).unwrap(),
        json!({
            "op": "subscribe",
            "args": [{"channel": "orders-algo", "instId": "ETH-USDT"}],
        })
    );
}

#[test]
fn login_payload_should_include_passphrase_and_fixed_verify_signature() {
    let payload =
        blofin_ws_login_payload("key", "secret", "pass", "nonce", "1700000000000").unwrap();
    assert_eq!(payload["op"], "login");
    assert_eq!(payload["args"][0]["apiKey"], "key");
    assert_eq!(payload["args"][0]["passphrase"], "pass");
    assert!(payload["args"][0]["sign"].as_str().unwrap().ends_with('='));
}

#[test]
fn public_stream_parser_should_emit_order_book_snapshot() {
    let exchange = ExchangeId::new("blofin").unwrap();
    let event = parse_blofin_public_stream_message(
        &exchange,
        &btc_scope(),
        &json!({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": {
                "asks": [["101", "2"]],
                "bids": [["100", "1"]],
                "ts": "1700000000000"
            }
        }),
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        event,
        rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot(_)
    ));
}

#[test]
fn private_stream_parser_should_emit_order_update() {
    let exchange = ExchangeId::new("blofin").unwrap();
    let events = parse_blofin_private_stream_message(
        &exchange,
        rustcta_types::TenantId::new("tenant").unwrap(),
        AccountId::new("acct").unwrap(),
        &fixture("ws_order"),
    )
    .unwrap();
    assert!(matches!(
        events.first(),
        Some(rustcta_exchange_api::ExchangeStreamEvent::OrderUpdate(_))
    ));
}
