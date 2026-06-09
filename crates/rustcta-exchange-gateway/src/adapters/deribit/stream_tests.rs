use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeStreamEvent, PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::{json, Value};

use super::streams::{
    deribit_book_change_ids, deribit_change_id_is_contiguous, deribit_private_subscribe_payload,
    deribit_public_order_book_channel, deribit_public_subscribe_payload,
    parse_deribit_private_stream_message, parse_deribit_public_stream_message,
    DeribitBookStreamOptions, DeribitPublicStreamMessage, DeribitPublicWsSession,
};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("deribit").expect("exchange")
}

fn perpetual_symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC-PERPETUAL")
            .expect("exchange symbol"),
    }
}

fn public_book_subscription(kind: PublicStreamKind) -> PublicStreamSubscription {
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        symbol: perpetual_symbol(),
        kind,
    }
}

#[test]
fn deribit_public_book_subscription_should_use_structured_group_depth_interval_channel() {
    let subscription = public_book_subscription(PublicStreamKind::OrderBookDelta);
    let payload = deribit_public_subscribe_payload(&subscription, 42).expect("payload");
    assert_eq!(payload["method"], "public/subscribe");
    assert_eq!(
        payload["params"]["channels"][0],
        "book.BTC-PERPETUAL.none.20.100ms"
    );

    let agg2 = deribit_public_order_book_channel(
        &subscription,
        DeribitBookStreamOptions {
            group: "none",
            depth: 1,
            interval: "agg2",
        },
    )
    .expect("agg2 channel");
    assert_eq!(agg2, "book.BTC-PERPETUAL.none.1.agg2");

    let raw = deribit_public_order_book_channel(
        &subscription,
        DeribitBookStreamOptions {
            group: "10",
            depth: 10,
            interval: "raw",
        },
    )
    .expect("raw channel");
    assert_eq!(raw, "book.BTC-PERPETUAL.10.10.raw");

    assert!(deribit_public_order_book_channel(
        &subscription,
        DeribitBookStreamOptions {
            group: "none",
            depth: 50,
            interval: "100ms",
        },
    )
    .is_err());
}

#[test]
fn deribit_public_book_parser_should_parse_grouped_fixture_and_change_ids() {
    let value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/ws/public_book_grouped.json"
    ))
    .expect("fixture");
    let message = parse_deribit_public_stream_message(&exchange_id(), perpetual_symbol(), &value)
        .expect("message");
    let DeribitPublicStreamMessage::OrderBook(book) = message else {
        panic!("expected order book");
    };
    assert_eq!(book.market_type, MarketType::Perpetual);
    assert_eq!(book.sequence, Some(412346));
    assert_eq!(book.best_bid().expect("bid").price, 67000.5);
    assert_eq!(book.best_ask().expect("ask").quantity, 300.0);

    let change_ids = deribit_book_change_ids(&value).expect("change ids");
    assert_eq!(change_ids.change_id, 412346);
    assert_eq!(change_ids.prev_change_id, Some(412345));
    assert!(deribit_change_id_is_contiguous(Some(412345), change_ids));
    assert!(!deribit_change_id_is_contiguous(Some(412344), change_ids));
}

#[test]
fn deribit_public_session_should_mark_resync_on_change_id_gap() {
    let mut session = DeribitPublicWsSession::new(
        exchange_id(),
        "wss://www.deribit.com/ws/api/v2".to_string(),
        public_book_subscription(PublicStreamKind::OrderBookDelta),
    )
    .expect("session");
    session
        .handle_text_message(
            &json!({
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "book.BTC-PERPETUAL.none.20.100ms",
                    "data": {
                        "timestamp": 1719500000000i64,
                        "instrument_name": "BTC-PERPETUAL",
                        "change_id": 100,
                        "bids": [[67000.0, 100.0]],
                        "asks": [[67001.0, 100.0]]
                    }
                }
            })
            .to_string(),
        )
        .expect("first book");
    assert!(!session.state().sequence_gap_detected);

    session
        .handle_text_message(
            &json!({
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "book.BTC-PERPETUAL.none.20.100ms",
                    "data": {
                        "timestamp": 1719500000100i64,
                        "instrument_name": "BTC-PERPETUAL",
                        "change_id": 101,
                        "prev_change_id": 99,
                        "bids": [[67000.5, 100.0]],
                        "asks": [[67001.5, 100.0]]
                    }
                }
            })
            .to_string(),
        )
        .expect("gap book");
    assert!(session.state().sequence_gap_detected);
    assert_eq!(
        session.state().last_resync_reason,
        Some(rustcta_exchange_api::ResyncReason::SequenceGap)
    );
}

#[test]
fn deribit_private_ws_should_parse_order_update() {
    let value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/ws_private_order.json"
    ))
    .expect("fixture");
    let events = parse_deribit_private_stream_message(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("main").expect("account"),
        None,
        &value,
    )
    .expect("events");
    assert!(matches!(events[0], ExchangeStreamEvent::OrderUpdate(_)));
}

#[test]
fn deribit_private_subscribe_payload_should_use_option_scope() {
    let subscription = rustcta_exchange_api::PrivateStreamSubscription {
        schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        exchange: exchange_id(),
        market_type: Some(MarketType::Option),
        account_id: AccountId::new("main").expect("account"),
        kind: rustcta_exchange_api::PrivateStreamKind::Orders,
    };
    let payload = deribit_private_subscribe_payload(&subscription, 7).expect("payload");
    assert_eq!(payload["method"], "private/subscribe");
    assert_eq!(payload["params"]["channels"][0], "user.orders.option.raw");
}
