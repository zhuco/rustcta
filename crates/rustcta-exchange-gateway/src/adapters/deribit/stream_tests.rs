use chrono::Utc;
use rustcta_exchange_api::{ExchangeStreamEvent, RequestContext};
use rustcta_types::{AccountId, ExchangeId, MarketType, TenantId};
use serde_json::Value;

use super::streams::{deribit_private_subscribe_payload, parse_deribit_private_stream_message};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("deribit").expect("exchange")
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
