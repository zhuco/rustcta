#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    CredentialScope, ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind,
    PrivateStreamCapabilities, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_coinone_symbol;
use super::signing::signed_headers;
use super::CoinoneGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq)]
pub struct CoinoneWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
    pub payload_header: Option<String>,
    pub signature_header: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinonePublicOrderBookWsPolicy {
    pub url: &'static str,
    pub protocol: &'static str,
    pub channel: &'static str,
    pub request_type: &'static str,
    pub topic_fields: &'static [&'static str],
    pub first_message_semantics: &'static str,
    pub update_semantics: &'static str,
    pub depth: Option<u32>,
    pub interval_ms: Option<u64>,
    pub order_book_id_field: &'static str,
    pub order_book_id_semantics: &'static str,
    pub sequence_field: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub reconnect_resync: &'static str,
}

pub fn coinone_public_order_book_ws_policy() -> CoinonePublicOrderBookWsPolicy {
    CoinonePublicOrderBookWsPolicy {
        url: "wss://stream.coinone.co.kr",
        protocol: "json_websocket",
        channel: "ORDERBOOK",
        request_type: "SUBSCRIBE",
        topic_fields: &["quote_currency", "target_currency"],
        first_message_semantics: "initial subscription sends the last order book snapshot once",
        update_semantics: "after the initial snapshot, server pushes only when the order book changes",
        depth: None,
        interval_ms: None,
        order_book_id_field: "data.id",
        order_book_id_semantics: "larger order book id is newer; use as monotonic freshness only, not a documented contiguous sequence",
        sequence_field: None,
        checksum: None,
        reconnect_resync: "fetch REST /public/v2/orderbook/KRW snapshot on connect/reconnect, then resubscribe because no contiguous sequence or checksum is documented",
    }
}

pub fn coinone_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

impl CoinoneGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = coinone_public_subscription_spec(&self.config.public_ws_url, &subscription)?;
        Ok(format!("coinone:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coinone.subscribe_private_stream")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_spot(market_type)?;
        let channel = coinone_private_channel(&subscription.kind)?;
        Ok(format!(
            "coinone:private:{channel}:{}",
            subscription.account_id
        ))
    }
}

pub fn coinone_public_subscription_spec(
    ws_url: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<CoinoneWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinone.public_stream.market_type",
        });
    }
    let (base, quote) = normalize_coinone_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => "TRADE",
        PublicStreamKind::Ticker => "TICKER",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "ORDERBOOK",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinone.public_stream.candles",
            });
        }
    };
    Ok(CoinoneWsSubscriptionSpec {
        url: ws_url.trim_end_matches('/').to_string(),
        channel: channel.to_string(),
        subscribe_payload: json!({
            "request_type": "SUBSCRIBE",
            "channel": channel,
            "topic": {
                "quote_currency": quote,
                "target_currency": base
            }
        }),
        unsubscribe_payload: json!({
            "request_type": "UNSUBSCRIBE",
            "channel": channel,
            "topic": {
                "quote_currency": quote,
                "target_currency": base
            }
        }),
        payload_header: None,
        signature_header: None,
    })
}

pub fn coinone_orderbook_id(value: &Value) -> Option<&str> {
    let data = value
        .get("data")
        .or_else(|| value.get("d"))
        .unwrap_or(value);
    data.get("id")
        .or_else(|| data.get("i"))
        .and_then(Value::as_str)
}

pub fn coinone_orderbook_id_is_newer(previous: &str, next: &str) -> bool {
    match (previous.parse::<u128>(), next.parse::<u128>()) {
        (Ok(previous), Ok(next)) => next > previous,
        _ => next > previous,
    }
}

pub fn coinone_private_subscription_spec(
    ws_url: &str,
    subscription: &PrivateStreamSubscription,
    access_token: &str,
    secret_key: &str,
    nonce: &str,
) -> ExchangeApiResult<CoinoneWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinone.private_stream.market_type",
        });
    }
    let channel = coinone_private_channel(&subscription.kind)?;
    let auth_payload = json!({
        "access_token": access_token,
        "nonce": nonce,
    });
    let (payload, signature) = signed_headers(secret_key, &auth_payload)?;
    Ok(CoinoneWsSubscriptionSpec {
        url: ws_url.trim_end_matches('/').to_string(),
        channel: channel.to_string(),
        subscribe_payload: json!({
            "request_type": "SUBSCRIBE",
            "channel": channel,
        }),
        unsubscribe_payload: json!({
            "request_type": "UNSUBSCRIBE",
            "channel": channel,
        }),
        payload_header: Some(payload),
        signature_header: Some(signature),
    })
}

pub fn coinone_pong_response(value: &Value) -> Option<Value> {
    let kind = value
        .get("request_type")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)?;
    if !kind.eq_ignore_ascii_case("PING") && !kind.eq_ignore_ascii_case("ping") {
        return None;
    }
    Some(json!({"request_type": "PONG"}))
}

fn coinone_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("ORDER"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("BALANCE"),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "coinone.private_stream.positions",
        }),
    }
}

pub fn coinone_credential_scopes() -> Vec<CredentialScope> {
    vec![CredentialScope::ReadOnly, CredentialScope::Trade]
}
