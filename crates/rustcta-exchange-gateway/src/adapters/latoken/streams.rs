#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::OrderBookSnapshot;
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, split_symbol};
use super::signing::{ws_auth_signature, LatokenDigest};
use super::LatokenGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const LATOKEN_PUBLIC_BOOK_SUBSCRIPTION_ID: &str = "latoken-public-book";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatokenPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub destination_template: &'static str,
    pub interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub sequence_field: &'static str,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub resync: &'static str,
}

pub fn latoken_public_order_book_ws_policy() -> LatokenPublicOrderBookWsPolicy {
    LatokenPublicOrderBookWsPolicy {
        url: "wss://api.latoken.com/stomp",
        destination_template: "/v1/book/{base}/{quote}",
        interval_ms: None,
        depth: None,
        sequence_field: "nonce",
        checksum: None,
        update_semantics: "STOMP book messages carry the documented bid/ask book payload; official docs do not publish fixed depth or cadence",
        resync: "track monotonic nonce per symbol; reconnect and resubscribe, then rebuild from REST /v2/book/{base}/{quote}, when nonce is missing, gaps, or regresses",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatokenNonceContinuity {
    First,
    Continuous,
    Gap { expected: u64, actual: u64 },
    NotIncreasing { previous: u64, actual: u64 },
}

impl LatokenNonceContinuity {
    pub fn requires_reconnect(self) -> bool {
        matches!(
            self,
            LatokenNonceContinuity::Gap { .. } | LatokenNonceContinuity::NotIncreasing { .. }
        )
    }
}

pub fn latoken_check_nonce_continuity(
    previous: Option<u64>,
    actual: u64,
) -> LatokenNonceContinuity {
    let Some(previous) = previous else {
        return LatokenNonceContinuity::First;
    };
    match actual.cmp(&previous.saturating_add(1)) {
        std::cmp::Ordering::Equal => LatokenNonceContinuity::Continuous,
        std::cmp::Ordering::Greater => LatokenNonceContinuity::Gap {
            expected: previous.saturating_add(1),
            actual,
        },
        std::cmp::Ordering::Less => LatokenNonceContinuity::NotIncreasing { previous, actual },
    }
}

impl LatokenGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let destination = latoken_public_book_destination_for_kind(&subscription)?;
        let subscription_id = subscription
            .context
            .request_id
            .as_deref()
            .unwrap_or(LATOKEN_PUBLIC_BOOK_SUBSCRIPTION_ID);
        Ok(stomp_subscribe_frame(subscription_id, &destination))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "latoken.private_streams_user_id_auth_unverified",
        })
    }
}

pub fn latoken_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn public_book_destination(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
    Ok(format!("/v1/book/{base}/{quote}"))
}

pub fn public_trade_destination(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let (base, quote) = split_symbol(&symbol.exchange_symbol.symbol)?;
    Ok(format!("/v1/trade/{base}/{quote}"))
}

pub fn latoken_public_book_subscribe_frame(
    subscription: &PublicStreamSubscription,
    subscription_id: &str,
) -> ExchangeApiResult<String> {
    let destination = latoken_public_book_destination_for_kind(subscription)?;
    Ok(stomp_subscribe_frame(subscription_id, &destination))
}

pub fn latoken_public_book_destination_for_kind(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<String> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    match subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            public_book_destination(&subscription.symbol)
        }
        PublicStreamKind::Trades => Err(ExchangeApiError::Unsupported {
            operation: "latoken.public_ws_trades_out_of_scope",
        }),
        PublicStreamKind::Ticker => Err(ExchangeApiError::Unsupported {
            operation: "latoken.public_ws_ticker_out_of_scope",
        }),
        PublicStreamKind::Candles { .. } => Err(ExchangeApiError::Unsupported {
            operation: "latoken.public_ws_candles_out_of_scope",
        }),
    }
}

pub fn stomp_subscribe_frame(subscription_id: &str, destination: &str) -> String {
    format!("SUBSCRIBE\nid:{subscription_id}\ndestination:{destination}\n\n\u{0}")
}

pub fn stomp_unsubscribe_frame(subscription_id: &str) -> String {
    format!("UNSUBSCRIBE\nid:{subscription_id}\n\n\u{0}")
}

pub fn ws_auth_headers(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: &str,
) -> ExchangeApiResult<Value> {
    let signature = ws_auth_signature(api_secret, timestamp_ms, LatokenDigest::Sha256)?;
    Ok(json!({
        "X-LA-APIKEY": api_key,
        "X-LA-DIGEST": "HMAC-SHA256",
        "X-LA-SIGNATURE": signature,
        "X-LA-SIGDATA": timestamp_ms
    }))
}

pub fn parse_public_book_message(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value.get("payload").unwrap_or(value);
    let mut snapshot = parse_orderbook_snapshot(exchange_id, symbol, 1000, payload)?;
    snapshot.sequence = value_as_u64(value.get("nonce"))
        .or_else(|| value_as_u64(payload.get("nonce")))
        .or(snapshot.sequence);
    Ok(snapshot)
}

pub fn public_book_nonce(value: &Value) -> Option<u64> {
    value_as_u64(value.get("nonce")).or_else(|| {
        value
            .get("payload")
            .and_then(|payload| value_as_u64(payload.get("nonce")))
    })
}

pub fn rest_reconciliation_fallback() -> &'static str {
    "private state remains REST request-spec-only: /v2/auth/order/active and /v2/auth/trade are the documented reconciliation paths after a separate private REST validation task"
}

pub fn heartbeat_policy() -> &'static str {
    "LATOKEN STOMP docs do not publish a fixed heartbeat interval; runtime should reconnect and resubscribe when nonce sequence gaps are detected"
}

fn value_as_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str()?.trim().parse().ok())
    })
}
