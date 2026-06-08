use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeId;
use serde_json::{json, Value};

use super::parser::{parse_orderbook, split_market};
use super::private_parser::parse_fills;
use crate::adapters::response_metadata;

pub fn dydx_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn public_subscribe(subscription: &PublicStreamSubscription) -> Value {
    let channel = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => "v4_orderbook",
        PublicStreamKind::Trades => "v4_trades",
        PublicStreamKind::Ticker => "v4_markets",
        PublicStreamKind::Candles { .. } => "v4_candles",
    };
    json!({
        "type": "subscribe",
        "channel": channel,
        "id": subscription.symbol.exchange_symbol.symbol,
        "batched": false
    })
}

pub fn public_unsubscribe(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "type": "unsubscribe",
        "channel": "v4_orderbook",
        "id": subscription.symbol.exchange_symbol.symbol
    })
}

pub fn private_subscribe(
    subscription: &PrivateStreamSubscription,
    wallet: &str,
    subaccount: u32,
) -> Value {
    let channel = match subscription.kind {
        PrivateStreamKind::Orders
        | PrivateStreamKind::Fills
        | PrivateStreamKind::Balances
        | PrivateStreamKind::Positions
        | PrivateStreamKind::Account => "v4_subaccounts",
    };
    json!({
        "type": "subscribe",
        "channel": channel,
        "id": format!("{wallet}/{subaccount}"),
        "batched": false
    })
}

#[allow(dead_code)]
pub fn parse_dydx_stream_event(
    exchange_id: &ExchangeId,
    tenant_id: Option<TenantId>,
    account_id: Option<AccountId>,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if value.get("type").and_then(Value::as_str) == Some("connected") {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: chrono::Utc::now(),
        }));
    }
    match value.get("channel").and_then(Value::as_str) {
        Some("v4_orderbook") => {
            let symbol = value.get("id").and_then(Value::as_str).ok_or_else(|| {
                ExchangeApiError::Serialization {
                    message: "dydx orderbook stream missing id".to_string(),
                }
            })?;
            let (base, quote) = split_market(symbol);
            let scope = rustcta_exchange_api::SymbolScope {
                exchange: exchange_id.clone(),
                market_type: rustcta_types::MarketType::Perpetual,
                canonical_symbol: Some(rustcta_types::CanonicalSymbol::new(base, quote).map_err(
                    |error| ExchangeApiError::InvalidRequest {
                        message: error.to_string(),
                    },
                )?),
                exchange_symbol: rustcta_types::ExchangeSymbol::new(
                    exchange_id.clone(),
                    rustcta_types::MarketType::Perpetual,
                    symbol,
                )
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                })?,
            };
            let contents = value.get("contents").unwrap_or(value);
            Ok(Some(ExchangeStreamEvent::OrderBookSnapshot(
                OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    order_book: parse_orderbook(exchange_id, scope, contents)?,
                },
            )))
        }
        Some("v4_subaccounts") => {
            let (Some(tenant_id), Some(account_id)) = (tenant_id, account_id) else {
                return Ok(None);
            };
            let fills = value
                .get("contents")
                .and_then(|contents| contents.get("fills"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            let mut fills = parse_fills(exchange_id, tenant_id, account_id, None, &fills)?;
            Ok(fills.pop().map(ExchangeStreamEvent::Fill))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rustcta_exchange_api::{
        ExchangeStreamEvent, PublicStreamKind, PublicStreamSubscription, RequestContext,
        SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
    };
    use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};

    use super::{parse_dydx_stream_event, public_subscribe};

    #[test]
    fn dydx_stream_payload_should_subscribe_orderbook() {
        let exchange = ExchangeId::new("dydx").unwrap();
        let payload = public_subscribe(&PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            symbol: SymbolScope {
                exchange: exchange.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").unwrap()),
                exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC-USD")
                    .unwrap(),
            },
            kind: PublicStreamKind::OrderBookSnapshot,
        });
        assert_eq!(payload["type"], "subscribe");
        assert_eq!(payload["channel"], "v4_orderbook");
        assert_eq!(payload["id"], "BTC-USD");
    }

    #[test]
    fn dydx_stream_parser_should_cover_orderbook_and_subaccount_fill() {
        let exchange = ExchangeId::new("dydx").unwrap();
        let orderbook = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/dydx/ws_orderbook.json"
        ))
        .unwrap();
        let parsed = parse_dydx_stream_event(&exchange, None, None, &orderbook)
            .unwrap()
            .unwrap();
        match parsed {
            ExchangeStreamEvent::OrderBookSnapshot(response) => {
                assert_eq!(response.order_book.best_bid().unwrap().price, 65000.0);
            }
            other => panic!("unexpected stream event {other:?}"),
        }

        let fill = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/dydx/ws_subaccount_fill.json"
        ))
        .unwrap();
        let parsed = parse_dydx_stream_event(
            &exchange,
            Some(rustcta_exchange_api::TenantId::new("tenant").unwrap()),
            Some(rustcta_exchange_api::AccountId::new("account").unwrap()),
            &fill,
        )
        .unwrap()
        .unwrap();
        match parsed {
            ExchangeStreamEvent::Fill(fill) => {
                assert_eq!(fill.fill_id.as_deref(), Some("fill-fixture-1"));
            }
            other => panic!("unexpected stream event {other:?}"),
        }
    }
}
