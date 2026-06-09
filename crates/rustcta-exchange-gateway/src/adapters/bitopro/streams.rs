#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::bitopro_ws_pair;
use super::signing::{payload_for_get_delete, sign_headers, BitoproSignedHeaders};

pub const BITOPRO_PUBLIC_ORDER_BOOK_LIMITS: [u32; 6] = [1, 5, 10, 20, 30, 50];
pub const BITOPRO_PUBLIC_ORDER_BOOK_DEFAULT_LIMIT: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitoproWsSpec {
    pub url: String,
    pub channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitoproPrivateWsSpec {
    pub url: String,
    pub channel: String,
    pub headers: BitoproSignedHeaders,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitoproPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub protocol: &'static str,
    pub channel_template: &'static str,
    pub request_model: &'static str,
    pub default_limit: u32,
    pub supported_limits: [u32; 6],
    pub interval_ms: u64,
    pub update_semantics: &'static str,
    pub sequence_field: Option<&'static str>,
    pub checksum: Option<&'static str>,
    pub sequence_risk: &'static str,
    pub reconnect_resync: &'static str,
}

pub fn bitopro_public_order_book_ws_policy() -> BitoproPublicOrderBookWsPolicy {
    BitoproPublicOrderBookWsPolicy {
        url: "wss://stream.bitopro.com:443/ws",
        protocol: "url_path_subscription",
        channel_template: "/v1/pub/order-books/{PAIR}:{limit}",
        request_model: "connect directly to the order-books URL path; multiple pairs use the pairs query parameter",
        default_limit: BITOPRO_PUBLIC_ORDER_BOOK_DEFAULT_LIMIT,
        supported_limits: BITOPRO_PUBLIC_ORDER_BOOK_LIMITS,
        interval_ms: 1_000,
        update_semantics: "full_order_book_snapshot_every_1s_when_updated",
        sequence_field: None,
        checksum: None,
        sequence_risk: "snapshot-only feed; eventID and timestamp are not book-continuity sequence/checksum fields",
        reconnect_resync: "fetch REST /order-book/{pair} snapshot and reconnect/resubscribe",
    }
}

pub fn bitopro_public_order_book_channel(pair: &str, limit: u32) -> ExchangeApiResult<String> {
    if !BITOPRO_PUBLIC_ORDER_BOOK_LIMITS.contains(&limit) {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitopro.public_order_book_stream.unsupported_limit",
        });
    }
    Ok(format!(
        "/v1/pub/order-books/{}:{limit}",
        bitopro_ws_pair(pair)?
    ))
}

pub fn bitopro_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: true,
            supports_positions: false,
            supports_account: true,
            order_event_kinds: vec![],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn public_stream_spec(
    subscription: &PublicStreamSubscription,
    base_url: &str,
) -> ExchangeApiResult<BitoproWsSpec> {
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitopro.public_stream.non_spot",
        });
    }
    let pair = bitopro_ws_pair(&subscription.symbol.exchange_symbol.symbol)?;
    let path = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot => {
            bitopro_public_order_book_channel(&pair, BITOPRO_PUBLIC_ORDER_BOOK_DEFAULT_LIMIT)?
        }
        PublicStreamKind::OrderBookDelta => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.public_stream.order_book_delta_unavailable",
            })
        }
        PublicStreamKind::Trades => format!("/v1/pub/trades/{pair}"),
        PublicStreamKind::Ticker => format!("/v1/pub/tickers/{pair}"),
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.public_stream.candles_unavailable",
            })
        }
    };
    Ok(BitoproWsSpec {
        url: format!("{}{}", base_url.trim_end_matches('/'), path),
        channel: path,
    })
}

pub fn private_stream_spec(
    subscription: &PrivateStreamSubscription,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    identity: &str,
    nonce_millis: i64,
) -> ExchangeApiResult<BitoproPrivateWsSpec> {
    if subscription
        .market_type
        .is_some_and(|market_type| market_type != MarketType::Spot)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitopro.private_stream.non_spot",
        });
    }
    let path = match subscription.kind {
        PrivateStreamKind::Orders => "/v1/pub/auth/orders",
        PrivateStreamKind::Fills => "/v1/pub/auth/user-trades",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "/v1/pub/auth/account-balance",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.private_stream.positions_unsupported_spot",
            })
        }
    };
    let payload = payload_for_get_delete(identity, nonce_millis)?;
    let headers = sign_headers(api_key, api_secret, payload)?;
    Ok(BitoproPrivateWsSpec {
        url: format!("{}{}", base_url.trim_end_matches('/'), path),
        channel: path.to_string(),
        headers,
    })
}

pub fn heartbeat_policy_ms() -> (u64, u64) {
    (20_000, 5_000)
}

impl super::BitoproGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let spec = public_stream_spec(&subscription, &self.config.websocket_base_url)?;
        Ok(format!("bitopro:{}", spec.url))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.private_stream.disabled_rest_reconciliation_fallback",
            });
        }
        let api_key = self
            .config
            .api_key
            .as_deref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitopro.private_stream.missing_api_key",
            })?;
        let api_secret =
            self.config
                .api_secret
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "bitopro.private_stream.missing_api_secret",
                })?;
        let identity =
            self.config
                .api_identity
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "bitopro.private_stream.missing_identity",
                })?;
        let spec = private_stream_spec(
            &subscription,
            &self.config.websocket_base_url,
            api_key,
            api_secret,
            identity,
            chrono::Utc::now().timestamp_millis(),
        )?;
        Ok(format!(
            "bitopro:private:{}:{}",
            subscription.account_id, spec.url
        ))
    }
}
