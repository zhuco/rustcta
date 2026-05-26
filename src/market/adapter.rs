use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta, MarketEvent,
    MarketFundingSnapshot, OrderBook5,
};

pub type OrderBookSnapshot = OrderBook5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCapabilities {
    pub supports_orderbook5: bool,
    pub supports_trades: bool,
    pub supports_funding: bool,
    pub supports_mark_price: bool,
    pub supports_sequence: bool,
    pub supports_public_ws: bool,
    pub supports_rest_snapshot: bool,
}

impl MarketCapabilities {
    pub const fn new(
        supports_orderbook5: bool,
        supports_trades: bool,
        supports_funding: bool,
        supports_mark_price: bool,
        supports_sequence: bool,
    ) -> Self {
        Self {
            supports_orderbook5,
            supports_trades,
            supports_funding,
            supports_mark_price,
            supports_sequence,
            supports_public_ws: true,
            supports_rest_snapshot: true,
        }
    }

    pub const fn metadata_only() -> Self {
        Self {
            supports_orderbook5: false,
            supports_trades: false,
            supports_funding: false,
            supports_mark_price: false,
            supports_sequence: false,
            supports_public_ws: false,
            supports_rest_snapshot: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsSubscription {
    pub exchange: ExchangeId,
    pub channel: String,
    pub symbols: Vec<ExchangeSymbol>,
    pub route: Option<String>,
}

impl WsSubscription {
    pub fn new(
        exchange: ExchangeId,
        channel: impl Into<String>,
        symbols: Vec<ExchangeSymbol>,
    ) -> Self {
        Self {
            exchange,
            channel: channel.into(),
            symbols,
            route: None,
        }
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }
}

#[async_trait]
pub trait MarketDataAdapter: Send + Sync {
    fn exchange(&self) -> ExchangeId;

    fn capabilities(&self) -> MarketCapabilities;

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>>;

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>>;

    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription>;

    fn parse_public_ws_message(
        &self,
        raw: &str,
        recv_ts: DateTime<Utc>,
    ) -> anyhow::Result<Vec<MarketEvent>>;

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot>;
}
