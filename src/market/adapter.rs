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
    pub profile: PublicBookProfileKind,
    pub depth: u16,
    pub expected_push_interval_ms: Option<u64>,
    pub supports_sequence: bool,
    pub supports_checksum: bool,
    pub requires_rest_snapshot: bool,
    pub requires_local_merge: bool,
    pub max_symbols_per_connection: Option<usize>,
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
            profile: PublicBookProfileKind::FastestDepth,
            depth: 5,
            expected_push_interval_ms: None,
            supports_sequence: false,
            supports_checksum: false,
            requires_rest_snapshot: false,
            requires_local_merge: false,
            max_symbols_per_connection: None,
        }
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    pub fn with_profile(mut self, profile: PublicBookProfile) -> Self {
        self.profile = profile.kind;
        self.depth = profile.depth;
        self.expected_push_interval_ms = profile.expected_push_interval_ms;
        self.supports_sequence = profile.supports_sequence;
        self.supports_checksum = profile.supports_checksum;
        self.requires_rest_snapshot = profile.requires_rest_snapshot;
        self.requires_local_merge = profile.requires_local_merge;
        self.max_symbols_per_connection = profile.max_symbols_per_connection;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicBookProfileKind {
    FastestL1,
    FastestDepth,
    ConservativeDepth,
}

impl PublicBookProfileKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FastestL1 => "fastest_l1",
            Self::FastestDepth => "fastest_depth",
            Self::ConservativeDepth => "conservative_depth",
        }
    }
}

impl std::fmt::Display for PublicBookProfileKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PublicBookProfile {
    pub kind: PublicBookProfileKind,
    pub channel: &'static str,
    pub depth: u16,
    pub expected_push_interval_ms: Option<u64>,
    pub supports_sequence: bool,
    pub supports_checksum: bool,
    pub requires_rest_snapshot: bool,
    pub requires_local_merge: bool,
    pub max_symbols_per_connection: Option<usize>,
    pub primary_fast_leg: bool,
}

impl PublicBookProfile {
    pub const fn new(
        kind: PublicBookProfileKind,
        channel: &'static str,
        depth: u16,
        expected_push_interval_ms: Option<u64>,
    ) -> Self {
        Self {
            kind,
            channel,
            depth,
            expected_push_interval_ms,
            supports_sequence: false,
            supports_checksum: false,
            requires_rest_snapshot: false,
            requires_local_merge: false,
            max_symbols_per_connection: None,
            primary_fast_leg: true,
        }
    }

    pub const fn with_sequence(mut self, supports_sequence: bool) -> Self {
        self.supports_sequence = supports_sequence;
        self
    }

    pub const fn with_checksum(mut self, supports_checksum: bool) -> Self {
        self.supports_checksum = supports_checksum;
        self
    }

    pub const fn with_rest_snapshot(mut self, requires_rest_snapshot: bool) -> Self {
        self.requires_rest_snapshot = requires_rest_snapshot;
        self
    }

    pub const fn with_local_merge(mut self, requires_local_merge: bool) -> Self {
        self.requires_local_merge = requires_local_merge;
        self
    }

    pub const fn with_max_symbols_per_connection(mut self, max_symbols: usize) -> Self {
        self.max_symbols_per_connection = Some(max_symbols);
        self
    }

    pub const fn with_primary_fast_leg(mut self, primary_fast_leg: bool) -> Self {
        self.primary_fast_leg = primary_fast_leg;
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

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        Vec::new()
    }

    fn build_public_ws_subscriptions_for_profile(
        &self,
        symbols: &[ExchangeSymbol],
        profile: PublicBookProfileKind,
    ) -> Vec<WsSubscription> {
        let _ = profile;
        self.build_public_ws_subscriptions(symbols)
    }

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
