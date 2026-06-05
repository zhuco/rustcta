use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{OrderBookLevel, SymbolRule};
use crate::execution::FeeSource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpotVenue {
    Mexc,
    CoinEx,
    GateIo,
    Bitget,
}

impl SpotVenue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mexc => "mexc",
            Self::CoinEx => "coinex",
            Self::GateIo => "gateio",
            Self::Bitget => "bitget",
        }
    }

    pub fn other(self) -> Self {
        match self {
            Self::Mexc => Self::CoinEx,
            Self::CoinEx => Self::Mexc,
            Self::GateIo => Self::Bitget,
            Self::Bitget => Self::GateIo,
        }
    }
}

pub fn configured_spot_pair(exchanges: &[String]) -> (SpotVenue, SpotVenue) {
    let has_gateio = exchanges.iter().any(|exchange| {
        matches!(
            exchange.trim().to_ascii_lowercase().as_str(),
            "gate" | "gateio" | "gate.io"
        )
    });
    let has_bitget = exchanges
        .iter()
        .any(|exchange| exchange.trim().eq_ignore_ascii_case("bitget"));
    if has_gateio && has_bitget {
        (SpotVenue::GateIo, SpotVenue::Bitget)
    } else {
        (SpotVenue::Mexc, SpotVenue::CoinEx)
    }
}

#[derive(Debug, Clone)]
pub struct CommonSymbolRules {
    pub mexc: SymbolRule,
    pub coinex: SymbolRule,
    pub gateio: Option<SymbolRule>,
    pub bitget: Option<SymbolRule>,
}

impl CommonSymbolRules {
    pub fn for_exchange(&self, exchange: SpotVenue) -> &SymbolRule {
        match exchange {
            SpotVenue::Mexc => &self.mexc,
            SpotVenue::CoinEx => &self.coinex,
            SpotVenue::GateIo => self.gateio.as_ref().unwrap_or(&self.mexc),
            SpotVenue::Bitget => self.bitget.as_ref().unwrap_or(&self.coinex),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    StaleBook,
    InsufficientDepth,
    MinNotional,
    SymbolRule,
    InsufficientQuoteBalance,
    InsufficientBaseBalance,
    NetSpreadBelowThreshold,
    NotionalLimit,
    Cooldown,
    ExchangeHealth,
    DailyLossLimit,
    TradeLossLimit,
    ConsecutiveRejections,
    SymbolBlacklisted,
    DisabledSymbol,
    DisabledExchange,
    DisabledExchangeSymbol,
    ControlPlaneBlocked,
    PaperExecutionRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSource {
    Websocket,
    Rest,
    Replay,
}

impl BookSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::Rest => "rest",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBook {
    pub exchange: String,
    pub symbol: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    pub sequence: Option<u64>,
    pub source: BookSource,
    pub is_stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub raw_spread_bps: f64,
    pub buy_fee_bps: f64,
    pub sell_fee_bps: f64,
    pub fee_source_buy: FeeSource,
    pub fee_source_sell: FeeSource,
    pub platform_discount_applied: bool,
    pub estimated_fee_bps: f64,
    pub estimated_slippage_bps: f64,
    pub safety_buffer_bps: f64,
    pub estimated_net_spread_bps: f64,
    pub estimated_total_fee: f64,
    pub estimated_gross_pnl: f64,
    pub estimated_net_pnl: f64,
    pub executable_notional: f64,
    pub quantity: f64,
    pub accepted: bool,
    pub rejection_reason: Option<RejectionReason>,
    pub rejection_detail: Option<String>,
    pub buy_book_age_ms: i64,
    pub sell_book_age_ms: i64,
    pub buy_book_source: BookSource,
    pub sell_book_source: BookSource,
    pub buy_latency_ms: Option<i64>,
    pub sell_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedTradeRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_avg_price: f64,
    pub sell_avg_price: f64,
    pub quantity: f64,
    pub notional: f64,
    pub buy_fee: f64,
    pub sell_fee: f64,
    pub gross_pnl: f64,
    pub net_pnl: f64,
    pub latency_ms: i64,
    pub order_book_age_ms: i64,
    pub execution_mode: String,
}
