use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    AccountId, BalancesResponse, ExchangeId, Fill, MarketType, OrderBookResponse, OrderState,
    PositionsResponse, RequestContext, SymbolScope,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PublicStreamKind {
    Trades,
    Ticker,
    OrderBookDelta,
    OrderBookSnapshot,
    Candles { interval: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicStreamSubscription {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub kind: PublicStreamKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrivateStreamKind {
    Orders,
    Fills,
    Balances,
    Positions,
    Account,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamSubscription {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub account_id: AccountId,
    pub kind: PrivateStreamKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExchangeStreamEvent {
    OrderBookSnapshot(OrderBookResponse),
    BalanceSnapshot(BalancesResponse),
    PositionSnapshot(PositionsResponse),
    OrderUpdate(OrderState),
    Fill(Fill),
    Heartbeat {
        schema_version: u16,
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
    },
}
