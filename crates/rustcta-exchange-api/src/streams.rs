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
pub enum PrivateOrderStreamEventKind {
    Ack,
    New,
    PartialFill,
    Fill,
    Cancel,
    Reject,
    Expired,
    BalanceUpdate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamCapabilities {
    pub schema_version: u16,
    pub supports_orders: bool,
    pub supports_fills: bool,
    pub supports_balances: bool,
    pub supports_positions: bool,
    pub supports_account: bool,
    pub order_event_kinds: Vec<PrivateOrderStreamEventKind>,
    pub supports_client_order_id: bool,
    pub supports_exchange_order_id: bool,
}

impl PrivateStreamCapabilities {
    pub fn unsupported(schema_version: u16) -> Self {
        Self {
            schema_version,
            supports_orders: false,
            supports_fills: false,
            supports_balances: false,
            supports_positions: false,
            supports_account: false,
            order_event_kinds: Vec::new(),
            supports_client_order_id: false,
            supports_exchange_order_id: false,
        }
    }
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
