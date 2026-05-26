//! Shared pure-domain types for the cross-exchange arbitrage strategy.

use crate::market::ExchangeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MakerLegKind {
    LongMakerBuy,
    ShortMakerSell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FillInferenceType {
    RealTrade,
    BookInferredFill,
    NotFilled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyRoute {
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub maker_side: OrderSide,
    pub taker_side: OrderSide,
    pub maker_leg_kind: MakerLegKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SimulatedBundleStatus {
    Observing,
    MakerPending,
    MakerFilled,
    Hedging,
    OpenSimulated,
    ClosingSimulated,
    Closed,
    OrphanLeg,
    RiskStopped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimulatedBundleState {
    pub bundle_id: String,
    pub opportunity_id: String,
    pub status: SimulatedBundleStatus,
    pub route: StrategyRoute,
    pub target_notional_usdt: f64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
