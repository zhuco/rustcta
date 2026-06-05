//! Detection-only contracts for spot cross-exchange arbitrage.

use crate::market::{BookLevel, CanonicalSymbol, ExchangeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedDepthSnapshot {
    pub exchange: ExchangeId,
    pub symbol: CanonicalSymbol,
    pub exchange_symbol: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub sequence: Option<u64>,
}

impl NormalizedDepthSnapshot {
    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids.first().copied()
    }

    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks.first().copied()
    }

    pub fn is_stale(&self, now: DateTime<Utc>, max_age_ms: i64) -> bool {
        now.signed_duration_since(self.received_at)
            .num_milliseconds()
            > max_age_ms
    }

    pub fn is_usable(&self, now: DateTime<Utc>, max_age_ms: i64) -> bool {
        self.best_bid().is_some()
            && self.best_ask().is_some()
            && self.best_bid().unwrap().price < self.best_ask().unwrap().price
            && !self.is_stale(now, max_age_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpportunityDecision {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: CanonicalSymbol,
    pub buy_exchange: ExchangeId,
    pub sell_exchange: ExchangeId,
    pub buy_price: f64,
    pub sell_price: f64,
    pub raw_spread_bps: f64,
    pub estimated_net_spread_bps: f64,
    pub estimated_notional: f64,
    pub decision: OpportunityDecision,
    pub reason: String,
}

impl OpportunityRecord {
    pub fn accepted(&self) -> bool {
        self.decision == OpportunityDecision::Accepted
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectionMetrics {
    pub books_received: u64,
    pub opportunities_recorded: u64,
    pub opportunities_accepted: u64,
    pub opportunities_rejected: u64,
}

impl DetectionMetrics {
    pub fn observe_book(&mut self) {
        self.books_received += 1;
    }

    pub fn observe_opportunity(&mut self, opportunity: &OpportunityRecord) {
        self.opportunities_recorded += 1;
        if opportunity.accepted() {
            self.opportunities_accepted += 1;
        } else {
            self.opportunities_rejected += 1;
        }
    }
}
