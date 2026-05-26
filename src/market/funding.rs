use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{CanonicalSymbol, ExchangeId, ExchangeSymbol};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketFundingSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub funding_rate: f64,
    pub predicted_funding_rate: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub recv_ts: DateTime<Utc>,
}

impl MarketFundingSnapshot {
    pub fn new(
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: Option<ExchangeSymbol>,
        funding_rate: f64,
        next_funding_time: Option<DateTime<Utc>>,
        recv_ts: DateTime<Utc>,
    ) -> Self {
        Self {
            exchange,
            canonical_symbol,
            exchange_symbol,
            funding_rate,
            predicted_funding_rate: None,
            mark_price: None,
            index_price: None,
            next_funding_time,
            recv_ts,
        }
    }

    pub fn with_prices(mut self, mark_price: Option<f64>, index_price: Option<f64>) -> Self {
        self.mark_price = mark_price;
        self.index_price = index_price;
        self
    }

    pub fn with_predicted_rate(mut self, predicted_funding_rate: Option<f64>) -> Self {
        self.predicted_funding_rate = predicted_funding_rate;
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FundingCache {
    snapshots: HashMap<(ExchangeId, CanonicalSymbol), MarketFundingSnapshot>,
}

impl FundingCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, snapshot: MarketFundingSnapshot) {
        self.snapshots.insert(
            (snapshot.exchange.clone(), snapshot.canonical_symbol.clone()),
            snapshot,
        );
    }

    pub fn get(
        &self,
        exchange: &ExchangeId,
        canonical_symbol: &CanonicalSymbol,
    ) -> Option<&MarketFundingSnapshot> {
        self.snapshots
            .get(&(exchange.clone(), canonical_symbol.clone()))
    }

    pub fn snapshots_for_symbol(
        &self,
        canonical_symbol: &CanonicalSymbol,
    ) -> Vec<&MarketFundingSnapshot> {
        self.snapshots
            .iter()
            .filter_map(|((_, symbol), snapshot)| (symbol == canonical_symbol).then_some(snapshot))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}
