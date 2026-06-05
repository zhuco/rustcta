use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{OpportunityRecord, RejectionReason, SimulatedTradeRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityDuration {
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub duration_ms: i64,
    pub max_net_spread_bps: f64,
    pub max_executable_notional: f64,
}

#[derive(Debug, Clone, Default)]
pub struct OpportunityDurationTracker {
    active: HashMap<(String, String, String), OpportunityDuration>,
    completed: Vec<OpportunityDuration>,
}

impl OpportunityDurationTracker {
    pub fn observe(&mut self, opportunity: &OpportunityRecord) {
        let key = (
            opportunity.symbol.clone(),
            opportunity.buy_exchange.clone(),
            opportunity.sell_exchange.clone(),
        );
        if opportunity.accepted {
            let entry = self
                .active
                .entry(key)
                .or_insert_with(|| OpportunityDuration {
                    symbol: opportunity.symbol.clone(),
                    buy_exchange: opportunity.buy_exchange.clone(),
                    sell_exchange: opportunity.sell_exchange.clone(),
                    first_seen: opportunity.timestamp,
                    last_seen: opportunity.timestamp,
                    duration_ms: 0,
                    max_net_spread_bps: opportunity.estimated_net_spread_bps,
                    max_executable_notional: opportunity.executable_notional,
                });
            entry.last_seen = opportunity.timestamp;
            entry.duration_ms = entry
                .last_seen
                .signed_duration_since(entry.first_seen)
                .num_milliseconds();
            entry.max_net_spread_bps = entry
                .max_net_spread_bps
                .max(opportunity.estimated_net_spread_bps);
            entry.max_executable_notional = entry
                .max_executable_notional
                .max(opportunity.executable_notional);
        } else if let Some(done) = self.active.remove(&key) {
            self.completed.push(done);
        }
    }

    pub fn finish(mut self) -> Vec<OpportunityDuration> {
        self.completed.extend(self.active.into_values());
        self.completed
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplayReport {
    pub total_book_events: u64,
    pub total_symbols: usize,
    pub replay_duration_ms: i64,
    pub opportunities_detected: u64,
    pub opportunities_accepted: u64,
    pub opportunities_rejected: u64,
    pub rejection_reasons: HashMap<RejectionReason, u64>,
    pub average_raw_spread_bps: f64,
    pub average_net_spread_bps: f64,
    pub average_opportunity_duration_ms: f64,
    pub average_book_age_ms: f64,
    pub stale_book_rejection_count: u64,
    pub insufficient_depth_rejection_count: u64,
    pub simulated_gross_pnl: f64,
    pub simulated_net_pnl: f64,
    pub total_fees: f64,
    pub best_symbols: Vec<(String, f64)>,
    pub worst_symbols: Vec<(String, f64)>,
    pub top_opportunities: Vec<OpportunityRecord>,
    pub top_rejected: Vec<OpportunityRecord>,
}

#[derive(Debug, Default)]
pub struct ReplayReportBuilder {
    pub total_book_events: u64,
    first_event_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    raw_spread_sum: f64,
    net_spread_sum: f64,
    book_age_sum: i64,
    book_age_count: u64,
    symbol_pnl: HashMap<String, f64>,
    opportunities: Vec<OpportunityRecord>,
    rejected: Vec<OpportunityRecord>,
    trades: Vec<SimulatedTradeRecord>,
    durations: OpportunityDurationTracker,
}

impl ReplayReportBuilder {
    pub fn record_book_event(&mut self, timestamp: DateTime<Utc>) {
        self.total_book_events += 1;
        self.first_event_at = Some(self.first_event_at.unwrap_or(timestamp).min(timestamp));
        self.last_event_at = Some(self.last_event_at.unwrap_or(timestamp).max(timestamp));
    }

    pub fn record_opportunity(&mut self, opportunity: OpportunityRecord) {
        self.raw_spread_sum += opportunity.raw_spread_bps;
        self.net_spread_sum += opportunity.estimated_net_spread_bps;
        self.book_age_sum += opportunity
            .buy_book_age_ms
            .max(opportunity.sell_book_age_ms);
        self.book_age_count += 1;
        self.durations.observe(&opportunity);
        if opportunity.accepted {
            self.opportunities.push(opportunity);
        } else {
            self.rejected.push(opportunity);
        }
    }

    pub fn record_trade(&mut self, trade: SimulatedTradeRecord) {
        *self.symbol_pnl.entry(trade.symbol.clone()).or_default() += trade.net_pnl;
        self.trades.push(trade);
    }

    pub fn build(self, total_symbols: usize) -> ReplayReport {
        let opportunities_detected = (self.opportunities.len() + self.rejected.len()) as u64;
        let opportunities_accepted = self.opportunities.len() as u64;
        let opportunities_rejected = self.rejected.len() as u64;
        let mut rejection_reasons = HashMap::new();
        for opportunity in &self.rejected {
            if let Some(reason) = opportunity.rejection_reason {
                *rejection_reasons.entry(reason).or_default() += 1;
            }
        }
        let durations = self.durations.finish();
        let avg_duration = if durations.is_empty() {
            0.0
        } else {
            durations
                .iter()
                .map(|item| item.duration_ms as f64)
                .sum::<f64>()
                / durations.len() as f64
        };
        let mut best_symbols = self.symbol_pnl.into_iter().collect::<Vec<_>>();
        best_symbols.sort_by(|left, right| right.1.total_cmp(&left.1));
        let mut worst_symbols = best_symbols.clone();
        worst_symbols.sort_by(|left, right| left.1.total_cmp(&right.1));
        let mut top_opportunities = self.opportunities;
        top_opportunities.sort_by(|left, right| {
            right
                .estimated_net_spread_bps
                .total_cmp(&left.estimated_net_spread_bps)
        });
        top_opportunities.truncate(20);
        let mut top_rejected = self.rejected;
        top_rejected.sort_by(|left, right| {
            right
                .estimated_net_spread_bps
                .total_cmp(&left.estimated_net_spread_bps)
        });
        top_rejected.truncate(20);
        ReplayReport {
            total_book_events: self.total_book_events,
            total_symbols,
            replay_duration_ms: self
                .last_event_at
                .zip(self.first_event_at)
                .map(|(last, first)| last.signed_duration_since(first).num_milliseconds())
                .unwrap_or_default(),
            opportunities_detected,
            opportunities_accepted,
            opportunities_rejected,
            rejection_reasons: rejection_reasons.clone(),
            average_raw_spread_bps: average(self.raw_spread_sum, opportunities_detected),
            average_net_spread_bps: average(self.net_spread_sum, opportunities_detected),
            average_opportunity_duration_ms: avg_duration,
            average_book_age_ms: if self.book_age_count == 0 {
                0.0
            } else {
                self.book_age_sum as f64 / self.book_age_count as f64
            },
            stale_book_rejection_count: rejection_reasons
                .get(&RejectionReason::StaleBook)
                .copied()
                .unwrap_or_default(),
            insufficient_depth_rejection_count: rejection_reasons
                .get(&RejectionReason::InsufficientDepth)
                .copied()
                .unwrap_or_default(),
            simulated_gross_pnl: self.trades.iter().map(|trade| trade.gross_pnl).sum(),
            simulated_net_pnl: self.trades.iter().map(|trade| trade.net_pnl).sum(),
            total_fees: self
                .trades
                .iter()
                .map(|trade| trade.buy_fee + trade.sell_fee)
                .sum(),
            best_symbols: best_symbols.into_iter().take(20).collect(),
            worst_symbols: worst_symbols.into_iter().take(20).collect(),
            top_opportunities,
            top_rejected,
        }
    }
}

fn average(sum: f64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}
