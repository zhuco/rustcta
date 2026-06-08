use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{OpportunityRecord, RejectionReason, SimulatedTradeRecord, TradePnlCategory};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
    pub theoretical_opportunities: u64,
    pub gross_theoretical_pnl: f64,
    pub latency_adjusted_opportunities: u64,
    pub latency_adjusted_accepted: u64,
    pub latency_adjusted_rejected: u64,
    pub latency_adjusted_gross_pnl: f64,
    pub latency_adjusted_estimated_net_pnl: f64,
    pub actual_fill_opportunities: u64,
    pub execution_reject_count: u64,
    pub execution_timeout_count: u64,
    pub partial_fill_count: u64,
    pub one_sided_risk_count: u64,
    pub latency_adjusted_realized_net_pnl: f64,
    pub simulated_gross_pnl: f64,
    pub simulated_net_pnl: f64,
    pub arbitrage_net_pnl: f64,
    pub inventory_recovery_pnl: f64,
    pub inventory_drift_pnl: f64,
    pub one_sided_exposure_pnl: f64,
    pub total_fees: f64,
    pub total_slippage_cost: f64,
    pub total_capital_cost: f64,
    pub total_transfer_cost: f64,
    pub total_inventory_rebalance_cost: f64,
    pub total_latency_penalty_cost: f64,
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
    latency_adjusted_opportunities: Vec<OpportunityRecord>,
    latency_adjusted_rejected: Vec<OpportunityRecord>,
    actual_fill_opportunities: u64,
    execution_reject_count: u64,
    execution_timeout_count: u64,
    partial_fill_count: u64,
    one_sided_risk_count: u64,
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

    pub fn record_latency_adjusted_opportunity(&mut self, opportunity: OpportunityRecord) {
        if opportunity.accepted {
            self.latency_adjusted_opportunities.push(opportunity);
        } else {
            self.latency_adjusted_rejected.push(opportunity);
        }
    }

    pub fn record_execution_result(
        &mut self,
        actual_fill: bool,
        rejected: bool,
        timed_out: bool,
        partial_fill: bool,
        one_sided_risk: bool,
    ) {
        if actual_fill {
            self.actual_fill_opportunities += 1;
        }
        if rejected {
            self.execution_reject_count += 1;
        }
        if timed_out {
            self.execution_timeout_count += 1;
        }
        if partial_fill {
            self.partial_fill_count += 1;
        }
        if one_sided_risk {
            self.one_sided_risk_count += 1;
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
        let latency_adjusted_opportunities = (self.latency_adjusted_opportunities.len()
            + self.latency_adjusted_rejected.len())
            as u64;
        let latency_adjusted_accepted = self.latency_adjusted_opportunities.len() as u64;
        let latency_adjusted_rejected = self.latency_adjusted_rejected.len() as u64;
        let simulated_gross_pnl = self.trades.iter().map(|trade| trade.gross_pnl).sum();
        let simulated_net_pnl = self.trades.iter().map(|trade| trade.net_pnl).sum();
        let arbitrage_net_pnl = self
            .trades
            .iter()
            .filter(|trade| trade.pnl_category == TradePnlCategory::Arbitrage)
            .map(|trade| trade.net_pnl)
            .sum();
        let inventory_recovery_pnl = self
            .trades
            .iter()
            .filter(|trade| {
                matches!(
                    trade.pnl_category,
                    TradePnlCategory::InventoryRecovery
                        | TradePnlCategory::InventoryDrift
                        | TradePnlCategory::OneSidedExposure
                )
            })
            .map(|trade| trade.net_pnl)
            .sum();
        let inventory_drift_pnl = self
            .trades
            .iter()
            .filter(|trade| trade.pnl_category == TradePnlCategory::InventoryDrift)
            .map(|trade| trade.net_pnl)
            .sum();
        let one_sided_exposure_pnl = self
            .trades
            .iter()
            .filter(|trade| trade.pnl_category == TradePnlCategory::OneSidedExposure)
            .map(|trade| trade.net_pnl)
            .sum();
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
        let gross_theoretical_pnl = self
            .opportunities
            .iter()
            .map(|opportunity| opportunity.estimated_gross_pnl)
            .sum();
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
            theoretical_opportunities: opportunities_accepted,
            gross_theoretical_pnl,
            latency_adjusted_opportunities,
            latency_adjusted_accepted,
            latency_adjusted_rejected,
            latency_adjusted_gross_pnl: self
                .latency_adjusted_opportunities
                .iter()
                .map(|opportunity| opportunity.estimated_gross_pnl)
                .sum(),
            latency_adjusted_estimated_net_pnl: self
                .latency_adjusted_opportunities
                .iter()
                .map(|opportunity| opportunity.estimated_net_pnl)
                .sum(),
            actual_fill_opportunities: self.actual_fill_opportunities,
            execution_reject_count: self.execution_reject_count,
            execution_timeout_count: self.execution_timeout_count,
            partial_fill_count: self.partial_fill_count,
            one_sided_risk_count: self.one_sided_risk_count,
            latency_adjusted_realized_net_pnl: simulated_net_pnl,
            simulated_gross_pnl,
            simulated_net_pnl,
            arbitrage_net_pnl,
            inventory_recovery_pnl,
            inventory_drift_pnl,
            one_sided_exposure_pnl,
            total_fees: self
                .trades
                .iter()
                .map(|trade| trade.buy_fee + trade.sell_fee)
                .sum(),
            total_slippage_cost: self.trades.iter().map(|trade| trade.slippage_cost).sum(),
            total_capital_cost: self.trades.iter().map(|trade| trade.capital_cost).sum(),
            total_transfer_cost: self.trades.iter().map(|trade| trade.transfer_cost).sum(),
            total_inventory_rebalance_cost: self
                .trades
                .iter()
                .map(|trade| trade.inventory_rebalance_cost)
                .sum(),
            total_latency_penalty_cost: self
                .trades
                .iter()
                .map(|trade| trade.latency_penalty_cost)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BookSource, SpotFeeSource};

    #[test]
    fn duration_tracker_should_finish_active_and_completed_windows() {
        let now = Utc::now();
        let mut tracker = OpportunityDurationTracker::default();
        let mut active = opportunity(now, true, None);
        tracker.observe(&active);
        active.timestamp = now + chrono::Duration::milliseconds(250);
        active.estimated_net_spread_bps = 90.0;
        active.executable_notional = 120.0;
        tracker.observe(&active);
        active.accepted = false;
        tracker.observe(&active);

        let completed = tracker.finish();

        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].duration_ms, 250);
        assert_eq!(completed[0].max_net_spread_bps, 90.0);
        assert_eq!(completed[0].max_executable_notional, 120.0);
    }

    #[test]
    fn replay_report_should_aggregate_opportunities_rejections_and_trades() {
        let now = Utc::now();
        let mut builder = ReplayReportBuilder::default();
        builder.record_book_event(now);
        builder.record_book_event(now + chrono::Duration::seconds(1));
        builder.record_opportunity(opportunity(now, true, None));
        builder.record_opportunity(opportunity(
            now + chrono::Duration::milliseconds(100),
            false,
            Some(RejectionReason::StaleBook),
        ));
        builder.record_latency_adjusted_opportunity(opportunity(now, true, None));
        builder.record_execution_result(true, false, false, false, false);
        builder.record_trade(trade(now));

        let report = builder.build(1);

        assert_eq!(report.total_book_events, 2);
        assert_eq!(report.replay_duration_ms, 1_000);
        assert_eq!(report.opportunities_detected, 2);
        assert_eq!(report.opportunities_accepted, 1);
        assert_eq!(report.stale_book_rejection_count, 1);
        assert_eq!(report.actual_fill_opportunities, 1);
        assert_eq!(report.best_symbols, vec![("BTCUSDT".to_string(), 0.8)]);
        assert!((report.simulated_net_pnl - 0.8).abs() < 1e-12);
    }

    fn opportunity(
        timestamp: DateTime<Utc>,
        accepted: bool,
        rejection_reason: Option<RejectionReason>,
    ) -> OpportunityRecord {
        OpportunityRecord {
            timestamp,
            symbol: "BTCUSDT".to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_price: 100.0,
            sell_price: 101.0,
            raw_spread_bps: 100.0,
            buy_fee_bps: 10.0,
            sell_fee_bps: 10.0,
            fee_source_buy: SpotFeeSource::Config,
            fee_source_sell: SpotFeeSource::Config,
            platform_discount_applied: false,
            estimated_fee_bps: 20.0,
            estimated_slippage_bps: 0.0,
            safety_buffer_bps: 0.0,
            estimated_net_spread_bps: 80.0,
            estimated_total_fee: 0.2,
            estimated_gross_pnl: 1.0,
            estimated_net_pnl: 0.8,
            capital_cost_bps: 0.0,
            transfer_cost_bps: 0.0,
            transfer_delay_penalty_bps: 0.0,
            inventory_rebalance_cost_bps: 0.0,
            latency_penalty_bps: 0.0,
            effective_min_net_spread_bps: 0.0,
            estimated_slippage_cost: 0.0,
            estimated_capital_cost: 0.0,
            estimated_transfer_cost: 0.0,
            estimated_inventory_rebalance_cost: 0.0,
            estimated_latency_penalty_cost: 0.0,
            estimated_total_cost: 0.2,
            executable_notional: 100.0,
            quantity: 1.0,
            accepted,
            rejection_reason,
            rejection_detail: None,
            buy_book_age_ms: 1,
            sell_book_age_ms: 2,
            buy_book_source: BookSource::Websocket,
            sell_book_source: BookSource::Websocket,
            buy_latency_ms: Some(1),
            sell_latency_ms: Some(1),
        }
    }

    fn trade(timestamp: DateTime<Utc>) -> SimulatedTradeRecord {
        SimulatedTradeRecord {
            timestamp,
            symbol: "BTCUSDT".to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_avg_price: 100.0,
            sell_avg_price: 101.0,
            quantity: 1.0,
            notional: 100.0,
            buy_fee: 0.1,
            sell_fee: 0.1,
            gross_pnl: 1.0,
            net_pnl: 0.8,
            pnl_category: TradePnlCategory::Arbitrage,
            slippage_cost: 0.0,
            capital_cost: 0.0,
            transfer_cost: 0.0,
            inventory_rebalance_cost: 0.0,
            latency_penalty_cost: 0.0,
            latency_ms: 1,
            order_book_age_ms: 1,
            execution_mode: "paper_taker_taker".to_string(),
        }
    }
}
