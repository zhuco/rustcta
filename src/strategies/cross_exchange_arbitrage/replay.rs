//! Replay and reporting support for cross-exchange arbitrage.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    ArbitrageFillRecord, ArbitrageOrderRecord, ArbitragePnlRecord, ArbitrageRiskEventRecord,
    CrossArbStorageEvent, CrossExchangeArbitrageConfig, MarketDataBookStore,
    NormalizedDepthSnapshot, OpportunityDecision, OpportunityRecord, SpreadEngine,
    StoredCrossArbEvent,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplayRecords {
    pub snapshots: Vec<NormalizedDepthSnapshot>,
    pub opportunities: Vec<OpportunityRecord>,
    pub orders: Vec<ArbitrageOrderRecord>,
    pub fills: Vec<ArbitrageFillRecord>,
    pub pnl: Vec<ArbitragePnlRecord>,
    pub risk_events: Vec<ArbitrageRiskEventRecord>,
}

impl ReplayRecords {
    pub fn from_storage_events(events: &[StoredCrossArbEvent]) -> Self {
        let mut records = Self::default();
        for stored in events {
            match &stored.event {
                CrossArbStorageEvent::MarketSnapshot(snapshot) => {
                    let mut book = NormalizedDepthSnapshot {
                        exchange: snapshot.exchange.clone(),
                        symbol: snapshot.symbol.clone(),
                        exchange_symbol: snapshot.exchange_symbol.clone(),
                        bids: Vec::new(),
                        asks: Vec::new(),
                        exchange_timestamp: Some(snapshot.timestamp),
                        received_at: snapshot.timestamp,
                        sequence: snapshot.sequence,
                    };
                    if let (Some(price), Some(quantity)) =
                        (snapshot.best_bid, snapshot.best_bid_quantity)
                    {
                        book.bids
                            .push(crate::market::BookLevel::new(price, quantity));
                    }
                    if let (Some(price), Some(quantity)) =
                        (snapshot.best_ask, snapshot.best_ask_quantity)
                    {
                        book.asks
                            .push(crate::market::BookLevel::new(price, quantity));
                    }
                    records.snapshots.push(book);
                }
                CrossArbStorageEvent::DetectedOpportunity(opportunity) => {
                    records.opportunities.push(opportunity.clone());
                }
                CrossArbStorageEvent::OrderTransition(order) => records.orders.push(order.clone()),
                CrossArbStorageEvent::Fill(fill) => records.fills.push(fill.clone()),
                CrossArbStorageEvent::Pnl(pnl) => records.pnl.push(pnl.clone()),
                CrossArbStorageEvent::RiskEvent(event) => records.risk_events.push(event.clone()),
                _ => {}
            }
        }
        records
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplaySummaryReport {
    pub generated_at: DateTime<Utc>,
    pub total_opportunities: usize,
    pub accepted_opportunities: usize,
    pub rejected_opportunities: usize,
    pub rejected_by_reason: HashMap<String, usize>,
    pub average_raw_spread_bps: f64,
    pub average_net_spread_bps: f64,
    pub fill_ratio: f64,
    pub maker_fill_ratio: f64,
    pub taker_hedge_ratio: f64,
    pub residual_leg_count: usize,
    pub stop_loss_count: usize,
    pub realized_pnl_usdt: f64,
    pub max_drawdown_usdt: f64,
}

#[derive(Debug, Clone)]
pub struct CrossArbReplayEngine {
    config: CrossExchangeArbitrageConfig,
}

impl CrossArbReplayEngine {
    pub fn new(config: CrossExchangeArbitrageConfig) -> Self {
        Self { config }
    }

    pub fn replay_market_snapshots(
        &self,
        snapshots: &[NormalizedDepthSnapshot],
        now: DateTime<Utc>,
    ) -> ReplaySummaryReport {
        let mut store = MarketDataBookStore::new();
        let engine = SpreadEngine::from_config(&self.config);
        let mut opportunities = Vec::new();
        for snapshot in snapshots {
            store.update(snapshot.clone());
            opportunities.extend(engine.scan_symbol(
                &store,
                &snapshot.symbol,
                &self.config.detection.exchanges,
                now,
            ));
        }
        Self::summary_from_parts(&opportunities, &[], &[], &[], &[], now)
    }

    pub fn replay_records(
        &self,
        records: &ReplayRecords,
        now: DateTime<Utc>,
    ) -> ReplaySummaryReport {
        let mut opportunities = records.opportunities.clone();
        if opportunities.is_empty() && !records.snapshots.is_empty() {
            return self.replay_market_snapshots(&records.snapshots, now);
        }
        opportunities.sort_by_key(|opportunity| opportunity.timestamp);
        Self::summary_from_parts(
            &opportunities,
            &records.orders,
            &records.fills,
            &records.pnl,
            &records.risk_events,
            now,
        )
    }

    pub fn summary_from_parts(
        opportunities: &[OpportunityRecord],
        orders: &[ArbitrageOrderRecord],
        fills: &[ArbitrageFillRecord],
        pnl_records: &[ArbitragePnlRecord],
        risk_events: &[ArbitrageRiskEventRecord],
        now: DateTime<Utc>,
    ) -> ReplaySummaryReport {
        let total_opportunities = opportunities.len();
        let accepted_opportunities = opportunities
            .iter()
            .filter(|opportunity| opportunity.decision == OpportunityDecision::Accepted)
            .count();
        let rejected_opportunities = total_opportunities.saturating_sub(accepted_opportunities);
        let mut rejected_by_reason = HashMap::new();
        for opportunity in opportunities
            .iter()
            .filter(|opportunity| opportunity.decision == OpportunityDecision::Rejected)
        {
            *rejected_by_reason
                .entry(opportunity.reason.clone())
                .or_insert(0) += 1;
        }

        let filled_orders = orders
            .iter()
            .filter(|order| {
                order.status.eq_ignore_ascii_case("filled")
                    || order.filled_quantity + f64::EPSILON >= order.quantity
            })
            .count();
        let maker_fills = fills
            .iter()
            .filter(|fill| fill.liquidity_role.eq_ignore_ascii_case("maker"))
            .count();
        let taker_fills = fills
            .iter()
            .filter(|fill| fill.liquidity_role.eq_ignore_ascii_case("taker"))
            .count();
        let realized_pnl_usdt = if let Some(last) = pnl_records.last() {
            last.realized_pnl_usdt
        } else {
            fills
                .iter()
                .map(ArbitrageFillRecord::signed_quote_pnl)
                .sum()
        };

        ReplaySummaryReport {
            generated_at: now,
            total_opportunities,
            accepted_opportunities,
            rejected_opportunities,
            rejected_by_reason,
            average_raw_spread_bps: average_by(opportunities, |opportunity| {
                opportunity.raw_spread_bps
            }),
            average_net_spread_bps: average_by(opportunities, |opportunity| {
                opportunity.estimated_net_spread_bps
            }),
            fill_ratio: ratio(filled_orders, orders.len()),
            maker_fill_ratio: ratio(maker_fills, fills.len()),
            taker_hedge_ratio: ratio(taker_fills, fills.len()),
            residual_leg_count: risk_events
                .iter()
                .filter(|event| {
                    event.risk_type.contains("residual") || event.reason.contains("residual")
                })
                .count(),
            stop_loss_count: risk_events
                .iter()
                .filter(|event| {
                    event.risk_type.contains("stop_loss") || event.reason.contains("stop_loss")
                })
                .count(),
            realized_pnl_usdt,
            max_drawdown_usdt: max_drawdown(pnl_records),
        }
    }
}

fn average_by(
    opportunities: &[OpportunityRecord],
    value: impl Fn(&OpportunityRecord) -> f64,
) -> f64 {
    if opportunities.is_empty() {
        return 0.0;
    }
    opportunities.iter().map(value).sum::<f64>() / opportunities.len() as f64
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn max_drawdown(records: &[ArbitragePnlRecord]) -> f64 {
    let mut peak = f64::NEG_INFINITY;
    let mut max_drawdown: f64 = 0.0;
    for record in records {
        peak = peak.max(record.equity_usdt);
        max_drawdown = max_drawdown.max((peak - record.equity_usdt).max(0.0));
    }
    max_drawdown
}
