use std::collections::HashMap;

use super::{OpportunityRecord, RejectionReason, SimulatedTradeRecord};

#[derive(Debug, Clone, Default)]
pub struct SummaryReport {
    pub symbols_scanned: u64,
    pub opportunities_detected: u64,
    pub opportunities_accepted: u64,
    pub opportunities_rejected: u64,
    pub rejection_reasons: HashMap<RejectionReason, u64>,
    pub total_simulated_trades: u64,
    pub total_gross_pnl: f64,
    pub total_net_pnl: f64,
    pub total_fees: f64,
    raw_spread_sum: f64,
    net_spread_sum: f64,
    book_age_sum: i64,
    book_age_count: u64,
    symbol_net_pnl: HashMap<String, f64>,
}

impl SummaryReport {
    pub fn record_opportunity(&mut self, opportunity: &OpportunityRecord) {
        self.opportunities_detected += 1;
        self.raw_spread_sum += opportunity.raw_spread_bps;
        self.net_spread_sum += opportunity.estimated_net_spread_bps;
        self.book_age_sum += opportunity
            .buy_book_age_ms
            .max(opportunity.sell_book_age_ms);
        self.book_age_count += 1;
        if opportunity.accepted {
            self.opportunities_accepted += 1;
        } else {
            self.opportunities_rejected += 1;
            if let Some(reason) = opportunity.rejection_reason {
                self.record_rejection(reason);
            }
        }
    }

    pub fn record_rejection(&mut self, reason: RejectionReason) {
        *self.rejection_reasons.entry(reason).or_default() += 1;
    }

    pub fn record_trade(&mut self, trade: &SimulatedTradeRecord) {
        self.total_simulated_trades += 1;
        self.total_gross_pnl += trade.gross_pnl;
        self.total_net_pnl += trade.net_pnl;
        self.total_fees += trade.buy_fee + trade.sell_fee;
        *self.symbol_net_pnl.entry(trade.symbol.clone()).or_default() += trade.net_pnl;
    }

    pub fn render(&self) -> String {
        let avg_raw = average(self.raw_spread_sum, self.opportunities_detected);
        let avg_net = average(self.net_spread_sum, self.opportunities_detected);
        let avg_age = if self.book_age_count > 0 {
            self.book_age_sum as f64 / self.book_age_count as f64
        } else {
            0.0
        };
        format!(
            "spot_spot_taker_arbitrage report symbols_scanned={} opportunities={} accepted={} rejected={} trades={} gross_pnl={:.6} net_pnl={:.6} fees={:.6} avg_raw_bps={:.3} avg_net_bps={:.3} avg_book_age_ms={:.1} rejections={:?}",
            self.symbols_scanned,
            self.opportunities_detected,
            self.opportunities_accepted,
            self.opportunities_rejected,
            self.total_simulated_trades,
            self.total_gross_pnl,
            self.total_net_pnl,
            self.total_fees,
            avg_raw,
            avg_net,
            avg_age,
            self.rejection_reasons
        )
    }
}

fn average(sum: f64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}
