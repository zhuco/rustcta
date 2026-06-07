use rustcta_strategy_spot_spot_arbitrage::{
    OpportunitySummaryRecord, SummaryReport as SdkSpotSummaryReport, TradeSummaryRecord,
};

use super::{OpportunityRecord, RejectionReason, SimulatedTradeRecord};

pub type SummaryReport = SdkSpotSummaryReport;

impl OpportunitySummaryRecord for OpportunityRecord {
    fn raw_spread_bps(&self) -> f64 {
        self.raw_spread_bps
    }

    fn estimated_net_spread_bps(&self) -> f64 {
        self.estimated_net_spread_bps
    }

    fn accepted(&self) -> bool {
        self.accepted
    }

    fn rejection_reason(&self) -> Option<RejectionReason> {
        self.rejection_reason
    }

    fn buy_book_age_ms(&self) -> i64 {
        self.buy_book_age_ms
    }

    fn sell_book_age_ms(&self) -> i64 {
        self.sell_book_age_ms
    }
}

impl TradeSummaryRecord for SimulatedTradeRecord {
    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn buy_fee(&self) -> f64 {
        self.buy_fee
    }

    fn sell_fee(&self) -> f64 {
        self.sell_fee
    }

    fn gross_pnl(&self) -> f64 {
        self.gross_pnl
    }

    fn net_pnl(&self) -> f64 {
        self.net_pnl
    }

    fn pnl_category(&self) -> super::TradePnlCategory {
        self.pnl_category
    }

    fn slippage_cost(&self) -> f64 {
        self.slippage_cost
    }

    fn capital_cost(&self) -> f64 {
        self.capital_cost
    }

    fn transfer_cost(&self) -> f64 {
        self.transfer_cost
    }

    fn inventory_rebalance_cost(&self) -> f64 {
        self.inventory_rebalance_cost
    }

    fn latency_penalty_cost(&self) -> f64 {
        self.latency_penalty_cost
    }
}
