//! Funding-rate direction and net funding estimates.

use super::state::PositionSide;
use crate::market::{CanonicalSymbol, ExchangeId};
use crate::utils::money;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingEstimate {
    pub long_leg_funding: f64,
    pub short_leg_funding: f64,
    pub net_funding: f64,
    pub net_funding_rate: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub minutes_to_funding: Option<i64>,
    pub dangerous: bool,
    pub near_negative_settlement: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingModel {
    pub max_adverse_funding_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingSettlement {
    pub bundle_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub position_side: PositionSide,
    pub notional_usdt: f64,
    pub funding_rate: f64,
    pub funding_pnl_usdt: f64,
    pub mark_price: Option<f64>,
    pub settled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FundingSettlementLedger {
    settlements: Vec<FundingSettlement>,
}

impl FundingModel {
    pub fn new(max_adverse_funding_rate: f64) -> Self {
        Self {
            max_adverse_funding_rate,
        }
    }

    pub fn leg_funding(position_side: PositionSide, notional_usdt: f64, funding_rate: f64) -> f64 {
        let abs_notional = notional_usdt.abs();
        let funding =
            money::multiply_f64(abs_notional, funding_rate, "notional_usdt", "funding_rate")
                .unwrap_or(abs_notional * funding_rate);
        match position_side {
            PositionSide::Long => -funding,
            PositionSide::Short => funding,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn settle_leg(
        bundle_id: impl Into<String>,
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        notional_usdt: f64,
        funding_rate: f64,
        mark_price: Option<f64>,
        settled_at: DateTime<Utc>,
    ) -> FundingSettlement {
        FundingSettlement {
            bundle_id: bundle_id.into(),
            exchange,
            canonical_symbol,
            position_side,
            notional_usdt: notional_usdt.abs(),
            funding_rate,
            funding_pnl_usdt: Self::leg_funding(position_side, notional_usdt, funding_rate),
            mark_price,
            settled_at,
        }
    }

    pub fn estimate_pair(
        &self,
        long_notional_usdt: f64,
        long_funding_rate: f64,
        short_notional_usdt: f64,
        short_funding_rate: f64,
    ) -> FundingEstimate {
        self.estimate_pair_with_timing(
            long_notional_usdt,
            long_funding_rate,
            None,
            short_notional_usdt,
            short_funding_rate,
            None,
            Utc::now(),
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn estimate_pair_with_timing(
        &self,
        long_notional_usdt: f64,
        long_funding_rate: f64,
        long_next_funding_time: Option<DateTime<Utc>>,
        short_notional_usdt: f64,
        short_funding_rate: f64,
        short_next_funding_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        no_open_before_funding_mins: i64,
    ) -> FundingEstimate {
        let long_leg_funding =
            Self::leg_funding(PositionSide::Long, long_notional_usdt, long_funding_rate);
        let short_leg_funding =
            Self::leg_funding(PositionSide::Short, short_notional_usdt, short_funding_rate);
        let net_funding = money::add_f64(
            long_leg_funding,
            short_leg_funding,
            "long_funding",
            "short_funding",
        )
        .unwrap_or(long_leg_funding + short_leg_funding);
        let base_notional = long_notional_usdt
            .abs()
            .max(short_notional_usdt.abs())
            .max(1.0);
        let net_funding_rate =
            money::divide_f64(net_funding, base_notional, "net_funding", "base_notional")
                .unwrap_or(net_funding / base_notional);
        let next_funding_time =
            earliest_future(long_next_funding_time, short_next_funding_time, now);
        let minutes_to_funding =
            next_funding_time.map(|time| time.signed_duration_since(now).num_minutes().max(0));
        let near_negative_settlement = net_funding < 0.0
            && minutes_to_funding
                .map(|mins| mins <= no_open_before_funding_mins.max(0))
                .unwrap_or(false);

        FundingEstimate {
            long_leg_funding,
            short_leg_funding,
            net_funding,
            net_funding_rate,
            next_funding_time,
            minutes_to_funding,
            dangerous: net_funding_rate < -self.max_adverse_funding_rate,
            near_negative_settlement,
        }
    }
}

fn earliest_future(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    [left, right]
        .into_iter()
        .flatten()
        .filter(|time| *time >= now)
        .min()
}

impl Default for FundingModel {
    fn default() -> Self {
        Self::new(0.001)
    }
}

impl FundingSettlementLedger {
    pub fn record(&mut self, settlement: FundingSettlement) {
        self.settlements.push(settlement);
    }

    pub fn settlements(&self) -> &[FundingSettlement] {
        &self.settlements
    }

    pub fn total_pnl_usdt(&self) -> f64 {
        self.settlements
            .iter()
            .map(|settlement| settlement.funding_pnl_usdt)
            .sum()
    }

    pub fn total_pnl_for_bundle(&self, bundle_id: &str) -> f64 {
        self.settlements
            .iter()
            .filter(|settlement| settlement.bundle_id == bundle_id)
            .map(|settlement| settlement.funding_pnl_usdt)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_exchange_arbitrage_funding_should_apply_long_short_direction() {
        let model = FundingModel::default();
        let estimate = model.estimate_pair(100.0, 0.0003, 100.0, 0.0005);

        assert!((estimate.long_leg_funding + 0.03).abs() < 1e-9);
        assert!((estimate.short_leg_funding - 0.05).abs() < 1e-9);
        assert!((estimate.net_funding - 0.02).abs() < 1e-9);
        assert_eq!(estimate.next_funding_time, None);

        let adverse = model.estimate_pair(100.0, 0.001, 100.0, -0.001);
        assert!(adverse.net_funding < 0.0);
        assert!(adverse.dangerous);
    }

    #[test]
    fn cross_exchange_arbitrage_funding_should_block_near_negative_settlement() {
        let model = FundingModel::default();
        let now = Utc::now();
        let estimate = model.estimate_pair_with_timing(
            100.0,
            0.001,
            Some(now + chrono::Duration::minutes(3)),
            100.0,
            -0.001,
            Some(now + chrono::Duration::minutes(10)),
            now,
            5,
        );

        assert_eq!(estimate.minutes_to_funding, Some(3));
        assert!(estimate.near_negative_settlement);
    }

    #[test]
    fn cross_exchange_arbitrage_funding_should_record_settlement_pnl() {
        let mut ledger = FundingSettlementLedger::default();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let now = Utc::now();

        ledger.record(FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::Binance,
            symbol.clone(),
            PositionSide::Long,
            100.0,
            0.0003,
            Some(65000.0),
            now,
        ));
        ledger.record(FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::Okx,
            symbol,
            PositionSide::Short,
            100.0,
            0.0005,
            Some(65050.0),
            now,
        ));

        assert_eq!(ledger.settlements().len(), 2);
        assert!((ledger.total_pnl_for_bundle("bundle-1") - 0.02).abs() < 1e-9);
        assert!((ledger.total_pnl_usdt() - 0.02).abs() < 1e-9);
    }
}
