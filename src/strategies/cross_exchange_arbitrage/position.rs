//! Position accounting primitives for cross-exchange arbitrage bundles.

use crate::execution::{BundleStatus, PositionSide};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PositionKey {
    pub account_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub side: PositionSide,
}

impl PositionKey {
    pub fn new(
        account_id: impl Into<String>,
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        side: PositionSide,
    ) -> Self {
        Self {
            account_id: account_id.into(),
            exchange,
            canonical_symbol,
            side,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LegStatus {
    Planned,
    OpenPending,
    PartiallyOpen,
    Open,
    Closing,
    Closed,
    Orphan,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleLegState {
    pub leg_id: String,
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub side: PositionSide,
    pub intended_qty: f64,
    pub filled_qty: f64,
    pub remaining_qty: f64,
    pub avg_entry_price: Option<f64>,
    pub avg_exit_price: Option<f64>,
    pub open_fee: f64,
    pub close_fee: f64,
    pub funding_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub reduce_only_reserved_qty: f64,
    pub status: LegStatus,
    pub updated_at: DateTime<Utc>,
}

impl BundleLegState {
    pub fn new(
        leg_id: impl Into<String>,
        exchange: ExchangeId,
        exchange_symbol: ExchangeSymbol,
        side: PositionSide,
        intended_qty: f64,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            leg_id: leg_id.into(),
            exchange,
            exchange_symbol,
            side,
            intended_qty,
            filled_qty: 0.0,
            remaining_qty: intended_qty.max(0.0),
            avg_entry_price: None,
            avg_exit_price: None,
            open_fee: 0.0,
            close_fee: 0.0,
            funding_pnl: 0.0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            reduce_only_reserved_qty: 0.0,
            status: LegStatus::Planned,
            updated_at: created_at,
        }
    }

    pub fn signed_qty(&self) -> f64 {
        match self.side {
            PositionSide::Long => self.filled_qty,
            PositionSide::Short => -self.filled_qty,
            PositionSide::Net => self.filled_qty,
        }
    }

    pub fn signed_notional(&self) -> f64 {
        let notional = self.filled_qty * self.avg_entry_price.unwrap_or_default();
        match self.side {
            PositionSide::Long => notional,
            PositionSide::Short => -notional,
            PositionSide::Net => notional,
        }
    }

    pub fn gross_notional(&self) -> f64 {
        self.filled_qty * self.avg_entry_price.unwrap_or_default()
    }

    pub fn available_to_close_qty(&self) -> f64 {
        (self.filled_qty - self.reduce_only_reserved_qty).max(0.0)
    }

    fn record_open_fill(
        &mut self,
        fill_qty: f64,
        fill_price: f64,
        fee: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        if fill_qty <= 0.0 || !fill_qty.is_finite() {
            return Err(PositionError::InvalidQuantity);
        }
        if fill_price <= 0.0 || !fill_price.is_finite() {
            return Err(PositionError::InvalidPrice);
        }

        let old_notional = self.avg_entry_price.unwrap_or_default() * self.filled_qty;
        let new_filled_qty = self.filled_qty + fill_qty;
        let new_notional = old_notional + fill_qty * fill_price;
        self.filled_qty = new_filled_qty;
        self.avg_entry_price = Some(new_notional / new_filled_qty);
        self.remaining_qty = (self.intended_qty - self.filled_qty).max(0.0);
        self.open_fee += fee.max(0.0);
        self.status = if self.remaining_qty > 0.0 {
            LegStatus::PartiallyOpen
        } else {
            LegStatus::Open
        };
        self.updated_at = now;
        Ok(())
    }

    fn reserve_reduce_only_qty(
        &mut self,
        quantity: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        if quantity <= 0.0 || !quantity.is_finite() {
            return Err(PositionError::InvalidQuantity);
        }
        if quantity > self.available_to_close_qty() + f64::EPSILON {
            return Err(PositionError::ReduceOnlyReservationTooLarge {
                requested: quantity,
                available: self.available_to_close_qty(),
            });
        }
        self.reduce_only_reserved_qty += quantity;
        self.status = LegStatus::Closing;
        self.updated_at = now;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundlePosition {
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_leg: BundleLegState,
    pub short_leg: BundleLegState,
    pub status: BundleStatus,
    pub entry_edge_pct: f64,
    pub target_notional_usdt: f64,
    pub gross_notional_usdt: f64,
    pub net_base_exposure: f64,
    pub net_usdt_exposure: f64,
    pub max_unhedged_qty: f64,
    pub max_unhedged_ms: i64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl BundlePosition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bundle_id: impl Into<String>,
        canonical_symbol: CanonicalSymbol,
        long_exchange: ExchangeId,
        long_exchange_symbol: ExchangeSymbol,
        short_exchange: ExchangeId,
        short_exchange_symbol: ExchangeSymbol,
        target_qty: f64,
        target_notional_usdt: f64,
        entry_edge_pct: f64,
        created_at: DateTime<Utc>,
    ) -> Self {
        let bundle_id = bundle_id.into();
        let mut position = Self {
            long_leg: BundleLegState::new(
                format!("{bundle_id}:long"),
                long_exchange,
                long_exchange_symbol,
                PositionSide::Long,
                target_qty,
                created_at,
            ),
            short_leg: BundleLegState::new(
                format!("{bundle_id}:short"),
                short_exchange,
                short_exchange_symbol,
                PositionSide::Short,
                target_qty,
                created_at,
            ),
            bundle_id,
            canonical_symbol,
            status: BundleStatus::Observing,
            entry_edge_pct,
            target_notional_usdt,
            gross_notional_usdt: 0.0,
            net_base_exposure: 0.0,
            net_usdt_exposure: 0.0,
            max_unhedged_qty: 0.0,
            max_unhedged_ms: 0,
            opened_at: None,
            updated_at: created_at,
        };
        position.refresh_exposure(created_at);
        position
    }

    pub fn is_balanced(&self, quantity_tolerance: f64) -> bool {
        self.unhedged_qty() <= quantity_tolerance
    }

    pub fn unhedged_qty(&self) -> f64 {
        (self.long_leg.filled_qty - self.short_leg.filled_qty).abs()
    }

    fn leg_mut(&mut self, side: PositionSide) -> Result<&mut BundleLegState, PositionError> {
        match side {
            PositionSide::Long => Ok(&mut self.long_leg),
            PositionSide::Short => Ok(&mut self.short_leg),
            PositionSide::Net => Err(PositionError::UnsupportedNetSide),
        }
    }

    fn refresh_exposure(&mut self, now: DateTime<Utc>) {
        self.gross_notional_usdt = self.long_leg.gross_notional() + self.short_leg.gross_notional();
        self.net_base_exposure = self.long_leg.signed_qty() + self.short_leg.signed_qty();
        self.net_usdt_exposure = self.long_leg.signed_notional() + self.short_leg.signed_notional();
        self.max_unhedged_qty = self.max_unhedged_qty.max(self.unhedged_qty());
        self.updated_at = now;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PortfolioExposureSummary {
    pub open_bundles: usize,
    pub gross_notional_usdt: f64,
    pub net_base_exposure: f64,
    pub net_usdt_exposure: f64,
    pub orphan_bundle_count: usize,
    pub orphan_qty: f64,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PositionError {
    #[error("bundle {0} not found")]
    BundleNotFound(String),
    #[error("quantity must be positive and finite")]
    InvalidQuantity,
    #[error("price must be positive and finite")]
    InvalidPrice,
    #[error("net side is not supported for two-leg arbitrage bundles")]
    UnsupportedNetSide,
    #[error("reduce-only reservation {requested} exceeds available close quantity {available}")]
    ReduceOnlyReservationTooLarge { requested: f64, available: f64 },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PositionManager {
    bundles: HashMap<String, BundlePosition>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_bundle(&mut self, bundle: BundlePosition) {
        self.bundles.insert(bundle.bundle_id.clone(), bundle);
    }

    pub fn bundle(&self, bundle_id: &str) -> Option<&BundlePosition> {
        self.bundles.get(bundle_id)
    }

    pub fn bundles(&self) -> impl Iterator<Item = &BundlePosition> {
        self.bundles.values()
    }

    pub fn record_leg_fill(
        &mut self,
        bundle_id: &str,
        side: PositionSide,
        fill_qty: f64,
        fill_price: f64,
        fee: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        let bundle = self
            .bundles
            .get_mut(bundle_id)
            .ok_or_else(|| PositionError::BundleNotFound(bundle_id.to_string()))?;
        bundle
            .leg_mut(side)?
            .record_open_fill(fill_qty, fill_price, fee, now)?;
        if bundle.opened_at.is_none() {
            bundle.opened_at = Some(now);
        }
        bundle.refresh_exposure(now);
        Ok(())
    }

    pub fn reserve_reduce_only_qty(
        &mut self,
        bundle_id: &str,
        side: PositionSide,
        quantity: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        let bundle = self
            .bundles
            .get_mut(bundle_id)
            .ok_or_else(|| PositionError::BundleNotFound(bundle_id.to_string()))?;
        bundle
            .leg_mut(side)?
            .reserve_reduce_only_qty(quantity, now)?;
        bundle.refresh_exposure(now);
        Ok(())
    }

    pub fn mark_bundle_status(
        &mut self,
        bundle_id: &str,
        status: BundleStatus,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        let bundle = self
            .bundles
            .get_mut(bundle_id)
            .ok_or_else(|| PositionError::BundleNotFound(bundle_id.to_string()))?;
        bundle.status = status;
        bundle.updated_at = now;
        Ok(())
    }

    pub fn portfolio_exposure_summary(&self, quantity_tolerance: f64) -> PortfolioExposureSummary {
        self.bundles
            .values()
            .filter(|bundle| !bundle.status.is_terminal())
            .fold(
                PortfolioExposureSummary::default(),
                |mut summary, bundle| {
                    summary.open_bundles += 1;
                    summary.gross_notional_usdt += bundle.gross_notional_usdt;
                    summary.net_base_exposure += bundle.net_base_exposure;
                    summary.net_usdt_exposure += bundle.net_usdt_exposure;
                    if !bundle.is_balanced(quantity_tolerance) {
                        summary.orphan_bundle_count += 1;
                        summary.orphan_qty += bundle.unhedged_qty();
                    }
                    summary
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bundle(now: DateTime<Utc>) -> BundlePosition {
        BundlePosition::new(
            "bundle-1",
            CanonicalSymbol::new("ARB", "USDT"),
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
            ExchangeId::Okx,
            ExchangeSymbol::new(ExchangeId::Okx, "ARB-USDT-SWAP"),
            10.0,
            100.0,
            0.005,
            now,
        )
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_mark_partial_fill_unbalanced() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        manager
            .record_leg_fill("bundle-1", PositionSide::Long, 4.0, 10.0, 0.02, now)
            .unwrap();

        let bundle = manager.bundle("bundle-1").unwrap();
        assert_eq!(bundle.long_leg.status, LegStatus::PartiallyOpen);
        assert_eq!(bundle.unhedged_qty(), 4.0);
        assert!(!bundle.is_balanced(1e-9));

        let summary = manager.portfolio_exposure_summary(1e-9);
        assert_eq!(summary.orphan_bundle_count, 1);
        assert_eq!(summary.orphan_qty, 4.0);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_report_balanced_two_leg_fill() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        manager
            .record_leg_fill("bundle-1", PositionSide::Long, 10.0, 10.0, 0.02, now)
            .unwrap();
        manager
            .record_leg_fill("bundle-1", PositionSide::Short, 10.0, 10.2, 0.02, now)
            .unwrap();

        let bundle = manager.bundle("bundle-1").unwrap();
        assert!(bundle.is_balanced(1e-9));
        assert_eq!(bundle.long_leg.status, LegStatus::Open);
        assert_eq!(bundle.short_leg.status, LegStatus::Open);
        assert_eq!(bundle.gross_notional_usdt, 202.0);

        let summary = manager.portfolio_exposure_summary(1e-9);
        assert_eq!(summary.orphan_bundle_count, 0);
        assert_eq!(summary.open_bundles, 1);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_reject_oversized_reduce_only_reservation() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));
        manager
            .record_leg_fill("bundle-1", PositionSide::Long, 2.0, 10.0, 0.0, now)
            .unwrap();

        let err = manager
            .reserve_reduce_only_qty("bundle-1", PositionSide::Long, 3.0, now)
            .unwrap_err();
        assert!(matches!(
            err,
            PositionError::ReduceOnlyReservationTooLarge { .. }
        ));

        manager
            .reserve_reduce_only_qty("bundle-1", PositionSide::Long, 1.5, now)
            .unwrap();
        assert_eq!(
            manager
                .bundle("bundle-1")
                .unwrap()
                .long_leg
                .available_to_close_qty(),
            0.5
        );
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_update_bundle_status() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        manager
            .mark_bundle_status("bundle-1", BundleStatus::Hedging, now)
            .unwrap();

        assert_eq!(
            manager.bundle("bundle-1").unwrap().status,
            BundleStatus::Hedging
        );
    }
}
