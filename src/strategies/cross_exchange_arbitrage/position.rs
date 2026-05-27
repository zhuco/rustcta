//! Position accounting primitives for cross-exchange arbitrage bundles.

use crate::execution::{
    recommended_actions_for_severity, ArbitrageBundle, BundleStatus, ExchangePosition, FillEvent,
    OrderSide, PositionSide, ReconcileAction, ReconcileSeverity,
};
use crate::market::{exchange_symbol_for, CanonicalSymbol, ExchangeId, ExchangeSymbol};
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
        if self.filled_qty + fill_qty > self.intended_qty + f64::EPSILON {
            return Err(PositionError::OpenFillExceedsIntended {
                leg_id: self.leg_id.clone(),
                requested: fill_qty,
                remaining: self.remaining_qty,
            });
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

    fn record_close_fill(
        &mut self,
        fill_qty: f64,
        fill_price: f64,
        fee: f64,
        realized_pnl: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionError> {
        if fill_qty <= 0.0 || !fill_qty.is_finite() {
            return Err(PositionError::InvalidQuantity);
        }
        if fill_price <= 0.0 || !fill_price.is_finite() {
            return Err(PositionError::InvalidPrice);
        }
        if fill_qty > self.filled_qty + f64::EPSILON {
            return Err(PositionError::CloseFillExceedsOpen {
                leg_id: self.leg_id.clone(),
                requested: fill_qty,
                available: self.filled_qty,
            });
        }

        let closed_before = self.intended_qty - self.remaining_qty - self.filled_qty;
        let old_exit_notional = self.avg_exit_price.unwrap_or_default() * closed_before;
        let new_closed_qty = closed_before + fill_qty;
        self.avg_exit_price = Some((old_exit_notional + fill_qty * fill_price) / new_closed_qty);
        self.filled_qty = (self.filled_qty - fill_qty).max(0.0);
        self.close_fee += fee.max(0.0);
        self.realized_pnl += realized_pnl;
        self.reduce_only_reserved_qty = self.reduce_only_reserved_qty.saturating_sub(fill_qty);
        self.status = if self.filled_qty <= f64::EPSILON {
            self.filled_qty = 0.0;
            LegStatus::Closed
        } else {
            LegStatus::Closing
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

    fn leg(&self, side: PositionSide) -> Result<&BundleLegState, PositionError> {
        match side {
            PositionSide::Long => Ok(&self.long_leg),
            PositionSide::Short => Ok(&self.short_leg),
            PositionSide::Net => Err(PositionError::UnsupportedNetSide),
        }
    }

    fn resolve_fill_side(&self, fill: &FillEvent) -> Result<PositionSide, PositionError> {
        if fill.canonical_symbol != self.canonical_symbol {
            return Err(PositionError::FillDoesNotMatchBundle {
                bundle_id: self.bundle_id.clone(),
                trade_id: fill.trade_id.clone(),
            });
        }

        let candidate = match fill.position_side {
            PositionSide::Long | PositionSide::Short => fill.position_side,
            PositionSide::Net => {
                let long_matches = leg_matches_fill(&self.long_leg, fill);
                let short_matches = leg_matches_fill(&self.short_leg, fill);
                match (long_matches, short_matches, fill.side) {
                    (true, false, _) => PositionSide::Long,
                    (false, true, _) => PositionSide::Short,
                    (true, true, OrderSide::Buy) if fill.reduce_only == Some(true) => {
                        PositionSide::Short
                    }
                    (true, true, OrderSide::Sell) if fill.reduce_only == Some(true) => {
                        PositionSide::Long
                    }
                    (true, true, OrderSide::Buy) => PositionSide::Long,
                    (true, true, OrderSide::Sell) => PositionSide::Short,
                    _ => {
                        return Err(PositionError::FillDoesNotMatchBundle {
                            bundle_id: self.bundle_id.clone(),
                            trade_id: fill.trade_id.clone(),
                        });
                    }
                }
            }
        };

        let leg = self.leg(candidate)?;
        if leg_matches_fill(leg, fill) && fill_direction_matches_leg(leg, fill) {
            Ok(candidate)
        } else {
            Err(PositionError::FillDoesNotMatchBundle {
                bundle_id: self.bundle_id.clone(),
                trade_id: fill.trade_id.clone(),
            })
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

fn leg_matches_fill(leg: &BundleLegState, fill: &FillEvent) -> bool {
    leg.exchange == fill.exchange && leg.exchange_symbol == fill.exchange_symbol
}

fn fill_direction_matches_leg(leg: &BundleLegState, fill: &FillEvent) -> bool {
    matches!(
        (leg.side, fill.reduce_only.unwrap_or(false), fill.side),
        (PositionSide::Long, false, OrderSide::Buy)
            | (PositionSide::Long, true, OrderSide::Sell)
            | (PositionSide::Short, false, OrderSide::Sell)
            | (PositionSide::Short, true, OrderSide::Buy)
    )
}

trait SaturatingSub {
    fn saturating_sub(self, rhs: Self) -> Self;
}

impl SaturatingSub for f64 {
    fn saturating_sub(self, rhs: Self) -> Self {
        (self - rhs).max(0.0)
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegRiskSummary {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub side: PositionSide,
    pub bundle_count: usize,
    pub filled_qty: f64,
    pub signed_qty: f64,
    pub gross_notional_usdt: f64,
    pub net_usdt_exposure: f64,
    pub open_fee: f64,
    pub close_fee: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillApplication {
    pub bundle_id: String,
    pub leg_id: String,
    pub side: PositionSide,
    pub reduce_only: bool,
    pub applied_qty: f64,
    pub applied_price: f64,
    pub fee: f64,
    pub status_after: LegStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionReconcileDecision {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub side: PositionSide,
    pub local_qty: f64,
    pub exchange_qty: f64,
    pub drift_qty: f64,
    pub severity: ReconcileSeverity,
    pub recommended_actions: Vec<ReconcileAction>,
    pub checked_at: DateTime<Utc>,
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
    #[error("open fill {requested} exceeds remaining quantity {remaining} for leg {leg_id}")]
    OpenFillExceedsIntended {
        leg_id: String,
        requested: f64,
        remaining: f64,
    },
    #[error("close fill {requested} exceeds open quantity {available} for leg {leg_id}")]
    CloseFillExceedsOpen {
        leg_id: String,
        requested: f64,
        available: f64,
    },
    #[error("fill {trade_id} does not match bundle {bundle_id}")]
    FillDoesNotMatchBundle { bundle_id: String, trade_id: String },
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionManager {
    bundles: HashMap<String, BundlePosition>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_from_bundle(
        &mut self,
        bundle: &ArbitrageBundle,
        target_qty: f64,
        entry_edge_pct: f64,
        now: DateTime<Utc>,
    ) {
        let long_exchange_symbol =
            exchange_symbol_for(&bundle.long_exchange, &bundle.canonical_symbol);
        let short_exchange_symbol =
            exchange_symbol_for(&bundle.short_exchange, &bundle.canonical_symbol);
        let mut position = BundlePosition::new(
            bundle.bundle_id.clone(),
            bundle.canonical_symbol.clone(),
            bundle.long_exchange.clone(),
            long_exchange_symbol,
            bundle.short_exchange.clone(),
            short_exchange_symbol,
            target_qty.max(0.0),
            bundle.target_notional,
            entry_edge_pct,
            now,
        );
        position.status = bundle.status;
        position.opened_at = bundle.open_time;
        position.updated_at = bundle.updated_at;
        self.upsert_bundle(position);
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

    pub fn apply_fill_event(
        &mut self,
        bundle_id: &str,
        fill: &FillEvent,
    ) -> Result<FillApplication, PositionError> {
        let bundle = self
            .bundles
            .get_mut(bundle_id)
            .ok_or_else(|| PositionError::BundleNotFound(bundle_id.to_string()))?;
        let side = bundle.resolve_fill_side(fill)?;
        let reduce_only = fill.reduce_only.unwrap_or(false);
        let fee = fill.fee.unwrap_or_default().max(0.0);
        let leg = bundle.leg_mut(side)?;

        if reduce_only {
            leg.record_close_fill(
                fill.quantity,
                fill.price,
                fee,
                fill.realized_pnl.unwrap_or_default(),
                fill.filled_at,
            )?;
        } else {
            leg.record_open_fill(fill.quantity, fill.price, fee, fill.filled_at)?;
            if bundle.opened_at.is_none() {
                bundle.opened_at = Some(fill.filled_at);
            }
        }

        let application = FillApplication {
            bundle_id: bundle.bundle_id.clone(),
            leg_id: bundle.leg(side)?.leg_id.clone(),
            side,
            reduce_only,
            applied_qty: fill.quantity,
            applied_price: fill.price,
            fee,
            status_after: bundle.leg(side)?.status,
        };

        bundle.refresh_exposure(fill.filled_at);
        Ok(application)
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

    pub fn risk_summary_by_leg(&self) -> Vec<LegRiskSummary> {
        let mut summaries: HashMap<
            (ExchangeId, CanonicalSymbol, ExchangeSymbol, PositionSide),
            LegRiskSummary,
        > = HashMap::new();

        for bundle in self
            .bundles
            .values()
            .filter(|bundle| !bundle.status.is_terminal())
        {
            for leg in [&bundle.long_leg, &bundle.short_leg] {
                let key = (
                    leg.exchange.clone(),
                    bundle.canonical_symbol.clone(),
                    leg.exchange_symbol.clone(),
                    leg.side,
                );
                let summary = summaries.entry(key).or_insert_with(|| LegRiskSummary {
                    exchange: leg.exchange.clone(),
                    canonical_symbol: bundle.canonical_symbol.clone(),
                    exchange_symbol: leg.exchange_symbol.clone(),
                    side: leg.side,
                    bundle_count: 0,
                    filled_qty: 0.0,
                    signed_qty: 0.0,
                    gross_notional_usdt: 0.0,
                    net_usdt_exposure: 0.0,
                    open_fee: 0.0,
                    close_fee: 0.0,
                    realized_pnl: 0.0,
                    unrealized_pnl: 0.0,
                });
                summary.bundle_count += 1;
                summary.filled_qty += leg.filled_qty;
                summary.signed_qty += leg.signed_qty();
                summary.gross_notional_usdt += leg.gross_notional();
                summary.net_usdt_exposure += leg.signed_notional();
                summary.open_fee += leg.open_fee;
                summary.close_fee += leg.close_fee;
                summary.realized_pnl += leg.realized_pnl;
                summary.unrealized_pnl += leg.unrealized_pnl;
            }
        }

        let mut result: Vec<_> = summaries.into_values().collect();
        result.sort_by(|left, right| {
            (
                left.exchange.as_str(),
                left.canonical_symbol.as_pair(),
                left.exchange_symbol.symbol.as_str(),
                side_sort_key(left.side),
            )
                .cmp(&(
                    right.exchange.as_str(),
                    right.canonical_symbol.as_pair(),
                    right.exchange_symbol.symbol.as_str(),
                    side_sort_key(right.side),
                ))
        });
        result
    }

    pub fn reconcile_exchange_positions(
        &self,
        exchange_positions: &[ExchangePosition],
        quantity_tolerance: f64,
        orphan_tolerance: f64,
        checked_at: DateTime<Utc>,
    ) -> Vec<PositionReconcileDecision> {
        let mut exchange_by_key: HashMap<
            (
                ExchangeId,
                CanonicalSymbol,
                Option<ExchangeSymbol>,
                PositionSide,
            ),
            &ExchangePosition,
        > = exchange_positions
            .iter()
            .map(|position| {
                (
                    (
                        position.exchange.clone(),
                        position.canonical_symbol.clone(),
                        Some(position.exchange_symbol.clone()),
                        position.position_side,
                    ),
                    position,
                )
            })
            .collect();

        let mut decisions = Vec::new();
        for summary in self.risk_summary_by_leg() {
            let key = (
                summary.exchange.clone(),
                summary.canonical_symbol.clone(),
                Some(summary.exchange_symbol.clone()),
                summary.side,
            );
            let exchange_position = exchange_by_key.remove(&key);
            decisions.push(reconcile_quantities(
                summary.exchange,
                summary.canonical_symbol,
                Some(summary.exchange_symbol),
                summary.side,
                summary.filled_qty,
                exchange_position
                    .map(|position| position.quantity)
                    .unwrap_or(0.0),
                quantity_tolerance,
                orphan_tolerance,
                checked_at,
            ));
        }

        for position in exchange_by_key.into_values() {
            decisions.push(reconcile_quantities(
                position.exchange.clone(),
                position.canonical_symbol.clone(),
                Some(position.exchange_symbol.clone()),
                position.position_side,
                0.0,
                position.quantity,
                quantity_tolerance,
                orphan_tolerance,
                checked_at,
            ));
        }

        decisions.sort_by(|left, right| {
            (
                left.exchange.as_str(),
                left.canonical_symbol.as_pair(),
                left.exchange_symbol
                    .as_ref()
                    .map(|symbol| symbol.symbol.as_str()),
                side_sort_key(left.side),
            )
                .cmp(&(
                    right.exchange.as_str(),
                    right.canonical_symbol.as_pair(),
                    right
                        .exchange_symbol
                        .as_ref()
                        .map(|symbol| symbol.symbol.as_str()),
                    side_sort_key(right.side),
                ))
        });
        decisions
    }
}

fn side_sort_key(side: PositionSide) -> u8 {
    match side {
        PositionSide::Long => 0,
        PositionSide::Short => 1,
        PositionSide::Net => 2,
    }
}

#[allow(clippy::too_many_arguments)]
fn reconcile_quantities(
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    exchange_symbol: Option<ExchangeSymbol>,
    side: PositionSide,
    local_qty: f64,
    exchange_qty: f64,
    quantity_tolerance: f64,
    orphan_tolerance: f64,
    checked_at: DateTime<Utc>,
) -> PositionReconcileDecision {
    let drift_qty = (local_qty.abs() - exchange_qty.abs()).abs();
    let severity = if drift_qty <= quantity_tolerance {
        ReconcileSeverity::Ok
    } else if local_qty.abs() <= orphan_tolerance && exchange_qty.abs() > orphan_tolerance {
        ReconcileSeverity::OrphanExposure
    } else if exchange_qty.abs() <= orphan_tolerance && local_qty.abs() > orphan_tolerance {
        ReconcileSeverity::PositionDrift
    } else if drift_qty <= orphan_tolerance {
        ReconcileSeverity::MinorDrift
    } else {
        ReconcileSeverity::PositionDrift
    };

    PositionReconcileDecision {
        exchange,
        canonical_symbol,
        exchange_symbol,
        side,
        local_qty,
        exchange_qty,
        drift_qty,
        severity,
        recommended_actions: recommended_actions_for_severity(severity),
        checked_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::FillLiquidity;

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

    #[allow(clippy::too_many_arguments)]
    fn fill_event(
        exchange: ExchangeId,
        exchange_symbol: ExchangeSymbol,
        position_side: PositionSide,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        now: DateTime<Utc>,
    ) -> FillEvent {
        FillEvent {
            exchange,
            canonical_symbol: CanonicalSymbol::new("ARB", "USDT"),
            exchange_symbol,
            trade_id: format!("trade-{quantity}-{price}-{reduce_only}"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            side,
            position_side,
            liquidity: FillLiquidity::Taker,
            price,
            quantity,
            quote_quantity: quantity * price,
            fee: Some(0.01),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0004),
            realized_pnl: reduce_only.then_some(1.25),
            reduce_only: Some(reduce_only),
            filled_at: now,
            received_at: now,
        }
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
    fn cross_exchange_arbitrage_position_should_apply_fill_event_to_matching_leg() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        let fill = fill_event(
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
            PositionSide::Long,
            OrderSide::Buy,
            4.0,
            10.0,
            false,
            now,
        );
        let application = manager.apply_fill_event("bundle-1", &fill).unwrap();

        assert_eq!(application.leg_id, "bundle-1:long");
        assert_eq!(application.side, PositionSide::Long);
        assert_eq!(application.status_after, LegStatus::PartiallyOpen);

        let bundle = manager.bundle("bundle-1").unwrap();
        assert_eq!(bundle.long_leg.filled_qty, 4.0);
        assert_eq!(bundle.long_leg.remaining_qty, 6.0);
        assert_eq!(bundle.long_leg.avg_entry_price, Some(10.0));
        assert_eq!(bundle.gross_notional_usdt, 40.0);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_apply_partial_then_complete_fill_events() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        let first = fill_event(
            ExchangeId::Okx,
            ExchangeSymbol::new(ExchangeId::Okx, "ARB-USDT-SWAP"),
            PositionSide::Short,
            OrderSide::Sell,
            3.0,
            10.2,
            false,
            now,
        );
        let second = fill_event(
            ExchangeId::Okx,
            ExchangeSymbol::new(ExchangeId::Okx, "ARB-USDT-SWAP"),
            PositionSide::Short,
            OrderSide::Sell,
            7.0,
            10.4,
            false,
            now,
        );

        manager.apply_fill_event("bundle-1", &first).unwrap();
        assert_eq!(
            manager.bundle("bundle-1").unwrap().short_leg.status,
            LegStatus::PartiallyOpen
        );

        manager.apply_fill_event("bundle-1", &second).unwrap();
        let short_leg = &manager.bundle("bundle-1").unwrap().short_leg;
        assert_eq!(short_leg.status, LegStatus::Open);
        assert_eq!(short_leg.filled_qty, 10.0);
        assert_eq!(short_leg.remaining_qty, 0.0);
        assert!((short_leg.avg_entry_price.unwrap() - 10.34).abs() < 1e-9);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_apply_reduce_only_fill_as_close() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));
        manager
            .record_leg_fill("bundle-1", PositionSide::Long, 5.0, 10.0, 0.0, now)
            .unwrap();

        let close_fill = fill_event(
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
            PositionSide::Long,
            OrderSide::Sell,
            5.0,
            10.5,
            true,
            now,
        );
        let application = manager.apply_fill_event("bundle-1", &close_fill).unwrap();

        let long_leg = &manager.bundle("bundle-1").unwrap().long_leg;
        assert!(application.reduce_only);
        assert_eq!(long_leg.status, LegStatus::Closed);
        assert_eq!(long_leg.filled_qty, 0.0);
        assert_eq!(long_leg.avg_exit_price, Some(10.5));
        assert_eq!(long_leg.realized_pnl, 1.25);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_reject_overfilled_open_fill_event() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        let fill = fill_event(
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
            PositionSide::Long,
            OrderSide::Buy,
            10.1,
            10.0,
            false,
            now,
        );
        let err = manager.apply_fill_event("bundle-1", &fill).unwrap_err();

        assert!(matches!(err, PositionError::OpenFillExceedsIntended { .. }));
        assert_eq!(manager.bundle("bundle-1").unwrap().long_leg.filled_qty, 0.0);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_reject_unknown_fill_leg() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        let fill = fill_event(
            ExchangeId::Bitget,
            ExchangeSymbol::new(ExchangeId::Bitget, "ARBUSDT"),
            PositionSide::Long,
            OrderSide::Buy,
            1.0,
            10.0,
            false,
            now,
        );
        let err = manager.apply_fill_event("bundle-1", &fill).unwrap_err();

        assert!(matches!(err, PositionError::FillDoesNotMatchBundle { .. }));
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_reject_wrong_direction_fill() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));

        let fill = fill_event(
            ExchangeId::Binance,
            ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
            PositionSide::Long,
            OrderSide::Sell,
            1.0,
            10.0,
            false,
            now,
        );
        let err = manager.apply_fill_event("bundle-1", &fill).unwrap_err();

        assert!(matches!(err, PositionError::FillDoesNotMatchBundle { .. }));
        assert_eq!(manager.bundle("bundle-1").unwrap().long_leg.filled_qty, 0.0);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_aggregate_leg_risk_summary() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));
        manager
            .record_leg_fill("bundle-1", PositionSide::Long, 4.0, 10.0, 0.02, now)
            .unwrap();
        manager
            .record_leg_fill("bundle-1", PositionSide::Short, 2.0, 10.2, 0.03, now)
            .unwrap();

        let summary = manager.risk_summary_by_leg();

        assert_eq!(summary.len(), 2);
        let long = summary
            .iter()
            .find(|item| item.side == PositionSide::Long)
            .unwrap();
        let short = summary
            .iter()
            .find(|item| item.side == PositionSide::Short)
            .unwrap();
        assert_eq!(long.filled_qty, 4.0);
        assert_eq!(long.signed_qty, 4.0);
        assert_eq!(long.gross_notional_usdt, 40.0);
        assert_eq!(short.filled_qty, 2.0);
        assert_eq!(short.signed_qty, -2.0);
        assert_eq!(short.net_usdt_exposure, -20.4);
    }

    #[test]
    fn cross_exchange_arbitrage_position_should_reconcile_exchange_positions() {
        let now = Utc::now();
        let mut manager = PositionManager::new();
        manager.upsert_bundle(test_bundle(now));
        manager
            .record_leg_fill("bundle-1", PositionSide::Long, 4.0, 10.0, 0.0, now)
            .unwrap();

        let decisions = manager.reconcile_exchange_positions(
            &[ExchangePosition {
                exchange: ExchangeId::Binance,
                canonical_symbol: CanonicalSymbol::new("ARB", "USDT"),
                exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
                position_side: PositionSide::Long,
                quantity: 4.02,
                entry_price: Some(10.0),
                mark_price: Some(10.1),
                unrealized_pnl: Some(0.4),
                updated_at: now,
            }],
            0.001,
            0.05,
            now,
        );

        let long = decisions
            .iter()
            .find(|decision| decision.side == PositionSide::Long)
            .unwrap();
        let short = decisions
            .iter()
            .find(|decision| decision.side == PositionSide::Short)
            .unwrap();
        assert_eq!(long.severity, ReconcileSeverity::MinorDrift);
        assert_eq!(
            long.recommended_actions,
            vec![ReconcileAction::AdjustLocalLedger]
        );
        assert_eq!(short.severity, ReconcileSeverity::Ok);
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

    #[test]
    fn cross_exchange_arbitrage_position_should_upsert_from_execution_bundle() {
        let now = Utc::now();
        let bundle = ArbitrageBundle::new(
            "bundle-1",
            crate::market::RuntimeMode::Simulation,
            CanonicalSymbol::new("ARB", "USDT"),
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Binance,
            ExchangeId::Okx,
            100.0,
            now,
        );
        let mut manager = PositionManager::new();

        manager.upsert_from_bundle(&bundle, 2.5, 0.0125, now);

        let position = manager.bundle("bundle-1").unwrap();
        assert_eq!(
            position.canonical_symbol,
            CanonicalSymbol::new("ARB", "USDT")
        );
        assert_eq!(position.status, BundleStatus::Observing);
        assert_eq!(position.target_notional_usdt, 100.0);
        assert_eq!(position.entry_edge_pct, 0.0125);
        assert_eq!(position.long_leg.exchange, ExchangeId::Binance);
        assert_eq!(position.short_leg.exchange, ExchangeId::Okx);
        assert_eq!(position.long_leg.intended_qty, 2.5);
        assert_eq!(position.short_leg.intended_qty, 2.5);
    }
}
