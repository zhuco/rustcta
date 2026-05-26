use crate::market::{CanonicalSymbol, ExchangeId, RuntimeMode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleStatus {
    Observing,
    MakerPending,
    MakerTimeout,
    MakerFilled,
    Hedging,
    OpenSimulated,
    ClosingSimulated,
    Closed,
    Expired,
    OrphanLeg,
    RiskStopped,
    DepthInsufficient,
    ReconcileRequired,
}

impl BundleStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Expired | Self::RiskStopped)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleLeg {
    Long,
    Short,
    Maker,
    Taker,
    Hedge,
    CloseLong,
    CloseShort,
    EmergencyCloseLong,
    EmergencyCloseShort,
}

impl BundleLeg {
    pub fn as_slug(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
            Self::Maker => "maker",
            Self::Taker => "taker",
            Self::Hedge => "hedge",
            Self::CloseLong => "close-long",
            Self::CloseShort => "close-short",
            Self::EmergencyCloseLong => "emergency-close-long",
            Self::EmergencyCloseShort => "emergency-close-short",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CloseReason {
    TargetReached,
    StopLoss,
    SpreadMeanReverted,
    FundingRisk,
    RouteDegraded,
    Manual,
    MakerTimeout,
    OrphanRecovered,
    ReconcileRecovery,
    EmergencyRisk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageBundle {
    pub bundle_id: String,
    pub mode: RuntimeMode,
    pub canonical_symbol: CanonicalSymbol,
    pub status: BundleStatus,
    pub open_time: Option<DateTime<Utc>>,
    pub close_time: Option<DateTime<Utc>>,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub taker_exchange: ExchangeId,
    pub long_entry_vwap: Option<f64>,
    pub short_entry_vwap: Option<f64>,
    pub long_qty: f64,
    pub short_qty: f64,
    pub target_notional: f64,
    pub entry_spread: Option<f64>,
    pub current_spread: Option<f64>,
    pub open_fee: f64,
    pub close_fee: f64,
    pub funding_pnl: f64,
    pub gross_spread_pnl: f64,
    pub net_pnl: f64,
    pub max_adverse_spread: Option<f64>,
    pub max_favorable_spread: Option<f64>,
    pub orphan_loss: f64,
    pub close_reason: Option<CloseReason>,
    pub last_reconcile_at: Option<DateTime<Utc>>,
    pub created_from_signal_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ArbitrageBundle {
    pub fn new(
        bundle_id: impl Into<String>,
        mode: RuntimeMode,
        canonical_symbol: CanonicalSymbol,
        long_exchange: ExchangeId,
        short_exchange: ExchangeId,
        maker_exchange: ExchangeId,
        taker_exchange: ExchangeId,
        target_notional: f64,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            bundle_id: bundle_id.into(),
            mode,
            canonical_symbol,
            status: BundleStatus::Observing,
            open_time: None,
            close_time: None,
            long_exchange,
            short_exchange,
            maker_exchange,
            taker_exchange,
            long_entry_vwap: None,
            short_entry_vwap: None,
            long_qty: 0.0,
            short_qty: 0.0,
            target_notional,
            entry_spread: None,
            current_spread: None,
            open_fee: 0.0,
            close_fee: 0.0,
            funding_pnl: 0.0,
            gross_spread_pnl: 0.0,
            net_pnl: 0.0,
            max_adverse_spread: None,
            max_favorable_spread: None,
            orphan_loss: 0.0,
            close_reason: None,
            last_reconcile_at: None,
            created_from_signal_id: None,
            created_at,
            updated_at: created_at,
        }
    }

    pub fn has_balanced_open_qty(&self, tolerance: f64) -> bool {
        (self.long_qty - self.short_qty).abs() <= tolerance
    }

    pub fn unhedged_qty(&self) -> f64 {
        (self.long_qty - self.short_qty).abs()
    }
}
