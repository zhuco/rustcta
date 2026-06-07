use crate::execution::{ArbitrageBundle, BundleStatus, CloseReason};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum BundleTransitionError {
    #[error("invalid bundle transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: BundleStatus,
        to: BundleStatus,
    },
}

pub fn transition_bundle(
    bundle: &mut ArbitrageBundle,
    next: BundleStatus,
    now: DateTime<Utc>,
) -> Result<(), BundleTransitionError> {
    validate_transition(bundle.status, next)?;

    bundle.status = next;
    bundle.updated_at = now;

    if matches!(next, BundleStatus::OpenSimulated) && bundle.open_time.is_none() {
        bundle.open_time = Some(now);
    }
    if matches!(
        next,
        BundleStatus::Closed | BundleStatus::Expired | BundleStatus::RiskStopped
    ) && bundle.close_time.is_none()
    {
        bundle.close_time = Some(now);
    }

    Ok(())
}

pub fn close_bundle(
    bundle: &mut ArbitrageBundle,
    reason: CloseReason,
    now: DateTime<Utc>,
) -> Result<(), BundleTransitionError> {
    transition_bundle(bundle, BundleStatus::Closed, now)?;
    bundle.close_reason = Some(reason);
    Ok(())
}

pub fn validate_transition(
    from: BundleStatus,
    to: BundleStatus,
) -> Result<(), BundleTransitionError> {
    if from == to {
        return Ok(());
    }

    let allowed = match from {
        BundleStatus::Observing => matches!(
            to,
            BundleStatus::MakerPending
                | BundleStatus::DepthInsufficient
                | BundleStatus::OneSidedExposure
                | BundleStatus::Expired
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::MakerPending => matches!(
            to,
            BundleStatus::MakerFilled
                | BundleStatus::MakerTimeout
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::MakerTimeout => matches!(
            to,
            BundleStatus::Closed | BundleStatus::Expired | BundleStatus::RiskStopped
        ),
        BundleStatus::MakerFilled => matches!(
            to,
            BundleStatus::Hedging
                | BundleStatus::OrphanLeg
                | BundleStatus::OneSidedExposure
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::Hedging => matches!(
            to,
            BundleStatus::OpenSimulated
                | BundleStatus::OrphanLeg
                | BundleStatus::OneSidedExposure
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::OpenSimulated => matches!(
            to,
            BundleStatus::ClosingSimulated
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::ClosingSimulated => matches!(
            to,
            BundleStatus::Closed
                | BundleStatus::OrphanLeg
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::OrphanLeg => matches!(
            to,
            BundleStatus::Hedging
                | BundleStatus::ClosingSimulated
                | BundleStatus::Closed
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::OneSidedExposure => matches!(
            to,
            BundleStatus::ClosingSimulated
                | BundleStatus::OrphanLeg
                | BundleStatus::Closed
                | BundleStatus::RiskStopped
                | BundleStatus::ReconcileRequired
        ),
        BundleStatus::ReconcileRequired => matches!(
            to,
            BundleStatus::OpenSimulated
                | BundleStatus::ClosingSimulated
                | BundleStatus::OrphanLeg
                | BundleStatus::OneSidedExposure
                | BundleStatus::Closed
                | BundleStatus::RiskStopped
        ),
        BundleStatus::DepthInsufficient => matches!(
            to,
            BundleStatus::Observing | BundleStatus::Expired | BundleStatus::RiskStopped
        ),
        BundleStatus::Closed | BundleStatus::Expired | BundleStatus::RiskStopped => false,
    };

    if allowed {
        Ok(())
    } else {
        Err(BundleTransitionError::InvalidTransition { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{CanonicalSymbol, ExchangeId, RuntimeMode};

    fn test_bundle() -> ArbitrageBundle {
        ArbitrageBundle::new(
            "bundle-1",
            RuntimeMode::Simulation,
            CanonicalSymbol::new("btc", "usdt"),
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Binance,
            ExchangeId::Okx,
            100.0,
            Utc::now(),
        )
    }

    #[test]
    fn state_machine_should_open_bundle_normally() {
        let now = Utc::now();
        let mut bundle = test_bundle();

        transition_bundle(&mut bundle, BundleStatus::MakerPending, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::MakerFilled, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::Hedging, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::OpenSimulated, now).unwrap();

        assert_eq!(bundle.status, BundleStatus::OpenSimulated);
        assert_eq!(bundle.open_time, Some(now));
    }

    #[test]
    fn state_machine_should_handle_maker_timeout() {
        let now = Utc::now();
        let mut bundle = test_bundle();

        transition_bundle(&mut bundle, BundleStatus::MakerPending, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::MakerTimeout, now).unwrap();
        close_bundle(&mut bundle, CloseReason::MakerTimeout, now).unwrap();

        assert_eq!(bundle.status, BundleStatus::Closed);
        assert_eq!(bundle.close_reason, Some(CloseReason::MakerTimeout));
    }

    #[test]
    fn state_machine_should_mark_orphan_after_failed_hedge() {
        let now = Utc::now();
        let mut bundle = test_bundle();

        transition_bundle(&mut bundle, BundleStatus::MakerPending, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::MakerFilled, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::Hedging, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::OrphanLeg, now).unwrap();

        assert_eq!(bundle.status, BundleStatus::OrphanLeg);
    }

    #[test]
    fn state_machine_should_recover_one_sided_exposure_through_close_path() {
        let now = Utc::now();
        let mut bundle = test_bundle();

        transition_bundle(&mut bundle, BundleStatus::OneSidedExposure, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::ClosingSimulated, now).unwrap();
        transition_bundle(&mut bundle, BundleStatus::Closed, now).unwrap();

        assert_eq!(bundle.status, BundleStatus::Closed);
        assert_eq!(bundle.close_time, Some(now));
    }
}
