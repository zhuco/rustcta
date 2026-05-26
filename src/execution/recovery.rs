use crate::execution::{ArbitrageBundle, BundleLeg};
use crate::market::RouteStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrphanRecoveryAction {
    RetryHedgeWithWiderSlippage { leg: BundleLeg, attempt: u32 },
    ReverseMakerLegWithTaker { leg: BundleLeg },
    EnterCloseOnlyAndAlert,
    ManualIntervention,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrphanRecoveryRecommendation {
    pub bundle_id: String,
    pub action: OrphanRecoveryAction,
    pub unhedged_qty: f64,
    pub route_status: RouteStatus,
    pub alert_required: bool,
    pub reason: String,
    pub recommended_at: DateTime<Utc>,
}

pub fn recommend_orphan_recovery(
    bundle: &ArbitrageBundle,
    route_status: RouteStatus,
    hedge_attempts: u32,
    max_hedge_retries: u32,
    recommended_at: DateTime<Utc>,
) -> OrphanRecoveryRecommendation {
    let unhedged_qty = bundle.unhedged_qty();
    let (action, reason, alert_required) =
        if route_status.allows_closes() && hedge_attempts < max_hedge_retries {
            (
                OrphanRecoveryAction::RetryHedgeWithWiderSlippage {
                    leg: BundleLeg::Hedge,
                    attempt: hedge_attempts + 1,
                },
                "retry hedge with bounded wider slippage".to_string(),
                true,
            )
        } else if route_status.allows_closes() {
            (
                OrphanRecoveryAction::ReverseMakerLegWithTaker {
                    leg: if bundle.long_qty > bundle.short_qty {
                        BundleLeg::EmergencyCloseLong
                    } else {
                        BundleLeg::EmergencyCloseShort
                    },
                },
                "hedge retry budget exhausted; reverse exposed maker leg".to_string(),
                true,
            )
        } else {
            (
                OrphanRecoveryAction::EnterCloseOnlyAndAlert,
                "route does not allow automated close; require immediate operator attention"
                    .to_string(),
                true,
            )
        };

    OrphanRecoveryRecommendation {
        bundle_id: bundle.bundle_id.clone(),
        action,
        unhedged_qty,
        route_status,
        alert_required,
        reason,
        recommended_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::BundleStatus;
    use crate::market::{CanonicalSymbol, ExchangeId, RuntimeMode};

    #[test]
    fn recovery_should_recommend_orphan_action() {
        let now = Utc::now();
        let mut bundle = ArbitrageBundle::new(
            "bundle-orphan",
            RuntimeMode::Simulation,
            CanonicalSymbol::new("eth", "usdt"),
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Binance,
            ExchangeId::Okx,
            100.0,
            now,
        );
        bundle.status = BundleStatus::OrphanLeg;
        bundle.long_qty = 1.0;
        bundle.short_qty = 0.0;

        let recommendation = recommend_orphan_recovery(&bundle, RouteStatus::Healthy, 0, 1, now);

        assert_eq!(recommendation.unhedged_qty, 1.0);
        assert!(matches!(
            recommendation.action,
            OrphanRecoveryAction::RetryHedgeWithWiderSlippage { .. }
        ));
        assert!(recommendation.alert_required);
    }
}
