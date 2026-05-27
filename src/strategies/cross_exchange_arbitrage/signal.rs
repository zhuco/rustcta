//! Strategy signal contract. Signals are side-effect free and consumed by simulation/execution.

use super::opportunity::Opportunity;
use super::risk::RejectReason;
use crate::market::RuntimeMode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ArbSignalAction {
    Open,
    Close,
    EmergencyClose,
    Cancel,
    Noop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbSignal {
    pub signal_id: String,
    pub mode: RuntimeMode,
    pub opportunity_id: Option<String>,
    pub action: ArbSignalAction,
    pub confidence: f64,
    pub expected_edge: f64,
    pub max_notional_usdt: f64,
    pub risk_flags: Vec<RejectReason>,
    pub generated_at: DateTime<Utc>,
}

impl ArbSignal {
    pub fn from_opportunity(
        opportunity: &Opportunity,
        mode: RuntimeMode,
        generated_at: DateTime<Utc>,
    ) -> Self {
        let action = if opportunity.can_open {
            ArbSignalAction::Open
        } else {
            ArbSignalAction::Noop
        };
        Self {
            signal_id: format!(
                "signal-{}-{}",
                opportunity.opportunity_id,
                generated_at.timestamp_millis()
            ),
            mode,
            opportunity_id: Some(opportunity.opportunity_id.clone()),
            action,
            confidence: confidence_from_edge(opportunity.maker_taker_net_edge),
            expected_edge: opportunity.maker_taker_net_edge,
            max_notional_usdt: opportunity.executable_notional_usdt,
            risk_flags: opportunity.reject_reasons.clone(),
            generated_at,
        }
    }

    pub fn noop(mode: RuntimeMode, generated_at: DateTime<Utc>) -> Self {
        Self {
            signal_id: format!("signal-noop-{}", generated_at.timestamp_millis()),
            mode,
            opportunity_id: None,
            action: ArbSignalAction::Noop,
            confidence: 0.0,
            expected_edge: 0.0,
            max_notional_usdt: 0.0,
            risk_flags: Vec::new(),
            generated_at,
        }
    }
}

fn confidence_from_edge(edge: f64) -> f64 {
    (edge / 0.01).clamp(0.0, 1.0)
}
