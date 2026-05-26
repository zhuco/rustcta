use crate::execution::{
    ArbitrageBundle, BundleStatus, OrderCommand, OrphanRecoveryRecommendation, ReconcileReport,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LedgerEventKind {
    BundleRecorded {
        bundle_id: String,
    },
    BundleStatusChanged {
        bundle_id: String,
        status: BundleStatus,
    },
    OrderRecorded {
        command_id: String,
        client_order_id: String,
    },
    ReconcileRecorded {
        exchange: String,
        symbol: String,
    },
    RecoveryRecommended {
        bundle_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub event_id: u64,
    pub kind: LedgerEventKind,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionLedger {
    bundles: HashMap<String, ArbitrageBundle>,
    orders: HashMap<String, OrderCommand>,
    reconcile_reports: Vec<ReconcileReport>,
    recovery_recommendations: Vec<OrphanRecoveryRecommendation>,
    events: Vec<LedgerEvent>,
    next_event_id: u64,
}

impl ExecutionLedger {
    pub fn record_bundle(&mut self, bundle: ArbitrageBundle, recorded_at: DateTime<Utc>) {
        let bundle_id = bundle.bundle_id.clone();
        let status = bundle.status;
        self.bundles.insert(bundle_id.clone(), bundle);
        self.push_event(
            LedgerEventKind::BundleRecorded {
                bundle_id: bundle_id.clone(),
            },
            recorded_at,
        );
        self.push_event(
            LedgerEventKind::BundleStatusChanged { bundle_id, status },
            recorded_at,
        );
    }

    pub fn record_order(&mut self, command: OrderCommand, recorded_at: DateTime<Utc>) {
        let command_id = command.command_id.clone();
        let client_order_id = command.client_order_id.clone();
        self.orders.insert(command_id.clone(), command);
        self.push_event(
            LedgerEventKind::OrderRecorded {
                command_id,
                client_order_id,
            },
            recorded_at,
        );
    }

    pub fn record_reconcile_report(&mut self, report: ReconcileReport, recorded_at: DateTime<Utc>) {
        let exchange = report.exchange.to_string();
        let symbol = report.canonical_symbol.to_string();
        self.reconcile_reports.push(report);
        self.push_event(
            LedgerEventKind::ReconcileRecorded { exchange, symbol },
            recorded_at,
        );
    }

    pub fn record_recovery_recommendation(
        &mut self,
        recommendation: OrphanRecoveryRecommendation,
        recorded_at: DateTime<Utc>,
    ) {
        let bundle_id = recommendation.bundle_id.clone();
        self.recovery_recommendations.push(recommendation);
        self.push_event(
            LedgerEventKind::RecoveryRecommended { bundle_id },
            recorded_at,
        );
    }

    pub fn bundle(&self, bundle_id: &str) -> Option<&ArbitrageBundle> {
        self.bundles.get(bundle_id)
    }

    pub fn order_by_command_id(&self, command_id: &str) -> Option<&OrderCommand> {
        self.orders.get(command_id)
    }

    pub fn events(&self) -> &[LedgerEvent] {
        &self.events
    }

    pub fn reconcile_reports(&self) -> &[ReconcileReport] {
        &self.reconcile_reports
    }

    fn push_event(&mut self, kind: LedgerEventKind, recorded_at: DateTime<Utc>) {
        self.next_event_id += 1;
        self.events.push(LedgerEvent {
            event_id: self.next_event_id,
            kind,
            recorded_at,
        });
    }
}
