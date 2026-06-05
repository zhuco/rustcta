use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;

use super::models::*;
use crate::control::spot_control::{SpotControlRuntimePublisher, SpotControlService};

#[derive(Clone)]
pub struct MonitoringState {
    config: MonitoringConfig,
    inner: Arc<RwLock<DashboardReadModel>>,
    control: Option<SpotControlService>,
    runtime_publisher: Option<SpotControlRuntimePublisher>,
}

impl MonitoringState {
    pub fn new(config: MonitoringConfig) -> Self {
        Self {
            config,
            inner: Arc::new(RwLock::new(DashboardReadModel::default())),
            control: None,
            runtime_publisher: None,
        }
    }

    pub fn from_read_model(config: MonitoringConfig, model: DashboardReadModel) -> Self {
        Self {
            config,
            inner: Arc::new(RwLock::new(model)),
            control: None,
            runtime_publisher: None,
        }
    }

    pub fn with_control_service(mut self, control: SpotControlService) -> Self {
        self.control = Some(control);
        self
    }

    pub fn with_runtime_publisher(mut self, publisher: SpotControlRuntimePublisher) -> Self {
        self.runtime_publisher = Some(publisher);
        self
    }

    pub fn config(&self) -> &MonitoringConfig {
        &self.config
    }

    pub fn control(&self) -> Option<&SpotControlService> {
        self.control.as_ref()
    }

    pub fn runtime_publisher(&self) -> Option<&SpotControlRuntimePublisher> {
        self.runtime_publisher.as_ref()
    }

    pub async fn snapshot(&self) -> DashboardReadModel {
        self.inner.read().await.clone()
    }

    pub async fn status(&self) -> StatusView {
        let model = self.inner.read().await;
        status_from_model(&model)
    }

    pub async fn update_model(&self, update: impl FnOnce(&mut DashboardReadModel)) {
        let mut model = self.inner.write().await;
        update(&mut model);
    }

    pub fn try_update_model(&self, update: impl FnOnce(&mut DashboardReadModel)) -> bool {
        match self.inner.try_write() {
            Ok(mut model) => {
                update(&mut model);
                true
            }
            Err(_) => false,
        }
    }

    pub fn publish_strategy_status(&self, status: impl Into<String>) {
        let status = status.into();
        self.try_update_model(|model| {
            model.strategy.status = status;
            model.strategy.last_loop_at = Some(Utc::now());
        });
    }

    pub fn publish_books(&self, books: Vec<BookView>) {
        self.try_update_model(|model| {
            model.books = books;
        });
    }

    pub fn publish_spot_symbol_rules(&self, rules: Vec<crate::exchanges::unified::SymbolRule>) {
        self.try_update_model(|model| {
            model.spot_symbol_rules = rules;
        });
    }

    pub fn publish_exchanges(&self, exchanges: Vec<ExchangeHealthView>) {
        self.try_update_model(|model| {
            model.exchanges = exchanges;
        });
    }

    pub fn publish_inventory(&self, inventory: Vec<InventoryView>) {
        self.try_update_model(|model| {
            model.inventory = inventory;
        });
    }

    pub fn publish_fees(&self, fees: Vec<FeeView>) {
        self.try_update_model(|model| {
            model.fees = fees;
        });
    }

    pub fn publish_disabled(&self, disabled: DisabledView, unmanaged: Vec<UnmanagedPositionView>) {
        self.try_update_model(|model| {
            model.disabled = disabled;
            model.unmanaged_positions = unmanaged;
        });
    }

    pub fn publish_recorder(&self, recorder: RecorderHealthView) {
        self.try_update_model(|model| {
            model.recorder = recorder;
        });
    }

    pub fn publish_config_summary(&self, summary: ConfigSummaryView) {
        self.try_update_model(|model| {
            model.config_summary = summary;
        });
    }

    pub fn publish_live_preflight(&self, report: crate::live_preflight::LivePreflightReport) {
        self.try_update_model(|model| {
            model.live_preflight_enabled = true;
            model.live_preflight = Some(report);
        });
    }

    pub fn record_live_dry_run_order(&self, plan: crate::execution::LiveDryRunOrderPlan) {
        let limit = self.config.max_recent_trades.max(1);
        self.try_update_model(|model| {
            model.live_dry_run_orders.push(plan);
            trim_front(&mut model.live_dry_run_orders, limit);
        });
    }

    pub fn publish_order_reconciliation_config(
        &self,
        config: crate::execution::OrderReconciliationConfig,
    ) {
        self.try_update_model(|model| {
            model.order_reconciliation_config = config;
        });
    }

    pub fn publish_order_reconciliation_status(
        &self,
        status: crate::execution::OrderReconciliationResult,
    ) {
        self.try_update_model(|model| {
            model.order_reconciliation_status = Some(status);
        });
    }

    pub fn publish_balance_reconciliation(
        &self,
        report: crate::execution::BalanceReconciliationReport,
    ) {
        self.try_update_model(|model| {
            model.balance_reconciliation = Some(report);
        });
    }

    pub fn publish_kill_switch(&self, state: crate::risk::KillSwitchState) {
        self.try_update_model(|model| {
            model.kill_switch = Some(state);
        });
    }

    pub fn publish_small_live_gate(&self, report: crate::live_preflight::SmallLiveGateReport) {
        self.try_update_model(|model| {
            model.small_live_gate = Some(report);
        });
    }

    pub fn publish_arbitrage_relationships(
        &self,
        relationships: Vec<crate::strategies::arbitrage_core::ArbitrageRelationship>,
    ) {
        self.try_update_model(|model| {
            model.arbitrage_relationships = relationships;
        });
    }

    pub fn record_arbitrage_relationship(
        &self,
        relationship: crate::strategies::arbitrage_core::ArbitrageRelationship,
    ) {
        self.try_update_model(|model| {
            let exists = model.arbitrage_relationships.iter().any(|item| {
                item.relationship_type == relationship.relationship_type
                    && item.buy_leg.exchange == relationship.buy_leg.exchange
                    && item.sell_leg.exchange == relationship.sell_leg.exchange
                    && item.buy_leg.market_type == relationship.buy_leg.market_type
                    && item.sell_leg.market_type == relationship.sell_leg.market_type
                    && item.buy_leg.internal_symbol == relationship.buy_leg.internal_symbol
            });
            if !exists {
                model.arbitrage_relationships.push(relationship);
            }
        });
    }

    pub fn publish_arbitrage_statistics(
        &self,
        statistics: Vec<crate::strategies::arbitrage_core::ArbitrageStatisticsSnapshot>,
    ) {
        self.try_update_model(|model| {
            model.arbitrage_statistics = statistics;
        });
    }

    pub fn publish_five_exchange_scanner(
        &self,
        scanner: crate::scanner::FiveExchangeScannerReadModel,
    ) {
        self.try_update_model(|model| {
            model.five_exchange_scanner = scanner;
        });
    }

    pub fn publish_hedge_policy(&self, hedge_policy: crate::risk::HedgePolicyReadModel) {
        self.try_update_model(|model| {
            model.hedge_policy = hedge_policy;
        });
    }

    pub fn record_arbitrage_opportunity(
        &self,
        opportunity: crate::strategies::arbitrage_core::ArbitrageOpportunityAnalysis,
    ) {
        let limit = self.config.max_recent_opportunities.max(1);
        self.try_update_model(|model| {
            model.arbitrage_opportunities.push(opportunity);
            trim_front(&mut model.arbitrage_opportunities, limit);
        });
    }

    pub fn record_opportunity(&self, opportunity: OpportunityView) {
        let limit = self.config.max_recent_opportunities.max(1);
        self.try_update_model(|model| {
            model.opportunities.push(opportunity);
            trim_front(&mut model.opportunities, limit);
        });
    }

    pub fn record_trade(&self, trade: TradeView) {
        let limit = self.config.max_recent_trades.max(1);
        self.try_update_model(|model| {
            model.trades.push(trade);
            trim_front(&mut model.trades, limit);
        });
    }

    pub fn record_risk_event(&self, event: RiskEventView) {
        let limit = self.config.max_recent_risk_events.max(1);
        self.try_update_model(|model| {
            model.risk_events.push(event);
            trim_front(&mut model.risk_events, limit);
        });
    }
}

fn trim_front<T>(values: &mut Vec<T>, limit: usize) {
    let extra = values.len().saturating_sub(limit);
    if extra > 0 {
        values.drain(0..extra);
    }
}

pub fn status_from_model(model: &DashboardReadModel) -> StatusView {
    StatusView {
        service_name: model.service_name.clone(),
        version: model.version.clone(),
        started_at: model.started_at,
        uptime_seconds: Utc::now()
            .signed_duration_since(model.started_at)
            .num_seconds()
            .max(0),
        trading_mode: model.trading_mode.clone(),
        live_trading_enabled: model.live_trading_enabled,
        dry_run: model.dry_run,
        strategy_name: model.strategy.strategy_name.clone(),
        strategy_status: model.strategy.status.clone(),
        last_loop_at: model.strategy.last_loop_at,
        error_count: model.strategy.error_count,
        warning_count: model.strategy.warning_count,
        live_preflight_enabled: model.live_preflight_enabled,
        live_readiness_decision: model.live_preflight.as_ref().map(|report| report.decision),
        live_preflight_last_run_at: model.live_preflight.as_ref().map(|report| report.timestamp),
        live_preflight_fail_count: model
            .live_preflight
            .as_ref()
            .map(|report| report.fail_count)
            .unwrap_or(0),
        live_preflight_warn_count: model
            .live_preflight
            .as_ref()
            .map(|report| report.warn_count)
            .unwrap_or(0),
        kill_switch_active: model
            .kill_switch
            .as_ref()
            .map(|state| state.active)
            .unwrap_or(true),
        small_live_gate_status: model
            .small_live_gate
            .as_ref()
            .map(|report| format!("{:?}", report.status).to_ascii_lowercase()),
        live_dry_run_last_plan_at: model.live_dry_run_orders.last().map(|plan| plan.timestamp),
        balance_reconciliation_status: model
            .balance_reconciliation
            .as_ref()
            .map(|report| report.max_severity),
        order_reconciliation_ready: model.order_reconciliation_config.enabled,
    }
}
