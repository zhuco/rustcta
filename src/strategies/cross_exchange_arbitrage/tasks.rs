use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    scan_opportunities, ArbSignal, BundleReadModel, CrossArbDashboardStatus, CrossArbStorageEvent,
    CrossExchangeArbitrageConfig, InMemoryStorageSink, MarketSnapshot, Opportunity,
    OpportunityReadModel, PortfolioExposureSummary, PositionManager, PrivateStreamHealthReadModel,
    RiskEventReadModel, RiskStateReadModel, RouteReadModel, SimulatedBundleState,
    SimulatedBundleStatus, StorageSink, StrategyRiskState,
};
use crate::execution::{
    AccountSyncState, ArbitrageBundle, ExchangePosition, FillEvent, PositionSnapshot, PrivateEvent,
    PrivateEventKind,
};
use crate::market::{CanonicalSymbol, ExchangeId, RouteStatus, RuntimeMode};
use crate::strategies::cross_exchange_arbitrage::{
    FillApplication, PositionError, PositionReconcileDecision,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossArbRuntimeState {
    pub config: CrossExchangeArbitrageConfig,
    pub opportunities: Vec<Opportunity>,
    pub signals: Vec<ArbSignal>,
    pub open_bundles: HashMap<String, SimulatedBundleState>,
    pub history: Vec<SimulatedBundleState>,
    pub risk_events: Vec<RiskEventReadModel>,
    pub position_manager: PositionManager,
    pub account_sync: HashMap<ExchangeId, AccountSyncState>,
    pub risk_state: StrategyRiskState,
    pub paused_new_entries: bool,
    pub close_only: bool,
    pub kill_switch: bool,
    pub updated_at: DateTime<Utc>,
}

impl CrossArbRuntimeState {
    pub fn new(config: CrossExchangeArbitrageConfig, now: DateTime<Utc>) -> Self {
        Self {
            config,
            opportunities: Vec::new(),
            signals: Vec::new(),
            open_bundles: HashMap::new(),
            history: Vec::new(),
            risk_events: Vec::new(),
            position_manager: PositionManager::default(),
            account_sync: HashMap::new(),
            risk_state: StrategyRiskState::new(now),
            paused_new_entries: false,
            close_only: false,
            kill_switch: false,
            updated_at: now,
        }
    }

    pub fn update_from_market_snapshots(
        &mut self,
        canonical_symbol: &CanonicalSymbol,
        snapshots: &[MarketSnapshot],
        now: DateTime<Utc>,
    ) -> Vec<ArbSignal> {
        self.updated_at = now;
        self.sync_risk_state(now);
        self.opportunities = scan_opportunities(canonical_symbol, snapshots, &self.config, now);

        let signals = self
            .opportunities
            .iter()
            .map(|opportunity| {
                if !self.risk_state.decision().allow_new_entries {
                    ArbSignal::noop(self.config.mode, now)
                } else {
                    ArbSignal::from_opportunity(opportunity, self.config.mode, now)
                }
            })
            .collect::<Vec<_>>();
        self.signals = signals.clone();
        signals
    }

    pub fn dashboard_status(&self) -> CrossArbDashboardStatus {
        CrossArbDashboardStatus {
            mode: self.config.mode,
            updated_at: self.updated_at,
            enabled_symbols: self.config.universe.symbols.len(),
            enabled_exchanges: self.config.universe.enabled_exchanges.len(),
            open_bundles: self.open_bundles.len(),
            position_summary: self
                .position_manager
                .portfolio_exposure_summary(self.config.reconciliation.quantity_tolerance),
            route_health: self.route_read_models(),
            risk_state: RiskStateReadModel::from(&self.risk_state.decision()),
            private_stream_health: self.private_stream_health_read_models(),
        }
    }

    pub fn register_bundle_position(
        &mut self,
        bundle: &ArbitrageBundle,
        target_qty: f64,
        entry_edge_pct: f64,
        now: DateTime<Utc>,
    ) {
        self.position_manager
            .upsert_from_bundle(bundle, target_qty, entry_edge_pct, now);
    }

    pub fn apply_fill_event(
        &mut self,
        bundle_id: &str,
        fill: &FillEvent,
    ) -> Result<FillApplication, PositionError> {
        self.position_manager.apply_fill_event(bundle_id, fill)
    }

    pub fn ingest_private_event(&mut self, event: PrivateEvent) {
        let exchange = event.exchange.clone();
        let received_at = event.received_at;
        let needs_resync = {
            let state = self.account_sync.entry(exchange.clone()).or_default();
            state.apply_private_event(event.clone());
            state.needs_resync(&exchange, received_at)
        };

        self.update_private_stream_health(exchange.clone(), received_at, needs_resync);
        self.record_private_reconciliation_from_account_state(&exchange, received_at);

        if matches!(
            &event.kind,
            PrivateEventKind::StreamDisconnected { .. } | PrivateEventKind::Error(_)
        ) {
            self.risk_events.push(RiskEventReadModel {
                event_id: format!(
                    "private-{}-{}",
                    exchange.as_str(),
                    received_at.timestamp_millis()
                ),
                canonical_symbol: None,
                exchange: Some(exchange.clone()),
                reason: super::risk::RejectReason::RouteUnhealthy,
                message: format!("private stream event: {:?}", &event.kind),
                created_at: received_at,
            });
        }

        self.updated_at = received_at;
    }

    pub fn reconcile_positions(
        &self,
        exchange_positions: &[ExchangePosition],
        quantity_tolerance: f64,
        orphan_tolerance: f64,
        checked_at: DateTime<Utc>,
    ) -> Vec<PositionReconcileDecision> {
        self.position_manager.reconcile_exchange_positions(
            exchange_positions,
            quantity_tolerance,
            orphan_tolerance,
            checked_at,
        )
    }

    pub fn position_summary(&self, quantity_tolerance: f64) -> PortfolioExposureSummary {
        self.position_manager
            .portfolio_exposure_summary(quantity_tolerance)
    }

    pub fn private_positions(&self, exchange: &ExchangeId) -> Vec<PositionSnapshot> {
        self.account_sync
            .get(exchange)
            .map(AccountSyncState::position_snapshots)
            .unwrap_or_default()
    }

    pub fn opportunity_read_models(&self) -> Vec<OpportunityReadModel> {
        self.opportunities
            .iter()
            .map(OpportunityReadModel::from)
            .collect()
    }

    pub fn open_bundle_read_models(&self) -> Vec<BundleReadModel> {
        self.open_bundles
            .values()
            .filter(|bundle| bundle.status != SimulatedBundleStatus::Closed)
            .map(BundleReadModel::from)
            .collect()
    }

    pub fn history_read_models(&self) -> Vec<BundleReadModel> {
        self.history.iter().map(BundleReadModel::from).collect()
    }

    pub fn pause_new_entries(&mut self) {
        self.paused_new_entries = true;
        self.risk_state.pause_new_entries(self.updated_at);
        self.sync_local_controls();
    }

    pub fn resume_new_entries(&mut self) {
        self.paused_new_entries = false;
        self.close_only = false;
        self.risk_state.paused_new_entries = false;
        self.risk_state.close_only = false;
        self.risk_state.recompute_mode(self.updated_at);
        self.sync_local_controls();
    }

    pub fn set_close_only(&mut self) {
        self.close_only = true;
        self.risk_state.set_close_only(self.updated_at);
        self.sync_local_controls();
    }

    pub fn kill_switch(&mut self) {
        self.kill_switch = true;
        self.paused_new_entries = true;
        self.close_only = true;
        self.risk_state.kill_switch(self.updated_at);
        self.sync_local_controls();
        self.mark_open_bundles_risk_stopped(self.updated_at);
    }

    pub fn update_private_stream_health(
        &mut self,
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
        needs_resync: bool,
    ) {
        self.risk_state
            .observe_private_event(exchange, received_at, needs_resync);
        self.updated_at = received_at;
        self.sync_local_controls();
    }

    pub fn record_reconciliation_result(
        &mut self,
        exchange: ExchangeId,
        symbol: String,
        severity: crate::execution::ReconcileSeverity,
        checked_at: DateTime<Utc>,
    ) {
        self.risk_state
            .record_reconciliation_severity(exchange, symbol, severity, checked_at);
        self.updated_at = checked_at;
        self.sync_local_controls();
    }

    pub fn record_orphan_exposure(&mut self, now: DateTime<Utc>) {
        let summary = self
            .position_manager
            .portfolio_exposure_summary(self.config.reconciliation.quantity_tolerance);
        self.risk_state.record_orphan_exposure(
            &summary,
            self.config.reconciliation.orphan_tolerance,
            now,
        );
        self.updated_at = now;
        self.sync_local_controls();
        if self.risk_state.decision().mode == super::risk::RiskOperatingMode::Halted {
            self.mark_open_bundles_risk_stopped(now);
        }
    }

    pub fn refresh_risk_state(&mut self, now: DateTime<Utc>) {
        self.risk_state.evaluate_private_health(now);
        self.updated_at = now;
        self.sync_local_controls();
        if self.risk_state.decision().mode == super::risk::RiskOperatingMode::Halted {
            self.mark_open_bundles_risk_stopped(now);
        }
    }

    fn sync_risk_state(&mut self, now: DateTime<Utc>) {
        self.record_orphan_exposure(now);
        self.refresh_risk_state(now);
    }

    fn record_private_reconciliation_from_account_state(
        &mut self,
        exchange: &ExchangeId,
        checked_at: DateTime<Utc>,
    ) {
        let Some(state) = self.account_sync.get(exchange) else {
            return;
        };

        let exchange_positions = state
            .position_snapshots()
            .into_iter()
            .map(|position| ExchangePosition {
                exchange: position.exchange,
                canonical_symbol: position.canonical_symbol.clone(),
                exchange_symbol: position.exchange_symbol.unwrap_or_else(|| {
                    crate::market::exchange_symbol_for(exchange, &position.canonical_symbol)
                }),
                position_side: position.position_side,
                quantity: position.quantity,
                entry_price: position.entry_price,
                mark_price: None,
                unrealized_pnl: position.unrealized_pnl,
                updated_at: position.updated_at,
            })
            .collect::<Vec<_>>();
        let decisions = self.reconcile_positions(
            &exchange_positions,
            self.config.reconciliation.quantity_tolerance,
            self.config.reconciliation.orphan_tolerance,
            checked_at,
        );

        for decision in decisions
            .into_iter()
            .filter(|decision| decision.severity != crate::execution::ReconcileSeverity::Ok)
        {
            self.record_reconciliation_result(
                decision.exchange,
                decision.canonical_symbol.to_string(),
                decision.severity,
                checked_at,
            );
        }
    }

    fn sync_local_controls(&mut self) {
        let decision = self.risk_state.decision();
        self.paused_new_entries = !decision.allow_new_entries;
        self.close_only = matches!(
            decision.mode,
            super::risk::RiskOperatingMode::CloseOnly | super::risk::RiskOperatingMode::Halted
        );
        self.kill_switch = matches!(decision.mode, super::risk::RiskOperatingMode::Halted);
    }

    fn mark_open_bundles_risk_stopped(&mut self, now: DateTime<Utc>) {
        for bundle in self.open_bundles.values_mut() {
            if bundle.status != SimulatedBundleStatus::Closed {
                bundle.status = SimulatedBundleStatus::RiskStopped;
                bundle.updated_at = now;
            }
        }
    }

    fn route_read_models(&self) -> Vec<RouteReadModel> {
        self.opportunities
            .iter()
            .map(|opportunity| RouteReadModel {
                exchange: opportunity.maker_exchange.clone(),
                canonical_symbol: opportunity.canonical_symbol.clone(),
                status: opportunity.route_status,
                last_book_age_ms: opportunity.book_age_ms,
                reject_reasons: opportunity.reject_reasons.clone(),
            })
            .collect()
    }

    fn private_stream_health_read_models(&self) -> Vec<PrivateStreamHealthReadModel> {
        self.risk_state
            .private_stream_health
            .values()
            .map(PrivateStreamHealthReadModel::from)
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct CrossArbRuntime {
    pub state: CrossArbRuntimeState,
    pub storage: InMemoryStorageSink,
}

impl CrossArbRuntime {
    pub fn new(config: CrossExchangeArbitrageConfig, now: DateTime<Utc>) -> Self {
        Self {
            state: CrossArbRuntimeState::new(config, now),
            storage: InMemoryStorageSink::default(),
        }
    }

    pub fn on_market_snapshots(
        &mut self,
        canonical_symbol: &CanonicalSymbol,
        snapshots: &[MarketSnapshot],
        now: DateTime<Utc>,
    ) -> Vec<ArbSignal> {
        let signals = self
            .state
            .update_from_market_snapshots(canonical_symbol, snapshots, now);
        for opportunity in &self.state.opportunities {
            self.storage
                .record(CrossArbStorageEvent::Opportunity(opportunity.clone()), now);
        }
        for signal in &signals {
            self.storage
                .record(CrossArbStorageEvent::Signal(signal.clone()), now);
        }
        signals
    }

    pub fn on_private_event(&mut self, event: PrivateEvent) {
        let recorded_at = event.received_at;
        self.state.ingest_private_event(event.clone());
        self.storage
            .record(CrossArbStorageEvent::PrivateEvent(event), recorded_at);
    }
}

impl Default for CrossArbRuntimeState {
    fn default() -> Self {
        Self::new(CrossExchangeArbitrageConfig::default(), Utc::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{
        ArbitrageBundle, BundleStatus, FillEvent, FillLiquidity, OrderSide, PositionSide,
        PrivateEvent, PrivateEventKind,
    };
    use crate::market::{BookLevel, ExchangeSymbol, OrderBook5};

    fn book(exchange: ExchangeId, bid: f64, ask: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange, "BTCUSDT"),
            vec![BookLevel::new(bid, 10.0)],
            vec![BookLevel::new(ask, 10.0)],
            Utc::now(),
            Utc::now(),
            Some(1),
            None,
        )
    }

    #[test]
    fn runtime_should_scan_market_and_store_signals() {
        let now = Utc::now();
        let mut runtime = CrossArbRuntime::new(CrossExchangeArbitrageConfig::default(), now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let snapshots = vec![
            MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0)),
            MarketSnapshot::healthy(book(ExchangeId::Okx, 104.0, 105.0)),
        ];

        let signals = runtime.on_market_snapshots(&symbol, &snapshots, now);

        assert!(!signals.is_empty());
        assert!(!runtime.state.opportunities.is_empty());
        assert!(!runtime.storage.events().is_empty());
    }

    #[test]
    fn runtime_controls_should_block_new_open_signals() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        state.pause_new_entries();
        assert!(state.paused_new_entries);
        state.set_close_only();
        assert!(state.close_only);
        state.kill_switch();
        assert!(state.kill_switch);
    }

    #[test]
    fn runtime_should_track_position_manager_fills_and_summary() {
        let now = Utc::now();
        let mut state = CrossArbRuntimeState::new(CrossExchangeArbitrageConfig::default(), now);
        let bundle = ArbitrageBundle::new(
            "bundle-1",
            RuntimeMode::Simulation,
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Binance,
            ExchangeId::Okx,
            100.0,
            now,
        );

        state.register_bundle_position(&bundle, 2.0, 0.015, now);

        let fill = FillEvent {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            trade_id: "trade-1".to_string(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            liquidity: FillLiquidity::Taker,
            price: 100.0,
            quantity: 2.0,
            quote_quantity: 200.0,
            fee: Some(0.1),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0005),
            realized_pnl: None,
            reduce_only: Some(false),
            filled_at: now,
            received_at: now,
        };

        let short_fill = FillEvent {
            exchange: ExchangeId::Okx,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            trade_id: "trade-2".to_string(),
            client_order_id: Some("client-2".to_string()),
            exchange_order_id: Some("order-2".to_string()),
            side: OrderSide::Sell,
            position_side: PositionSide::Short,
            liquidity: FillLiquidity::Taker,
            price: 101.0,
            quantity: 2.0,
            quote_quantity: 202.0,
            fee: Some(0.1),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0005),
            realized_pnl: None,
            reduce_only: Some(false),
            filled_at: now,
            received_at: now,
        };

        let application = state.apply_fill_event("bundle-1", &fill).unwrap();
        assert_eq!(
            application.status_after,
            crate::strategies::cross_exchange_arbitrage::LegStatus::Open
        );
        state.apply_fill_event("bundle-1", &short_fill).unwrap();

        let summary = state.position_summary(0.001);
        assert_eq!(summary.open_bundles, 1);
        assert_eq!(summary.orphan_bundle_count, 0);
        assert_eq!(
            state
                .position_manager
                .bundle("bundle-1")
                .unwrap()
                .long_leg
                .filled_qty,
            2.0
        );
    }

    #[test]
    fn runtime_should_record_private_events_and_update_health() {
        let now = Utc::now();
        let mut runtime = CrossArbRuntime::new(CrossExchangeArbitrageConfig::default(), now);

        runtime.on_private_event(PrivateEvent::new(
            ExchangeId::Binance,
            PrivateEventKind::Heartbeat,
            now,
        ));

        assert_eq!(runtime.storage.events().len(), 1);
        assert!(matches!(
            runtime.storage.events()[0].event,
            CrossArbStorageEvent::PrivateEvent(_)
        ));
        assert!(runtime
            .state
            .risk_state
            .private_stream_health
            .contains_key(&ExchangeId::Binance));
    }
}
