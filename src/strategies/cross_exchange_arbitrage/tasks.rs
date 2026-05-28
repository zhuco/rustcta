use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    scan_opportunities, ArbSignal, BundleReadModel, CrossArbDashboardStatus, CrossArbStorageEvent,
    CrossExchangeArbitrageConfig, InMemoryStorageSink, MarketSnapshot, Opportunity,
    OpportunityReadModel, OrderSide as StrategyOrderSide, PortfolioExposureSummary,
    PositionManager, PrivateStreamHealthReadModel, RiskEventReadModel, RiskStateReadModel,
    RouteReadModel, SimulatedBundleState, SimulatedBundleStatus, StorageSink, StrategyRiskState,
};
use crate::execution::BundleStatus;
use crate::execution::{
    AccountSyncState, ArbitrageBundle, ExchangePosition, FillEvent, PositionSnapshot, PrivateEvent,
    PrivateEventKind,
};
use crate::market::{CanonicalSymbol, ExchangeId, RouteStatus, RuntimeMode};
use crate::strategies::cross_exchange_arbitrage::{
    one_way_conflict_for_open, FillApplication, PositionError, PositionReconcileDecision,
    RejectReason,
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
        let start_paused_new_entries = config.controls.start_paused_new_entries;
        let start_close_only = config.controls.start_close_only;
        let mut state = Self {
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
        };
        if start_close_only {
            state.set_close_only();
        } else if start_paused_new_entries {
            state.pause_new_entries();
        }
        state
    }

    pub fn update_from_market_snapshots(
        &mut self,
        canonical_symbol: &CanonicalSymbol,
        snapshots: &[MarketSnapshot],
        now: DateTime<Utc>,
    ) -> Vec<ArbSignal> {
        self.updated_at = now;
        self.sync_risk_state(now);
        self.opportunities
            .retain(|opportunity| opportunity.canonical_symbol != *canonical_symbol);
        let mut scanned = scan_opportunities(canonical_symbol, snapshots, &self.config, now);
        let existing_one_way = self.position_manager.active_one_way_exposure_keys();
        for opportunity in &mut scanned {
            let maker_side = match opportunity.maker_side {
                StrategyOrderSide::Buy => crate::execution::PositionSide::Long,
                StrategyOrderSide::Sell => crate::execution::PositionSide::Short,
            };
            let taker_side = match opportunity.taker_side {
                StrategyOrderSide::Buy => crate::execution::PositionSide::Long,
                StrategyOrderSide::Sell => crate::execution::PositionSide::Short,
            };
            if one_way_conflict_for_open(
                &opportunity.maker_exchange,
                &opportunity.canonical_symbol,
                maker_side,
                existing_one_way.clone(),
            ) || one_way_conflict_for_open(
                &opportunity.taker_exchange,
                &opportunity.canonical_symbol,
                taker_side,
                existing_one_way.clone(),
            ) {
                opportunity
                    .reject_reasons
                    .push(RejectReason::ExchangePositionLimitExceeded);
                opportunity.can_open = false;
            }
        }
        self.opportunities.extend(scanned);
        self.opportunities.sort_by(|left, right| {
            right
                .maker_taker_net_edge
                .partial_cmp(&left.maker_taker_net_edge)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let signals = self
            .opportunities
            .iter()
            .filter(|opportunity| opportunity.canonical_symbol == *canonical_symbol)
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
        let application = self.position_manager.apply_fill_event(bundle_id, fill)?;
        self.refresh_bundle_status_from_position(bundle_id, fill.filled_at);
        Ok(application)
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
        if self.config.risk.block_on_external_account_exposure {
            self.record_private_reconciliation_from_account_state(&exchange, received_at);
        }
        self.reconcile_open_bundles_from_account_positions(received_at);

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

    fn reconcile_open_bundles_from_account_positions(&mut self, checked_at: DateTime<Utc>) {
        let exchange_positions = self
            .account_sync
            .values()
            .flat_map(AccountSyncState::position_snapshots)
            .filter(|position| {
                position.quantity.abs() > self.config.reconciliation.quantity_tolerance
            })
            .map(|position| ExchangePosition {
                exchange: position.exchange.clone(),
                canonical_symbol: position.canonical_symbol.clone(),
                exchange_symbol: position.exchange_symbol.unwrap_or_else(|| {
                    crate::market::exchange_symbol_for(
                        &position.exchange,
                        &position.canonical_symbol,
                    )
                }),
                position_side: position.position_side,
                quantity: position.quantity,
                entry_price: position.entry_price,
                mark_price: None,
                unrealized_pnl: position.unrealized_pnl,
                updated_at: position.updated_at,
            })
            .collect::<Vec<_>>();
        if exchange_positions.is_empty() {
            return;
        }
        self.restore_open_bundles_from_account_positions(&exchange_positions, checked_at);

        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let bundle_ids = self
            .open_bundles
            .iter()
            .filter(|(_, bundle)| {
                matches!(
                    bundle.status,
                    SimulatedBundleStatus::Hedging
                        | SimulatedBundleStatus::OrphanLeg
                        | SimulatedBundleStatus::MakerFilled
                        | SimulatedBundleStatus::OpenSimulated
                )
            })
            .map(|(bundle_id, _)| bundle_id.clone())
            .collect::<Vec<_>>();

        for bundle_id in bundle_ids {
            let Ok(reconciled) = self
                .position_manager
                .reconcile_bundle_open_from_exchange_positions(
                    &bundle_id,
                    &exchange_positions,
                    quantity_tolerance,
                    checked_at,
                )
            else {
                continue;
            };
            if reconciled {
                self.refresh_bundle_status_from_position(&bundle_id, checked_at);
                self.history.retain(|item| item.bundle_id != bundle_id);
            }
        }
    }

    fn restore_open_bundles_from_account_positions(
        &mut self,
        exchange_positions: &[ExchangePosition],
        checked_at: DateTime<Utc>,
    ) {
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        for long_position in exchange_positions.iter().filter(|position| {
            position.position_side == crate::execution::PositionSide::Long
                && position.quantity.abs() > quantity_tolerance
                && position.entry_price.unwrap_or_default() > 0.0
        }) {
            for short_position in exchange_positions.iter().filter(|position| {
                position.position_side == crate::execution::PositionSide::Short
                    && position.quantity.abs() > quantity_tolerance
                    && position.entry_price.unwrap_or_default() > 0.0
                    && position.canonical_symbol == long_position.canonical_symbol
                    && position.exchange != long_position.exchange
            }) {
                if (long_position.quantity.abs() - short_position.quantity.abs()).abs()
                    > quantity_tolerance
                {
                    continue;
                }
                if !self
                    .config
                    .universe
                    .symbols
                    .contains(&long_position.canonical_symbol)
                {
                    continue;
                }

                let bundle_id = format!(
                    "restored-{}-{}-{}",
                    long_position
                        .canonical_symbol
                        .to_string()
                        .replace('/', "")
                        .to_ascii_lowercase(),
                    long_position.exchange.as_str(),
                    short_position.exchange.as_str()
                );
                if self.open_bundles.contains_key(&bundle_id)
                    || self.position_manager.contains_bundle(&bundle_id)
                {
                    continue;
                }

                let target_notional = long_position.quantity.abs()
                    * long_position.entry_price.unwrap_or_default().max(0.0);
                let mut bundle = ArbitrageBundle::new(
                    bundle_id.clone(),
                    self.config.mode,
                    long_position.canonical_symbol.clone(),
                    long_position.exchange.clone(),
                    short_position.exchange.clone(),
                    long_position.exchange.clone(),
                    short_position.exchange.clone(),
                    target_notional,
                    checked_at,
                );
                bundle.status = BundleStatus::OpenSimulated;
                bundle.open_time = Some(checked_at);
                self.register_bundle_position(
                    &bundle,
                    long_position.quantity.abs(),
                    0.0,
                    checked_at,
                );
                let _ = self
                    .position_manager
                    .reconcile_bundle_open_from_exchange_positions(
                        &bundle_id,
                        exchange_positions,
                        quantity_tolerance,
                        checked_at,
                    );
                self.open_bundles.insert(
                    bundle_id.clone(),
                    SimulatedBundleState {
                        bundle_id: bundle_id.clone(),
                        opportunity_id: format!("restored-{}", bundle_id),
                        status: SimulatedBundleStatus::OpenSimulated,
                        route: super::StrategyRoute {
                            long_exchange: long_position.exchange.clone(),
                            short_exchange: short_position.exchange.clone(),
                            maker_exchange: long_position.exchange.clone(),
                            taker_exchange: short_position.exchange.clone(),
                            maker_side: StrategyOrderSide::Buy,
                            taker_side: StrategyOrderSide::Sell,
                            maker_leg_kind: super::MakerLegKind::LongMakerBuy,
                        },
                        target_notional_usdt: target_notional,
                        opened_at: Some(checked_at),
                        updated_at: checked_at,
                    },
                );
                self.set_close_only();
            }
        }
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

    fn refresh_bundle_status_from_position(&mut self, bundle_id: &str, now: DateTime<Utc>) {
        let Some(position) = self.position_manager.bundle(bundle_id) else {
            return;
        };
        let quantity_tolerance = self.config.reconciliation.quantity_tolerance;
        let Some(bundle) = self.open_bundles.get_mut(bundle_id) else {
            return;
        };
        if position.is_fully_closed(quantity_tolerance) || position.status == BundleStatus::Closed {
            bundle.status = SimulatedBundleStatus::Closed;
            bundle.updated_at = now;
            self.history.push(bundle.clone());
        } else if position.is_fully_open(quantity_tolerance) {
            bundle.status = SimulatedBundleStatus::OpenSimulated;
            bundle.opened_at.get_or_insert(now);
            bundle.updated_at = now;
        } else if position.unhedged_qty() > quantity_tolerance {
            bundle.status = SimulatedBundleStatus::OrphanLeg;
            bundle.updated_at = now;
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
        for opportunity in self
            .state
            .opportunities
            .iter()
            .filter(|opportunity| opportunity.canonical_symbol == *canonical_symbol)
        {
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
        ArbitrageBundle, BundleStatus, FillEvent, FillLiquidity, OrderSide as ExecutionOrderSide,
        PositionSide, PrivateEvent, PrivateEventKind,
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
            side: ExecutionOrderSide::Buy,
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
            side: ExecutionOrderSide::Sell,
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

    #[test]
    fn runtime_should_block_gate_one_way_opposite_direction_conflict() {
        let now = Utc::now();
        let mut config = CrossExchangeArbitrageConfig::default();
        config.thresholds.min_open_maker_taker_net_edge = -1.0;
        config.thresholds.min_display_raw_spread = -1.0;
        config.risk.max_book_age_ms = 10_000;
        let mut state = CrossArbRuntimeState::new(config, now);
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let existing = ArbitrageBundle::new(
            "gate-long-existing",
            RuntimeMode::LiveSmall,
            symbol.clone(),
            ExchangeId::Gate,
            ExchangeId::Binance,
            ExchangeId::Gate,
            ExchangeId::Binance,
            100.0,
            now,
        );
        state.register_bundle_position(&existing, 1.0, 0.01, now);
        state
            .position_manager
            .bundle_mut("gate-long-existing")
            .unwrap()
            .long_leg
            .filled_qty = 1.0;

        state.update_from_market_snapshots(
            &symbol,
            &[
                MarketSnapshot::healthy(book(ExchangeId::Gate, 104.0, 105.0)),
                MarketSnapshot::healthy(book(ExchangeId::Binance, 100.0, 101.0)),
            ],
            now,
        );

        let blocked = state
            .opportunities
            .iter()
            .find(|opportunity| {
                opportunity.maker_exchange == ExchangeId::Gate
                    && matches!(opportunity.maker_side, StrategyOrderSide::Sell)
            })
            .expect("gate short-maker opportunity");
        assert!(!blocked.can_open);
        assert!(blocked
            .reject_reasons
            .contains(&RejectReason::ExchangePositionLimitExceeded));
    }
}
