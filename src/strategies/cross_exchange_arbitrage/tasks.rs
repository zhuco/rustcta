use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    scan_opportunities, ArbSignal, BundleReadModel, CrossArbDashboardStatus, CrossArbStorageEvent,
    CrossExchangeArbitrageConfig, InMemoryStorageSink, MarketSnapshot, Opportunity,
    OpportunityReadModel, PortfolioExposureSummary, PositionManager, RiskEventReadModel,
    RouteReadModel, SimulatedBundleState, SimulatedBundleStatus, StorageSink,
};
use crate::execution::{ArbitrageBundle, ExchangePosition, FillEvent};
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
        self.opportunities = scan_opportunities(canonical_symbol, snapshots, &self.config, now);

        let signals = self
            .opportunities
            .iter()
            .map(|opportunity| {
                if self.kill_switch || self.close_only || self.paused_new_entries {
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
    }

    pub fn resume_new_entries(&mut self) {
        self.paused_new_entries = false;
        self.close_only = false;
    }

    pub fn set_close_only(&mut self) {
        self.close_only = true;
    }

    pub fn kill_switch(&mut self) {
        self.kill_switch = true;
        self.paused_new_entries = true;
        self.close_only = true;
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
}
