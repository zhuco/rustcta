use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    scan_opportunities, ArbSignal, BundleReadModel, CrossArbDashboardStatus, CrossArbStorageEvent,
    CrossExchangeArbitrageConfig, InMemoryStorageSink, MarketSnapshot, Opportunity,
    OpportunityReadModel, RiskEventReadModel, RouteReadModel, SimulatedBundleState,
    SimulatedBundleStatus, StorageSink,
};
use crate::market::{CanonicalSymbol, ExchangeId, RouteStatus, RuntimeMode};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossArbRuntimeState {
    pub config: CrossExchangeArbitrageConfig,
    pub opportunities: Vec<Opportunity>,
    pub signals: Vec<ArbSignal>,
    pub open_bundles: HashMap<String, SimulatedBundleState>,
    pub history: Vec<SimulatedBundleState>,
    pub risk_events: Vec<RiskEventReadModel>,
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
            route_health: self.route_read_models(),
        }
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
}
