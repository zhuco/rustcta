//! Risk gates and strategy-level risk state for cross-exchange arbitrage.

use super::config::CrossExchangeArbitrageConfig;
use super::funding::FundingEstimate;
use super::position::PortfolioExposureSummary;
use super::simulation::TakerVwapResult;
use crate::execution::ReconcileSeverity;
use crate::market::{ExchangeId, OrderBook5, RouteStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RejectReason {
    RawSpreadTooSmall,
    NetEdgeTooSmall,
    StaleBook,
    RouteUnhealthy,
    DepthInsufficient,
    FundingDangerous,
    FundingWindowTooClose,
    NotionalOverLimit,
    SlippageTooHigh,
    BadOrderBook,
    PrecisionInvalid,
    AbnormalCrossExchangeSpread,
    ExchangeCapacityExceeded,
    ExchangePositionLimitExceeded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskGateDecision {
    pub can_open: bool,
    pub reject_reasons: Vec<RejectReason>,
}

impl RiskGateDecision {
    pub fn allow() -> Self {
        Self {
            can_open: true,
            reject_reasons: Vec::new(),
        }
    }

    pub fn from_reasons(reject_reasons: Vec<RejectReason>) -> Self {
        Self {
            can_open: reject_reasons.is_empty(),
            reject_reasons,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskOperatingMode {
    Normal,
    Degraded,
    CloseOnly,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskTriggerKind {
    PrivateStreamStale,
    PrivateStreamDisconnected,
    ReconciliationDrift,
    OrphanExposure,
    RouteDegraded,
    ManualPause,
    ManualCloseOnly,
    KillSwitch,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskTriggerRecord {
    pub trigger_id: String,
    pub kind: RiskTriggerKind,
    pub exchange: Option<ExchangeId>,
    pub symbol: Option<String>,
    pub message: String,
    pub triggered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamHealth {
    pub exchange: ExchangeId,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub stale_after_ms: i64,
    pub needs_resync: bool,
    pub consecutive_stale_checks: u32,
    pub resync_requested_at: Option<DateTime<Utc>>,
}

impl PrivateStreamHealth {
    pub fn new(exchange: ExchangeId, stale_after_ms: i64) -> Self {
        Self {
            exchange,
            last_event_at: None,
            last_disconnect_at: None,
            stale_after_ms,
            needs_resync: false,
            consecutive_stale_checks: 0,
            resync_requested_at: None,
        }
    }

    pub fn observe_event(&mut self, received_at: DateTime<Utc>) {
        self.last_event_at = Some(received_at);
        self.needs_resync = false;
        self.consecutive_stale_checks = 0;
        self.resync_requested_at = None;
    }

    pub fn observe_disconnect(&mut self, disconnected_at: DateTime<Utc>) {
        self.last_disconnect_at = Some(disconnected_at);
        self.needs_resync = true;
        self.resync_requested_at.get_or_insert(disconnected_at);
    }

    pub fn evaluate(&mut self, now: DateTime<Utc>) -> bool {
        let stale = self
            .last_event_at
            .map(|last| now.signed_duration_since(last).num_milliseconds() > self.stale_after_ms)
            .unwrap_or(true);
        if stale {
            self.consecutive_stale_checks = self.consecutive_stale_checks.saturating_add(1);
            self.needs_resync = true;
            self.resync_requested_at.get_or_insert(now);
        } else {
            self.consecutive_stale_checks = 0;
        }
        stale
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskState {
    pub mode: RiskOperatingMode,
    pub paused_new_entries: bool,
    pub close_only: bool,
    pub kill_switch: bool,
    pub private_stream_health: HashMap<ExchangeId, PrivateStreamHealth>,
    pub triggers: Vec<RiskTriggerRecord>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskDecision {
    pub mode: RiskOperatingMode,
    pub allow_new_entries: bool,
    pub allow_closes: bool,
    pub needs_reconciliation: bool,
    pub needs_private_resync: bool,
    pub trigger_count: usize,
}

impl StrategyRiskState {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            mode: RiskOperatingMode::Normal,
            paused_new_entries: false,
            close_only: false,
            kill_switch: false,
            private_stream_health: HashMap::new(),
            triggers: Vec::new(),
            updated_at: now,
        }
    }

    pub fn observe_private_event(
        &mut self,
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
        needs_resync: bool,
    ) {
        let health = self
            .private_stream_health
            .entry(exchange.clone())
            .or_insert_with(|| PrivateStreamHealth::new(exchange.clone(), 10_000));
        health.observe_event(received_at);
        if needs_resync {
            health.needs_resync = true;
            health.resync_requested_at.get_or_insert(received_at);
            self.raise_trigger(
                RiskTriggerKind::PrivateStreamStale,
                Some(exchange.clone()),
                None,
                "private stream requested resync".to_string(),
                received_at,
            );
        }
        self.updated_at = received_at;
        self.recompute_mode(received_at);
    }

    pub fn observe_private_disconnect(
        &mut self,
        exchange: ExchangeId,
        disconnected_at: DateTime<Utc>,
    ) {
        let health = self
            .private_stream_health
            .entry(exchange.clone())
            .or_insert_with(|| PrivateStreamHealth::new(exchange.clone(), 10_000));
        health.observe_disconnect(disconnected_at);
        self.raise_trigger(
            RiskTriggerKind::PrivateStreamDisconnected,
            Some(exchange.clone()),
            None,
            "private websocket disconnected".to_string(),
            disconnected_at,
        );
        self.updated_at = disconnected_at;
        self.recompute_mode(disconnected_at);
    }

    pub fn evaluate_private_health(&mut self, now: DateTime<Utc>) -> bool {
        let mut any_stale = false;
        for health in self.private_stream_health.values_mut() {
            any_stale |= health.evaluate(now);
        }
        if any_stale {
            self.recompute_mode(now);
        }
        any_stale
    }

    pub fn record_orphan_exposure(
        &mut self,
        summary: &PortfolioExposureSummary,
        orphan_threshold: f64,
        now: DateTime<Utc>,
    ) {
        if summary.orphan_qty > orphan_threshold {
            self.raise_trigger(
                RiskTriggerKind::OrphanExposure,
                None,
                None,
                format!("orphan exposure {:.8}", summary.orphan_qty),
                now,
            );
            self.close_only = true;
        }
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn record_reconciliation_severity(
        &mut self,
        exchange: ExchangeId,
        symbol: String,
        severity: ReconcileSeverity,
        now: DateTime<Utc>,
    ) {
        match severity {
            ReconcileSeverity::Ok => {}
            ReconcileSeverity::MinorDrift => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange.clone()),
                    Some(symbol.clone()),
                    "minor reconciliation drift".to_string(),
                    now,
                );
                self.paused_new_entries = true;
            }
            ReconcileSeverity::OrderDrift
            | ReconcileSeverity::PositionDrift
            | ReconcileSeverity::OrphanExposure => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange.clone()),
                    Some(symbol.clone()),
                    format!("reconciliation drift: {:?}", severity),
                    now,
                );
                self.close_only = true;
            }
            ReconcileSeverity::UnknownCritical => {
                self.raise_trigger(
                    RiskTriggerKind::ReconciliationDrift,
                    Some(exchange),
                    Some(symbol),
                    format!("reconciliation severity: {:?}", severity),
                    now,
                );
                self.close_only = true;
            }
        }
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn record_route_status(
        &mut self,
        exchange: ExchangeId,
        status: RouteStatus,
        now: DateTime<Utc>,
    ) {
        if !status.allows_new_entries() {
            self.raise_trigger(
                RiskTriggerKind::RouteDegraded,
                Some(exchange.clone()),
                None,
                format!("route status {:?}", status),
                now,
            );
        }
        if !status.allows_closes() {
            self.kill_switch = true;
            self.raise_trigger(
                RiskTriggerKind::KillSwitch,
                Some(exchange),
                None,
                "route cannot safely close".to_string(),
                now,
            );
        }
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn pause_new_entries(&mut self, now: DateTime<Utc>) {
        self.paused_new_entries = true;
        self.raise_trigger(
            RiskTriggerKind::ManualPause,
            None,
            None,
            "manual pause".to_string(),
            now,
        );
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn set_close_only(&mut self, now: DateTime<Utc>) {
        self.close_only = true;
        self.raise_trigger(
            RiskTriggerKind::ManualCloseOnly,
            None,
            None,
            "manual close-only".to_string(),
            now,
        );
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn kill_switch(&mut self, now: DateTime<Utc>) {
        self.kill_switch = true;
        self.paused_new_entries = true;
        self.close_only = true;
        self.raise_trigger(
            RiskTriggerKind::KillSwitch,
            None,
            None,
            "kill switch".to_string(),
            now,
        );
        self.updated_at = now;
        self.recompute_mode(now);
    }

    pub fn decision(&self) -> RiskDecision {
        let needs_private_resync = self
            .private_stream_health
            .values()
            .any(|health| health.needs_resync);
        RiskDecision {
            mode: self.mode.clone(),
            allow_new_entries: !self.paused_new_entries
                && !self.close_only
                && !self.kill_switch
                && !needs_private_resync,
            allow_closes: !self.kill_switch,
            needs_reconciliation: self.close_only || self.paused_new_entries,
            needs_private_resync,
            trigger_count: self.triggers.len(),
        }
    }

    pub fn recompute_mode(&mut self, now: DateTime<Utc>) {
        let any_resync = self
            .private_stream_health
            .values()
            .any(|health| health.needs_resync);
        self.mode = if self.kill_switch {
            RiskOperatingMode::Halted
        } else if self.close_only || any_resync {
            RiskOperatingMode::CloseOnly
        } else if self.paused_new_entries {
            RiskOperatingMode::Degraded
        } else {
            RiskOperatingMode::Normal
        };
        self.updated_at = now;
    }

    fn raise_trigger(
        &mut self,
        kind: RiskTriggerKind,
        exchange: Option<ExchangeId>,
        symbol: Option<String>,
        message: String,
        triggered_at: DateTime<Utc>,
    ) {
        let trigger_id = format!(
            "risk-{}-{}",
            triggered_at.timestamp_millis(),
            self.triggers.len()
        );
        self.triggers.push(RiskTriggerRecord {
            trigger_id,
            kind,
            exchange,
            symbol,
            message,
            triggered_at,
        });
        if self.triggers.len() > 1000 {
            let drain = self.triggers.len() - 1000;
            self.triggers.drain(0..drain);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskGate;

impl RiskGate {
    pub fn evaluate_open(
        maker_book: &OrderBook5,
        taker_book: &OrderBook5,
        maker_route_status: RouteStatus,
        taker_route_status: RouteStatus,
        taker_vwap: &TakerVwapResult,
        funding: &FundingEstimate,
        target_notional_usdt: f64,
        config: &CrossExchangeArbitrageConfig,
        now: DateTime<Utc>,
    ) -> RiskGateDecision {
        let mut reasons = Vec::new();

        if !maker_book.is_usable() || !taker_book.is_usable() {
            reasons.push(RejectReason::BadOrderBook);
        }
        if maker_book.quality.stale
            || taker_book.quality.stale
            || book_age_ms(maker_book, now) > config.risk.max_book_age_ms
            || book_age_ms(taker_book, now) > config.risk.max_book_age_ms
        {
            reasons.push(RejectReason::StaleBook);
        }
        if !maker_route_status.allows_new_entries() || !taker_route_status.allows_new_entries() {
            reasons.push(RejectReason::RouteUnhealthy);
        }
        if !taker_vwap.depth_enough {
            reasons.push(RejectReason::DepthInsufficient);
        }
        if funding.dangerous {
            reasons.push(RejectReason::FundingDangerous);
        }
        if funding.near_negative_settlement {
            reasons.push(RejectReason::FundingWindowTooClose);
        }
        if target_notional_usdt > config.risk.max_notional_per_symbol_usdt {
            reasons.push(RejectReason::NotionalOverLimit);
        }
        if taker_vwap.slippage_pct > config.risk.max_taker_slippage_pct {
            reasons.push(RejectReason::SlippageTooHigh);
        }

        RiskGateDecision::from_reasons(reasons)
    }
}

pub fn book_age_ms(book: &OrderBook5, now: DateTime<Utc>) -> i64 {
    now.signed_duration_since(book.recv_ts).num_milliseconds()
}

impl Default for StrategyRiskState {
    fn default() -> Self {
        Self::new(Utc::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{BookLevel, CanonicalSymbol, ExchangeSymbol};
    use crate::strategies::cross_exchange_arbitrage::simulation::calculate_taker_vwap;
    use crate::strategies::cross_exchange_arbitrage::state::OrderSide;

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
    fn strategy_risk_state_should_enter_close_only_on_resync() {
        let now = Utc::now();
        let mut state = StrategyRiskState::new(now);
        state.observe_private_event(ExchangeId::Binance, now, true);

        let decision = state.decision();

        assert_eq!(decision.mode, RiskOperatingMode::CloseOnly);
        assert!(decision.needs_private_resync);
        assert!(!decision.allow_new_entries);
        assert!(decision.allow_closes);
    }

    #[test]
    fn risk_gate_should_reject_stale_books() {
        let now = Utc::now();
        let config = CrossExchangeArbitrageConfig::default();
        let mut maker = book(ExchangeId::Binance, 100.0, 101.0);
        maker.recv_ts = now - chrono::Duration::seconds(10);
        let taker = book(ExchangeId::Okx, 100.5, 101.5);
        let funding = FundingEstimate {
            long_leg_funding: 0.0,
            short_leg_funding: 0.0,
            net_funding: 0.0,
            net_funding_rate: 0.0,
            next_funding_time: None,
            minutes_to_funding: None,
            dangerous: false,
            near_negative_settlement: false,
        };
        let vwap = calculate_taker_vwap(&taker, OrderSide::Buy, 100.0);

        let decision = RiskGate::evaluate_open(
            &maker,
            &taker,
            RouteStatus::Healthy,
            RouteStatus::Healthy,
            &vwap,
            &funding,
            config.risk.max_notional_per_symbol_usdt,
            &config,
            now,
        );

        assert!(decision.reject_reasons.contains(&RejectReason::StaleBook));
    }
}
