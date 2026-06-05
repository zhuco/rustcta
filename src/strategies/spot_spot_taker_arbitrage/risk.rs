use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};

use super::{RejectionReason, SimulatedTradeRecord, SpotSpotTakerArbitrageConfig};

#[derive(Debug, Clone, Default)]
pub struct RiskState {
    symbol_notional: HashMap<String, f64>,
    total_notional: f64,
    consecutive_rejections: u32,
    symbol_failures: HashMap<String, u32>,
    cooldown_until: HashMap<String, DateTime<Utc>>,
    blacklisted_symbols: HashSet<String>,
    daily_pnl: f64,
}

impl RiskState {
    pub fn new(_config: &SpotSpotTakerArbitrageConfig) -> Self {
        Self::default()
    }

    pub fn is_in_cooldown(&self, symbol: &str) -> bool {
        self.cooldown_until
            .get(symbol)
            .is_some_and(|until| *until > Utc::now())
    }

    pub fn is_symbol_blacklisted(&self, symbol: &str) -> bool {
        self.blacklisted_symbols.contains(symbol)
    }

    pub fn daily_loss_limit_hit(&self, config: &SpotSpotTakerArbitrageConfig) -> bool {
        self.daily_pnl < -config.max_daily_loss
    }

    pub fn consecutive_rejection_limit_hit(&self, config: &SpotSpotTakerArbitrageConfig) -> bool {
        self.consecutive_rejections >= config.max_consecutive_rejections
    }

    pub fn notional_limit_hit(
        &self,
        config: &SpotSpotTakerArbitrageConfig,
        symbol: &str,
        notional: f64,
    ) -> bool {
        self.total_notional + notional > config.max_total_notional + 1e-12
            || self
                .symbol_notional
                .get(symbol)
                .copied()
                .unwrap_or_default()
                + notional
                > config.max_notional_per_symbol + 1e-12
    }

    pub fn record_rejection(&mut self, symbol: &str, reason: RejectionReason) {
        self.consecutive_rejections += 1;
        if matches!(
            reason,
            RejectionReason::InsufficientDepth
                | RejectionReason::StaleBook
                | RejectionReason::ExchangeHealth
        ) {
            let failures = self.symbol_failures.entry(symbol.to_string()).or_default();
            *failures += 1;
            if *failures >= 20 {
                self.blacklisted_symbols.insert(symbol.to_string());
            }
        }
    }

    pub fn record_trade(&mut self, trade: &SimulatedTradeRecord) {
        self.consecutive_rejections = 0;
        self.total_notional += trade.notional;
        *self
            .symbol_notional
            .entry(trade.symbol.clone())
            .or_default() += trade.notional;
        self.daily_pnl += trade.net_pnl;
        self.cooldown_until
            .insert(trade.symbol.clone(), Utc::now() + Duration::milliseconds(0));
    }

    pub fn apply_trade_cooldown(&mut self, symbol: &str, cooldown_ms: u64) {
        self.cooldown_until.insert(
            symbol.to_string(),
            Utc::now() + Duration::milliseconds(cooldown_ms as i64),
        );
    }
}
