use chrono::Utc;
use rustcta_strategy_spot_spot_arbitrage::{
    RejectionReason as CoreRejectionReason, SimulatedTradeRecord as CoreSimulatedTradeRecord,
    SpotRiskLimits, SpotRiskState,
};

use super::{RejectionReason, SimulatedTradeRecord, SpotSpotTakerArbitrageConfig};

#[derive(Debug, Clone, Default)]
pub struct RiskState {
    core: SpotRiskState,
}

impl RiskState {
    pub fn new(_config: &SpotSpotTakerArbitrageConfig) -> Self {
        Self::default()
    }

    pub fn is_in_cooldown(&self, symbol: &str) -> bool {
        self.core.is_in_cooldown(symbol, Utc::now())
    }

    pub fn is_symbol_blacklisted(&self, symbol: &str) -> bool {
        self.core.is_symbol_blacklisted(symbol, Utc::now())
    }

    pub fn daily_loss_limit_hit(&self, config: &SpotSpotTakerArbitrageConfig) -> bool {
        self.core.daily_loss_limit_hit(&risk_limits(config))
    }

    pub fn consecutive_rejection_limit_hit(&self, config: &SpotSpotTakerArbitrageConfig) -> bool {
        self.core
            .consecutive_rejection_limit_hit(&risk_limits(config))
    }

    pub fn trade_loss_limit_hit(
        &self,
        config: &SpotSpotTakerArbitrageConfig,
        trade: &SimulatedTradeRecord,
    ) -> bool {
        self.core
            .trade_loss_limit_hit(&risk_limits(config), &core_trade(trade))
    }

    pub fn consecutive_rejections(&self) -> u32 {
        self.core.consecutive_rejections()
    }

    pub fn daily_pnl(&self) -> f64 {
        self.core.daily_pnl()
    }

    pub fn notional_limit_hit(
        &self,
        config: &SpotSpotTakerArbitrageConfig,
        symbol: &str,
        notional: f64,
    ) -> bool {
        self.core
            .notional_limit_hit(&risk_limits(config), symbol, notional)
    }

    pub fn record_rejection(&mut self, symbol: &str, reason: RejectionReason) {
        self.core
            .record_rejection(symbol, core_rejection_reason(reason), Utc::now());
    }

    pub fn record_trade(&mut self, trade: &SimulatedTradeRecord) {
        self.core.record_trade(&core_trade(trade), Utc::now());
    }

    pub fn apply_trade_cooldown(&mut self, symbol: &str, cooldown_ms: u64) {
        self.core
            .apply_trade_cooldown(symbol, cooldown_ms, Utc::now());
    }
}

fn core_rejection_reason(reason: RejectionReason) -> CoreRejectionReason {
    match reason {
        RejectionReason::StaleBook => CoreRejectionReason::StaleBook,
        RejectionReason::InsufficientDepth => CoreRejectionReason::InsufficientDepth,
        RejectionReason::MinNotional => CoreRejectionReason::MinNotional,
        RejectionReason::SymbolRule => CoreRejectionReason::SymbolRule,
        RejectionReason::InsufficientQuoteBalance => CoreRejectionReason::InsufficientQuoteBalance,
        RejectionReason::InsufficientBaseBalance => CoreRejectionReason::InsufficientBaseBalance,
        RejectionReason::NetSpreadBelowThreshold => CoreRejectionReason::NetSpreadBelowThreshold,
        RejectionReason::AbnormalSpread => CoreRejectionReason::AbnormalSpread,
        RejectionReason::NotionalLimit => CoreRejectionReason::NotionalLimit,
        RejectionReason::Cooldown => CoreRejectionReason::Cooldown,
        RejectionReason::ExchangeHealth => CoreRejectionReason::ExchangeHealth,
        RejectionReason::DailyLossLimit => CoreRejectionReason::DailyLossLimit,
        RejectionReason::TradeLossLimit => CoreRejectionReason::TradeLossLimit,
        RejectionReason::ConsecutiveRejections => CoreRejectionReason::ConsecutiveRejections,
        RejectionReason::SymbolBlacklisted => CoreRejectionReason::SymbolBlacklisted,
        RejectionReason::DisabledSymbol => CoreRejectionReason::DisabledSymbol,
        RejectionReason::DisabledExchange => CoreRejectionReason::DisabledExchange,
        RejectionReason::DisabledExchangeSymbol => CoreRejectionReason::DisabledExchangeSymbol,
        RejectionReason::ControlPlaneBlocked => CoreRejectionReason::ControlPlaneBlocked,
        RejectionReason::PaperExecutionRejected => CoreRejectionReason::PaperExecutionRejected,
    }
}

fn core_trade(trade: &SimulatedTradeRecord) -> CoreSimulatedTradeRecord {
    CoreSimulatedTradeRecord {
        timestamp: trade.timestamp,
        symbol: trade.symbol.clone(),
        buy_exchange: trade.buy_exchange.clone(),
        sell_exchange: trade.sell_exchange.clone(),
        buy_avg_price: trade.buy_avg_price,
        sell_avg_price: trade.sell_avg_price,
        quantity: trade.quantity,
        notional: trade.notional,
        buy_fee: trade.buy_fee,
        sell_fee: trade.sell_fee,
        gross_pnl: trade.gross_pnl,
        net_pnl: trade.net_pnl,
        latency_ms: trade.latency_ms,
        order_book_age_ms: trade.order_book_age_ms,
        execution_mode: trade.execution_mode.clone(),
    }
}

fn risk_limits(config: &SpotSpotTakerArbitrageConfig) -> SpotRiskLimits {
    SpotRiskLimits {
        max_notional_per_symbol: config.max_notional_per_symbol,
        max_total_notional: config.max_total_notional,
        max_daily_loss: config.max_daily_loss,
        max_trade_loss: config.max_trade_loss,
        max_consecutive_rejections: config.max_consecutive_rejections,
    }
}
