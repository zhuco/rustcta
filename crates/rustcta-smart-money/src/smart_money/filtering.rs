use crate::smart_money::{WalletPositionSnapshot, WalletProfile, WalletTrade};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletFilterConfig {
    pub min_trade_notional_usdt: Decimal,
    pub min_trade_equity_pct: Decimal,
    pub min_position_notional_usdt: Decimal,
    pub min_position_equity_pct: Decimal,
    pub min_wallet_equity_usdt: Decimal,
    pub min_closed_trades: u32,
    pub min_history_days: u32,
    pub min_active_days: u32,
    pub max_wallet_leverage: Decimal,
    pub max_drawdown: Decimal,
    pub min_profit_factor: Decimal,
    pub stale_after_secs: i64,
}

impl Default for WalletFilterConfig {
    fn default() -> Self {
        Self {
            min_trade_notional_usdt: Decimal::new(100, 0),
            min_trade_equity_pct: Decimal::new(10, 4),
            min_position_notional_usdt: Decimal::new(250, 0),
            min_position_equity_pct: Decimal::new(50, 4),
            min_wallet_equity_usdt: Decimal::new(5000, 0),
            min_closed_trades: 50,
            min_history_days: 45,
            min_active_days: 15,
            max_wallet_leverage: Decimal::new(15, 0),
            max_drawdown: Decimal::new(40, 2),
            min_profit_factor: Decimal::new(110, 2),
            stale_after_secs: 120,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterRejectReason {
    TradeBelowNotional,
    TradeBelowEquityPct,
    PositionBelowNotional,
    PositionBelowEquityPct,
    WalletBelowEquity,
    InsufficientClosedTrades,
    InsufficientHistory,
    InsufficientActiveDays,
    ExcessiveLeverage,
    DrawdownTooLarge,
    PoorProfitFactor,
    StaleWalletState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterDecision {
    pub accepted: bool,
    pub reasons: Vec<FilterRejectReason>,
}

impl FilterDecision {
    pub fn accepted() -> Self {
        Self {
            accepted: true,
            reasons: Vec::new(),
        }
    }

    fn from_reasons(reasons: Vec<FilterRejectReason>) -> Self {
        Self {
            accepted: reasons.is_empty(),
            reasons,
        }
    }
}

pub fn filter_trade(
    trade: &WalletTrade,
    wallet_equity_usdt: Decimal,
    config: &WalletFilterConfig,
) -> FilterDecision {
    let mut reasons = Vec::new();
    let abs_notional = trade.notional_usdt.abs();
    if abs_notional < config.min_trade_notional_usdt {
        reasons.push(FilterRejectReason::TradeBelowNotional);
    }
    if wallet_equity_usdt > Decimal::ZERO
        && abs_notional / wallet_equity_usdt < config.min_trade_equity_pct
    {
        reasons.push(FilterRejectReason::TradeBelowEquityPct);
    }
    FilterDecision::from_reasons(reasons)
}

pub fn filter_position(
    position: &WalletPositionSnapshot,
    now: DateTime<Utc>,
    config: &WalletFilterConfig,
) -> FilterDecision {
    let mut reasons = Vec::new();
    let abs_notional = position.notional_usdt.abs();
    if abs_notional < config.min_position_notional_usdt {
        reasons.push(FilterRejectReason::PositionBelowNotional);
    }
    if position.equity_usdt > Decimal::ZERO
        && abs_notional / position.equity_usdt < config.min_position_equity_pct
    {
        reasons.push(FilterRejectReason::PositionBelowEquityPct);
    }
    if now
        .signed_duration_since(position.observed_at)
        .num_seconds()
        > config.stale_after_secs
    {
        reasons.push(FilterRejectReason::StaleWalletState);
    }
    FilterDecision::from_reasons(reasons)
}

pub fn filter_wallet_profile(
    profile: &WalletProfile,
    config: &WalletFilterConfig,
) -> FilterDecision {
    let mut reasons = Vec::new();
    if profile.equity_usdt < config.min_wallet_equity_usdt {
        reasons.push(FilterRejectReason::WalletBelowEquity);
    }
    if profile.closed_trades < config.min_closed_trades {
        reasons.push(FilterRejectReason::InsufficientClosedTrades);
    }
    if profile.history_days < config.min_history_days {
        reasons.push(FilterRejectReason::InsufficientHistory);
    }
    if profile.active_days < config.min_active_days {
        reasons.push(FilterRejectReason::InsufficientActiveDays);
    }
    if profile.maximum_leverage > config.max_wallet_leverage {
        reasons.push(FilterRejectReason::ExcessiveLeverage);
    }
    if profile.max_drawdown > config.max_drawdown {
        reasons.push(FilterRejectReason::DrawdownTooLarge);
    }
    if profile.profit_factor < config.min_profit_factor {
        reasons.push(FilterRejectReason::PoorProfitFactor);
    }
    FilterDecision::from_reasons(reasons)
}
