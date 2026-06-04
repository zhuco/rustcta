use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraderId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalletId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Direction {
    Long,
    Short,
    Flat,
}

impl Direction {
    pub fn sign(self) -> Decimal {
        match self {
            Self::Long => Decimal::ONE,
            Self::Short => -Decimal::ONE,
            Self::Flat => Decimal::ZERO,
        }
    }

    pub fn from_signed_notional(notional: Decimal) -> Self {
        if notional > Decimal::ZERO {
            Self::Long
        } else if notional < Decimal::ZERO {
            Self::Short
        } else {
            Self::Flat
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TraderCluster {
    Trend,
    Swing,
    Intraday,
    Scalper,
    Momentum,
    MeanReversion,
    Whale,
    InstitutionalStyle,
    ConsistentAlpha,
    Gambler,
    NewAlpha,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TraderStatus {
    Active,
    Inactive,
    Quarantined,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraderProfile {
    pub trader_id: TraderId,
    pub wallet_id: WalletId,
    pub address: String,
    pub first_seen: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
    pub status: TraderStatus,
    pub clusters: Vec<TraderCluster>,
    pub behavior_tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletTrade {
    pub wallet_id: WalletId,
    pub symbol: String,
    pub direction: Direction,
    pub price: Decimal,
    pub quantity: Decimal,
    pub notional_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub realized_pnl_usdt: Decimal,
    pub executed_at: DateTime<Utc>,
    pub external_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletPositionSnapshot {
    pub wallet_id: WalletId,
    pub symbol: String,
    pub direction: Direction,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub notional_usdt: Decimal,
    pub equity_usdt: Decimal,
    pub margin_used_usdt: Decimal,
    pub leverage: Decimal,
    pub unrealized_pnl_usdt: Decimal,
    pub observed_at: DateTime<Utc>,
}

impl WalletPositionSnapshot {
    pub fn signed_notional(&self) -> Decimal {
        self.notional_usdt.abs() * self.direction.sign()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletProfile {
    pub wallet_id: WalletId,
    pub as_of: DateTime<Utc>,
    pub total_return: Decimal,
    pub return_30d: Decimal,
    pub return_90d: Decimal,
    pub return_180d: Decimal,
    pub sharpe: Option<Decimal>,
    pub sortino: Option<Decimal>,
    pub calmar: Option<Decimal>,
    pub max_drawdown: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub average_holding_secs: i64,
    pub average_leverage: Decimal,
    pub maximum_leverage: Decimal,
    pub trade_frequency_daily: Decimal,
    pub position_concentration: Decimal,
    pub risk_per_trade: Decimal,
    pub signal_reproducibility: Decimal,
    pub capital_efficiency: Decimal,
    pub behavior_stability: Decimal,
    pub recent_performance: Decimal,
    pub closed_trades: u32,
    pub active_days: u32,
    pub history_days: u32,
    pub equity_usdt: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketRegimeSnapshot {
    pub as_of: DateTime<Utc>,
    pub bull_trend: Decimal,
    pub bear_trend: Decimal,
    pub range: Decimal,
    pub volatility_expansion: Decimal,
    pub volatility_compression: Decimal,
    pub risk_on: Decimal,
    pub risk_off: Decimal,
    pub funding_extreme: Decimal,
    pub liquidity_crisis: Decimal,
    pub trend_acceleration: Decimal,
    pub trend_exhaustion: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletScoreMatrix {
    pub wallet_id: WalletId,
    pub as_of: DateTime<Utc>,
    pub trend_score: Decimal,
    pub range_score: Decimal,
    pub high_volatility_score: Decimal,
    pub low_volatility_score: Decimal,
    pub risk_on_score: Decimal,
    pub risk_off_score: Decimal,
    pub current_regime_score: Decimal,
    pub recent_performance_score: Decimal,
    pub consistency_score: Decimal,
    pub signal_quality_score: Decimal,
    pub execution_quality_score: Decimal,
    pub final_score: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalletOpinion {
    pub wallet_id: WalletId,
    pub symbol: String,
    pub direction: Direction,
    pub confidence: Decimal,
    pub conviction: Decimal,
    pub dynamic_score: Decimal,
    pub wallet_equity_usdt: Decimal,
    pub clusters: Vec<TraderCluster>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregatedAlpha {
    pub symbol: String,
    pub as_of: DateTime<Utc>,
    pub long_pressure: Decimal,
    pub short_pressure: Decimal,
    pub consensus_strength: Decimal,
    pub capital_weighted_consensus: Decimal,
    pub cluster_weighted_consensus: Decimal,
    pub net_alpha_score: Decimal,
    pub alpha_confidence_score: Decimal,
    pub contributing_wallets: usize,
    pub dominant_wallet_share: Decimal,
    pub dominant_cluster_share: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioConstraints {
    pub initial_capital_usdt: Decimal,
    pub standard_entry_notional_usdt: Decimal,
    pub max_leverage: Decimal,
    pub max_gross_notional_usdt: Decimal,
    pub max_single_asset_gross_share: Decimal,
}

impl Default for PortfolioConstraints {
    fn default() -> Self {
        Self {
            initial_capital_usdt: Decimal::new(2000, 0),
            standard_entry_notional_usdt: Decimal::new(1000, 0),
            max_leverage: Decimal::new(10, 0),
            max_gross_notional_usdt: Decimal::new(20000, 0),
            max_single_asset_gross_share: Decimal::new(35, 2),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetPortfolio {
    pub portfolio_id: Uuid,
    pub as_of: DateTime<Utc>,
    pub nav_usdt: Decimal,
    pub gross_notional_usdt: Decimal,
    pub leverage: Decimal,
    pub target_weights: BTreeMap<String, Decimal>,
    pub target_notional: BTreeMap<String, Decimal>,
    pub cash_weight: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionState {
    pub symbol: String,
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
    pub notional_usdt: Decimal,
    pub realized_pnl_usdt: Decimal,
    pub unrealized_pnl_usdt: Decimal,
    pub funding_pnl_usdt: Decimal,
    pub fee_paid_usdt: Decimal,
    pub margin_used_usdt: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PortfolioAction {
    Open,
    ScaleIn,
    ScaleOut,
    Reduce,
    Close,
    Reverse,
    Noop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioTransition {
    pub symbol: String,
    pub action: PortfolioAction,
    pub current_notional: Decimal,
    pub target_notional: Decimal,
    pub delta_notional: Decimal,
}
