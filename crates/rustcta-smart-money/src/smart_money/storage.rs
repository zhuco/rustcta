use crate::smart_money::{
    AggregatedAlpha, MarketRegimeSnapshot, RiskDecision, TargetPortfolio, WalletPositionSnapshot,
    WalletProfile, WalletScoreMatrix, WalletTrade,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait WalletFactRepository: Send + Sync {
    async fn record_wallet_positions(&self, positions: Vec<WalletPositionSnapshot>);
    async fn record_wallet_trades(&self, trades: Vec<WalletTrade>);
}

#[async_trait]
pub trait ResearchStateRepository: Send + Sync {
    async fn record_wallet_profiles(&self, profiles: Vec<WalletProfile>);
    async fn record_wallet_scores(&self, scores: Vec<WalletScoreMatrix>);
    async fn record_market_regime(&self, regime: MarketRegimeSnapshot);
    async fn record_alpha(&self, alpha: Vec<AggregatedAlpha>);
    async fn record_target_portfolio(&self, target: TargetPortfolio);
    async fn record_risk_decision(&self, decision: RiskDecision);
}

#[derive(Debug, Default, Clone)]
pub struct InMemorySmartMoneyStore {
    inner: Arc<RwLock<InMemorySmartMoneyState>>,
}

#[derive(Debug, Default, Clone)]
pub struct InMemorySmartMoneyState {
    pub positions: Vec<WalletPositionSnapshot>,
    pub trades: Vec<WalletTrade>,
    pub profiles: Vec<WalletProfile>,
    pub scores: Vec<WalletScoreMatrix>,
    pub regimes: Vec<MarketRegimeSnapshot>,
    pub alpha: Vec<AggregatedAlpha>,
    pub targets: Vec<TargetPortfolio>,
    pub risk_decisions: Vec<RiskDecision>,
}

impl InMemorySmartMoneyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn snapshot(&self) -> InMemorySmartMoneyState {
        self.inner.read().await.clone()
    }

    pub async fn latest_profiles_before(&self, as_of: DateTime<Utc>) -> Vec<WalletProfile> {
        self.inner
            .read()
            .await
            .profiles
            .iter()
            .filter(|profile| profile.as_of <= as_of)
            .cloned()
            .collect()
    }

    pub async fn latest_positions_before(
        &self,
        as_of: DateTime<Utc>,
    ) -> Vec<WalletPositionSnapshot> {
        self.inner
            .read()
            .await
            .positions
            .iter()
            .filter(|position| position.observed_at <= as_of)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl WalletFactRepository for InMemorySmartMoneyStore {
    async fn record_wallet_positions(&self, positions: Vec<WalletPositionSnapshot>) {
        self.inner.write().await.positions.extend(positions);
    }

    async fn record_wallet_trades(&self, trades: Vec<WalletTrade>) {
        self.inner.write().await.trades.extend(trades);
    }
}

#[async_trait]
impl ResearchStateRepository for InMemorySmartMoneyStore {
    async fn record_wallet_profiles(&self, profiles: Vec<WalletProfile>) {
        self.inner.write().await.profiles.extend(profiles);
    }

    async fn record_wallet_scores(&self, scores: Vec<WalletScoreMatrix>) {
        self.inner.write().await.scores.extend(scores);
    }

    async fn record_market_regime(&self, regime: MarketRegimeSnapshot) {
        self.inner.write().await.regimes.push(regime);
    }

    async fn record_alpha(&self, alpha: Vec<AggregatedAlpha>) {
        self.inner.write().await.alpha.extend(alpha);
    }

    async fn record_target_portfolio(&self, target: TargetPortfolio) {
        self.inner.write().await.targets.push(target);
    }

    async fn record_risk_decision(&self, decision: RiskDecision) {
        self.inner.write().await.risk_decisions.push(decision);
    }
}
