use crate::smart_money::{
    scoring::clamp01, AggregatedAlpha, Direction, TraderCluster, WalletOpinion,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct AlphaAggregationConfig {
    pub max_wallet_pressure_share: Decimal,
    pub max_cluster_pressure_share: Decimal,
    pub stale_after_secs: i64,
}

impl Default for AlphaAggregationConfig {
    fn default() -> Self {
        Self {
            max_wallet_pressure_share: Decimal::new(20, 2),
            max_cluster_pressure_share: Decimal::new(40, 2),
            stale_after_secs: 300,
        }
    }
}

pub fn aggregate_alpha(
    opinions: &[WalletOpinion],
    as_of: DateTime<Utc>,
    config: &AlphaAggregationConfig,
) -> Vec<AggregatedAlpha> {
    let mut by_symbol: HashMap<String, Vec<&WalletOpinion>> = HashMap::new();
    for opinion in opinions {
        if opinion.direction == Direction::Flat || opinion.expires_at <= as_of {
            continue;
        }
        if as_of
            .signed_duration_since(opinion.observed_at)
            .num_seconds()
            > config.stale_after_secs
        {
            continue;
        }
        by_symbol
            .entry(opinion.symbol.clone())
            .or_default()
            .push(opinion);
    }

    let mut output = by_symbol
        .into_iter()
        .map(|(symbol, opinions)| aggregate_symbol(symbol, opinions, as_of))
        .collect::<Vec<_>>();
    output.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    output
}

fn aggregate_symbol(
    symbol: String,
    opinions: Vec<&WalletOpinion>,
    as_of: DateTime<Utc>,
) -> AggregatedAlpha {
    let mut long_pressure = Decimal::ZERO;
    let mut short_pressure = Decimal::ZERO;
    let mut capital_weighted_consensus = Decimal::ZERO;
    let mut cluster_pressure: HashMap<TraderCluster, Decimal> = HashMap::new();
    let mut wallet_pressure: HashMap<String, Decimal> = HashMap::new();

    for opinion in &opinions {
        let pressure = opinion_weight(opinion, as_of);
        match opinion.direction {
            Direction::Long => long_pressure += pressure,
            Direction::Short => short_pressure += pressure,
            Direction::Flat => {}
        }
        capital_weighted_consensus +=
            opinion.wallet_equity_usdt * pressure * opinion.direction.sign();
        wallet_pressure.insert(format!("{:?}", opinion.wallet_id), pressure);
        for cluster in &opinion.clusters {
            *cluster_pressure.entry(cluster.clone()).or_default() += pressure;
        }
    }

    let total_pressure = long_pressure + short_pressure;
    let total_cluster_pressure = cluster_pressure.values().copied().sum::<Decimal>();
    let net_alpha_score = if total_pressure > Decimal::ZERO {
        (long_pressure - short_pressure) / total_pressure
    } else {
        Decimal::ZERO
    };
    let consensus_strength = net_alpha_score.abs();
    let dominant_wallet_share = dominant_share(wallet_pressure.values().copied(), total_pressure);
    let dominant_cluster_share =
        dominant_share(cluster_pressure.values().copied(), total_cluster_pressure);
    let cluster_weighted_consensus = if cluster_pressure.is_empty() {
        Decimal::ZERO
    } else {
        let cluster_count = Decimal::from(cluster_pressure.len() as u64);
        cluster_pressure.values().copied().sum::<Decimal>() / cluster_count
    };
    let diversity_penalty = dominant_wallet_share.max(dominant_cluster_share);
    let alpha_confidence_score = clamp01(consensus_strength * (Decimal::ONE - diversity_penalty));

    AggregatedAlpha {
        symbol,
        as_of,
        long_pressure,
        short_pressure,
        consensus_strength,
        capital_weighted_consensus,
        cluster_weighted_consensus,
        net_alpha_score,
        alpha_confidence_score,
        contributing_wallets: opinions.len(),
        dominant_wallet_share,
        dominant_cluster_share,
    }
}

pub fn opinion_weight(opinion: &WalletOpinion, as_of: DateTime<Utc>) -> Decimal {
    let age_secs = as_of
        .signed_duration_since(opinion.observed_at)
        .num_seconds()
        .max(0);
    let freshness_decay = if age_secs <= 60 {
        Decimal::ONE
    } else {
        Decimal::ONE / Decimal::from(1 + age_secs / 60)
    };
    clamp01(opinion.dynamic_score)
        * clamp01(opinion.confidence)
        * clamp01(opinion.conviction)
        * freshness_decay
}

fn dominant_share(values: impl Iterator<Item = Decimal>, total: Decimal) -> Decimal {
    if total <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    values.max().unwrap_or(Decimal::ZERO) / total
}
