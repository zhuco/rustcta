use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::time::{timeout, Duration};

use crate::market::{
    CanonicalSymbol, ExchangeId, InstrumentMeta, MarketDataAdapter, MarketFundingSnapshot,
};

use super::config::FundingRateArbitrageConfig;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExchangeFundingSelection {
    pub exchange: ExchangeId,
    pub selected: Option<FundingCandidate>,
    pub scanned_symbols: usize,
    pub funding_snapshots: usize,
    pub eligible_candidates: usize,
    pub skipped_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingCandidate {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<String>,
    pub funding_rate: f64,
    pub funding_rate_pct: f64,
    pub predicted_funding_rate: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub seconds_to_settlement: Option<i64>,
    pub snapshot_age_ms: i64,
    pub qualifies: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExchangeScanError {
    pub exchange: ExchangeId,
    pub stage: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingScanReport {
    pub generated_at: DateTime<Utc>,
    pub threshold: f64,
    pub threshold_pct: f64,
    pub selections: Vec<ExchangeFundingSelection>,
    pub errors: Vec<ExchangeScanError>,
}

pub async fn scan_funding_opportunities(
    adapters: &[Box<dyn MarketDataAdapter + Send + Sync>],
    config: &FundingRateArbitrageConfig,
    now: DateTime<Utc>,
) -> FundingScanReport {
    scan_funding_opportunities_with_timeout(adapters, config, now, None).await
}

pub async fn scan_funding_opportunities_with_timeout(
    adapters: &[Box<dyn MarketDataAdapter + Send + Sync>],
    config: &FundingRateArbitrageConfig,
    now: DateTime<Utc>,
    timeout_ms: Option<u64>,
) -> FundingScanReport {
    let mut selections = Vec::new();
    let mut errors = Vec::new();

    for adapter in adapters {
        let result = if let Some(timeout_ms) = timeout_ms {
            match timeout(
                Duration::from_millis(timeout_ms),
                scan_exchange(adapter.as_ref(), config, now),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(anyhow!(
                    "{} scan timed out after {} ms",
                    adapter.exchange(),
                    timeout_ms
                )),
            }
        } else {
            scan_exchange(adapter.as_ref(), config, now).await
        };

        match result {
            Ok(selection) => selections.push(selection),
            Err(err) => errors.push(ExchangeScanError {
                exchange: adapter.exchange(),
                stage: "scan_exchange",
                message: format!("{err:#}"),
            }),
        }
    }

    FundingScanReport {
        generated_at: now,
        threshold: config.selection.min_funding_rate,
        threshold_pct: config.selection.min_funding_rate * 100.0,
        selections,
        errors,
    }
}

pub async fn scan_exchange(
    adapter: &(dyn MarketDataAdapter + Send + Sync),
    config: &FundingRateArbitrageConfig,
    now: DateTime<Utc>,
) -> Result<ExchangeFundingSelection> {
    let exchange = adapter.exchange();
    let instruments = match adapter.load_instruments().await {
        Ok(instruments) => instruments,
        Err(err) => {
            log::warn!(
                "{} load instruments failed; continuing with funding-only scan: {err:#}",
                exchange
            );
            Vec::new()
        }
    };
    let symbols = eligible_symbols(&instruments, config);
    let snapshots = adapter
        .load_funding(&[])
        .await
        .with_context(|| format!("{} load funding", exchange.as_str()))?;
    let snapshot_count = snapshots.len();

    let mut candidates = snapshots
        .into_iter()
        .filter(|snapshot| symbol_allowed(&snapshot.canonical_symbol, config))
        .filter_map(|snapshot| candidate_from_snapshot(snapshot, config, now))
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        left.funding_rate
            .partial_cmp(&right.funding_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let eligible_candidates = candidates
        .iter()
        .filter(|candidate| candidate.qualifies)
        .count();
    let selected = candidates
        .into_iter()
        .find(|candidate| candidate.qualifies)
        .or_else(|| None);
    let skipped_reason = selected
        .is_none()
        .then(|| selection_skip_reason(snapshot_count, eligible_candidates));

    Ok(ExchangeFundingSelection {
        exchange,
        selected,
        scanned_symbols: symbols.len(),
        funding_snapshots: snapshot_count,
        eligible_candidates,
        skipped_reason,
    })
}

fn eligible_symbols(
    instruments: &[InstrumentMeta],
    config: &FundingRateArbitrageConfig,
) -> Vec<CanonicalSymbol> {
    let mut symbols = instruments
        .iter()
        .filter(|instrument| instrument.is_tradeable_usdt_perpetual())
        .filter(|instrument| symbol_allowed(&instrument.canonical_symbol, config))
        .map(|instrument| instrument.canonical_symbol.clone())
        .collect::<Vec<_>>();

    symbols.sort_by(|left, right| left.as_pair().cmp(&right.as_pair()));
    symbols.dedup();
    symbols
}

fn candidate_from_snapshot(
    snapshot: MarketFundingSnapshot,
    config: &FundingRateArbitrageConfig,
    now: DateTime<Utc>,
) -> Option<FundingCandidate> {
    let snapshot_age_ms = now
        .signed_duration_since(snapshot.recv_ts)
        .num_milliseconds()
        .max(0);
    if snapshot_age_ms > config.selection.max_funding_snapshot_age_ms {
        return None;
    }

    let seconds_to_settlement = snapshot
        .next_funding_time
        .map(|time| time.signed_duration_since(now).num_seconds());
    if config.selection.require_next_funding_time && seconds_to_settlement.is_none() {
        return None;
    }
    if let Some(min_seconds) = config.selection.min_seconds_to_settlement_at_scan {
        if seconds_to_settlement.is_some_and(|seconds| seconds < min_seconds) {
            return None;
        }
    }
    if let Some(max_seconds) = config.selection.max_seconds_to_settlement_at_scan {
        if seconds_to_settlement.is_some_and(|seconds| seconds > max_seconds) {
            return None;
        }
    }

    let qualifies = snapshot.funding_rate <= config.selection.min_funding_rate;

    Some(FundingCandidate {
        exchange: snapshot.exchange,
        canonical_symbol: snapshot.canonical_symbol,
        exchange_symbol: snapshot.exchange_symbol.map(|symbol| symbol.symbol),
        funding_rate: snapshot.funding_rate,
        funding_rate_pct: snapshot.funding_rate * 100.0,
        predicted_funding_rate: snapshot.predicted_funding_rate,
        mark_price: snapshot.mark_price,
        index_price: snapshot.index_price,
        next_funding_time: snapshot.next_funding_time,
        seconds_to_settlement,
        snapshot_age_ms,
        qualifies,
    })
}

fn symbol_allowed(symbol: &CanonicalSymbol, config: &FundingRateArbitrageConfig) -> bool {
    if !symbol
        .quote()
        .eq_ignore_ascii_case(&config.universe.quote_asset)
    {
        return false;
    }

    let allowlist = normalized_symbol_set(&config.universe.symbol_allowlist);
    let blocklist = normalized_symbol_set(&config.universe.symbol_blocklist);
    let normalized = normalize_symbol(&symbol.as_pair());

    if blocklist.contains(&normalized) {
        return false;
    }
    allowlist.is_empty() || allowlist.contains(&normalized)
}

fn normalized_symbol_set(values: &[String]) -> HashSet<String> {
    values.iter().map(|value| normalize_symbol(value)).collect()
}

fn normalize_symbol(value: &str) -> String {
    value
        .trim()
        .to_ascii_uppercase()
        .replace('-', "")
        .replace('_', "")
        .replace('/', "")
        .replace("SWAP", "")
}

fn selection_skip_reason(snapshot_count: usize, eligible_candidates: usize) -> String {
    if snapshot_count == 0 {
        "no funding snapshots loaded".to_string()
    } else if eligible_candidates == 0 {
        "no candidate met threshold and filters".to_string()
    } else {
        "no selected candidate".to_string()
    }
}

pub fn require_observe_mode(config: &FundingRateArbitrageConfig) -> Result<()> {
    if config.mode.trim().eq_ignore_ascii_case("observe") {
        Ok(())
    } else {
        Err(anyhow!(
            "funding_arb_observe never places orders; set mode: observe for this binary"
        ))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use crate::market::ExchangeSymbol;

    use super::*;

    #[test]
    fn candidate_should_qualify_at_negative_half_percent_threshold() {
        let config = FundingRateArbitrageConfig::default();
        let now = Utc::now();
        let snapshot = MarketFundingSnapshot::new(
            ExchangeId::Bitget,
            CanonicalSymbol::new("btc", "usdt"),
            Some(ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT")),
            -0.0051,
            Some(now + Duration::minutes(5)),
            now,
        );

        let candidate = candidate_from_snapshot(snapshot, &config, now).expect("candidate");

        assert!(candidate.qualifies);
        assert_eq!(candidate.funding_rate_pct, -0.51);
    }

    #[test]
    fn candidate_should_not_qualify_above_threshold() {
        let config = FundingRateArbitrageConfig::default();
        let now = Utc::now();
        let snapshot = MarketFundingSnapshot::new(
            ExchangeId::Gate,
            CanonicalSymbol::new("eth", "usdt"),
            Some(ExchangeSymbol::new(ExchangeId::Gate, "ETH_USDT")),
            -0.0049,
            Some(now + Duration::minutes(5)),
            now,
        );

        let candidate = candidate_from_snapshot(snapshot, &config, now).expect("candidate");

        assert!(!candidate.qualifies);
    }

    #[test]
    fn symbol_filter_should_respect_blocklist() {
        let mut config = FundingRateArbitrageConfig::default();
        config.universe.symbol_blocklist = vec!["BTC/USDT".to_string()];

        assert!(!symbol_allowed(
            &CanonicalSymbol::new("btc", "usdt"),
            &config
        ));
        assert!(symbol_allowed(
            &CanonicalSymbol::new("eth", "usdt"),
            &config
        ));
    }
}
