use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{CachedBook, OpportunityRecord, SpotSpotTakerArbitrageConfig, SpotVenue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArbitragePairStatus {
    Opening,
    Arbitraging,
    InventoryDrift,
    OneSidedExposure,
    Exiting,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OneSidedExposureKind {
    LongBase,
    ShortBase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OneSidedExposure {
    pub symbol: String,
    pub kind: OneSidedExposureKind,
    pub exchange: SpotVenue,
    pub quantity: f64,
    pub reference_price: f64,
    pub notional: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArbitragePairRuntime {
    pub symbol: String,
    pub status: ArbitragePairStatus,
    pub created_at: DateTime<Utc>,
    pub last_opportunity_at: DateTime<Utc>,
    pub last_entry_attempt_at: Option<DateTime<Utc>>,
    pub entry_attempts: u32,
    pub exit_attempts: u32,
    pub pause_reason: Option<String>,
    pub one_sided_exposure: Option<OneSidedExposure>,
}

impl ArbitragePairRuntime {
    pub fn opening(symbol: &str, now: DateTime<Utc>) -> Self {
        Self {
            symbol: normalize_symbol(symbol),
            status: ArbitragePairStatus::Opening,
            created_at: now,
            last_opportunity_at: now,
            last_entry_attempt_at: None,
            entry_attempts: 0,
            exit_attempts: 0,
            pause_reason: None,
            one_sided_exposure: None,
        }
    }

    pub fn should_attempt_entry(&self, now: DateTime<Utc>, cooldown_seconds: i64) -> bool {
        self.last_entry_attempt_at
            .is_none_or(|last| now.signed_duration_since(last).num_seconds() >= cooldown_seconds)
    }

    pub fn mark_entry_attempt(&mut self, now: DateTime<Utc>) {
        self.last_entry_attempt_at = Some(now);
        self.entry_attempts = self.entry_attempts.saturating_add(1);
    }

    pub fn mark_arbitraging(&mut self, now: DateTime<Utc>) {
        self.status = ArbitragePairStatus::Arbitraging;
        self.last_opportunity_at = now;
        self.pause_reason = None;
        self.one_sided_exposure = None;
    }

    pub fn mark_opportunity(&mut self, now: DateTime<Utc>) {
        self.last_opportunity_at = now;
    }

    pub fn should_exit_for_inactivity(
        &self,
        now: DateTime<Utc>,
        inactivity_exit_seconds: u64,
    ) -> bool {
        self.status == ArbitragePairStatus::Arbitraging
            && now
                .signed_duration_since(self.last_opportunity_at)
                .num_seconds()
                >= inactivity_exit_seconds as i64
    }

    pub fn mark_inventory_drift(&mut self, reason: impl Into<String>) {
        self.status = ArbitragePairStatus::InventoryDrift;
        self.pause_reason = Some(reason.into());
    }

    pub fn mark_one_sided_exposure(&mut self, exposure: OneSidedExposure) {
        self.status = ArbitragePairStatus::OneSidedExposure;
        self.pause_reason = Some(match exposure.kind {
            OneSidedExposureKind::LongBase => {
                "buy leg filled but sell leg failed; waiting for no-loss sell recovery".to_string()
            }
            OneSidedExposureKind::ShortBase => {
                "sell leg filled but buy leg failed; waiting for no-loss buy recovery".to_string()
            }
        });
        self.one_sided_exposure = Some(exposure);
    }
}

#[derive(Debug, Default)]
pub struct SpreadDurationTracker {
    active: BTreeMap<String, DateTime<Utc>>,
}

impl SpreadDurationTracker {
    pub fn observe(
        &mut self,
        opportunity: &OpportunityRecord,
        now: DateTime<Utc>,
        threshold_seconds: u64,
    ) -> bool {
        let symbol = normalize_symbol(&opportunity.symbol);
        if !opportunity.accepted {
            self.active.remove(&symbol);
            return false;
        }
        let first_seen = self.active.entry(symbol).or_insert(now);
        now.signed_duration_since(*first_seen).num_seconds() >= threshold_seconds as i64
    }

    pub fn clear(&mut self, symbol: &str) {
        self.active.remove(&normalize_symbol(symbol));
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryAllocation {
    pub exchange: SpotVenue,
    pub best_bid: f64,
    pub best_ask: f64,
    pub weight: f64,
    pub notional: f64,
}

pub fn configured_spot_venues(exchanges: &[String]) -> Vec<SpotVenue> {
    let mut venues = Vec::new();
    let mut seen = BTreeSet::new();
    for exchange in exchanges {
        let venue = match exchange.trim().to_ascii_lowercase().as_str() {
            "mexc" => Some(SpotVenue::Mexc),
            "coinex" => Some(SpotVenue::CoinEx),
            "gate" | "gateio" | "gate.io" => Some(SpotVenue::GateIo),
            "bitget" => Some(SpotVenue::Bitget),
            _ => None,
        };
        if let Some(venue) = venue {
            if seen.insert(venue.as_str()) {
                venues.push(venue);
            }
        }
    }
    venues.truncate(4);
    venues
}

pub fn allocation_weights(exchange_count: usize) -> &'static [f64] {
    match exchange_count {
        4.. => &[0.40, 0.30, 0.20, 0.10],
        3 => &[0.50, 0.30, 0.20],
        2 => &[0.65, 0.35],
        1 => &[1.0],
        _ => &[],
    }
}

pub fn build_entry_allocations(
    config: &SpotSpotTakerArbitrageConfig,
    books: &[(SpotVenue, CachedBook)],
) -> Vec<EntryAllocation> {
    let mut candidates = books
        .iter()
        .filter_map(|(exchange, book)| {
            Some(EntryAllocation {
                exchange: *exchange,
                best_bid: book
                    .best_bid
                    .or_else(|| book.bids.first().map(|l| l.price))?,
                best_ask: book
                    .best_ask
                    .or_else(|| book.asks.first().map(|l| l.price))?,
                weight: 0.0,
                notional: 0.0,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.best_bid.total_cmp(&right.best_bid));
    candidates.truncate(4);
    let weights = allocation_weights(candidates.len());
    for (candidate, weight) in candidates.iter_mut().zip(weights.iter().copied()) {
        candidate.weight = weight;
        candidate.notional = config.initial_entry_notional_usdt * weight;
    }
    candidates
}

pub fn duration_from_seconds(seconds: u64) -> chrono::Duration {
    chrono::Duration::seconds(seconds.min(i64::MAX as u64) as i64)
}

pub fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BookSource, SpotOrderBookLevel};

    #[test]
    fn runtime_should_track_entry_cooldown_and_inactivity() {
        let now = Utc::now();
        let mut runtime = ArbitragePairRuntime::opening("btc/usdt", now);

        assert_eq!(runtime.symbol, "BTCUSDT");
        assert!(runtime.should_attempt_entry(now, 10));
        runtime.mark_entry_attempt(now);
        assert!(!runtime.should_attempt_entry(now + chrono::Duration::seconds(9), 10));
        assert!(runtime.should_attempt_entry(now + chrono::Duration::seconds(10), 10));

        runtime.mark_arbitraging(now);
        assert!(runtime.should_exit_for_inactivity(now + chrono::Duration::seconds(30), 30));
    }

    #[test]
    fn spread_duration_should_require_continuous_accepted_opportunity() {
        let now = Utc::now();
        let mut tracker = SpreadDurationTracker::default();
        let mut opportunity = opportunity(now, true);

        assert!(!tracker.observe(&opportunity, now, 10));
        assert!(tracker.observe(&opportunity, now + chrono::Duration::seconds(10), 10));
        opportunity.accepted = false;
        assert!(!tracker.observe(&opportunity, now + chrono::Duration::seconds(11), 10));
        opportunity.accepted = true;
        assert!(!tracker.observe(&opportunity, now + chrono::Duration::seconds(12), 10));
    }

    #[test]
    fn entry_allocations_should_weight_low_bid_venues_first() {
        let config = SpotSpotTakerArbitrageConfig {
            enabled: true,
            trading_mode: "paper".to_string(),
            exchanges: vec!["gateio".to_string(), "bitget".to_string()],
            symbols: vec!["BTCUSDT".to_string()],
            quote_asset: "USDT".to_string(),
            max_notional_per_trade: 10.0,
            min_notional_per_trade: 1.0,
            max_notional_per_symbol: 100.0,
            max_total_notional: 100.0,
            initial_entry_notional_usdt: 20.0,
            min_net_spread_bps: 0.0,
            taker_fee_bps_override: None,
            min_depth_notional: 1.0,
            ..serde_yaml::from_str("exchanges: [gateio, bitget]\nsymbols: [BTCUSDT]\nmax_notional_per_trade: 10\nmin_notional_per_trade: 1\nmax_notional_per_symbol: 100\nmax_total_notional: 100\nmin_net_spread_bps: 0\nmin_depth_notional: 1\n").unwrap()
        };
        let books = vec![
            (SpotVenue::Bitget, book("bitget", 105.0, 106.0)),
            (SpotVenue::GateIo, book("gateio", 100.0, 101.0)),
        ];

        let allocations = build_entry_allocations(&config, &books);

        assert_eq!(allocations[0].exchange, SpotVenue::GateIo);
        assert_eq!(allocations[0].weight, 0.65);
        assert_eq!(allocations[0].notional, 13.0);
        assert_eq!(allocations[1].exchange, SpotVenue::Bitget);
        assert_eq!(allocations[1].weight, 0.35);
    }

    fn book(exchange: &str, bid: f64, ask: f64) -> CachedBook {
        CachedBook {
            exchange: exchange.to_string(),
            symbol: "BTCUSDT".to_string(),
            bids: vec![SpotOrderBookLevel {
                price: bid,
                quantity: 1.0,
            }],
            asks: vec![SpotOrderBookLevel {
                price: ask,
                quantity: 1.0,
            }],
            best_bid: Some(bid),
            best_ask: Some(ask),
            exchange_timestamp: Some(Utc::now()),
            local_timestamp: Utc::now(),
            latency_ms: Some(1),
            sequence: Some(1),
            source: BookSource::Websocket,
            is_stale: false,
        }
    }

    fn opportunity(timestamp: DateTime<Utc>, accepted: bool) -> OpportunityRecord {
        OpportunityRecord {
            timestamp,
            symbol: "BTCUSDT".to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            buy_price: 100.0,
            sell_price: 101.0,
            raw_spread_bps: 100.0,
            buy_fee_bps: 10.0,
            sell_fee_bps: 10.0,
            fee_source_buy: crate::SpotFeeSource::Config,
            fee_source_sell: crate::SpotFeeSource::Config,
            platform_discount_applied: false,
            estimated_fee_bps: 20.0,
            estimated_slippage_bps: 0.0,
            safety_buffer_bps: 0.0,
            estimated_net_spread_bps: 80.0,
            estimated_total_fee: 0.2,
            estimated_gross_pnl: 1.0,
            estimated_net_pnl: 0.8,
            capital_cost_bps: 0.0,
            transfer_cost_bps: 0.0,
            transfer_delay_penalty_bps: 0.0,
            inventory_rebalance_cost_bps: 0.0,
            latency_penalty_bps: 0.0,
            effective_min_net_spread_bps: 0.0,
            estimated_slippage_cost: 0.0,
            estimated_capital_cost: 0.0,
            estimated_transfer_cost: 0.0,
            estimated_inventory_rebalance_cost: 0.0,
            estimated_latency_penalty_cost: 0.0,
            estimated_total_cost: 0.2,
            executable_notional: 100.0,
            quantity: 1.0,
            accepted,
            rejection_reason: None,
            rejection_detail: None,
            buy_book_age_ms: 1,
            sell_book_age_ms: 1,
            buy_book_source: BookSource::Websocket,
            sell_book_source: BookSource::Websocket,
            buy_latency_ms: Some(1),
            sell_latency_ms: Some(1),
        }
    }
}
