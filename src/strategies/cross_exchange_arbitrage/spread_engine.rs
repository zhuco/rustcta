//! Executable spread calculation for detection-only spot arbitrage.

use chrono::{DateTime, Utc};

use super::config::CrossExchangeArbitrageConfig;
use super::fees::{FeeModel, FeeRole};
use super::market_data::MarketDataBookStore;
use super::types::{NormalizedDepthSnapshot, OpportunityDecision, OpportunityRecord};
use crate::market::{CanonicalSymbol, ExchangeId};

#[derive(Debug, Clone)]
pub struct SpreadEngine {
    fee_model: FeeModel,
    stale_book_ms: i64,
    estimated_slippage_bps: f64,
    safety_buffer_bps: f64,
    min_net_spread_bps: f64,
    estimated_notional_usdt: f64,
}

impl SpreadEngine {
    pub fn from_config(config: &CrossExchangeArbitrageConfig) -> Self {
        Self {
            fee_model: FeeModel::from_config(&config.fees),
            stale_book_ms: config.detection.stale_book_ms,
            estimated_slippage_bps: config.detection.estimated_slippage_bps,
            safety_buffer_bps: config.detection.safety_buffer_bps,
            min_net_spread_bps: config.detection.min_net_spread_bps,
            estimated_notional_usdt: config.detection.estimated_notional_usdt,
        }
    }

    pub fn evaluate(
        &self,
        buy_book: &NormalizedDepthSnapshot,
        sell_book: &NormalizedDepthSnapshot,
        now: DateTime<Utc>,
    ) -> OpportunityRecord {
        if buy_book.symbol != sell_book.symbol {
            return rejected_record(
                now,
                buy_book,
                sell_book,
                0.0,
                0.0,
                self.estimated_notional_usdt,
                "symbol_mismatch",
            );
        }
        if !buy_book.is_usable(now, self.stale_book_ms)
            || !sell_book.is_usable(now, self.stale_book_ms)
        {
            return rejected_record(
                now,
                buy_book,
                sell_book,
                0.0,
                0.0,
                self.estimated_notional_usdt,
                "stale_or_unusable_order_book",
            );
        }

        let buy_ask = buy_book.best_ask().expect("checked usable book");
        let sell_bid = sell_book.best_bid().expect("checked usable book");
        let estimated_notional = self
            .estimated_notional_usdt
            .min(buy_ask.price * buy_ask.quantity)
            .min(sell_bid.price * sell_bid.quantity);
        let raw_spread_bps = ((sell_bid.price - buy_ask.price) / buy_ask.price) * 10_000.0;
        let fee_bps = self.fee_model.rate(&buy_book.exchange, FeeRole::Taker) * 10_000.0
            + self.fee_model.rate(&sell_book.exchange, FeeRole::Maker) * 10_000.0;
        let estimated_net_spread_bps =
            raw_spread_bps - fee_bps - self.estimated_slippage_bps - self.safety_buffer_bps;

        let (decision, reason) = if estimated_net_spread_bps >= self.min_net_spread_bps {
            (OpportunityDecision::Accepted, "accepted")
        } else {
            (OpportunityDecision::Rejected, "below_min_net_spread")
        };

        OpportunityRecord {
            timestamp: now,
            symbol: buy_book.symbol.clone(),
            buy_exchange: buy_book.exchange.clone(),
            sell_exchange: sell_book.exchange.clone(),
            buy_price: buy_ask.price,
            sell_price: sell_bid.price,
            raw_spread_bps,
            estimated_net_spread_bps,
            estimated_notional,
            decision,
            reason: reason.to_string(),
        }
    }

    pub fn scan_symbol(
        &self,
        store: &MarketDataBookStore,
        symbol: &CanonicalSymbol,
        exchanges: &[ExchangeId],
        now: DateTime<Utc>,
    ) -> Vec<OpportunityRecord> {
        let mut records = Vec::new();
        for buy_exchange in exchanges {
            for sell_exchange in exchanges {
                if buy_exchange == sell_exchange {
                    continue;
                }
                let Some(buy_book) = store.get(buy_exchange, symbol) else {
                    continue;
                };
                let Some(sell_book) = store.get(sell_exchange, symbol) else {
                    continue;
                };
                records.push(self.evaluate(buy_book, sell_book, now));
            }
        }
        records
    }
}

fn rejected_record(
    now: DateTime<Utc>,
    buy_book: &NormalizedDepthSnapshot,
    sell_book: &NormalizedDepthSnapshot,
    raw_spread_bps: f64,
    estimated_net_spread_bps: f64,
    estimated_notional: f64,
    reason: &str,
) -> OpportunityRecord {
    OpportunityRecord {
        timestamp: now,
        symbol: buy_book.symbol.clone(),
        buy_exchange: buy_book.exchange.clone(),
        sell_exchange: sell_book.exchange.clone(),
        buy_price: buy_book.best_ask().map(|level| level.price).unwrap_or(0.0),
        sell_price: sell_book.best_bid().map(|level| level.price).unwrap_or(0.0),
        raw_spread_bps,
        estimated_net_spread_bps,
        estimated_notional,
        decision: OpportunityDecision::Rejected,
        reason: reason.to_string(),
    }
}
