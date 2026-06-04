use crate::smart_money::Direction;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub ts: DateTime<Utc>,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

impl OrderBookSnapshot {
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|level| level.price)
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|level| level.price)
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        Some((self.best_bid()? + self.best_ask()?) / Decimal::new(2, 0))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakerExecutionConfig {
    pub taker_fee_rate: Decimal,
    pub max_depth_levels: usize,
    pub max_participation_notional_usdt: Option<Decimal>,
}

impl Default for TakerExecutionConfig {
    fn default() -> Self {
        Self {
            taker_fee_rate: Decimal::new(4, 4),
            max_depth_levels: 100,
            max_participation_notional_usdt: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimulatedFill {
    pub symbol: String,
    pub direction: Direction,
    pub requested_notional_usdt: Decimal,
    pub filled_notional_usdt: Decimal,
    pub filled_quantity: Decimal,
    pub average_price: Option<Decimal>,
    pub fee_usdt: Decimal,
    pub slippage_bps: Decimal,
    pub levels_consumed: usize,
    pub partial_fill: bool,
    pub filled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionSimError {
    InvalidNotional,
    InvalidBook,
    EmptySide,
}

pub fn simulate_taker_market_order(
    book: &OrderBookSnapshot,
    direction: Direction,
    requested_notional_usdt: Decimal,
    config: &TakerExecutionConfig,
) -> Result<SimulatedFill, ExecutionSimError> {
    if requested_notional_usdt <= Decimal::ZERO || direction == Direction::Flat {
        return Err(ExecutionSimError::InvalidNotional);
    }
    let arrival_mid = book.mid_price().ok_or(ExecutionSimError::InvalidBook)?;
    if arrival_mid <= Decimal::ZERO {
        return Err(ExecutionSimError::InvalidBook);
    }
    let max_notional = config
        .max_participation_notional_usdt
        .unwrap_or(requested_notional_usdt)
        .min(requested_notional_usdt);
    let side = match direction {
        Direction::Long => &book.asks,
        Direction::Short => &book.bids,
        Direction::Flat => return Err(ExecutionSimError::InvalidNotional),
    };
    if side.is_empty() {
        return Err(ExecutionSimError::EmptySide);
    }

    let mut remaining_notional = max_notional;
    let mut filled_notional = Decimal::ZERO;
    let mut filled_quantity = Decimal::ZERO;
    let mut levels_consumed = 0usize;

    for level in side.iter().take(config.max_depth_levels) {
        if remaining_notional <= Decimal::ZERO {
            break;
        }
        if level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO {
            continue;
        }
        let level_notional = level.price * level.quantity;
        let consume_notional = level_notional.min(remaining_notional);
        let consume_qty = consume_notional / level.price;
        filled_notional += consume_notional;
        filled_quantity += consume_qty;
        remaining_notional -= consume_notional;
        levels_consumed += 1;
    }

    if filled_quantity <= Decimal::ZERO {
        return Err(ExecutionSimError::EmptySide);
    }

    let average_price = filled_notional / filled_quantity;
    let fee_usdt = filled_notional * config.taker_fee_rate;
    let slippage_bps = match direction {
        Direction::Long => (average_price / arrival_mid - Decimal::ONE) * Decimal::new(10000, 0),
        Direction::Short => (Decimal::ONE - average_price / arrival_mid) * Decimal::new(10000, 0),
        Direction::Flat => Decimal::ZERO,
    };

    Ok(SimulatedFill {
        symbol: book.symbol.clone(),
        direction,
        requested_notional_usdt,
        filled_notional_usdt: filled_notional,
        filled_quantity,
        average_price: Some(average_price),
        fee_usdt,
        slippage_bps,
        levels_consumed,
        partial_fill: filled_notional < requested_notional_usdt,
        filled_at: book.ts,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NavState {
    pub initial_capital_usdt: Decimal,
    pub realized_pnl_usdt: Decimal,
    pub unrealized_pnl_usdt: Decimal,
    pub funding_pnl_usdt: Decimal,
    pub fees_paid_usdt: Decimal,
}

impl NavState {
    pub fn nav_usdt(&self) -> Decimal {
        self.initial_capital_usdt
            + self.realized_pnl_usdt
            + self.unrealized_pnl_usdt
            + self.funding_pnl_usdt
            - self.fees_paid_usdt
    }

    pub fn apply_fill_fee(&mut self, fill: &SimulatedFill) {
        self.fees_paid_usdt += fill.fee_usdt;
    }
}
