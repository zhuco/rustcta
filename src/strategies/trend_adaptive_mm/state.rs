use crate::core::types::OrderSide;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub struct SymbolMetadata {
    pub display_symbol: String,
    pub api_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_order_size: f64,
    pub min_notional: f64,
    pub price_precision: u32,
    pub quantity_precision: u32,
}

impl SymbolMetadata {
    pub fn min_notional(&self) -> f64 {
        self.min_notional
    }
}

#[derive(Debug, Clone)]
pub struct ActiveOrder {
    pub order_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub filled: f64,
    pub placed_at: DateTime<Utc>,
    pub level: usize,
}

impl ActiveOrder {
    pub fn new(order_id: String, side: OrderSide, price: f64, quantity: f64, level: usize) -> Self {
        Self {
            order_id,
            side,
            price,
            quantity,
            filled: 0.0,
            placed_at: Utc::now(),
            level,
        }
    }
}

#[derive(Debug, Default)]
pub struct StrategyState {
    pub inventory: f64,
    pub target_inventory: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub max_drawdown_pct: f64,
    pub active_buy: BTreeMap<usize, ActiveOrder>,
    pub active_sell: BTreeMap<usize, ActiveOrder>,
    pub order_lookup: HashMap<String, (OrderSide, usize)>,
    pub last_quote_time: Option<DateTime<Utc>>,
    pub last_alpha: f64,
    pub last_volatility: f64,
    pub last_risk_action: Option<String>,
    pub paused: bool,
    pub last_inventory_sync: Option<DateTime<Utc>>,
    pub last_order_place_buy: Option<DateTime<Utc>>,
    pub last_order_place_sell: Option<DateTime<Utc>>,
    pub post_only_penalty_buy: u32,
    pub post_only_penalty_sell: u32,
    pub last_post_only_reject_buy: Option<DateTime<Utc>>,
    pub last_post_only_reject_sell: Option<DateTime<Utc>>,
}

impl StrategyState {
    pub fn snapshot(&self) -> StrategyStateSnapshot {
        StrategyStateSnapshot {
            inventory: self.inventory,
            target_inventory: self.target_inventory,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: self.unrealized_pnl,
            max_drawdown_pct: self.max_drawdown_pct,
        }
    }

    pub fn update_inventory(&mut self, value: f64) {
        self.inventory = value;
        self.last_inventory_sync = Some(Utc::now());
    }
}

#[derive(Debug, Clone)]
pub struct StrategyStateSnapshot {
    pub inventory: f64,
    pub target_inventory: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub max_drawdown_pct: f64,
}

pub fn precision_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 0;
    }

    let formatted = format!("{:.16}", step);
    if let Some(idx) = formatted.find('.') {
        let decimals = formatted[idx + 1..].trim_end_matches('0').len() as u32;
        decimals
    } else {
        0
    }
}

pub fn round_price_for_side(price: f64, tick_size: f64, side: OrderSide, precision: u32) -> f64 {
    if tick_size <= 0.0 {
        return round_to_precision(price, precision);
    }

    let tick_dec = Decimal::from_f64(tick_size).unwrap_or(Decimal::ZERO);
    let price_dec = Decimal::from_f64(price).unwrap_or(Decimal::ZERO);

    if tick_dec.is_zero() {
        return round_to_precision(price, precision);
    }

    let ratio = price_dec / tick_dec;
    let rounded_ratio = match side {
        OrderSide::Buy => ratio.floor(),
        OrderSide::Sell => ratio.ceil(),
    };
    let rounded = rounded_ratio * tick_dec;
    rounded
        .round_dp(precision)
        .to_f64()
        .unwrap_or_else(|| round_to_precision(price, precision))
}

pub fn round_quantity(quantity: f64, step: f64, precision: u32) -> f64 {
    if step <= 0.0 {
        return round_to_precision(quantity, precision);
    }

    let step_dec = Decimal::from_f64(step).unwrap_or(Decimal::ZERO);
    let qty_dec = Decimal::from_f64(quantity).unwrap_or(Decimal::ZERO);

    if step_dec.is_zero() {
        return round_to_precision(quantity, precision);
    }

    let ratio = (qty_dec / step_dec).floor();
    let rounded = ratio * step_dec;
    rounded
        .round_dp(precision)
        .to_f64()
        .unwrap_or_else(|| round_to_precision(quantity, precision))
}

pub fn ceil_quantity(quantity: f64, step: f64, precision: u32) -> f64 {
    if step <= 0.0 {
        return round_to_precision(quantity, precision);
    }

    let step_dec = Decimal::from_f64(step).unwrap_or(Decimal::ZERO);
    let qty_dec = Decimal::from_f64(quantity).unwrap_or(Decimal::ZERO);

    if step_dec.is_zero() {
        return round_to_precision(quantity, precision);
    }

    let ratio = (qty_dec / step_dec).ceil();
    let rounded = ratio * step_dec;
    rounded
        .round_dp(precision)
        .to_f64()
        .unwrap_or_else(|| round_to_precision(quantity, precision))
}

fn round_to_precision(value: f64, precision: u32) -> f64 {
    Decimal::from_f64(value)
        .unwrap_or(Decimal::ZERO)
        .round_dp(precision)
        .to_f64()
        .unwrap_or(value)
}
