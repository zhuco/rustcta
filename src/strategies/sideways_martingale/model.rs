use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::types::{Interval, Kline, OrderSide};

use super::config::{MartingaleConfig, SymbolConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolPrecision {
    pub step_size: f64,
    pub tick_size: f64,
    pub min_notional: f64,
    pub min_order_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerFill {
    pub layer_index: usize,
    pub multiplier: f64,
    pub quantity: f64,
    pub price: f64,
    pub notional: f64,
    pub filled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidePositionState {
    pub side: OrderSide,
    pub layers: Vec<LayerFill>,
    pub total_quantity: f64,
    pub total_cost: f64,
    pub average_price: f64,
    pub last_entry_price: f64,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SidePositionState {
    pub fn new(side: OrderSide, quantity: f64, price: f64, multiplier: f64) -> Self {
        let now = Utc::now();
        let notional = quantity * price;
        Self {
            side,
            layers: vec![LayerFill {
                layer_index: 0,
                multiplier,
                quantity,
                price,
                notional,
                filled_at: now,
            }],
            total_quantity: quantity,
            total_cost: quantity * price,
            average_price: price,
            last_entry_price: price,
            opened_at: now,
            updated_at: now,
        }
    }

    pub fn is_open(&self) -> bool {
        self.total_quantity > 1e-9
    }

    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    pub fn next_layer_index(&self) -> usize {
        self.layers.len()
    }

    pub fn apply_fill(&mut self, quantity: f64, price: f64, multiplier: f64) {
        let now = Utc::now();
        self.total_quantity += quantity;
        self.total_cost += quantity * price;
        self.average_price = if self.total_quantity > 0.0 {
            self.total_cost / self.total_quantity
        } else {
            0.0
        };
        self.last_entry_price = price;
        self.updated_at = now;
        self.layers.push(LayerFill {
            layer_index: self.layers.len(),
            multiplier,
            quantity,
            price,
            notional: quantity * price,
            filled_at: now,
        });
    }

    pub fn unrealized_pnl_pct(&self, current_price: f64) -> f64 {
        if self.average_price <= 0.0 {
            return 0.0;
        }
        match self.side {
            OrderSide::Buy => (current_price - self.average_price) / self.average_price,
            OrderSide::Sell => (self.average_price - current_price) / self.average_price,
        }
    }

    pub fn take_profit_price(&self, target_pct: f64) -> f64 {
        match self.side {
            OrderSide::Buy => self.average_price * (1.0 + target_pct),
            OrderSide::Sell => self.average_price * (1.0 - target_pct),
        }
    }

    pub fn next_add_price(&self, adverse_move_pct: f64) -> Option<f64> {
        if self.layer_count() == 0 {
            return None;
        }
        Some(match self.side {
            OrderSide::Buy => self.last_entry_price * (1.0 - adverse_move_pct),
            OrderSide::Sell => self.last_entry_price * (1.0 + adverse_move_pct),
        })
    }

    pub fn should_add_layer(
        &self,
        current_price: f64,
        martingale: &MartingaleConfig,
        max_layers: usize,
    ) -> bool {
        if self.layer_count() >= max_layers {
            return false;
        }
        let Some(trigger) = self.next_add_price(martingale.adverse_move_pct) else {
            return false;
        };
        match self.side {
            OrderSide::Buy => current_price <= trigger,
            OrderSide::Sell => current_price >= trigger,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolState {
    pub config: SymbolConfig,
    pub precision: Option<SymbolPrecision>,
    pub last_klines: Vec<Kline>,
    pub last_scan_at: Option<DateTime<Utc>>,
    pub last_signal_at: Option<DateTime<Utc>>,
    pub entry_pause_until: Option<DateTime<Utc>>,
    pub pause_reason: Option<String>,
    pub realized_pnl: f64,
    pub last_close_reason: Option<String>,
    pub long: Option<SidePositionState>,
    pub short: Option<SidePositionState>,
}

impl SymbolState {
    pub fn new(config: SymbolConfig) -> Self {
        Self {
            config,
            precision: None,
            last_klines: Vec::new(),
            last_scan_at: None,
            last_signal_at: None,
            entry_pause_until: None,
            pause_reason: None,
            realized_pnl: 0.0,
            last_close_reason: None,
            long: None,
            short: None,
        }
    }

    pub fn side(&self, side: OrderSide) -> Option<&SidePositionState> {
        match side {
            OrderSide::Buy => self.long.as_ref(),
            OrderSide::Sell => self.short.as_ref(),
        }
    }

    pub fn side_mut(&mut self, side: OrderSide) -> Option<&mut SidePositionState> {
        match side {
            OrderSide::Buy => self.long.as_mut(),
            OrderSide::Sell => self.short.as_mut(),
        }
    }

    pub fn set_side(&mut self, position: SidePositionState) {
        match position.side {
            OrderSide::Buy => self.long = Some(position),
            OrderSide::Sell => self.short = Some(position),
        }
    }

    pub fn clear_side(&mut self, side: OrderSide) {
        match side {
            OrderSide::Buy => self.long = None,
            OrderSide::Sell => self.short = None,
        }
    }

    pub fn can_open_again(&self, now: DateTime<Utc>, cooldown_secs: u64) -> bool {
        match self.last_signal_at {
            Some(last) => (now - last).num_seconds() >= cooldown_secs as i64,
            None => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndicatorSnapshot {
    pub current_price: f64,
    pub ema20: f64,
    pub rsi14: f64,
    pub atr14: f64,
    pub adx14: f64,
    pub atr_ratio: f64,
    pub amplitude_20: f64,
    pub latest_bar_change: f64,
    pub last_close_time: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub symbols: HashMap<String, SymbolState>,
    pub consecutive_stopouts: u32,
    pub trading_paused_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

pub fn parse_interval(interval: &str) -> Interval {
    Interval::from_string(interval).unwrap_or(Interval::FiveMinutes)
}

pub fn interval_duration_secs(interval: Interval) -> i64 {
    match interval {
        Interval::OneMinute => 60,
        Interval::ThreeMinutes => 180,
        Interval::FiveMinutes => 300,
        Interval::FifteenMinutes => 900,
        Interval::ThirtyMinutes => 1800,
        Interval::OneHour => 3600,
        Interval::TwoHours => 7200,
        Interval::FourHours => 14400,
        Interval::SixHours => 21600,
        Interval::EightHours => 28800,
        Interval::TwelveHours => 43200,
        Interval::OneDay => 86400,
        Interval::ThreeDays => 259200,
        Interval::OneWeek => 604800,
        Interval::OneMonth => 2592000,
    }
}

pub fn precision_round_down(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    if step <= 0.0 {
        return value;
    }
    (value / step).floor() * step
}

pub fn latest_bar_change_pct(bar: &Kline) -> f64 {
    if bar.open.abs() < 1e-9 {
        return 0.0;
    }
    (bar.close - bar.open).abs() / bar.open.abs()
}

pub fn amplitude_pct(klines: &[Kline]) -> f64 {
    if klines.is_empty() {
        return 0.0;
    }
    let highest = klines.iter().map(|k| k.high).fold(f64::MIN, f64::max);
    let lowest = klines.iter().map(|k| k.low).fold(f64::MAX, f64::min);
    let base = klines.last().map(|k| k.close.abs()).unwrap_or(0.0);
    if base < 1e-9 {
        return 0.0;
    }
    (highest - lowest) / base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn martingale_trigger_for_long_uses_last_entry_price() {
        let mut state = SidePositionState::new(OrderSide::Buy, 1.0, 100.0, 1.0);
        state.apply_fill(1.5, 98.8, 1.5);
        let cfg = MartingaleConfig {
            layer_multipliers: vec![1.0, 1.5, 2.0, 2.5],
            adverse_move_pct: 0.012,
        };
        assert!(state.should_add_layer(97.6, &cfg, 4));
        assert!(!state.should_add_layer(98.0, &cfg, 4));
    }

    #[test]
    fn take_profit_price_matches_side() {
        let long = SidePositionState::new(OrderSide::Buy, 1.0, 100.0, 1.0);
        let short = SidePositionState::new(OrderSide::Sell, 1.0, 100.0, 1.0);
        assert_eq!(long.take_profit_price(0.006), 100.6);
        assert_eq!(short.take_profit_price(0.006), 99.4);
    }

    #[test]
    fn precision_round_down_respects_step() {
        assert!((precision_round_down(1.239, 0.01) - 1.23).abs() < 1e-9);
        assert!((precision_round_down(0.009, 0.01) - 0.0).abs() < 1e-9);
    }
}
