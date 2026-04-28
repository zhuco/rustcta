use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::core::types::{Kline, OrderSide};

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolPrecision {
    pub step_size: f64,
    pub tick_size: f64,
    pub min_notional: f64,
    pub min_order_size: f64,
}

#[derive(Debug, Clone)]
pub struct LiveShortPosition {
    pub average_entry_price: f64,
    pub quantity: f64,
    pub current_notional: f64,
    pub filled_layers: usize,
    pub next_layer_index: usize,
    pub last_layer_price: f64,
    pub atr_at_entry: f64,
    pub breakeven_armed: bool,
    pub trailing_take_profit_armed: bool,
    pub best_favorable_price: Option<f64>,
    pub opened_at: DateTime<Utc>,
    pub last_sync_at: DateTime<Utc>,
}

impl LiveShortPosition {
    pub fn unrealized_pnl(&self, reference_price: f64) -> f64 {
        (self.average_entry_price - reference_price) * self.quantity
    }

    pub fn is_losing(&self, reference_price: f64) -> bool {
        self.unrealized_pnl(reference_price) < 0.0
    }

    pub fn update_short_trailing_take_profit(
        &mut self,
        current_price: f64,
        activation_atr: f64,
        distance_atr: f64,
    ) -> Option<f64> {
        if self.atr_at_entry <= 0.0 || activation_atr <= 0.0 || distance_atr <= 0.0 {
            return None;
        }

        let activation_price = self.average_entry_price - activation_atr * self.atr_at_entry;
        if current_price <= activation_price {
            self.trailing_take_profit_armed = true;
            self.best_favorable_price = Some(
                self.best_favorable_price
                    .map(|best| best.min(current_price))
                    .unwrap_or(current_price),
            );
        }

        if !self.trailing_take_profit_armed {
            return None;
        }

        if let Some(best_price) = self.best_favorable_price {
            let stop_price = best_price + distance_atr * self.atr_at_entry;
            if current_price >= stop_price {
                return Some(stop_price);
            }
        }

        None
    }
}

#[derive(Debug, Clone)]
pub struct PendingInitialEntry {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub notional: f64,
    pub order_price: f64,
    pub reference_price: f64,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SymbolState {
    pub symbol: String,
    pub precision: Option<SymbolPrecision>,
    pub short: Option<LiveShortPosition>,
    pub last_klines: Vec<Kline>,
    pub last_order_at: Option<DateTime<Utc>>,
    pub pending_initial_entry: Option<PendingInitialEntry>,
    pub last_error: Option<String>,
}

impl SymbolState {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            precision: None,
            short: None,
            last_klines: Vec::new(),
            last_order_at: None,
            pending_initial_entry: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub symbols: HashMap<String, SymbolState>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdoptedShortProgress {
    pub current_notional: f64,
    pub filled_layers: usize,
    pub next_layer_index: usize,
    pub capped_at_max_notional: bool,
}

pub fn cumulative_layer_notionals(initial_notional: f64, layer_weights: &[f64]) -> Vec<f64> {
    let mut total = 0.0;
    layer_weights
        .iter()
        .map(|weight| {
            total += initial_notional * *weight;
            total
        })
        .collect()
}

pub fn adopt_short_progress(
    quantity: f64,
    average_entry_price: f64,
    initial_notional: f64,
    max_notional: f64,
    layer_weights: &[f64],
    tolerance_pct: f64,
) -> AdoptedShortProgress {
    let current_notional = (quantity.abs() * average_entry_price).max(0.0);
    let cumulative = cumulative_layer_notionals(initial_notional, layer_weights);
    let mut filled_layers = 0usize;
    let tolerance = tolerance_pct.max(0.0);

    for (index, target) in cumulative.iter().enumerate() {
        if current_notional <= *target * (1.0 + tolerance) {
            filled_layers = index + 1;
            break;
        }
    }

    if filled_layers == 0 && !cumulative.is_empty() {
        filled_layers = cumulative.len();
    }
    if current_notional <= initial_notional * (1.0 + tolerance) {
        filled_layers = filled_layers.max(1).min(cumulative.len().max(1));
    }

    let capped_at_max_notional = current_notional >= max_notional * (1.0 - tolerance);
    if capped_at_max_notional && !cumulative.is_empty() {
        filled_layers = cumulative.len();
    }

    let next_layer_index = filled_layers.min(layer_weights.len());
    AdoptedShortProgress {
        current_notional,
        filled_layers,
        next_layer_index,
        capped_at_max_notional,
    }
}

pub fn infer_short_ladder_last_price(
    average_entry_price: f64,
    atr_spacing: f64,
    layer_notionals: &[f64],
) -> f64 {
    if layer_notionals.len() <= 1 || atr_spacing <= 0.0 || average_entry_price <= 0.0 {
        return average_entry_price;
    }

    let mut low = (average_entry_price - atr_spacing * layer_notionals.len() as f64 * 4.0)
        .max(average_entry_price * 0.05)
        .max(1e-9);
    let high = average_entry_price;

    while implied_average(low, atr_spacing, layer_notionals) > average_entry_price && low > 1e-8 {
        low *= 0.5;
    }

    let mut left = low;
    let mut right = high;
    for _ in 0..80 {
        let mid = (left + right) * 0.5;
        if implied_average(mid, atr_spacing, layer_notionals) <= average_entry_price {
            left = mid;
        } else {
            right = mid;
        }
    }

    left + atr_spacing * (layer_notionals.len() as f64 - 1.0)
}

fn implied_average(first_price: f64, atr_spacing: f64, layer_notionals: &[f64]) -> f64 {
    let total_notional: f64 = layer_notionals.iter().sum();
    let total_qty: f64 = layer_notionals
        .iter()
        .enumerate()
        .map(|(index, notional)| notional / (first_price + atr_spacing * index as f64))
        .sum();
    if total_qty <= 0.0 {
        return first_price;
    }
    total_notional / total_qty
}

pub fn should_add_short_layer(
    position: &LiveShortPosition,
    current_price: f64,
    layer_count: usize,
    layer_spacing_atr: f64,
    allow_final_layer: bool,
) -> Option<(usize, f64)> {
    if !position.is_losing(current_price) || position.next_layer_index >= layer_count {
        return None;
    }
    let is_final_layer = position.next_layer_index + 1 == layer_count;
    if is_final_layer && !allow_final_layer {
        return None;
    }
    let trigger_price = position.last_layer_price + layer_spacing_atr * position.atr_at_entry;
    if current_price >= trigger_price {
        Some((position.next_layer_index, trigger_price))
    } else {
        None
    }
}

pub fn capped_layer_notional(
    initial_notional: f64,
    max_notional: f64,
    current_notional: f64,
    layer_weights: &[f64],
    layer_index: usize,
) -> Option<f64> {
    if layer_index >= layer_weights.len() || initial_notional <= 0.0 || max_notional <= 0.0 {
        return None;
    }
    let remaining = max_notional - current_notional.max(0.0);
    if remaining <= 0.0 {
        return None;
    }
    let planned = initial_notional * layer_weights[layer_index].max(0.0);
    let capped = planned.min(remaining);
    if capped > 0.0 {
        Some(capped)
    } else {
        None
    }
}

pub fn held_bars_since(
    opened_at: DateTime<Utc>,
    reference_time: DateTime<Utc>,
    interval_secs: u64,
) -> usize {
    if interval_secs == 0 || reference_time <= opened_at {
        return 0;
    }
    let elapsed = reference_time
        .signed_duration_since(opened_at)
        .num_seconds()
        .max(0) as u64;
    (elapsed / interval_secs) as usize
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

pub fn precision_round_up(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    if step <= 0.0 {
        return value;
    }
    (value / step).ceil() * step
}

pub fn matches_short_position_side(side: &str) -> bool {
    let normalized = side.trim().to_ascii_uppercase();
    normalized == "SHORT" || normalized == "SELL"
}

pub fn position_params(
    dual_position_mode: bool,
    side: OrderSide,
) -> Option<HashMap<String, String>> {
    if !dual_position_mode {
        return None;
    }
    let mut params = HashMap::new();
    params.insert(
        "positionSide".to_string(),
        match side {
            OrderSide::Buy => "LONG".to_string(),
            OrderSide::Sell => "SHORT".to_string(),
        },
    );
    Some(params)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn adopt_progress_maps_notional_to_ladder_layer() {
        let weights = vec![1.0, 1.0, 3.0, 5.0];
        let l1 = adopt_short_progress(2.0, 100.0, 200.0, 2000.0, &weights, 0.02);
        let l2 = adopt_short_progress(4.0, 100.0, 200.0, 2000.0, &weights, 0.02);
        let l3 = adopt_short_progress(7.2, 100.0, 200.0, 2000.0, &weights, 0.02);
        let l4 = adopt_short_progress(20.0, 100.0, 200.0, 2000.0, &weights, 0.02);

        assert_eq!(l1.filled_layers, 1);
        assert_eq!(l1.next_layer_index, 1);
        assert_eq!(l2.filled_layers, 2);
        assert_eq!(l2.next_layer_index, 2);
        assert_eq!(l3.filled_layers, 3);
        assert_eq!(l3.next_layer_index, 3);
        assert_eq!(l4.filled_layers, 4);
        assert_eq!(l4.next_layer_index, 4);
        assert!(l4.capped_at_max_notional);
    }

    #[test]
    fn infer_last_price_reconstructs_short_ladder_average() {
        let notionals = vec![200.0, 200.0, 600.0];
        let spacing = 1.5;
        let prices = [100.0, 101.5, 103.0];
        let total_notional: f64 = notionals.iter().sum();
        let qty: f64 = notionals
            .iter()
            .zip(prices.iter())
            .map(|(notional, price)| notional / price)
            .sum();
        let average = total_notional / qty;

        let last = infer_short_ladder_last_price(average, spacing, &notionals);
        assert!((last - 103.0).abs() < 1e-6);
    }

    #[test]
    fn add_layer_requires_short_position_loss() {
        let position = LiveShortPosition {
            average_entry_price: 100.0,
            quantity: 4.0,
            current_notional: 400.0,
            filled_layers: 2,
            next_layer_index: 2,
            last_layer_price: 101.0,
            atr_at_entry: 2.0,
            breakeven_armed: false,
            trailing_take_profit_armed: false,
            best_favorable_price: None,
            opened_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            last_sync_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        };

        assert_eq!(
            should_add_short_layer(&position, 100.5, 4, 0.55, true),
            None
        );
        assert_eq!(
            should_add_short_layer(&position, 101.5, 4, 0.55, true),
            None
        );
        assert_eq!(
            should_add_short_layer(&position, 102.1, 4, 0.55, true),
            Some((2, 102.1))
        );
    }

    #[test]
    fn capped_layer_notional_respects_symbol_max_notional() {
        let weights = vec![1.0, 1.0, 3.0, 5.0];

        assert_eq!(
            capped_layer_notional(200.0, 2000.0, 1000.0, &weights, 3),
            Some(1000.0)
        );
        assert_eq!(
            capped_layer_notional(200.0, 1800.0, 1000.0, &weights, 3),
            Some(800.0)
        );
        assert_eq!(
            capped_layer_notional(200.0, 2000.0, 2000.0, &weights, 3),
            None
        );
    }

    #[test]
    fn held_bars_since_floors_to_completed_decision_bars() {
        let opened_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let reference = Utc.with_ymd_and_hms(2026, 1, 1, 1, 4, 59).unwrap();

        assert_eq!(held_bars_since(opened_at, reference, 300), 12);
    }

    #[test]
    fn precision_round_up_respects_exchange_tick() {
        assert!((precision_round_up(10.001, 0.01) - 10.01).abs() < 1e-9);
        assert!((precision_round_up(10.0, 0.01) - 10.0).abs() < 1e-9);
    }

    #[test]
    fn short_trailing_take_profit_activates_then_exits_on_rebound() {
        let opened_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut position = LiveShortPosition {
            average_entry_price: 100.0,
            quantity: 2.0,
            current_notional: 200.0,
            filled_layers: 1,
            next_layer_index: 1,
            last_layer_price: 100.0,
            atr_at_entry: 2.0,
            breakeven_armed: false,
            trailing_take_profit_armed: false,
            best_favorable_price: None,
            opened_at,
            last_sync_at: opened_at,
        };

        assert_eq!(
            position.update_short_trailing_take_profit(98.0, 1.4, 1.4),
            None
        );
        assert_eq!(
            position.update_short_trailing_take_profit(97.0, 1.4, 1.4),
            None
        );
        assert!(position.trailing_take_profit_armed);
        assert_eq!(position.best_favorable_price, Some(97.0));

        assert_eq!(
            position.update_short_trailing_take_profit(96.0, 1.4, 1.4),
            None
        );
        assert_eq!(position.best_favorable_price, Some(96.0));
        assert_eq!(
            position.update_short_trailing_take_profit(98.9, 1.4, 1.4),
            Some(98.8)
        );
    }
}
