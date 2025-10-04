use chrono::{DateTime, Utc};

use crate::core::types::OrderSide;

#[derive(Default)]
pub(super) struct CancelRateLimiter {
    queue: std::collections::VecDeque<DateTime<Utc>>,
    max_per_minute: u32,
}

impl CancelRateLimiter {
    pub fn new(max_per_minute: u32) -> Self {
        Self {
            queue: std::collections::VecDeque::new(),
            max_per_minute,
        }
    }

    pub fn allow(&mut self) -> bool {
        let now = Utc::now();
        while let Some(front) = self.queue.front() {
            if (now - *front).num_seconds() > 60 {
                self.queue.pop_front();
            } else {
                break;
            }
        }
        if self.queue.len() < self.max_per_minute as usize {
            self.queue.push_back(now);
            true
        } else {
            false
        }
    }
}

fn trim_to_precision(value: f64, precision: u32) -> f64 {
    if precision == 0 {
        return value.round();
    }
    let factor = 10_f64.powi(precision as i32);
    (value * factor).round() / factor
}

fn quantize_with_mode(value: f64, step: f64, precision: u32, mode: fn(f64) -> f64) -> f64 {
    if step <= 0.0 {
        return trim_to_precision(value, precision);
    }
    let multiples = mode(value / step);
    let quantized = multiples * step;
    trim_to_precision(quantized, precision)
}

pub(super) fn precision_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 0;
    }

    let mut precision = 0;
    let mut scaled = step;
    while precision < 12 {
        if (scaled.round() - scaled).abs() < 1e-9 {
            break;
        }
        scaled *= 10.0;
        precision += 1;
    }
    precision
}

pub(super) fn normalize_to_step(value: f64, step: f64, precision: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if step <= 0.0 {
        return trim_to_precision(value, precision);
    }
    quantize_with_mode(value, step, precision, f64::round)
}

pub(super) fn ceil_to_step(value: f64, step: f64, precision: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if step <= 0.0 {
        return trim_to_precision(value, precision);
    }
    quantize_with_mode(value, step, precision, f64::ceil)
}

pub(super) fn round_price(
    price: f64,
    tick: f64,
    precision: u32,
    side: OrderSide,
) -> anyhow::Result<f64> {
    if !price.is_finite() {
        return Err(anyhow::anyhow!("invalid price"));
    }

    if tick <= 0.0 {
        return Ok(price);
    }

    let rounded = match side {
        OrderSide::Buy => quantize_with_mode(price, tick, precision, f64::floor),
        OrderSide::Sell => quantize_with_mode(price, tick, precision, f64::ceil),
    };

    if rounded <= 0.0 {
        Err(anyhow::anyhow!("invalid rounded price"))
    } else {
        Ok(rounded)
    }
}

pub(super) fn round_quantity(quantity: f64, step: f64, precision: u32) -> anyhow::Result<f64> {
    if !quantity.is_finite() {
        return Err(anyhow::anyhow!("invalid quantity"));
    }

    if step <= 0.0 {
        return Ok(quantity);
    }

    let adjusted = quantize_with_mode(quantity, step, precision, f64::floor);
    if adjusted <= 0.0 {
        Err(anyhow::anyhow!("invalid quantity after rounding"))
    } else {
        Ok(adjusted)
    }
}

pub(super) fn normalize_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_uppercase()
}
