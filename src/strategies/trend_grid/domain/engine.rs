//! 核心算法逻辑模块
//!
//! 负责维护策略的纯算法部分，避免在业务层持有繁重的状态锁。

use super::config::{GridConfig, SpacingType, TradingConfig, TrendAdjustment};
use super::state::TrendStrength;
use crate::core::types::OrderSide;

/// 生成网格订单时所需的上下文快照
#[derive(Debug, Clone, Copy)]
pub struct GridContext {
    pub current_price: f64,
    pub price_precision: u32,
    pub amount_precision: u32,
    pub trend_strength: TrendStrength,
}

/// 网格下单计划
#[derive(Debug, Clone, Copy)]
pub struct GridOrderPlan {
    pub price: f64,
    pub quantity: f64,
    pub side: OrderSide,
}

pub struct TrendGridEngine;

impl TrendGridEngine {
    pub fn new() -> Self {
        Self
    }

    /// 依据配置和当前状态生成网格下单计划
    pub fn build_grid_orders(
        &self,
        config: &TradingConfig,
        context: GridContext,
        trend_adjustment: &TrendAdjustment,
    ) -> Vec<GridOrderPlan> {
        let GridContext {
            current_price,
            price_precision,
            amount_precision,
            trend_strength,
        } = context;

        let grid = &config.grid;
        let spacing = grid.spacing;
        let order_amount = grid.order_amount;
        let orders_per_side = grid.orders_per_side;

        let (buy_multiplier, sell_multiplier) =
            Self::trend_multipliers(trend_strength, trend_adjustment);

        let mut plans = Vec::with_capacity((orders_per_side * 2) as usize);

        for i in 1..=orders_per_side {
            let price =
                Self::calculate_price(current_price, spacing, &grid.spacing_type, i as i32, false);
            let rounded_price = Self::round_price(price, price_precision);
            let notional = order_amount * buy_multiplier;
            if let Some(quantity) = Self::derive_quantity(rounded_price, notional, amount_precision)
            {
                plans.push(GridOrderPlan {
                    price: rounded_price,
                    quantity,
                    side: OrderSide::Buy,
                });
            }
        }

        for i in 1..=orders_per_side {
            let price =
                Self::calculate_price(current_price, spacing, &grid.spacing_type, i as i32, true);
            let rounded_price = Self::round_price(price, price_precision);
            let notional = order_amount * sell_multiplier;
            if let Some(quantity) = Self::derive_quantity(rounded_price, notional, amount_precision)
            {
                plans.push(GridOrderPlan {
                    price: rounded_price,
                    quantity,
                    side: OrderSide::Sell,
                });
            }
        }

        plans
    }

    fn trend_multipliers(strength: TrendStrength, adjustment: &TrendAdjustment) -> (f64, f64) {
        match strength {
            TrendStrength::StrongBull => (adjustment.strong_bull_buy_multiplier, 1.0),
            TrendStrength::Bull => (adjustment.bull_buy_multiplier, 1.0),
            TrendStrength::Bear => (1.0, adjustment.bear_sell_multiplier),
            TrendStrength::StrongBear => (1.0, adjustment.strong_bear_sell_multiplier),
            TrendStrength::Neutral => (1.0, 1.0),
        }
    }

    fn calculate_price(
        reference_price: f64,
        spacing: f64,
        spacing_type: &SpacingType,
        index: i32,
        is_sell_side: bool,
    ) -> f64 {
        match spacing_type {
            SpacingType::Geometric => {
                if is_sell_side {
                    reference_price * (1.0 + spacing).powi(index)
                } else {
                    (reference_price * (1.0 - spacing).powi(index)).max(0.0)
                }
            }
            SpacingType::Arithmetic => {
                let offset = spacing * index as f64;
                if is_sell_side {
                    reference_price + offset
                } else {
                    (reference_price - offset).max(0.0)
                }
            }
        }
    }

    fn derive_quantity(price: f64, notional: f64, amount_precision: u32) -> Option<f64> {
        if price <= f64::EPSILON {
            return None;
        }

        let raw_amount = notional / price;
        let quantity = if amount_precision == 0 {
            raw_amount.round()
        } else {
            Self::round_amount(raw_amount, amount_precision)
        };

        if quantity * price >= 5.0 {
            Some(quantity)
        } else {
            None
        }
    }

    pub fn round_price(price: f64, precision: u32) -> f64 {
        let multiplier = 10_f64.powi(precision as i32);
        (price * multiplier).round() / multiplier
    }

    pub fn round_amount(amount: f64, precision: u32) -> f64 {
        let multiplier = 10_f64.powi(precision as i32);
        (amount * multiplier).round() / multiplier
    }

    pub fn precision_from_step(step: f64) -> u32 {
        if step == 0.0 {
            return 8;
        }

        let s = format!("{:.10}", step);
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 1 {
            parts[1].trim_end_matches('0').len() as u32
        } else {
            0
        }
    }
}
