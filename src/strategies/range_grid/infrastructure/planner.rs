use anyhow::{anyhow, Result};

use crate::core::types::{OrderSide, OrderType};
use crate::utils::generate_order_id_with_tag;

use crate::strategies::range_grid::domain::config::{GridConfig, SymbolConfig};
use crate::strategies::range_grid::domain::model::{GridOrderPlan, GridPlan, PairPrecision};

/// 基于当前价格生成双向网格订单计划
pub fn build_grid_plan(
    symbol_cfg: &SymbolConfig,
    precision: &PairPrecision,
    center_price: f64,
) -> Result<GridPlan> {
    if center_price <= 0.0 {
        return Err(anyhow!("中心价格必须大于0"));
    }

    let grid_cfg = &symbol_cfg.grid;
    let spacing_ratio = grid_cfg.grid_spacing_pct / 100.0;
    let levels = grid_cfg.levels_per_side.max(1);

    let total_notional = grid_cfg.base_order_notional * levels as f64;
    let adj_base = if total_notional > grid_cfg.max_position_notional && total_notional > 0.0 {
        grid_cfg.max_position_notional / levels as f64
    } else {
        grid_cfg.base_order_notional
    };

    let mut orders = Vec::with_capacity(levels * 2);
    for level in 1..=levels {
        let step = spacing_ratio * level as f64;
        let buy_price = center_price * (1.0 - step);
        let sell_price = center_price * (1.0 + step);

        let buy_price = round_to_precision(buy_price, precision.price_digits, precision.price_step);
        let sell_price =
            round_to_precision(sell_price, precision.price_digits, precision.price_step);

        let buy_qty = quantize_amount(
            adj_base / buy_price,
            precision.amount_digits,
            precision.amount_step,
        );
        let sell_qty = quantize_amount(
            adj_base / sell_price,
            precision.amount_digits,
            precision.amount_step,
        );

        if let Some(min_notional) = precision.min_notional {
            if buy_price * buy_qty < min_notional {
                log::debug!(
                    "买单名义金额 {:.2} 低于最小值 {:.2}，跳过 level {}",
                    buy_price * buy_qty,
                    min_notional,
                    level
                );
            } else {
                orders.push(GridOrderPlan {
                    client_id: generate_order_id_with_tag(
                        "range_grid",
                        &symbol_cfg.account.exchange,
                        "B",
                    ),
                    price: buy_price,
                    quantity: buy_qty,
                    side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                });
            }

            if sell_price * sell_qty < min_notional {
                log::debug!(
                    "卖单名义金额 {:.2} 低于最小值 {:.2}，跳过 level {}",
                    sell_price * sell_qty,
                    min_notional,
                    level
                );
            } else {
                orders.push(GridOrderPlan {
                    client_id: generate_order_id_with_tag(
                        "range_grid",
                        &symbol_cfg.account.exchange,
                        "S",
                    ),
                    price: sell_price,
                    quantity: sell_qty,
                    side: OrderSide::Sell,
                    order_type: OrderType::Limit,
                });
            }
        } else {
            orders.push(GridOrderPlan {
                client_id: generate_order_id_with_tag(
                    "range_grid",
                    &symbol_cfg.account.exchange,
                    "B",
                ),
                price: buy_price,
                quantity: buy_qty,
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
            });
            orders.push(GridOrderPlan {
                client_id: generate_order_id_with_tag(
                    "range_grid",
                    &symbol_cfg.account.exchange,
                    "S",
                ),
                price: sell_price,
                quantity: sell_qty,
                side: OrderSide::Sell,
                order_type: OrderType::Limit,
            });
        }
    }

    Ok(GridPlan {
        center_price,
        orders,
    })
}

fn round_to_precision(value: f64, digits: u32, step: f64) -> f64 {
    let mut rounded = value;
    if step > 0.0 {
        let multiples = (rounded / step).round();
        rounded = multiples * step;
    }

    if digits == 0 {
        return rounded.round();
    }

    let factor = 10_f64.powi(digits as i32);
    (rounded * factor).round() / factor
}

fn quantize_amount(value: f64, digits: u32, step: f64) -> f64 {
    let mut amount = round_to_precision(value, digits, step);
    if amount <= 0.0 && step > 0.0 {
        amount = step;
    }
    amount
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::range_grid::domain::config::{
        AccountConfig, GridConfig, MarketExitConfig, OrderConfig, RegimeFilterConfig, SymbolConfig,
        SymbolPrecision,
    };

    fn sample_symbol() -> SymbolConfig {
        SymbolConfig {
            config_id: "TEST".to_string(),
            enabled: true,
            account: AccountConfig {
                id: "acc".to_string(),
                exchange: "binance".to_string(),
                env_prefix: "BINANCE".to_string(),
            },
            symbol: "BTCUSDT".to_string(),
            precision: SymbolPrecision {
                price_digits: Some(2),
                amount_digits: Some(3),
            },
            grid: GridConfig {
                grid_spacing_pct: 0.5,
                levels_per_side: 2,
                base_order_notional: 100.0,
                max_position_notional: 400.0,
            },
            regime_filters: RegimeFilterConfig {
                min_liquidity_usd: 1000000.0,
                require_conditions: 2,
                adx_max: 20.0,
                bbw_quantile: 0.4,
                slope_threshold: 0.2,
            },
            order: OrderConfig {
                post_only: true,
                tif: Some("GTC".to_string()),
            },
            market_exit: MarketExitConfig {
                use_market_order: true,
            },
        }
    }

    #[test]
    fn test_build_grid_plan_generates_levels() {
        let symbol = sample_symbol();
        let precision = PairPrecision {
            price_digits: 2,
            amount_digits: 3,
            price_step: 0.01,
            amount_step: 0.001,
            min_notional: Some(5.0),
        };

        let plan = build_grid_plan(&symbol, &precision, 50000.0).unwrap();
        assert_eq!(plan.orders.len(), 4);

        let buy_orders: Vec<_> = plan
            .orders
            .iter()
            .filter(|o| o.side == OrderSide::Buy)
            .collect();
        let sell_orders: Vec<_> = plan
            .orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .collect();

        assert_eq!(buy_orders.len(), 2);
        assert_eq!(sell_orders.len(), 2);

        assert!(buy_orders[0].price < 50000.0);
        assert!(sell_orders[0].price > 50000.0);

        let qty = buy_orders[0].quantity;
        assert!(qty > 0.0);
        assert!((qty * buy_orders[0].price) >= 5.0);
    }
}
