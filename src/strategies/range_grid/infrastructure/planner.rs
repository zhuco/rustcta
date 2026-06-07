use anyhow::Result;

use crate::core::types::{OrderSide, OrderType};
use crate::strategies::range_grid::domain::config::SymbolConfig;
use crate::strategies::range_grid::domain::model::{GridOrderPlan, GridPlan, PairPrecision};

/// 基于当前价格生成双向网格订单计划
pub fn build_grid_plan(
    symbol_cfg: &SymbolConfig,
    precision: &PairPrecision,
    center_price: f64,
) -> Result<GridPlan> {
    let core_plan = rustcta_strategy_range_grid::build_grid_plan(
        &core_symbol_config(symbol_cfg),
        &core_precision(precision),
        center_price,
    )?;
    Ok(GridPlan {
        center_price: core_plan.center_price,
        orders: core_plan
            .orders
            .into_iter()
            .map(grid_order_from_core)
            .collect(),
    })
}

fn core_symbol_config(symbol: &SymbolConfig) -> rustcta_strategy_range_grid::SymbolConfig {
    rustcta_strategy_range_grid::SymbolConfig {
        config_id: symbol.config_id.clone(),
        enabled: symbol.enabled,
        account: rustcta_strategy_range_grid::AccountConfig {
            id: symbol.account.id.clone(),
            exchange: symbol.account.exchange.clone(),
            env_prefix: symbol.account.env_prefix.clone(),
        },
        symbol: symbol.symbol.clone(),
        precision: rustcta_strategy_range_grid::SymbolPrecision {
            price_digits: symbol.precision.price_digits,
            amount_digits: symbol.precision.amount_digits,
        },
        grid: rustcta_strategy_range_grid::GridConfig {
            grid_spacing_pct: symbol.grid.grid_spacing_pct,
            levels_per_side: symbol.grid.levels_per_side,
            base_order_notional: symbol.grid.base_order_notional,
            max_position_notional: symbol.grid.max_position_notional,
        },
        regime_filters: rustcta_strategy_range_grid::RegimeFilterConfig {
            min_liquidity_usd: symbol.regime_filters.min_liquidity_usd,
            require_conditions: symbol.regime_filters.require_conditions,
            adx_max: symbol.regime_filters.adx_max,
            bbw_quantile: symbol.regime_filters.bbw_quantile,
            slope_threshold: symbol.regime_filters.slope_threshold,
        },
        order: rustcta_strategy_range_grid::OrderConfig {
            post_only: symbol.order.post_only,
            tif: symbol.order.tif.clone(),
        },
        market_exit: rustcta_strategy_range_grid::MarketExitConfig {
            use_market_order: symbol.market_exit.use_market_order,
        },
    }
}

fn core_precision(precision: &PairPrecision) -> rustcta_strategy_range_grid::PairPrecision {
    rustcta_strategy_range_grid::PairPrecision {
        price_digits: precision.price_digits,
        amount_digits: precision.amount_digits,
        price_step: precision.price_step,
        amount_step: precision.amount_step,
        min_notional: precision.min_notional,
    }
}

fn grid_order_from_core(order: rustcta_strategy_range_grid::GridOrderPlan) -> GridOrderPlan {
    GridOrderPlan {
        client_id: order.client_id,
        price: order.price,
        quantity: order.quantity,
        side: match order.side {
            rustcta_strategy_sdk::OrderSide::Buy => OrderSide::Buy,
            rustcta_strategy_sdk::OrderSide::Sell => OrderSide::Sell,
        },
        order_type: match order.order_type {
            rustcta_strategy_sdk::OrderType::Market => OrderType::Market,
            rustcta_strategy_sdk::OrderType::Limit
            | rustcta_strategy_sdk::OrderType::PostOnly
            | rustcta_strategy_sdk::OrderType::ImmediateOrCancel
            | rustcta_strategy_sdk::OrderType::Custom(_) => OrderType::Limit,
        },
    }
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
