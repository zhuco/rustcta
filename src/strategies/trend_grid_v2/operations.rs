use std::sync::Arc;

use chrono::Local;
use tokio::sync::Mutex;
use tokio::time::Duration;

use super::config::{BatchSettings, GridManagement, TradingConfig, TrendAdjustment};
use super::engine::{GridContext, TrendGridEngine};
use super::state::{ConfigState, TrendStrength};
use crate::core::{
    error::ExchangeError,
    types::{MarketType, OrderRequest, OrderSide, OrderStatus, OrderType},
};
use crate::cta::account_manager::AccountManager;
use crate::utils::{generate_order_id, generate_order_id_with_tag};

/// 计算并提交网格订单
pub(super) async fn calculate_and_submit_grid(
    config: &TradingConfig,
    state: &Arc<Mutex<ConfigState>>,
    account_manager: &AccountManager,
    batch_settings: &BatchSettings,
    trend_adjustment: &TrendAdjustment,
    _grid_management: &GridManagement,
) -> Result<()> {
    let account = account_manager
        .get_account(&config.account.id)
        .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", config.account.id)))?;

    let (mut current_price, price_precision, amount_precision, trend_strength) = {
        let mut guard = state.lock().await;
        guard.grid_orders.clear();
        (
            guard.current_price,
            guard.price_precision,
            guard.amount_precision,
            guard.trend_strength,
        )
    };

    match account
        .exchange
        .get_ticker(&config.symbol, MarketType::Futures)
        .await
    {
        Ok(ticker) => {
            current_price = ticker.last;
            log::info!(
                "🔄 {} 重置网格前获取最新价格: {:.4}",
                config.config_id,
                current_price
            );
        }
        Err(e) => {
            log::warn!(
                "⚠️ {} 无法获取最新价格，使用缓存价格: {:.4}, 错误: {}",
                config.config_id,
                current_price,
                e
            );
        }
    }

    {
        let mut guard = state.lock().await;
        guard.current_price = current_price;
    }

    let context = GridContext {
        current_price,
        price_precision,
        amount_precision,
        trend_strength,
    };

    let (buy_multiplier, sell_multiplier) = match trend_strength {
        TrendStrength::StrongBull => (trend_adjustment.strong_bull_buy_multiplier, 1.0),
        TrendStrength::Bull => (trend_adjustment.bull_buy_multiplier, 1.0),
        TrendStrength::Bear => (1.0, trend_adjustment.bear_sell_multiplier),
        TrendStrength::StrongBear => (1.0, trend_adjustment.strong_bear_sell_multiplier),
        TrendStrength::Neutral => (1.0, 1.0),
    };

    log::debug!(
        "趋势: {:?} | 买单{}x 卖单{}x",
        trend_strength,
        buy_multiplier,
        sell_multiplier
    );

    let engine = TrendGridEngine::new();
    let planned_orders = engine.build_grid_orders(config, context, trend_adjustment);

    if planned_orders.is_empty() {
        log::warn!(
            "⚠️ {} 生成的网格订单为空，检查配置或精度设置",
            config.config_id
        );
        return Ok(());
    }

    let batch_size = match account.exchange_name.as_str() {
        "binance" => batch_settings.binance_batch_size,
        "okx" => batch_settings.okx_batch_size,
        "hyperliquid" => batch_settings.hyperliquid_batch_size,
        _ => batch_settings.default_batch_size,
    };

    let mut success_count = 0;
    let mut fail_count = 0;

    for chunk in planned_orders.chunks(batch_size as usize) {
        let orders: Vec<OrderRequest> = chunk
            .iter()
            .map(|grid_order| {
                let mut order = OrderRequest::new(
                    config.symbol.clone(),
                    grid_order.side.clone(),
                    OrderType::Limit,
                    grid_order.quantity,
                    Some(grid_order.price),
                    MarketType::Futures,
                );
                let tag = if grid_order.side == OrderSide::Buy {
                    "B"
                } else {
                    "S"
                };
                order.client_order_id = Some(generate_order_id_with_tag(
                    "trend_grid_v2",
                    &account.exchange_name,
                    tag,
                ));
                order
            })
            .collect();

        log::debug!("📝 {} 准备提交 {} 个订单", config.config_id, orders.len());
        for (i, order) in orders.iter().enumerate() {
            log::debug!(
                "  订单{}: {:?} {} @ {:.4}, 数量: {}",
                i + 1,
                order.side,
                order.symbol,
                order.price.unwrap_or(0.0),
                order.amount
            );
        }

        match account_manager
            .create_batch_orders(&config.account.id, orders)
            .await
        {
            Ok(response) => {
                let successful_orders = response.successful_orders;
                let mut failed_orders = response.failed_orders;

                log::info!(
                    "📊 {} 批次结果: {} 成功, {} 失败",
                    config.config_id,
                    successful_orders.len(),
                    failed_orders.len()
                );

                success_count += successful_orders.len();
                fail_count += failed_orders.len();

                for order in &successful_orders {
                    log::debug!("  ✅ 订单 {} 创建成功", order.id);
                }

                {
                    let mut guard = state.lock().await;
                    for order in successful_orders {
                        guard.active_orders.insert(order.id.clone(), order.clone());
                        guard.grid_orders.insert(order.id.clone(), order);
                    }
                }

                for failed in failed_orders.drain(..) {
                    log::warn!(
                        "  ⚠️ 订单失败: {} - {}",
                        failed.order_request.symbol,
                        failed.error_message
                    );
                }
            }
            Err(e) => {
                log::error!("❌ {} 批量下单失败: {}", config.config_id, e);
                fail_count += chunk.len();
            }
        }
    }

    log::info!(
        "✅ {} 网格订单提交完成: {} 成功, {} 失败",
        config.config_id,
        success_count,
        fail_count
    );

    Ok(())
}

/// 重置指定配置的网格
pub(super) async fn reset_grid_for_config(
    config: &TradingConfig,
    state: &Arc<Mutex<ConfigState>>,
    account_manager: &AccountManager,
    batch_settings: &BatchSettings,
    trend_adjustment: &TrendAdjustment,
    grid_management: &GridManagement,
) -> Result<()> {
    log::info!("🔄 {} 开始重置网格", config.config_id);

    match account_manager
        .cancel_all_orders(&config.account.id, Some(&config.symbol))
        .await
    {
        Ok(cancelled) => {
            log::info!(
                "✅ {} 取消了 {} 个旧订单",
                config.config_id,
                cancelled.len()
            );
        }
        Err(e) => {
            log::error!("❌ {} 取消订单失败: {}", config.config_id, e);
            return Err(e);
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    calculate_and_submit_grid(
        config,
        state,
        account_manager,
        batch_settings,
        trend_adjustment,
        grid_management,
    )
    .await?;

    {
        let mut guard = state.lock().await;
        guard.need_grid_reset = false;
        guard.last_grid_check = chrono::Utc::now();
    }

    log::info!("✅ {} 网格重置完成", config.config_id);
    Ok(())
}

/// 网格均匀性检查
pub(super) fn check_grid_uniformity(state: &mut ConfigState, rebalance_threshold: f64) -> bool {
    let buy_orders_count = state
        .active_orders
        .values()
        .filter(|o| o.side == OrderSide::Buy)
        .count();
    let sell_orders_count = state
        .active_orders
        .values()
        .filter(|o| o.side == OrderSide::Sell)
        .count();

    if state.active_orders.is_empty() {
        log::warn!(
            "⚠️ {} 当前没有活跃订单，标记需要重置网格",
            state.config.config_id
        );
        state.need_grid_reset = true;
        return true;
    }

    let total_notional: f64 = state
        .active_orders
        .values()
        .map(|o| o.price.unwrap_or(0.0) * o.amount)
        .sum();
    let avg_notional = total_notional / (state.active_orders.len() as f64);

    let mut high_notional = 0.0;
    let mut low_notional = f64::MAX;

    for order in state.active_orders.values() {
        let notional = order.price.unwrap_or(0.0) * order.amount;
        if notional > high_notional {
            high_notional = notional;
        }
        if notional < low_notional {
            low_notional = notional;
        }
    }

    if avg_notional > 0.0 && avg_notional * rebalance_threshold > 0.0 {
        if high_notional > avg_notional * (1.0 + rebalance_threshold)
            || low_notional < avg_notional * (1.0 - rebalance_threshold)
        {
            log::warn!(
                "⚠️ {} 网格名义金额差异过大，高:{:.2} 低:{:.2} 平均:{:.2} 阈值:{:.2}",
                state.config.config_id,
                high_notional,
                low_notional,
                avg_notional,
                rebalance_threshold
            );
            state.need_grid_reset = true;
            return true;
        }
    }

    let order_diff = (buy_orders_count as i32 - sell_orders_count as i32).abs();
    let total_orders = buy_orders_count + sell_orders_count;
    let imbalance_ratio = order_diff as f64 / total_orders.max(1) as f64;

    if imbalance_ratio > 0.3 {
        let now = Local::now();
        log::warn!(
            "[网格不均匀] {} 买单:{} 卖单:{} 总计:{} 不平衡度:{:.1}%",
            now.format("%H:%M:%S"),
            buy_orders_count,
            sell_orders_count,
            total_orders,
            imbalance_ratio * 100.0
        );
        state.need_grid_reset = true;
        return true;
    }

    log::debug!(
        "网格均匀性检查通过: 买单 {}, 卖单 {}",
        buy_orders_count,
        sell_orders_count
    );
    false
}

pub(super) fn calculate_precision(step: f64) -> u32 {
    TrendGridEngine::precision_from_step(step)
}

pub(super) fn round_price(price: f64, precision: u32) -> f64 {
    TrendGridEngine::round_price(price, precision)
}

pub(super) fn round_amount(amount: f64, precision: u32) -> f64 {
    TrendGridEngine::round_amount(amount, precision)
}

type Result<T> = std::result::Result<T, ExchangeError>;
