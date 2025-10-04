use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, RwLock};

use crate::core::types::{MarketType, OrderRequest, OrderSide, OrderType};
use crate::cta::account_manager::AccountManager;
use crate::strategies::common::{StrategyRiskLimits, UnifiedRiskEvaluator};

use crate::strategies::range_grid::application::notifications::RangeGridNotifier;
use crate::strategies::range_grid::application::risk;
use crate::strategies::range_grid::domain::classifier::RangeRegimeClassifier;
use crate::strategies::range_grid::domain::config::{
    ExecutionConfig, IndicatorConfig, PrecisionManagementConfig, RangeGridConfig, SymbolConfig,
};
use crate::strategies::range_grid::domain::model::{
    ActiveOrder, GridOrderPlan, GridPlan, MarketRegime, PairPrecision, PairRuntimeState,
};
use crate::strategies::range_grid::infrastructure::planner;
use crate::strategies::range_grid::infrastructure::services::{IndicatorService, PrecisionService};
use crate::utils::generate_order_id_with_tag;

pub struct SymbolTaskParams {
    pub global_config: Arc<RangeGridConfig>,
    pub symbol_config: SymbolConfig,
    pub state: Arc<Mutex<PairRuntimeState>>,
    pub running: Arc<RwLock<bool>>,
    pub account_manager: Arc<AccountManager>,
    pub indicator_service: IndicatorService,
    pub precision_service: PrecisionService,
    pub notifier: RangeGridNotifier,
    pub risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    pub risk_limits: StrategyRiskLimits,
    pub market_type: MarketType,
}

pub fn spawn_symbol_loop(params: SymbolTaskParams) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = symbol_loop(params).await {
            log::error!("[range_grid] 交易对任务异常结束: {}", e);
        }
    })
}

fn quantize_value(value: f64, digits: u32, step: f64) -> f64 {
    let mut adjusted = value;
    if step > 0.0 {
        let multiples = (adjusted / step).round();
        adjusted = multiples * step;
    }

    if digits == 0 {
        return adjusted.round();
    }

    let factor = 10_f64.powi(digits as i32);
    (adjusted * factor).round() / factor
}

async fn symbol_loop(params: SymbolTaskParams) -> Result<()> {
    let SymbolTaskParams {
        global_config,
        symbol_config,
        state,
        running,
        account_manager,
        indicator_service,
        precision_service,
        notifier,
        risk_evaluator,
        risk_limits,
        market_type,
    } = params;

    let exec_cfg = global_config.execution.clone();
    let indicator_cfg = global_config.indicators.clone();
    let precision_cfg = global_config.precision_management.clone();

    let interval = Duration::from_secs(exec_cfg.evaluation_interval_secs.max(300));

    loop {
        if !*running.read().await {
            log::info!(
                "[range_grid] {} 检测到策略停止标志，退出任务",
                symbol_config.config_id
            );
            break;
        }

        if !symbol_config.enabled {
            tokio::time::sleep(interval).await;
            continue;
        }

        ensure_precision(&symbol_config, &precision_cfg, &precision_service, &state).await?;

        if is_in_cooldown(&state).await {
            tokio::time::sleep(interval).await;
            continue;
        }

        let snapshot = match indicator_service
            .load_snapshot(&symbol_config, &indicator_cfg)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                log::warn!(
                    "[range_grid] {} 加载指标失败: {}",
                    symbol_config.config_id,
                    err
                );
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        {
            let mut guard = state.lock().await;
            guard.mark_indicator(snapshot.clone());
        }

        let (current_regime, grid_active, precision_opt) = {
            let guard = state.lock().await;
            (guard.regime, guard.grid_active, guard.precision.clone())
        };

        let classifier = RangeRegimeClassifier::new(&symbol_config, &indicator_cfg);
        let decision = classifier.evaluate(&snapshot, current_regime, grid_active);

        let regime_changed = decision.new_regime != current_regime;
        if regime_changed {
            let mut guard = state.lock().await;
            guard.mark_regime(decision.new_regime);
            guard.need_rebuild = true;
            log::info!(
                "[range_grid] {} 市场状态切换 {:?} -> {:?}: {:?}",
                symbol_config.config_id,
                current_regime,
                decision.new_regime,
                decision.reason
            );
        }

        let should_deactivate =
            decision.deactivate_grid || (!decision.new_regime.is_range() && regime_changed);
        if should_deactivate {
            deactivate_grid(
                &symbol_config,
                &state,
                &account_manager,
                &notifier,
                exec_cfg.cooldown_secs,
                market_type,
            )
            .await?;
        }

        if decision.activate_grid {
            let precision = match precision_opt.clone() {
                Some(p) => p,
                None => {
                    log::warn!(
                        "[range_grid] {} 缺少精度信息，无法构建网格",
                        symbol_config.config_id
                    );
                    tokio::time::sleep(interval).await;
                    continue;
                }
            };

            activate_grid(
                &symbol_config,
                &state,
                snapshot.last_price,
                &precision,
                &account_manager,
                &notifier,
                market_type,
            )
            .await?;
        } else if decision.new_regime.is_range() {
            let (rebuild_precision, center_override) = {
                let mut guard = state.lock().await;
                if guard.grid_active && guard.need_rebuild {
                    if let Some(precision) = guard.precision.clone() {
                        let center = guard
                            .pending_center_price
                            .take()
                            .unwrap_or(snapshot.last_price);
                        guard.need_rebuild = false;
                        guard.deactivate_grid();
                        (Some(precision), Some(center))
                    } else {
                        log::warn!(
                            "[range_grid] {} 缺少精度信息，无法重建网格",
                            symbol_config.config_id
                        );
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            };

            if let (Some(precision), Some(center)) = (rebuild_precision, center_override) {
                if let Err(err) = account_manager
                    .cancel_all_orders(&symbol_config.account.id, Some(&symbol_config.symbol))
                    .await
                {
                    log::warn!(
                        "[range_grid] {} 重建前取消挂单失败: {}",
                        symbol_config.config_id,
                        err
                    );
                }

                activate_grid(
                    &symbol_config,
                    &state,
                    center,
                    &precision,
                    &account_manager,
                    &notifier,
                    market_type,
                )
                .await?;
            }
        }

        perform_risk_check(
            &symbol_config,
            &state,
            &risk_limits,
            &risk_evaluator,
            &running,
        )
        .await?;

        if let Some(precision_clone) = precision_opt.clone() {
            ensure_grid_uniformity(
                &symbol_config,
                &state,
                &account_manager,
                &notifier,
                &precision_clone,
                market_type,
            )
            .await?;
        }

        tokio::time::sleep(interval).await;
    }

    Ok(())
}

async fn is_in_cooldown(state: &Arc<Mutex<PairRuntimeState>>) -> bool {
    let mut guard = state.lock().await;
    if let Some(until) = guard.cooldown_until {
        if until > Utc::now() {
            guard.cooling_down = true;
            return true;
        }
        guard.cooldown_until = None;
        guard.cooling_down = false;
    }
    false
}

async fn ensure_precision(
    symbol_config: &SymbolConfig,
    precision_cfg: &PrecisionManagementConfig,
    precision_service: &PrecisionService,
    state: &Arc<Mutex<PairRuntimeState>>,
) -> Result<()> {
    if !precision_cfg.auto_fetch {
        return Ok(());
    }

    let need_fetch = {
        let guard = state.lock().await;
        guard.precision.is_none()
    };

    if !need_fetch {
        return Ok(());
    }

    let precision = precision_service.fetch_precision(symbol_config).await?;
    log::info!(
        "[range_grid] {} 精度更新: price_digits={} amount_digits={} price_step={} amount_step={} min_notional={:?}",
        symbol_config.config_id,
        precision.price_digits,
        precision.amount_digits,
        precision.price_step,
        precision.amount_step,
        precision.min_notional
    );

    {
        let mut guard = state.lock().await;
        guard.mark_precision(precision.clone());
    }

    if precision_service
        .write_back_precision(symbol_config, &precision)
        .await?
    {
        log::info!(
            "[range_grid] {} 精度写回配置文件完成",
            symbol_config.config_id
        );
    }

    Ok(())
}

async fn activate_grid(
    symbol_config: &SymbolConfig,
    state: &Arc<Mutex<PairRuntimeState>>,
    center_price: f64,
    precision: &PairPrecision,
    account_manager: &Arc<AccountManager>,
    notifier: &RangeGridNotifier,
    market_type: MarketType,
) -> Result<()> {
    let plan = planner::build_grid_plan(symbol_config, precision, center_price)?;

    if plan.orders.is_empty() {
        return Err(anyhow!("网格计划为空"));
    }

    let mut plan_lookup: HashMap<String, GridOrderPlan> = HashMap::new();
    for order in &plan.orders {
        let key = plan_lookup_key(&order.side, order.price);
        plan_lookup.insert(key, order.clone());
    }

    let requests = build_order_requests(symbol_config, &plan, market_type);
    let response = account_manager
        .create_batch_orders(&symbol_config.account.id, requests)
        .await?;

    let success_count = response.successful_orders.len();
    let fail_count = response.failed_orders.len();

    if response.successful_orders.is_empty() {
        for failed in response.failed_orders {
            log::error!(
                "[range_grid] {} 订单失败: {} {:?}",
                symbol_config.config_id,
                failed.error_message,
                failed.error_code
            );
        }
        return Err(anyhow!("网格下单全部失败"));
    }

    let mut active_orders = HashMap::new();
    for order in response.successful_orders {
        let key = order
            .price
            .map(|p| plan_lookup_key(&order.side, p))
            .and_then(|k| plan_lookup.get(&k));
        let client_id = key.map(|plan| plan.client_id.clone());
        let order_id = order.id.clone();
        active_orders.insert(order_id.clone(), ActiveOrder::new(order.clone(), client_id));
    }

    let mut notify = false;
    {
        let mut guard = state.lock().await;
        let was_active = guard.grid_active;
        guard.activate_grid(plan.clone());
        guard.open_orders = active_orders;
        guard.need_rebuild = false;
        guard.pending_center_price = None;
        if !was_active {
            guard.last_notification = Some(Utc::now());
            notify = true;
        }
    }

    log::info!(
        "[range_grid] {} 网格已挂单，成功 {} 笔，失败 {} 笔",
        symbol_config.config_id,
        success_count,
        fail_count
    );

    if notify {
        notifier
            .send_text(
                "网格启动",
                &format!(
                    "{} 启动网格，中心价 {:.4}，挂单数 {}",
                    symbol_config.config_id,
                    center_price,
                    plan.orders.len()
                ),
            )
            .await;
    }

    Ok(())
}

fn build_order_requests(
    symbol_config: &SymbolConfig,
    plan: &GridPlan,
    market_type: MarketType,
) -> Vec<OrderRequest> {
    plan.orders
        .iter()
        .map(|order_plan| {
            let mut request = OrderRequest::new(
                symbol_config.symbol.clone(),
                order_plan.side,
                order_plan.order_type,
                order_plan.quantity,
                Some(order_plan.price),
                market_type,
            );
            request.client_order_id = Some(order_plan.client_id.clone());
            request.post_only = Some(symbol_config.order.post_only);
            let mut time_in_force = symbol_config.order.tif.as_ref().map(|t| t.to_uppercase());
            if symbol_config.order.post_only && time_in_force.as_deref() != Some("GTX") {
                time_in_force = Some("GTX".to_string());
            }
            request.time_in_force = time_in_force;
            request
        })
        .collect()
}

fn quantize_price(price: f64) -> i64 {
    (price * 1_000_000_000f64).round() as i64
}

fn plan_lookup_key(side: &OrderSide, price: f64) -> String {
    let prefix = match side {
        OrderSide::Buy => 'B',
        OrderSide::Sell => 'S',
    };
    format!("{}:{}", prefix, quantize_price(price))
}

async fn ensure_grid_uniformity(
    symbol_config: &SymbolConfig,
    state: &Arc<Mutex<PairRuntimeState>>,
    account_manager: &Arc<AccountManager>,
    notifier: &RangeGridNotifier,
    precision: &PairPrecision,
    market_type: MarketType,
) -> Result<()> {
    let (needs_reset, center_price, account_id, symbol) = {
        let mut guard = state.lock().await;
        if !guard.grid_active {
            return Ok(());
        }

        let buys: Vec<f64> = guard
            .open_orders
            .values()
            .filter(|o| o.order.side == OrderSide::Buy)
            .filter_map(|o| o.order.price)
            .collect();
        let sells: Vec<f64> = guard
            .open_orders
            .values()
            .filter(|o| o.order.side == OrderSide::Sell)
            .filter_map(|o| o.order.price)
            .collect();

        let levels = symbol_config.grid.levels_per_side as usize;
        let spacing = symbol_config.grid.grid_spacing_pct / 100.0;
        let tolerance = spacing * 0.25 + 1e-6;

        let counts_ok = buys.len() == sells.len() && buys.len() == levels;
        let spacing_ok = counts_ok
            && verify_spacing(&buys, spacing, tolerance, true)
            && verify_spacing(&sells, spacing, tolerance, false);

        if counts_ok && spacing_ok {
            return Ok(());
        }

        let center = guard
            .last_fill_price
            .or(Some(guard.current_price))
            .unwrap_or(guard.current_price);
        guard.pending_center_price = Some(center);
        guard.need_rebuild = false;
        guard.grid_active = false;
        guard.current_plan = None;
        guard.open_orders.clear();

        (
            true,
            center,
            guard.config.account.id.clone(),
            guard.config.symbol.clone(),
        )
    };

    if !needs_reset {
        return Ok(());
    }

    if let Err(err) = account_manager
        .cancel_all_orders(&account_id, Some(&symbol))
        .await
    {
        log::warn!(
            "[range_grid] {} 取消订单失败: {}",
            symbol_config.config_id,
            err
        );
    }

    activate_grid(
        symbol_config,
        state,
        center_price,
        precision,
        account_manager,
        notifier,
        market_type,
    )
    .await
}

fn verify_spacing(prices: &[f64], spacing: f64, tolerance: f64, is_buy: bool) -> bool {
    if prices.len() < 2 {
        return true;
    }

    let mut sorted = prices.to_vec();
    if is_buy {
        sorted.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
    } else {
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    }

    for window in sorted.windows(2) {
        if let [first, second] = window {
            let ratio = if is_buy {
                (first - second) / first.abs().max(1e-9)
            } else {
                (second - first) / first.abs().max(1e-9)
            };
            if (ratio - spacing).abs() > tolerance {
                return false;
            }
        }
    }
    true
}

async fn deactivate_grid(
    symbol_config: &SymbolConfig,
    state: &Arc<Mutex<PairRuntimeState>>,
    account_manager: &Arc<AccountManager>,
    notifier: &RangeGridNotifier,
    cooldown_secs: u64,
    market_type: MarketType,
) -> Result<()> {
    let (net_position, precision_opt) = {
        let guard = state.lock().await;
        (guard.net_position, guard.precision.clone())
    };

    if let Err(err) = account_manager
        .cancel_all_orders(&symbol_config.account.id, Some(&symbol_config.symbol))
        .await
    {
        log::warn!(
            "[range_grid] {} 取消全部挂单失败: {}",
            symbol_config.config_id,
            err
        );
    }

    let mut flatten_success = false;
    if symbol_config.market_exit.use_market_order && net_position.abs() > 1e-9 {
        if let Some(precision) = precision_opt {
            let qty = quantize_value(
                net_position.abs(),
                precision.amount_digits,
                precision.amount_step,
            );
            if qty > 0.0 {
                let side = if net_position > 0.0 {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };
                let mut request = OrderRequest::new(
                    symbol_config.symbol.clone(),
                    side,
                    OrderType::Market,
                    qty,
                    None,
                    market_type,
                );
                request.client_order_id = Some(generate_order_id_with_tag(
                    "range_grid",
                    &symbol_config.account.exchange,
                    "F",
                ));
                request.time_in_force = Some("IOC".to_string());
                request.reduce_only = Some(true);
                request.post_only = Some(false);

                match account_manager
                    .create_batch_orders(&symbol_config.account.id, vec![request])
                    .await
                {
                    Ok(response) => {
                        if !response.successful_orders.is_empty() {
                            flatten_success = true;
                            log::info!(
                                "[range_grid] {} 市价平仓已提交，净仓 {:.4} -> {:.4}",
                                symbol_config.config_id,
                                net_position,
                                0.0
                            );
                        }
                        if !response.failed_orders.is_empty() {
                            log::warn!(
                                "[range_grid] {} 市价平仓失败 {} 笔: {:?}",
                                symbol_config.config_id,
                                response.failed_orders.len(),
                                response.failed_orders
                            );
                        }
                    }
                    Err(err) => {
                        log::error!(
                            "[range_grid] {} 市价平仓下单失败: {}",
                            symbol_config.config_id,
                            err
                        );
                    }
                }
            }
        }
    }

    let cooldown_until = Utc::now() + chrono::Duration::seconds(cooldown_secs as i64);
    {
        let mut guard = state.lock().await;
        guard.deactivate_grid();
        guard.cooldown_until = Some(cooldown_until);
        guard.cooling_down = cooldown_secs > 0;
        if flatten_success {
            guard.net_position = 0.0;
        }
    }

    notifier
        .send_text(
            "网格停止",
            &format!(
                "{} 停止网格，进入冷却 {}s",
                symbol_config.config_id, cooldown_secs
            ),
        )
        .await;

    Ok(())
}

async fn perform_risk_check(
    symbol_config: &SymbolConfig,
    state: &Arc<Mutex<PairRuntimeState>>,
    risk_limits: &StrategyRiskLimits,
    evaluator: &Arc<dyn UnifiedRiskEvaluator>,
    running: &Arc<RwLock<bool>>,
) -> Result<()> {
    let snapshot_state = {
        let guard = state.lock().await;
        risk::build_snapshot(
            &format!("range_grid::{}", symbol_config.config_id),
            &guard,
            risk_limits,
        )
    };

    let decision = risk::evaluate_risk(evaluator, &snapshot_state).await;
    risk::apply_risk_decision(state, running, decision).await
}
