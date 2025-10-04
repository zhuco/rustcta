use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde_json::json;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Duration};

use crate::core::error::ExchangeError;
use crate::core::exchange::Exchange;
use crate::core::types::{
    BatchOrderResponse, ExecutionReport, MarketType, Order, OrderRequest, OrderSide, OrderStatus,
    OrderType, WsMessage,
};
use crate::core::websocket::{MessageHandler, WebSocketClient};
use crate::cta::account_manager::AccountManager;
use crate::exchanges::binance::{BinanceExchange, BinanceMessageHandler, BinanceWebSocketClient};
use crate::strategies::common::grid::{plan_maker_fill, MakerFillPlan};
use crate::strategies::range_grid::domain::config::SymbolConfig;
use crate::strategies::range_grid::domain::model::{ActiveOrder, PairPrecision, PairRuntimeState};
use crate::utils::generate_order_id_with_tag;

pub struct RangeGridWebsocket;

impl RangeGridWebsocket {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_for_account(
        account_id: String,
        account_manager: Arc<AccountManager>,
        market_type: MarketType,
        symbol_map: Arc<HashMap<String, String>>,
        allowed_configs: Arc<HashSet<String>>,
        states: Arc<RwLock<HashMap<String, Arc<Mutex<PairRuntimeState>>>>>,
        running: Arc<RwLock<bool>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut retry_delay = Duration::from_secs(5);
            while *running.read().await {
                match Self::run_stream_once(
                    &account_id,
                    account_manager.clone(),
                    market_type,
                    symbol_map.clone(),
                    allowed_configs.clone(),
                    states.clone(),
                    running.clone(),
                )
                .await
                {
                    Ok(()) => break,
                    Err(err) => {
                        log::error!(
                            "[range_grid] {} WebSocket运行异常: {}，{} 秒后重试",
                            account_id,
                            err,
                            retry_delay.as_secs()
                        );
                        sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(Duration::from_secs(60));
                    }
                }
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_stream_once(
        account_id: &str,
        account_manager: Arc<AccountManager>,
        market_type: MarketType,
        symbol_map: Arc<HashMap<String, String>>,
        allowed_configs: Arc<HashSet<String>>,
        states: Arc<RwLock<HashMap<String, Arc<Mutex<PairRuntimeState>>>>>,
        running: Arc<RwLock<bool>>,
    ) -> Result<()> {
        let account = account_manager
            .get_account(account_id)
            .ok_or_else(|| anyhow!("账户 {} 不存在", account_id))?;

        if account.exchange_name.to_lowercase() != "binance" {
            log::warn!(
                "[range_grid] 账户 {} ({}) 暂不支持 WebSocket 成交处理",
                account_id,
                account.exchange_name
            );
            return Ok(());
        }

        let listen_key = account
            .exchange
            .create_user_data_stream(market_type)
            .await?;
        log::info!(
            "[range_grid] 账户 {} 获取 Binance listenKey 成功",
            account_id
        );

        let ws_url = match market_type {
            MarketType::Futures => format!("wss://fstream.binance.com/ws/{}", listen_key),
            MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
        };

        let mut ws_client = BinanceWebSocketClient::new(ws_url, market_type);
        ws_client.connect().await?;
        log::info!(
            "[range_grid] 账户 {} WebSocket 已连接 ({} symbols)",
            account_id,
            allowed_configs.len()
        );

        let binance_exchange = (&**account.exchange)
            .as_any()
            .downcast_ref::<BinanceExchange>()
            .map(|ex| ex.clone());

        if let Some(exchange_clone) = binance_exchange.clone() {
            let listen_key_keepalive = listen_key.clone();
            let running_clone = running.clone();
            tokio::spawn(async move {
                let mut keepalive_timer = interval(Duration::from_secs(15 * 60));
                while *running_clone.read().await {
                    keepalive_timer.tick().await;
                    if let Err(err) = exchange_clone
                        .keepalive_user_data_stream(&listen_key_keepalive, market_type)
                        .await
                    {
                        log::warn!("[range_grid] ListenKey 保活失败: {}，稍后重试", err);
                    }
                }
            });
        }

        let handler = RangeGridWsHandler {
            account_id: account_id.to_string(),
            market_type,
            account_manager: account_manager.clone(),
            states: states.clone(),
            symbol_map,
            allowed_configs,
        };

        let message_handler = if let Some(exchange_clone) = binance_exchange {
            BinanceMessageHandler::with_exchange(Box::new(handler), market_type, exchange_clone)
        } else {
            BinanceMessageHandler::new(Box::new(handler), market_type)
        };

        let mut message_count: u64 = 0;
        while *running.read().await {
            match ws_client.receive().await {
                Ok(Some(raw)) => {
                    message_count += 1;
                    match ws_client.parse_binance_message(&raw) {
                        Ok(message) => {
                            if let Err(err) = message_handler.handle_message(message).await {
                                log::error!(
                                    "[range_grid] {} WebSocket消息处理失败: {}",
                                    account_id,
                                    err
                                );
                            }
                        }
                        Err(err) => {
                            log::debug!(
                                "[range_grid] {} WebSocket消息解析失败: {}",
                                account_id,
                                err
                            );
                        }
                    }

                    if message_count % 200 == 0 {
                        log::debug!(
                            "[range_grid] {} WebSocket累计处理 {} 条消息",
                            account_id,
                            message_count
                        );
                    }
                }
                Ok(None) => {
                    sleep(Duration::from_millis(50)).await;
                }
                Err(err) => {
                    return Err(anyhow!(
                        "WebSocket 接收错误 (account={}): {}",
                        account_id,
                        err
                    ));
                }
            }
        }

        Ok(())
    }
}

fn quantize_value(value: f64, digits: u32, step: f64) -> f64 {
    let mut adjusted = value;
    if step > 0.0 {
        let multiples = (adjusted / step).round();
        adjusted = multiples * step;
    }

    if digits == 0 {
        adjusted = adjusted.round();
    } else {
        let factor = 10_f64.powi(digits as i32);
        adjusted = (adjusted * factor).round() / factor;
    }

    if adjusted <= 0.0 && value > 0.0 && step > 0.0 {
        step
    } else {
        adjusted
    }
}

struct MakerPlan {
    symbol_config: SymbolConfig,
    precision: Option<PairPrecision>,
    fill_price: f64,
    fill_plan: MakerFillPlan,
    cancel_target: Option<(String, OrderSide, Option<f64>)>,
}

#[derive(Default)]
struct MakerExecutionResult {
    planned_new_orders: Vec<String>,
    executed_new_orders: Vec<String>,
    cancel_logs: Vec<String>,
    rebuild_triggered: bool,
}

struct RangeGridWsHandler {
    account_id: String,
    market_type: MarketType,
    account_manager: Arc<AccountManager>,
    states: Arc<RwLock<HashMap<String, Arc<Mutex<PairRuntimeState>>>>>,
    symbol_map: Arc<HashMap<String, String>>,
    allowed_configs: Arc<HashSet<String>>,
}

impl RangeGridWsHandler {
    fn normalize_symbol(symbol: &str) -> String {
        symbol.replace('/', "").to_uppercase()
    }

    fn order_from_execution_report(report: ExecutionReport, market_type: MarketType) -> Order {
        let ExecutionReport {
            symbol,
            order_id,
            client_order_id,
            side,
            order_type,
            status,
            price,
            amount,
            executed_amount,
            executed_price,
            commission,
            commission_asset,
            timestamp,
            is_maker,
        } = report;

        let reference_price = if price > 0.0 { price } else { executed_price };
        let price_opt = if reference_price > 0.0 {
            Some(reference_price)
        } else {
            None
        };
        let remaining = (amount - executed_amount).max(0.0);

        let info = json!({
            "isMaker": is_maker,
            "clientOrderId": client_order_id,
            "commission": commission,
            "commissionAsset": commission_asset,
            "executedAmount": executed_amount,
            "executedPrice": executed_price,
            "source": "execution_report",
        });

        Order {
            id: order_id,
            symbol,
            side,
            order_type,
            amount,
            price: price_opt,
            filled: executed_amount,
            remaining,
            status,
            market_type,
            timestamp,
            last_trade_timestamp: if executed_amount > 0.0 {
                Some(timestamp)
            } else {
                None
            },
            info,
        }
    }

    async fn handle_order_update(&self, order: Order) -> Result<()> {
        let symbol_key = Self::normalize_symbol(&order.symbol);
        let Some(config_id) = self.symbol_map.get(&symbol_key) else {
            log::debug!(
                "[range_grid] {} 收到未跟踪交易对订单: {}",
                self.account_id,
                order.symbol
            );
            return Ok(());
        };

        if !self.allowed_configs.contains(config_id) {
            return Ok(());
        }

        let state_arc = {
            let guard = self.states.read().await;
            guard.get(config_id).cloned()
        };

        let Some(state_arc) = state_arc else {
            log::warn!("[range_grid] 找不到配置 {} 的状态，忽略订单更新", config_id);
            return Ok(());
        };

        let mut fill_summary: Option<(OrderSide, f64, f64)> = None;
        let mut maker_plan: Option<MakerPlan> = None;
        let mut rebuild_needed = false;
        let mut rebuild_context: Option<(SymbolConfig, f64)> = None;
        let mut had_fill_delta = false;
        let mut taker_rebuild = false;

        {
            let mut guard = state_arc.lock().await;
            guard.current_price = order.price.unwrap_or(guard.current_price);

            let symbol_config = guard.config.clone();
            let precision_clone = guard.precision.clone();

            let order_id = order.id.clone();
            let mut delta = 0.0;

            if let Some(active) = guard.open_orders.get_mut(&order_id) {
                let previous_filled = active.order.filled;
                let new_filled = order.filled;
                delta = (new_filled - previous_filled).max(0.0);
                active.order = order.clone();
            } else {
                let client_id = order
                    .info
                    .get("clientOrderId")
                    .or_else(|| order.info.get("client_order_id"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                guard
                    .open_orders
                    .insert(order_id.clone(), ActiveOrder::new(order.clone(), client_id));
            }

            if delta > 1e-9 {
                let signed = match order.side {
                    OrderSide::Buy => delta,
                    OrderSide::Sell => -delta,
                };
                guard.net_position += signed;
                let fill_price = order.price.unwrap_or(guard.current_price);
                guard.last_fill_price = Some(fill_price);
                fill_summary = Some((order.side, fill_price, delta));
                had_fill_delta = true;
                log::info!(
                    "[range_grid] {} 订单成交: {} {:?} @ {:.6} (Δ {:.6})",
                    config_id,
                    order.symbol,
                    order.side,
                    fill_price,
                    delta
                );
            }

            let order_price = order.price.unwrap_or(guard.current_price);
            let is_maker = order
                .info
                .get("isMaker")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let is_complete = order.status == OrderStatus::Closed;

            if is_complete {
                guard.open_orders.remove(&order_id);
                guard.last_fill_price = Some(order_price);

                if is_maker && had_fill_delta {
                    let spacing = symbol_config.grid.grid_spacing_pct / 100.0;

                    if spacing <= 0.0 {
                        log::warn!(
                            "[range_grid] {} 网格间距无效 ({}), 触发重建",
                            config_id,
                            spacing
                        );
                        guard.pending_center_price = Some(order_price);
                        guard.need_rebuild = true;
                        guard.grid_active = false;
                        guard.current_plan = None;
                        guard.open_orders.clear();
                        if rebuild_context.is_none() {
                            rebuild_context = Some((symbol_config.clone(), order_price));
                        }
                        rebuild_needed = true;
                    } else {
                        let fill_plan = plan_maker_fill(order.side, order_price, spacing);

                        if fill_plan.new_orders.is_empty() {
                            log::warn!("[range_grid] {} 补单计划为空，触发重建", config_id);
                            guard.pending_center_price = Some(order_price);
                            guard.need_rebuild = true;
                            guard.grid_active = false;
                            guard.current_plan = None;
                            guard.open_orders.clear();
                            if rebuild_context.is_none() {
                                rebuild_context = Some((symbol_config.clone(), order_price));
                            }
                            rebuild_needed = true;
                        } else {
                            let cancel_target = match fill_plan.cancel_side {
                                OrderSide::Sell => guard
                                    .open_orders
                                    .iter()
                                    .filter(|(_, active)| active.order.side == OrderSide::Sell)
                                    .filter_map(|(id, active)| {
                                        active.order.price.map(|p| (id.clone(), p))
                                    })
                                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
                                    .map(|(id, price)| (id, OrderSide::Sell, Some(price))),
                                OrderSide::Buy => guard
                                    .open_orders
                                    .iter()
                                    .filter(|(_, active)| active.order.side == OrderSide::Buy)
                                    .filter_map(|(id, active)| {
                                        active.order.price.map(|p| (id.clone(), p))
                                    })
                                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
                                    .map(|(id, price)| (id, OrderSide::Buy, Some(price))),
                            };

                            if let Some(_) = cancel_target {
                                maker_plan = Some(MakerPlan {
                                    symbol_config: symbol_config.clone(),
                                    precision: precision_clone,
                                    fill_price: order_price,
                                    fill_plan,
                                    cancel_target,
                                });
                            } else {
                                log::warn!(
                                    "[range_grid] {} 成交后找不到对侧挂单，触发重建",
                                    config_id
                                );
                                guard.pending_center_price = Some(order_price);
                                guard.need_rebuild = true;
                                guard.grid_active = false;
                                guard.current_plan = None;
                                guard.open_orders.clear();
                                if rebuild_context.is_none() {
                                    rebuild_context = Some((symbol_config.clone(), order_price));
                                }
                                rebuild_needed = true;
                            }
                        }
                    }
                } else if is_maker {
                    // 订单关闭但无新增成交，保持当前状态
                    guard.grid_active = !guard.open_orders.is_empty();
                } else {
                    guard.pending_center_price = Some(order_price);
                    guard.need_rebuild = true;
                    guard.grid_active = false;
                    guard.current_plan = None;
                    guard.open_orders.clear();
                    if rebuild_context.is_none() {
                        rebuild_context = Some((symbol_config.clone(), order_price));
                    }
                    rebuild_needed = true;
                    taker_rebuild = true;
                }
            } else if matches!(
                order.status,
                OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected
            ) {
                let client_id = order
                    .info
                    .get("clientOrderId")
                    .or_else(|| order.info.get("client_order_id"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let existed = guard.open_orders.remove(&order_id).is_some();
                if existed {
                    log::info!(
                        "[range_grid] {} 订单状态更新: id={} status={:?} clientId={:?}",
                        config_id,
                        order_id,
                        order.status,
                        client_id
                    );
                    guard.grid_active = !guard.open_orders.is_empty();
                }

                if guard.open_orders.is_empty() {
                    log::warn!(
                        "[range_grid] {} 订单簿耗尽: 最后更新 id={} status={:?}",
                        config_id,
                        order_id,
                        order.status
                    );
                    guard.pending_center_price = Some(order_price);
                    guard.need_rebuild = true;
                    guard.grid_active = false;
                    guard.current_plan = None;
                }
            } else if !is_maker && had_fill_delta {
                log::warn!(
                    "[range_grid] {} 检测到吃单成交 id={} price={:.6} Δ={:.6}，进入重建",
                    config_id,
                    order_id,
                    order_price,
                    delta
                );
                guard.pending_center_price = Some(order_price);
                guard.need_rebuild = true;
                guard.grid_active = false;
                guard.current_plan = None;
                guard.open_orders.clear();
                if rebuild_context.is_none() {
                    rebuild_context = Some((symbol_config.clone(), order_price));
                }
                rebuild_needed = true;
                taker_rebuild = true;
            }
        }

        if rebuild_needed {
            if taker_rebuild {
                sleep(Duration::from_secs(1)).await;
            }
            if let Some((symbol_config, reference_price)) = rebuild_context {
                self.trigger_full_rebuild(
                    &symbol_config.account.id,
                    &symbol_config.symbol,
                    config_id,
                    reference_price,
                    &state_arc,
                )
                .await?;
                log::info!(
                    "[range_grid] {} 触发全撤: reference_price={:.6} open_orders清空",
                    config_id,
                    reference_price
                );
            } else {
                log::info!(
                    "[range_grid] {} 触发全撤: 无参考价格 open_orders清空",
                    config_id
                );
            }
            return Ok(());
        }

        let mut planned_new_orders: Vec<String> = Vec::new();
        let mut executed_new_orders: Vec<String> = Vec::new();
        let mut cancel_logs: Vec<String> = Vec::new();

        if let Some(plan) = maker_plan {
            let exec_result = self.execute_maker_plan(config_id, plan, &state_arc).await?;

            if exec_result.rebuild_triggered {
                return Ok(());
            }

            planned_new_orders = exec_result.planned_new_orders;
            executed_new_orders = exec_result.executed_new_orders;
            cancel_logs = exec_result.cancel_logs;
        }

        if let Some((side, price, delta)) = fill_summary {
            log::info!(
                "[range_grid] {} 成交总结: {:?} @ {:.6} Δ {:.6} | 新单请求: [{}] | 新单确认: [{}] | 撤单: [{}]",
                config_id,
                side,
                price,
                delta,
                if planned_new_orders.is_empty() {
                    "无".to_string()
                } else {
                    planned_new_orders.join("; ")
                },
                if executed_new_orders.is_empty() {
                    "无".to_string()
                } else {
                    executed_new_orders.join("; ")
                },
                if cancel_logs.is_empty() {
                    "无".to_string()
                } else {
                    cancel_logs.join("; ")
                }
            );

            let guard = state_arc.lock().await;
            let mut buy_levels: Vec<f64> = guard
                .open_orders
                .values()
                .filter(|o| o.order.side == OrderSide::Buy)
                .filter_map(|o| o.order.price)
                .collect();
            buy_levels.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));

            let mut sell_levels: Vec<f64> = guard
                .open_orders
                .values()
                .filter(|o| o.order.side == OrderSide::Sell)
                .filter_map(|o| o.order.price)
                .collect();
            sell_levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

            log::info!(
                "[range_grid] {} 当前挂单分布: Buys [{}] | Sells [{}]",
                config_id,
                buy_levels
                    .iter()
                    .map(|p| format!("{:.6}", p))
                    .collect::<Vec<_>>()
                    .join(", "),
                sell_levels
                    .iter()
                    .map(|p| format!("{:.6}", p))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        Ok(())
    }

    async fn execute_maker_plan(
        &self,
        config_id: &str,
        plan: MakerPlan,
        state_arc: &Arc<Mutex<PairRuntimeState>>,
    ) -> Result<MakerExecutionResult> {
        let MakerPlan {
            symbol_config,
            precision,
            fill_price,
            fill_plan,
            cancel_target,
        } = plan;

        let mut result = MakerExecutionResult::default();
        let account_id = &symbol_config.account.id;
        let symbol = &symbol_config.symbol;

        let precision = match precision {
            Some(p) => p,
            None => {
                log::warn!("[range_grid] {} 缺少精度信息，触发重建", config_id);
                self.trigger_full_rebuild(account_id, symbol, config_id, fill_price, state_arc)
                    .await?;
                result.rebuild_triggered = true;
                return Ok(result);
            }
        };

        let tif_upper = symbol_config.order.tif.as_ref().map(|t| t.to_uppercase());
        let tif_ref = tif_upper.as_deref();
        let post_only_flag = symbol_config.order.post_only;

        let mut requests: Vec<OrderRequest> = Vec::new();
        for (side, target_price) in &fill_plan.new_orders {
            if *target_price <= 0.0 {
                continue;
            }
            if let Some(req) = build_order_request(
                symbol,
                *side,
                *target_price,
                symbol_config.grid.base_order_notional,
                &precision,
                &symbol_config.account.exchange,
                if *side == OrderSide::Buy { "B" } else { "S" },
                self.market_type,
                tif_ref,
                post_only_flag,
            ) {
                requests.push(req);
            }
        }

        if requests.is_empty() {
            return Ok(result);
        }

        let request_summaries: Vec<String> = requests
            .iter()
            .map(|req| {
                format!(
                    "{} {:.6} qty {:.6} reduce_only={} tif={} client={}",
                    match req.side {
                        OrderSide::Buy => "BUY",
                        OrderSide::Sell => "SELL",
                    },
                    req.price.unwrap_or(0.0),
                    req.amount,
                    req.reduce_only.unwrap_or(false),
                    req.time_in_force.clone().unwrap_or_default(),
                    req.client_order_id.clone().unwrap_or_default()
                )
            })
            .collect();
        result.planned_new_orders.extend(request_summaries.clone());

        match self
            .account_manager
            .create_batch_orders(account_id, requests)
            .await
        {
            Ok(BatchOrderResponse {
                mut successful_orders,
                failed_orders,
            }) => {
                if !successful_orders.is_empty() {
                    let success_descriptions: Vec<String> = successful_orders
                        .iter()
                        .map(|order| {
                            format!(
                                "{} {:.6} qty {:.6} id={} reduce_only={}",
                                match order.side {
                                    OrderSide::Buy => "BUY",
                                    OrderSide::Sell => "SELL",
                                },
                                order.price.unwrap_or(0.0),
                                order.amount,
                                order.id,
                                order
                                    .info
                                    .get("reduceOnly")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false)
                            )
                        })
                        .collect();
                    result
                        .executed_new_orders
                        .extend(success_descriptions.clone());
                    log::info!(
                        "[range_grid] {} 补单成功 {} 笔: {}",
                        config_id,
                        success_descriptions.len(),
                        success_descriptions.join("; ")
                    );

                    {
                        let mut guard = state_arc.lock().await;
                        guard.grid_active = true;
                        guard.pending_center_price = None;
                        for order in successful_orders.drain(..) {
                            let client_id = order
                                .info
                                .get("clientOrderId")
                                .or_else(|| order.info.get("client_order_id"))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                            guard
                                .open_orders
                                .insert(order.id.clone(), ActiveOrder::new(order, client_id));
                        }
                    }
                }

                if !failed_orders.is_empty() {
                    let failure_details: Vec<String> = failed_orders
                        .iter()
                        .map(|f| {
                            format!(
                                "{} {:.6} qty {:.6} reduce_only={} -> {} ({})",
                                match f.order_request.side {
                                    OrderSide::Buy => "BUY",
                                    OrderSide::Sell => "SELL",
                                },
                                f.order_request.price.unwrap_or(0.0),
                                f.order_request.amount,
                                f.order_request.reduce_only.unwrap_or(false),
                                f.error_message,
                                f.error_code.clone().unwrap_or_default()
                            )
                        })
                        .collect();
                    log::warn!(
                        "[range_grid] {} 补单存在失败 {} 笔: {}",
                        config_id,
                        failure_details.len(),
                        failure_details.join("; ")
                    );
                    self.trigger_full_rebuild(account_id, symbol, config_id, fill_price, state_arc)
                        .await?;
                    result.rebuild_triggered = true;
                    return Ok(result);
                }
            }
            Err(err) => {
                log::error!("[range_grid] {} 补单失败: {}", config_id, err);
                self.trigger_full_rebuild(account_id, symbol, config_id, fill_price, state_arc)
                    .await?;
                result.rebuild_triggered = true;
                return Ok(result);
            }
        }

        if result.rebuild_triggered {
            return Ok(result);
        }

        if let Some((cancel_id, cancel_side, cancel_price)) = cancel_target {
            let side_label = match cancel_side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            };
            result.cancel_logs.push(format!(
                "尝试撤单 {} {} {:.6}",
                cancel_id,
                side_label,
                cancel_price.unwrap_or(0.0)
            ));

            let account = self
                .account_manager
                .get_account(account_id)
                .ok_or_else(|| anyhow!("账户 {} 不存在", account_id))?;

            match account
                .exchange
                .cancel_order(&cancel_id, symbol, self.market_type)
                .await
            {
                Ok(_) => {
                    result.cancel_logs.push(format!("{} 已取消", cancel_id));
                    let mut guard = state_arc.lock().await;
                    guard.open_orders.remove(&cancel_id);
                }
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("Unknown order")
                        || message.contains("-2011")
                        || message.contains("-2013")
                    {
                        result
                            .cancel_logs
                            .push(format!("{} 已缺失 ({} )", cancel_id, message));
                        let mut guard = state_arc.lock().await;
                        guard.open_orders.remove(&cancel_id);
                    } else {
                        log::warn!(
                            "[range_grid] {} 取消订单 {} 失败: {}",
                            config_id,
                            cancel_id,
                            message
                        );
                        result
                            .cancel_logs
                            .push(format!("{} 撤单失败: {}", cancel_id, message));
                        self.trigger_full_rebuild(
                            account_id, symbol, config_id, fill_price, state_arc,
                        )
                        .await?;
                        result.rebuild_triggered = true;
                        return Ok(result);
                    }
                }
            }
        }

        result.cancel_logs.shrink_to_fit();
        result.planned_new_orders.shrink_to_fit();
        result.executed_new_orders.shrink_to_fit();
        Ok(result)
    }
    async fn trigger_full_rebuild(
        &self,
        account_id: &str,
        symbol: &str,
        config_id: &str,
        reference_price: f64,
        state_arc: &Arc<Mutex<PairRuntimeState>>,
    ) -> Result<()> {
        if let Err(err) = self
            .account_manager
            .cancel_all_orders(account_id, Some(symbol))
            .await
        {
            log::warn!(
                "[range_grid] {} 触发全撤时取消全部挂单失败: {}",
                config_id,
                err
            );
        } else {
            log::info!("[range_grid] {} 已触发全撤，等待重建", config_id);
        }

        let mut guard = state_arc.lock().await;
        guard.open_orders.clear();
        guard.grid_active = false;
        guard.need_rebuild = true;
        guard.pending_center_price = Some(reference_price);
        guard.current_plan = None;
        Ok(())
    }

    fn same_price(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-9
    }

    async fn handle_trade(&self, trade: crate::core::types::Trade) -> Result<()> {
        let symbol_key = Self::normalize_symbol(&trade.symbol);
        if let Some(config_id) = self.symbol_map.get(&symbol_key) {
            if !self.allowed_configs.contains(config_id) {
                return Ok(());
            }

            if let Some(state_arc) = {
                let guard = self.states.read().await;
                guard.get(config_id).cloned()
            } {
                let mut guard = state_arc.lock().await;
                guard.current_price = trade.price;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageHandler for RangeGridWsHandler {
    async fn handle_message(&self, message: WsMessage) -> std::result::Result<(), ExchangeError> {
        match message {
            WsMessage::Order(order) => self
                .handle_order_update(order)
                .await
                .map_err(|e| ExchangeError::Other(e.to_string())),
            WsMessage::ExecutionReport(report) => self
                .handle_order_update(Self::order_from_execution_report(report, self.market_type))
                .await
                .map_err(|e| ExchangeError::Other(e.to_string())),
            WsMessage::Trade(trade) => self
                .handle_trade(trade)
                .await
                .map_err(|e| ExchangeError::Other(e.to_string())),
            WsMessage::Error(err) => {
                if err.starts_with("ListenKeyRenewed") {
                    log::info!("[range_grid] {} ListenKey 已更新: {}", self.account_id, err);
                } else {
                    log::warn!(
                        "[range_grid] {} WebSocket错误消息: {}",
                        self.account_id,
                        err
                    );
                }
                Ok(())
            }
            WsMessage::Ticker(ticker) => {
                if let Some(config_id) =
                    self.symbol_map.get(&Self::normalize_symbol(&ticker.symbol))
                {
                    if self.allowed_configs.contains(config_id) {
                        if let Some(state_arc) = {
                            let guard = self.states.read().await;
                            guard.get(config_id).cloned()
                        } {
                            let mut guard = state_arc.lock().await;
                            guard.current_price = ticker.last;
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn handle_error(&self, error: ExchangeError) -> std::result::Result<(), ExchangeError> {
        log::error!(
            "[range_grid] {} WebSocket底层错误: {}",
            self.account_id,
            error
        );
        Ok(())
    }
}

fn snapshot_open_orders(state: &PairRuntimeState) -> String {
    let entries: Vec<String> = state
        .open_orders
        .values()
        .map(|o| {
            format!(
                "{}@{:.6} status={:?}",
                match o.order.side {
                    OrderSide::Buy => "BUY",
                    OrderSide::Sell => "SELL",
                },
                o.order.price.unwrap_or(0.0),
                o.order.status
            )
        })
        .collect();

    if entries.is_empty() {
        "<空>".to_string()
    } else {
        entries.join(", ")
    }
}

fn build_order_request(
    symbol: &str,
    side: OrderSide,
    target_price: f64,
    base_notional: f64,
    precision: &PairPrecision,
    exchange_label: &str,
    tag: &str,
    market_type: MarketType,
    time_in_force: Option<&str>,
    post_only: bool,
) -> Option<OrderRequest> {
    if target_price <= 0.0 {
        return None;
    }

    let price = quantize_value(target_price, precision.price_digits, precision.price_step);
    if price <= 0.0 {
        return None;
    }

    let qty_raw = base_notional / price;
    let quantity = quantize_value(qty_raw, precision.amount_digits, precision.amount_step);
    if quantity <= 0.0 {
        return None;
    }

    if let Some(min) = precision.min_notional {
        if price * quantity < min {
            return None;
        }
    }

    let mut request = OrderRequest::new(
        symbol.to_string(),
        side,
        OrderType::Limit,
        quantity,
        Some(price),
        market_type,
    );
    request.client_order_id = Some(generate_order_id_with_tag(
        "range_grid",
        exchange_label,
        tag,
    ));
    request.post_only = Some(post_only);
    let mut tif = time_in_force.map(|t| t.to_string());
    if post_only && tif.as_deref() != Some("GTX") {
        tif = Some("GTX".to_string());
    }
    if let Some(tif_value) = tif {
        request.time_in_force = Some(tif_value);
    }

    Some(request)
}
