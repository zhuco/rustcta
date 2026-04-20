use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Instant;

use chrono::Utc;
use log::{info, warn};
use tokio::time::{sleep, timeout, Duration};

use crate::core::error::ExchangeError;
use crate::core::types::{MarketType, Order, OrderRequest, OrderSide, OrderType, TradingPair};
use crate::cta::AccountManager;

use super::config::ExecutionConfig;
use super::order_tracker::{FillEvent, OrderTracker};
use super::signal_generator::TradeSignal;

#[derive(Clone, Copy, Debug, Default)]
pub struct SymbolPrecision {
    pub qty_precision: u32,
    pub price_precision: u32,
    pub step_size: f64,
    pub tick_size: f64,
    pub min_notional: Option<f64>,
}

impl SymbolPrecision {
    pub fn from_trading_pair(pair: &TradingPair) -> Self {
        Self {
            qty_precision: decimals_from_step(pair.step_size),
            price_precision: decimals_from_step(pair.tick_size),
            step_size: pair.step_size,
            tick_size: pair.tick_size,
            min_notional: pair.min_notional,
        }
    }
}

/// 执行结果
pub struct ExecutionReport {
    pub order: Order,
    pub attempts: u32,
    pub used_maker: bool,
    pub slippage: f64,
    pub execution_time_ms: f64,
    pub filled_qty: f64,
    pub average_price: f64,
    pub client_order_id: Option<String>,
}

/// 趋势策略执行引擎
#[derive(Clone)]
pub struct TrendExecutionEngine {
    account_manager: Arc<AccountManager>,
    account_id: String,
    config: ExecutionConfig,
    order_tracker: Arc<OrderTracker>,
    symbol_precisions: Arc<StdRwLock<HashMap<String, SymbolPrecision>>>,
}

impl TrendExecutionEngine {
    pub fn new(
        account_manager: Arc<AccountManager>,
        account_id: impl Into<String>,
        config: ExecutionConfig,
        order_tracker: Arc<OrderTracker>,
        symbol_precisions: Arc<StdRwLock<HashMap<String, SymbolPrecision>>>,
    ) -> Self {
        Self {
            account_manager,
            account_id: account_id.into(),
            config,
            order_tracker,
            symbol_precisions,
        }
    }

    /// 执行交易信号
    pub async fn execute_order(
        &self,
        signal: &TradeSignal,
        size: f64,
    ) -> Result<ExecutionReport, ExchangeError> {
        let account = self
            .account_manager
            .get_account(&self.account_id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", self.account_id)))?;

        let exchange = account.exchange.clone();
        let mut attempts = 0;
        let prefer_maker = true;
        let timeout_secs = self.config.order_timeout_secs.max(1);

        loop {
            attempts += 1;
            let client_id = self.build_entry_client_id(&signal.symbol);
            let receiver = self.order_tracker.watch_client(client_id.clone()).await;
            let request = self.build_order_request(signal, size, prefer_maker, &client_id);
            let used_maker = matches!(request.order_type, OrderType::Limit);
            let start = Instant::now();

            match exchange.create_order(request.clone()).await {
                Ok(order) => {
                    let wait_result = timeout(Duration::from_secs(timeout_secs), receiver).await;
                    match wait_result {
                        Ok(Ok(fill_event)) => {
                            return Ok(self.build_report(
                                order,
                                fill_event,
                                attempts,
                                used_maker,
                                start.elapsed().as_secs_f64() * 1000.0,
                                signal.entry_price,
                            ));
                        }
                        Ok(Err(_)) => {
                            warn!("订单{}跟踪通道被关闭", order.id);
                            return Err(ExchangeError::Other("订单成交回报通道中断".to_string()));
                        }
                        Err(_) => {
                            warn!(
                                "订单 {} 在 {}s 内未完全成交，尝试取消",
                                order.id, timeout_secs
                            );
                            let _ = exchange
                                .cancel_order(&order.id, &signal.symbol, MarketType::Futures)
                                .await;
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        "执行订单失败: {} (attempt={}/{}, maker={})",
                        err, attempts, self.config.max_retries, used_maker
                    );

                    if attempts >= self.config.max_retries {
                        return Err(err);
                    }

                    sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                }
            }
        }
    }

    fn build_order_request(
        &self,
        signal: &TradeSignal,
        size: f64,
        prefer_maker: bool,
        client_id: &str,
    ) -> OrderRequest {
        let precision = self.precision_for(&signal.symbol);
        let mut qty = self.quantize_amount(size, &precision);
        if qty <= 0.0 {
            let min_step = if precision.step_size > 0.0 {
                precision.step_size
            } else {
                1.0 / 10f64.powi(std::cmp::max(1, precision.qty_precision as i32))
            };
            qty = min_step;
        }
        let price = self
            .compute_price(signal, prefer_maker)
            .map(|p| self.quantize_maker_price(p, &precision, &signal.side));
        let order_type = if prefer_maker {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let mut request = OrderRequest::new(
            signal.symbol.clone(),
            signal.side.clone(),
            order_type,
            qty,
            price,
            MarketType::Futures,
        );

        if prefer_maker {
            request.post_only = Some(true);
            request.time_in_force = Some("GTC".to_string());
        } else {
            request.post_only = Some(false);
        }

        request.client_order_id = Some(client_id.to_string());

        request
    }

    fn build_report(
        &self,
        mut order: Order,
        fill: FillEvent,
        attempts: u32,
        used_maker: bool,
        execution_time_ms: f64,
        expected_price: f64,
    ) -> ExecutionReport {
        order.filled = fill.filled_qty.max(order.filled);
        order.remaining = (order.amount - order.filled).max(0.0);
        order.price = Some(fill.average_price.max(1e-8));
        order.status = fill.status.clone();
        order.last_trade_timestamp = Some(fill.event_time);

        let slippage = self.compute_slippage(expected_price, fill.average_price);

        info!(
            "执行订单成功: {} {:?} filled={:.4} attempts={} maker={} slippage={:.4}%",
            order.symbol,
            order.side,
            order.filled,
            attempts,
            used_maker,
            slippage * 100.0
        );

        ExecutionReport {
            order,
            attempts,
            used_maker,
            slippage,
            execution_time_ms,
            filled_qty: fill.filled_qty,
            average_price: fill.average_price,
            client_order_id: fill.client_order_id,
        }
    }

    fn compute_price(&self, signal: &TradeSignal, prefer_maker: bool) -> Option<f64> {
        if !prefer_maker {
            return None;
        }

        let offset = self.config.maker_offset_bps / 10_000.0;
        let adjusted_price = match signal.side {
            OrderSide::Buy => signal.entry_price * (1.0 - offset),
            OrderSide::Sell => signal.entry_price * (1.0 + offset),
        };

        Some(adjusted_price.max(1e-8))
    }

    fn compute_slippage(&self, expected: f64, actual: f64) -> f64 {
        if expected.abs() < f64::EPSILON {
            return 0.0;
        }
        ((actual - expected) / expected).abs()
    }

    pub fn format_amount(&self, symbol: &str, amount: f64) -> f64 {
        let precision = self.precision_for(symbol);
        self.quantize_amount(amount, &precision)
    }

    pub fn format_price(&self, symbol: &str, price: f64) -> f64 {
        let precision = self.precision_for(symbol);
        self.quantize_price(price, &precision)
    }

    pub fn format_price_string(&self, symbol: &str, price: f64) -> String {
        let precision = self.precision_for(symbol);
        let sanitized = trim_with_precision(price, precision.price_precision);
        format_decimal(sanitized, precision.price_precision)
    }

    pub fn format_amount_string(&self, symbol: &str, amount: f64) -> String {
        let precision = self.precision_for(symbol);
        let sanitized = trim_with_precision(amount, precision.qty_precision);
        format_decimal(sanitized, precision.qty_precision)
    }

    fn precision_for(&self, symbol: &str) -> SymbolPrecision {
        self.symbol_precisions
            .read()
            .ok()
            .and_then(|map| map.get(symbol).copied())
            .unwrap_or_default()
    }

    fn quantize_amount(&self, amount: f64, precision: &SymbolPrecision) -> f64 {
        if precision.step_size > 0.0 {
            let quantized = ((amount / precision.step_size).floor()) * precision.step_size;
            return trim_with_precision(quantized, precision.qty_precision);
        }
        if precision.qty_precision == 0 {
            return amount.floor();
        }
        let scale = 10f64.powi(precision.qty_precision as i32);
        ((amount * scale).floor()) / scale
    }

    fn quantize_price(&self, price: f64, precision: &SymbolPrecision) -> f64 {
        if precision.tick_size > 0.0 {
            let quantized = ((price / precision.tick_size).round()) * precision.tick_size;
            return trim_with_precision(quantized, precision.price_precision);
        }
        if precision.price_precision == 0 {
            return price.round();
        }
        let scale = 10f64.powi(precision.price_precision as i32);
        ((price * scale).round()) / scale
    }

    fn quantize_maker_price(
        &self,
        price: f64,
        precision: &SymbolPrecision,
        side: &OrderSide,
    ) -> f64 {
        if precision.tick_size > 0.0 {
            let steps = price / precision.tick_size;
            let adjusted_steps = match side {
                OrderSide::Buy => steps.floor(),
                OrderSide::Sell => steps.ceil(),
            };
            let quantized = (adjusted_steps * precision.tick_size).max(precision.tick_size);
            return trim_with_precision(quantized, precision.price_precision);
        }

        if precision.price_precision == 0 {
            return match side {
                OrderSide::Buy => price.floor(),
                OrderSide::Sell => price.ceil(),
            };
        }

        let scale = 10f64.powi(precision.price_precision as i32);
        let scaled = price * scale;
        let adjusted_scaled = match side {
            OrderSide::Buy => scaled.floor(),
            OrderSide::Sell => scaled.ceil(),
        };
        adjusted_scaled / scale
    }

    fn build_entry_client_id(&self, symbol: &str) -> String {
        let sanitized = symbol.replace('/', "").to_lowercase();
        let suffix = chrono::Utc::now().timestamp_micros();
        let mut id = format!("trdent_{}_{}", sanitized, suffix);
        if id.len() > 30 {
            id.truncate(30);
        }
        id
    }
}

pub fn decimals_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 3;
    }
    let mut decimals = 0;
    let mut scaled = step;
    while (scaled - scaled.round()).abs() > 1e-9 && decimals < 12 {
        scaled *= 10.0;
        decimals += 1;
    }
    decimals
}

fn trim_with_precision(value: f64, precision: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if precision == 0 {
        return value.round();
    }
    let scale = 10f64.powi(precision as i32);
    (value * scale).round() / scale
}

fn format_decimal(value: f64, precision: u32) -> String {
    if precision == 0 {
        format!("{:.0}", value)
    } else {
        format!("{:.*}", precision as usize, value)
    }
}
