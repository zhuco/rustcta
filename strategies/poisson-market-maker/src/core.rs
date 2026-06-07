use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::OrderSide;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonMMConfig {
    pub name: String,
    pub enabled: bool,
    pub version: String,
    pub account: PoissonAccountConfig,
    pub trading: PoissonTradingConfig,
    pub poisson: PoissonModelConfig,
    pub risk: PoissonRiskConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoissonAccountConfig {
    pub account_id: String,
    pub exchange: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonTradingConfig {
    pub symbol: String,
    pub order_size_usdc: f64,
    pub max_inventory: f64,
    pub min_spread_bp: f64,
    pub max_spread_bp: f64,
    pub refresh_interval_secs: u64,
    pub price_precision: usize,
    pub quantity_precision: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonModelConfig {
    pub observation_window_secs: u64,
    pub min_samples: usize,
    pub smoothing_alpha: f64,
    pub depth_levels: usize,
    pub confidence_interval: f64,
    pub initial_lambda: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonRiskConfig {
    pub max_unrealized_loss: f64,
    pub max_daily_loss: f64,
    pub inventory_skew_limit: f64,
    pub stop_loss_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderFlowEvent {
    pub timestamp: DateTime<Utc>,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub event_type: OrderEventType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderEventType {
    NewOrder,
    Trade,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonParameters {
    pub lambda_bid: f64,
    pub lambda_ask: f64,
    pub mu_bid: f64,
    pub mu_ask: f64,
    pub avg_queue_bid: f64,
    pub avg_queue_ask: f64,
    pub last_update: DateTime<Utc>,
    pub last_trade_time: Option<DateTime<Utc>>,
}

impl PoissonParameters {
    pub fn from_initial_lambda(initial_lambda: f64, now: DateTime<Utc>) -> Self {
        let initial_mu = initial_lambda * 1.2;
        let queue = initial_lambda / (initial_mu - initial_lambda).max(0.1);
        Self {
            lambda_bid: initial_lambda,
            lambda_ask: initial_lambda,
            mu_bid: initial_mu,
            mu_ask: initial_mu,
            avg_queue_bid: queue,
            avg_queue_ask: queue,
            last_update: now,
            last_trade_time: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolInfo {
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
    pub price_precision: usize,
    pub quantity_precision: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalOrderBook {
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub last_update: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderIntent {
    OpenLong,
    CloseLong,
    OpenShort,
    CloseShort,
}

impl OrderIntent {
    pub fn side(&self) -> OrderSide {
        match self {
            OrderIntent::OpenLong | OrderIntent::CloseShort => OrderSide::Buy,
            OrderIntent::OpenShort | OrderIntent::CloseLong => OrderSide::Sell,
        }
    }

    pub fn counterpart(&self) -> OrderIntent {
        match self {
            OrderIntent::OpenLong => OrderIntent::CloseLong,
            OrderIntent::CloseLong => OrderIntent::OpenLong,
            OrderIntent::OpenShort => OrderIntent::CloseShort,
            OrderIntent::CloseShort => OrderIntent::OpenShort,
        }
    }

    pub fn tag(&self) -> &'static str {
        match self {
            OrderIntent::OpenLong => "OL",
            OrderIntent::CloseLong => "CL",
            OrderIntent::OpenShort => "OS",
            OrderIntent::CloseShort => "CS",
        }
    }

    pub fn position_side(&self) -> &'static str {
        match self {
            OrderIntent::OpenLong | OrderIntent::CloseLong => "LONG",
            OrderIntent::OpenShort | OrderIntent::CloseShort => "SHORT",
        }
    }

    pub fn reduce_only(&self) -> bool {
        matches!(self, OrderIntent::CloseLong | OrderIntent::CloseShort)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoissonOrderStatus {
    New,
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonOrderRecord {
    pub id: String,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub price: Option<f64>,
    pub quantity: f64,
    pub status: PoissonOrderStatus,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderSlotInfo {
    pub exchange_id: String,
    pub client_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MMStrategyState {
    pub inventory: f64,
    pub long_inventory: f64,
    pub short_inventory: f64,
    pub avg_price: f64,
    pub long_avg_price: f64,
    pub short_avg_price: f64,
    pub active_buy_orders: HashMap<String, PoissonOrderRecord>,
    pub active_sell_orders: HashMap<String, PoissonOrderRecord>,
    pub buy_client_to_exchange: HashMap<String, String>,
    pub buy_exchange_to_client: HashMap<String, String>,
    pub sell_client_to_exchange: HashMap<String, String>,
    pub sell_exchange_to_client: HashMap<String, String>,
    pub order_slots: HashMap<OrderIntent, OrderSlotInfo>,
    pub client_to_slot: HashMap<String, OrderIntent>,
    pub exchange_to_slot: HashMap<String, OrderIntent>,
    pub total_pnl: f64,
    pub daily_pnl: f64,
    pub trade_count: u64,
    pub start_time: DateTime<Utc>,
}

impl MMStrategyState {
    pub fn new(start_time: DateTime<Utc>) -> Self {
        Self {
            inventory: 0.0,
            long_inventory: 0.0,
            short_inventory: 0.0,
            avg_price: 0.0,
            long_avg_price: 0.0,
            short_avg_price: 0.0,
            active_buy_orders: HashMap::new(),
            active_sell_orders: HashMap::new(),
            buy_client_to_exchange: HashMap::new(),
            buy_exchange_to_client: HashMap::new(),
            sell_client_to_exchange: HashMap::new(),
            sell_exchange_to_client: HashMap::new(),
            order_slots: HashMap::new(),
            client_to_slot: HashMap::new(),
            exchange_to_slot: HashMap::new(),
            total_pnl: 0.0,
            daily_pnl: 0.0,
            trade_count: 0,
            start_time,
        }
    }

    pub fn register_order(
        &mut self,
        intent: OrderIntent,
        client_id: String,
        order: PoissonOrderRecord,
    ) {
        self.unregister_intent(intent);

        let exchange_id = order.id.clone();
        match intent.side() {
            OrderSide::Buy => {
                self.active_buy_orders
                    .insert(exchange_id.clone(), order.clone());
                self.buy_client_to_exchange
                    .insert(client_id.clone(), exchange_id.clone());
                self.buy_exchange_to_client
                    .insert(exchange_id.clone(), client_id.clone());
            }
            OrderSide::Sell => {
                self.active_sell_orders
                    .insert(exchange_id.clone(), order.clone());
                self.sell_client_to_exchange
                    .insert(client_id.clone(), exchange_id.clone());
                self.sell_exchange_to_client
                    .insert(exchange_id.clone(), client_id.clone());
            }
        }

        self.order_slots.insert(
            intent,
            OrderSlotInfo {
                exchange_id: exchange_id.clone(),
                client_id: client_id.clone(),
            },
        );
        self.client_to_slot.insert(client_id, intent);
        self.exchange_to_slot.insert(exchange_id, intent);
    }

    pub fn unregister_intent(&mut self, intent: OrderIntent) {
        if let Some(info) = self.order_slots.remove(&intent) {
            self.client_to_slot.remove(&info.client_id);
            self.exchange_to_slot.remove(&info.exchange_id);
            match intent.side() {
                OrderSide::Buy => {
                    self.active_buy_orders.remove(&info.exchange_id);
                    if let Some(client_id) = self.buy_exchange_to_client.remove(&info.exchange_id) {
                        self.buy_client_to_exchange.remove(&client_id);
                    }
                }
                OrderSide::Sell => {
                    self.active_sell_orders.remove(&info.exchange_id);
                    if let Some(client_id) = self.sell_exchange_to_client.remove(&info.exchange_id)
                    {
                        self.sell_client_to_exchange.remove(&client_id);
                    }
                }
            }
        }
    }

    pub fn detach_order_by_exchange(
        &mut self,
        exchange_id: &str,
    ) -> Option<(OrderIntent, PoissonOrderRecord)> {
        if let Some(intent) = self.exchange_to_slot.remove(exchange_id) {
            if let Some(info) = self.order_slots.remove(&intent) {
                self.client_to_slot.remove(&info.client_id);
            }
            let order = match intent.side() {
                OrderSide::Buy => {
                    if let Some(client_id) = self.buy_exchange_to_client.remove(exchange_id) {
                        self.buy_client_to_exchange.remove(&client_id);
                    }
                    self.active_buy_orders.remove(exchange_id)
                }
                OrderSide::Sell => {
                    if let Some(client_id) = self.sell_exchange_to_client.remove(exchange_id) {
                        self.sell_client_to_exchange.remove(&client_id);
                    }
                    self.active_sell_orders.remove(exchange_id)
                }
            };
            if let Some(order) = order {
                return Some((intent, order));
            }
        }
        None
    }

    pub fn detach_order_by_client(
        &mut self,
        client_id: &str,
    ) -> Option<(OrderIntent, PoissonOrderRecord)> {
        if let Some(intent) = self.client_to_slot.remove(client_id) {
            if let Some(info) = self.order_slots.remove(&intent) {
                self.exchange_to_slot.remove(&info.exchange_id);
            }
            let order = match intent.side() {
                OrderSide::Buy => {
                    if let Some(exchange_id) = self.buy_client_to_exchange.remove(client_id) {
                        self.buy_exchange_to_client.remove(&exchange_id);
                        self.active_buy_orders.remove(&exchange_id)
                    } else {
                        None
                    }
                }
                OrderSide::Sell => {
                    if let Some(exchange_id) = self.sell_client_to_exchange.remove(client_id) {
                        self.sell_exchange_to_client.remove(&exchange_id);
                        self.active_sell_orders.remove(&exchange_id)
                    } else {
                        None
                    }
                }
            };
            if let Some(order) = order {
                return Some((intent, order));
            }
        }
        None
    }

    pub fn trim_slot_orders(&mut self) {
        let mut valid_ids: HashSet<String> = HashSet::new();
        valid_ids.extend(self.active_buy_orders.keys().cloned());
        valid_ids.extend(self.active_sell_orders.keys().cloned());

        self.order_slots.retain(|intent, info| {
            if valid_ids.contains(&info.exchange_id) {
                self.exchange_to_slot
                    .insert(info.exchange_id.clone(), *intent);
                self.client_to_slot.insert(info.client_id.clone(), *intent);
                true
            } else {
                false
            }
        });

        self.exchange_to_slot
            .retain(|exchange_id, _| valid_ids.contains(exchange_id));
        self.client_to_slot.retain(|client_id, intent| {
            self.order_slots
                .get(intent)
                .map(|info| info.client_id == *client_id)
                .unwrap_or(false)
        });
        self.buy_client_to_exchange
            .retain(|client_id, exchange_id| {
                valid_ids.contains(exchange_id)
                    && self
                        .order_slots
                        .values()
                        .any(|info| info.client_id == *client_id)
            });
        self.buy_exchange_to_client
            .retain(|exchange_id, client_id| {
                valid_ids.contains(exchange_id)
                    && self.order_slots.values().any(|info| {
                        info.exchange_id == *exchange_id && info.client_id == *client_id
                    })
            });
        self.sell_client_to_exchange
            .retain(|client_id, exchange_id| {
                valid_ids.contains(exchange_id)
                    && self
                        .order_slots
                        .values()
                        .any(|info| info.client_id == *client_id)
            });
        self.sell_exchange_to_client
            .retain(|exchange_id, client_id| {
                valid_ids.contains(exchange_id)
                    && self.order_slots.values().any(|info| {
                        info.exchange_id == *exchange_id && info.client_id == *client_id
                    })
            });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceRoundingSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpreadQuote {
    pub bid_spread: f64,
    pub ask_spread: f64,
    pub inventory_ratio: f64,
    pub activity_factor: f64,
    pub urgency_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderDraft {
    pub intent: OrderIntent,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub post_only: bool,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelDraft {
    pub intent: OrderIntent,
    pub exchange_id: String,
    pub client_id: Option<String>,
    pub reason: CancelReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    ReplacePrice,
    ReduceMode,
    DuplicateOrders,
    CounterpartFilled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonOrderPlan {
    pub orders: Vec<OrderDraft>,
    pub cancels: Vec<CancelDraft>,
    pub cancel_all: bool,
    pub can_buy: bool,
    pub can_sell: bool,
    pub reduce_buy: bool,
    pub reduce_sell: bool,
    pub target_buy_price: f64,
    pub target_sell_price: f64,
    pub order_quantity: f64,
}

pub fn tick_size(config: &PoissonTradingConfig, symbol_info: Option<&SymbolInfo>) -> f64 {
    symbol_info
        .map(|info| info.tick_size)
        .filter(|tick| *tick > 0.0)
        .unwrap_or_else(|| 1.0 / 10_f64.powi(config.price_precision as i32))
}

pub fn quantity_step_size(config: &PoissonTradingConfig, symbol_info: Option<&SymbolInfo>) -> f64 {
    symbol_info
        .map(|info| info.step_size)
        .filter(|step| *step > 0.0)
        .unwrap_or_else(|| 1.0 / 10_f64.powi(config.quantity_precision as i32))
}

pub fn round_price(
    price: f64,
    config: &PoissonTradingConfig,
    symbol_info: Option<&SymbolInfo>,
) -> f64 {
    let precision = symbol_info
        .map(|info| info.price_precision)
        .unwrap_or(config.price_precision);
    let multiplier = 10_f64.powi(precision as i32);
    (price * multiplier).round() / multiplier
}

pub fn round_price_for_side(
    price: f64,
    side: PriceRoundingSide,
    config: &PoissonTradingConfig,
    symbol_info: Option<&SymbolInfo>,
) -> f64 {
    let precision = symbol_info
        .map(|info| info.price_precision)
        .unwrap_or(config.price_precision);
    let multiplier = 10_f64.powi(precision as i32);
    match side {
        PriceRoundingSide::Bid => (price * multiplier).floor() / multiplier,
        PriceRoundingSide::Ask => (price * multiplier).ceil() / multiplier,
    }
}

pub fn round_quantity(
    quantity: f64,
    config: &PoissonTradingConfig,
    symbol_info: Option<&SymbolInfo>,
) -> f64 {
    if quantity <= 0.0 {
        return 0.0;
    }
    let step = quantity_step_size(config, symbol_info);
    if step <= 0.0 {
        return 0.0;
    }
    (quantity / step).floor() * step
}

pub fn max_inventory_quantity(max_inventory_notional: f64, current_price: f64) -> f64 {
    if max_inventory_notional <= 0.0 || current_price <= 0.0 {
        return 0.0;
    }
    max_inventory_notional / current_price
}

pub fn close_order_quantity(
    position_quantity: f64,
    order_quantity: f64,
    config: &PoissonTradingConfig,
    symbol_info: Option<&SymbolInfo>,
) -> f64 {
    round_quantity(position_quantity.min(order_quantity), config, symbol_info)
}

pub fn should_refresh_orders(
    state: &MMStrategyState,
    current_price: f64,
    last_bid_price: f64,
    last_ask_price: f64,
    now: DateTime<Utc>,
) -> bool {
    if state.active_buy_orders.is_empty() && state.active_sell_orders.is_empty() {
        return true;
    }
    let stale_buy = state
        .active_buy_orders
        .values()
        .any(|order| now.signed_duration_since(order.timestamp).num_seconds() > 30);
    let stale_sell = state
        .active_sell_orders
        .values()
        .any(|order| now.signed_duration_since(order.timestamp).num_seconds() > 30);
    if stale_buy || stale_sell {
        return true;
    }
    if last_bid_price > 0.0 && last_ask_price > 0.0 {
        let mid_price = (last_bid_price + last_ask_price) / 2.0;
        let price_change_pct = ((current_price - mid_price) / mid_price).abs();
        return price_change_pct > 0.0005;
    }
    false
}

pub fn calculate_market_activity_factor(params: &PoissonParameters) -> f64 {
    let avg_lambda = (params.lambda_bid + params.lambda_ask) / 2.0;
    if avg_lambda > 10.0 {
        0.8
    } else if avg_lambda > 5.0 {
        0.9
    } else if avg_lambda < 1.0 {
        1.2
    } else {
        1.0
    }
}

pub fn calculate_optimal_spread(
    config: &PoissonMMConfig,
    params: &PoissonParameters,
    state: &MMStrategyState,
    current_price: f64,
    now: DateTime<Utc>,
) -> SpreadQuote {
    let base_spread = config.trading.min_spread_bp / 10000.0;
    let activity_factor = calculate_market_activity_factor(params);
    let dynamic_spread = base_spread * activity_factor;
    let queue_adjustment = (params.avg_queue_bid + params.avg_queue_ask) / 20.0;
    let time_since_last_trade = params
        .last_trade_time
        .map(|last_trade| now.signed_duration_since(last_trade).num_seconds() as f64)
        .unwrap_or(300.0);
    let urgency_factor = if time_since_last_trade > 30.0 {
        0.9_f64.max(1.0 - (time_since_last_trade - 30.0) / 300.0)
    } else {
        1.0
    };
    let inventory_ratio = if config.trading.max_inventory > 0.0 {
        state.inventory * current_price / config.trading.max_inventory
    } else {
        0.0
    };
    let inventory_penalty = inventory_ratio.abs() * 0.001;
    let mut bid_spread =
        (dynamic_spread + queue_adjustment * base_spread + inventory_penalty) * urgency_factor;
    let mut ask_spread =
        (dynamic_spread + queue_adjustment * base_spread + inventory_penalty) * urgency_factor;

    if inventory_ratio > 0.0 {
        bid_spread *= 1.0 + inventory_ratio * 0.5;
        ask_spread *= 1.0 - inventory_ratio * 0.3;
    } else if inventory_ratio < 0.0 {
        bid_spread *= 1.0 + inventory_ratio.abs() * 0.3;
        ask_spread *= 1.0 - inventory_ratio.abs() * 0.5;
    }

    let max_spread = config.trading.max_spread_bp / 10000.0;
    SpreadQuote {
        bid_spread: bid_spread.min(max_spread),
        ask_spread: ask_spread.min(max_spread),
        inventory_ratio,
        activity_factor,
        urgency_factor,
    }
}

pub fn calculate_rates(events: &[OrderFlowEvent]) -> (f64, f64, f64, f64) {
    let (Some(first), Some(last)) = (events.first(), events.last()) else {
        return (0.0, 0.0, 0.0, 0.0);
    };
    let duration = last
        .timestamp
        .signed_duration_since(first.timestamp)
        .num_seconds() as f64;
    if duration <= 0.0 {
        return (0.0, 0.0, 0.0, 0.0);
    }

    let mut bid_trades = 0_u64;
    let mut ask_trades = 0_u64;
    for event in events {
        if event.event_type == OrderEventType::Trade {
            match event.side {
                OrderSide::Buy => bid_trades += 1,
                OrderSide::Sell => ask_trades += 1,
            }
        }
    }
    let bid_rate = bid_trades as f64 / duration;
    let ask_rate = ask_trades as f64 / duration;
    (bid_rate * 1.5, ask_rate * 1.5, bid_rate, ask_rate)
}

pub fn estimate_poisson_parameters(
    current: &PoissonParameters,
    events: &[OrderFlowEvent],
    smoothing_alpha: f64,
    now: DateTime<Utc>,
) -> PoissonParameters {
    let (lambda_bid, lambda_ask, mu_bid, mu_ask) = calculate_rates(events);
    let avg_queue_bid = if mu_bid > lambda_bid {
        lambda_bid / (mu_bid - lambda_bid)
    } else {
        10.0
    };
    let avg_queue_ask = if mu_ask > lambda_ask {
        lambda_ask / (mu_ask - lambda_ask)
    } else {
        10.0
    };

    if current.lambda_bid == 0.0 {
        return PoissonParameters {
            lambda_bid,
            lambda_ask,
            mu_bid,
            mu_ask,
            avg_queue_bid,
            avg_queue_ask,
            last_update: now,
            last_trade_time: current.last_trade_time,
        };
    }

    let alpha = smoothing_alpha.clamp(0.0, 1.0);
    PoissonParameters {
        lambda_bid: alpha * lambda_bid + (1.0 - alpha) * current.lambda_bid,
        lambda_ask: alpha * lambda_ask + (1.0 - alpha) * current.lambda_ask,
        mu_bid: alpha * mu_bid + (1.0 - alpha) * current.mu_bid,
        mu_ask: alpha * mu_ask + (1.0 - alpha) * current.mu_ask,
        avg_queue_bid: alpha * avg_queue_bid + (1.0 - alpha) * current.avg_queue_bid,
        avg_queue_ask: alpha * avg_queue_ask + (1.0 - alpha) * current.avg_queue_ask,
        last_update: now,
        last_trade_time: current.last_trade_time,
    }
}

pub fn update_poisson_params_on_fill(
    current: &PoissonParameters,
    now: DateTime<Utc>,
) -> PoissonParameters {
    let mut next = current.clone();
    let time_diff = now.signed_duration_since(current.last_update).num_seconds() as f64;
    if time_diff > 0.0 {
        let instant_lambda = 1.0 / time_diff * 60.0;
        next.lambda_bid = next.lambda_bid * 0.9 + instant_lambda * 0.1;
        next.lambda_ask = next.lambda_ask * 0.9 + instant_lambda * 0.1;
    }
    next.last_update = now;
    next.last_trade_time = Some(now);
    next
}

pub fn build_order_plan(
    config: &PoissonMMConfig,
    state: &MMStrategyState,
    orderbook: &LocalOrderBook,
    current_price: f64,
    spreads: &SpreadQuote,
    symbol_info: Option<&SymbolInfo>,
    is_dual_mode: bool,
) -> Option<PoissonOrderPlan> {
    if current_price <= 0.0 || orderbook.bids.is_empty() || orderbook.asks.is_empty() {
        return None;
    }

    let best_bid = orderbook.bids[0].0;
    let best_ask = orderbook.asks[0].0;
    let inventory_cap = max_inventory_quantity(config.trading.max_inventory, current_price) * 0.9;
    let (can_buy, can_sell) = if is_dual_mode {
        (
            state.long_inventory < inventory_cap,
            state.short_inventory < inventory_cap,
        )
    } else {
        (
            state.inventory < inventory_cap,
            state.inventory > -inventory_cap,
        )
    };

    let order_quantity = round_quantity(
        config.trading.order_size_usdc / current_price,
        &config.trading,
        symbol_info,
    );
    let tick = tick_size(&config.trading, symbol_info);
    let target_buy_price = round_price_for_side(
        best_bid * (1.0 - spreads.bid_spread),
        PriceRoundingSide::Bid,
        &config.trading,
        symbol_info,
    );
    let target_sell_price = round_price_for_side(
        best_ask * (1.0 + spreads.ask_spread),
        PriceRoundingSide::Ask,
        &config.trading,
        symbol_info,
    );
    let reduce_threshold = (order_quantity * 0.5).max(0.0001);
    let reduce_buy = state.short_inventory > reduce_threshold;
    let reduce_sell = state.long_inventory > reduce_threshold;

    let mut cancels = Vec::new();
    maybe_replace_existing(
        state,
        OrderIntent::OpenLong,
        target_buy_price,
        tick,
        reduce_buy,
        &mut cancels,
    );
    maybe_replace_existing(
        state,
        OrderIntent::OpenShort,
        target_sell_price,
        tick,
        reduce_sell,
        &mut cancels,
    );

    let buy_count = state.active_buy_orders.len();
    let sell_count = state.active_sell_orders.len();
    let cancel_all = buy_count > 1 || sell_count > 1;
    if cancel_all {
        for (exchange_id, client_id) in &state.buy_exchange_to_client {
            cancels.push(CancelDraft {
                intent: *state
                    .exchange_to_slot
                    .get(exchange_id)
                    .unwrap_or(&OrderIntent::OpenLong),
                exchange_id: exchange_id.clone(),
                client_id: Some(client_id.clone()),
                reason: CancelReason::DuplicateOrders,
            });
        }
        for (exchange_id, client_id) in &state.sell_exchange_to_client {
            cancels.push(CancelDraft {
                intent: *state
                    .exchange_to_slot
                    .get(exchange_id)
                    .unwrap_or(&OrderIntent::OpenShort),
                exchange_id: exchange_id.clone(),
                client_id: Some(client_id.clone()),
                reason: CancelReason::DuplicateOrders,
            });
        }
        return Some(PoissonOrderPlan {
            orders: Vec::new(),
            cancels,
            cancel_all,
            can_buy,
            can_sell,
            reduce_buy,
            reduce_sell,
            target_buy_price,
            target_sell_price,
            order_quantity,
        });
    }

    let mut need_buy_order =
        buy_count == 0 || cancels.iter().any(|c| c.intent.side() == OrderSide::Buy);
    let mut need_sell_order =
        sell_count == 0 || cancels.iter().any(|c| c.intent.side() == OrderSide::Sell);
    if reduce_sell && !reduce_buy {
        need_buy_order = false;
    }
    if reduce_buy && !reduce_sell {
        need_sell_order = false;
    }

    let mut orders = Vec::new();
    if can_buy && need_buy_order {
        let intent = if reduce_buy {
            OrderIntent::CloseShort
        } else {
            OrderIntent::OpenLong
        };
        let price = if reduce_buy {
            round_price_for_side(
                best_bid - tick,
                PriceRoundingSide::Bid,
                &config.trading,
                symbol_info,
            )
        } else {
            target_buy_price
        };
        let quantity = if reduce_buy {
            close_order_quantity(
                state.short_inventory,
                order_quantity,
                &config.trading,
                symbol_info,
            )
        } else {
            order_quantity
        };
        if quantity > 0.0 {
            orders.push(OrderDraft {
                intent,
                side: OrderSide::Buy,
                price,
                quantity,
                post_only: !reduce_buy,
                reduce_only: reduce_buy,
            });
        }
    }
    if can_sell && need_sell_order {
        let intent = if reduce_sell {
            OrderIntent::CloseLong
        } else {
            OrderIntent::OpenShort
        };
        let price = if reduce_sell {
            round_price_for_side(
                best_ask + tick,
                PriceRoundingSide::Ask,
                &config.trading,
                symbol_info,
            )
        } else {
            target_sell_price
        };
        let quantity = if reduce_sell {
            close_order_quantity(
                state.long_inventory,
                order_quantity,
                &config.trading,
                symbol_info,
            )
        } else {
            order_quantity
        };
        if quantity > 0.0 {
            orders.push(OrderDraft {
                intent,
                side: OrderSide::Sell,
                price,
                quantity,
                post_only: !reduce_sell,
                reduce_only: reduce_sell,
            });
        }
    }

    Some(PoissonOrderPlan {
        orders,
        cancels,
        cancel_all,
        can_buy,
        can_sell,
        reduce_buy,
        reduce_sell,
        target_buy_price,
        target_sell_price,
        order_quantity,
    })
}

fn maybe_replace_existing(
    state: &MMStrategyState,
    intent: OrderIntent,
    target_price: f64,
    tick: f64,
    reduce_mode: bool,
    cancels: &mut Vec<CancelDraft>,
) {
    let orders = match intent.side() {
        OrderSide::Buy => &state.active_buy_orders,
        OrderSide::Sell => &state.active_sell_orders,
    };
    if let Some((id, order)) = orders.iter().next() {
        let price_mismatch = order
            .price
            .map(|price| (price - target_price).abs() >= tick * 0.5)
            .unwrap_or(true);
        if price_mismatch || reduce_mode {
            let client_id = match intent.side() {
                OrderSide::Buy => state.buy_exchange_to_client.get(id).cloned(),
                OrderSide::Sell => state.sell_exchange_to_client.get(id).cloned(),
            };
            cancels.push(CancelDraft {
                intent,
                exchange_id: id.clone(),
                client_id,
                reason: if reduce_mode {
                    CancelReason::ReduceMode
                } else {
                    CancelReason::ReplacePrice
                },
            });
        }
    }
}

pub fn filled_intent_cancels_counterpart(
    state: &MMStrategyState,
    filled: OrderIntent,
) -> Option<CancelDraft> {
    let counterpart = filled.counterpart();
    state.order_slots.get(&counterpart).map(|slot| CancelDraft {
        intent: counterpart,
        exchange_id: slot.exchange_id.clone(),
        client_id: Some(slot.client_id.clone()),
        reason: CancelReason::CounterpartFilled,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonRiskLimits {
    pub warning_scale_factor: Option<f64>,
    pub danger_scale_factor: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    pub max_inventory_notional: Option<f64>,
    pub max_daily_loss: Option<f64>,
    pub inventory_skew_limit: Option<f64>,
    pub max_unrealized_loss: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoissonRiskSnapshot {
    pub strategy_name: String,
    pub notional_exposure: f64,
    pub net_inventory: f64,
    pub long_position: f64,
    pub short_position: f64,
    pub inventory_ratio: Option<f64>,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub daily_pnl: Option<f64>,
    pub timestamp: DateTime<Utc>,
    pub risk_limits: PoissonRiskLimits,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PoissonRiskAction {
    None,
    StopLoss { unrealized_pnl: f64 },
    ReduceInventory { inventory_ratio: f64 },
    HaltDailyLoss { daily_pnl: f64 },
}

pub fn build_risk_limits(config: &PoissonMMConfig) -> PoissonRiskLimits {
    PoissonRiskLimits {
        warning_scale_factor: Some(0.8),
        danger_scale_factor: Some(0.4),
        stop_loss_pct: Some(config.risk.stop_loss_pct),
        max_inventory_notional: None,
        max_daily_loss: Some(config.risk.max_daily_loss),
        inventory_skew_limit: Some(config.risk.inventory_skew_limit),
        max_unrealized_loss: Some(config.risk.max_unrealized_loss),
    }
}

pub fn build_risk_snapshot(
    config: &PoissonMMConfig,
    state: &MMStrategyState,
    current_price: f64,
    now: DateTime<Utc>,
) -> PoissonRiskSnapshot {
    let net_inventory = state.long_inventory - state.short_inventory;
    let gross_inventory = state.long_inventory.max(state.short_inventory);
    let inventory_ratio = if config.trading.max_inventory > 0.0 {
        Some((current_price * gross_inventory / config.trading.max_inventory).min(10.0))
    } else {
        None
    };
    PoissonRiskSnapshot {
        strategy_name: config.name.clone(),
        notional_exposure: current_price * net_inventory,
        net_inventory,
        long_position: state.long_inventory,
        short_position: state.short_inventory,
        inventory_ratio,
        realized_pnl: state.total_pnl,
        unrealized_pnl: net_inventory * (current_price - state.avg_price),
        daily_pnl: Some(state.daily_pnl),
        timestamp: now,
        risk_limits: build_risk_limits(config),
    }
}

pub fn evaluate_risk(
    config: &PoissonMMConfig,
    snapshot: &PoissonRiskSnapshot,
) -> PoissonRiskAction {
    if snapshot.unrealized_pnl < -config.risk.max_unrealized_loss {
        return PoissonRiskAction::StopLoss {
            unrealized_pnl: snapshot.unrealized_pnl,
        };
    }
    if let Some(ratio) = snapshot.inventory_ratio {
        if ratio > config.risk.inventory_skew_limit {
            return PoissonRiskAction::ReduceInventory {
                inventory_ratio: ratio,
            };
        }
    }
    if let Some(daily_pnl) = snapshot.daily_pnl {
        if daily_pnl < -config.risk.max_daily_loss {
            return PoissonRiskAction::HaltDailyLoss { daily_pnl };
        }
    }
    PoissonRiskAction::None
}

pub fn adjust_post_only_price(
    side: OrderSide,
    price: f64,
    config: &PoissonTradingConfig,
    symbol_info: Option<&SymbolInfo>,
) -> Option<f64> {
    let tick = tick_size(config, symbol_info);
    if tick <= 0.0 {
        return None;
    }
    match side {
        OrderSide::Buy => {
            let adjusted = price - tick;
            (adjusted > 0.0).then(|| round_price(adjusted, config, symbol_info))
        }
        OrderSide::Sell => Some(round_price(price + tick, config, symbol_info)),
    }
}

pub fn is_post_only_reject(err_msg: &str) -> bool {
    let lower = err_msg.to_ascii_lowercase();
    err_msg.contains("-5022")
        || err_msg.contains("-5021")
        || err_msg.contains("POST_ONLY_REJECT")
        || err_msg.contains("Post Only order will be rejected")
        || lower.contains("post only")
}

pub fn is_order_missing_error(err_msg: &str) -> bool {
    let lower = err_msg.to_ascii_lowercase();
    err_msg.contains("-2011")
        || lower.contains("unknown order")
        || lower.contains("order does not exist")
        || lower.contains("not found")
}

pub fn is_reduce_only_rejection(err_msg: &str) -> bool {
    let lower = err_msg.to_ascii_lowercase();
    err_msg.contains("-2022")
        || lower.contains("reduceonly order is rejected")
        || lower.contains("reduce-only")
        || lower.contains("reduce only")
}

pub fn build_market_streams(exchange: &str, raw_symbol: &str) -> Vec<String> {
    let exchange = exchange.to_ascii_lowercase();
    let stream_symbol = if exchange == "binance" {
        let mut parts = raw_symbol.split('/');
        let base = parts.next().unwrap_or("TOKEN").to_ascii_lowercase();
        let mut quote = parts.next().unwrap_or("USDT").to_ascii_lowercase();
        if quote == "usdc" || quote == "busd" {
            quote = "usdt".to_string();
        }
        format!("{base}{quote}")
    } else {
        raw_symbol.to_ascii_lowercase().replace('/', "")
    };
    vec![
        format!("{stream_symbol}@depth20@100ms"),
        format!("{stream_symbol}@trade"),
    ]
}
