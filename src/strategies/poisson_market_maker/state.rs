use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

use crate::core::types::{Order, OrderSide};

/// 订单流事件
#[derive(Debug, Clone)]
pub struct OrderFlowEvent {
    pub timestamp: DateTime<Utc>,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub event_type: OrderEventType,
}

#[derive(Debug, Clone)]
pub enum OrderEventType {
    NewOrder,
    Trade,
    Cancel,
}

/// 泊松参数
#[derive(Debug, Clone)]
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

/// 交易对信息
#[derive(Debug, Clone)]
pub struct SymbolInfo {
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
    pub price_precision: usize,
    pub quantity_precision: usize,
}

#[derive(Debug, Clone)]
pub struct MMStrategyState {
    pub inventory: f64,
    pub long_inventory: f64,
    pub short_inventory: f64,
    pub avg_price: f64,
    pub long_avg_price: f64,
    pub short_avg_price: f64,
    pub active_buy_orders: HashMap<String, Order>,
    pub active_sell_orders: HashMap<String, Order>,
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

/// 内部订单簿缓存
#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub last_update: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderIntent {
    OpenLong,
    CloseLong,
    OpenShort,
    CloseShort,
}

#[derive(Debug, Clone)]
pub struct OrderSlotInfo {
    pub exchange_id: String,
    pub client_id: String,
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
}

impl MMStrategyState {
    pub fn register_order(&mut self, intent: OrderIntent, client_id: String, order: Order) {
        // 若已有同意图挂单，先移除
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

    pub fn detach_order_by_exchange(&mut self, exchange_id: &str) -> Option<(OrderIntent, Order)> {
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

    pub fn detach_order_by_client(&mut self, client_id: &str) -> Option<(OrderIntent, Order)> {
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
