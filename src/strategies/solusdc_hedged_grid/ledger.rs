use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::core::types::OrderSide;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            PositionSide::Long => "LONG",
            PositionSide::Short => "SHORT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderIntent {
    OpenLongBuy,
    CloseLongSell,
    OpenShortSell,
    CloseShortBuy,
}

impl OrderIntent {
    pub fn side(&self) -> OrderSide {
        match self {
            OrderIntent::OpenLongBuy | OrderIntent::CloseShortBuy => OrderSide::Buy,
            OrderIntent::OpenShortSell | OrderIntent::CloseLongSell => OrderSide::Sell,
        }
    }

    pub fn position_side(&self) -> PositionSide {
        match self {
            OrderIntent::OpenLongBuy | OrderIntent::CloseLongSell => PositionSide::Long,
            OrderIntent::OpenShortSell | OrderIntent::CloseShortBuy => PositionSide::Short,
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(self, OrderIntent::OpenLongBuy | OrderIntent::OpenShortSell)
    }

    pub fn is_close(&self) -> bool {
        matches!(
            self,
            OrderIntent::CloseLongSell | OrderIntent::CloseShortBuy
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrderRecord {
    pub id: String,
    pub intent: OrderIntent,
    pub price: f64,
    pub qty: f64,
    pub filled_qty: f64,
    pub created_at: DateTime<Utc>,
    pub retries: u32,
}

#[derive(Debug, Clone)]
pub struct OrderSlot {
    pub id: String,
    pub intent: OrderIntent,
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone)]
pub struct GridSideBook {
    pub side: OrderSide,
    pub slots: Vec<OrderSlot>,
}

impl GridSideBook {
    pub fn new(side: OrderSide) -> Self {
        Self {
            side,
            slots: Vec::new(),
        }
    }

    pub fn insert(&mut self, slot: OrderSlot) {
        self.slots.push(slot);
        self.sort();
    }

    pub fn remove(&mut self, order_id: &str) -> Option<OrderSlot> {
        if let Some(idx) = self.slots.iter().position(|slot| slot.id == order_id) {
            Some(self.slots.remove(idx))
        } else {
            None
        }
    }

    pub fn update_qty(&mut self, order_id: &str, qty: f64) {
        if let Some(slot) = self.slots.iter_mut().find(|slot| slot.id == order_id) {
            slot.qty = qty;
        }
    }

    pub fn nearest(&self) -> Option<&OrderSlot> {
        self.slots.first()
    }

    pub fn farthest(&self) -> Option<&OrderSlot> {
        self.slots.last()
    }

    pub fn nearest_price(&self) -> Option<f64> {
        self.nearest().map(|slot| slot.price)
    }

    pub fn farthest_price(&self) -> Option<f64> {
        self.farthest().map(|slot| slot.price)
    }

    pub fn count_by_intent(&self, intent: OrderIntent) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .count()
    }

    pub fn total_qty_for_intent(&self, intent: OrderIntent) -> f64 {
        self.slots
            .iter()
            .filter(|slot| slot.intent == intent)
            .map(|slot| slot.qty)
            .sum()
    }

    fn sort(&mut self) {
        match self.side {
            OrderSide::Buy => {
                self.slots.sort_by(|a, b| {
                    b.price
                        .partial_cmp(&a.price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            OrderSide::Sell => {
                self.slots.sort_by(|a, b| {
                    a.price
                        .partial_cmp(&b.price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderLedger {
    pub orders: HashMap<String, OrderRecord>,
}

impl OrderLedger {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
        }
    }

    pub fn insert(&mut self, record: OrderRecord) {
        self.orders.insert(record.id.clone(), record);
    }

    pub fn remove(&mut self, order_id: &str) -> Option<OrderRecord> {
        self.orders.remove(order_id)
    }

    pub fn get(&self, order_id: &str) -> Option<&OrderRecord> {
        self.orders.get(order_id)
    }

    pub fn get_mut(&mut self, order_id: &str) -> Option<&mut OrderRecord> {
        self.orders.get_mut(order_id)
    }

    pub fn all_ids(&self) -> Vec<String> {
        self.orders.keys().cloned().collect()
    }
}
