use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct DepthDelta {
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OrderBookState {
    symbol: String,
    max_depth: usize,
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_update_id: Option<u64>,
    timestamp: Option<DateTime<Utc>>,
}

impl OrderBookState {
    pub fn new(symbol: &str, max_depth: usize) -> Self {
        Self {
            symbol: symbol.to_string(),
            max_depth,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: None,
            timestamp: None,
        }
    }

    pub fn apply_snapshot(
        &mut self,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
        last_update_id: u64,
        timestamp: DateTime<Utc>,
    ) {
        self.bids.clear();
        self.asks.clear();
        for level in bids {
            self.upsert_level(Side::Bid, level);
        }
        for level in asks {
            self.upsert_level(Side::Ask, level);
        }
        self.last_update_id = Some(last_update_id);
        self.timestamp = Some(timestamp);
        self.trim();
    }

    pub fn apply_delta(&mut self, delta: DepthDelta) -> Result<()> {
        if let Some(last) = self.last_update_id {
            if delta.first_update_id > last + 1 {
                return Err(anyhow!(
                    "depth gap detected for {}: expected <= {}, got {}",
                    self.symbol,
                    last + 1,
                    delta.first_update_id
                ));
            }
        }

        for level in delta.bids {
            self.upsert_level(Side::Bid, level);
        }
        for level in delta.asks {
            self.upsert_level(Side::Ask, level);
        }
        self.last_update_id = Some(delta.final_update_id);
        self.timestamp = Some(delta.timestamp);
        self.trim();

        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) {
            if bid[0] >= ask[0] {
                return Err(anyhow!("crossed book detected for {}", self.symbol));
            }
        }

        Ok(())
    }

    pub fn best_bid(&self) -> Option<[f64; 2]> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, qty)| [decode_price(*price), *qty])
    }

    pub fn best_ask(&self) -> Option<[f64; 2]> {
        self.asks
            .iter()
            .next()
            .map(|(price, qty)| [decode_price(*price), *qty])
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    pub fn bid_qty_at(&self, price: f64) -> f64 {
        self.bids.get(&encode_price(price)).copied().unwrap_or(0.0)
    }

    pub fn ask_qty_at(&self, price: f64) -> f64 {
        self.asks.get(&encode_price(price)).copied().unwrap_or(0.0)
    }

    pub fn consume_bid_qty(&mut self, price: f64, quantity: f64) -> f64 {
        consume_level_qty(&mut self.bids, encode_price(price), quantity)
    }

    pub fn consume_ask_qty(&mut self, price: f64, quantity: f64) -> f64 {
        consume_level_qty(&mut self.asks, encode_price(price), quantity)
    }

    pub fn consume_market_buy_qty(&mut self, quantity: f64) -> Vec<[f64; 2]> {
        consume_market_qty(&mut self.asks, quantity, true)
    }

    pub fn consume_market_sell_qty(&mut self, quantity: f64) -> Vec<[f64; 2]> {
        consume_market_qty(&mut self.bids, quantity, false)
    }

    fn upsert_level(&mut self, side: Side, level: [f64; 2]) {
        let price_key = encode_price(level[0]);
        let target = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if level[1] <= 0.0 {
            target.remove(&price_key);
        } else {
            target.insert(price_key, level[1]);
        }
    }

    fn trim(&mut self) {
        while self.bids.len() > self.max_depth {
            let first = self.bids.keys().next().copied();
            if let Some(first) = first {
                self.bids.remove(&first);
            }
        }
        while self.asks.len() > self.max_depth {
            let last = self.asks.keys().next_back().copied();
            if let Some(last) = last {
                self.asks.remove(&last);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Side {
    Bid,
    Ask,
}

fn encode_price(price: f64) -> i64 {
    (price * 100_000_000.0).round() as i64
}

fn decode_price(price: i64) -> f64 {
    price as f64 / 100_000_000.0
}

fn consume_level_qty(levels: &mut BTreeMap<i64, f64>, price: i64, quantity: f64) -> f64 {
    if quantity <= 0.0 {
        return 0.0;
    }

    let Some(level_qty) = levels.get_mut(&price) else {
        return 0.0;
    };

    let consumed = level_qty.min(quantity);
    *level_qty -= consumed;
    if *level_qty <= 1e-12 {
        levels.remove(&price);
    }

    consumed
}

fn consume_market_qty(
    levels: &mut BTreeMap<i64, f64>,
    quantity: f64,
    ascending: bool,
) -> Vec<[f64; 2]> {
    if quantity <= 0.0 {
        return Vec::new();
    }

    let mut remaining = quantity;
    let mut fills = Vec::new();

    while remaining > 1e-12 {
        let next_price = if ascending {
            levels.keys().next().copied()
        } else {
            levels.keys().next_back().copied()
        };
        let Some(price_key) = next_price else {
            break;
        };

        let consumed = consume_level_qty(levels, price_key, remaining);
        if consumed <= 1e-12 {
            break;
        }

        fills.push([decode_price(price_key), consumed]);
        remaining -= consumed;
    }

    fills
}
