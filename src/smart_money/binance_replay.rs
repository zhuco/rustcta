use crate::smart_money::execution_sim::{BookLevel, OrderBookSnapshot};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceBookLevel(pub Decimal, pub Decimal);

impl BinanceBookLevel {
    pub fn price(self) -> Decimal {
        self.0
    }

    pub fn quantity(self) -> Decimal {
        self.1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinanceDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<BinanceBookLevel>,
    pub asks: Vec<BinanceBookLevel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinanceDepthUpdate {
    #[serde(rename = "s")]
    pub symbol: Option<String>,
    #[serde(rename = "E")]
    pub event_time_millis: Option<i64>,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "pu")]
    pub previous_final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<BinanceBookLevel>,
    #[serde(rename = "a")]
    pub asks: Vec<BinanceBookLevel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayUpdateOutcome {
    Applied,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceReplayError {
    InvalidLevel {
        price: Decimal,
        quantity: Decimal,
    },
    InvalidUpdateRange {
        first_update_id: u64,
        final_update_id: u64,
        previous_final_update_id: u64,
    },
    SnapshotBridgeGap {
        snapshot_last_update_id: u64,
        first_update_id: u64,
        final_update_id: u64,
    },
    UpdateGap {
        expected_first_update_id: u64,
        expected_previous_final_update_id: u64,
        first_update_id: u64,
        previous_final_update_id: u64,
    },
    SymbolMismatch {
        expected: String,
        actual: String,
    },
    CrossedBook {
        best_bid: Decimal,
        best_ask: Decimal,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinanceOrderBookReplay {
    symbol: String,
    last_update_id: u64,
    ts: DateTime<Utc>,
    has_applied_update: bool,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl BinanceOrderBookReplay {
    pub fn from_snapshot(
        symbol: impl Into<String>,
        snapshot: BinanceDepthSnapshot,
        ts: DateTime<Utc>,
    ) -> Result<Self, BinanceReplayError> {
        let mut book = Self {
            symbol: symbol.into(),
            last_update_id: snapshot.last_update_id,
            ts,
            has_applied_update: false,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };

        for level in snapshot.bids {
            book.insert_snapshot_level(true, level)?;
        }
        for level in snapshot.asks {
            book.insert_snapshot_level(false, level)?;
        }
        book.validate_not_crossed()?;
        Ok(book)
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.ts
    }

    pub fn apply_update(
        &mut self,
        update: BinanceDepthUpdate,
    ) -> Result<ReplayUpdateOutcome, BinanceReplayError> {
        if let Some(actual) = update.symbol.as_deref() {
            if actual != self.symbol {
                return Err(BinanceReplayError::SymbolMismatch {
                    expected: self.symbol.clone(),
                    actual: actual.to_string(),
                });
            }
        }

        validate_update_range(&update)?;

        if update.final_update_id <= self.last_update_id {
            return Ok(ReplayUpdateOutcome::Stale);
        }

        if self.has_applied_update {
            let expected = self.last_update_id + 1;
            if update.previous_final_update_id != self.last_update_id
                || update.first_update_id != expected
            {
                return Err(BinanceReplayError::UpdateGap {
                    expected_first_update_id: expected,
                    expected_previous_final_update_id: self.last_update_id,
                    first_update_id: update.first_update_id,
                    previous_final_update_id: update.previous_final_update_id,
                });
            }
        } else {
            let expected = self.last_update_id + 1;
            if update.first_update_id > expected || update.final_update_id < expected {
                return Err(BinanceReplayError::SnapshotBridgeGap {
                    snapshot_last_update_id: self.last_update_id,
                    first_update_id: update.first_update_id,
                    final_update_id: update.final_update_id,
                });
            }
        }

        for level in update.bids {
            self.apply_diff_level(true, level)?;
        }
        for level in update.asks {
            self.apply_diff_level(false, level)?;
        }

        self.last_update_id = update.final_update_id;
        self.has_applied_update = true;
        if let Some(ts) = update
            .event_time_millis
            .and_then(DateTime::<Utc>::from_timestamp_millis)
        {
            self.ts = ts;
        }
        self.validate_not_crossed()?;

        Ok(ReplayUpdateOutcome::Applied)
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    pub fn best_bid_level(&self) -> Option<BookLevel> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, quantity)| BookLevel {
                price: *price,
                quantity: *quantity,
            })
    }

    pub fn best_ask_level(&self) -> Option<BookLevel> {
        self.asks.iter().next().map(|(price, quantity)| BookLevel {
            price: *price,
            quantity: *quantity,
        })
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        Some((self.best_bid()? + self.best_ask()?) / Decimal::new(2, 0))
    }

    pub fn to_order_book_snapshot(&self) -> OrderBookSnapshot {
        self.to_order_book_snapshot_with_depth(usize::MAX)
    }

    pub fn to_order_book_snapshot_with_depth(&self, depth: usize) -> OrderBookSnapshot {
        OrderBookSnapshot {
            symbol: self.symbol.clone(),
            ts: self.ts,
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(price, quantity)| BookLevel {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(price, quantity)| BookLevel {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
        }
    }

    fn insert_snapshot_level(
        &mut self,
        is_bid: bool,
        level: BinanceBookLevel,
    ) -> Result<(), BinanceReplayError> {
        validate_snapshot_level(level)?;
        let side = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };
        side.insert(level.price(), level.quantity());
        Ok(())
    }

    fn apply_diff_level(
        &mut self,
        is_bid: bool,
        level: BinanceBookLevel,
    ) -> Result<(), BinanceReplayError> {
        validate_diff_level(level)?;
        let side = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };
        if level.quantity().is_zero() {
            side.remove(&level.price());
        } else {
            side.insert(level.price(), level.quantity());
        }
        Ok(())
    }

    fn validate_not_crossed(&self) -> Result<(), BinanceReplayError> {
        match (self.best_bid(), self.best_ask()) {
            (Some(best_bid), Some(best_ask)) if best_bid >= best_ask => {
                Err(BinanceReplayError::CrossedBook { best_bid, best_ask })
            }
            _ => Ok(()),
        }
    }
}

fn validate_snapshot_level(level: BinanceBookLevel) -> Result<(), BinanceReplayError> {
    if level.price() <= Decimal::ZERO || level.quantity() <= Decimal::ZERO {
        return Err(BinanceReplayError::InvalidLevel {
            price: level.price(),
            quantity: level.quantity(),
        });
    }
    Ok(())
}

fn validate_diff_level(level: BinanceBookLevel) -> Result<(), BinanceReplayError> {
    if level.price() <= Decimal::ZERO || level.quantity() < Decimal::ZERO {
        return Err(BinanceReplayError::InvalidLevel {
            price: level.price(),
            quantity: level.quantity(),
        });
    }
    Ok(())
}

fn validate_update_range(update: &BinanceDepthUpdate) -> Result<(), BinanceReplayError> {
    if update.first_update_id > update.final_update_id
        || update.previous_final_update_id >= update.first_update_id
    {
        return Err(BinanceReplayError::InvalidUpdateRange {
            first_update_id: update.first_update_id,
            final_update_id: update.final_update_id,
            previous_final_update_id: update.previous_final_update_id,
        });
    }
    Ok(())
}
