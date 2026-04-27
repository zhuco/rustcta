use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::core::types::{MarketType, OrderSide, OrderType};

#[derive(Debug, Clone)]
pub struct FillResult {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub market_type: MarketType,
    pub quantity: f64,
    pub price: f64,
    pub is_maker: bool,
    pub fee_paid: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct FundingSettlement {
    pub symbol: String,
    pub rate: f64,
    pub mark_price: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct LedgerPosition {
    pub symbol: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub last_update: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct BacktestLedger {
    settlement_currency: String,
    cash_balance: f64,
    total_fees_paid: f64,
    total_funding_paid: f64,
    realized_pnl: f64,
    positions: HashMap<String, LedgerPosition>,
}

impl BacktestLedger {
    pub fn new(settlement_currency: &str, initial_cash: f64) -> Self {
        Self {
            settlement_currency: settlement_currency.to_string(),
            cash_balance: initial_cash,
            total_fees_paid: 0.0,
            total_funding_paid: 0.0,
            realized_pnl: 0.0,
            positions: HashMap::new(),
        }
    }

    pub fn apply_fill(&mut self, fill: FillResult) -> Result<()> {
        let signed_qty = match fill.side {
            OrderSide::Buy => fill.quantity,
            OrderSide::Sell => -fill.quantity,
        };

        self.cash_balance -= fill.fee_paid;
        self.total_fees_paid += fill.fee_paid;

        let mut remove_position = false;
        let entry = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert(LedgerPosition {
                symbol: fill.symbol.clone(),
                quantity: 0.0,
                entry_price: fill.price,
                mark_price: fill.price,
                last_update: fill.timestamp,
            });

        let current_qty = entry.quantity;
        let new_qty = current_qty + signed_qty;

        if current_qty.abs() < f64::EPSILON {
            entry.entry_price = fill.price;
        } else if current_qty.signum() == signed_qty.signum() {
            let weighted_notional =
                entry.entry_price * current_qty.abs() + fill.price * fill.quantity.abs();
            entry.entry_price = weighted_notional / new_qty.abs().max(f64::EPSILON);
        } else {
            let closed_qty = current_qty.abs().min(fill.quantity.abs());
            let realized = (fill.price - entry.entry_price) * closed_qty * current_qty.signum();
            self.cash_balance += realized;
            self.realized_pnl += realized;

            if new_qty.abs() < f64::EPSILON {
                remove_position = true;
            } else if new_qty.signum() != current_qty.signum() {
                entry.entry_price = fill.price;
            }
        }

        entry.quantity = new_qty;
        entry.mark_price = fill.price;
        entry.last_update = fill.timestamp;

        if remove_position {
            self.positions.remove(&fill.symbol);
        }

        Ok(())
    }

    pub fn apply_mark_price(&mut self, symbol: &str, mark_price: f64, timestamp: DateTime<Utc>) {
        if let Some(position) = self.positions.get_mut(symbol) {
            position.mark_price = mark_price;
            position.last_update = timestamp;
        }
    }

    pub fn apply_funding(&mut self, funding: FundingSettlement) {
        if let Some(position) = self.positions.get(&funding.symbol) {
            let notional = position.quantity * funding.mark_price;
            let payment = notional * funding.rate;
            self.cash_balance -= payment;
            self.total_funding_paid += payment;
        }
    }

    pub fn position(&self, symbol: &str) -> Option<&LedgerPosition> {
        self.positions.get(symbol)
    }

    pub fn unrealized_pnl(&self, symbol: &str) -> f64 {
        self.positions
            .get(symbol)
            .map(|position| (position.mark_price - position.entry_price) * position.quantity)
            .unwrap_or(0.0)
    }

    pub fn cash_balance(&self) -> f64 {
        self.cash_balance
    }

    pub fn total_funding_paid(&self) -> f64 {
        self.total_funding_paid
    }

    pub fn total_fees_paid(&self) -> f64 {
        self.total_fees_paid
    }

    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }

    pub fn settlement_currency(&self) -> &str {
        &self.settlement_currency
    }

    pub fn positions(&self) -> impl Iterator<Item = &LedgerPosition> {
        self.positions.values()
    }

    pub fn total_equity(&self) -> f64 {
        self.cash_balance
            + self
                .positions
                .values()
                .map(|position| (position.mark_price - position.entry_price) * position.quantity)
                .sum::<f64>()
    }
}
