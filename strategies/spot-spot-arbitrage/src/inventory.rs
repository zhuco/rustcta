use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::{RejectionReason, SpotSpotTakerArbitrageConfig, SpotVenue};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperAssetState {
    pub total: f64,
    pub available: f64,
    pub locked_by_exchange: f64,
    pub locally_reserved: f64,
}

impl PaperAssetState {
    pub fn new(amount: f64) -> Self {
        Self {
            total: amount,
            available: amount,
            locked_by_exchange: 0.0,
            locally_reserved: 0.0,
        }
    }

    pub fn effective_available(&self) -> f64 {
        (self.available - self.locally_reserved).max(0.0)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PaperInventory {
    balances: HashMap<(SpotVenue, String), PaperAssetState>,
    pub realized_pnl: f64,
    pub fees_paid: f64,
    pub gross_pnl: f64,
}

impl PaperInventory {
    pub fn from_config(config: &SpotSpotTakerArbitrageConfig) -> Result<Self> {
        let mut inventory = Self::default();
        for (exchange, balances) in &config.initial_balances {
            let exchange = parse_spot_venue(exchange)
                .map_err(|_| anyhow!("unsupported initial balance exchange: {exchange}"))?;
            for (asset, amount) in balances {
                inventory.set_balance(exchange, asset, *amount);
            }
        }
        Ok(inventory)
    }

    pub fn set_balance(&mut self, exchange: SpotVenue, asset: &str, amount: f64) {
        self.balances.insert(
            (exchange, normalize_asset(asset)),
            PaperAssetState::new(amount.max(0.0)),
        );
    }

    pub fn balance(&self, exchange: SpotVenue, asset: &str) -> PaperAssetState {
        self.balances
            .get(&(exchange, normalize_asset(asset)))
            .cloned()
            .unwrap_or_else(|| PaperAssetState::new(0.0))
    }

    pub fn balances_snapshot(&self) -> Vec<(SpotVenue, String, PaperAssetState)> {
        let mut values = self
            .balances
            .iter()
            .map(|((exchange, asset), state)| (*exchange, asset.clone(), state.clone()))
            .collect::<Vec<_>>();
        values.sort_by(|left, right| {
            (left.0.as_str(), left.1.as_str()).cmp(&(right.0.as_str(), right.1.as_str()))
        });
        values
    }

    pub fn reserve(
        &mut self,
        exchange: SpotVenue,
        asset: &str,
        amount: f64,
        reason: RejectionReason,
    ) -> Result<InventoryReservation, RejectionReason> {
        let key = (exchange, normalize_asset(asset));
        let state = self
            .balances
            .entry(key.clone())
            .or_insert_with(|| PaperAssetState::new(0.0));
        if state.effective_available() + 1e-12 < amount {
            return Err(reason);
        }
        state.locally_reserved += amount;
        Ok(InventoryReservation {
            exchange,
            asset: key.1,
            amount,
            released: false,
        })
    }

    pub fn release(&mut self, reservation: &mut InventoryReservation) {
        if reservation.released {
            return;
        }
        if let Some(state) = self
            .balances
            .get_mut(&(reservation.exchange, reservation.asset.clone()))
        {
            state.locally_reserved = (state.locally_reserved - reservation.amount).max(0.0);
        }
        reservation.released = true;
    }

    pub fn settle_buy(
        &mut self,
        exchange: SpotVenue,
        base_asset: &str,
        quote_asset: &str,
        quantity: f64,
        quote_spent: f64,
        fee_quote: f64,
    ) {
        self.debit(exchange, quote_asset, quote_spent + fee_quote);
        self.credit(exchange, base_asset, quantity);
        self.fees_paid += fee_quote;
    }

    pub fn settle_sell(
        &mut self,
        exchange: SpotVenue,
        base_asset: &str,
        quote_asset: &str,
        quantity: f64,
        quote_received: f64,
        fee_quote: f64,
    ) {
        self.debit(exchange, base_asset, quantity);
        self.credit(exchange, quote_asset, quote_received - fee_quote);
        self.fees_paid += fee_quote;
    }

    pub fn add_pnl(&mut self, gross_pnl: f64, net_pnl: f64) {
        self.gross_pnl += gross_pnl;
        self.realized_pnl += net_pnl;
    }

    fn debit(&mut self, exchange: SpotVenue, asset: &str, amount: f64) {
        let state = self
            .balances
            .entry((exchange, normalize_asset(asset)))
            .or_insert_with(|| PaperAssetState::new(0.0));
        state.available = (state.available - amount).max(0.0);
        state.total = (state.total - amount).max(0.0);
    }

    fn credit(&mut self, exchange: SpotVenue, asset: &str, amount: f64) {
        let state = self
            .balances
            .entry((exchange, normalize_asset(asset)))
            .or_insert_with(|| PaperAssetState::new(0.0));
        state.available += amount;
        state.total += amount;
    }
}

#[derive(Debug, Clone)]
pub struct InventoryReservation {
    pub exchange: SpotVenue,
    pub asset: String,
    pub amount: f64,
    released: bool,
}

pub fn parse_spot_venue(value: &str) -> Result<SpotVenue, RejectionReason> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mexc" => Ok(SpotVenue::Mexc),
        "coinex" => Ok(SpotVenue::CoinEx),
        "gate" | "gateio" | "gate.io" => Ok(SpotVenue::GateIo),
        "bitget" => Ok(SpotVenue::Bitget),
        _ => Err(RejectionReason::PaperExecutionRejected),
    }
}

fn normalize_asset(asset: &str) -> String {
    asset.trim().to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_should_reserve_release_and_settle_balances() {
        let mut inventory = PaperInventory::default();
        inventory.set_balance(SpotVenue::GateIo, "USDT", 100.0);
        inventory.set_balance(SpotVenue::Bitget, "BTC", 1.0);

        let mut quote = inventory
            .reserve(
                SpotVenue::GateIo,
                "usdt",
                20.0,
                RejectionReason::InsufficientQuoteBalance,
            )
            .expect("quote reserve");
        assert_eq!(
            inventory
                .balance(SpotVenue::GateIo, "USDT")
                .effective_available(),
            80.0
        );
        inventory.release(&mut quote);
        inventory.settle_buy(SpotVenue::GateIo, "BTC", "USDT", 0.1, 20.0, 0.02);
        inventory.settle_sell(SpotVenue::Bitget, "BTC", "USDT", 0.1, 21.0, 0.02);
        inventory.add_pnl(1.0, 0.96);

        assert!((inventory.balance(SpotVenue::GateIo, "USDT").total - 79.98).abs() < 1e-12);
        assert!((inventory.balance(SpotVenue::GateIo, "BTC").total - 0.1).abs() < 1e-12);
        assert!((inventory.balance(SpotVenue::Bitget, "BTC").total - 0.9).abs() < 1e-12);
        assert!((inventory.balance(SpotVenue::Bitget, "USDT").total - 20.98).abs() < 1e-12);
        assert!((inventory.realized_pnl - 0.96).abs() < 1e-12);
        assert!((inventory.fees_paid - 0.04).abs() < 1e-12);
    }

    #[test]
    fn inventory_should_reject_insufficient_available_balance() {
        let mut inventory = PaperInventory::default();
        inventory.set_balance(SpotVenue::Mexc, "USDT", 1.0);

        let result = inventory.reserve(
            SpotVenue::Mexc,
            "USDT",
            2.0,
            RejectionReason::InsufficientQuoteBalance,
        );

        assert_eq!(
            result.unwrap_err(),
            RejectionReason::InsufficientQuoteBalance
        );
    }
}
