use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use super::{RejectionReason, SpotSpotTakerArbitrageConfig, SpotVenue};
use crate::exchanges::unified::AssetBalance;
use crate::risk::DisabledRegistry;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn as_asset_balance(&self, asset: &str) -> AssetBalance {
        AssetBalance::new(asset, self.total, self.available, self.locked_by_exchange)
            .with_reservation(self.locally_reserved)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PaperInventory {
    balances: HashMap<(SpotVenue, String), PaperAssetState>,
    pub realized_pnl: f64,
    pub fees_paid: f64,
    pub gross_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct InventoryReservation {
    pub exchange: SpotVenue,
    pub asset: String,
    pub amount: f64,
    released: bool,
}

impl PaperInventory {
    pub fn from_config(config: &SpotSpotTakerArbitrageConfig) -> Result<Self> {
        let mut inventory = Self::default();
        for (exchange, balances) in &config.initial_balances {
            let exchange = match exchange.trim().to_ascii_lowercase().as_str() {
                "mexc" => SpotVenue::Mexc,
                "coinex" => SpotVenue::CoinEx,
                "gate" | "gateio" | "gate.io" => SpotVenue::GateIo,
                "bitget" => SpotVenue::Bitget,
                other => return Err(anyhow!("unsupported initial balance exchange: {other}")),
            };
            for (asset, amount) in balances {
                inventory.set_balance(exchange, asset, *amount);
            }
        }
        Ok(inventory)
    }

    pub fn set_balance(&mut self, exchange: SpotVenue, asset: &str, amount: f64) {
        self.balances.insert(
            (exchange, asset.trim().to_ascii_uppercase()),
            PaperAssetState::new(amount.max(0.0)),
        );
    }

    pub fn balance(&self, exchange: SpotVenue, asset: &str) -> PaperAssetState {
        self.balances
            .get(&(exchange, asset.trim().to_ascii_uppercase()))
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

    pub fn exclude_unmanaged_positions(&mut self, registry: &DisabledRegistry) {
        for position in registry.unmanaged_positions() {
            let exchange = match position.exchange.as_str() {
                "mexc" => SpotVenue::Mexc,
                "coinex" => SpotVenue::CoinEx,
                "gate" | "gateio" | "gate.io" => SpotVenue::GateIo,
                "bitget" => SpotVenue::Bitget,
                _ => continue,
            };
            let key = (exchange, position.asset.trim().to_ascii_uppercase());
            if let Some(state) = self.balances.get_mut(&key) {
                state.available = (state.available - position.quantity).max(0.0);
                state.total = (state.total - position.quantity).max(0.0);
            }
        }
    }

    pub fn reserve(
        &mut self,
        exchange: SpotVenue,
        asset: &str,
        amount: f64,
        reason: RejectionReason,
    ) -> Result<InventoryReservation, RejectionReason> {
        let key = (exchange, asset.trim().to_ascii_uppercase());
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
            .entry((exchange, asset.trim().to_ascii_uppercase()))
            .or_insert_with(|| PaperAssetState::new(0.0));
        state.available = (state.available - amount).max(0.0);
        state.total = (state.total - amount).max(0.0);
    }

    fn credit(&mut self, exchange: SpotVenue, asset: &str, amount: f64) {
        let state = self
            .balances
            .entry((exchange, asset.trim().to_ascii_uppercase()))
            .or_insert_with(|| PaperAssetState::new(0.0));
        state.available += amount;
        state.total += amount;
    }
}
