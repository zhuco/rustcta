use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::exchanges::unified::{AssetBalance, ExchangeClientError, ExchangeClientResult};

#[derive(Debug, Clone, PartialEq)]
pub struct BalanceReservation {
    pub exchange: String,
    pub asset: String,
    pub amount: f64,
    settled: f64,
    released: bool,
}

impl BalanceReservation {
    pub fn remaining(&self) -> f64 {
        (self.amount - self.settled).max(0.0)
    }
}

#[derive(Debug, Clone, Default)]
pub struct BalanceReservationManager {
    states: Arc<Mutex<HashMap<String, AssetReservationState>>>,
}

#[derive(Debug, Clone, Default)]
struct AssetReservationState {
    available: f64,
    locked_by_exchange: f64,
    locally_reserved: f64,
}

impl BalanceReservationManager {
    pub fn update_balances(
        &self,
        exchange: &str,
        balances: &[AssetBalance],
    ) -> ExchangeClientResult<()> {
        let mut states = self.states.lock();
        for balance in balances {
            let key = reservation_key(exchange, &balance.asset);
            let locally_reserved = states
                .get(&key)
                .map(|state| state.locally_reserved)
                .unwrap_or(0.0);
            states.insert(
                key,
                AssetReservationState {
                    available: balance.available,
                    locked_by_exchange: balance.locked_by_exchange.max(balance.locked),
                    locally_reserved,
                },
            );
        }
        Ok(())
    }

    pub fn reserve(
        &self,
        exchange: &str,
        asset: &str,
        amount: f64,
    ) -> ExchangeClientResult<BalanceReservation> {
        if !amount.is_finite() || amount <= 0.0 {
            return Err(ExchangeClientError::Validation {
                field: "amount",
                reason: "reservation amount must be finite and positive".to_string(),
            });
        }
        let mut states = self.states.lock();
        let key = reservation_key(exchange, asset);
        let state = states.entry(key).or_default();
        let effective_available = state.available - state.locally_reserved;
        if effective_available + 1e-12 < amount {
            return Err(ExchangeClientError::Validation {
                field: "balance",
                reason: format!(
                    "{} effective_available={} is below reservation amount {}",
                    asset, effective_available, amount
                ),
            });
        }
        state.locally_reserved += amount;
        log::info!(
            "spot balance reservation created exchange={} asset={} amount={} locally_reserved={} effective_available={}",
            exchange,
            asset,
            amount,
            state.locally_reserved,
            state.available - state.locally_reserved
        );
        Ok(BalanceReservation {
            exchange: exchange.to_string(),
            asset: asset.to_ascii_uppercase(),
            amount,
            settled: 0.0,
            released: false,
        })
    }

    pub fn release(&self, reservation: &mut BalanceReservation) -> ExchangeClientResult<()> {
        if reservation.released {
            return Ok(());
        }
        let release_amount = reservation.remaining();
        self.adjust_reserved(&reservation.exchange, &reservation.asset, -release_amount)?;
        reservation.released = true;
        log::info!(
            "spot balance reservation released exchange={} asset={} amount={}",
            reservation.exchange,
            reservation.asset,
            release_amount
        );
        Ok(())
    }

    pub fn release_asset_reservation(
        &self,
        exchange: &str,
        asset: &str,
        amount: f64,
    ) -> ExchangeClientResult<()> {
        if !amount.is_finite() || amount <= 0.0 {
            return Ok(());
        }
        self.adjust_reserved(exchange, asset, -amount)?;
        log::info!(
            "spot balance reservation released exchange={} asset={} amount={}",
            exchange,
            asset.to_ascii_uppercase(),
            amount
        );
        Ok(())
    }

    pub fn settle(
        &self,
        reservation: &mut BalanceReservation,
        filled_amount: f64,
        final_status: bool,
    ) -> ExchangeClientResult<()> {
        if !filled_amount.is_finite() || filled_amount < 0.0 {
            return Err(ExchangeClientError::Validation {
                field: "filled_amount",
                reason: "filled amount must be finite and non-negative".to_string(),
            });
        }
        let delta = (filled_amount - reservation.settled).max(0.0);
        if delta > 0.0 {
            self.adjust_reserved(&reservation.exchange, &reservation.asset, -delta)?;
            reservation.settled += delta;
            log::info!(
                "spot balance reservation settled exchange={} asset={} amount={} total_settled={}",
                reservation.exchange,
                reservation.asset,
                delta,
                reservation.settled
            );
        }
        if final_status {
            self.release(reservation)?;
        }
        Ok(())
    }

    pub fn balance_with_reservation(&self, exchange: &str, balance: AssetBalance) -> AssetBalance {
        let states = self.states.lock();
        let locally_reserved = states
            .get(&reservation_key(exchange, &balance.asset))
            .map(|state| state.locally_reserved)
            .unwrap_or_default();
        balance.with_reservation(locally_reserved)
    }

    pub fn locally_reserved(&self, exchange: &str, asset: &str) -> f64 {
        self.states
            .lock()
            .get(&reservation_key(exchange, asset))
            .map(|state| state.locally_reserved)
            .unwrap_or_default()
    }

    fn adjust_reserved(&self, exchange: &str, asset: &str, delta: f64) -> ExchangeClientResult<()> {
        let mut states = self.states.lock();
        let key = reservation_key(exchange, asset);
        let state = states.entry(key).or_default();
        state.locally_reserved = (state.locally_reserved + delta).max(0.0);
        Ok(())
    }
}

fn reservation_key(exchange: &str, asset: &str) -> String {
    format!(
        "{}-{}",
        exchange.to_ascii_lowercase(),
        asset.to_ascii_uppercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager_with_usdt() -> BalanceReservationManager {
        let manager = BalanceReservationManager::default();
        manager
            .update_balances("mexc", &[AssetBalance::new("USDT", 100.0, 100.0, 0.0)])
            .unwrap();
        manager
    }

    #[test]
    fn sell_order_reserves_base_asset() {
        let manager = BalanceReservationManager::default();
        manager
            .update_balances("mexc", &[AssetBalance::new("DKA", 10.0, 10.0, 0.0)])
            .unwrap();

        let reservation = manager.reserve("mexc", "DKA", 4.0).unwrap();

        assert_eq!(reservation.asset, "DKA");
        assert_eq!(manager.locally_reserved("mexc", "DKA"), 4.0);
    }

    #[test]
    fn buy_order_reserves_quote_asset() {
        let manager = manager_with_usdt();

        manager.reserve("mexc", "USDT", 25.0).unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 25.0);
    }

    #[test]
    fn order_reject_releases_reservation() {
        let manager = manager_with_usdt();
        let mut reservation = manager.reserve("mexc", "USDT", 50.0).unwrap();

        manager.release(&mut reservation).unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 0.0);
    }

    #[test]
    fn cancel_releases_unfilled_reservation() {
        let manager = manager_with_usdt();
        let mut reservation = manager.reserve("mexc", "USDT", 50.0).unwrap();

        manager.settle(&mut reservation, 20.0, true).unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 0.0);
    }

    #[test]
    fn partial_fill_settles_filled_quantity_and_keeps_rest() {
        let manager = manager_with_usdt();
        let mut reservation = manager.reserve("mexc", "USDT", 50.0).unwrap();

        manager.settle(&mut reservation, 20.0, false).unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 30.0);
    }

    #[test]
    fn filled_order_settles_full_reservation() {
        let manager = manager_with_usdt();
        let mut reservation = manager.reserve("mexc", "USDT", 50.0).unwrap();

        manager.settle(&mut reservation, 50.0, true).unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 0.0);
    }

    #[test]
    fn release_asset_reservation_releases_without_handle() {
        let manager = manager_with_usdt();
        manager.reserve("mexc", "USDT", 50.0).unwrap();

        manager
            .release_asset_reservation("mexc", "USDT", 50.0)
            .unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 0.0);
    }

    #[test]
    fn concurrent_sell_orders_cannot_oversell_same_asset() {
        let manager = BalanceReservationManager::default();
        manager
            .update_balances("coinex", &[AssetBalance::new("CUDIS", 10.0, 10.0, 0.0)])
            .unwrap();

        manager.reserve("coinex", "CUDIS", 7.0).unwrap();
        assert!(manager.reserve("coinex", "CUDIS", 4.0).is_err());
    }

    #[test]
    fn concurrent_buy_orders_cannot_overspend_same_quote_balance() {
        let manager = manager_with_usdt();

        manager.reserve("mexc", "USDT", 80.0).unwrap();
        assert!(manager.reserve("mexc", "USDT", 30.0).is_err());
    }

    #[test]
    fn reservation_survives_until_final_order_status() {
        let manager = manager_with_usdt();
        let mut reservation = manager.reserve("mexc", "USDT", 40.0).unwrap();

        manager.settle(&mut reservation, 10.0, false).unwrap();

        assert_eq!(manager.locally_reserved("mexc", "USDT"), 30.0);
    }

    #[test]
    fn reservation_and_exchange_locked_balance_are_not_double_counted() {
        let manager = BalanceReservationManager::default();
        let balance = AssetBalance::new("USDT", 100.0, 70.0, 30.0);
        manager.update_balances("mexc", &[balance.clone()]).unwrap();
        manager.reserve("mexc", "USDT", 20.0).unwrap();

        let effective = manager.balance_with_reservation("mexc", balance);

        assert_eq!(effective.locked_by_exchange, 30.0);
        assert_eq!(effective.locally_reserved, 20.0);
        assert_eq!(effective.effective_available, 50.0);
    }
}
