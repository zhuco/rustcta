use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SymbolOperationOwner {
    Arbitrage,
    EnableWorkflow,
    DisableWorkflow,
    MarketLiquidation,
    PassiveLiquidation,
    Rebalance,
    ManualOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolOperationLock {
    pub internal_symbol: String,
    pub owner: SymbolOperationOwner,
    pub acquired_at: DateTime<Utc>,
    pub command_id: String,
    pub timeout_ms: u64,
}

impl SymbolOperationLock {
    pub fn is_stale(&self, now: DateTime<Utc>) -> bool {
        now.signed_duration_since(self.acquired_at) > Duration::milliseconds(self.timeout_ms as i64)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolOperationLockRegistry {
    locks: HashMap<String, SymbolOperationLock>,
}

impl SymbolOperationLockRegistry {
    pub fn acquire(
        &mut self,
        symbol: impl Into<String>,
        owner: SymbolOperationOwner,
        command_id: impl Into<String>,
        timeout_ms: u64,
        now: DateTime<Utc>,
    ) -> Result<SymbolOperationLock, String> {
        let symbol = super::normalize_symbol(&symbol.into());
        if let Some(existing) = self.locks.get(&symbol) {
            return Err(format!(
                "symbol {symbol} already locked by {:?} command {}",
                existing.owner, existing.command_id
            ));
        }
        let lock = SymbolOperationLock {
            internal_symbol: symbol.clone(),
            owner,
            acquired_at: now,
            command_id: command_id.into(),
            timeout_ms,
        };
        self.locks.insert(symbol, lock.clone());
        Ok(lock)
    }

    pub fn release(&mut self, symbol: &str, command_id: &str) -> Result<(), String> {
        let symbol = super::normalize_symbol(symbol);
        let Some(existing) = self.locks.get(&symbol) else {
            return Ok(());
        };
        if existing.command_id != command_id {
            return Err(format!(
                "cannot release lock for {symbol}: owned by command {}",
                existing.command_id
            ));
        }
        self.locks.remove(&symbol);
        Ok(())
    }

    pub fn get(&self, symbol: &str) -> Option<&SymbolOperationLock> {
        self.locks.get(&super::normalize_symbol(symbol))
    }

    pub fn operation_allows_arbitrage(&self, symbol: &str) -> bool {
        match self.get(symbol) {
            None => true,
            Some(lock) => matches!(lock.owner, SymbolOperationOwner::Arbitrage),
        }
    }

    pub fn all(&self) -> Vec<SymbolOperationLock> {
        self.locks.values().cloned().collect()
    }
}
