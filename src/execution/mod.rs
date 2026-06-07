//! Execution-layer contracts for cross-exchange arbitrage.
//!
//! This module is intentionally side-effect free. It models bundle lifecycle,
//! order intents, reconciliation, recovery recommendations, and in-memory
//! ledgers without placing real exchange orders.

pub mod adapter;
pub mod balance_reconciliation;
pub mod bundle;
pub mod command;
pub mod engine;
pub mod fee_model;
pub mod hedge;
pub mod idempotency;
pub mod latency;
pub mod ledger;
pub mod live_dry_run;
pub mod order_reconciliation;
pub mod reconciler;
pub mod recovery;
pub mod router;
pub mod state_machine;
pub mod user_stream;

pub use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol, RouteStatus, RuntimeMode};
pub use adapter::*;
pub use balance_reconciliation::*;
pub use bundle::*;
pub use command::*;
pub use engine::*;
pub use fee_model::*;
pub use hedge::*;
pub use idempotency::*;
pub use latency::*;
pub use ledger::*;
pub use live_dry_run::*;
pub use order_reconciliation::*;
pub use reconciler::*;
pub use recovery::*;
pub use router::*;
pub use state_machine::*;
pub use user_stream::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_contracts_should_default_to_non_live_mode() {
        let mode = RuntimeMode::default();

        assert_eq!(mode, RuntimeMode::Simulation);
        assert!(!mode.allows_live_orders());
    }
}
