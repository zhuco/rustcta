//! Read-only opportunity scanning and exchange comparison.
//!
//! Scanner modules consume market data, fee models, and exchange metadata to
//! produce analytical read models. They must not submit, cancel, or mutate
//! exchange orders.

pub mod exchange_roles;
pub mod five_exchange_spot;
pub mod symbol_coverage;

pub use exchange_roles::*;
pub use five_exchange_spot::*;
pub use symbol_coverage::*;
