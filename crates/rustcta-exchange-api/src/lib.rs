pub mod account;
pub mod capabilities;
pub mod client;
pub mod context;
pub mod error;
pub mod market;
pub mod order;
pub mod protocol;
pub mod streams;
pub mod types;

pub const EXCHANGE_API_SCHEMA_VERSION: u16 = 1;

pub use account::*;
pub use capabilities::*;
pub use client::*;
pub use context::*;
pub use error::*;
pub use market::*;
pub use order::*;
pub use protocol::*;
pub use streams::*;
pub use types::*;

#[cfg(test)]
mod tests;
