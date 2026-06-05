//! Shared market-data acquisition layer.
//!
//! This layer owns read-only market-data ingestion primitives used by scanners,
//! strategies, runtime publishers, and dashboards. It sits below strategies:
//! strategies subscribe to or read from this layer, while this layer never
//! depends on strategy code.

pub mod book_cache;
pub mod book_event;
pub mod book_health;
pub mod book_recorder;
pub mod books;
pub mod websocket_books;

pub use book_cache::*;
pub use book_event::*;
pub use book_health::*;
pub use book_recorder::*;
pub use books::*;
pub use websocket_books::*;
