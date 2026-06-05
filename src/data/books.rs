//! Order-book data acquisition and cache access.
//!
//! Public market data should flow exchange websocket/REST readers -> `BookEvent`
//! -> `BookCache`/`BookHealth` -> scanner or strategy consumers. Trading
//! decisions must not fetch browser-provided books or place orders from this
//! layer.

pub use crate::data::{
    BookCache, BookCacheConfig, BookEvent, BookEventKind, BookHealth, BookKey, BookRecord,
    BookRecorder, BookRecorderConfig, BookSource, CachedBook, ExchangeMarketHealth,
    WebSocketBookManager, WebSocketBookManagerConfig,
};
