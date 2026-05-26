use serde::{Deserialize, Serialize};

use super::{CanonicalSymbol, ExchangeId, ExchangeSymbol, TakerSide};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedTrade {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub trade_id: Option<String>,
    pub side: TakerSide,
    pub price: f64,
    pub quantity: f64,
    pub exchange_ts: chrono::DateTime<chrono::Utc>,
    pub recv_ts: chrono::DateTime<chrono::Utc>,
}

impl NormalizedTrade {
    pub fn notional(&self) -> f64 {
        self.price * self.quantity
    }
}
