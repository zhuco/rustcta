use serde::{Deserialize, Serialize};

use crate::{ExchangeId, ExchangeSymbol, MarketType, Position, RequestContext, ResponseMetadata};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionsRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbols: Vec<ExchangeSymbol>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionsResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub positions: Vec<Position>,
}
