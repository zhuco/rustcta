use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{apex_public_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::ApexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const SYMBOLS_PATH: &str = "/api/v3/symbols";
pub const DEPTH_PATH: &str = "/api/v3/depth";
pub const TICKER_PATH: &str = "/api/v3/ticker";

impl ApexGatewayAdapter {
    pub(super) async fn get_symbol_rules_public_rest(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .unwrap_or_else(|| self.exchange_id.clone());
        self.ensure_exchange(&exchange)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let value = self
            .rest
            .send_public_get(SYMBOLS_PATH, &HashMap::new())
            .await?;
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: parse_symbol_rules(&self.exchange_id, &request.symbols, &value)?,
        })
    }

    pub(super) async fn get_order_book_public_rest(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            apex_public_symbol(&request.symbol.exchange_symbol.symbol),
        );
        if let Some(depth) = request.depth {
            params.insert("limit".to_string(), depth.min(200).to_string());
        }
        let value = self.rest.send_public_get(DEPTH_PATH, &params).await?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book: parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?,
        })
    }
}
