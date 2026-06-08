use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{parse_markets, parse_orderbook};
use super::DydxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DydxGatewayAdapter {
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
            self.ensure_perpetual(symbol.market_type)?;
        }
        let mut params = HashMap::new();
        if request.symbols.len() == 1 {
            params.insert(
                "market".to_string(),
                request.symbols[0].exchange_symbol.symbol.clone(),
            );
        }
        let value = self.indexer.get("/v4/perpetualMarkets", &params).await?;
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: parse_markets(&self.exchange_id, &request.symbols, &value)?,
        })
    }

    pub(super) async fn get_order_book_public_rest(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let endpoint = format!(
            "/v4/orderbooks/perpetualMarket/{}",
            urlencoding::encode(&request.symbol.exchange_symbol.symbol)
        );
        let value = self.indexer.get(&endpoint, &HashMap::new()).await?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book: parse_orderbook(&self.exchange_id, request.symbol, &value)?,
        })
    }
}
