use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_coincheck_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::CoincheckGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoincheckGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
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
            self.ensure_spot(symbol.market_type)?;
        }
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: parse_symbol_rules(&self.exchange_id, &request.symbols)?,
        })
    }

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "pair".to_string(),
            normalize_coincheck_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .rest
            .send_public_request("/api/order_books", &params)
            .await?;
        let mut order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        let depth = normalize_depth(request.depth.unwrap_or(50)) as usize;
        order_book.bids.truncate(depth);
        order_book.asks.truncate(depth);
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
