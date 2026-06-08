use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    coin_param_from_symbol, normalize_btcbox_symbol, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::BtcboxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BtcboxGatewayAdapter {
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
        let value = self
            .rest
            .send_public_request("/tickers", &HashMap::new())
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &value)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_btcbox_symbol(&symbol.exchange_symbol.symbol))
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
        }
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules,
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
            "coin".to_string(),
            coin_param_from_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self.rest.send_public_request("/depth", &params).await?;
        let mut order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        if let Some(depth) = request.depth {
            let depth = depth as usize;
            order_book.bids.truncate(depth);
            order_book.asks.truncate(depth);
        }
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
