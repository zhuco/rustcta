use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_paymium_symbol, parse_orderbook_snapshot, parse_symbol_rules, paymium_currency_path,
};
use super::PaymiumGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl PaymiumGatewayAdapter {
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
            normalize_paymium_symbol(&symbol.exchange_symbol.symbol)?;
        }
        let value = self
            .rest
            .send_public_get("/data/eur/ticker", &HashMap::new())
            .await?;
        let rules = parse_symbol_rules(&self.exchange_id, &value)?;
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
        let currency = paymium_currency_path(&request.symbol.exchange_symbol.symbol)?;
        let endpoint = format!("/data/{currency}/depth");
        let value = self
            .rest
            .send_public_get(&endpoint, &HashMap::new())
            .await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, request.depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
