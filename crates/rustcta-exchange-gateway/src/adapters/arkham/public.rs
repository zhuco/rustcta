#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{arkham_symbol, parse_arkham_order_book, parse_arkham_symbol_rules};
use super::ArkhamGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const PAIRS_PATH: &str = "/public/pairs";
pub const ORDER_BOOK_PATH: &str = "/public/book";

impl ArkhamGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.ensure_public_rest()?;
        let params = HashMap::new();
        let value = self.rest.send_public_request(PAIRS_PATH, &params).await?;
        let rules = parse_arkham_symbol_rules(self.exchange_id.clone(), &request.symbols, &value)?;
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            rules,
        })
    }

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_public_rest()?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), arkham_symbol(&request.symbol));
        if let Some(depth) = request.depth {
            params.insert("limit".to_string(), depth.min(50).to_string());
        }
        let value = self
            .rest
            .send_public_request(ORDER_BOOK_PATH, &params)
            .await?;
        let order_book = parse_arkham_order_book(&request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
