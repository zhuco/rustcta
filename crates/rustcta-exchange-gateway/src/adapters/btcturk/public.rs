#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{parse_btcturk_order_book, parse_btcturk_symbol_rules};
use super::BtcTurkGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const EXCHANGE_INFO_PATH: &str = "/api/v2/server/exchangeinfo";
pub const ORDER_BOOK_PATH: &str = "/api/v2/orderbook";

impl BtcTurkGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.ensure_public_rest()?;
        let params = HashMap::new();
        let value = self
            .rest
            .send_public_request(EXCHANGE_INFO_PATH, &params)
            .await?;
        let rules = parse_btcturk_symbol_rules(self.exchange_id.clone(), &request.symbols, &value)?;
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
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_public_rest()?;
        let mut params = HashMap::new();
        params.insert(
            "pairSymbol".to_string(),
            super::parser::btcturk_symbol(&request.symbol),
        );
        if let Some(depth) = request.depth {
            params.insert("limit".to_string(), depth.min(100).to_string());
        }
        let value = self
            .rest
            .send_public_request(ORDER_BOOK_PATH, &params)
            .await?;
        let order_book = parse_btcturk_order_book(&request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
