#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    blockchaincom_symbol, parse_blockchaincom_order_book, parse_blockchaincom_symbol_rules,
};
use super::BlockchainComGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const SYMBOLS_PATH: &str = "/symbols";
pub const ORDER_BOOK_L2_PREFIX: &str = "/l2";

impl BlockchainComGatewayAdapter {
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
        let value = self.rest.send_public_get(SYMBOLS_PATH, &params).await?;
        let rules =
            parse_blockchaincom_symbol_rules(self.exchange_id.clone(), &request.symbols, &value)?;
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
        let endpoint = format!(
            "{}/{}",
            ORDER_BOOK_L2_PREFIX,
            blockchaincom_symbol(&request.symbol)
        );
        let params = HashMap::new();
        let value = self.rest.send_public_get(&endpoint, &params).await?;
        let order_book = parse_blockchaincom_order_book(&request.symbol, request.depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
