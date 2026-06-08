#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{cryptomus_symbol, parse_cryptomus_order_book, parse_cryptomus_symbol_rules};
use super::CryptomusGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const MARKETS_PATH: &str = "/v2/user-api/exchange/markets";
pub const ORDER_BOOK_PATH_PREFIX: &str = "/v1/exchange/market/order-book";

impl CryptomusGatewayAdapter {
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
        let value = self
            .rest
            .send_public_request(MARKETS_PATH, &HashMap::new())
            .await?;
        let rules =
            parse_cryptomus_symbol_rules(self.exchange_id.clone(), &request.symbols, &value)?;
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
        let level = cryptomus_depth_level(request.depth.unwrap_or(50));
        let mut params = HashMap::new();
        params.insert("level".to_string(), level.to_string());
        let endpoint = format!(
            "{ORDER_BOOK_PATH_PREFIX}/{}",
            cryptomus_symbol(&request.symbol)
        );
        let value = self.rest.send_public_request(&endpoint, &params).await?;
        let order_book = parse_cryptomus_order_book(&request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

pub fn cryptomus_depth_level(depth: u32) -> u32 {
    match depth {
        0..=5 => 0,
        6..=10 => 1,
        11..=25 => 2,
        26..=50 => 3,
        51..=100 => 4,
        _ => 5,
    }
}
