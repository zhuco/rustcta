#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{
    onetrading_symbol, parse_onetrading_order_book, parse_onetrading_symbol_rules,
};
use super::transport::public_get_request_spec;
use super::OneTradingGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const INSTRUMENTS_PATH: &str = "/instruments";
pub const ORDER_BOOK_PATH: &str = "/order-book";

impl OneTradingGatewayAdapter {
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
        let mut params = HashMap::new();
        params.insert("type".to_string(), "SPOT".to_string());
        let value = self
            .rest
            .send_public_request(INSTRUMENTS_PATH, &params)
            .await?;
        let rules =
            parse_onetrading_symbol_rules(self.exchange_id.clone(), &request.symbols, &value)?;
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
        params.insert("level".to_string(), "1".to_string());
        if let Some(depth) = request.depth {
            params.insert("depth".to_string(), depth.min(50).to_string());
        }
        let path = format!(
            "{ORDER_BOOK_PATH}/{}",
            urlencoding::encode(&onetrading_symbol(&request.symbol))
        );
        let value = self.rest.send_public_request(&path, &params).await?;
        let order_book = parse_onetrading_order_book(&request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

pub fn instruments_request_spec_fixture() -> serde_json::Value {
    public_get_request_spec(INSTRUMENTS_PATH, json!({ "type": "SPOT" }))
}

pub fn order_book_request_spec_fixture() -> serde_json::Value {
    public_get_request_spec(
        "/order-book/BTC_EUR",
        json!({
            "level": "1",
            "depth": "16"
        }),
    )
}
