use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_coinbase_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::CoinbaseGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinbaseGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let market_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Spot);
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }

        let mut params = HashMap::new();
        match market_type {
            MarketType::Spot => {
                params.insert("product_type".to_string(), "SPOT".to_string());
            }
            MarketType::Perpetual => {
                params.insert("product_type".to_string(), "FUTURE".to_string());
                params.insert("contract_expiry_type".to_string(), "PERPETUAL".to_string());
                params.insert("product_venue".to_string(), "INTX".to_string());
            }
            other => self.ensure_supported_market(other)?,
        }
        let token = self.optional_bearer_token();
        let response = if market_type == MarketType::Perpetual {
            self.rest
                .send_intx_get("/products", &params, token.as_deref())
                .await?
        } else {
            self.rest
                .send_spot_get("/products", &params, token.as_deref())
                .await?
        };
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_coinbase_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, market_type, &response)?;
        if !requested.is_empty() {
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
        }

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
        self.ensure_supported_market(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "product_id".to_string(),
            normalize_coinbase_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "limit".to_string(),
            normalize_depth(request.depth.unwrap_or(50)).to_string(),
        );
        let token = self.optional_bearer_token();
        let value = if request.symbol.market_type == MarketType::Perpetual {
            self.rest
                .send_intx_get("/product_book", &params, token.as_deref())
                .await?
        } else {
            self.rest
                .send_spot_get("/product_book", &params, token.as_deref())
                .await?
        };
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
