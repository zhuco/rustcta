use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_bitbns_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::BitbnsGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitbnsGatewayAdapter {
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
        let requested = request
            .symbols
            .iter()
            .map(|symbol| {
                self.ensure_exchange(&symbol.exchange)?;
                self.ensure_spot(symbol.market_type)?;
                normalize_bitbns_symbol(&symbol.exchange_symbol.symbol)
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;

        let response = self
            .rest
            .send_public_get("/order/fetchMarkets/", &HashMap::new())
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested.is_empty() {
            rules.retain(|rule| {
                requested.iter().any(|(base, quote)| {
                    rule.base_asset == *base
                        && rule.quote_asset == *quote
                        && rule.symbol.market_type == rustcta_types::MarketType::Spot
                })
            });
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
        let depth = normalize_depth(request.depth)?;
        let (coin, market) = normalize_bitbns_symbol(&request.symbol.exchange_symbol.symbol)?;
        let mut params = HashMap::new();
        params.insert("coin".to_string(), coin);
        params.insert("market".to_string(), market);
        let value = self
            .rest
            .send_public_get("/exchangeData/orderBook", &params)
            .await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
