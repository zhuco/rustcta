use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_deepcoin_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
    product_inst_type,
};
use super::DeepcoinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DeepcoinGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }

        let markets = if request.symbols.is_empty() {
            vec![MarketType::Spot, MarketType::Perpetual]
        } else {
            let mut markets = Vec::new();
            for symbol in &request.symbols {
                if !markets.contains(&symbol.market_type) {
                    markets.push(symbol.market_type);
                }
            }
            markets
        };

        let mut rules = Vec::new();
        for market_type in markets {
            let mut params = HashMap::new();
            params.insert(
                "instType".to_string(),
                product_inst_type(market_type).to_string(),
            );
            let response = self
                .rest
                .send_public_request("/deepcoin/market/instruments", &params)
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                market_type,
                &response,
            )?);
        }

        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| {
                    normalize_deepcoin_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
                        .map(|normalized| (symbol.market_type, normalized))
                })
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            rules.retain(|rule| {
                requested.contains(&(
                    rule.symbol.market_type,
                    rule.symbol.exchange_symbol.symbol.clone(),
                ))
            });
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
            "instId".to_string(),
            normalize_deepcoin_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        params.insert(
            "sz".to_string(),
            normalize_depth(request.depth.unwrap_or(20)).to_string(),
        );
        let value = self
            .rest
            .send_public_request("/deepcoin/market/books", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
