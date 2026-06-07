use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_bitunix_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::BitunixGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitunixGatewayAdapter {
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
            let endpoint = match market_type {
                MarketType::Spot => "/api/spot/v1/common/coin_pair/list",
                MarketType::Perpetual => "/api/v1/futures/market/trading_pairs",
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut params = HashMap::new();
            if market_type == MarketType::Perpetual && !request.symbols.is_empty() {
                let symbols = request
                    .symbols
                    .iter()
                    .filter(|symbol| symbol.market_type == market_type)
                    .map(|symbol| normalize_bitunix_symbol(&symbol.exchange_symbol.symbol))
                    .collect::<ExchangeApiResult<Vec<_>>>()?
                    .join(",");
                if !symbols.is_empty() {
                    params.insert("symbols".to_string(), symbols);
                }
            }
            let response = self
                .rest
                .send_public_request(market_type, endpoint, &params)
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
                    normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)
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
            "symbol".to_string(),
            normalize_bitunix_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        match request.symbol.market_type {
            MarketType::Spot => {
                params.insert(
                    "precision".to_string(),
                    normalize_depth(request.depth.unwrap_or(20), request.symbol.market_type),
                );
            }
            MarketType::Perpetual => {
                params.insert(
                    "limit".to_string(),
                    normalize_depth(request.depth.unwrap_or(50), request.symbol.market_type),
                );
            }
            _ => unreachable!("checked by ensure_supported_market"),
        }
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/spot/v1/market/depth",
            MarketType::Perpetual => "/api/v1/futures/market/depth",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .rest
            .send_public_request(request.symbol.market_type, endpoint, &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
