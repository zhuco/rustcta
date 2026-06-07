use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_coinw_perp_base, normalize_coinw_perp_symbol, normalize_coinw_spot_symbol,
    normalize_spot_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::CoinwGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinwGatewayAdapter {
    pub(super) async fn get_symbol_rules_public_rest(
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
            self.ensure_supported_market_type(symbol.market_type)?;
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
                MarketType::Spot => "/api/v1/public?command=returnSymbol",
                MarketType::Perpetual => "/v1/perpum/instruments",
                _ => unreachable!("checked by ensure_supported_market_type"),
            };
            let value = self
                .rest
                .send_public_request(endpoint, &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(&self.exchange_id, market_type, &value)?);
        }

        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| match symbol.market_type {
                    MarketType::Spot => normalize_coinw_spot_symbol(&symbol.exchange_symbol.symbol)
                        .map(|normalized| (symbol.market_type, normalized)),
                    MarketType::Perpetual => {
                        normalize_coinw_perp_symbol(&symbol.exchange_symbol.symbol)
                            .map(|normalized| (symbol.market_type, normalized))
                    }
                    _ => unreachable!("checked by ensure_supported_market_type"),
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
            metadata: response_metadata(exchange, request.context.request_id),
            rules,
        })
    }

    pub(super) async fn get_order_book_public_rest(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut params = HashMap::new();
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert(
                    "symbol".to_string(),
                    normalize_coinw_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                params.insert(
                    "size".to_string(),
                    normalize_spot_depth(request.depth.unwrap_or(5)).to_string(),
                );
                "/api/v1/public?command=returnOrderBook"
            }
            MarketType::Perpetual => {
                params.insert(
                    "base".to_string(),
                    normalize_coinw_perp_base(&request.symbol.exchange_symbol.symbol)?,
                );
                "/v1/perpumPublic/depth"
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        let value = self.rest.send_public_request(endpoint, &params).await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
