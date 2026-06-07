use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_symbol, parse_futures_symbol_rules, parse_orderbook_snapshot, parse_spot_symbol_rules,
};
use super::AscendexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl AscendexGatewayAdapter {
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
            match market_type {
                MarketType::Spot => {
                    let value = self
                        .rest
                        .send_public_get("/api/pro/v1/cash/products", &HashMap::new())
                        .await?;
                    rules.extend(parse_spot_symbol_rules(&self.exchange_id, &value)?);
                }
                MarketType::Perpetual => {
                    let value = self
                        .rest
                        .send_public_get("/api/pro/v2/futures/contract", &HashMap::new())
                        .await?;
                    rules.extend(parse_futures_symbol_rules(&self.exchange_id, &value)?);
                }
                _ => unreachable!("checked by ensure_supported_market"),
            }
        }

        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| {
                    normalize_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
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
            normalize_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        let value = self
            .rest
            .send_public_get("/api/pro/v1/depth", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(
            &self.exchange_id,
            request.symbol.market_type,
            request.symbol,
            &value,
        )?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
