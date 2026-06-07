use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{normalize_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::DigiFinexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DigiFinexGatewayAdapter {
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }

        let has_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Spot);
        let has_swap = request
            .symbols
            .iter()
            .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let mut rules = Vec::new();
        if has_spot {
            let value = self
                .rest
                .send_public_get(false, "/v3/spot/markets", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Spot,
                &value,
            )?);
        }
        if has_swap {
            let value = self
                .rest
                .send_public_get(true, "/swap/v2/public/instruments", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Perpetual,
                &value,
            )?);
        }
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_symbol(&symbol.exchange_symbol.symbol, symbol.market_type))
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let mut params = HashMap::new();
        let normalized = normalize_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?;
        let depth = request.depth.unwrap_or(100).clamp(1, 200).to_string();
        let (endpoint, market_is_swap) = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert("symbol".to_string(), normalized);
                params.insert("limit".to_string(), depth);
                ("/v3/spot/order_book", false)
            }
            MarketType::Perpetual => {
                params.insert("instrument_id".to_string(), normalized);
                params.insert("size".to_string(), depth);
                ("/swap/v2/public/order_book", true)
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .rest
            .send_public_get(market_is_swap, endpoint, &params)
            .await?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order_book: parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?,
        })
    }
}
