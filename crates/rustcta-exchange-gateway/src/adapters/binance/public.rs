use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::Value;

use super::parser::{
    normalize_binance_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::BinanceGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BinanceGatewayAdapter {
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
            let requested = request
                .symbols
                .iter()
                .filter(|symbol| symbol.market_type == market_type)
                .map(|symbol| normalize_binance_symbol(&symbol.exchange_symbol.symbol))
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            let mut params = HashMap::new();
            if requested.len() == 1 {
                params.insert("symbol".to_string(), requested[0].clone());
            }
            let endpoint = match market_type {
                MarketType::Spot => "/api/v3/exchangeInfo",
                MarketType::Perpetual => "/fapi/v1/exchangeInfo",
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut response = self
                .rest_for_market(market_type)?
                .send_public_request(endpoint, &params)
                .await?;
            retain_requested_symbols(&mut response, &requested);
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                market_type,
                &response,
            )?);
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
        let depth = normalize_depth(request.depth.unwrap_or(5), request.symbol.market_type);
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binance_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("limit".to_string(), depth.to_string());
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v3/depth",
            MarketType::Perpetual => "/fapi/v1/depth",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .rest_for_market(request.symbol.market_type)?
            .send_public_request(endpoint, &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn retain_requested_symbols(response: &mut Value, requested: &[String]) {
    if requested.is_empty() {
        return;
    }
    let Some(symbols) = response.get_mut("symbols").and_then(Value::as_array_mut) else {
        return;
    };
    symbols.retain(|symbol| {
        symbol
            .get("symbol")
            .and_then(Value::as_str)
            .and_then(|symbol| normalize_binance_symbol(symbol).ok())
            .is_some_and(|symbol| requested.contains(&symbol))
    });
}
