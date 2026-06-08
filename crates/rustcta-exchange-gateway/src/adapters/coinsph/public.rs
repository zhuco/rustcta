use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::parser::{
    ensure_php_symbol, normalize_coinsph_symbol, normalize_depth, parse_orderbook_snapshot,
    parse_symbol_rules,
};
use super::CoinsPhGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinsPhGatewayAdapter {
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
            self.ensure_spot(symbol.market_type)?;
            ensure_php_symbol(symbol, "coinsph.non_php_market")?;
        }

        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_coinsph_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut params = HashMap::new();
        if requested.len() == 1 {
            params.insert("symbol".to_string(), requested[0].clone());
        }

        let mut response = self
            .rest
            .send_public_request("/openapi/v1/exchangeInfo", &params)
            .await?;
        retain_requested_symbols(&mut response, &requested);
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested.is_empty() {
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
        self.ensure_spot(request.symbol.market_type)?;
        ensure_php_symbol(&request.symbol, "coinsph.non_php_market")?;
        let depth = normalize_depth(request.depth.unwrap_or(5));
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_coinsph_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("limit".to_string(), depth.to_string());
        // Coins.ph keeps exchangeInfo under the Binance-like /openapi/v1 tree, but
        // live book snapshots are served from the local quote API namespace.
        let value = self
            .rest
            .send_public_request("/openapi/quote/v1/depth", &params)
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
            .and_then(|symbol| normalize_coinsph_symbol(symbol).ok())
            .is_some_and(|symbol| requested.contains(&symbol))
    });
}
