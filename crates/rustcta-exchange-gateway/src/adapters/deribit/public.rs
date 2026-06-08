use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    currency_for_symbol, normalize_deribit_symbol, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::DeribitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DeribitGatewayAdapter {
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

        let mut responses = Vec::new();
        if request.symbols.is_empty() {
            for (currency, kind) in [
                ("BTC", "future"),
                ("ETH", "future"),
                ("BTC", "option"),
                ("ETH", "option"),
            ] {
                let mut params = HashMap::new();
                params.insert("currency".to_string(), currency.to_string());
                params.insert("kind".to_string(), kind.to_string());
                params.insert("expired".to_string(), "false".to_string());
                let value = self
                    .rest
                    .send_public_get("/api/v2/public/get_instruments", &params)
                    .await?;
                responses.extend(parse_symbol_rules(&self.exchange_id, &value)?);
            }
        } else {
            let mut grouped = Vec::<(String, String)>::new();
            for symbol in &request.symbols {
                let kind = match symbol.market_type {
                    rustcta_types::MarketType::Option => "option",
                    rustcta_types::MarketType::Futures | rustcta_types::MarketType::Perpetual => {
                        "future"
                    }
                    _ => "future",
                };
                let group = (currency_for_symbol(symbol), kind.to_string());
                if !grouped.contains(&group) {
                    grouped.push(group);
                }
            }
            for (currency, kind) in grouped {
                let mut params = HashMap::new();
                params.insert("currency".to_string(), currency);
                params.insert("kind".to_string(), kind);
                params.insert("expired".to_string(), "false".to_string());
                let value = self
                    .rest
                    .send_public_get("/api/v2/public/get_instruments", &params)
                    .await?;
                responses.extend(parse_symbol_rules(&self.exchange_id, &value)?);
            }
            let requested = request
                .symbols
                .iter()
                .map(normalize_deribit_symbol)
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            responses.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
        }

        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: responses,
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
            "instrument_name".to_string(),
            normalize_deribit_symbol(&request.symbol)?,
        );
        params.insert(
            "depth".to_string(),
            request.depth.unwrap_or(20).clamp(1, 1000).to_string(),
        );
        let value = self
            .rest
            .send_public_get("/api/v2/public/get_order_book", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
