use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, FeesRequest, FeesResponse, OrderBookRequest, OrderBookResponse,
    SymbolRulesRequest, SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_delta_symbol, normalize_depth, parse_fees_from_products, parse_funding_rates,
    parse_option_chain, parse_orderbook_snapshot, parse_symbol_rules, DeltaFundingRate,
    DeltaOptionChainSnapshot,
};
use super::DeltaGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DeltaGatewayAdapter {
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
        let mut params = HashMap::new();
        params.insert("page_size".to_string(), "100".to_string());
        if let Some(contract_types) = contract_types_for_symbols(&request.symbols) {
            params.insert("contract_types".to_string(), contract_types);
        }
        let value = self.rest.send_public_get("/v2/products", &params).await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| symbol.exchange_symbol.symbol.to_ascii_uppercase())
            .collect::<Vec<_>>();
        let mut rules = parse_symbol_rules(&self.exchange_id, &value)?;
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let symbol = normalize_delta_symbol(&request.symbol)?;
        let mut params = HashMap::new();
        params.insert(
            "depth".to_string(),
            normalize_depth(request.depth.unwrap_or(50)).to_string(),
        );
        let value = self
            .rest
            .send_public_get(&format!("/v2/l2orderbook/{symbol}"), &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("page_size".to_string(), "100".to_string());
        if let Some(contract_types) = contract_types_for_symbols(&request.symbols) {
            params.insert("contract_types".to_string(), contract_types);
        }
        let value = self.rest.send_public_get("/v2/products", &params).await?;
        let fees = parse_fees_from_products(&self.exchange_id, &request.symbols, &value)?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub async fn get_delta_option_chain(
        &self,
        underlying_asset: &str,
    ) -> ExchangeApiResult<DeltaOptionChainSnapshot> {
        let mut params = HashMap::new();
        params.insert(
            "underlying_asset_symbols".to_string(),
            underlying_asset.to_ascii_uppercase(),
        );
        params.insert(
            "contract_types".to_string(),
            "call_options,put_options".to_string(),
        );
        params.insert("page_size".to_string(), "100".to_string());
        let value = self.rest.send_public_get("/v2/products", &params).await?;
        parse_option_chain(&self.exchange_id, underlying_asset, &value)
    }

    pub async fn get_delta_funding_rates(
        &self,
        symbol: Option<rustcta_exchange_api::SymbolScope>,
    ) -> ExchangeApiResult<Vec<DeltaFundingRate>> {
        if let Some(symbol) = &symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_derivative(symbol.market_type)?;
            let value = self
                .rest
                .send_public_get(
                    &format!("/v2/tickers/{}", normalize_delta_symbol(symbol)?),
                    &HashMap::new(),
                )
                .await?;
            let result = value.get("result").cloned().unwrap_or(value);
            return parse_funding_rates(
                &self.exchange_id,
                &serde_json::json!({
                    "success": true,
                    "result": [result]
                }),
            );
        }
        let value = self
            .rest
            .send_public_get("/v2/tickers", &HashMap::new())
            .await?;
        parse_funding_rates(&self.exchange_id, &value)
    }
}

fn contract_types_for_symbols(symbols: &[rustcta_exchange_api::SymbolScope]) -> Option<String> {
    if symbols.is_empty() {
        return Some("perpetual_futures,futures,call_options,put_options".to_string());
    }
    let mut types = Vec::new();
    for symbol in symbols {
        match symbol.market_type {
            MarketType::Perpetual if !types.contains(&"perpetual_futures") => {
                types.push("perpetual_futures")
            }
            MarketType::Futures if !types.contains(&"futures") => types.push("futures"),
            MarketType::Option => {
                if !types.contains(&"call_options") {
                    types.push("call_options");
                }
                if !types.contains(&"put_options") {
                    types.push("put_options");
                }
            }
            _ => {}
        }
    }
    (!types.is_empty()).then(|| types.join(","))
}
