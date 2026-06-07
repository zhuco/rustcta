use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{
    market_currency, market_kind, normalize_depth, normalize_orangex_symbol,
    parse_orderbook_snapshot, parse_symbol_rules,
};
use super::OrangeXGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl OrangeXGatewayAdapter {
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

        let mut market_types = request
            .symbols
            .iter()
            .map(|symbol| symbol.market_type)
            .collect::<Vec<_>>();
        if market_types.is_empty() {
            market_types = vec![MarketType::Spot, MarketType::Perpetual];
        }
        market_types.sort_by_key(|market_type| match market_type {
            MarketType::Spot => 0,
            MarketType::Perpetual => 1,
            _ => 2,
        });
        market_types.dedup();

        let mut rules = Vec::new();
        for market_type in market_types {
            let value = self
                .rest
                .send_public_rpc(
                    "/public/get_instruments",
                    json!({
                        "currency": market_currency(market_type)?,
                        "kind": market_kind(market_type)?,
                    }),
                )
                .await?;
            rules.extend(parse_symbol_rules(&self.exchange_id, market_type, &value)?);
        }

        let requested = request
            .symbols
            .iter()
            .map(|symbol| {
                normalize_orangex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        if !requested.is_empty() {
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
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
        let depth = normalize_depth(request.depth.unwrap_or(50));
        let instrument_name = normalize_orangex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?;
        let value = self
            .rest
            .send_public_rpc(
                "/public/get_order_book",
                json!({
                    "instrument_name": instrument_name,
                    "depth": depth,
                }),
            )
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
