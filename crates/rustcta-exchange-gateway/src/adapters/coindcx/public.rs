use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    coindcx_futures_symbol, coindcx_market_symbol, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::CoinDcxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinDcxGatewayAdapter {
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
        let market_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Spot);
        let value = if market_type == MarketType::Perpetual {
            self.rest
                .send_futures_public_get(
                    "/exchange/v1/derivatives/futures/data/active_instruments",
                    &HashMap::new(),
                )
                .await?
        } else {
            self.rest
                .send_spot_public_get("/exchange/v1/markets_details", &HashMap::new())
                .await?
        };
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: parse_symbol_rules(
                &self.exchange_id,
                &request.symbols,
                &value,
                Some(market_type),
            )?,
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
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert(
                    "pair".to_string(),
                    coindcx_market_symbol(&request.symbol.exchange_symbol.symbol),
                );
                if let Some(depth) = request.depth {
                    params.insert("limit".to_string(), depth.min(50).to_string());
                }
                self.rest
                    .send_public_market_get("/market_data/orderbook", &params)
                    .await?
            }
            MarketType::Perpetual => {
                let instrument = coindcx_futures_symbol(&request.symbol.exchange_symbol.symbol);
                let depth = request.depth.unwrap_or(50).min(50);
                self.rest
                    .send_public_market_get(
                        &format!("/market_data/v3/orderbook/{instrument}/{depth}"),
                        &params,
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book: parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?,
        })
    }
}
