use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{normalize_symbol, parse_orderbook_response, parse_symbol_rules};
use super::transport::HtxRestProduct;
use super::HtxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl HtxGatewayAdapter {
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
        let requested = request
            .symbols
            .iter()
            .map(normalize_symbol)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let wants_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Spot);
        let wants_perp = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let mut rules = Vec::new();
        if wants_spot {
            let value = self
                .rest
                .send_public_get(HtxRestProduct::Spot, "/v1/common/symbols", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Spot,
                &value,
            )?);
        }
        if wants_perp {
            let value = self
                .rest
                .send_public_get(
                    HtxRestProduct::LinearSwap,
                    "/linear-swap-api/v1/swap_contract_info",
                    &HashMap::new(),
                )
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Perpetual,
                &value,
            )?);
        }
        if !requested.is_empty() {
            rules.retain(|rule| {
                requested.contains(&normalize_symbol(&rule.symbol).unwrap_or_default())
            });
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
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert("symbol".to_string(), normalize_symbol(&request.symbol)?);
                params.insert("type".to_string(), "step0".to_string());
                (HtxRestProduct::Spot, "/market/depth")
            }
            MarketType::Perpetual => {
                params.insert(
                    "contract_code".to_string(),
                    normalize_symbol(&request.symbol)?,
                );
                params.insert("type".to_string(), "step0".to_string());
                (HtxRestProduct::LinearSwap, "/linear-swap-ex/market/depth")
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .rest
            .send_public_get(endpoint.0, endpoint.1, &params)
            .await?;
        parse_orderbook_response(
            &self.exchange_id,
            request.symbol,
            request.context.request_id,
            &value,
        )
    }
}
