use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_spot_symbol, parse_futures_orderbook_snapshot, parse_futures_symbol_rules,
    parse_spot_orderbook_snapshot, parse_spot_symbol_rules,
};
use super::CoinstoreGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinstoreGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let wants_perpetual = request
            .symbols
            .iter()
            .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let wants_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Spot);
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let mut rules = Vec::new();
        if wants_spot {
            let value = self
                .rest
                .send_spot_public_post("/v2/public/config/spot/symbols", serde_json::json!({}))
                .await?;
            rules.extend(parse_spot_symbol_rules(&self.exchange_id, &value)?);
        }
        if wants_perpetual {
            let value = self
                .rest
                .send_futures_public_get("/api/configs/public", &[])
                .await?;
            rules.extend(parse_futures_symbol_rules(&self.exchange_id, &value)?);
        }
        if !request.symbols.is_empty() {
            rules.retain(|rule| {
                request.symbols.iter().any(|requested| {
                    requested.market_type == rule.symbol.market_type
                        && requested
                            .exchange_symbol
                            .symbol
                            .eq_ignore_ascii_case(&rule.symbol.exchange_symbol.symbol)
                })
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let depth = request.depth.unwrap_or(20).clamp(5, 100);
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                let symbol = normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?;
                self.rest
                    .send_spot_public_get(
                        &format!("/v1/market/depth/{symbol}"),
                        &[("depth".to_string(), depth.to_string())],
                    )
                    .await?
            }
            MarketType::Perpetual => {
                let contract_id = request
                    .symbol
                    .exchange_symbol
                    .symbol
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                        message: format!(
                            "coinstore futures order book requires numeric contractId in exchange_symbol, got {}",
                            request.symbol.exchange_symbol.symbol
                        ),
                    })?;
                self.rest
                    .send_futures_public_get(
                        "/v1/futureQuot/querySnapshot",
                        &[("contractId".to_string(), contract_id.to_string())],
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        let order_book = match request.symbol.market_type {
            MarketType::Spot => {
                parse_spot_orderbook_snapshot(&self.exchange_id, request.symbol.clone(), &value)?
            }
            MarketType::Perpetual => {
                parse_futures_orderbook_snapshot(&self.exchange_id, request.symbol.clone(), &value)?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order_book,
        })
    }
}
