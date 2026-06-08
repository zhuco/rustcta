use super::parser::{parse_orderbook_snapshot, parse_symbol_rule, split_pair};
use super::WavesExchangeGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

impl WavesExchangeGatewayAdapter {
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
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "wavesexchange.public_rest_disabled",
            });
        }
        let mut rules = Vec::new();
        for symbol in request.symbols {
            let (amount_asset, price_asset) = split_pair(&symbol.exchange_symbol)?;
            let response = self
                .rest
                .send_public_get(&format!(
                    "/matcher/orderbook/{amount_asset}/{price_asset}/info"
                ))
                .await?;
            rules.push(parse_symbol_rule(&self.exchange_id, symbol, &response)?);
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "wavesexchange.public_rest_disabled",
            });
        }
        let (amount_asset, price_asset) = split_pair(&request.symbol.exchange_symbol)?;
        let depth = request.depth.map(|depth| depth.to_string());
        let path = match depth {
            Some(depth) => format!("/matcher/orderbook/{amount_asset}/{price_asset}?depth={depth}"),
            None => format!("/matcher/orderbook/{amount_asset}/{price_asset}"),
        };
        let response = self.rest.send_public_get(&path).await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &response)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
