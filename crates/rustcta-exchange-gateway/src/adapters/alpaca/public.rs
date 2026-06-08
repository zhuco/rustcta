use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{normalize_alpaca_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::AlpacaGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl AlpacaGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_credentials("alpaca.get_symbol_rules")?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("asset_class".to_string(), "crypto".to_string());
        params.insert("status".to_string(), "active".to_string());
        let value = self
            .rest
            .send_broker_get(
                "/v1/assets",
                &params,
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_alpaca_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &value)?;
        if !requested.is_empty() {
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
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
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_credentials("alpaca.get_order_book")?;
        let mut params = HashMap::new();
        params.insert(
            "symbols".to_string(),
            normalize_alpaca_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let endpoint = format!(
            "/v1beta3/crypto/{}/latest/orderbooks",
            self.config.crypto_location
        );
        let value = self
            .rest
            .send_market_data_get(
                &endpoint,
                &params,
                &self.config.api_key,
                &self.config.api_secret,
            )
            .await?;
        let mut response = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        response.metadata = response_metadata(self.exchange_id.clone(), request.context.request_id);
        Ok(response)
    }
}
