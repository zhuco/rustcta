use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    independentreserve_asset, parse_orderbook_snapshot, split_symbol_assets, symbol_scope,
};
use super::IndependentReserveGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

const DEFAULT_SPOT_MARKETS: [&str; 9] = [
    "BTC_AUD", "ETH_AUD", "XRP_AUD", "BTC_SGD", "ETH_SGD", "XRP_SGD", "BTC_USD", "ETH_USD",
    "XRP_USD",
];

impl IndependentReserveGatewayAdapter {
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
            self.ensure_supported_quote(&symbol.exchange_symbol.symbol)?;
        }
        let symbols = if request.symbols.is_empty() {
            DEFAULT_SPOT_MARKETS
                .iter()
                .map(|symbol| symbol_scope(&self.exchange_id, symbol))
                .collect::<ExchangeApiResult<Vec<_>>>()?
        } else {
            request.symbols.clone()
        };
        let rules = symbols
            .iter()
            .map(|symbol| self.static_symbol_rule(symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
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
        self.ensure_supported_quote(&request.symbol.exchange_symbol.symbol)?;
        let (base, quote) = split_symbol_assets(&request.symbol.exchange_symbol.symbol);
        let params = vec![
            (
                "primaryCurrencyCode".to_string(),
                independentreserve_asset(&base),
            ),
            (
                "secondaryCurrencyCode".to_string(),
                independentreserve_asset(&quote),
            ),
        ];
        let value = self
            .rest
            .send_public_get("/Public/GetOrderBook", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }

    fn static_symbol_rule(
        &self,
        symbol: &rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<rustcta_exchange_api::SymbolRules> {
        self.ensure_supported_market_type(symbol.market_type)?;
        self.ensure_supported_quote(&symbol.exchange_symbol.symbol)?;
        let (base, quote) = split_symbol_assets(&symbol.exchange_symbol.symbol);
        Ok(rustcta_exchange_api::SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            base_asset: base,
            quote_asset: quote,
            price_increment: None,
            quantity_increment: None,
            min_price: None,
            max_price: None,
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            price_precision: None,
            quantity_precision: None,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: chrono::Utc::now(),
        })
    }
}
