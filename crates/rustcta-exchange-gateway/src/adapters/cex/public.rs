use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{normalize_cex_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::CexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CexGatewayAdapter {
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
        }
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_cex_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut value = self
            .rest
            .send_public_get("/currency_limits", &HashMap::new())
            .await?;
        retain_requested_pairs(&mut value, &requested);
        let rules = parse_symbol_rules(&self.exchange_id, &value)?;
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
        let (base, quote) = normalize_cex_symbol(&request.symbol.exchange_symbol.symbol)?;
        let endpoint = format!("/order_book/{base}/{quote}/");
        let mut params = HashMap::new();
        if let Some(depth) = request.depth {
            params.insert("depth".to_string(), depth.clamp(1, 100).to_string());
        }
        let value = self.rest.send_public_get(&endpoint, &params).await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, request.depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn retain_requested_pairs(value: &mut serde_json::Value, requested: &[(String, String)]) {
    if requested.is_empty() {
        return;
    }
    let Some(pairs) = value
        .get_mut("data")
        .and_then(|data| data.get_mut("pairs"))
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };
    pairs.retain(|pair| {
        let base = pair
            .get("symbol1")
            .and_then(serde_json::Value::as_str)
            .map(str::to_ascii_uppercase);
        let quote = pair
            .get("symbol2")
            .and_then(serde_json::Value::as_str)
            .map(str::to_ascii_uppercase);
        base.zip(quote)
            .is_some_and(|pair| requested.iter().any(|requested| requested == &pair))
    });
}
