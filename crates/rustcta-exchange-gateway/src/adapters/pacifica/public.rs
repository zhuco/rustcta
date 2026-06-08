#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use urlencoding::encode;

use super::parser::{parse_order_book_snapshot, parse_symbol_rules_response};
use super::transport::PacificaTransport;
use super::PacificaGatewayAdapter;

impl PacificaGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let transport = PacificaTransport::new(self.config.clone())?;
        let payload = transport.send_public_get("/api/v1/info").await?;
        let mut response = parse_symbol_rules_response(self.exchange_id.clone(), &payload)?;
        response.schema_version = EXCHANGE_API_SCHEMA_VERSION;
        response.metadata.request_id = request.context.request_id;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| symbol.exchange_symbol.symbol.to_ascii_uppercase())
                .collect::<std::collections::HashSet<_>>();
            response.rules.retain(|rule| {
                requested.contains(&rule.symbol.exchange_symbol.symbol.to_ascii_uppercase())
            });
        }
        Ok(response)
    }

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let symbol = encode(&request.symbol.exchange_symbol.symbol);
        let path = format!("/api/v1/book?symbol={symbol}");
        let transport = PacificaTransport::new(self.config.clone())?;
        let payload = transport.send_public_get(&path).await?;
        let mut order_book = parse_order_book_snapshot(
            self.exchange_id.clone(),
            request.symbol.market_type,
            &payload,
        )?;
        order_book.exchange_symbol = Some(request.symbol.exchange_symbol);
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: rustcta_exchange_api::ResponseMetadata {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: self.exchange_id.clone(),
                request_id: request.context.request_id,
                received_at: chrono::Utc::now(),
                exchange_timestamp: order_book.exchange_timestamp,
            },
            order_book,
        })
    }
}
