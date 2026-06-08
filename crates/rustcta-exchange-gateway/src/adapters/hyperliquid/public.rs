#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{normalize_hyperliquid_coin, parse_orderbook_snapshot, parse_symbol_rules};
use super::HyperliquidGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl HyperliquidGatewayAdapter {
    pub async fn get_hyperliquid_meta(&self) -> ExchangeApiResult<Value> {
        self.rest.send_info(json!({ "type": "meta" })).await
    }

    pub async fn get_hyperliquid_meta_and_asset_ctxs(&self) -> ExchangeApiResult<Value> {
        self.rest
            .send_info(json!({ "type": "metaAndAssetCtxs" }))
            .await
    }

    pub async fn get_hyperliquid_l2_book(&self, coin: &str) -> ExchangeApiResult<Value> {
        self.rest
            .send_info(json!({
                "type": "l2Book",
                "coin": normalize_hyperliquid_coin(coin)?,
            }))
            .await
    }

    pub async fn get_hyperliquid_funding_history(
        &self,
        coin: &str,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
    ) -> ExchangeApiResult<Value> {
        let mut body = json!({
            "type": "fundingHistory",
            "coin": normalize_hyperliquid_coin(coin)?,
        });
        if let Some(start_time_ms) = start_time_ms {
            body["startTime"] = json!(start_time_ms);
        }
        if let Some(end_time_ms) = end_time_ms {
            body["endTime"] = json!(end_time_ms);
        }
        self.rest.send_info(body).await
    }

    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(
                symbol.market_type,
                "hyperliquid.spot_symbol_rules_unsupported",
            )?;
        }
        let value = self.get_hyperliquid_meta_and_asset_ctxs().await?;
        let rules = parse_symbol_rules(&self.exchange_id, &request.symbols, &value)?;
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
        self.ensure_perpetual(
            request.symbol.market_type,
            "hyperliquid.spot_order_book_unsupported",
        )?;
        let value = self
            .get_hyperliquid_l2_book(&request.symbol.exchange_symbol.symbol)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}
