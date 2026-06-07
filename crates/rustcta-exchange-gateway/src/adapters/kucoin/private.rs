use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::normalize_kucoin_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::KuCoinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl KuCoinGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get("kucoin.get_balances", "/api/v1/accounts", &HashMap::new())
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            MarketType::Spot,
            &request.assets,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoin get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbols".to_string(),
                normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("kucoin.get_fees", "/api/v1/trade-fees", &params)
                .await?;
            fees.extend(parse_fee_snapshots(
                &self.exchange_id,
                std::slice::from_ref(symbol),
                &value,
            )?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin query_order requires exchange_order_id".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoin_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_get(
                "kucoin.query_order",
                &format!("/api/v1/hf/orders/{order_id}"),
                &params,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("status".to_string(), "active".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "kucoin.get_open_orders",
                "/api/v1/hf/orders/active",
                &params,
            )
            .await?;
        let orders = parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startAt".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("endAt".to_string(), end_time.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("pageSize".to_string(), limit.min(500).to_string());
        }
        let value = self
            .send_signed_get("kucoin.get_recent_fills", "/api/v1/hf/fills", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}
