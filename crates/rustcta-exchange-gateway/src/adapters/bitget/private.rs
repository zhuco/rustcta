use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::normalize_bitget_symbol;
use super::private_parser::{parse_balances, parse_fees, parse_fills, parse_order, parse_orders};
use super::BitgetGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitgetGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.ensure_private_rest("bitget.get_balances")?;
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_balances",
                "/api/v2/spot/account/assets",
                &HashMap::new(),
            )
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_balances requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_balances requires account_id in request context"
                        .to_string(),
                })?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
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
        self.ensure_private_rest("bitget.get_fees")?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            );
            params.insert("category".to_string(), "SPOT".to_string());
            let value = self
                .rest
                .send_signed_get("bitget.get_fees", "/api/v3/account/fee-rate", &params)
                .await?;
            fees.extend(parse_fees(&self.exchange_id, symbol, &value)?);
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
        self.ensure_private_rest("bitget.query_order")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        match (&request.exchange_order_id, &request.client_order_id) {
            (Some(exchange_order_id), _) => {
                params.insert("orderId".to_string(), exchange_order_id.clone());
            }
            (None, Some(client_order_id)) => {
                params.insert("clientOid".to_string(), client_order_id.clone());
            }
            (None, None) => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitget.query_order requires exchange_order_id or client_order_id"
                        .to_string(),
                });
            }
        }
        let value = self
            .rest
            .send_signed_get(
                "bitget.query_order",
                "/api/v2/spot/trade/orderInfo",
                &params,
            )
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order,
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
        self.ensure_private_rest("bitget.get_open_orders")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_open_orders",
                "/api/v2/spot/trade/unfilled-orders",
                &params,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
        self.ensure_private_rest("bitget.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(exchange_order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), exchange_order_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("clientOid".to_string(), client_order_id.clone());
        }
        if let Some(from_trade_id) = &request.from_trade_id {
            params.insert("idLessThan".to_string(), from_trade_id.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startTime".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "endTime".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).clamp(1, 100).to_string(),
        );
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_recent_fills",
                "/api/v2/spot/trade/fills",
                &params,
            )
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_recent_fills requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_recent_fills requires account_id in request context"
                        .to_string(),
                })?;
        let fills = parse_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            Some(symbol),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}
