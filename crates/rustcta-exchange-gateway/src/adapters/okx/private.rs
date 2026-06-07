use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::normalize_okx_symbol;
use super::types::{parse_balances, parse_fees, parse_fills, parse_order, parse_orders};
use super::OkxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl OkxGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("okx.get_balances")?;
        let value = self
            .rest
            .send_signed_get("/api/v5/account/balance", &HashMap::new())
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_balances requires tenant_id in request context".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_balances requires account_id in request context".to_string(),
                })?;
        let requested_assets = request
            .assets
            .iter()
            .map(|asset| asset.to_ascii_uppercase())
            .collect::<Vec<_>>();
        let mut balances = parse_balances(&self.exchange_id, tenant_id, account_id, &value)?;
        if !requested_assets.is_empty() {
            for balance in &mut balances {
                balance
                    .balances
                    .retain(|asset| requested_assets.contains(&asset.asset));
            }
            balances.retain(|balance| !balance.balances.is_empty());
        }
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_fees_private_rest(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_private_rest("okx.get_fees")?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "okx.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert("instType".to_string(), "SPOT".to_string());
            params.insert(
                "instId".to_string(),
                normalize_okx_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .rest
                .send_signed_get("/api/v5/account/trade-fee", &params)
                .await?;
            fees.extend(parse_fees(&self.exchange_id, Some(symbol), &value)?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("okx.query_order")?;
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        match (&request.exchange_order_id, &request.client_order_id) {
            (Some(exchange_order_id), _) => {
                params.insert("ordId".to_string(), exchange_order_id.clone());
            }
            (None, Some(client_order_id)) => {
                params.insert("clOrdId".to_string(), client_order_id.clone());
            }
            (None, None) => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "okx.query_order requires exchange_order_id or client_order_id"
                        .to_string(),
                });
            }
        }
        let value = self
            .rest
            .send_signed_get("/api/v5/trade/order", &params)
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("okx.get_open_orders")?;
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "instId".to_string(),
                normalize_okx_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .rest
            .send_signed_get("/api/v5/trade/orders-pending", &params)
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("okx.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(exchange_order_id) = &request.exchange_order_id {
            params.insert("ordId".to_string(), exchange_order_id.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "begin".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("end".to_string(), end_time.timestamp_millis().to_string());
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).clamp(1, 100).to_string(),
        );
        let value = self
            .rest
            .send_signed_get("/api/v5/trade/fills-history", &params)
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_recent_fills requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_recent_fills requires account_id in request context"
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
