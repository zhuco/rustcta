use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::normalize_gateio_symbol;
use super::private_parser::{
    parse_balances, parse_fees, parse_fills, parse_open_orders, parse_order,
};
use super::GateIoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl GateIoGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "gateio.get_balances")?;
        let value = self
            .send_signed_get("gateio.get_balances", "/spot/accounts", &HashMap::new())
            .await?;
        let balances = parse_balances(
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
                message: "gateio.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "currency_pair".to_string(),
                normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("gateio.get_fees", "/spot/fee", &params)
                .await?;
            fees.push(parse_fees(&self.exchange_id, symbol, &value)?);
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
                message: "gateio.query_order requires exchange_order_id".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "currency_pair".to_string(),
            normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let endpoint = format!("/spot/orders/{order_id}");
        let value = self
            .send_signed_get("gateio.query_order", &endpoint, &params)
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
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
        self.ensure_optional_spot(request.market_type)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "currency_pair".to_string(),
                normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("gateio.get_open_orders", "/spot/open_orders", &params)
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
        self.ensure_optional_spot(request.market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "gateio.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "currency_pair".to_string(),
            normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(from_trade_id) = request.from_trade_id.as_deref() {
            params.insert("last_id".to_string(), from_trade_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert("from".to_string(), start_time.timestamp().to_string());
        }
        if let Some(end_time) = request.end_time {
            params.insert("to".to_string(), end_time.timestamp().to_string());
        }
        let value = self
            .send_signed_get("gateio.get_recent_fills", "/spot/my_trades", &params)
            .await?;
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
