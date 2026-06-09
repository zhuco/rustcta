use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{parse_fills, parse_orders, parse_positions, parse_subaccount_balance};
use super::signing::{
    node_write_project_unimplemented, BATCH_CANCEL_PROJECT_UNIMPLEMENTED,
    BATCH_PLACE_PROJECT_UNIMPLEMENTED, NODE_WRITE_PROJECT_UNIMPLEMENTED,
};
use super::DydxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DydxGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "dydx.get_balances")?;
        let value = self.subaccount().await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: vec![parse_subaccount_balance(
                &self.exchange_id,
                tenant_id,
                account_id,
                &value,
            )?],
        })
    }

    pub(super) async fn get_positions_private_rest(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "dydx.get_positions")?;
        let mut params = HashMap::new();
        params.insert(
            "address".to_string(),
            self.wallet_address("dydx.get_positions")?.to_string(),
        );
        params.insert(
            "subaccountNumber".to_string(),
            self.config.subaccount_number.to_string(),
        );
        params.insert("status".to_string(), "OPEN".to_string());
        let value = self.indexer.get("/v4/perpetualPositions", &params).await?;
        let mut positions = parse_positions(&self.exchange_id, tenant_id, account_id, &value)?;
        if !request.symbols.is_empty() {
            positions.retain(|position| {
                position.exchange_symbol.as_ref().is_some_and(|symbol| {
                    request
                        .symbols
                        .iter()
                        .any(|requested| requested.symbol.eq_ignore_ascii_case(&symbol.symbol))
                })
            });
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_private_rest(
        &self,
        _request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "dydx.fee_tiers_not_mapped_to_shared_fee_snapshot",
        })
    }

    pub(super) async fn place_order_private_rest(
        &self,
        _request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        Err(node_write_project_unimplemented(
            NODE_WRITE_PROJECT_UNIMPLEMENTED,
        ))
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        Err(node_write_project_unimplemented(
            NODE_WRITE_PROJECT_UNIMPLEMENTED,
        ))
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        Err(node_write_project_unimplemented(
            BATCH_PLACE_PROJECT_UNIMPLEMENTED,
        ))
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        Err(node_write_project_unimplemented(
            BATCH_CANCEL_PROJECT_UNIMPLEMENTED,
        ))
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        Err(node_write_project_unimplemented(
            NODE_WRITE_PROJECT_UNIMPLEMENTED,
        ))
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "dydx query_order requires exchange_order_id".to_string(),
                })?;
        let endpoint = format!("/v4/orders/{}", urlencoding::encode(&order_id));
        let value = self.indexer.get(&endpoint, &HashMap::new()).await?;
        let order = parse_orders(
            &self.exchange_id,
            &serde_json::json!({
                "orders": [value.get("order").cloned().unwrap_or(value)]
            }),
        )?
        .into_iter()
        .next();
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
        let mut params = self.subaccount_params("dydx.get_open_orders")?;
        params.insert("status".to_string(), "OPEN".to_string());
        if let Some(symbol) = &request.symbol {
            params.insert("ticker".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        let value = self.indexer.get("/v4/orders", &params).await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "dydx.get_recent_fills")?;
        let mut params = self.subaccount_params("dydx.get_recent_fills")?;
        if let Some(symbol) = &request.symbol {
            params.insert("ticker".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let value = self.indexer.get("/v4/fills", &params).await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(&self.exchange_id, tenant_id, account_id, &value)?,
        })
    }

    async fn subaccount(&self) -> ExchangeApiResult<serde_json::Value> {
        let endpoint = format!(
            "/v4/addresses/{}/subaccountNumber/{}",
            urlencoding::encode(self.wallet_address("dydx.subaccount")?),
            self.config.subaccount_number
        );
        self.indexer.get(&endpoint, &HashMap::new()).await
    }

    fn subaccount_params(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<HashMap<String, String>> {
        let mut params = HashMap::new();
        params.insert(
            "address".to_string(),
            self.wallet_address(operation)?.to_string(),
        );
        params.insert(
            "subaccountNumber".to_string(),
            self.config.subaccount_number.to_string(),
        );
        Ok(params)
    }
}
