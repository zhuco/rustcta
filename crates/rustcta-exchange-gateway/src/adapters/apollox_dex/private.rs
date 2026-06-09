use std::collections::HashMap;

use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, ExchangeApiError, ExchangeApiResult,
    OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::{
    normalize_apollox_symbol, parse_batch_place_orders_ack, parse_open_orders, parse_order_state,
    parse_recent_fills, time_in_force_value,
};
use super::signing::apollox_signed_query;
use super::transport::ApolloxDexRest;
use super::ApolloxDexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

impl ApolloxDexGatewayAdapter {
    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(
                    request.exchange,
                    request.context.request_id,
                ),
                orders: Vec::new(),
                report: None,
            });
        }
        if request.orders.len() > 5 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ApolloX DEX batch place accepts at most 5 orders".to_string(),
            });
        }
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_perpetual(order.symbol.market_type)?;
        }
        let (api_key, api_secret) =
            self.private_credentials("apollox_dex.batch_place_orders_private_rest_not_enabled")?;
        let batch_orders = batch_place_orders_json(&request.orders)?;
        let params = vec![
            ("batchOrders".to_string(), batch_orders),
            ("recvWindow".to_string(), "5000".to_string()),
            (
                "timestamp".to_string(),
                chrono::Utc::now().timestamp_millis().to_string(),
            ),
        ];
        let (query, signature) = apollox_signed_query(&api_secret, params)?;
        let mut signed_params = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (key.to_string(), value.to_string())
            })
            .collect::<HashMap<_, _>>();
        signed_params.insert("signature".to_string(), signature);
        let value = self
            .rest
            .send_signed_post("/fapi/v1/batchOrders", &signed_params, &api_key)
            .await?;
        let (orders, report) = parse_batch_place_orders_ack(&self.exchange_id, &request, &value)?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: crate::adapters::response_metadata(
                request.exchange,
                request.context.request_id,
            ),
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_apollox_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ApolloX DEX query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_read_get(
                QUERY_ORDER_UNSUPPORTED,
                ApolloxDexRest::place_order_path(),
                params,
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
        let market_type = request
            .market_type
            .unwrap_or(rustcta_types::MarketType::Perpetual);
        self.ensure_perpetual(market_type)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "ApolloX DEX get_open_orders market_type must match symbol"
                        .to_string(),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_apollox_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_read_get(
                OPEN_ORDERS_UNSUPPORTED,
                ApolloxDexRest::open_orders_path(),
                params,
            )
            .await?;
        let orders = parse_open_orders(
            &self.exchange_id,
            request.symbol.as_ref(),
            market_type,
            &value,
        )?;
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
        let market_type = request
            .market_type
            .unwrap_or(rustcta_types::MarketType::Perpetual);
        self.ensure_perpetual(market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "ApolloX DEX get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        if symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ApolloX DEX get_recent_fills market_type must match symbol".to_string(),
            });
        }
        let (tenant_id, account_id) = context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_apollox_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(from_trade_id) = request.from_trade_id.as_deref() {
            params.insert("fromId".to_string(), from_trade_id.to_string());
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
            request.limit.unwrap_or(1000).min(1000).to_string(),
        );
        let value = self
            .send_signed_read_get(
                RECENT_FILLS_UNSUPPORTED,
                ApolloxDexRest::user_trades_path(),
                params,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_signed_read_get(
        &self,
        operation: &'static str,
        path: &str,
        params: HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let mut params = params;
        params.insert("recvWindow".to_string(), "5000".to_string());
        params.insert(
            "timestamp".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        );
        let (query, signature) = apollox_signed_query(&api_secret, params)?;
        let mut signed_params = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (key.to_string(), value.to_string())
            })
            .collect::<HashMap<_, _>>();
        signed_params.insert("signature".to_string(), signature);
        self.rest
            .send_signed_get(path, &signed_params, &api_key)
            .await
    }
}

fn context_account(
    context: &rustcta_exchange_api::RequestContext,
) -> ExchangeApiResult<(
    rustcta_exchange_api::TenantId,
    rustcta_exchange_api::AccountId,
)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "ApolloX DEX private REST readback requires context.tenant_id".to_string(),
        })?;
    let account_id =
        context
            .account_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "ApolloX DEX private REST readback requires context.account_id"
                    .to_string(),
            })?;
    Ok((tenant_id, account_id))
}

pub(super) fn batch_place_orders_json(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<String> {
    let values = orders
        .iter()
        .map(batch_place_order_json)
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    serde_json::to_string(&values).map_err(|error| ExchangeApiError::Serialization {
        message: error.to_string(),
    })
}

fn batch_place_order_json(order: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let symbol = normalize_apollox_symbol(&order.symbol.exchange_symbol.symbol)?;
    let side = match order.side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };
    let order_type = if order.post_only || order.order_type == OrderType::PostOnly {
        "LIMIT"
    } else {
        match order.order_type {
            OrderType::Market => "MARKET",
            OrderType::Limit | OrderType::IOC | OrderType::FOK => "LIMIT",
            OrderType::PostOnly => "LIMIT",
            OrderType::StopMarket | OrderType::StopLimit => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "apollox_dex.batch_place_stop_orders_unmapped",
                })
            }
        }
    };
    if order_type == "LIMIT" && order.price.as_deref().unwrap_or("").trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "ApolloX DEX batch limit order requires price".to_string(),
        });
    }
    let mut value = serde_json::Map::new();
    value.insert("symbol".to_string(), json!(symbol));
    value.insert("side".to_string(), json!(side));
    value.insert("type".to_string(), json!(order_type));
    if order_type == "LIMIT" {
        value.insert(
            "timeInForce".to_string(),
            json!(time_in_force_value(order.time_in_force, order.post_only)),
        );
        value.insert(
            "price".to_string(),
            json!(order.price.as_deref().unwrap_or_default()),
        );
    }
    value.insert("quantity".to_string(), json!(order.quantity));
    value.insert(
        "reduceOnly".to_string(),
        json!(order.reduce_only.to_string()),
    );
    if let Some(client_order_id) = order.client_order_id.as_deref() {
        value.insert("newClientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(value))
}

pub(super) const BALANCES_UNSUPPORTED: &str =
    "apollox_dex.private_rest_runtime_disabled_balances_request_spec_only";
pub(super) const POSITIONS_UNSUPPORTED: &str =
    "apollox_dex.private_rest_runtime_disabled_positions_request_spec_only";
pub(super) const FEES_UNSUPPORTED: &str =
    "apollox_dex.fee_readback_requires_private_rest_runtime_audit";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "apollox_dex.place_order_request_spec_only_no_live_writes";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "apollox_dex.cancel_order_request_spec_only_no_live_writes";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "apollox_dex.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "apollox_dex.order_list_unsupported";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "apollox_dex.batch_place_orders_request_spec_parser_only_no_private_runtime";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str = "apollox_dex.batch_cancel_unsupported";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "apollox_dex.cancel_all_request_spec_only_no_live_writes";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "apollox_dex.query_order_private_rest_runtime_disabled";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "apollox_dex.open_orders_private_rest_runtime_disabled";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "apollox_dex.recent_fills_private_rest_runtime_disabled";
