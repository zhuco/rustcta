use std::collections::BTreeMap;

use reqwest::Method;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PageCursor, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_futures_symbol;
use super::private_parser::{
    ack_order_state, cancelled_order_state, parse_balances, parse_order_list, parse_order_state,
    parse_positions, parse_recent_fills,
};
use super::KrakenFuturesGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl KrakenFuturesGatewayAdapter {
    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        validate_batch_place_request(self, &request)?;
        let batch = request
            .orders
            .iter()
            .map(|order| {
                futures_order_params(order).map(|params| {
                    json!({
                        "order": "send",
                        "params": params,
                    })
                })
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut params = BTreeMap::new();
        params.insert("batchOrder".to_string(), json_string(json!(batch))?);
        let value = self
            .send_futures_private(
                "krakenfutures.futures.batch_place_orders",
                Method::POST,
                "batchorder",
                params,
            )
            .await?;
        let exchange_order_ids = batch_order_ids(&value);
        let orders = request
            .orders
            .iter()
            .enumerate()
            .map(|(index, order_request)| {
                ack_order_state(
                    &self.exchange_id,
                    order_request,
                    exchange_order_ids.get(index).cloned().flatten(),
                )
            })
            .collect();
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        validate_batch_cancel_request(self, &request)?;
        let batch = request
            .cancels
            .iter()
            .map(|cancel| {
                let mut params = BTreeMap::new();
                params.insert("order_id".to_string(), cancel_id(cancel)?);
                Ok(json!({
                    "order": "cancel",
                    "params": params,
                }))
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut params = BTreeMap::new();
        params.insert("batchOrder".to_string(), json_string(json!(batch))?);
        self.send_futures_private(
            "krakenfutures.futures.batch_cancel_orders",
            Method::POST,
            "batchorder",
            params,
        )
        .await?;
        let orders = request
            .cancels
            .iter()
            .map(|cancel| {
                cancelled_order_state(
                    &self.exchange_id,
                    &cancel.symbol,
                    cancel.exchange_order_id.clone(),
                    cancel.client_order_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let params = futures_order_params(&request)?;
        let value = self
            .send_futures_private(
                "krakenfutures.futures.place_order",
                Method::POST,
                "sendorder",
                params,
            )
            .await?;
        let exchange_order_id = value
            .get("txid")
            .and_then(serde_json::Value::as_array)
            .and_then(|ids| ids.first())
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                value
                    .get("sendStatus")
                    .and_then(|status| status.get("order_id"))
                    .and_then(serde_json::Value::as_str)
            })
            .map(str::to_string);
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            exchange_order_id.as_deref(),
            &value,
        )
        .unwrap_or_else(|_| ack_order_state(&self.exchange_id, &request, exchange_order_id));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "krakenfutures.quote_sized_market_order",
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let id = request
            .exchange_order_id
            .as_deref()
            .or(request.client_order_id.as_deref())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "krakenfutures cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let mut params = BTreeMap::new();
        params.insert("order_id".to_string(), id.to_string());
        self.send_futures_private(
            "krakenfutures.futures.cancel_order",
            Method::POST,
            "cancelorder",
            params,
        )
        .await?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: cancelled_order_state(
                &self.exchange_id,
                &request.symbol,
                request.exchange_order_id,
                request.client_order_id,
            ),
            cancelled: true,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
        }
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(rustcta_types::MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        let mut params = BTreeMap::new();
        if let Some(symbol) = &request.symbol {
            params.insert("symbol".to_string(), normalize_futures_symbol(symbol)?);
        }
        self.send_futures_private(
            "krakenfutures.futures.cancel_all_orders",
            Method::POST,
            "cancelallorders",
            params,
        )
        .await?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            orders: Vec::new(),
            cancelled_count: 0,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "get_balances")?;
        let market_type = request
            .market_type
            .unwrap_or(rustcta_types::MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        let value = self
            .send_futures_private(
                "krakenfutures.futures.get_balances",
                Method::GET,
                "accounts",
                BTreeMap::new(),
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "get_positions")?;
        let value = self
            .send_futures_private(
                "krakenfutures.futures.get_positions",
                Method::GET,
                "openpositions",
                BTreeMap::new(),
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            positions: parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "krakenfutures get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
        }
        Err(ExchangeApiError::Unsupported {
            operation: "krakenfutures.fees_source_boundary_only",
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let id = request
            .exchange_order_id
            .as_deref()
            .or(request.client_order_id.as_deref())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "krakenfutures query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let mut params = BTreeMap::new();
        params.insert("orderIds".to_string(), id.to_string());
        let value = self
            .send_futures_private(
                "krakenfutures.futures.query_order",
                Method::GET,
                "orders",
                params,
            )
            .await?;
        let order = parse_order_list(&self.exchange_id, Some(&request.symbol), &value)?
            .into_iter()
            .next();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(rustcta_types::MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        let value = self
            .send_futures_private(
                "krakenfutures.futures.get_open_orders",
                Method::GET,
                "openorders",
                BTreeMap::new(),
            )
            .await?;
        let mut orders = parse_order_list(&self.exchange_id, request.symbol.as_ref(), &value)?;
        if let Some(symbol) = &request.symbol {
            orders.retain(|order| {
                order
                    .exchange_symbol
                    .symbol
                    .eq_ignore_ascii_case(&symbol.exchange_symbol.symbol)
            });
        }
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "get_recent_fills")?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(rustcta_types::MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        let value = self
            .send_futures_private(
                "krakenfutures.futures.get_recent_fills",
                Method::GET,
                "fills",
                recent_fills_params(&request)?,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            fills: parse_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }
}

fn recent_fills_params(
    request: &RecentFillsRequest,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    if let Some(page) = &request.page {
        page.validate(Some(1000))
            .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    }
    if request
        .limit
        .is_some_and(|limit| limit == 0 || limit > 1000)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "krakenfutures get_recent_fills limit must be between 1 and 1000".to_string(),
        });
    }
    let mut params = BTreeMap::new();
    if let Some(symbol) = &request.symbol {
        params.insert("symbol".to_string(), normalize_futures_symbol(symbol)?);
    }
    if let Some(order_id) = request
        .exchange_order_id
        .as_deref()
        .or(request.client_order_id.as_deref())
    {
        params.insert("orderIds".to_string(), order_id.to_string());
    }
    let limit = request
        .page
        .as_ref()
        .and_then(|page| page.limit)
        .or(request.limit);
    if let Some(limit) = limit {
        params.insert("count".to_string(), limit.min(1000).to_string());
    }
    if let Some(start_time) = request.start_time.as_ref() {
        params.insert("lastFillTime".to_string(), start_time.to_rfc3339());
    }
    if let Some(from_trade_id) = request.from_trade_id.as_deref() {
        params.insert("lastFillTime".to_string(), from_trade_id.to_string());
    }
    if let Some(cursor) = request.page.as_ref().and_then(|page| page.cursor.as_ref()) {
        match cursor {
            PageCursor::Timestamp { millis } => {
                let seconds = millis.div_euclid(1000);
                let nanos = millis.rem_euclid(1000) as u32 * 1_000_000;
                let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos)
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: format!(
                            "invalid krakenfutures futures fills timestamp cursor {millis}"
                        ),
                    })?;
                params.insert("lastFillTime".to_string(), timestamp.to_rfc3339());
            }
            PageCursor::Token { token } | PageCursor::Id { id: token } => {
                params.insert("lastFillTime".to_string(), token.clone());
            }
            PageCursor::Offset { .. } | PageCursor::TimeRange { .. } => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "krakenfutures futures fills pagination supports timestamp/token cursors, not offset or time-range cursors".to_string(),
                });
            }
        }
    }
    Ok(params)
}

fn validate_batch_place_request(
    adapter: &KrakenFuturesGatewayAdapter,
    request: &BatchPlaceOrdersRequest,
) -> ExchangeApiResult<MarketType> {
    if request.orders.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "krakenfutures batch_place_orders requires at least one order".to_string(),
        });
    }
    let market_type = request.orders[0].symbol.market_type;
    adapter.ensure_market_type(market_type)?;
    for order in &request.orders {
        adapter.ensure_exchange(&order.symbol.exchange)?;
        adapter.ensure_market_type(order.symbol.market_type)?;
        if order.symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "krakenfutures batch_place_orders requires one market type".to_string(),
            });
        }
    }
    Ok(market_type)
}

fn validate_batch_cancel_request(
    adapter: &KrakenFuturesGatewayAdapter,
    request: &BatchCancelOrdersRequest,
) -> ExchangeApiResult<MarketType> {
    if request.cancels.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "krakenfutures batch_cancel_orders requires at least one cancel".to_string(),
        });
    }
    let market_type = request.cancels[0].symbol.market_type;
    adapter.ensure_market_type(market_type)?;
    for cancel in &request.cancels {
        adapter.ensure_exchange(&cancel.symbol.exchange)?;
        adapter.ensure_market_type(cancel.symbol.market_type)?;
        if cancel.symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "krakenfutures batch_cancel_orders requires one market type".to_string(),
            });
        }
        let _ = cancel_id(cancel)?;
    }
    Ok(market_type)
}

fn cancel_id(request: &CancelOrderRequest) -> ExchangeApiResult<String> {
    request
        .exchange_order_id
        .as_deref()
        .or(request.client_order_id.as_deref())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "krakenfutures batch cancel requires exchange_order_id or client_order_id"
                .to_string(),
        })
}

fn json_string(value: Value) -> ExchangeApiResult<String> {
    serde_json::to_string(&value).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("failed to encode krakenfutures batch payload: {error}"),
    })
}

fn batch_order_ids(value: &Value) -> Vec<Option<String>> {
    if let Some(txids) = value.get("txid").and_then(Value::as_array) {
        return txids.iter().map(|value| value_text(Some(value))).collect();
    }
    batch_rows(value)
        .iter()
        .map(|row| {
            value_text(row.get("txid"))
                .or_else(|| {
                    row.get("txid")
                        .and_then(Value::as_array)
                        .and_then(|ids| ids.first())
                        .and_then(|value| value_text(Some(value)))
                })
                .or_else(|| value_text(row.get("order_id")))
                .or_else(|| value_text(row.get("orderId")))
                .or_else(|| {
                    row.get("sendStatus")
                        .and_then(|status| value_text(status.get("order_id")))
                })
        })
        .collect()
}

fn batch_rows(value: &Value) -> Vec<&Value> {
    value
        .get("orders")
        .or_else(|| value.get("batchStatus"))
        .or_else(|| value.get("elements"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .map(|rows| rows.iter().collect())
        .unwrap_or_default()
}

fn value_text(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| {
        value
            .as_str()
            .map(str::to_string)
            .or_else(|| value.as_i64().map(|number| number.to_string()))
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })
}

fn futures_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_futures_symbol(&request.symbol)?,
    );
    params.insert("side".to_string(), side_text(request.side).to_string());
    params.insert(
        "orderType".to_string(),
        futures_order_type(request)?.to_string(),
    );
    params.insert("size".to_string(), request.quantity.clone());
    if request.order_type.requires_limit_price() {
        params.insert(
            "limitPrice".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "krakenfutures futures limit-style order requires price".to_string(),
                })?,
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("cliOrdId".to_string(), client_order_id.to_string());
    }
    if request.reduce_only {
        params.insert("reduceOnly".to_string(), "true".to_string());
    }
    Ok(params)
}

fn futures_order_type(request: &PlaceOrderRequest) -> ExchangeApiResult<&'static str> {
    Ok(match request.order_type {
        OrderType::Market => "mkt",
        OrderType::Limit => "lmt",
        OrderType::PostOnly => "post",
        OrderType::IOC => "ioc",
        OrderType::FOK => "fok",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "krakenfutures.futures_order_type",
            })
        }
    })
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
