use std::collections::BTreeMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, PageCursor, PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_bithumb_symbol;
use super::private_parser::{
    cancel_ack_order, parse_balances, parse_fee_snapshot, parse_fills_from_orders, parse_order,
    parse_orders,
};
use super::BithumbGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BithumbGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("bithumb.get_balances")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bithumb.get_balances")?;
        let value = self
            .rest
            .send_signed_get("/v1/accounts", &BTreeMap::new())
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_fees_private_rest(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bithumb.get_fees requires at least one symbol".to_string(),
            });
        }
        self.ensure_private_rest("bithumb.get_fees")?;
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = BTreeMap::new();
            params.insert(
                "market".to_string(),
                normalize_bithumb_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .rest
                .send_signed_get("/v1/orders/chance", &params)
                .await?;
            fees.push(parse_fee_snapshot(symbol, &value)?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("bithumb.place_order")?;
        let body = bithumb_place_order_body(&request)?;
        let value = self.rest.send_signed_post("/v2/orders", &body).await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("bithumb.place_quote_market_order")?;
        let body = bithumb_quote_market_order_body(&request)?;
        let value = self.rest.send_signed_post("/v2/orders", &body).await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("bithumb.cancel_order")?;
        let params = bithumb_cancel_order_query(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let value = self.rest.send_signed_delete("/v2/order", &params).await?;
        let order =
            parse_order(&self.exchange_id, Some(&request.symbol), &value).unwrap_or_else(|_| {
                cancel_ack_order(
                    &self.exchange_id,
                    &request.symbol,
                    request.exchange_order_id,
                    request.client_order_id,
                )
            });
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("bithumb.query_order")?;
        let params = bithumb_query_order_query(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let value = self.rest.send_signed_get("/v1/order", &params).await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                &value,
            )?),
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("bithumb.get_open_orders")?;
        let mut params = BTreeMap::new();
        params.insert("state".to_string(), "wait".to_string());
        params.insert("limit".to_string(), "100".to_string());
        params.insert("order_by".to_string(), "desc".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "market".to_string(),
                normalize_bithumb_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        if let Some(PageCursor::Offset { offset }) =
            request.page.as_ref().and_then(|p| p.cursor.as_ref())
        {
            params.insert("page".to_string(), (offset + 1).to_string());
        }
        let value = self.rest.send_signed_get("/v1/orders", &params).await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("bithumb.get_recent_fills")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bithumb.get_recent_fills")?;
        let mut params = BTreeMap::new();
        params.insert("state".to_string(), "done".to_string());
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).min(100).to_string(),
        );
        params.insert("order_by".to_string(), "desc".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "market".to_string(),
                normalize_bithumb_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        if let Some(order_id) = request.exchange_order_id.as_ref() {
            params.insert("uuids[]".to_string(), order_id.clone());
        }
        if let Some(client_order_id) = request.client_order_id.as_ref() {
            params.insert("client_order_ids[]".to_string(), client_order_id.clone());
        }
        let value = self.rest.send_signed_get("/v1/orders", &params).await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills_from_orders(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bithumb.batch_place_orders supports at most 20 orders".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in request.orders {
            orders.push(self.place_order_private_rest(order).await?.order);
        }
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.cancels.len() > 30 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bithumb.batch_cancel_orders supports at most 30 cancels".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        let mut cancelled_count = 0_u32;
        for cancel in request.cancels {
            let response = self.cancel_order_private_rest(cancel).await?;
            if response.cancelled {
                cancelled_count += 1;
            }
            orders.push(response.order);
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }
}

pub fn bithumb_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "bithumb.reduce_only_spot_order",
        });
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "bithumb.post_only_order",
        });
    }
    if !matches!(
        request.time_in_force,
        None | Some(TimeInForce::GTC) | Some(TimeInForce::IOC) | Some(TimeInForce::FOK)
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "bithumb.time_in_force",
        });
    }
    let order_type = match request.order_type {
        OrderType::Market => match request.side {
            OrderSide::Buy => "price",
            OrderSide::Sell => "market",
        },
        OrderType::Limit | OrderType::IOC | OrderType::FOK => "limit",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bithumb.order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "market".to_string(),
        json!(normalize_bithumb_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert("order_type".to_string(), json!(order_type));
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("client_order_id".to_string(), json!(client_order_id));
    }
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "bid",
            OrderSide::Sell => "ask",
        }),
    );
    match order_type {
        "limit" => {
            body.insert("price".to_string(), json!(required_price(request)?));
            body.insert("volume".to_string(), json!(request.quantity));
        }
        "price" => {
            body.insert(
                "price".to_string(),
                json!(request
                    .quote_quantity
                    .as_ref()
                    .or(request.price.as_ref())
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "bithumb market buy requires quote_quantity or price".to_string(),
                    })?),
            );
        }
        "market" => {
            body.insert("volume".to_string(), json!(request.quantity));
        }
        _ => {}
    }
    Ok(Value::Object(body))
}

pub fn bithumb_quote_market_order_body(
    request: &QuoteMarketOrderRequest,
) -> ExchangeApiResult<Value> {
    if request.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bithumb.non_spot_quote_market_order",
        });
    }
    Ok(json!({
        "client_order_id": request.client_order_id,
        "market": normalize_bithumb_symbol(&request.symbol.exchange_symbol.symbol)?,
        "order_type": "price",
        "price": request.quote_quantity,
        "side": "bid",
    }))
}

pub fn bithumb_cancel_order_query(
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("order_id".to_string(), order_id.to_string());
    } else if let Some(client_order_id) = client_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("client_order_id".to_string(), client_order_id.to_string());
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bithumb.cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

pub fn bithumb_query_order_query(
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    if let Some(uuid) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("uuid".to_string(), uuid.to_string());
    } else if let Some(client_order_id) = client_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("client_order_id".to_string(), client_order_id.to_string());
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bithumb.query_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn required_price(request: &PlaceOrderRequest) -> ExchangeApiResult<&str> {
    request
        .price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bithumb limit order requires price".to_string(),
        })
}
