use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, TimeInForce};
use serde_json::{json, Value};

use super::parser::normalize_bitfinex_symbol;
use super::private_parser::{
    parse_balances, parse_fills, parse_order, parse_orders, parse_positions,
};
use super::BitfinexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitfinexGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitfinex.balances")?;
        let value = self
            .send_signed_post("bitfinex.get_balances", "/v2/auth/r/wallets", json!({}))
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
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
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        if !matches!(market_type, MarketType::Margin | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitfinex.spot_positions",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitfinex.positions")?;
        let value = self
            .send_signed_post("bitfinex.get_positions", "/v2/auth/r/positions", json!({}))
            .await?;
        let mut positions = parse_positions(&self.exchange_id, tenant_id, account_id, &value)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| symbol.symbol.clone())
                .collect::<Vec<_>>();
            positions.retain(|position| {
                position
                    .exchange_symbol
                    .as_ref()
                    .is_some_and(|symbol| requested.contains(&symbol.symbol))
            });
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: request
                .symbols
                .into_iter()
                .map(|symbol| FeeRateSnapshot {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    symbol,
                    maker_rate: "0".to_string(),
                    taker_rate: "0".to_string(),
                    source: Some("bitfinex.runtime_fee_endpoint_unsupported".to_string()),
                    updated_at: chrono::Utc::now(),
                })
                .collect(),
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = bitfinex_place_order_body(&request)?;
        let value = self
            .send_signed_post("bitfinex.place_order", "/v2/auth/w/order/submit", body)
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: parse_order(&self.exchange_id, request.symbol.market_type, &value)?,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let id = request
            .exchange_order_id
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitfinex.cancel_by_client_id_requires_cid_date",
            })?;
        let value = self
            .send_signed_post(
                "bitfinex.cancel_order",
                "/v2/auth/w/order/cancel",
                json!({ "id": parse_i64("exchange_order_id", &id)? }),
            )
            .await?;
        let order = parse_order(&self.exchange_id, request.symbol.market_type, &value)?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled: true,
            order,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bitfinex.amend_requires_side_and_verified_amount_semantics",
        })
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let ids = request
            .cancels
            .iter()
            .map(|cancel| {
                cancel
                    .exchange_order_id
                    .as_ref()
                    .ok_or(ExchangeApiError::Unsupported {
                        operation: "bitfinex.batch_cancel_by_client_id_requires_cid_date",
                    })
                    .and_then(|id| parse_i64("exchange_order_id", id))
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .send_signed_post(
                "bitfinex.batch_cancel_orders",
                "/v2/auth/w/order/cancel/multi",
                json!({ "id": ids }),
            )
            .await?;
        let market_type = request
            .cancels
            .first()
            .map(|cancel| cancel.symbol.market_type)
            .unwrap_or(MarketType::Spot);
        let orders = parse_orders(&self.exchange_id, market_type, &value).unwrap_or_default();
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled_count: ids.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let value = self
            .send_signed_post(
                "bitfinex.cancel_all_orders",
                "/v2/auth/w/order/cancel/multi",
                json!({ "all": 1 }),
            )
            .await?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        let orders = parse_orders(&self.exchange_id, market_type, &value).unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let id = request
            .exchange_order_id
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitfinex.query_by_client_id_requires_cid_date",
            })?;
        let value = self
            .send_signed_post(
                "bitfinex.query_order",
                "/v2/auth/r/orders",
                json!({ "id": parse_i64("exchange_order_id", &id)? }),
            )
            .await?;
        let order = parse_orders(&self.exchange_id, request.symbol.market_type, &value)?
            .into_iter()
            .next();
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
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let endpoint = if let Some(symbol) = &request.symbol {
            format!(
                "/v2/auth/r/orders/{}",
                normalize_bitfinex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?
            )
        } else {
            "/v2/auth/r/orders".to_string()
        };
        let value = self
            .send_signed_post("bitfinex.get_open_orders", &endpoint, json!({}))
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            orders: parse_orders(&self.exchange_id, market_type, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitfinex.recent_fills")?;
        let endpoint = if let Some(symbol) = &request.symbol {
            format!(
                "/v2/auth/r/trades/{}/hist",
                normalize_bitfinex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?
            )
        } else {
            "/v2/auth/r/trades/hist".to_string()
        };
        let mut body = serde_json::Map::new();
        if let Some(limit) = request.limit {
            body.insert("limit".to_string(), json!(limit.min(2500)));
        }
        if let Some(start) = request.start_time {
            body.insert("start".to_string(), json!(start.timestamp_millis()));
        }
        if let Some(end) = request.end_time {
            body.insert("end".to_string(), json!(end.timestamp_millis()));
        }
        let value = self
            .send_signed_post("bitfinex.get_recent_fills", &endpoint, Value::Object(body))
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
                &value,
            )?,
        })
    }
}

pub fn bitfinex_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "type".to_string(),
        json!(bitfinex_order_type(
            request.symbol.market_type,
            request.order_type,
            request.time_in_force,
            request.post_only
        )?),
    );
    body.insert(
        "symbol".to_string(),
        json!(normalize_bitfinex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type
        )?),
    );
    let amount = request
        .quote_quantity
        .as_ref()
        .unwrap_or(&request.quantity)
        .as_str();
    body.insert(
        "amount".to_string(),
        json!(signed_amount(
            request.symbol.market_type,
            request.side,
            amount
        )?),
    );
    body.insert(
        "price".to_string(),
        json!(request.price.clone().unwrap_or_else(|| "0".to_string())),
    );
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "cid".to_string(),
            json!(parse_i64("client_order_id", client_order_id)?),
        );
    }
    let flags = bitfinex_order_flags(request.reduce_only, request.post_only);
    if flags != 0 {
        body.insert("flags".to_string(), json!(flags));
    }
    Ok(Value::Object(body))
}

fn bitfinex_order_type(
    market_type: MarketType,
    order_type: OrderType,
    time_in_force: Option<TimeInForce>,
    post_only: bool,
) -> ExchangeApiResult<&'static str> {
    if post_only || order_type == OrderType::PostOnly {
        return Ok(if market_type == MarketType::Spot {
            "EXCHANGE LIMIT"
        } else {
            "LIMIT"
        });
    }
    let base = match (order_type, time_in_force) {
        (OrderType::Market, _) => "MARKET",
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => "IOC",
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => "FOK",
        (OrderType::Limit, _) => "LIMIT",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitfinex.stop_order_shared_mapping",
            })
        }
    };
    Ok(if market_type == MarketType::Spot {
        match base {
            "MARKET" => "EXCHANGE MARKET",
            "IOC" => "EXCHANGE IOC",
            "FOK" => "EXCHANGE FOK",
            _ => "EXCHANGE LIMIT",
        }
    } else {
        base
    })
}

fn signed_amount(
    _market_type: MarketType,
    side: OrderSide,
    amount: &str,
) -> ExchangeApiResult<String> {
    let value = amount
        .parse::<f64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("invalid Bitfinex amount {amount}"),
        })?
        .abs();
    Ok(match side {
        OrderSide::Buy => value.to_string(),
        OrderSide::Sell => format!("-{value}"),
    })
}

fn bitfinex_order_flags(reduce_only: bool, post_only: bool) -> i64 {
    let mut flags = 0;
    if reduce_only {
        flags |= 1024;
    }
    if post_only {
        flags |= 4096;
    }
    flags
}

fn parse_i64(field: &str, value: &str) -> ExchangeApiResult<i64> {
    value
        .parse::<i64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("{field} must be a Bitfinex int id"),
        })
}

impl BitfinexGatewayAdapter {
    pub(super) async fn place_order_list_impl(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "bitfinex.order_list_runtime",
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "bitfinex.batch_place_runtime",
        })
    }
}
