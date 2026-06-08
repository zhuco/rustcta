#![allow(dead_code)]

use std::sync::atomic::Ordering;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeeRateSnapshot, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce};
use serde::Serialize;
use serde_json::{json, Value};

use super::parser::{
    normalize_hyperliquid_coin, parse_balance_and_positions, parse_fills, parse_open_orders,
    parse_order_status,
};
use super::HyperliquidGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Serialize, Clone)]
pub struct HyperliquidOrderWire {
    pub a: u32,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct HyperliquidOrderAction {
    #[serde(rename = "type")]
    pub action_type: &'static str,
    pub orders: Vec<HyperliquidOrderWire>,
    pub grouping: &'static str,
}

#[derive(Debug, Serialize, Clone)]
pub struct HyperliquidCancelWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub o: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct HyperliquidCancelAction {
    #[serde(rename = "type")]
    pub action_type: &'static str,
    pub cancels: Vec<HyperliquidCancelWire>,
}

#[derive(Debug, Serialize, Clone)]
pub struct HyperliquidScheduleCancelAction {
    #[serde(rename = "type")]
    pub action_type: &'static str,
    pub time: Option<u64>,
}

impl HyperliquidGatewayAdapter {
    pub async fn get_hyperliquid_clearinghouse_state(&self) -> ExchangeApiResult<Value> {
        self.rest
            .send_info(json!({
                "type": "clearinghouseState",
                "user": self.account_address("hyperliquid.clearinghouse_state")?,
            }))
            .await
    }

    pub async fn get_hyperliquid_open_orders(&self) -> ExchangeApiResult<Value> {
        self.rest
            .send_info(json!({
                "type": "openOrders",
                "user": self.account_address("hyperliquid.open_orders")?,
            }))
            .await
    }

    pub async fn get_hyperliquid_frontend_open_orders(&self) -> ExchangeApiResult<Value> {
        self.rest
            .send_info(json!({
                "type": "frontendOpenOrders",
                "user": self.account_address("hyperliquid.frontend_open_orders")?,
            }))
            .await
    }

    pub async fn get_hyperliquid_user_fills(
        &self,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        aggregate_by_time: bool,
    ) -> ExchangeApiResult<Value> {
        let mut body = if start_time_ms.is_some() || end_time_ms.is_some() {
            json!({
                "type": "userFillsByTime",
                "user": self.account_address("hyperliquid.user_fills")?,
                "aggregateByTime": aggregate_by_time,
            })
        } else {
            json!({
                "type": "userFills",
                "user": self.account_address("hyperliquid.user_fills")?,
            })
        };
        if let Some(start_time_ms) = start_time_ms {
            body["startTime"] = json!(start_time_ms);
        }
        if let Some(end_time_ms) = end_time_ms {
            body["endTime"] = json!(end_time_ms);
        }
        self.rest.send_info(body).await
    }

    pub async fn get_hyperliquid_historical_orders(&self) -> ExchangeApiResult<Value> {
        self.rest
            .send_info(json!({
                "type": "historicalOrders",
                "user": self.account_address("hyperliquid.historical_orders")?,
            }))
            .await
    }

    pub async fn schedule_hyperliquid_cancel(
        &self,
        time_ms: Option<u64>,
    ) -> ExchangeApiResult<Value> {
        let action = HyperliquidScheduleCancelAction {
            action_type: "scheduleCancel",
            time: time_ms,
        };
        self.send_exchange_action("hyperliquid.schedule_cancel", &action)
            .await
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_perpetual(
            request.market_type,
            "hyperliquid.spot_balances_unsupported",
        )?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "hyperliquid.balances")?;
        let value = self.get_hyperliquid_clearinghouse_state().await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            balances: vec![
                parse_balance_and_positions(&self.exchange_id, tenant_id, account_id, &value)?.0,
            ],
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_perpetual(
            request.market_type,
            "hyperliquid.spot_positions_unsupported",
        )?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "hyperliquid.positions")?;
        let value = self.get_hyperliquid_clearinghouse_state().await?;
        let mut positions =
            parse_balance_and_positions(&self.exchange_id, tenant_id, account_id, &value)?.1;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_hyperliquid_coin(&symbol.symbol))
                .collect::<ExchangeApiResult<Vec<_>>>()?;
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
            self.ensure_perpetual(symbol.market_type, "hyperliquid.spot_fees_unsupported")?;
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols),
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(
            request.symbol.market_type,
            "hyperliquid.spot_place_order_unsupported",
        )?;
        self.ensure_private_rest("hyperliquid.place_order")?;
        let action = HyperliquidOrderAction {
            action_type: "order",
            orders: vec![self.order_wire_from_request(&request).await?],
            grouping: "na",
        };
        let value = self
            .send_exchange_action("hyperliquid.place_order", &action)
            .await?;
        let order = parse_order_ack(
            &self.exchange_id,
            &request.symbol,
            &value,
            request.side,
            request.order_type,
            &request.quantity,
            request.price.clone(),
        )?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(
            request.symbol.market_type,
            "hyperliquid.spot_cancel_order_unsupported",
        )?;
        self.ensure_private_rest("hyperliquid.cancel_order")?;
        let by_cloid = request.client_order_id.is_some() && request.exchange_order_id.is_none();
        let action = HyperliquidCancelAction {
            action_type: if by_cloid { "cancelByCloid" } else { "cancel" },
            cancels: vec![self.cancel_wire_from_request(&request, by_cloid).await?],
        };
        let value = self
            .send_exchange_action("hyperliquid.cancel_order", &action)
            .await?;
        let order = parse_cancel_ack(
            &self.exchange_id,
            &request.symbol,
            &value,
            request.exchange_order_id,
            request.client_order_id,
        )?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("hyperliquid.batch_place_orders")?;
        let mut wires = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_perpetual(
                order.symbol.market_type,
                "hyperliquid.spot_batch_place_unsupported",
            )?;
            wires.push(self.order_wire_from_request(order).await?);
        }
        let action = HyperliquidOrderAction {
            action_type: "order",
            orders: wires,
            grouping: "na",
        };
        let value = self
            .send_exchange_action("hyperliquid.batch_place_orders", &action)
            .await?;
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            orders.push(parse_order_ack(
                &self.exchange_id,
                &order.symbol,
                &value,
                order.side,
                order.order_type,
                &order.quantity,
                order.price.clone(),
            )?);
        }
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
        self.ensure_private_rest("hyperliquid.batch_cancel_orders")?;
        let by_cloid = request
            .cancels
            .iter()
            .all(|cancel| cancel.client_order_id.is_some() && cancel.exchange_order_id.is_none());
        if !by_cloid
            && request
                .cancels
                .iter()
                .any(|cancel| cancel.exchange_order_id.is_none())
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "hyperliquid batch cancel cannot mix cloid-only cancels with oid cancels"
                    .to_string(),
            });
        }
        let mut wires = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_perpetual(
                cancel.symbol.market_type,
                "hyperliquid.spot_batch_cancel_unsupported",
            )?;
            wires.push(self.cancel_wire_from_request(cancel, by_cloid).await?);
        }
        let action = HyperliquidCancelAction {
            action_type: if by_cloid { "cancelByCloid" } else { "cancel" },
            cancels: wires,
        };
        let value = self
            .send_exchange_action("hyperliquid.batch_cancel_orders", &action)
            .await?;
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            orders.push(parse_cancel_ack(
                &self.exchange_id,
                &cancel.symbol,
                &value,
                cancel.exchange_order_id.clone(),
                cancel.client_order_id.clone(),
            )?);
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled_count: orders.len() as u32,
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
        self.ensure_optional_perpetual(
            request.market_type,
            "hyperliquid.spot_cancel_all_unsupported",
        )?;
        self.ensure_private_rest("hyperliquid.cancel_all_orders")?;
        let open_orders = self.get_hyperliquid_open_orders().await?;
        let mut orders = parse_open_orders(&self.exchange_id, &open_orders)?;
        if let Some(requested_symbol) = &request.symbol {
            self.ensure_exchange(&requested_symbol.exchange)?;
            self.ensure_perpetual(
                requested_symbol.market_type,
                "hyperliquid.spot_cancel_all_unsupported",
            )?;
            orders.retain(|order| {
                order
                    .exchange_symbol
                    .symbol
                    .eq_ignore_ascii_case(&requested_symbol.exchange_symbol.symbol)
            });
        }
        if orders.is_empty() {
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
            });
        }
        let mut cancels = Vec::with_capacity(orders.len());
        for order in &orders {
            let symbol =
                super::parser::symbol_scope(&self.exchange_id, &order.exchange_symbol.symbol)?;
            cancels.push(CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                symbol,
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: order.exchange_order_id.clone(),
            });
        }
        let cancelled = self
            .batch_cancel_orders_impl(BatchCancelOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                cancels,
            })
            .await?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled_count: cancelled.cancelled_count,
            orders: cancelled.orders,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(
            request.symbol.market_type,
            "hyperliquid.spot_query_order_unsupported",
        )?;
        if request.client_order_id.is_none() && request.exchange_order_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "hyperliquid.query_order requires cloid or oid".to_string(),
            });
        }
        let value = self.get_hyperliquid_historical_orders().await?;
        let target_oid = request.exchange_order_id.as_deref();
        let target_cloid = request.client_order_id.as_deref();
        let empty = Vec::new();
        let filtered = Value::Array(
            value
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .filter(|item| {
                    let order = item.get("order").unwrap_or(item);
                    target_oid.is_some_and(|oid| string_eq(order.get("oid"), oid))
                        || target_cloid.is_some_and(|cloid| string_eq(order.get("cloid"), cloid))
                })
                .cloned()
                .collect(),
        );
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: filtered
                .as_array()
                .and_then(|orders| orders.first())
                .map(|item| parse_order_status(&self.exchange_id, item))
                .transpose()?
                .flatten(),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_perpetual(
            request.market_type,
            "hyperliquid.spot_open_orders_unsupported",
        )?;
        let value = self.get_hyperliquid_open_orders().await?;
        let mut orders = parse_open_orders(&self.exchange_id, &value)?;
        if let Some(symbol) = &request.symbol {
            let coin = normalize_hyperliquid_coin(&symbol.exchange_symbol.symbol)?;
            orders.retain(|order| order.exchange_symbol.symbol == coin);
        }
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_perpetual(request.market_type, "hyperliquid.spot_fills_unsupported")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "hyperliquid.recent_fills")?;
        let value = self
            .get_hyperliquid_user_fills(
                request.start_time.map(|time| time.timestamp_millis()),
                request.end_time.map(|time| time.timestamp_millis()),
                true,
            )
            .await?;
        let mut fills = parse_fills(&self.exchange_id, tenant_id, account_id, &value)?;
        if let Some(symbol) = &request.symbol {
            let coin = normalize_hyperliquid_coin(&symbol.exchange_symbol.symbol)?;
            fills.retain(|fill| {
                fill.exchange_symbol
                    .as_ref()
                    .is_some_and(|symbol| symbol.symbol == coin)
            });
        }
        if let Some(limit) = request.limit {
            fills.truncate(limit as usize);
        }
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fills,
        })
    }

    async fn order_wire_from_request(
        &self,
        request: &PlaceOrderRequest,
    ) -> ExchangeApiResult<HyperliquidOrderWire> {
        let price = request
            .price
            .clone()
            .or_else(|| {
                if request.order_type == OrderType::Market {
                    Some("0".to_string())
                } else {
                    None
                }
            })
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hyperliquid limit orders require price".to_string(),
            })?;
        Ok(HyperliquidOrderWire {
            a: self
                .asset_id_for_symbol(&request.symbol.exchange_symbol.symbol)
                .await?,
            b: request.side == OrderSide::Buy,
            p: price,
            s: request.quantity.clone(),
            r: request.reduce_only,
            t: order_type_wire(request.order_type, request.time_in_force, request.post_only),
            c: request.client_order_id.clone(),
        })
    }

    async fn cancel_wire_from_request(
        &self,
        request: &CancelOrderRequest,
        by_cloid: bool,
    ) -> ExchangeApiResult<HyperliquidCancelWire> {
        let asset = self
            .asset_id_for_symbol(&request.symbol.exchange_symbol.symbol)
            .await?;
        let order_id = request
            .exchange_order_id
            .as_ref()
            .and_then(|value| value.parse::<u64>().ok());
        if order_id.is_none() && request.client_order_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "hyperliquid.cancel_order requires cloid or oid".to_string(),
            });
        }
        Ok(HyperliquidCancelWire {
            a: if by_cloid { None } else { Some(asset) },
            asset: if by_cloid { Some(asset) } else { None },
            o: if by_cloid { None } else { order_id },
            cloid: if by_cloid {
                request.client_order_id.clone()
            } else {
                None
            },
        })
    }

    async fn send_exchange_action<T: Serialize>(
        &self,
        operation: &'static str,
        action: &T,
    ) -> ExchangeApiResult<Value> {
        let credentials = self.private_credentials(operation)?;
        let nonce = self.next_nonce.fetch_add(1, Ordering::SeqCst);
        self.rest
            .send_exchange_action(action, &credentials, nonce)
            .await
    }
}

fn order_type_wire(
    order_type: OrderType,
    time_in_force: Option<TimeInForce>,
    post_only: bool,
) -> Value {
    if order_type == OrderType::Market {
        return json!({ "limit": { "tif": "Ioc" } });
    }
    let tif = match (post_only, order_type, time_in_force) {
        (true, _, _) | (_, OrderType::PostOnly, _) | (_, _, Some(TimeInForce::GTX)) => "Alo",
        (_, OrderType::IOC, _) | (_, _, Some(TimeInForce::IOC)) => "Ioc",
        _ => "Gtc",
    };
    json!({ "limit": { "tif": tif } })
}

fn string_eq(value: Option<&Value>, expected: &str) -> bool {
    value
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
        .is_some_and(|value| value == expected)
}

fn parse_fee_snapshots(symbols: &[SymbolScope]) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: "0.0001".to_string(),
            taker_rate: "0.00035".to_string(),
            source: Some(
                "hyperliquid.default_fee_schedule_until_user_fee_endpoint_is_wired".to_string(),
            ),
            updated_at: Utc::now(),
        })
        .collect()
}

fn parse_order_ack(
    exchange_id: &rustcta_types::ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
    request_side: OrderSide,
    request_type: OrderType,
    request_quantity: &str,
    request_price: Option<String>,
) -> ExchangeApiResult<OrderState> {
    let status = first_status(value);
    let (exchange_order_id, order_status) =
        if let Some(resting) = status.and_then(|status| status.get("resting")) {
            (value_string(resting.get("oid")), OrderStatus::Open)
        } else if let Some(filled) = status.and_then(|status| status.get("filled")) {
            (value_string(filled.get("oid")), OrderStatus::Filled)
        } else if status.and_then(|status| status.get("error")).is_some() {
            (None, OrderStatus::Rejected)
        } else {
            (None, OrderStatus::New)
        };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id,
        side: request_side,
        position_side: Some(PositionSide::Net),
        order_type: request_type,
        time_in_force: None,
        status: order_status,
        quantity: request_quantity.to_string(),
        price: request_price,
        filled_quantity: status
            .and_then(|status| status.get("filled"))
            .and_then(|filled| value_string(filled.get("totalSz")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: status
            .and_then(|status| status.get("filled"))
            .and_then(|filled| value_string(filled.get("avgPx"))),
        reduce_only: false,
        post_only: request_type == OrderType::PostOnly,
        created_at: None,
        updated_at: Utc::now(),
    })
}

fn parse_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> ExchangeApiResult<OrderState> {
    let status = first_status(value);
    let cancelled = status
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("success"));
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: if cancelled {
            OrderStatus::Cancelled
        } else {
            OrderStatus::Unknown
        },
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    })
}

fn first_status(value: &Value) -> Option<&Value> {
    value
        .get("response")
        .and_then(|response| response.get("data"))
        .and_then(|data| data.get("statuses"))
        .and_then(Value::as_array)
        .and_then(|statuses| statuses.first())
}

fn value_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
