use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_symbol;
use super::private_parser::{
    ack_order_from_request, cancelled_order_from_request, parse_balances, parse_fee_snapshots,
    parse_fills_from_orders, parse_order, parse_orders, parse_positions,
};
use super::AscendexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

impl AscendexGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "ascendex.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                let endpoint = self.group_endpoint("/api/pro/v1/cash/balance")?;
                self.send_signed_get(
                    "ascendex.get_balances",
                    &endpoint,
                    "balance",
                    &HashMap::new(),
                )
                .await?
            }
            MarketType::Perpetual => {
                let endpoint = self.group_endpoint("/api/pro/v2/futures/position")?;
                self.send_signed_get(
                    "ascendex.get_balances",
                    &endpoint,
                    "v2/futures/position",
                    &HashMap::new(),
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            market_type,
            &request.assets,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "ascendex.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "ascendex.get_positions")?;
        for symbol in &request.symbols {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "ascendex adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
        }
        let endpoint = self.group_endpoint("/api/pro/v2/futures/position")?;
        let value = self
            .send_signed_get(
                "ascendex.get_positions",
                &endpoint,
                "v2/futures/position",
                &HashMap::new(),
            )
            .await?;
        let mut positions = parse_positions(&self.exchange_id, tenant_id, account_id, &value)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_symbol(&symbol.symbol, MarketType::Perpetual))
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            positions.retain(|position| {
                position.exchange_symbol.as_ref().is_some_and(|symbol| {
                    requested
                        .iter()
                        .any(|requested| symbol.symbol.eq_ignore_ascii_case(requested))
                })
            });
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ascendex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let endpoint = match symbol.market_type {
                MarketType::Spot => self.group_endpoint("/api/pro/v1/spot/fee")?,
                MarketType::Perpetual => self.group_endpoint("/api/pro/v1/futures/fee")?,
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let value = self
                .send_signed_get("ascendex.get_fees", &endpoint, "fee", &HashMap::new())
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

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => self.group_endpoint("/api/pro/v1/cash/order")?,
            MarketType::Perpetual => self.group_endpoint("/api/pro/v2/futures/order")?,
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let sign_path = match request.symbol.market_type {
            MarketType::Spot => "order",
            MarketType::Perpetual => "v2/futures/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_json(
                "ascendex.place_order",
                reqwest::Method::POST,
                &endpoint,
                sign_path,
                ascendex_place_order_body(&request)?,
            )
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| ack_order_from_request(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ascendex cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => self.group_endpoint("/api/pro/v1/cash/order")?,
            MarketType::Perpetual => self.group_endpoint("/api/pro/v2/futures/order")?,
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let sign_path = match request.symbol.market_type {
            MarketType::Spot => "order",
            MarketType::Perpetual => "v2/futures/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_json(
                "ascendex.cancel_order",
                reqwest::Method::DELETE,
                &endpoint,
                sign_path,
                ascendex_cancel_body(&request)?,
            )
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| cancelled_order_from_request(&self.exchange_id, &request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
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
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                report: None,
            });
        }
        if request.orders.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ascendex batch_place_orders supports at most 10 orders".to_string(),
            });
        }
        let market_type = batch_order_market_type(&request.orders)?;
        self.ensure_supported_market(market_type)?;
        let mut orders_body = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "ascendex batch_place_orders cannot mix market types".to_string(),
                });
            }
            orders_body.push(ascendex_place_order_body(order)?);
        }
        let (endpoint, sign_path) = match market_type {
            MarketType::Spot => (
                self.group_endpoint("/api/pro/v1/cash/order/batch")?,
                "order/batch",
            ),
            MarketType::Perpetual => (
                self.group_endpoint("/api/pro/v2/futures/order/batch")?,
                "v2/futures/order/batch",
            ),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_json(
                "ascendex.batch_place_orders",
                reqwest::Method::POST,
                &endpoint,
                sign_path,
                json!({ "orders": orders_body }),
            )
            .await?;
        let items = response_items(&value);
        let mut orders = Vec::with_capacity(request.orders.len());
        for (index, order_request) in request.orders.iter().enumerate() {
            let item = items.get(index).copied().unwrap_or(&value);
            let order = parse_order(
                &self.exchange_id,
                Some(&order_request.symbol),
                market_type,
                item,
            )
            .ok()
            .flatten()
            .unwrap_or_else(|| ack_order_from_request(&self.exchange_id, order_request, item));
            orders.push(order);
        }
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
        if request.cancels.is_empty() {
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        if request.cancels.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "ascendex batch_cancel_orders supports at most 10 cancels".to_string(),
            });
        }
        let market_type = batch_cancel_market_type(&request.cancels)?;
        self.ensure_supported_market(market_type)?;
        let mut cancel_body = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "ascendex batch_cancel_orders cannot mix market types".to_string(),
                });
            }
            if missing_order_identity(cancel) {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "ascendex batch_cancel_orders requires ids for every cancel"
                        .to_string(),
                });
            }
            cancel_body.push(ascendex_cancel_body(cancel)?);
        }
        let (endpoint, sign_path) = match market_type {
            MarketType::Spot => (
                self.group_endpoint("/api/pro/v1/cash/order/batch")?,
                "order/batch",
            ),
            MarketType::Perpetual => (
                self.group_endpoint("/api/pro/v2/futures/order/batch")?,
                "v2/futures/order/batch",
            ),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_json(
                "ascendex.batch_cancel_orders",
                reqwest::Method::DELETE,
                &endpoint,
                sign_path,
                json!({ "orders": cancel_body }),
            )
            .await?;
        let items = response_items(&value);
        let mut orders = Vec::with_capacity(request.cancels.len());
        for (index, cancel) in request.cancels.iter().enumerate() {
            let item = items.get(index).copied().unwrap_or(&value);
            let order = parse_order(&self.exchange_id, Some(&cancel.symbol), market_type, item)
                .ok()
                .flatten()
                .unwrap_or_else(|| cancelled_order_from_request(&self.exchange_id, cancel));
            orders.push(order);
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "ascendex cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let endpoint = match symbol.market_type {
            MarketType::Spot => self.group_endpoint("/api/pro/v1/cash/order/all")?,
            MarketType::Perpetual => self.group_endpoint("/api/pro/v2/futures/order/all")?,
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let sign_path = match symbol.market_type {
            MarketType::Spot => "order/all",
            MarketType::Perpetual => "v2/futures/order/all",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let body = json!({
            "symbol": normalize_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            "time": Utc::now().timestamp_millis()
        });
        let value = self
            .send_signed_json(
                "ascendex.cancel_all_orders",
                reqwest::Method::DELETE,
                &endpoint,
                sign_path,
                body,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
            .unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
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
        let order_id =
            request
                .exchange_order_id
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "ascendex query_order requires exchange_order_id".to_string(),
                })?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => self.group_endpoint("/api/pro/v1/cash/order/status")?,
            MarketType::Perpetual => self.group_endpoint("/api/pro/v2/futures/order/status")?,
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let sign_path = match request.symbol.market_type {
            MarketType::Spot => "order/status",
            MarketType::Perpetual => "v2/futures/order/status",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let mut params = HashMap::new();
        params.insert("orderId".to_string(), order_id.clone());
        let value = self
            .send_signed_get("ascendex.query_order", &endpoint, sign_path, &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
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
        let endpoint = match market_type {
            MarketType::Spot => self.group_endpoint("/api/pro/v1/cash/order/open")?,
            MarketType::Perpetual => self.group_endpoint("/api/pro/v2/futures/order/open")?,
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let sign_path = match market_type {
            MarketType::Spot => "order/open",
            MarketType::Perpetual => "v2/futures/order/open",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            params.insert(
                "symbol".to_string(),
                normalize_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let value = self
            .send_signed_get("ascendex.get_open_orders", &endpoint, sign_path, &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(
                &self.exchange_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "ascendex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "ascendex.get_recent_fills")?;
        let endpoint = match symbol.market_type {
            MarketType::Spot => self.group_endpoint("/api/pro/v1/cash/order/hist/current")?,
            MarketType::Perpetual => {
                self.group_endpoint("/api/pro/v2/futures/order/hist/current")?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let sign_path = match symbol.market_type {
            MarketType::Spot => "order/hist/current",
            MarketType::Perpetual => "v2/futures/order/hist/current",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        params.insert(
            "n".to_string(),
            request.limit.unwrap_or(100).clamp(1, 500).to_string(),
        );
        params.insert("executedOnly".to_string(), "true".to_string());
        let value = self
            .send_signed_get("ascendex.get_recent_fills", &endpoint, sign_path, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills_from_orders(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                symbol.market_type,
                &value,
            )?,
        })
    }
}

pub(super) fn ascendex_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "ascendex.place_order.quote_quantity",
        });
    }
    if matches!(
        request.order_type,
        OrderType::StopMarket | OrderType::StopLimit
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "ascendex.place_order.stop_order",
        });
    }
    if request.reduce_only && request.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::InvalidRequest {
            message: "ascendex spot order does not support reduce_only".to_string(),
        });
    }
    if request.order_type != OrderType::Market && request.price.as_deref().is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "ascendex limit-style order requires price".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type
        )?),
    );
    body.insert("time".to_string(), json!(Utc::now().timestamp_millis()));
    body.insert("orderQty".to_string(), json!(request.quantity));
    body.insert(
        "orderType".to_string(),
        json!(match request.order_type {
            OrderType::Market => "market",
            _ => "limit",
        }),
    );
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }),
    );
    body.insert("respInst".to_string(), json!("ACCEPT"));
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert("id".to_string(), json!(client_order_id));
    }
    if let Some(price) = request.price.as_deref() {
        body.insert("orderPrice".to_string(), json!(price));
    }
    if let Some(tif) = request.time_in_force {
        body.insert(
            "timeInForce".to_string(),
            json!(match tif {
                TimeInForce::IOC => "IOC",
                TimeInForce::FOK => "FOK",
                _ => "GTC",
            }),
        );
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        body.insert("postOnly".to_string(), json!(true));
    }
    if request.reduce_only {
        body.insert("execInst".to_string(), json!("ReduceOnly"));
    }
    if request.symbol.market_type == MarketType::Perpetual {
        match request.position_side.unwrap_or(PositionSide::Net) {
            PositionSide::Long => {
                body.insert("posSide".to_string(), json!("Long"));
            }
            PositionSide::Short => {
                body.insert("posSide".to_string(), json!("Short"));
            }
            _ => {}
        }
    }
    Ok(Value::Object(body))
}

pub(super) fn ascendex_cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type
        )?),
    );
    body.insert("time".to_string(), json!(Utc::now().timestamp_millis()));
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body.insert("orderId".to_string(), json!(order_id));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert("id".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

fn batch_order_market_type(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    orders
        .first()
        .map(|order| order.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "ascendex batch_place_orders requires at least one order".to_string(),
        })
}

fn batch_cancel_market_type(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<MarketType> {
    cancels
        .first()
        .map(|cancel| cancel.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "ascendex batch_cancel_orders requires at least one cancel".to_string(),
        })
}

fn response_items(value: &Value) -> Vec<&Value> {
    let data = value.get("data").unwrap_or(value);
    data.get("info")
        .or_else(|| data.get("orders"))
        .or_else(|| data.get("data"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .or_else(|| data.as_array().map(|items| items.iter().collect()))
        .unwrap_or_default()
}
