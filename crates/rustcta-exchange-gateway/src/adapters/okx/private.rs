use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::normalize_okx_symbol;
use super::types::{parse_balances, parse_fees, parse_fills, parse_order, parse_orders};
use super::OkxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl OkxGatewayAdapter {
    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("okx.place_order")?;
        let body = okx_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "place_order")?;
        let order = order_state_from_place_ack(&self.exchange_id, &request, ack);
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("okx.place_quote_market_order")?;
        let body = okx_quote_market_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "place_quote_market_order")?;
        let order = order_state_from_quote_ack(&self.exchange_id, &request, ack);
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("okx.cancel_order")?;
        let body = okx_cancel_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/cancel-order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "cancel_order")?;
        let order = order_state_from_cancel_ack(&self.exchange_id, &request, ack);
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_spot(request.market_type)?;
        self.ensure_private_rest("okx.cancel_all_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let pending = self
            .rest
            .send_signed_get("/api/v5/trade/orders-pending", &params)
            .await?;
        let cancel_body = okx_cancel_all_body(symbol, &pending)?;
        if cancel_body.as_array().is_some_and(Vec::is_empty) {
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
            });
        }
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/cancel-batch-orders", &cancel_body)
            .await?;
        let orders = order_states_from_cancel_all_ack(&self.exchange_id, symbol, &value)?;
        let cancelled_count = orders.len() as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn amend_order_private_rest(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("okx.amend_order")?;
        let body = okx_amend_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/amend-order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "amend_order")?;
        let order = order_state_from_amend_ack(&self.exchange_id, &request, ack);
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

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

fn okx_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol(
            &request.symbol.exchange_symbol.symbol,
        )?),
    );
    body.insert("tdMode".to_string(), Value::String("cash".to_string()));
    body.insert(
        "side".to_string(),
        Value::String(okx_side(request.side).to_string()),
    );
    body.insert(
        "ordType".to_string(),
        Value::String(
            okx_order_type(request.order_type, request.time_in_force, request.post_only)
                .to_string(),
        ),
    );
    if request.order_type == OrderType::Market {
        body.insert(
            "sz".to_string(),
            Value::String(non_empty("quantity", &request.quantity)?),
        );
        body.insert(
            "tgtCcy".to_string(),
            Value::String(okx_market_target(&request.symbol)?),
        );
    } else {
        body.insert(
            "sz".to_string(),
            Value::String(non_empty("quantity", &request.quantity)?),
        );
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx limit-style order requires price".to_string(),
            })?;
        body.insert("px".to_string(), Value::String(non_empty("price", price)?));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert(
            "clOrdId".to_string(),
            Value::String(non_empty("client_order_id", client_order_id)?),
        );
    }
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "okx spot order does not support reduce_only".to_string(),
        });
    }
    Ok(Value::Object(body))
}

fn okx_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol(
            &request.symbol.exchange_symbol.symbol,
        )?),
    );
    body.insert("tdMode".to_string(), Value::String("cash".to_string()));
    body.insert(
        "side".to_string(),
        Value::String(okx_side(request.side).to_string()),
    );
    body.insert("ordType".to_string(), Value::String("market".to_string()));
    body.insert(
        "sz".to_string(),
        Value::String(non_empty("quote_quantity", &request.quote_quantity)?),
    );
    body.insert("tgtCcy".to_string(), Value::String("quote_ccy".to_string()));
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert(
            "clOrdId".to_string(),
            Value::String(non_empty("client_order_id", client_order_id)?),
        );
    }
    Ok(Value::Object(body))
}

fn okx_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol(
            &request.symbol.exchange_symbol.symbol,
        )?),
    );
    insert_okx_order_identity(
        &mut body,
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "cancel_order",
    )?;
    Ok(Value::Object(body))
}

fn okx_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol(
            &request.symbol.exchange_symbol.symbol,
        )?),
    );
    insert_okx_order_identity(
        &mut body,
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "amend_order",
    )?;
    body.insert(
        "newSz".to_string(),
        Value::String(non_empty("new_quantity", &request.new_quantity)?),
    );
    if let Some(new_client_order_id) = request.new_client_order_id.as_deref() {
        body.insert(
            "newClOrdId".to_string(),
            Value::String(non_empty("new_client_order_id", new_client_order_id)?),
        );
    }
    Ok(Value::Object(body))
}

fn okx_cancel_all_body(
    symbol: &rustcta_exchange_api::SymbolScope,
    pending: &Value,
) -> ExchangeApiResult<Value> {
    let orders = pending
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "okx pending orders response is not an array".to_string(),
        })?;
    let mut rows = Vec::new();
    for order in orders {
        let inst_id = order
            .get("instId")
            .and_then(Value::as_str)
            .unwrap_or(symbol.exchange_symbol.symbol.as_str());
        let mut row = serde_json::Map::new();
        row.insert(
            "instId".to_string(),
            Value::String(normalize_okx_symbol(inst_id)?),
        );
        if let Some(ord_id) = value_text(order.get("ordId")) {
            row.insert("ordId".to_string(), Value::String(ord_id));
        } else if let Some(client_order_id) = value_text(order.get("clOrdId")) {
            row.insert("clOrdId".to_string(), Value::String(client_order_id));
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("okx pending order missing ordId/clOrdId: {order}"),
            });
        }
        rows.push(Value::Object(row));
    }
    Ok(Value::Array(rows))
}

fn order_states_from_cancel_all_ack(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "okx cancel batch response is not an array".to_string(),
        })?;
    Ok(items
        .iter()
        .map(|ack| order_state_from_cancel_ack_fields(exchange_id, symbol, ack))
        .collect())
}

fn okx_ack_item<'a>(
    exchange_id: &rustcta_types::ExchangeId,
    value: &'a Value,
    operation: &str,
) -> ExchangeApiResult<&'a Value> {
    let item = value
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("okx {operation} response missing ack item"),
        })?;
    let code = item.get("sCode").and_then(Value::as_str).unwrap_or("0");
    if code != "0" {
        return Err(ExchangeApiError::Exchange(
            rustcta_types::ExchangeError::new(
                exchange_id.clone(),
                rustcta_types::ExchangeErrorClass::OrderRejected,
                item.get("sMsg")
                    .and_then(Value::as_str)
                    .unwrap_or("OKX order mutation rejected"),
                Utc::now(),
            ),
        ));
    }
    Ok(item)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    ack: &Value,
) -> OrderState {
    let ord_type = okx_order_type(request.order_type, request.time_in_force, request.post_only);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId")).or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("ordId")),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: okx_time_in_force(ord_type),
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: ord_type == "post_only",
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId")).or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("ordId")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: request.quote_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    ack: &Value,
) -> OrderState {
    order_state_from_cancel_ack_fields(exchange_id, &request.symbol, ack)
}

fn order_state_from_cancel_ack_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId")),
        exchange_order_id: value_text(ack.get("ordId")),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId"))
            .or_else(|| request.new_client_order_id.clone())
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("ordId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::New,
        quantity: request.new_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn insert_okx_order_identity(
    body: &mut serde_json::Map<String, Value>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert("ordId".to_string(), Value::String(order_id.to_string()));
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert("clOrdId".to_string(), Value::String(client_id.to_string()));
    }
    if !body.contains_key("ordId") && !body.contains_key("clOrdId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("okx {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn okx_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn okx_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> &'static str {
    if post_only
        || matches!(order_type, OrderType::PostOnly)
        || matches!(tif, Some(TimeInForce::GTX))
    {
        return "post_only";
    }
    match tif {
        Some(TimeInForce::IOC) => "ioc",
        Some(TimeInForce::FOK) => "fok",
        _ => match order_type {
            OrderType::Market => "market",
            OrderType::IOC => "ioc",
            OrderType::FOK => "fok",
            _ => "limit",
        },
    }
}

fn okx_time_in_force(ord_type: &str) -> Option<TimeInForce> {
    match ord_type {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "post_only" => Some(TimeInForce::GTX),
        "limit" => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn okx_market_target(symbol: &rustcta_exchange_api::SymbolScope) -> ExchangeApiResult<String> {
    let normalized = normalize_okx_symbol(&symbol.exchange_symbol.symbol)?;
    let (base, _) = normalized
        .split_once('-')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("OKX Spot symbol missing dash: {normalized}"),
        })?;
    Ok(base.to_ascii_lowercase())
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("okx {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
