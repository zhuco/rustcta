use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_coinex_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::CoinExGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinExGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinex_order_body(&request)?;
        let value = self
            .send_signed_post("coinex.place_order", "/spot/order", &HashMap::new(), &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinex_quote_market_order_body(&request)?;
        let value = self
            .send_signed_post(
                "coinex.place_quote_market_order",
                "/spot/order",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinex_cancel_order_body(&request)?;
        let value = self
            .send_signed_delete("coinex.cancel_order", "/spot/order", &HashMap::new(), &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| coinex_cancel_order_state(&self.exchange_id, &request, &value));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let body = json!({
            "market": normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
            "market_type": "SPOT",
        });
        let value = self
            .send_signed_post(
                "coinex.cancel_all_orders",
                "/spot/cancel-all-order",
                &HashMap::new(),
                &body,
            )
            .await?;
        let orders = coinex_cancel_all_orders(&self.exchange_id, symbol, &value);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinex_amend_order_body(&request)?;
        let value = self
            .send_signed_post(
                "coinex.amend_order",
                "/spot/modify-order",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get(
                "coinex.get_balances",
                "/assets/spot/balance",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
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
                message: "coinex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "market".to_string(),
                normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("coinex.get_fees", "/spot/market", &params)
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

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("client_id".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("order_id") && !params.contains_key("client_id") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinex query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("coinex.query_order", "/spot/order-status", &params)
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "market".to_string(),
                normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("coinex.get_open_orders", "/spot/pending-order", &params)
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "start_time".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "end_time".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        } else {
            params.insert("limit".to_string(), "1000".to_string());
        }
        let value = self
            .send_signed_get("coinex.get_recent_fills", "/spot/finished-order", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn coinex_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinex spot order does not support reduce_only".to_string(),
        });
    }
    let (order_type, option) = coinex_order_type(request.order_type, request.time_in_force)?;
    let mut body = json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": "SPOT",
        "side": coinex_side(request.side),
        "type": order_type,
        "amount": non_empty("quantity", &request.quantity)?,
    });
    if let Some(option) = option {
        body["option"] = Value::String(option.to_string());
    }
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn coinex_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.quote_market_sell",
        });
    }
    let quote_asset = request
        .symbol
        .canonical_symbol
        .as_ref()
        .map(|symbol| symbol.quote_asset().to_string())
        .unwrap_or_else(|| "USDT".to_string());
    let mut body = json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": "SPOT",
        "side": "buy",
        "type": "market",
        "amount": non_empty("quote_quantity", &request.quote_quantity)?,
        "ccy": quote_asset,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn coinex_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["order_id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("order_id").is_none() && body.get("client_id").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinex cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(body)
}

fn coinex_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request
        .new_client_order_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.amend_new_client_order_id",
        });
    }
    let order_id = request
        .exchange_order_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "coinex.amend_by_client_order_id",
        })?;
    Ok(json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": "SPOT",
        "order_id": coinex_numeric_order_id(order_id)?,
        "amount": non_empty("new_quantity", &request.new_quantity)?,
    }))
}

fn coinex_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    parse_order_state(exchange_id, Some(symbol), item).unwrap_or_else(|_| {
                        coinex_cancel_order_state_from_fields(
                            exchange_id,
                            symbol,
                            value_text(item.get("order_id").or_else(|| item.get("id"))),
                            value_text(item.get("client_id")),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn coinex_cancel_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    coinex_cancel_order_state_from_fields(
        exchange_id,
        &request.symbol,
        value_text(value.get("order_id").or_else(|| value.get("id")))
            .or_else(|| request.exchange_order_id.clone()),
        value_text(value.get("client_id")).or_else(|| request.client_order_id.clone()),
    )
}

fn coinex_cancel_order_state_from_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::None),
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
        updated_at: chrono::Utc::now(),
    }
}

fn coinex_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn coinex_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
) -> ExchangeApiResult<(&'static str, Option<&'static str>)> {
    Ok(match (order_type, tif) {
        (OrderType::Market, _) => ("market", None),
        (OrderType::PostOnly, _) | (_, Some(TimeInForce::GTX)) => ("limit", Some("maker_only")),
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => ("limit", Some("ioc")),
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => ("limit", Some("fok")),
        (OrderType::Limit, _) => ("limit", Some("normal")),
        (OrderType::StopMarket | OrderType::StopLimit, _) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.stop_order",
            });
        }
    })
}

fn coinex_numeric_order_id(order_id: &str) -> ExchangeApiResult<Value> {
    let numeric_id = order_id
        .parse::<u64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: "coinex amend_order requires numeric exchange_order_id".to_string(),
        })?;
    Ok(Value::Number(numeric_id.into()))
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinex {field} must not be empty"),
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
