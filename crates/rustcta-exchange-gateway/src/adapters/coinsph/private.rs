use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};

use super::parser::{ensure_php_symbol, normalize_coinsph_symbol};
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::CoinsPhGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinsPhGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        ensure_php_symbol(&request.symbol, "coinsph.non_php_market")?;
        let mut params = coinsph_place_order_params(&request)?;
        params.insert(
            "symbol".to_string(),
            normalize_coinsph_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        // Coins.ph accepts Binance-like order fields, but the host, path and
        // X-COINS-APIKEY header are local to Coins.ph and intentionally not
        // shared with the Binance adapter.
        let value = self
            .send_signed_post("coinsph.place_order", "/openapi/v1/order", &params)
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
        ensure_php_symbol(&request.symbol, "coinsph.non_php_market")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_coinsph_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "cancel_order",
        )?;
        let value = self
            .send_signed_delete("coinsph.cancel_order", "/openapi/v1/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
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
                "coinsph.get_balances",
                "/openapi/v1/account",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_account_balances(
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
                message: "Coins.ph get_fees requires at least one PHP symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            ensure_php_symbol(symbol, "coinsph.non_php_market")?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_coinsph_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("coinsph.get_fees", "/openapi/v1/asset/tradeFee", &params)
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
        ensure_php_symbol(&request.symbol, "coinsph.non_php_market")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_coinsph_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "Coins.ph query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("coinsph.query_order", "/openapi/v1/order", &params)
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
            ensure_php_symbol(symbol, "coinsph.non_php_market")?;
            params.insert(
                "symbol".to_string(),
                normalize_coinsph_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("coinsph.get_open_orders", "/openapi/v1/openOrders", &params)
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
                message: "Coins.ph get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        ensure_php_symbol(symbol, "coinsph.non_php_market")?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_coinsph_symbol(&symbol.exchange_symbol.symbol)?,
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
            .send_signed_get("coinsph.get_recent_fills", "/openapi/v1/myTrades", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn coinsph_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("side".to_string(), coinsph_side(request.side).to_string());
    params.insert(
        "type".to_string(),
        coinsph_order_type(request.order_type, request.post_only).to_string(),
    );
    if request.order_type == OrderType::Market {
        if request.quote_quantity.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinsph.place_quote_market_order",
            });
        }
        insert_non_empty(&mut params, "quantity", &request.quantity)?;
    } else {
        insert_non_empty(&mut params, "quantity", &request.quantity)?;
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Coins.ph limit-style order requires price".to_string(),
            })?;
        insert_non_empty(&mut params, "price", price)?;
        params.insert(
            "timeInForce".to_string(),
            coinsph_time_in_force(request.order_type, request.time_in_force).to_string(),
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
    }
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Coins.ph spot order does not support reduce_only".to_string(),
        });
    }
    Ok(params)
}

fn insert_order_identifier(
    params: &mut HashMap<String, String>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("origClientOrderId".to_string(), client_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("Coins.ph {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn insert_non_empty(
    params: &mut HashMap<String, String>,
    key: &str,
    value: &str,
) -> ExchangeApiResult<()> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("Coins.ph parameter {key} must not be empty"),
        });
    }
    params.insert(key.to_string(), value.to_string());
    Ok(())
}

fn coinsph_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn coinsph_order_type(order_type: OrderType, post_only: bool) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit if post_only => "LIMIT_MAKER",
        OrderType::Limit => "LIMIT",
        OrderType::PostOnly => "LIMIT_MAKER",
        OrderType::IOC | OrderType::FOK => "LIMIT",
        _ => "LIMIT",
    }
}

fn coinsph_time_in_force(order_type: OrderType, tif: Option<TimeInForce>) -> &'static str {
    if order_type == OrderType::IOC {
        return "IOC";
    }
    if order_type == OrderType::FOK {
        return "FOK";
    }
    tif.map(coinsph_time_in_force_from_tif).unwrap_or("GTC")
}

fn coinsph_time_in_force_from_tif(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTC",
    }
}
