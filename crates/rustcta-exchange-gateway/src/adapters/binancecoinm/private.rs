use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, OrderListConditionalLeg, OrderListLegType, OrderListOrderLeg,
    OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::normalize_binancecoinm_symbol;
use super::private_parser::{
    parse_account_balances, parse_binancecoinm_cancel_all_orders, parse_fee_snapshots,
    parse_open_orders, parse_order_state, parse_positions, parse_recent_fills,
};
use super::BinanceCoinMGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BinanceCoinMGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = binancecoinm_place_order_params(&request)?;
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_post("binancecoinm.place_order", "/dapi/v1/order", &params)
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
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "side".to_string(),
            binancecoinm_side(request.side).to_string(),
        );
        params.insert("type".to_string(), "MARKET".to_string());
        insert_non_empty(&mut params, "quoteOrderQty", &request.quote_quantity)?;
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
        }
        let value = self
            .send_signed_post(
                "binancecoinm.place_quote_market_order",
                "/dapi/v1/order",
                &params,
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
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "cancel_order",
        )?;
        let value = self
            .send_signed_delete("binancecoinm.cancel_order", "/dapi/v1/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
            self.ensure_coin_m(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_coin_m(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_delete(
                "binancecoinm.cancel_all_orders",
                "/dapi/v1/allOpenOrders",
                &params,
            )
            .await?;
        let orders = parse_binancecoinm_cancel_all_orders(&self.exchange_id, symbol, &value)?;
        let cancelled_count = orders.len() as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "amend_order",
        )?;
        insert_non_empty(&mut params, "quantity", &request.new_quantity)?;
        if let Some(client_order_id) = request.new_client_order_id.as_deref() {
            insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
        }
        let value = self
            .send_signed_put("binancecoinm.amend_order", "/dapi/v1/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        let symbol = request.symbol().clone();
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_coin_m(symbol.market_type)?;
        let (schema_version, context_request_id, endpoint, params, kind) =
            binancecoinm_order_list_params(&request)?;
        ensure_exchange_api_schema(schema_version)?;
        let value = self
            .send_signed_post("binancecoinm.place_order_list", endpoint, &params)
            .await?;
        let response = super::private_parser::parse_order_list_response(
            &self.exchange_id,
            &symbol,
            kind,
            &value,
        )?;
        Ok(OrderListResponse {
            metadata: response_metadata(symbol.exchange, context_request_id),
            ..response
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_coin_m(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get(
                "binancecoinm.get_balances",
                "/dapi/v1/balance",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            MarketType::Perpetual,
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
        if let Some(market_type) = request.market_type {
            self.ensure_coin_m(market_type)?;
        }
        for symbol in &request.symbols {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "binancecoinm adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if request.symbols.len() == 1 {
            params.insert(
                "pair".to_string(),
                normalize_binancecoinm_pair(&request.symbols[0].symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "binancecoinm.get_positions",
                "/dapi/v1/positionRisk",
                &params,
            )
            .await?;
        let positions = parse_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.symbols,
            &value,
        )?;
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
                message: "binancecoinm get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_coin_m(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get(
                    "binancecoinm.get_fees",
                    "/dapi/v1/account/commission",
                    &params,
                )
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
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "binancecoinm query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("binancecoinm.query_order", "/dapi/v1/order", &params)
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
            self.ensure_coin_m(market_type)?;
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_coin_m(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "binancecoinm.get_open_orders",
                "/dapi/v1/openOrders",
                &params,
            )
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
            self.ensure_coin_m(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_coin_m(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
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
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        } else {
            params.insert("limit".to_string(), "1000".to_string());
        }
        let value = self
            .send_signed_get(
                "binancecoinm.get_recent_fills",
                "/dapi/v1/myTrades",
                &params,
            )
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn binancecoinm_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "side".to_string(),
        binancecoinm_side(request.side).to_string(),
    );
    params.insert(
        "type".to_string(),
        binancecoinm_order_type(request.order_type, request.post_only).to_string(),
    );
    if request.order_type == OrderType::Market {
        if let Some(quote_quantity) = request.quote_quantity.as_deref() {
            insert_non_empty(&mut params, "quoteOrderQty", quote_quantity)?;
        } else {
            insert_non_empty(&mut params, "quantity", &request.quantity)?;
        }
    } else {
        insert_non_empty(&mut params, "quantity", &request.quantity)?;
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm limit-style order requires price".to_string(),
            })?;
        insert_non_empty(&mut params, "price", price)?;
        let time_in_force = if request.post_only {
            "GTX"
        } else {
            binancecoinm_time_in_force(request.order_type, request.time_in_force)
        };
        params.insert("timeInForce".to_string(), time_in_force.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
    }
    if request.reduce_only {
        params.insert("reduceOnly".to_string(), "true".to_string());
    }
    if let Some(position_side) = request.position_side {
        params.insert(
            "positionSide".to_string(),
            binancecoinm_position_side(position_side).to_string(),
        );
    }
    Ok(params)
}

fn binancecoinm_order_list_params(
    request: &OrderListRequest,
) -> ExchangeApiResult<(
    u16,
    Option<String>,
    &'static str,
    HashMap<String, String>,
    rustcta_exchange_api::OrderListKind,
)> {
    match request {
        OrderListRequest::Oco {
            schema_version,
            context,
            symbol,
            list_client_order_id,
            side,
            quantity,
            above,
            below,
        } => {
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
            params.insert("side".to_string(), binancecoinm_side(*side).to_string());
            insert_non_empty(&mut params, "quantity", quantity)?;
            if let Some(client_id) = list_client_order_id.as_deref() {
                insert_non_empty(&mut params, "listClientOrderId", client_id)?;
            }
            insert_conditional_leg(&mut params, "above", above)?;
            insert_conditional_leg(&mut params, "below", below)?;
            Ok((
                *schema_version,
                context.request_id.clone(),
                "/dapi/v1/orderList/oco",
                params,
                rustcta_exchange_api::OrderListKind::Oco,
            ))
        }
        OrderListRequest::Oto {
            schema_version,
            context,
            symbol,
            list_client_order_id,
            working,
            pending,
        } => {
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
            if let Some(client_id) = list_client_order_id.as_deref() {
                insert_non_empty(&mut params, "listClientOrderId", client_id)?;
            }
            insert_order_leg(&mut params, "working", working)?;
            insert_order_leg(&mut params, "pending", pending)?;
            Ok((
                *schema_version,
                context.request_id.clone(),
                "/dapi/v1/orderList/oto",
                params,
                rustcta_exchange_api::OrderListKind::Oto,
            ))
        }
    }
}

fn insert_conditional_leg(
    params: &mut HashMap<String, String>,
    prefix: &str,
    leg: &OrderListConditionalLeg,
) -> ExchangeApiResult<()> {
    params.insert(
        format!("{prefix}Type"),
        binancecoinm_order_list_type(leg.order_type).to_string(),
    );
    if let Some(price) = leg.price.as_deref() {
        insert_non_empty(params, &format!("{prefix}Price"), price)?;
    }
    if let Some(stop_price) = leg.stop_price.as_deref() {
        insert_non_empty(params, &format!("{prefix}StopPrice"), stop_price)?;
    }
    if let Some(time_in_force) = leg.time_in_force {
        params.insert(
            format!("{prefix}TimeInForce"),
            binancecoinm_time_in_force_from_tif(time_in_force).to_string(),
        );
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        insert_non_empty(params, &format!("{prefix}ClientOrderId"), client_order_id)?;
    }
    Ok(())
}

fn insert_order_leg(
    params: &mut HashMap<String, String>,
    prefix: &str,
    leg: &OrderListOrderLeg,
) -> ExchangeApiResult<()> {
    params.insert(
        format!("{prefix}Side"),
        binancecoinm_side(leg.side).to_string(),
    );
    params.insert(
        format!("{prefix}Type"),
        binancecoinm_order_list_type(leg.order_type).to_string(),
    );
    insert_non_empty(params, &format!("{prefix}Quantity"), &leg.quantity)?;
    if let Some(price) = leg.price.as_deref() {
        insert_non_empty(params, &format!("{prefix}Price"), price)?;
    }
    if let Some(stop_price) = leg.stop_price.as_deref() {
        insert_non_empty(params, &format!("{prefix}StopPrice"), stop_price)?;
    }
    if let Some(time_in_force) = leg.time_in_force {
        params.insert(
            format!("{prefix}TimeInForce"),
            binancecoinm_time_in_force_from_tif(time_in_force).to_string(),
        );
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        insert_non_empty(params, &format!("{prefix}ClientOrderId"), client_order_id)?;
    }
    Ok(())
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
            message: format!(
                "binancecoinm {operation} requires exchange_order_id or client_order_id"
            ),
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
            message: format!("binancecoinm parameter {key} must not be empty"),
        });
    }
    params.insert(key.to_string(), value.to_string());
    Ok(())
}

fn binancecoinm_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn binancecoinm_order_type(order_type: OrderType, post_only: bool) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit if post_only => "LIMIT",
        OrderType::Limit => "LIMIT",
        OrderType::PostOnly => "LIMIT",
        OrderType::IOC | OrderType::FOK => "LIMIT",
        _ => "LIMIT",
    }
}

fn binancecoinm_time_in_force(order_type: OrderType, tif: Option<TimeInForce>) -> &'static str {
    if order_type == OrderType::PostOnly {
        return "GTX";
    }
    if order_type == OrderType::IOC {
        return "IOC";
    }
    if order_type == OrderType::FOK {
        return "FOK";
    }
    tif.map(binancecoinm_time_in_force_from_tif)
        .unwrap_or("GTC")
}

fn binancecoinm_time_in_force_from_tif(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn binancecoinm_position_side(side: rustcta_types::PositionSide) -> &'static str {
    match side {
        rustcta_types::PositionSide::Long => "LONG",
        rustcta_types::PositionSide::Short => "SHORT",
        rustcta_types::PositionSide::Net | rustcta_types::PositionSide::None => "BOTH",
    }
}

fn normalize_binancecoinm_pair(symbol: &str) -> ExchangeApiResult<String> {
    let symbol = normalize_binancecoinm_symbol(symbol)?;
    if let Some((pair, _delivery)) = symbol.split_once('_') {
        Ok(pair.to_string())
    } else {
        Ok(symbol.trim_end_matches("PERP").to_string())
    }
}

fn binancecoinm_order_list_type(order_type: OrderListLegType) -> &'static str {
    match order_type {
        OrderListLegType::Market => "MARKET",
        OrderListLegType::Limit => "LIMIT",
        OrderListLegType::LimitMaker => "LIMIT_MAKER",
        OrderListLegType::StopLoss => "STOP_LOSS",
        OrderListLegType::StopLossLimit => "STOP_LOSS_LIMIT",
        OrderListLegType::TakeProfit => "TAKE_PROFIT",
        OrderListLegType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
    }
}
