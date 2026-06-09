use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, TimeInForce};
use serde_json::{json, Value};

use super::parser::normalize_symbol;
use super::private_parser::{
    parse_balances, parse_fills, parse_order, parse_orders, parse_positions,
};
use super::BitmartGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

impl BitmartGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitmart.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/wallet",
            MarketType::Perpetual => "/contract/private/assets-detail",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get(
                "bitmart.get_balances",
                market_type,
                endpoint,
                &HashMap::new(),
            )
            .await?;
        let mut balances = parse_balances(
            &self.exchange_id,
            market_type,
            tenant_id,
            account_id,
            &value,
        )?;
        if !request.assets.is_empty() {
            let requested = request
                .assets
                .iter()
                .map(|asset| asset.to_ascii_uppercase())
                .collect::<Vec<_>>();
            balances.retain(|balance| {
                balance
                    .balances
                    .first()
                    .is_some_and(|asset| requested.contains(&asset.asset))
            });
        }
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
                operation: "bitmart.positions_spot",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitmart.get_positions")?;
        let mut query = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            query.insert(
                "symbol".to_string(),
                normalize_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get(
                "bitmart.get_positions",
                MarketType::Perpetual,
                "/contract/private/position",
                &query,
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            positions: parse_positions(&self.exchange_id, tenant_id, account_id, &value)?,
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
        Err(ExchangeApiError::Unsupported {
            operation: "bitmart.fees_config_source_only",
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = self.place_order_body(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/spot/v2/submit_order",
            MarketType::Perpetual => "/contract/private/submit-order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitmart.place_order",
                request.symbol.market_type,
                endpoint,
                &HashMap::new(),
                body,
            )
            .await?;
        let order = order_ack_state(
            &self.exchange_id,
            request.symbol,
            request.side,
            request.order_type,
            request.quantity,
            request.price,
            request.client_order_id,
            value
                .get("data")
                .and_then(|data| data.get("order_id"))
                .and_then(Value::as_str)
                .map(str::to_string),
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
        self.ensure_supported_market(request.symbol.market_type)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitmart cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let request_id = request.context.request_id.clone();
        let market_type = request.symbol.market_type;
        let mut body = json!({
            "symbol": normalize_symbol(&request.symbol.exchange_symbol.symbol, market_type)?,
        });
        if let Some(order_id) = &request.exchange_order_id {
            body["order_id"] = json!(order_id);
        }
        if let Some(client_order_id) = &request.client_order_id {
            body["client_order_id"] = json!(client_order_id);
        }
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v3/cancel_order",
            MarketType::Perpetual => "/contract/private/cancel-order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitmart.cancel_order",
                market_type,
                endpoint,
                &HashMap::new(),
                body,
            )
            .await?;
        let fallback_order_id = request.exchange_order_id.clone();
        let fallback_client_id = request.client_order_id.clone();
        let mut order =
            parse_order(&self.exchange_id, request.symbol, &value).unwrap_or_else(|_| {
                cancelled_stub(
                    &self.exchange_id,
                    market_type,
                    fallback_order_id,
                    fallback_client_id,
                )
            });
        order.status = OrderStatus::Cancelled;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let mut query = HashMap::new();
        query.insert(
            "symbol".to_string(),
            normalize_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            query.insert("order_id".to_string(), order_id.clone());
        } else if let Some(client_order_id) = &request.client_order_id {
            query.insert("client_order_id".to_string(), client_order_id.clone());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitmart query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/spot/v2/order_detail",
            MarketType::Perpetual => "/contract/private/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get(
                "bitmart.query_order",
                request.symbol.market_type,
                endpoint,
                &query,
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: Some(parse_order(&self.exchange_id, request.symbol, &value)?),
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
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let mut query = HashMap::new();
        if let Some(symbol) = &request.symbol {
            query.insert(
                "symbol".to_string(),
                normalize_symbol(&symbol.exchange_symbol.symbol, market_type)?,
            );
        }
        if let Some(limit) = request.page.as_ref().and_then(|page| page.limit) {
            query.insert("limit".to_string(), limit.min(100).to_string());
        }
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v2/orders",
            MarketType::Perpetual => "/contract/private/get-open-orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bitmart.get_open_orders", market_type, endpoint, &query)
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
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitmart.get_recent_fills")?;
        let mut query = HashMap::new();
        if let Some(symbol) = &request.symbol {
            query.insert(
                "symbol".to_string(),
                normalize_symbol(&symbol.exchange_symbol.symbol, market_type)?,
            );
        }
        if let Some(order_id) = &request.exchange_order_id {
            query.insert("order_id".to_string(), order_id.clone());
        }
        if let Some(limit) = request.limit {
            query.insert("limit".to_string(), limit.min(100).to_string());
        }
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v2/trades",
            MarketType::Perpetual => "/contract/private/trades",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bitmart.get_recent_fills", market_type, endpoint, &query)
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

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let mut body = json!({});
        if let Some(symbol) = &request.symbol {
            body["symbol"] = json!(normalize_symbol(
                &symbol.exchange_symbol.symbol,
                market_type
            )?);
        }
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v3/cancel_orders",
            MarketType::Perpetual => "/contract/private/cancel-orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitmart.cancel_all_orders",
                market_type,
                endpoint,
                &HashMap::new(),
                body,
            )
            .await?;
        let order_ids = value
            .get("data")
            .and_then(|data| data.get("order_ids"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let orders = order_ids
            .iter()
            .map(|value| {
                cancelled_stub(
                    &self.exchange_id,
                    market_type,
                    value.as_str().map(str::to_string),
                    None,
                )
            })
            .collect::<Vec<_>>();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bitmart.batch_place_regular_orders",
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
                metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        if market_type != MarketType::Perpetual
            || request
                .cancels
                .iter()
                .any(|cancel| cancel.symbol.market_type != market_type)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmart.spot_or_mixed_batch_cancel",
            });
        }
        let body = json!({
            "symbol": normalize_symbol(&request.cancels[0].symbol.exchange_symbol.symbol, market_type)?,
            "order_ids": request
                .cancels
                .iter()
                .filter_map(|cancel| cancel.exchange_order_id.clone())
                .collect::<Vec<_>>(),
        });
        let _value = self
            .send_signed_post(
                "bitmart.batch_cancel_orders",
                market_type,
                "/contract/private/cancel-orders",
                &HashMap::new(),
                body,
            )
            .await?;
        let orders = request
            .cancels
            .iter()
            .map(|cancel| {
                cancelled_stub(
                    &self.exchange_id,
                    market_type,
                    cancel.exchange_order_id.clone(),
                    cancel.client_order_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    fn place_order_body(&self, request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
        let exchange_symbol = normalize_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?;
        let order_type = match request.order_type {
            OrderType::Market => "market",
            OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "limit",
            _ => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "bitmart.stop_order_shared_mapping",
                })
            }
        };
        let mut body = json!({
            "symbol": exchange_symbol,
            "side": match request.side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            "type": order_type,
        });
        if let Some(client_order_id) = &request.client_order_id {
            body["client_order_id"] = json!(client_order_id);
        }
        if request.symbol.market_type == MarketType::Perpetual {
            body["side"] = json!(match (request.side, request.reduce_only) {
                (OrderSide::Buy, false) => 1,
                (OrderSide::Sell, false) => 4,
                (OrderSide::Buy, true) => 2,
                (OrderSide::Sell, true) => 3,
            });
            body["mode"] = json!(1);
            body["open_type"] = json!("isolated");
            body["leverage"] = json!("1");
        }
        if request.order_type == OrderType::Market
            && request.symbol.market_type == MarketType::Spot
            && request.side == OrderSide::Buy
        {
            let quote_quantity = request.quote_quantity.as_ref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "bitmart spot market buy requires quote_quantity".to_string(),
                }
            })?;
            body["notional"] = json!(quote_quantity);
        } else {
            body["size"] = json!(request.quantity);
        }
        if let Some(price) = &request.price {
            body["price"] = json!(price);
        } else if request.order_type.requires_limit_price() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitmart limit order requires price".to_string(),
            });
        }
        if request.post_only || request.time_in_force == Some(TimeInForce::GTX) {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmart.post_only_unverified",
            });
        }
        Ok(body)
    }
}

fn order_ack_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    side: OrderSide,
    order_type: OrderType,
    quantity: String,
    price: Option<String>,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> ExchangeApiResult<rustcta_exchange_api::OrderState> {
    Ok(rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id,
        exchange_order_id,
        side,
        position_side: None,
        order_type,
        time_in_force: None,
        status: OrderStatus::Open,
        quantity,
        price,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    })
}

fn cancelled_stub(
    exchange_id: &rustcta_types::ExchangeId,
    market_type: MarketType,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> rustcta_exchange_api::OrderState {
    let exchange_symbol =
        rustcta_types::ExchangeSymbol::new(exchange_id.clone(), market_type, "UNKNOWN")
            .expect("static exchange symbol");
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: None,
        exchange_symbol,
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
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
