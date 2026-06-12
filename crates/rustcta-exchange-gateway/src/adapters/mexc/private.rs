use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot,
    FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse, OrderState, PageCursor,
    PlaceOrderRequest, PlaceOrderResponse, Position, PositionMode, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, SetLeverageRequest, SetLeverageResponse,
    SetPositionModeRequest, SetPositionModeResponse, SymbolAccountConfig,
    SymbolAccountConfigRequest, SymbolAccountConfigResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeSymbol, Fill, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{decimal_text_to_f64, normalize_mexc_symbol, normalize_mexc_symbol_for_market};
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::MexcGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl MexcGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        if request.symbol.market_type == MarketType::Perpetual {
            let body = mexc_contract_order_body(&request)?;
            let value = self
                .send_contract_signed_post(
                    "mexc.contract.place_order",
                    "/api/v1/private/order/create",
                    &HashMap::new(),
                    Some(&body),
                )
                .await?;
            let order =
                parse_contract_order_state(&self.exchange_id, Some(&request.symbol), &value)
                    .unwrap_or_else(|_| {
                        order_state_from_place_ack(&self.exchange_id, &request, &value)
                    });
            return Ok(PlaceOrderResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.symbol.exchange, request.context.request_id),
                order,
            });
        }
        let params = mexc_place_order_params(&request)?;
        let value = self
            .send_signed_post("mexc.place_order", "/api/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_place_ack(&self.exchange_id, &request, &value));
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
        let params = mexc_quote_market_order_params(&request)?;
        let value = self
            .send_signed_post("mexc.place_quote_market_order", "/api/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_quote_ack(&self.exchange_id, &request, &value));
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
        self.ensure_market_type(request.symbol.market_type)?;
        if request.symbol.market_type == MarketType::Perpetual {
            let body = mexc_contract_cancel_body(&request)?;
            let value = self
                .send_contract_signed_post(
                    "mexc.contract.cancel_order",
                    "/api/v1/private/order/cancel",
                    &HashMap::new(),
                    Some(&body),
                )
                .await?;
            let order =
                parse_contract_order_state(&self.exchange_id, Some(&request.symbol), &value)
                    .unwrap_or_else(|_| {
                        order_state_from_cancel_ack(&self.exchange_id, &request, &value)
                    });
            return Ok(CancelOrderResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.symbol.exchange, request.context.request_id),
                order,
                cancelled: true,
            });
        }
        let params = mexc_cancel_order_params(&request)?;
        let value = self
            .send_signed_delete("mexc.cancel_order", "/api/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_cancel_ack(&self.exchange_id, &request, &value));
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
        self.ensure_optional_market_type(request.market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        if let Some(market_type) = request.market_type {
            if market_type != symbol.market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "mexc cancel_all_orders symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
        }
        if symbol.market_type == MarketType::Perpetual {
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_mexc_symbol_for_market(
                    &symbol.exchange_symbol.symbol,
                    symbol.market_type,
                )?,
            );
            let value = self
                .send_contract_signed_post(
                    "mexc.contract.cancel_all_orders",
                    "/api/v1/private/order/cancel_all",
                    &params,
                    None,
                )
                .await?;
            let orders =
                parse_contract_orders(&self.exchange_id, Some(symbol), &value).unwrap_or_default();
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                cancelled_count: orders.len() as u32,
                orders,
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_delete("mexc.cancel_all_orders", "/api/v3/openOrders", &params)
            .await?;
        let orders = mexc_cancel_all_orders(&self.exchange_id, symbol, &value)?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        if market_type == MarketType::Perpetual {
            let value = self
                .send_contract_signed_get(
                    "mexc.contract.get_balances",
                    "/api/v1/private/account/assets",
                    &HashMap::new(),
                )
                .await?;
            let balances = parse_contract_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?;
            return Ok(BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                balances,
            });
        }
        let value = self
            .send_signed_get("mexc.get_balances", "/api/v3/account", &HashMap::new())
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            MarketType::Spot,
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
                message: "mexc get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_mexc_symbol_for_market(
                    &symbol.exchange_symbol.symbol,
                    symbol.market_type,
                )?,
            );
            if symbol.market_type == MarketType::Perpetual {
                let value = self
                    .send_contract_signed_get(
                        "mexc.contract.get_fees",
                        "/api/v1/private/account/tiered_fee_rate",
                        &params,
                    )
                    .await?;
                fees.extend(parse_contract_fee_snapshots(
                    &self.exchange_id,
                    symbol,
                    &value,
                )?);
            } else {
                let value = self
                    .send_signed_get("mexc.get_fees", "/api/v3/tradeFee", &params)
                    .await?;
                fees.extend(parse_fee_snapshots(
                    &self.exchange_id,
                    std::slice::from_ref(symbol),
                    &value,
                )?);
            }
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
        self.ensure_market_type(request.symbol.market_type)?;
        if request.symbol.market_type == MarketType::Perpetual {
            let value = if let Some(order_id) = request.exchange_order_id.as_deref() {
                self.send_contract_signed_get(
                    "mexc.contract.query_order",
                    &format!(
                        "/api/v1/private/order/get/{}",
                        non_empty("exchange_order_id", order_id)?
                    ),
                    &HashMap::new(),
                )
                .await?
            } else if let Some(client_order_id) = request.client_order_id.as_deref() {
                let symbol = normalize_mexc_symbol_for_market(
                    &request.symbol.exchange_symbol.symbol,
                    request.symbol.market_type,
                )?;
                self.send_contract_signed_get(
                    "mexc.contract.query_order",
                    &format!(
                        "/api/v1/private/order/external/{}/{}",
                        symbol,
                        non_empty("client_order_id", client_order_id)?
                    ),
                    &HashMap::new(),
                )
                .await?
            } else {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "mexc query_order requires exchange_order_id or client_order_id"
                        .to_string(),
                });
            };
            let order =
                parse_contract_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
            return Ok(QueryOrderResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.symbol.exchange, request.context.request_id),
                order: Some(order),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "mexc query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("mexc.query_order", "/api/v3/order", &params)
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
        self.ensure_optional_market_type(request.market_type)?;
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Spot);
        if market_type == MarketType::Perpetual {
            let symbol =
                request
                    .symbol
                    .as_ref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "mexc contract open orders requires symbol".to_string(),
                    })?;
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "mexc contract open orders requires perpetual symbol".to_string(),
                });
            }
            let page_size = request
                .page
                .as_ref()
                .and_then(|page| page.limit)
                .unwrap_or(100)
                .min(100);
            let page_num = request
                .page
                .as_ref()
                .and_then(|page| page.cursor.as_ref())
                .and_then(|cursor| match cursor {
                    PageCursor::Offset { offset } => Some((offset / u64::from(page_size)) + 1),
                    PageCursor::Token { token } => token.parse::<u64>().ok(),
                    PageCursor::Id { id } => id.parse::<u64>().ok(),
                    _ => None,
                })
                .unwrap_or(1);
            let mut params = HashMap::new();
            params.insert("page_num".to_string(), page_num.to_string());
            params.insert("page_size".to_string(), page_size.to_string());
            let value = self
                .send_contract_signed_get(
                    "mexc.contract.get_open_orders",
                    "/api/v1/private/order/list/open_orders",
                    &params,
                )
                .await?;
            let orders = parse_contract_orders(&self.exchange_id, Some(symbol), &value)?;
            return Ok(OpenOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders,
            });
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("mexc.get_open_orders", "/api/v3/openOrders", &params)
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
        self.ensure_optional_market_type(request.market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        if let Some(market_type) = request.market_type {
            if market_type != symbol.market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "mexc recent fills symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        if symbol.market_type == MarketType::Perpetual {
            let normalized = normalize_mexc_symbol_for_market(
                &symbol.exchange_symbol.symbol,
                symbol.market_type,
            )?;
            let mut params = HashMap::new();
            if let Some(order_id) = request.exchange_order_id.as_deref() {
                params.insert("order_id".to_string(), order_id.to_string());
            }
            if let Some(limit) = request.limit {
                params.insert("page_size".to_string(), limit.min(100).to_string());
            }
            let value = self
                .send_contract_signed_get(
                    "mexc.contract.get_recent_fills",
                    &format!("/api/v1/private/order/list/order_deals/{normalized}"),
                    &params,
                )
                .await?;
            let fills = parse_contract_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?;
            return Ok(RecentFillsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                fills,
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
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
            .send_signed_get("mexc.get_recent_fills", "/api/v3/myTrades", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        if market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.spot_positions",
            });
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            self.ensure_exchange(&symbol.exchange_id)?;
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "mexc contract positions require perpetual symbols".to_string(),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_mexc_symbol_for_market(&symbol.symbol, symbol.market_type)?,
            );
        }
        let value = self
            .send_contract_signed_get(
                "mexc.contract.get_positions",
                "/api/v1/private/position/open_positions",
                &params,
            )
            .await?;
        let positions = parse_contract_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            request.symbols.as_slice(),
            &value,
        )?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn set_leverage_impl(
        &self,
        request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if request.symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.set_leverage_non_perpetual",
            });
        }
        let mut params = mexc_contract_set_leverage_params(&request, 1)?;
        self.send_contract_signed_post(
            "mexc.contract.set_leverage",
            "/api/v1/private/position/change_leverage",
            &params,
            None,
        )
        .await?;
        params.insert("positionType".to_string(), "2".to_string());
        self.send_contract_signed_post(
            "mexc.contract.set_leverage",
            "/api/v1/private/position/change_leverage",
            &params,
            None,
        )
        .await?;
        Ok(SetLeverageResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            symbol: request.symbol,
            leverage: request.leverage,
            accepted: true,
            message: Some(
                "mexc contract leverage applied to long and short position types".to_string(),
            ),
        })
    }

    pub(super) async fn get_symbol_account_config_impl(
        &self,
        request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if request.symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.get_symbol_account_config_non_perpetual",
            });
        }
        let value = self
            .send_contract_signed_get(
                "mexc.contract.get_position_mode",
                "/api/v1/private/position/position_mode",
                &HashMap::new(),
            )
            .await?;
        let position_mode = parse_mexc_position_mode(&value)?;
        Ok(SymbolAccountConfigResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            config: SymbolAccountConfig {
                symbol: request.symbol,
                position_mode: Some(position_mode),
                margin_mode: None,
                leverage: None,
                max_leverage: None,
                updated_at: chrono::Utc::now(),
            },
        })
    }

    pub(super) async fn set_position_mode_impl(
        &self,
        request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let mut params = HashMap::new();
        params.insert(
            "positionMode".to_string(),
            mexc_position_mode_code(request.mode).to_string(),
        );
        self.send_contract_signed_post(
            "mexc.contract.set_position_mode",
            "/api/v1/private/position/change_position_mode",
            &params,
            None,
        )
        .await?;
        let readback = self
            .send_contract_signed_get(
                "mexc.contract.get_position_mode_after_set",
                "/api/v1/private/position/position_mode",
                &HashMap::new(),
            )
            .await?;
        let confirmed_mode = parse_mexc_position_mode(&readback)?;
        if confirmed_mode != request.mode {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "mexc position mode readback mismatch after set: requested {:?}, got {:?}",
                    request.mode, confirmed_mode
                ),
            });
        }
        Ok(SetPositionModeResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            mode: confirmed_mode,
            accepted: true,
            message: Some(
                "mexc position mode change requires no active orders, plan orders, or unfinished positions"
                    .to_string(),
            ),
        })
    }
}

fn mexc_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "mexc spot order does not support reduce_only".to_string(),
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert("side".to_string(), mexc_side(request.side).to_string());
    params.insert(
        "type".to_string(),
        mexc_order_type(request.order_type, request.time_in_force)?.to_string(),
    );
    params.insert(
        "quantity".to_string(),
        non_empty("quantity", &request.quantity)?,
    );
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc limit-style order requires price".to_string(),
            })?;
        params.insert("price".to_string(), non_empty("price", price)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "newClientOrderId".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    if let Some(time_in_force) = request.time_in_force {
        params.insert(
            "timeInForce".to_string(),
            mexc_time_in_force(time_in_force).to_string(),
        );
    }
    Ok(params)
}

fn mexc_quote_market_order_params(
    request: &QuoteMarketOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "mexc.quote_market_sell",
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert("side".to_string(), "BUY".to_string());
    params.insert("type".to_string(), "MARKET".to_string());
    params.insert(
        "quoteOrderQty".to_string(),
        non_empty("quote_quantity", &request.quote_quantity)?,
    );
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "newClientOrderId".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    Ok(params)
}

fn mexc_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert(
            "orderId".to_string(),
            non_empty("exchange_order_id", order_id)?,
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "origClientOrderId".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "mexc cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn mexc_contract_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        Value::String(normalize_mexc_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?),
    );
    body.insert(
        "price".to_string(),
        Value::String(request.price.clone().unwrap_or_else(|| "0".to_string())),
    );
    body.insert(
        "vol".to_string(),
        Value::String(non_empty("quantity", &request.quantity)?),
    );
    ensure_mexc_contract_position_side_hint(request)?;
    body.insert(
        "side".to_string(),
        Value::Number(mexc_contract_side(request.side, request.reduce_only).into()),
    );
    body.insert(
        "type".to_string(),
        Value::Number(mexc_contract_order_type(request.order_type, request.time_in_force)?.into()),
    );
    body.insert("openType".to_string(), Value::Number(2.into()));
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert(
            "externalOid".to_string(),
            Value::String(non_empty("client_order_id", client_order_id)?),
        );
    }
    Ok(Value::Object(body))
}

fn mexc_contract_cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body.insert(
            "orderId".to_string(),
            Value::String(non_empty("exchange_order_id", order_id)?),
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert(
            "externalOid".to_string(),
            Value::String(non_empty("client_order_id", client_order_id)?),
        );
        body.insert(
            "symbol".to_string(),
            Value::String(normalize_mexc_symbol_for_market(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?),
        );
    }
    if !body.contains_key("orderId") && !body.contains_key("externalOid") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "mexc contract cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(Value::Object(body))
}

fn mexc_contract_set_leverage_params(
    request: &SetLeverageRequest,
    position_type: u32,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.leverage == 0 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "mexc set_leverage requires leverage greater than zero".to_string(),
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    params.insert("leverage".to_string(), request.leverage.to_string());
    params.insert("openType".to_string(), "2".to_string());
    params.insert("positionType".to_string(), position_type.to_string());
    Ok(params)
}

fn mexc_position_mode_code(mode: PositionMode) -> i64 {
    match mode {
        PositionMode::Hedge => 1,
        PositionMode::OneWay => 2,
    }
}

fn parse_mexc_position_mode(value: &Value) -> ExchangeApiResult<PositionMode> {
    let code = value
        .get("data")
        .and_then(|data| {
            data.get("positionMode")
                .or_else(|| data.get("position_mode"))
                .or(Some(data))
        })
        .or_else(|| value.get("positionMode"))
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("mexc position_mode response missing positionMode: {value}"),
        })?;
    match code {
        1 => Ok(PositionMode::Hedge),
        2 => Ok(PositionMode::OneWay),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("mexc unsupported positionMode {code}"),
        }),
    }
}

fn mexc_contract_side(side: OrderSide, reduce_only: bool) -> i64 {
    match (side, reduce_only) {
        (OrderSide::Buy, false) => 1,
        (OrderSide::Buy, true) => 2,
        (OrderSide::Sell, false) => 3,
        (OrderSide::Sell, true) => 4,
    }
}

fn ensure_mexc_contract_position_side_hint(request: &PlaceOrderRequest) -> ExchangeApiResult<()> {
    let Some(position_side) = request.position_side else {
        return Ok(());
    };
    let expected = match (request.side, request.reduce_only) {
        (OrderSide::Buy, false) | (OrderSide::Sell, true) => PositionSide::Long,
        (OrderSide::Sell, false) | (OrderSide::Buy, true) => PositionSide::Short,
    };
    if matches!(position_side, PositionSide::None | PositionSide::Net) || position_side == expected
    {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!(
            "mexc contract order position_side {position_side:?} conflicts with side {:?} reduce_only={}; expected {expected:?}",
            request.side, request.reduce_only
        ),
    })
}

fn mexc_contract_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
) -> ExchangeApiResult<i64> {
    Ok(match (order_type, tif) {
        (OrderType::Market, _) => 5,
        (OrderType::PostOnly, _) | (_, Some(TimeInForce::GTX)) => 2,
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => 3,
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => 4,
        (OrderType::Limit, _) => 1,
        (OrderType::StopMarket | OrderType::StopLimit, _) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.contract.stop_order",
            });
        }
    })
}

fn mexc_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc cancel-all response is not an array".to_string(),
        })?;
    let mut orders = Vec::new();
    for item in items {
        if let Some(reports) = item.get("orderReports").and_then(Value::as_array) {
            for report in reports {
                orders.push(
                    parse_order_state(exchange_id, Some(symbol), report).unwrap_or_else(|_| {
                        order_state_from_cancel_fields(
                            exchange_id,
                            symbol,
                            value_text(report.get("orderId")),
                            value_text(report.get("clientOrderId")),
                        )
                    }),
                );
            }
        } else {
            orders.push(
                parse_order_state(exchange_id, Some(symbol), item).unwrap_or_else(|_| {
                    order_state_from_cancel_fields(
                        exchange_id,
                        symbol,
                        value_text(item.get("orderId")),
                        value_text(item.get("clientOrderId")),
                    )
                }),
            );
        }
    }
    Ok(orders)
}

fn parse_contract_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol_hint: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let item = contract_data_item(value).unwrap_or(value);
    let symbol = symbol_hint
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc contract order parser requires symbol hint".to_string(),
        })?;
    let side_code = value_text(item.get("side")).and_then(|value| value.parse::<i64>().ok());
    let order_type_code = value_text(item.get("type")).and_then(|value| value.parse::<i64>().ok());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: value_text(
            item.get("externalOid")
                .or_else(|| item.get("clientOrderId")),
        ),
        exchange_order_id: value_text(item.get("orderId").or_else(|| item.get("id"))),
        side: side_code
            .map(mexc_contract_side_to_order_side)
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::Net),
        order_type: order_type_code
            .map(mexc_contract_type_to_order_type)
            .unwrap_or(OrderType::Limit),
        time_in_force: order_type_code.and_then(mexc_contract_type_to_tif),
        status: item
            .get("state")
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
            .map(mexc_contract_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: value_text(item.get("vol").or_else(|| item.get("quantity")))
            .unwrap_or_else(|| "0".to_string()),
        price: value_text(item.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: value_text(
            item.get("dealVol")
                .or_else(|| item.get("deal_vol"))
                .or_else(|| item.get("filledQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: value_text(item.get("dealAvgPrice").or_else(|| item.get("avgPrice")))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: side_code.is_some_and(|side| matches!(side, 2 | 4)),
        post_only: order_type_code == Some(2),
        created_at: item
            .get("createTime")
            .or_else(|| item.get("create_time"))
            .or_else(|| item.get("ts"))
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis),
        updated_at: item
            .get("updateTime")
            .or_else(|| item.get("update_time"))
            .or_else(|| item.get("ts"))
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
            .unwrap_or_else(chrono::Utc::now),
    })
}

fn parse_contract_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol_hint: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = contract_data_array(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "mexc contract orders response is not an array".to_string(),
    })?;
    items
        .iter()
        .filter(|item| contract_order_matches_symbol(item, symbol_hint))
        .map(|item| parse_contract_order_state(exchange_id, symbol_hint, item))
        .collect()
}

fn contract_order_matches_symbol(
    item: &Value,
    symbol_hint: Option<&rustcta_exchange_api::SymbolScope>,
) -> bool {
    let Some(symbol_hint) = symbol_hint else {
        return true;
    };
    let item = contract_data_item(item).unwrap_or(item);
    let Some(raw_symbol) = value_text(item.get("symbol")) else {
        return true;
    };
    normalize_mexc_symbol_for_market(&raw_symbol, MarketType::Perpetual)
        .is_ok_and(|normalized| normalized == symbol_hint.exchange_symbol.symbol)
}

fn parse_contract_balances(
    exchange_id: &rustcta_types::ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::Balance>> {
    let items = contract_data_array(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "mexc contract balances response is not an array".to_string(),
    })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = value_text(
            item.get("currency")
                .or_else(|| item.get("asset"))
                .or_else(|| item.get("coin")),
        )
        .unwrap_or_else(|| "USDT".to_string())
        .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_text_to_f64(
            &value_text(
                item.get("availableBalance")
                    .or_else(|| item.get("available_balance"))
                    .or_else(|| item.get("available")),
            )
            .unwrap_or_else(|| "0".to_string()),
        )?;
        let total = decimal_text_to_f64(
            &value_text(
                item.get("equity")
                    .or_else(|| item.get("balance"))
                    .or_else(|| item.get("cashBalance")),
            )
            .unwrap_or_else(|| available.to_string()),
        )?;
        let locked = (total - available).max(0.0);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(|error| {
                    ExchangeApiError::InvalidRequest {
                        message: error.to_string(),
                    }
                })?,
            );
        }
    }
    Ok(vec![rustcta_exchange_api::Balance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances,
        observed_at: chrono::Utc::now(),
    }])
}

fn parse_contract_fee_snapshots(
    _exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let item = contract_data_item(value).unwrap_or(value);
    let maker = value_text(
        item.get("makerFeeRate")
            .or_else(|| item.get("maker_fee_rate"))
            .or_else(|| item.get("maker")),
    )
    .unwrap_or_else(|| "0".to_string());
    let taker = value_text(
        item.get("takerFeeRate")
            .or_else(|| item.get("taker_fee_rate"))
            .or_else(|| item.get("taker")),
    )
    .unwrap_or_else(|| "0".to_string());
    Ok(vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol.clone(),
        maker_rate: maker,
        taker_rate: taker,
        source: Some("mexc.contract.tiered_fee_rate".to_string()),
        updated_at: chrono::Utc::now(),
    }])
}

fn parse_contract_recent_fills(
    exchange_id: &rustcta_types::ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc contract fills require canonical symbol".to_string(),
            })?;
    let items = contract_data_array(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "mexc contract fills response is not an array".to_string(),
    })?;
    items
        .iter()
        .map(|item| {
            let side_code =
                value_text(item.get("side")).and_then(|value| value.parse::<i64>().ok());
            let price = decimal_text_to_f64(
                &value_text(item.get("price").or_else(|| item.get("dealPrice")))
                    .unwrap_or_else(|| "0".to_string()),
            )?;
            let quantity = decimal_text_to_f64(
                &value_text(item.get("vol").or_else(|| item.get("quantity")))
                    .unwrap_or_else(|| "0".to_string()),
            )?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_text(item.get("orderId")),
                client_order_id: value_text(item.get("externalOid")),
                fill_id: value_text(item.get("id").or_else(|| item.get("tradeId"))),
                side: side_code
                    .map(mexc_contract_side_to_order_side)
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::Net,
                status: rustcta_types::FillStatus::Confirmed,
                liquidity_role: rustcta_types::LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: value_text(item.get("feeCurrency").or_else(|| item.get("feeCcy"))),
                fee_amount: value_text(item.get("fee")).and_then(|value| value.parse().ok()),
                fee_rate: None,
                realized_pnl: value_text(item.get("profit")).and_then(|value| value.parse().ok()),
                filled_at: item
                    .get("createTime")
                    .or_else(|| item.get("ts"))
                    .and_then(value_as_i64)
                    .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
                    .unwrap_or_else(chrono::Utc::now),
                received_at: chrono::Utc::now(),
            })
        })
        .collect()
}

fn parse_contract_positions(
    exchange_id: &rustcta_types::ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    symbol_filters: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<Position>> {
    let items = contract_data_array(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "mexc contract positions response is not an array".to_string(),
    })?;
    let filters = symbol_filters
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for item in items {
        let raw_symbol = value_text(item.get("symbol")).unwrap_or_default();
        if !filters.is_empty() && !filters.contains(&raw_symbol.to_ascii_uppercase()) {
            continue;
        }
        let (base, quote) =
            split_contract_symbol(&raw_symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("mexc contract position symbol missing base/quote: {raw_symbol}"),
            })?;
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
        let quantity = decimal_text_to_f64(
            &value_text(item.get("holdVol").or_else(|| item.get("positionVol")))
                .unwrap_or_else(|| "0".to_string()),
        )?;
        if quantity <= 0.0 {
            continue;
        }
        positions.push(Position {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, raw_symbol)
                    .map_err(|error| ExchangeApiError::InvalidRequest {
                        message: error.to_string(),
                    })?,
            ),
            side: mexc_contract_position_side(item),
            quantity,
            entry_price: value_text(
                item.get("holdAvgPrice")
                    .or_else(|| item.get("openAvgPrice")),
            )
            .and_then(|value| value.parse().ok()),
            mark_price: value_text(item.get("markPrice")).and_then(|value| value.parse().ok()),
            liquidation_price: value_text(item.get("liquidatePrice"))
                .and_then(|value| value.parse().ok()),
            unrealized_pnl: value_text(item.get("unrealised")).and_then(|value| value.parse().ok()),
            leverage: value_text(item.get("leverage")).and_then(|value| value.parse().ok()),
            observed_at: chrono::Utc::now(),
        });
    }
    Ok(positions)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let item = contract_data_item(value).unwrap_or(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(
            item.get("externalOid")
                .or_else(|| item.get("clientOrderId")),
        )
        .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(item.get("orderId").or_else(|| item.get("id"))),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: chrono::Utc::now().into(),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: value_text(value.get("origQty"))
            .unwrap_or_else(|| request.quote_quantity.clone()),
        price: None,
        filled_quantity: value_text(value.get("executedQty")).unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: chrono::Utc::now().into(),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    order_state_from_cancel_fields(
        exchange_id,
        &request.symbol,
        value_text(value.get("orderId")).or_else(|| request.exchange_order_id.clone()),
        value_text(value.get("clientOrderId")).or_else(|| request.client_order_id.clone()),
    )
}

fn order_state_from_cancel_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
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
        updated_at: chrono::Utc::now(),
    }
}

fn mexc_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn mexc_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
) -> ExchangeApiResult<&'static str> {
    Ok(match (order_type, tif) {
        (OrderType::PostOnly, _) | (_, Some(TimeInForce::GTX)) => "LIMIT_MAKER",
        (OrderType::Market, _) => "MARKET",
        (OrderType::Limit | OrderType::IOC | OrderType::FOK, _) => "LIMIT",
        (OrderType::StopMarket | OrderType::StopLimit, _) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.stop_order",
            });
        }
    })
}

fn mexc_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn contract_data_item(value: &Value) -> Option<&Value> {
    value.get("data").and_then(|data| {
        data.as_array()
            .and_then(|items| items.first())
            .or(Some(data))
    })
}

fn contract_data_array(value: &Value) -> Option<&[Value]> {
    value
        .get("data")
        .and_then(|data| {
            data.as_array()
                .or_else(|| data.get("resultList").and_then(Value::as_array))
                .or_else(|| data.get("orders").and_then(Value::as_array))
                .or_else(|| data.get("list").and_then(Value::as_array))
        })
        .or_else(|| value.as_array())
        .map(Vec::as_slice)
}

fn mexc_contract_side_to_order_side(side: i64) -> OrderSide {
    match side {
        3 | 4 => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn mexc_contract_type_to_order_type(order_type: i64) -> OrderType {
    match order_type {
        2 => OrderType::PostOnly,
        3 => OrderType::IOC,
        4 => OrderType::FOK,
        5 => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn mexc_contract_type_to_tif(order_type: i64) -> Option<TimeInForce> {
    match order_type {
        2 => Some(TimeInForce::GTX),
        3 => Some(TimeInForce::IOC),
        4 => Some(TimeInForce::FOK),
        1 => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn mexc_contract_status(state: i64) -> OrderStatus {
    match state {
        1 => OrderStatus::New,
        2 => OrderStatus::Filled,
        3 => OrderStatus::PartiallyFilled,
        4 => OrderStatus::Cancelled,
        5 => OrderStatus::PartiallyFilled,
        _ => OrderStatus::Unknown,
    }
}

fn mexc_contract_position_side(item: &Value) -> PositionSide {
    match value_text(
        item.get("positionType")
            .or_else(|| item.get("position_type")),
    )
    .as_deref()
    .unwrap_or_default()
    {
        "1" | "LONG" | "long" => PositionSide::Long,
        "2" | "SHORT" | "short" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn split_contract_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol.split('_');
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts.next()?.to_ascii_uppercase();
    (!base.is_empty() && !quote.is_empty()).then_some((base, quote))
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().ok().is_some_and(|value| value == 0.0)
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("mexc {field} must not be empty"),
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
