#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce};
use serde_json::{json, Value};

use super::parser::{data_payload, normalize_blofin_symbol};
use super::private_parser::{
    ack_order, parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state,
    parse_orders, parse_positions,
};
use super::public::BlofinMarginMode;
use super::BlofinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlofinPositionMode {
    Net,
    LongShort,
}

impl BlofinPositionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Net => "net_mode",
            Self::LongShort => "long_short_mode",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlofinQueryParams<'a> {
    pub inst_id: Option<&'a str>,
    pub order_id: Option<&'a str>,
    pub tpsl_id: Option<&'a str>,
    pub algo_id: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
    pub order_type: Option<&'a str>,
    pub state: Option<&'a str>,
    pub currency: Option<&'a str>,
    pub from_account: Option<&'a str>,
    pub to_account: Option<&'a str>,
    pub withdraw_id: Option<&'a str>,
    pub deposit_id: Option<&'a str>,
    pub tx_id: Option<&'a str>,
    pub before: Option<&'a str>,
    pub after: Option<&'a str>,
    pub begin: Option<&'a str>,
    pub end: Option<&'a str>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct BlofinTransferRequest<'a> {
    pub currency: &'a str,
    pub amount: &'a str,
    pub from_account: &'a str,
    pub to_account: &'a str,
    pub client_id: Option<&'a str>,
    pub sub_account: Option<&'a str>,
    pub main_to_sub_account: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct BlofinSetLeverageRequest<'a> {
    pub inst_id: &'a str,
    pub leverage: &'a str,
    pub margin_mode: BlofinMarginMode,
    pub position_side: Option<PositionSide>,
}

#[derive(Debug, Clone)]
pub struct BlofinTpslOrderRequest<'a> {
    pub inst_id: &'a str,
    pub margin_mode: BlofinMarginMode,
    pub position_side: PositionSide,
    pub side: OrderSide,
    pub size: &'a str,
    pub reduce_only: Option<bool>,
    pub client_order_id: Option<&'a str>,
    pub broker_id: Option<&'a str>,
    pub tp_trigger_price: Option<&'a str>,
    pub tp_order_price: Option<&'a str>,
    pub tp_trigger_price_type: Option<&'a str>,
    pub sl_trigger_price: Option<&'a str>,
    pub sl_order_price: Option<&'a str>,
    pub sl_trigger_price_type: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct BlofinAttachedAlgoOrder<'a> {
    pub tp_trigger_price: Option<&'a str>,
    pub tp_order_price: Option<&'a str>,
    pub tp_trigger_price_type: Option<&'a str>,
    pub sl_trigger_price: Option<&'a str>,
    pub sl_order_price: Option<&'a str>,
    pub sl_trigger_price_type: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct BlofinAlgoOrderRequest<'a> {
    pub inst_id: &'a str,
    pub margin_mode: BlofinMarginMode,
    pub position_side: PositionSide,
    pub side: OrderSide,
    pub size: &'a str,
    pub order_type: &'a str,
    pub order_price: Option<&'a str>,
    pub trigger_price: Option<&'a str>,
    pub trigger_price_type: Option<&'a str>,
    pub reduce_only: Option<bool>,
    pub client_order_id: Option<&'a str>,
    pub broker_id: Option<&'a str>,
    pub attach_algo_orders: Vec<BlofinAttachedAlgoOrder<'a>>,
}

#[derive(Debug, Clone)]
pub struct BlofinTpslCancelRequest<'a> {
    pub inst_id: Option<&'a str>,
    pub tpsl_id: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct BlofinAlgoCancelRequest<'a> {
    pub inst_id: Option<&'a str>,
    pub algo_id: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct BlofinClosePositionRequest<'a> {
    pub inst_id: &'a str,
    pub margin_mode: BlofinMarginMode,
    pub position_side: PositionSide,
    pub client_order_id: Option<&'a str>,
    pub broker_id: Option<&'a str>,
}

impl BlofinGatewayAdapter {
    pub async fn get_blofin_asset_balances(
        &self,
        account_type: &str,
        currency: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("accountType".to_string(), account_type.to_string());
        optional_str_param(&mut params, "currency", currency);
        self.send_signed_get(
            "blofin.get_asset_balances",
            "/api/v1/asset/balances",
            &params,
        )
        .await
    }

    pub async fn transfer_blofin_funds(
        &self,
        request: &BlofinTransferRequest<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.transfer_funds",
            "/api/v1/asset/transfer",
            &HashMap::new(),
            blofin_transfer_body(request),
        )
        .await
    }

    pub async fn get_blofin_asset_bills(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_asset_bills",
            "/api/v1/asset/bills",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_withdrawal_history(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_withdrawal_history",
            "/api/v1/asset/withdrawal-history",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_deposit_history(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_deposit_history",
            "/api/v1/asset/deposit-history",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_account_config(&self) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_account_config",
            "/api/v1/account/config",
            &HashMap::new(),
        )
        .await
    }

    pub async fn apply_blofin_demo_funds(&self, body: Value) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.apply_demo_funds",
            "/api/v1/asset/demo-apply-money",
            &HashMap::new(),
            body,
        )
        .await
    }

    pub async fn get_blofin_currencies(&self) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_currencies",
            "/api/v1/asset/currencies",
            &HashMap::new(),
        )
        .await
    }

    pub async fn get_blofin_positions_history(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_positions_history",
            "/api/v1/account/positions-history",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_margin_mode(&self) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_margin_mode",
            "/api/v1/account/margin-mode",
            &HashMap::new(),
        )
        .await
    }

    pub async fn set_blofin_margin_mode(
        &self,
        margin_mode: BlofinMarginMode,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.set_margin_mode",
            "/api/v1/account/set-margin-mode",
            &HashMap::new(),
            json!({ "marginMode": margin_mode.as_str() }),
        )
        .await
    }

    pub async fn get_blofin_position_mode(&self) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_position_mode",
            "/api/v1/account/position-mode",
            &HashMap::new(),
        )
        .await
    }

    pub async fn set_blofin_position_mode(
        &self,
        position_mode: BlofinPositionMode,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.set_position_mode",
            "/api/v1/account/set-position-mode",
            &HashMap::new(),
            json!({ "positionMode": position_mode.as_str() }),
        )
        .await
    }

    pub async fn get_blofin_leverage(
        &self,
        inst_id: &str,
        margin_mode: BlofinMarginMode,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), normalize_blofin_symbol(inst_id)?);
        params.insert("marginMode".to_string(), margin_mode.as_str().to_string());
        self.send_signed_get(
            "blofin.get_leverage",
            "/api/v1/account/leverage-info",
            &params,
        )
        .await
    }

    pub async fn get_blofin_batch_leverage(
        &self,
        inst_ids: &[&str],
        margin_mode: BlofinMarginMode,
    ) -> ExchangeApiResult<Value> {
        if inst_ids.is_empty() || inst_ids.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "blofin batch leverage requires 1..=20 instruments".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            inst_ids
                .iter()
                .map(|symbol| normalize_blofin_symbol(symbol))
                .collect::<ExchangeApiResult<Vec<_>>>()?
                .join(","),
        );
        params.insert("marginMode".to_string(), margin_mode.as_str().to_string());
        self.send_signed_get(
            "blofin.get_batch_leverage",
            "/api/v1/account/batch-leverage-info",
            &params,
        )
        .await
    }

    pub async fn set_blofin_leverage(
        &self,
        request: &BlofinSetLeverageRequest<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.set_leverage",
            "/api/v1/account/set-leverage",
            &HashMap::new(),
            blofin_set_leverage_body(request)?,
        )
        .await
    }

    pub async fn place_blofin_tpsl_order(
        &self,
        request: &BlofinTpslOrderRequest<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.place_tpsl_order",
            "/api/v1/trade/order-tpsl",
            &HashMap::new(),
            blofin_tpsl_order_body(request)?,
        )
        .await
    }

    pub async fn place_blofin_algo_order(
        &self,
        request: &BlofinAlgoOrderRequest<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.place_algo_order",
            "/api/v1/trade/order-algo",
            &HashMap::new(),
            blofin_algo_order_body(request)?,
        )
        .await
    }

    pub async fn cancel_blofin_tpsl_orders(
        &self,
        requests: &[BlofinTpslCancelRequest<'_>],
    ) -> ExchangeApiResult<Value> {
        if requests.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "blofin cancel_tpsl requires at least one item".to_string(),
            });
        }
        self.send_signed_post(
            "blofin.cancel_tpsl_orders",
            "/api/v1/trade/cancel-tpsl",
            &HashMap::new(),
            Value::Array(
                requests
                    .iter()
                    .map(blofin_tpsl_cancel_body)
                    .collect::<ExchangeApiResult<Vec<_>>>()?,
            ),
        )
        .await
    }

    pub async fn cancel_blofin_algo_order(
        &self,
        request: &BlofinAlgoCancelRequest<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.cancel_algo_order",
            "/api/v1/trade/cancel-algo",
            &HashMap::new(),
            blofin_algo_cancel_body(request)?,
        )
        .await
    }

    pub async fn get_blofin_active_tpsl_orders(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_active_tpsl_orders",
            "/api/v1/trade/orders-tpsl-pending",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_tpsl_order_detail(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_tpsl_order_detail",
            "/api/v1/trade/order-tpsl-detail",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_active_algo_orders(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_active_algo_orders",
            "/api/v1/trade/orders-algo-pending",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn close_blofin_position(
        &self,
        request: &BlofinClosePositionRequest<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            "blofin.close_position",
            "/api/v1/trade/close-position",
            &HashMap::new(),
            blofin_close_position_body(request)?,
        )
        .await
    }

    pub async fn get_blofin_order_history(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_order_history",
            "/api/v1/trade/orders-history",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_tpsl_order_history(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_tpsl_order_history",
            "/api/v1/trade/orders-tpsl-history",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_algo_order_history(
        &self,
        query: &BlofinQueryParams<'_>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "blofin.get_algo_order_history",
            "/api/v1/trade/orders-algo-history",
            &blofin_query_params(query)?,
        )
        .await
    }

    pub async fn get_blofin_trade_order_price_range(
        &self,
        inst_id: &str,
        side: OrderSide,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), normalize_blofin_symbol(inst_id)?);
        params.insert("side".to_string(), side_text(side).to_string());
        self.send_signed_get(
            "blofin.get_trade_order_price_range",
            "/api/v1/trade/order/price-range",
            &params,
        )
        .await
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_perpetual(
            request.market_type.unwrap_or(MarketType::Perpetual),
            "blofin.spot_balances_trading_account_unsupported",
        )?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "blofin.get_balances")?;
        let value = self
            .send_signed_get(
                "blofin.get_balances",
                "/api/v1/account/balance",
                &HashMap::new(),
            )
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

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_perpetual(
            request.market_type.unwrap_or(MarketType::Perpetual),
            "blofin.spot_positions_unsupported",
        )?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "blofin.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "blofin adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "instId".to_string(),
                normalize_blofin_symbol(&symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("blofin.get_positions", "/api/v1/account/positions", &params)
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
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
            self.ensure_perpetual(symbol.market_type, "blofin.spot_fees_unsupported")?;
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
            "blofin.spot_place_order_unsupported",
        )?;
        let body = blofin_place_order_body(&self.config.margin_mode, &request)?;
        let value = self
            .send_signed_post(
                "blofin.place_order",
                "/api/v1/trade/order",
                &HashMap::new(),
                body,
            )
            .await?;
        let order = first_data_item(&value)
            .and_then(|item| parse_order_state(&self.exchange_id, Some(&request.symbol), item).ok())
            .unwrap_or_else(|| {
                ack_order(
                    &self.exchange_id,
                    &request.symbol,
                    first_data_item(&value).unwrap_or_else(|| data_payload(&value)),
                    OrderStatus::New,
                )
            });
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
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
            "blofin.spot_cancel_order_unsupported",
        )?;
        let body = blofin_cancel_body(&request)?;
        let value = self
            .send_signed_post(
                "blofin.cancel_order",
                "/api/v1/trade/cancel-order",
                &HashMap::new(),
                body,
            )
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                ack_order(
                    &self.exchange_id,
                    &request.symbol,
                    data_payload(&value),
                    OrderStatus::Cancelled,
                )
            });
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
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
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_perpetual(
                order.symbol.market_type,
                "blofin.spot_batch_place_unsupported",
            )?;
        }
        let body = request
            .orders
            .iter()
            .map(|order| blofin_place_order_body(&self.config.margin_mode, order))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .send_signed_post(
                "blofin.batch_place_orders",
                "/api/v1/trade/batch-orders",
                &HashMap::new(),
                Value::Array(body),
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, None, &value).unwrap_or_else(|_| {
            let data = data_payload(&value);
            request
                .orders
                .iter()
                .enumerate()
                .map(|(index, order)| {
                    let item = data
                        .as_array()
                        .and_then(|items| items.get(index))
                        .unwrap_or(data);
                    ack_order(&self.exchange_id, &order.symbol, item, OrderStatus::New)
                })
                .collect()
        });
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
        let body = request
            .cancels
            .iter()
            .map(|cancel| {
                self.ensure_exchange(&cancel.symbol.exchange)?;
                self.ensure_perpetual(
                    cancel.symbol.market_type,
                    "blofin.spot_batch_cancel_unsupported",
                )?;
                blofin_cancel_body(cancel)
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .send_signed_post(
                "blofin.batch_cancel_orders",
                "/api/v1/trade/cancel-batch-orders",
                &HashMap::new(),
                Value::Array(body),
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, None, &value).unwrap_or_else(|_| {
            let data = data_payload(&value);
            request
                .cancels
                .iter()
                .enumerate()
                .map(|(index, cancel)| {
                    let item = data
                        .as_array()
                        .and_then(|items| items.get(index))
                        .unwrap_or(data);
                    ack_order(
                        &self.exchange_id,
                        &cancel.symbol,
                        item,
                        OrderStatus::Cancelled,
                    )
                })
                .collect::<Vec<_>>()
        });
        let cancelled_count = orders
            .iter()
            .filter(|order| {
                order.status == OrderStatus::Cancelled || order.status == OrderStatus::Unknown
            })
            .count() as u32;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_perpetual(
            request.market_type.unwrap_or(MarketType::Perpetual),
            "blofin.spot_cancel_all_unsupported",
        )?;
        let open = self
            .get_open_orders_impl(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                market_type: Some(MarketType::Perpetual),
                symbol: request.symbol.clone(),
                page: None,
            })
            .await?;
        let cancels = open
            .orders
            .iter()
            .map(|order| CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                symbol: request.symbol.clone().unwrap_or_else(|| {
                    rustcta_exchange_api::SymbolScope {
                        exchange: request.exchange.clone(),
                        market_type: MarketType::Perpetual,
                        canonical_symbol: order.canonical_symbol.clone(),
                        exchange_symbol: order.exchange_symbol.clone(),
                    }
                }),
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: order.exchange_order_id.clone(),
            })
            .collect::<Vec<_>>();
        let response = self
            .batch_cancel_orders_impl(BatchCancelOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                cancels,
            })
            .await?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: response.cancelled_count,
            orders: response.orders,
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
            "blofin.spot_query_order_unsupported",
        )?;
        if missing_order_identity(&CancelOrderRequest {
            schema_version: request.schema_version,
            context: request.context.clone(),
            symbol: request.symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: request.exchange_order_id.clone(),
        }) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "blofin query_order requires order_id or client_order_id".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            normalize_blofin_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(client_id) = &request.client_order_id {
            params.insert("clientOrderId".to_string(), client_id.clone());
        }
        let value = self
            .send_signed_get("blofin.query_order", "/api/v1/trade/order-detail", &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_perpetual(
            request.market_type.unwrap_or(MarketType::Perpetual),
            "blofin.spot_open_orders_unsupported",
        )?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type, "blofin.spot_open_orders_unsupported")?;
            params.insert(
                "instId".to_string(),
                normalize_blofin_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "blofin.get_open_orders",
                "/api/v1/trade/orders-pending",
                &params,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_perpetual(
            request.market_type.unwrap_or(MarketType::Perpetual),
            "blofin.spot_recent_fills_unsupported",
        )?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "blofin.get_recent_fills")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            params.insert(
                "instId".to_string(),
                normalize_blofin_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(start) = request.start_time {
            params.insert("begin".to_string(), start.timestamp_millis().to_string());
        }
        if let Some(end) = request.end_time {
            params.insert("end".to_string(), end.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.clamp(1, 100).to_string());
        }
        let value = self
            .send_signed_get(
                "blofin.get_recent_fills",
                "/api/v1/trade/fills-history",
                &params,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }
}

pub(super) fn blofin_place_order_body(
    margin_mode: &str,
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<Value> {
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "blofin.quote_quantity_order",
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        json!(normalize_blofin_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert("marginMode".to_string(), json!(margin_mode));
    body.insert(
        "positionSide".to_string(),
        json!(position_side_text(
            request.position_side.unwrap_or(PositionSide::Net)
        )),
    );
    body.insert("side".to_string(), json!(side_text(request.side)));
    body.insert("orderType".to_string(), json!(order_type_text(request)));
    body.insert("size".to_string(), json!(request.quantity));
    body.insert(
        "reduceOnly".to_string(),
        json!(request.reduce_only.to_string()),
    );
    if let Some(price) = &request.price {
        body.insert("price".to_string(), json!(price));
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub(super) fn blofin_cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    if missing_order_identity(request) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "blofin cancel_order requires order_id or client_order_id".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        json!(normalize_blofin_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    if let Some(order_id) = &request.exchange_order_id {
        body.insert("orderId".to_string(), json!(order_id));
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub(super) fn blofin_transfer_body(request: &BlofinTransferRequest<'_>) -> Value {
    let mut body = serde_json::Map::new();
    body.insert("currency".to_string(), json!(request.currency));
    body.insert("amount".to_string(), json!(request.amount));
    body.insert("fromAccount".to_string(), json!(request.from_account));
    body.insert("toAccount".to_string(), json!(request.to_account));
    optional_json_str(&mut body, "clientId", request.client_id);
    optional_json_str(&mut body, "subAccount", request.sub_account);
    if let Some(value) = request.main_to_sub_account {
        body.insert("mainToSubAccount".to_string(), json!(value));
    }
    Value::Object(body)
}

pub(super) fn blofin_set_leverage_body(
    request: &BlofinSetLeverageRequest<'_>,
) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        json!(normalize_blofin_symbol(request.inst_id)?),
    );
    body.insert("leverage".to_string(), json!(request.leverage));
    body.insert(
        "marginMode".to_string(),
        json!(request.margin_mode.as_str()),
    );
    if let Some(side) = request.position_side {
        body.insert("positionSide".to_string(), json!(position_side_text(side)));
    }
    Ok(Value::Object(body))
}

pub(super) fn blofin_tpsl_order_body(
    request: &BlofinTpslOrderRequest<'_>,
) -> ExchangeApiResult<Value> {
    if request.tp_trigger_price.is_none() && request.sl_trigger_price.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "blofin TPSL order requires tp_trigger_price or sl_trigger_price".to_string(),
        });
    }
    let mut body = base_algo_body(
        request.inst_id,
        request.margin_mode,
        request.position_side,
        request.side,
        request.size,
    )?;
    optional_json_bool_string(&mut body, "reduceOnly", request.reduce_only);
    optional_json_str(&mut body, "clientOrderId", request.client_order_id);
    optional_json_str(&mut body, "brokerId", request.broker_id);
    optional_json_str(&mut body, "tpTriggerPrice", request.tp_trigger_price);
    optional_json_str(&mut body, "tpOrderPrice", request.tp_order_price);
    optional_json_str(
        &mut body,
        "tpTriggerPriceType",
        request.tp_trigger_price_type,
    );
    optional_json_str(&mut body, "slTriggerPrice", request.sl_trigger_price);
    optional_json_str(&mut body, "slOrderPrice", request.sl_order_price);
    optional_json_str(
        &mut body,
        "slTriggerPriceType",
        request.sl_trigger_price_type,
    );
    Ok(Value::Object(body))
}

pub(super) fn blofin_algo_order_body(
    request: &BlofinAlgoOrderRequest<'_>,
) -> ExchangeApiResult<Value> {
    let mut body = base_algo_body(
        request.inst_id,
        request.margin_mode,
        request.position_side,
        request.side,
        request.size,
    )?;
    body.insert("orderType".to_string(), json!(request.order_type));
    optional_json_str(&mut body, "orderPrice", request.order_price);
    optional_json_str(&mut body, "triggerPrice", request.trigger_price);
    optional_json_str(&mut body, "triggerPriceType", request.trigger_price_type);
    optional_json_bool_string(&mut body, "reduceOnly", request.reduce_only);
    optional_json_str(&mut body, "clientOrderId", request.client_order_id);
    optional_json_str(&mut body, "brokerId", request.broker_id);
    if !request.attach_algo_orders.is_empty() {
        body.insert(
            "attachAlgoOrders".to_string(),
            Value::Array(
                request
                    .attach_algo_orders
                    .iter()
                    .map(blofin_attached_algo_order_body)
                    .collect(),
            ),
        );
    }
    Ok(Value::Object(body))
}

pub(super) fn blofin_tpsl_cancel_body(
    request: &BlofinTpslCancelRequest<'_>,
) -> ExchangeApiResult<Value> {
    if request.tpsl_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "blofin cancel TPSL requires tpsl_id or client_order_id".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    optional_normalized_symbol(&mut body, "instId", request.inst_id)?;
    optional_json_str(&mut body, "tpslId", request.tpsl_id);
    optional_json_str(&mut body, "clientOrderId", request.client_order_id);
    Ok(Value::Object(body))
}

pub(super) fn blofin_algo_cancel_body(
    request: &BlofinAlgoCancelRequest<'_>,
) -> ExchangeApiResult<Value> {
    if request.algo_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "blofin cancel algo requires algo_id or client_order_id".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    optional_normalized_symbol(&mut body, "instId", request.inst_id)?;
    optional_json_str(&mut body, "algoId", request.algo_id);
    optional_json_str(&mut body, "clientOrderId", request.client_order_id);
    Ok(Value::Object(body))
}

pub(super) fn blofin_close_position_body(
    request: &BlofinClosePositionRequest<'_>,
) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        json!(normalize_blofin_symbol(request.inst_id)?),
    );
    body.insert(
        "marginMode".to_string(),
        json!(request.margin_mode.as_str()),
    );
    body.insert(
        "positionSide".to_string(),
        json!(position_side_text(request.position_side)),
    );
    optional_json_str(&mut body, "clientOrderId", request.client_order_id);
    optional_json_str(&mut body, "brokerId", request.broker_id);
    Ok(Value::Object(body))
}

pub(super) fn blofin_query_params(
    query: &BlofinQueryParams<'_>,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    optional_query_symbol(&mut params, "instId", query.inst_id)?;
    optional_query_str(&mut params, "orderId", query.order_id);
    optional_query_str(&mut params, "tpslId", query.tpsl_id);
    optional_query_str(&mut params, "algoId", query.algo_id);
    optional_query_str(&mut params, "clientOrderId", query.client_order_id);
    optional_query_str(&mut params, "orderType", query.order_type);
    optional_query_str(&mut params, "state", query.state);
    optional_query_str(&mut params, "currency", query.currency);
    optional_query_str(&mut params, "fromAccount", query.from_account);
    optional_query_str(&mut params, "toAccount", query.to_account);
    optional_query_str(&mut params, "withdrawId", query.withdraw_id);
    optional_query_str(&mut params, "depositId", query.deposit_id);
    optional_query_str(&mut params, "txId", query.tx_id);
    optional_query_str(&mut params, "before", query.before);
    optional_query_str(&mut params, "after", query.after);
    optional_query_str(&mut params, "begin", query.begin);
    optional_query_str(&mut params, "end", query.end);
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), limit.clamp(1, 100).to_string());
    }
    Ok(params)
}

fn first_data_item(value: &Value) -> Option<&Value> {
    data_payload(value)
        .as_array()
        .and_then(|items| items.first())
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn position_side_text(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net | PositionSide::None => "net",
    }
}

fn order_type_text(request: &PlaceOrderRequest) -> &'static str {
    if request.post_only {
        return "post_only";
    }
    match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit => match request.time_in_force {
            Some(TimeInForce::IOC) => "ioc",
            Some(TimeInForce::FOK) => "fok",
            Some(TimeInForce::GTX) => "post_only",
            _ => "limit",
        },
        OrderType::PostOnly => "post_only",
        OrderType::IOC => "ioc",
        OrderType::FOK => "fok",
        OrderType::StopMarket | OrderType::StopLimit => "market",
    }
}

fn base_algo_body(
    inst_id: &str,
    margin_mode: BlofinMarginMode,
    position_side: PositionSide,
    side: OrderSide,
    size: &str,
) -> ExchangeApiResult<serde_json::Map<String, Value>> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        json!(normalize_blofin_symbol(inst_id)?),
    );
    body.insert("marginMode".to_string(), json!(margin_mode.as_str()));
    body.insert(
        "positionSide".to_string(),
        json!(position_side_text(position_side)),
    );
    body.insert("side".to_string(), json!(side_text(side)));
    body.insert("size".to_string(), json!(size));
    Ok(body)
}

fn blofin_attached_algo_order_body(request: &BlofinAttachedAlgoOrder<'_>) -> Value {
    let mut body = serde_json::Map::new();
    optional_json_str(&mut body, "tpTriggerPrice", request.tp_trigger_price);
    optional_json_str(&mut body, "tpOrderPrice", request.tp_order_price);
    optional_json_str(
        &mut body,
        "tpTriggerPriceType",
        request.tp_trigger_price_type,
    );
    optional_json_str(&mut body, "slTriggerPrice", request.sl_trigger_price);
    optional_json_str(&mut body, "slOrderPrice", request.sl_order_price);
    optional_json_str(
        &mut body,
        "slTriggerPriceType",
        request.sl_trigger_price_type,
    );
    Value::Object(body)
}

fn optional_json_str(body: &mut serde_json::Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        body.insert(key.to_string(), json!(value));
    }
}

fn optional_json_bool_string(
    body: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<bool>,
) {
    if let Some(value) = value {
        body.insert(key.to_string(), json!(value.to_string()));
    }
}

fn optional_normalized_symbol(
    body: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(value) = value {
        body.insert(key.to_string(), json!(normalize_blofin_symbol(value)?));
    }
    Ok(())
}

fn optional_query_str(params: &mut HashMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        params.insert(key.to_string(), value.to_string());
    }
}

fn optional_query_symbol(
    params: &mut HashMap<String, String>,
    key: &str,
    value: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(value) = value {
        params.insert(key.to_string(), normalize_blofin_symbol(value)?);
    }
    Ok(())
}

fn optional_str_param(params: &mut HashMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        params.insert(key.to_string(), value.to_string());
    }
}
