use std::collections::BTreeMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PageCursor,
    QueryOrderRequest, QueryOrderResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{coin_param_from_symbol, parse_btcbox_open_orders, parse_btcbox_order_state};
use super::signing::{canonical_form, sign_canonical_form};
use super::BtcboxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub fn private_post_request_spec(
    operation: &str,
    endpoint: &str,
    api_key: &str,
    private_key: &str,
    mut params: BTreeMap<String, String>,
) -> ExchangeApiResult<Value> {
    params.insert("key".to_string(), api_key.to_string());
    let canonical = canonical_form(&params);
    let signature = sign_canonical_form(private_key, &params)?;
    let body = format!("{canonical}&signature={signature}");
    Ok(json!({
        "exchange": "btcbox",
        "operation": operation,
        "method": "POST",
        "path": format!("/api/v1/{endpoint}"),
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "body": body,
        "canonical": canonical,
        "auth": "hmac_sha256_md5_private_key",
        "expected_signature": signature,
        "network": "offline_request_spec_only"
    }))
}

pub fn trade_view_params(
    symbol: &str,
    order_id: &str,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    if order_id.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "btcbox query_order requires exchange_order_id".to_string(),
        });
    }
    let mut params = BTreeMap::new();
    params.insert("coin".to_string(), coin_param_from_symbol(symbol)?);
    params.insert("id".to_string(), order_id.trim().to_string());
    Ok(params)
}

pub fn trade_list_params(
    symbol: &str,
    cursor: Option<&PageCursor>,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    params.insert("coin".to_string(), coin_param_from_symbol(symbol)?);
    params.insert("since".to_string(), since_from_cursor(cursor)?.to_string());
    Ok(params)
}

impl BtcboxGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcbox.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "btcbox query_order requires exchange_order_id".to_string(),
            }
        })?;
        let (api_key, api_secret) = self.private_credentials("btcbox.query_order")?;
        let value = self
            .rest
            .send_private_post(
                api_key,
                api_secret,
                "trade_view",
                trade_view_params(&request.symbol.exchange_symbol.symbol, order_id)?,
            )
            .await?;
        let response_exchange = request.symbol.exchange.clone();
        let symbol = request.symbol.clone();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(response_exchange, request.context.request_id),
            order: Some(parse_btcbox_order_state(
                &self.exchange_id,
                &symbol,
                &value,
            )?),
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcbox get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        if let Some(page) = request.page.as_ref() {
            page.validate(Some(100))
                .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
            if page.limit.is_some() {
                return Err(ExchangeApiError::Unsupported {
                    operation: "btcbox.get_open_orders.limit",
                });
            }
        }
        let (api_key, api_secret) = self.private_credentials("btcbox.get_open_orders")?;
        let value = self
            .rest
            .send_private_post(
                api_key,
                api_secret,
                "trade_list",
                trade_list_params(
                    &symbol.exchange_symbol.symbol,
                    request.page.as_ref().and_then(|page| page.cursor.as_ref()),
                )?,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_btcbox_open_orders(&self.exchange_id, symbol, &value)?,
        })
    }
}

fn since_from_cursor(cursor: Option<&PageCursor>) -> ExchangeApiResult<i64> {
    match cursor {
        None => Ok(0),
        Some(PageCursor::Timestamp { millis }) => Ok(millis.div_euclid(1000)),
        Some(PageCursor::TimeRange { start_ms, .. }) => Ok(start_ms.div_euclid(1000)),
        Some(PageCursor::Offset { offset }) => {
            i64::try_from(*offset).map_err(|_| ExchangeApiError::InvalidRequest {
                message: "btcbox since offset exceeds i64 range".to_string(),
            })
        }
        Some(PageCursor::Id { .. }) | Some(PageCursor::Token { .. }) => {
            Err(ExchangeApiError::Unsupported {
                operation: "btcbox.get_open_orders.cursor",
            })
        }
    }
}
