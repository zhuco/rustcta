#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_tokocrypto_symbol, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::TokocryptoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokocryptoRequestSpec {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: String,
}

pub fn place_limit_order_spec(
    symbol: &str,
    side: &str,
    price: &str,
    quantity: &str,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    let params = BTreeMap::from([
        ("price".to_string(), price.to_string()),
        ("quantity".to_string(), quantity.to_string()),
        ("recvWindow".to_string(), recv_window.to_string()),
        ("side".to_string(), side_code(side)?.to_string()),
        ("symbol".to_string(), normalize_tokocrypto_symbol(symbol)?),
        ("timeInForce".to_string(), "1".to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
        ("type".to_string(), "1".to_string()),
    ]);
    Ok(TokocryptoRequestSpec {
        method: "POST".to_string(),
        path: "/open/v1/orders".to_string(),
        query: params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
        body: String::new(),
    })
}

pub fn cancel_order_spec(
    symbol: &str,
    order_id: u64,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    Ok(TokocryptoRequestSpec {
        method: "POST".to_string(),
        path: "/open/v1/orders/cancel".to_string(),
        query: format!(
            "orderId={order_id}&recvWindow={recv_window}&symbol={}&timestamp={timestamp}",
            normalize_tokocrypto_symbol(symbol)?
        ),
        body: String::new(),
    })
}

pub fn query_order_spec(
    symbol: &str,
    order_id: u64,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    Ok(TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/orders/detail".to_string(),
        query: format!(
            "orderId={order_id}&recvWindow={recv_window}&symbol={}&timestamp={timestamp}",
            normalize_tokocrypto_symbol(symbol)?
        ),
        body: String::new(),
    })
}

pub fn account_spec(timestamp: u64, recv_window: u64) -> TokocryptoRequestSpec {
    TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/account/spot".to_string(),
        query: signed_base_query(timestamp, recv_window),
        body: String::new(),
    }
}

pub fn open_orders_spec(
    symbol: Option<&str>,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    let mut params = BTreeMap::from([
        ("recvWindow".to_string(), recv_window.to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
    ]);
    if let Some(symbol) = symbol {
        params.insert("symbol".to_string(), normalize_tokocrypto_symbol(symbol)?);
    }
    Ok(TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/orders".to_string(),
        query: params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
        body: String::new(),
    })
}

pub fn recent_fills_spec(
    symbol: &str,
    order_id: Option<u64>,
    limit: Option<u32>,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    let mut params = BTreeMap::from([
        ("recvWindow".to_string(), recv_window.to_string()),
        ("symbol".to_string(), normalize_tokocrypto_symbol(symbol)?),
        ("timestamp".to_string(), timestamp.to_string()),
    ]);
    if let Some(order_id) = order_id {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(limit) = limit {
        params.insert("limit".to_string(), limit.min(1000).to_string());
    }
    Ok(TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/orders/trades".to_string(),
        query: params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
        body: String::new(),
    })
}

pub fn listen_token_spec(timestamp: u64, recv_window: u64) -> TokocryptoRequestSpec {
    TokocryptoRequestSpec {
        method: "POST".to_string(),
        path: "/open/v1/user-listen-token".to_string(),
        query: signed_base_query(timestamp, recv_window),
        body: String::new(),
    }
}

fn signed_base_query(timestamp: u64, recv_window: u64) -> String {
    let params = BTreeMap::from([
        ("recvWindow".to_string(), recv_window.to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
    ]);
    params
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn side_code(side: &str) -> ExchangeApiResult<i32> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" | "0" => Ok(0),
        "sell" | "1" => Ok(1),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported tokocrypto side {side}"),
        }),
    }
}

impl TokocryptoGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message:
                    "tokocrypto query_order requires exchange_order_id; client_order_id lookup is not promoted"
                        .to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_tokocrypto_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("orderId".to_string(), order_id.to_string());
        let value = self
            .send_signed_get("tokocrypto.query_order", "/open/v1/orders/detail", &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_order_state(
                &self.exchange_id,
                Some(&request.symbol),
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
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_tokocrypto_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("tokocrypto.get_open_orders", "/open/v1/orders", &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
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
                message: "tokocrypto get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_tokocrypto_symbol(&symbol.exchange_symbol.symbol)?,
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
        }
        let value = self
            .send_signed_get(
                "tokocrypto.get_recent_fills",
                "/open/v1/orders/trades",
                &params,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str, u64)> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
            self.config.recv_window_ms,
        ))
    }

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "tokocrypto private REST readback requires context.tenant_id"
                        .to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "tokocrypto private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        account_spec, cancel_order_spec, listen_token_spec, open_orders_spec,
        place_limit_order_spec, query_order_spec, recent_fills_spec,
    };

    #[test]
    fn tokocrypto_request_specs_should_match_fixtures() {
        assert_spec(
            "place_limit_order.json",
            place_limit_order_spec("BTC_USDT", "buy", "7500", "0.16", 1581720670624, 5000)
                .expect("place"),
        );
        assert_spec("account_get.json", account_spec(1581720670624, 5000));
        assert_spec(
            "open_orders_get.json",
            open_orders_spec(Some("BTC_USDT"), 1581720670624, 5000).expect("open orders"),
        );
        assert_spec(
            "cancel_order.json",
            cancel_order_spec("BTC_USDT", 12345, 1581720670624, 5000).expect("cancel"),
        );
        assert_spec(
            "query_order.json",
            query_order_spec("BTC_USDT", 12345, 1581720670624, 5000).expect("query"),
        );
        assert_spec(
            "recent_fills_get.json",
            recent_fills_spec("BTC_USDT", Some(12345), Some(50), 1581720670624, 5000)
                .expect("fills"),
        );
        assert_spec("listen_token.json", listen_token_spec(1581720670624, 5000));
    }

    fn assert_spec(name: &str, actual: super::TokocryptoRequestSpec) {
        let fixture: Value = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../../tests/fixtures/exchanges/tokocrypto/request_specs/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .expect("fixture"),
        )
        .expect("fixture json");
        assert_eq!(actual.method, fixture["method"].as_str().unwrap());
        assert_eq!(actual.path, fixture["path"].as_str().unwrap());
        assert_eq!(actual.query, fixture["query"].as_str().unwrap());
        if actual.body.is_empty() {
            assert!(fixture["body"].is_null());
        } else {
            assert_eq!(
                serde_json::from_str::<Value>(&actual.body).unwrap(),
                fixture["body"]
            );
        }
    }
}
