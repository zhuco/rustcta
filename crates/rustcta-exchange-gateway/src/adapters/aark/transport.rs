use std::collections::BTreeMap;
use std::time::Duration;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::ExchangeId;
use serde_json::Value;

use super::signing::{sign_orderly_request, OrderlyAuth};

#[derive(Clone)]
pub struct AarkRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl AarkRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(16)
            .tcp_keepalive(Duration::from_secs(60))
            .timeout(Duration::from_millis(request_timeout_ms))
            .user_agent("RustCTA-Gateway/0.3")
            .build()
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        Ok(Self {
            exchange_id,
            rest_base_url,
            http,
        })
    }

    pub async fn send_public_get(&self, path: &str) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(self.url(path))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub async fn send_signed_get(
        &self,
        path: &str,
        params: &BTreeMap<String, String>,
        auth: &OrderlyAuth,
    ) -> ExchangeApiResult<Value> {
        let path = normalized_path(path);
        let query = encoded_query(params);
        let path_with_query = if query.is_empty() {
            path.clone()
        } else {
            format!("{path}?{query}")
        };
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let signed = sign_orderly_request(auth, timestamp_ms, "GET", &path_with_query, "")?;
        let response = self
            .http
            .get(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                path_with_query
            ))
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header("orderly-account-id", signed.orderly_account_id)
            .header("orderly-key", signed.orderly_key)
            .header("orderly-timestamp", signed.timestamp_ms.to_string())
            .header("orderly-signature", signed.orderly_signature)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub fn public_info_path() -> &'static str {
        "/v1/public/info"
    }

    pub fn signed_orderbook_path(symbol: &str) -> String {
        format!("/v1/orderbook/{}", symbol.trim())
    }

    pub fn query_order_path(order_id: &str) -> String {
        format!("/v1/order/{}", order_id.trim())
    }

    pub fn query_client_order_path(client_order_id: &str) -> String {
        format!("/v1/client/order/{}", client_order_id.trim())
    }

    pub fn orders_path() -> &'static str {
        "/v1/orders"
    }

    pub fn trades_path() -> &'static str {
        "/v1/trades"
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            normalized_path(path)
        )
    }
}

fn encoded_query(params: &BTreeMap<String, String>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

async fn parse_response(
    exchange_id: &ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = if text.trim().is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str::<Value>(&text).map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?
    };
    if !status.is_success() {
        return Err(ExchangeApiError::Transport {
            message: format!("{exchange_id} REST request failed with HTTP {status}: {value}"),
        });
    }
    Ok(value)
}

fn normalized_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}
