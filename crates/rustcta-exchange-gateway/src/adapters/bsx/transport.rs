use std::time::Duration;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::ExchangeId;
use serde_json::Value;

use super::signing;

#[derive(Clone)]
pub struct BsxRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BsxRest {
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
        query: &[(String, String)],
        auth: &BsxRestAuth,
    ) -> ExchangeApiResult<Value> {
        let normalized_path = normalized_path(path);
        let timestamp_ns = chrono::Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            ExchangeApiError::Transport {
                message: "failed to build BSX auth timestamp".to_string(),
            }
        })?;
        let query_string = canonical_query(query);
        let signature = signing::bsx_rest_auth_signature(
            &auth.api_key,
            &auth.api_secret,
            timestamp_ns.into(),
            "GET",
            &normalized_path,
            &query_string,
        );
        let mut request = self
            .http
            .get(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                normalized_path
            ))
            .header("BSX-KEY", auth.api_key.trim())
            .header("BSX-SIGNATURE", signature)
            .header("BSX-TIMESTAMP", timestamp_ns.to_string())
            .header("BSX-ACCOUNT", auth.wallet_address.trim());
        if !query.is_empty() {
            request = request.query(query);
        }
        let response = request
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub fn products_path() -> &'static str {
        "/products"
    }

    pub fn orderbook_path(product_id: &str) -> String {
        format!("/products/{}/book", sanitize_path_segment(product_id))
    }

    pub fn open_orders_path() -> &'static str {
        "/orders"
    }

    pub fn query_order_path(order_id: &str) -> String {
        format!("/orders/{}", sanitize_path_segment(order_id))
    }

    pub fn recent_fills_path() -> &'static str {
        "/trades"
    }

    pub fn place_order_path() -> &'static str {
        "/orders"
    }

    pub fn cancel_order_path(order_id: &str) -> String {
        format!("/orders/{}", sanitize_path_segment(order_id))
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            normalized_path(path)
        )
    }
}

#[derive(Debug, Clone)]
pub struct BsxRestAuth {
    pub api_key: String,
    pub api_secret: String,
    pub wallet_address: String,
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

fn sanitize_path_segment(segment: &str) -> String {
    segment
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        .collect()
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs = query
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0).then_with(|| left.1.cmp(right.1)));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
