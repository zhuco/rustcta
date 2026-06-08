use std::time::Duration;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::ExchangeId;
use serde_json::Value;

#[derive(Clone)]
pub struct HibachiRest {
    exchange_id: ExchangeId,
    data_rest_base_url: String,
    account_rest_base_url: String,
    api_key: Option<String>,
    http: reqwest::Client,
}

impl HibachiRest {
    pub fn new(
        exchange_id: ExchangeId,
        data_rest_base_url: String,
        account_rest_base_url: String,
        api_key: Option<String>,
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
            data_rest_base_url,
            account_rest_base_url,
            api_key,
            http,
        })
    }

    pub async fn send_data_get(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(self.data_url(path))
            .query(query)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub async fn send_account_get(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> ExchangeApiResult<Value> {
        let response = self
            .with_api_key(self.http.get(self.account_url(path)).query(query))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    fn data_url(&self, path: &str) -> String {
        join_url(&self.data_rest_base_url, path)
    }

    fn account_url(&self, path: &str) -> String {
        join_url(&self.account_rest_base_url, path)
    }

    fn with_api_key(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.api_key.as_deref() {
            Some(api_key) if !api_key.trim().is_empty() => {
                request.header("Authorization", format!("Bearer {api_key}"))
            }
            _ => request,
        }
    }
}

pub fn hibachi_account_query(account_id: &str) -> Vec<(&'static str, String)> {
    vec![("accountId", account_id.to_string())]
}

fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        }
    )
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
