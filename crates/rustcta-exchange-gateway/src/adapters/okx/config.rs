use std::fmt;

#[derive(Clone)]
pub struct OkxGatewayConfig {
    pub exchange_id: String,
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub request_timeout_ms: u64,
    pub enabled: bool,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub status_message: String,
    pub unsupported_market_type_operation: &'static str,
}

impl Default for OkxGatewayConfig {
    fn default() -> Self {
        let api_key = env_first(["OKX_API_KEY", "OKX_SPOT_API_KEY"]);
        let api_secret = env_first(["OKX_API_SECRET", "OKX_SPOT_API_SECRET"]);
        let passphrase = env_first(["OKX_PASSPHRASE", "OKX_SPOT_PASSPHRASE"]);
        let enabled_private_rest =
            api_key.is_some() && api_secret.is_some() && passphrase.is_some();
        Self {
            exchange_id: "okx".to_string(),
            rest_base_url: "https://www.okx.com".to_string(),
            public_ws_url: env_first(["OKX_PUBLIC_WS_URL", "OKX_SPOT_PUBLIC_WS_URL"])
                .unwrap_or_else(|| "wss://ws.okx.com:8443/ws/v5/public".to_string()),
            request_timeout_ms: 10_000,
            enabled: true,
            api_key,
            api_secret,
            passphrase,
            enabled_private_rest,
            enabled_public_streams: env_bool("OKX_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            status_message: "okx public REST gateway adapter".to_string(),
            unsupported_market_type_operation: "okx.non_spot_market_type",
        }
    }
}

impl OkxGatewayConfig {
    pub fn has_private_credentials(&self) -> bool {
        self.api_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .passphrase
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }
}

impl fmt::Debug for OkxGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OkxGatewayConfig")
            .field("exchange_id", &self.exchange_id)
            .field("rest_base_url", &self.rest_base_url)
            .field("public_ws_url", &self.public_ws_url)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("enabled", &self.enabled)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "passphrase",
                &self.passphrase.as_ref().map(|_| "<redacted>"),
            )
            .field("enabled_private_rest", &self.enabled_private_rest)
            .field("enabled_public_streams", &self.enabled_public_streams)
            .field("status_message", &self.status_message)
            .field(
                "unsupported_market_type_operation",
                &self.unsupported_market_type_operation,
            )
            .finish()
    }
}

fn env_first<const N: usize>(keys: [&str; N]) -> Option<String> {
    keys.into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    match env_first([key])?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
