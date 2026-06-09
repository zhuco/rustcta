#[derive(Debug, Clone)]
pub struct NdaxGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub oms_id: i64,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub user_id: Option<String>,
    pub account_id: Option<i64>,
    pub enabled_public_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_rest: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for NdaxGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: non_empty_env("NDAX_REST_BASE_URL")
                .unwrap_or_else(|| "https://api.ndax.in".to_string()),
            ws_url: non_empty_env("NDAX_WS_URL")
                .unwrap_or_else(|| "wss://api.ndax.io/WSGateway/".to_string()),
            oms_id: non_empty_env("NDAX_OMS_ID")
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            api_key: non_empty_env("NDAX_API_KEY"),
            api_secret: non_empty_env("NDAX_API_SECRET"),
            passphrase: non_empty_env("NDAX_API_PASSPHRASE"),
            user_id: non_empty_env("NDAX_USER_ID"),
            account_id: non_empty_env("NDAX_ACCOUNT_ID").and_then(|value| value.parse().ok()),
            enabled_public_rest: env_bool("NDAX_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("NDAX_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_rest: env_bool("NDAX_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("NDAX_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl NdaxGatewayConfig {
    pub fn private_credentials_available(&self) -> bool {
        self.api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self.private_credentials_available()
            && self
                .user_id
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self.account_id.is_some()
            && self.oms_id > 0
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(format!("RUSTCTA_{key}"))
        .or_else(|_| std::env::var(key))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    match non_empty_env(key)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
