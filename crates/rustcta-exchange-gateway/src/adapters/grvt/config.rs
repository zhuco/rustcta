#[derive(Debug, Clone)]
pub struct GrvtGatewayConfig {
    pub market_data_rest_base_url: String,
    pub trading_rest_base_url: String,
    pub auth_base_url: String,
    pub market_data_ws_url: String,
    pub trading_ws_url: String,
    pub api_key: Option<String>,
    pub session_cookie: Option<String>,
    pub account_id: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for GrvtGatewayConfig {
    fn default() -> Self {
        Self {
            market_data_rest_base_url: "https://market-data.grvt.io/full".to_string(),
            trading_rest_base_url: "https://trades.grvt.io/full".to_string(),
            auth_base_url: "https://edge.grvt.io/auth".to_string(),
            market_data_ws_url: "wss://market-data.grvt.io/ws/full".to_string(),
            trading_ws_url: "wss://trades.grvt.io/ws/full".to_string(),
            api_key: non_empty_env("GRVT_API_KEY"),
            session_cookie: non_empty_env("GRVT_COOKIE"),
            account_id: non_empty_env("GRVT_ACCOUNT_ID"),
            enabled_private_rest: env_bool("GRVT_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("RUSTCTA_GRVT_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            enabled_public_streams: env_bool("GRVT_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("GRVT_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl GrvtGatewayConfig {
    pub fn private_session_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .session_cookie
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .account_id
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
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
