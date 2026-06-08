#[derive(Debug, Clone)]
pub struct FmfwioGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub trading_ws_url: String,
    pub wallet_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub auth_window_ms: u64,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for FmfwioGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: non_empty_env("FMFWIO_REST_BASE_URL")
                .or_else(|| non_empty_env("RUSTCTA_FMFWIO_REST_BASE_URL"))
                .unwrap_or_else(|| "https://api.fmfw.io/api/3".to_string()),
            public_ws_url: non_empty_env("FMFWIO_PUBLIC_WS_URL")
                .or_else(|| non_empty_env("RUSTCTA_FMFWIO_PUBLIC_WS_URL"))
                .unwrap_or_else(|| "wss://api.fmfw.io/api/3/ws/public".to_string()),
            trading_ws_url: non_empty_env("FMFWIO_TRADING_WS_URL")
                .or_else(|| non_empty_env("RUSTCTA_FMFWIO_TRADING_WS_URL"))
                .unwrap_or_else(|| "wss://api.fmfw.io/api/3/ws/trading".to_string()),
            wallet_ws_url: non_empty_env("FMFWIO_WALLET_WS_URL")
                .or_else(|| non_empty_env("RUSTCTA_FMFWIO_WALLET_WS_URL"))
                .unwrap_or_else(|| "wss://api.fmfw.io/api/3/ws/wallet".to_string()),
            api_key: non_empty_env("FMFWIO_API_KEY")
                .or_else(|| non_empty_env("RUSTCTA_FMFWIO_API_KEY")),
            api_secret: non_empty_env("FMFWIO_API_SECRET")
                .or_else(|| non_empty_env("RUSTCTA_FMFWIO_API_SECRET")),
            auth_window_ms: env_u64("FMFWIO_AUTH_WINDOW_MS")
                .or_else(|| env_u64("RUSTCTA_FMFWIO_AUTH_WINDOW_MS"))
                .unwrap_or(10_000),
            enabled_private_rest: env_bool("FMFWIO_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("RUSTCTA_FMFWIO_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: false,
        }
    }
}

impl FmfwioGatewayConfig {
    pub fn private_request_specs_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
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

fn env_u64(key: &str) -> Option<u64> {
    non_empty_env(key)?.parse().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    match non_empty_env(key)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
