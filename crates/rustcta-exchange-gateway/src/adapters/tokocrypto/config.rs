const DEFAULT_REST_BASE_URL: &str = "https://www.tokocrypto.com";
const DEFAULT_MARKET_REST_BASE_URL: &str = "https://www.tokocrypto.site";
const DEFAULT_NEXTME_REST_BASE_URL: &str = "https://cloudme-toko.2meta.app";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://stream-cloud.tokocrypto.site/stream";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://ws-api.tokocrypto.site/ws-api/v3";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;

#[derive(Debug, Clone)]
pub struct TokocryptoGatewayConfig {
    pub rest_base_url: String,
    pub market_rest_base_url: String,
    pub nextme_rest_base_url: String,
    pub public_websocket_url: String,
    pub private_websocket_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub recv_window_ms: u64,
    pub enabled: bool,
}

impl Default for TokocryptoGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("TOKOCRYPTO_API_KEY")
            .or_else(|| non_empty_env("RUSTCTA_TOKOCRYPTO_API_KEY"));
        let api_secret = non_empty_env("TOKOCRYPTO_API_SECRET")
            .or_else(|| non_empty_env("RUSTCTA_TOKOCRYPTO_API_SECRET"));
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            market_rest_base_url: DEFAULT_MARKET_REST_BASE_URL.to_string(),
            nextme_rest_base_url: DEFAULT_NEXTME_REST_BASE_URL.to_string(),
            public_websocket_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_websocket_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: env_bool("TOKOCRYPTO_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("RUSTCTA_TOKOCRYPTO_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            enabled_public_streams: false,
            enabled_private_streams: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            recv_window_ms: DEFAULT_RECV_WINDOW_MS,
            enabled: true,
        }
    }
}

impl TokocryptoGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
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

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    let value = non_empty_env(name)?;
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
