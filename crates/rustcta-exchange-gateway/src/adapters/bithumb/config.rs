const DEFAULT_REST_BASE_URL: &str = "https://api.bithumb.com";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://pubwss.bithumb.com/pub/ws";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://pubwss.bithumb.com/pub/ws";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BithumbGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BithumbGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("BITHUMB_API_KEY");
        let api_secret = non_empty_env("BITHUMB_API_SECRET");
        let has_credentials = api_key.is_some() && api_secret.is_some();
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_ws_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_ws_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: env_bool("BITHUMB_PRIVATE_REST_ENABLED")
                .unwrap_or(has_credentials),
            enabled_public_streams: env_bool("BITHUMB_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("BITHUMB_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(has_credentials),
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl BithumbGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
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

    pub fn private_stream_available(&self) -> bool {
        self.enabled_private_streams && self.private_rest_available()
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
