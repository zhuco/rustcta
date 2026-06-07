const DEFAULT_REST_BASE_URL: &str = "https://www.biconomy.com";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://bei.biconomy.com/ws";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://www.biconomy.com/ws";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;

#[derive(Debug, Clone)]
pub struct BiconomyGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub recv_window_ms: u64,
    pub enabled: bool,
}

impl Default for BiconomyGatewayConfig {
    fn default() -> Self {
        let api_key = first_env(&["BICONOMY_API_KEY", "BICONOMY_EXCHANGE_API_KEY"]);
        let api_secret = first_env(&["BICONOMY_API_SECRET", "BICONOMY_EXCHANGE_API_SECRET"]);
        let has_credentials = api_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());
        let enabled_private_rest = std::env::var("BICONOMY_PRIVATE_REST_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_ws_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_ws_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            recv_window_ms: DEFAULT_RECV_WINDOW_MS,
            enabled: true,
        }
    }
}

impl BiconomyGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
