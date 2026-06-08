#[derive(Debug, Clone)]
pub struct CoinbaseExchangeGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinbaseExchangeGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("COINBASE_EXCHANGE_API_KEY")
            .or_else(|| non_empty_env("COINBASEEXCHANGE_API_KEY"));
        let api_secret = non_empty_env("COINBASE_EXCHANGE_API_SECRET")
            .or_else(|| non_empty_env("COINBASEEXCHANGE_API_SECRET"));
        let api_passphrase = non_empty_env("COINBASE_EXCHANGE_API_PASSPHRASE")
            .or_else(|| non_empty_env("COINBASEEXCHANGE_API_PASSPHRASE"));
        let enabled_private_rest = env_bool("COINBASE_EXCHANGE_PRIVATE_REST_ENABLED")
            .or_else(|| env_bool("COINBASEEXCHANGE_PRIVATE_REST_ENABLED"))
            .unwrap_or_else(|| {
                api_key.is_some() && api_secret.is_some() && api_passphrase.is_some()
            });
        Self {
            rest_base_url: "https://api.exchange.coinbase.com".to_string(),
            ws_url: "wss://ws-feed.exchange.coinbase.com".to_string(),
            api_key,
            api_secret,
            api_passphrase,
            enabled_private_rest,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CoinbaseExchangeGatewayConfig {
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
            && self
                .api_passphrase
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
