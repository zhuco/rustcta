use std::fmt;

#[derive(Clone)]
pub struct BitgetGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub public_order_book_channel: String,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitgetGatewayConfig {
    fn default() -> Self {
        let api_key = env_first(["BITGET_API_KEY", "BITGET_SPOT_API_KEY"]);
        let api_secret = env_first(["BITGET_API_SECRET", "BITGET_SPOT_API_SECRET"]);
        let passphrase = env_first([
            "BITGET_PASSPHRASE",
            "BITGET_API_PASSPHRASE",
            "BITGET_SPOT_PASSPHRASE",
            "BITGET_SPOT_API_PASSPHRASE",
        ]);
        let enabled_private_rest =
            api_key.is_some() && api_secret.is_some() && passphrase.is_some();
        Self {
            rest_base_url: "https://api.bitget.com".to_string(),
            public_ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            api_key,
            api_secret,
            passphrase,
            enabled_private_rest,
            enabled_public_streams: env_bool("BITGET_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            public_order_book_channel: env_first(["BITGET_PUBLIC_ORDER_BOOK_CHANNEL"])
                .unwrap_or_else(|| "books5".to_string()),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BitgetGatewayConfig {
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

impl fmt::Debug for BitgetGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BitgetGatewayConfig")
            .field("rest_base_url", &self.rest_base_url)
            .field("public_ws_url", &self.public_ws_url)
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
            .field("public_order_book_channel", &self.public_order_book_channel)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("enabled", &self.enabled)
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
