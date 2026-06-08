use std::fmt;

#[derive(Clone)]
pub struct DeltaGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_stream: bool,
    pub enabled_private_stream: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for DeltaGatewayConfig {
    fn default() -> Self {
        let api_key = env_first(["DELTA_API_KEY", "DELTA_EXCHANGE_API_KEY"]);
        let api_secret = env_first(["DELTA_API_SECRET", "DELTA_EXCHANGE_API_SECRET"]);
        let enabled_private_rest = api_key.is_some() && api_secret.is_some();
        Self {
            rest_base_url: "https://api.india.delta.exchange".to_string(),
            public_ws_url: "wss://public-socket.india.delta.exchange".to_string(),
            private_ws_url: "wss://socket.india.delta.exchange".to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            enabled_public_stream: true,
            enabled_private_stream: enabled_private_rest,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl DeltaGatewayConfig {
    pub fn has_private_credentials(&self) -> bool {
        self.api_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }

    pub fn private_stream_available(&self) -> bool {
        self.enabled_private_stream && self.private_rest_available()
    }
}

impl fmt::Debug for DeltaGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaGatewayConfig")
            .field("rest_base_url", &self.rest_base_url)
            .field("public_ws_url", &self.public_ws_url)
            .field("private_ws_url", &self.private_ws_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("enabled_private_rest", &self.enabled_private_rest)
            .field("enabled_public_stream", &self.enabled_public_stream)
            .field("enabled_private_stream", &self.enabled_private_stream)
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
