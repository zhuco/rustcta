use std::fmt;

#[derive(Clone)]
pub struct DeribitGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_stream: bool,
    pub enabled_private_stream: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for DeribitGatewayConfig {
    fn default() -> Self {
        let client_id = env_first(["DERIBIT_CLIENT_ID", "DERIBIT_API_KEY"]);
        let client_secret = env_first(["DERIBIT_CLIENT_SECRET", "DERIBIT_API_SECRET"]);
        let enabled_private = client_id.is_some() && client_secret.is_some();
        Self {
            rest_base_url: "https://www.deribit.com".to_string(),
            public_ws_url: "wss://www.deribit.com/ws/api/v2".to_string(),
            private_ws_url: "wss://www.deribit.com/ws/api/v2".to_string(),
            client_id,
            client_secret,
            enabled_private_rest: enabled_private,
            enabled_public_stream: true,
            enabled_private_stream: enabled_private,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl DeribitGatewayConfig {
    pub fn has_private_credentials(&self) -> bool {
        self.client_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .client_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }

    pub fn private_stream_available(&self) -> bool {
        self.enabled_private_stream && self.has_private_credentials()
    }
}

impl fmt::Debug for DeribitGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeribitGatewayConfig")
            .field("rest_base_url", &self.rest_base_url)
            .field("public_ws_url", &self.public_ws_url)
            .field("private_ws_url", &self.private_ws_url)
            .field("client_id", &self.client_id.as_ref().map(|_| "<redacted>"))
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
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
