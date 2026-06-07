use std::fmt;

#[derive(Clone)]
pub struct DeepcoinGatewayConfig {
    pub rest_base_url: String,
    pub public_spot_ws_url: String,
    pub public_swap_ws_url: String,
    pub private_ws_url: String,
    pub request_timeout_ms: u64,
    pub enabled: bool,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub enabled_private_rest: bool,
}

impl Default for DeepcoinGatewayConfig {
    fn default() -> Self {
        let api_key = env_first(["DEEPCOIN_API_KEY"]);
        let api_secret = env_first(["DEEPCOIN_API_SECRET"]);
        let passphrase = env_first(["DEEPCOIN_PASSPHRASE"]);
        let enabled_private_rest =
            api_key.is_some() && api_secret.is_some() && passphrase.is_some();
        Self {
            rest_base_url: "https://api.deepcoin.com".to_string(),
            public_spot_ws_url: env_first(["DEEPCOIN_PUBLIC_SPOT_WS_URL"]).unwrap_or_else(|| {
                "wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api&version=v2"
                    .to_string()
            }),
            public_swap_ws_url: env_first(["DEEPCOIN_PUBLIC_SWAP_WS_URL"]).unwrap_or_else(|| {
                "wss://stream.deepcoin.com/streamlet/trade/public/swap?platform=api&version=v2"
                    .to_string()
            }),
            private_ws_url: env_first(["DEEPCOIN_PRIVATE_WS_URL"])
                .unwrap_or_else(|| "wss://stream.deepcoin.com/v1/private".to_string()),
            request_timeout_ms: 10_000,
            enabled: true,
            api_key,
            api_secret,
            passphrase,
            enabled_private_rest,
        }
    }
}

impl DeepcoinGatewayConfig {
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

    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }
}

impl fmt::Debug for DeepcoinGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeepcoinGatewayConfig")
            .field("rest_base_url", &self.rest_base_url)
            .field("public_spot_ws_url", &self.public_spot_ws_url)
            .field("public_swap_ws_url", &self.public_swap_ws_url)
            .field("private_ws_url", &self.private_ws_url)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("enabled", &self.enabled)
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
            .finish()
    }
}

fn env_first<const N: usize>(keys: [&str; N]) -> Option<String> {
    keys.into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
