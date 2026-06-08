#[derive(Debug, Clone)]
pub struct BtcMarketsGatewayConfig {
    pub enabled: bool,
    pub rest_base_url: String,
    pub ws_url: String,
    pub request_timeout_ms: u64,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
}

impl Default for BtcMarketsGatewayConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rest_base_url: "https://api.btcmarkets.net".to_string(),
            ws_url: "wss://socket.btcmarkets.net/v2".to_string(),
            request_timeout_ms: 10_000,
            api_key: None,
            api_secret: None,
            enabled_private_rest: true,
        }
    }
}

impl BtcMarketsGatewayConfig {
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
