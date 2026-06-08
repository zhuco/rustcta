#[derive(Debug, Clone)]
pub struct OkxusGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for OkxusGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://us.okx.com".to_string(),
            public_ws_url: "wss://wsus.okx.com:8443/ws/v5/public".to_string(),
            private_ws_url: "wss://wsus.okx.com:8443/ws/v5/private".to_string(),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}
