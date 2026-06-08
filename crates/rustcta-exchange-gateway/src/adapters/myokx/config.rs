#[derive(Debug, Clone)]
pub struct MyOkxGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub business_ws_url: String,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for MyOkxGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: env_first(["RUSTCTA_MYOKX_REST_BASE_URL", "MYOKX_REST_BASE_URL"])
                .unwrap_or_else(|| "https://eea.okx.com".to_string()),
            public_ws_url: env_first(["RUSTCTA_MYOKX_PUBLIC_WS_URL", "MYOKX_PUBLIC_WS_URL"])
                .unwrap_or_else(|| "wss://wseea.okx.com:8443/ws/v5/public".to_string()),
            private_ws_url: env_first(["RUSTCTA_MYOKX_PRIVATE_WS_URL", "MYOKX_PRIVATE_WS_URL"])
                .unwrap_or_else(|| "wss://wseea.okx.com:8443/ws/v5/private".to_string()),
            business_ws_url: env_first(["RUSTCTA_MYOKX_BUSINESS_WS_URL", "MYOKX_BUSINESS_WS_URL"])
                .unwrap_or_else(|| "wss://wseea.okx.com:8443/ws/v5/business".to_string()),
            request_timeout_ms: env_first([
                "RUSTCTA_MYOKX_REQUEST_TIMEOUT_MS",
                "MYOKX_REQUEST_TIMEOUT_MS",
            ])
            .and_then(|value| value.parse().ok())
            .unwrap_or(10_000),
            enabled: env_bool("RUSTCTA_MYOKX_ENABLED")
                .or_else(|| env_bool("MYOKX_ENABLED"))
                .unwrap_or(true),
        }
    }
}

impl MyOkxGatewayConfig {
    pub fn into_okx_config(self) -> super::super::okx::OkxGatewayConfig {
        super::super::okx::OkxGatewayConfig {
            exchange_id: "myokx".to_string(),
            rest_base_url: self.rest_base_url,
            request_timeout_ms: self.request_timeout_ms,
            enabled: self.enabled,
            api_key: None,
            api_secret: None,
            passphrase: None,
            enabled_private_rest: false,
            status_message: "myokx EEA profile over OKX Spot public REST; private trading disabled"
                .to_string(),
            unsupported_market_type_operation: "myokx.non_spot_market_type",
        }
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
