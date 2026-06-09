#[derive(Debug, Clone)]
pub struct MyOkxGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub business_ws_url: String,
    pub request_timeout_ms: u64,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub enabled_private_rest: bool,
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
            api_key: env_first(["RUSTCTA_MYOKX_API_KEY", "MYOKX_API_KEY"]),
            api_secret: env_first(["RUSTCTA_MYOKX_API_SECRET", "MYOKX_API_SECRET"]),
            passphrase: env_first([
                "RUSTCTA_MYOKX_PASSPHRASE",
                "MYOKX_PASSPHRASE",
                "RUSTCTA_MYOKX_API_PASSPHRASE",
                "MYOKX_API_PASSPHRASE",
            ]),
            enabled_private_rest: env_bool("RUSTCTA_MYOKX_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("MYOKX_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
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
            public_ws_url: self.public_ws_url,
            request_timeout_ms: self.request_timeout_ms,
            enabled: self.enabled,
            api_key: self.api_key,
            api_secret: self.api_secret,
            passphrase: self.passphrase,
            enabled_private_rest: self.enabled_private_rest,
            enabled_public_streams: true,
            status_message: "myokx EEA profile over OKX V5 Spot; private REST reuses OKX runtime when regional credentials and explicit enable flag are configured"
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
