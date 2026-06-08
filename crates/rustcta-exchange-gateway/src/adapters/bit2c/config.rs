#[derive(Debug, Clone)]
pub struct Bit2cGatewayConfig {
    pub rest_base_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for Bit2cGatewayConfig {
    fn default() -> Self {
        let api_key =
            non_empty_env("BIT2C_API_KEY").or_else(|| non_empty_env("RUSTCTA_BIT2C_API_KEY"));
        let api_secret =
            non_empty_env("BIT2C_API_SECRET").or_else(|| non_empty_env("RUSTCTA_BIT2C_API_SECRET"));
        Self {
            rest_base_url: non_empty_env("RUSTCTA_BIT2C_REST_BASE_URL")
                .unwrap_or_else(|| "https://bit2c.co.il".to_string()),
            api_key,
            api_secret,
            enabled_private_rest: false,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl Bit2cGatewayConfig {
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
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
