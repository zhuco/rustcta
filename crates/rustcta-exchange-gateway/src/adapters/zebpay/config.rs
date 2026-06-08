#[derive(Debug, Clone)]
pub struct ZebpayGatewayConfig {
    pub rest_base_url: String,
    pub group: String,
    pub client_id: String,
    pub client_secret: String,
    pub access_token: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for ZebpayGatewayConfig {
    fn default() -> Self {
        let client_id = non_empty_env("ZEBPAY_CLIENT_ID")
            .or_else(|| non_empty_env("ZEBPAY_API_KEY"))
            .or_else(|| non_empty_env("RUSTCTA_ZEBPAY_CLIENT_ID"));
        let client_secret = non_empty_env("ZEBPAY_CLIENT_SECRET")
            .or_else(|| non_empty_env("ZEBPAY_API_SECRET"))
            .or_else(|| non_empty_env("RUSTCTA_ZEBPAY_CLIENT_SECRET"));
        let access_token = non_empty_env("ZEBPAY_ACCESS_TOKEN")
            .or_else(|| non_empty_env("RUSTCTA_ZEBPAY_ACCESS_TOKEN"));
        Self {
            rest_base_url: "https://www.zebapi.com".to_string(),
            group: non_empty_env("ZEBPAY_GROUP")
                .or_else(|| non_empty_env("RUSTCTA_ZEBPAY_GROUP"))
                .unwrap_or_else(|| "singapore".to_string()),
            client_id: client_id.unwrap_or_default(),
            client_secret: client_secret.unwrap_or_default(),
            access_token: access_token.unwrap_or_default(),
            enabled_private_rest: env_bool("ZEBPAY_PRIVATE_REST_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl ZebpayGatewayConfig {
    pub fn private_rest_configured(&self) -> bool {
        self.enabled_private_rest
            && !self.client_id.trim().is_empty()
            && !self.client_secret.trim().is_empty()
            && !self.access_token.trim().is_empty()
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
