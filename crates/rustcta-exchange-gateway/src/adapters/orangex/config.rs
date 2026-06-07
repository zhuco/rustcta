#[derive(Debug, Clone)]
pub struct OrangeXGatewayConfig {
    pub rest_base_url: String,
    pub ws_base_url: String,
    pub client_id: Option<String>,
    pub access_token: Option<String>,
    pub client_secret: Option<String>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for OrangeXGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: non_empty_env("ORANGEX_REST_BASE_URL")
                .unwrap_or_else(|| "https://api.orangex.com/api/v1".to_string()),
            ws_base_url: non_empty_env("ORANGEX_WS_BASE_URL")
                .unwrap_or_else(|| "wss://api.orangex.com/ws/api/v1".to_string()),
            client_id: non_empty_env("ORANGEX_CLIENT_ID"),
            access_token: non_empty_env("ORANGEX_ACCESS_TOKEN"),
            client_secret: non_empty_env("ORANGEX_CLIENT_SECRET"),
            api_key: non_empty_env("ORANGEX_API_KEY"),
            api_secret: non_empty_env("ORANGEX_API_SECRET"),
            enabled_private_rest: env_bool("ORANGEX_PRIVATE_REST_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl OrangeXGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        if !self.enabled_private_rest {
            return false;
        }
        let has_token = self
            .access_token
            .as_ref()
            .is_some_and(|token| !token.trim().is_empty());
        let has_auth_credentials = self
            .client_id_value()
            .is_some_and(|client_id| !client_id.trim().is_empty())
            && self
                .client_secret_value()
                .is_some_and(|secret| !secret.trim().is_empty());
        let has_legacy_token = self
            .api_key
            .as_ref()
            .is_some_and(|token| !token.trim().is_empty())
            && !self
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty())
            && !self
                .client_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty());
        has_token || has_auth_credentials || has_legacy_token
    }

    pub fn client_id_value(&self) -> Option<&str> {
        self.client_id
            .as_deref()
            .or(self.api_key.as_deref())
            .filter(|client_id| !client_id.trim().is_empty())
    }

    pub fn client_secret_value(&self) -> Option<&str> {
        self.client_secret
            .as_deref()
            .or(self.api_secret.as_deref())
            .filter(|secret| !secret.trim().is_empty())
    }

    pub fn access_token_value(&self) -> Option<&str> {
        self.access_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .or_else(|| {
                if self.client_secret_value().is_none() {
                    self.api_key
                        .as_deref()
                        .filter(|token| !token.trim().is_empty())
                } else {
                    None
                }
            })
    }

    pub fn client_secret_available(&self) -> bool {
        self.client_secret_value().is_some()
    }

    pub fn api_credentials_available(&self) -> bool {
        self.client_id_value().is_some()
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
