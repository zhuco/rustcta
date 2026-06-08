#[derive(Debug, Clone)]
pub struct DeriveGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub wallet: Option<String>,
    pub subaccount_id: Option<String>,
    pub session_key: Option<String>,
    pub session_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for DeriveGatewayConfig {
    fn default() -> Self {
        let wallet =
            non_empty_env("RUSTCTA_DERIVE_WALLET").or_else(|| non_empty_env("DERIVE_WALLET"));
        let subaccount_id = non_empty_env("RUSTCTA_DERIVE_SUBACCOUNT_ID")
            .or_else(|| non_empty_env("DERIVE_SUBACCOUNT_ID"));
        let session_key = non_empty_env("RUSTCTA_DERIVE_SESSION_KEY")
            .or_else(|| non_empty_env("DERIVE_SESSION_KEY"));
        let session_secret = non_empty_env("RUSTCTA_DERIVE_SESSION_SECRET")
            .or_else(|| non_empty_env("DERIVE_SESSION_SECRET"));
        let has_private =
            subaccount_id.is_some() && session_key.is_some() && session_secret.is_some();
        Self {
            rest_base_url: "https://api.derive.xyz".to_string(),
            ws_url: "wss://api.derive.xyz/ws".to_string(),
            wallet,
            subaccount_id,
            session_key,
            session_secret,
            enabled_private_rest: env_bool("RUSTCTA_DERIVE_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("DERIVE_PRIVATE_REST_ENABLED"))
                .unwrap_or(has_private),
            enabled_private_streams: env_bool("RUSTCTA_DERIVE_PRIVATE_STREAMS_ENABLED")
                .or_else(|| env_bool("DERIVE_PRIVATE_STREAMS_ENABLED"))
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl DeriveGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && self.subaccount_id.as_deref().is_some_and(has_text)
            && self.session_key.as_deref().is_some_and(has_text)
            && self.session_secret.as_deref().is_some_and(has_text)
    }

    pub fn private_streams_available(&self) -> bool {
        self.enabled_private_streams && self.private_rest_available()
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

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}
