const DEFAULT_SPOT_REST_BASE_URL: &str = "https://api.huobi.pro";
const DEFAULT_LINEAR_REST_BASE_URL: &str = "https://api.hbdm.com";
const DEFAULT_SPOT_PUBLIC_WS_URL: &str = "wss://api.huobi.pro/ws";
const DEFAULT_SPOT_PRIVATE_WS_URL: &str = "wss://api.huobi.pro/ws/v2";
const DEFAULT_LINEAR_PUBLIC_WS_URL: &str = "wss://api.hbdm.com/linear-swap-ws";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://api.hbdm.com/linear-swap-notification";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HtxGatewayProfile {
    Htx,
    HuobiLegacy,
}

impl HtxGatewayProfile {
    pub fn adapter_id(self) -> &'static str {
        match self {
            Self::Htx => "htx",
            Self::HuobiLegacy => "huobi",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Htx => "HTX",
            Self::HuobiLegacy => "Huobi legacy",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HtxGatewayConfig {
    pub profile: HtxGatewayProfile,
    pub spot_rest_base_url: String,
    pub linear_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub spot_private_ws_url: String,
    pub linear_public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub spot_account_id: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for HtxGatewayConfig {
    fn default() -> Self {
        Self::for_profile(HtxGatewayProfile::Htx)
    }
}

impl HtxGatewayConfig {
    pub fn for_profile(profile: HtxGatewayProfile) -> Self {
        let env_prefix = match profile {
            HtxGatewayProfile::Htx => "HTX",
            HtxGatewayProfile::HuobiLegacy => "HUOBI",
        };
        let api_key = first_env(&[
            &format!("{env_prefix}_API_KEY"),
            "HTX_API_KEY",
            "HUOBI_API_KEY",
        ]);
        let api_secret = first_env(&[
            &format!("{env_prefix}_API_SECRET"),
            "HTX_API_SECRET",
            "HUOBI_API_SECRET",
        ]);
        let spot_account_id = first_env(&[
            &format!("{env_prefix}_SPOT_ACCOUNT_ID"),
            "HTX_SPOT_ACCOUNT_ID",
            "HUOBI_SPOT_ACCOUNT_ID",
        ]);
        let has_credentials = api_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());
        let enabled_private_rest = std::env::var(format!("{env_prefix}_PRIVATE_REST_ENABLED"))
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            profile,
            spot_rest_base_url: DEFAULT_SPOT_REST_BASE_URL.to_string(),
            linear_rest_base_url: DEFAULT_LINEAR_REST_BASE_URL.to_string(),
            spot_public_ws_url: DEFAULT_SPOT_PUBLIC_WS_URL.to_string(),
            spot_private_ws_url: DEFAULT_SPOT_PRIVATE_WS_URL.to_string(),
            linear_public_ws_url: DEFAULT_LINEAR_PUBLIC_WS_URL.to_string(),
            private_ws_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            spot_account_id,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }

    pub fn huobi_legacy() -> Self {
        Self::for_profile(HtxGatewayProfile::HuobiLegacy)
    }

    pub fn set_huobi_legacy_profile(&mut self) {
        self.profile = HtxGatewayProfile::HuobiLegacy;
    }

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

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
