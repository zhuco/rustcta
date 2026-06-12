#[derive(Debug, Clone)]
pub struct MexcGatewayConfig {
    pub rest_base_url: String,
    pub contract_rest_base_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

pub(super) const MEXC_SPOT_REST_BASE_URL: &str = "https://api.mexc.com";
pub(super) const MEXC_CONTRACT_REST_BASE_URL: &str = "https://api.mexc.com";

impl Default for MexcGatewayConfig {
    fn default() -> Self {
        Self::from_env_lookup(|key| std::env::var(key).ok())
    }
}

impl MexcGatewayConfig {
    pub(super) fn from_env_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        Self {
            rest_base_url: first_non_empty_lookup(
                &mut lookup,
                &["MEXC_SPOT_REST_BASE_URL", "MEXC_REST_BASE_URL"],
            )
            .unwrap_or_else(|| MEXC_SPOT_REST_BASE_URL.to_string()),
            contract_rest_base_url: first_non_empty_lookup(
                &mut lookup,
                &["MEXC_CONTRACT_REST_BASE_URL", "MEXC_FUTURES_REST_BASE_URL"],
            )
            .unwrap_or_else(|| MEXC_CONTRACT_REST_BASE_URL.to_string()),
            api_key: first_non_empty_lookup(
                &mut lookup,
                &[
                    "MEXC_CONTRACT_API_KEY",
                    "MEXC_FUTURES_API_KEY",
                    "MEXC_SPOT_API_KEY",
                    "MEXC_API_KEY",
                ],
            ),
            api_secret: first_non_empty_lookup(
                &mut lookup,
                &[
                    "MEXC_CONTRACT_API_SECRET",
                    "MEXC_FUTURES_API_SECRET",
                    "MEXC_SPOT_API_SECRET",
                    "MEXC_API_SECRET",
                ],
            ),
            recv_window_ms: env_u64_lookup(
                &mut lookup,
                &[
                    "MEXC_CONTRACT_RECV_WINDOW_MS",
                    "MEXC_FUTURES_RECV_WINDOW_MS",
                    "MEXC_SPOT_RECV_WINDOW_MS",
                ],
            )
            .unwrap_or(5_000),
            enabled_private_rest: env_bool_lookup(
                &mut lookup,
                &[
                    "MEXC_CONTRACT_PRIVATE_REST_ENABLED",
                    "MEXC_FUTURES_PRIVATE_REST_ENABLED",
                    "MEXC_SPOT_PRIVATE_REST_ENABLED",
                ],
            )
            .unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }

    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|key| !key.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty())
    }
}

fn first_non_empty_lookup(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    keys: &[&str],
) -> Option<String> {
    keys.iter().find_map(|key| {
        lookup(key)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn env_u64_lookup(lookup: &mut impl FnMut(&str) -> Option<String>, keys: &[&str]) -> Option<u64> {
    first_non_empty_lookup(lookup, keys)?.parse().ok()
}

fn env_bool_lookup(lookup: &mut impl FnMut(&str) -> Option<String>, keys: &[&str]) -> Option<bool> {
    match first_non_empty_lookup(lookup, keys)?
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
