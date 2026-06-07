use std::collections::HashMap;

use anyhow::Result;
use rustcta_exchange_gateway::{
    AdapterBackedGateway, BinanceGatewayConfig, BitgetGatewayConfig, CoinExGatewayConfig,
    GateIoGatewayConfig, KuCoinGatewayConfig, MexcGatewayConfig, OkxGatewayConfig,
};

#[derive(Clone, PartialEq, Eq)]
pub struct GatewayAppConfig {
    pub bind_addr: String,
    pub adapters: Vec<String>,
    pub rest_base_urls: GatewayRestBaseUrls,
    pub private_credentials: GatewayPrivateCredentials,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayRestBaseUrls {
    urls: HashMap<String, String>,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct GatewayPrivateCredentials {
    credentials: HashMap<String, ExchangePrivateCredentials>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ExchangePrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
}

impl GatewayAppConfig {
    pub fn from_env() -> Self {
        Self::from_env_reader(|key| std::env::var(key).ok())
    }

    pub fn from_env_reader(mut env: impl FnMut(&str) -> Option<String>) -> Self {
        let bind_addr = env("RUSTCTA_GATEWAY_BIND")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "127.0.0.1:18081".to_string());
        let adapters = parse_adapters(env("RUSTCTA_GATEWAY_ADAPTERS").as_deref());
        let rest_base_urls = GatewayRestBaseUrls::from_env_reader(&mut env);
        let private_credentials = GatewayPrivateCredentials::from_env_reader(&mut env);
        Self {
            bind_addr,
            adapters,
            rest_base_urls,
            private_credentials,
        }
    }

    pub fn build_gateway(&self) -> Result<AdapterBackedGateway> {
        let gateway = AdapterBackedGateway::new("local-gateway");
        for adapter in &self.adapters {
            register_adapter(
                &gateway,
                adapter,
                &self.rest_base_urls,
                &self.private_credentials,
            )?;
        }
        Ok(gateway)
    }
}

impl GatewayRestBaseUrls {
    pub fn from_env_reader(mut env: impl FnMut(&str) -> Option<String>) -> Self {
        let mut urls = HashMap::new();
        for (adapter, key) in [
            ("binance", "RUSTCTA_BINANCE_REST_BASE_URL"),
            ("bitget", "RUSTCTA_BITGET_REST_BASE_URL"),
            ("coinex", "RUSTCTA_COINEX_REST_BASE_URL"),
            ("gateio", "RUSTCTA_GATEIO_REST_BASE_URL"),
            ("kucoin", "RUSTCTA_KUCOIN_REST_BASE_URL"),
            ("mexc", "RUSTCTA_MEXC_REST_BASE_URL"),
            ("okx", "RUSTCTA_OKX_REST_BASE_URL"),
        ] {
            if let Some(value) = env(key).filter(|value| !value.trim().is_empty()) {
                urls.insert(adapter.to_string(), value);
            }
        }
        Self { urls }
    }

    pub fn get(&self, adapter: &str) -> Option<String> {
        self.urls.get(adapter).cloned()
    }
}

impl GatewayPrivateCredentials {
    pub fn from_env_reader(mut env: impl FnMut(&str) -> Option<String>) -> Self {
        let mut credentials = HashMap::new();
        for spec in PRIVATE_CREDENTIAL_SPECS {
            let Some(api_key) = first_env(&mut env, spec.api_key_keys) else {
                continue;
            };
            let Some(api_secret) = first_env(&mut env, spec.api_secret_keys) else {
                continue;
            };
            let passphrase = match spec.passphrase_keys {
                Some(keys) => first_env(&mut env, keys),
                None => None,
            };
            if spec.requires_passphrase && passphrase.is_none() {
                continue;
            }
            credentials.insert(
                spec.adapter.to_string(),
                ExchangePrivateCredentials {
                    api_key,
                    api_secret,
                    passphrase,
                },
            );
        }
        Self { credentials }
    }

    pub fn get(&self, adapter: &str) -> Option<&ExchangePrivateCredentials> {
        self.credentials.get(adapter)
    }

    pub fn contains(&self, adapter: &str) -> bool {
        self.credentials.contains_key(adapter)
    }
}

impl std::fmt::Debug for GatewayAppConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayAppConfig")
            .field("bind_addr", &self.bind_addr)
            .field("adapters", &self.adapters)
            .field("rest_base_urls", &self.rest_base_urls)
            .field("private_credentials", &self.private_credentials)
            .finish()
    }
}

impl std::fmt::Debug for GatewayPrivateCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = self
            .credentials
            .keys()
            .map(|adapter| (adapter.as_str(), "<redacted>"))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("GatewayPrivateCredentials")
            .field("credentials", &redacted)
            .finish()
    }
}

impl std::fmt::Debug for ExchangePrivateCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExchangePrivateCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field(
                "passphrase",
                &self.passphrase.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

fn parse_adapters(value: Option<&str>) -> Vec<String> {
    let adapters = value
        .unwrap_or("paper")
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if adapters.is_empty() {
        vec!["paper".to_string()]
    } else {
        adapters
    }
}

fn register_adapter(
    gateway: &AdapterBackedGateway,
    adapter: &str,
    rest_base_urls: &GatewayRestBaseUrls,
    private_credentials: &GatewayPrivateCredentials,
) -> Result<()> {
    match adapter.trim().to_ascii_lowercase().as_str() {
        "binance" => gateway.register_binance_adapter(binance_config(
            rest_base_urls.get("binance"),
            private_credentials.get("binance"),
        ))?,
        "bitget" => gateway.register_bitget_adapter(bitget_config(
            rest_base_urls.get("bitget"),
            private_credentials.get("bitget"),
        ))?,
        "coinex" => gateway.register_coinex_adapter(coinex_config(
            rest_base_urls.get("coinex"),
            private_credentials.get("coinex"),
        ))?,
        "gate" | "gate.io" | "gateio" => gateway.register_gateio_adapter(gateio_config(
            rest_base_urls.get("gateio"),
            private_credentials.get("gateio"),
        ))?,
        "kucoin" => gateway.register_kucoin_adapter(kucoin_config(
            rest_base_urls.get("kucoin"),
            private_credentials.get("kucoin"),
        ))?,
        "mexc" => gateway.register_mexc_adapter(mexc_config(
            rest_base_urls.get("mexc"),
            private_credentials.get("mexc"),
        ))?,
        "okx" => gateway.register_okx_adapter(okx_config(
            rest_base_urls.get("okx"),
            private_credentials.get("okx"),
        ))?,
        other => gateway.register_named_adapter(other)?,
    }
    Ok(())
}

fn binance_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BinanceGatewayConfig {
    let mut config = BinanceGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn bitget_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitgetGatewayConfig {
    let mut config = BitgetGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.passphrase = credentials.passphrase.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn coinex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinExGatewayConfig {
    let mut config = CoinExGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn gateio_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> GateIoGatewayConfig {
    let mut config = GateIoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn kucoin_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> KuCoinGatewayConfig {
    let mut config = KuCoinGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.api_passphrase = credentials.passphrase.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn mexc_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> MexcGatewayConfig {
    let mut config = MexcGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn okx_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> OkxGatewayConfig {
    let mut config = OkxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.passphrase = credentials.passphrase.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn apply_rest_base_url(target: &mut String, rest_base_url: Option<String>) {
    if let Some(rest_base_url) = rest_base_url {
        *target = rest_base_url;
    }
}

fn first_env(env: &mut impl FnMut(&str) -> Option<String>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| env(key))
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

struct PrivateCredentialSpec {
    adapter: &'static str,
    api_key_keys: &'static [&'static str],
    api_secret_keys: &'static [&'static str],
    passphrase_keys: Option<&'static [&'static str]>,
    requires_passphrase: bool,
}

const PRIVATE_CREDENTIAL_SPECS: &[PrivateCredentialSpec] = &[
    PrivateCredentialSpec {
        adapter: "binance",
        api_key_keys: &[
            "RUSTCTA_BINANCE_API_KEY",
            "BINANCE_SPOT_API_KEY",
            "BINANCE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BINANCE_API_SECRET",
            "BINANCE_SPOT_API_SECRET",
            "BINANCE_API_SECRET",
        ],
        passphrase_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitget",
        api_key_keys: &[
            "RUSTCTA_BITGET_API_KEY",
            "BITGET_API_KEY",
            "BITGET_SPOT_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BITGET_API_SECRET",
            "BITGET_API_SECRET",
            "BITGET_SPOT_API_SECRET",
        ],
        passphrase_keys: Some(&[
            "RUSTCTA_BITGET_API_PASSPHRASE",
            "BITGET_PASSPHRASE",
            "BITGET_API_PASSPHRASE",
            "BITGET_SPOT_PASSPHRASE",
            "BITGET_SPOT_API_PASSPHRASE",
        ]),
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "coinex",
        api_key_keys: &["RUSTCTA_COINEX_API_KEY", "COINEX_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINEX_API_SECRET", "COINEX_API_SECRET"],
        passphrase_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "gateio",
        api_key_keys: &["RUSTCTA_GATEIO_API_KEY", "GATEIO_API_KEY", "GATE_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_GATEIO_API_SECRET",
            "GATEIO_API_SECRET",
            "GATE_API_SECRET",
        ],
        passphrase_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "kucoin",
        api_key_keys: &[
            "RUSTCTA_KUCOIN_API_KEY",
            "KUCOIN_SPOT_API_KEY",
            "KUCOIN_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_KUCOIN_API_SECRET",
            "KUCOIN_SPOT_API_SECRET",
            "KUCOIN_API_SECRET",
        ],
        passphrase_keys: Some(&[
            "RUSTCTA_KUCOIN_API_PASSPHRASE",
            "KUCOIN_SPOT_API_PASSPHRASE",
            "KUCOIN_API_PASSPHRASE",
        ]),
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "mexc",
        api_key_keys: &["RUSTCTA_MEXC_API_KEY", "MEXC_SPOT_API_KEY", "MEXC_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_MEXC_API_SECRET",
            "MEXC_SPOT_API_SECRET",
            "MEXC_API_SECRET",
        ],
        passphrase_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "okx",
        api_key_keys: &["RUSTCTA_OKX_API_KEY", "OKX_API_KEY", "OKX_SPOT_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_OKX_API_SECRET",
            "OKX_API_SECRET",
            "OKX_SPOT_API_SECRET",
        ],
        passphrase_keys: Some(&[
            "RUSTCTA_OKX_API_PASSPHRASE",
            "OKX_PASSPHRASE",
            "OKX_SPOT_PASSPHRASE",
        ]),
        requires_passphrase: true,
    },
];

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use rustcta_exchange_api::{
        AccountId, ExchangeId, RequestContext, TenantId, EXCHANGE_API_SCHEMA_VERSION,
    };
    use rustcta_exchange_gateway::{
        gateway_router, GatewayClient, GetCapabilitiesRequest, InProcessGatewayClient,
        LocalGateway, GATEWAY_PROTOCOL_SCHEMA_VERSION,
    };
    use tower::ServiceExt;

    use super::*;

    fn env_from<'a>(values: &'a [(&'a str, &'a str)]) -> impl FnMut(&str) -> Option<String> + 'a {
        move |key| {
            values
                .iter()
                .find(|(candidate, _)| *candidate == key)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn config_should_default_to_local_paper_gateway() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[]));
        assert_eq!(config.bind_addr, "127.0.0.1:18081");
        assert_eq!(config.adapters, vec!["paper"]);
        assert!(config.rest_base_urls.get("binance").is_none());
        assert!(!config.private_credentials.contains("binance"));
    }

    #[test]
    fn config_should_parse_adapters_and_redirection_urls_without_secret_fields() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_BIND", "127.0.0.1:0"),
            ("RUSTCTA_GATEWAY_ADAPTERS", " paper, BINANCE, gate.io "),
            ("RUSTCTA_BINANCE_REST_BASE_URL", "http://127.0.0.1:9001"),
            ("RUSTCTA_GATEIO_REST_BASE_URL", "http://127.0.0.1:9002"),
            ("RUSTCTA_BINANCE_API_KEY", "binance-key"),
            ("RUSTCTA_BINANCE_API_SECRET", "binance-secret"),
            ("RUSTCTA_OKX_API_KEY", "okx-key"),
            ("RUSTCTA_OKX_API_SECRET", "okx-secret"),
            ("RUSTCTA_OKX_API_PASSPHRASE", "okx-passphrase"),
        ]));

        assert_eq!(config.bind_addr, "127.0.0.1:0");
        assert_eq!(config.adapters, vec!["paper", "binance", "gate.io"]);
        assert_eq!(
            config.rest_base_urls.get("binance").as_deref(),
            Some("http://127.0.0.1:9001")
        );
        assert_eq!(
            config.rest_base_urls.get("gateio").as_deref(),
            Some("http://127.0.0.1:9002")
        );
        assert!(config.private_credentials.contains("binance"));
        assert!(config.private_credentials.contains("okx"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("binance-secret"));
        assert!(!debug.contains("okx-passphrase"));
    }

    #[tokio::test]
    async fn config_should_build_registered_gateway() {
        let config =
            GatewayAppConfig::from_env_reader(env_from(&[("RUSTCTA_GATEWAY_ADAPTERS", "paper")]));
        let gateway = config.build_gateway().expect("gateway");
        assert_eq!(gateway.status().await.expect("status").exchanges.len(), 1);
    }

    #[tokio::test]
    async fn config_built_gateway_should_serve_health_and_status_routes() {
        let config =
            GatewayAppConfig::from_env_reader(env_from(&[("RUSTCTA_GATEWAY_ADAPTERS", "paper")]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let app = gateway_router(gateway);

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);

        let status = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .expect("status request"),
            )
            .await
            .expect("status response");
        assert_eq!(status.status(), StatusCode::OK);
        let body = to_bytes(status.into_body(), usize::MAX)
            .await
            .expect("status body");
        let status: serde_json::Value = serde_json::from_slice(&body).expect("status json");
        assert_eq!(
            status["identity"]["credential_boundary"], "GatewayOnly",
            "gateway app must serve gateway-owned credential boundary status"
        );
        assert_eq!(status["exchanges"].as_array().expect("exchanges").len(), 1);
    }

    #[tokio::test]
    async fn config_should_load_private_credentials_only_into_gateway_adapters() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "binance"),
            ("RUSTCTA_BINANCE_API_KEY", "binance-key"),
            ("RUSTCTA_BINANCE_API_SECRET", "binance-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("binance").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.schema_version, GATEWAY_PROTOCOL_SCHEMA_VERSION);
        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_balances);
    }
}
