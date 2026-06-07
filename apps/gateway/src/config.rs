use std::collections::HashMap;

use anyhow::Result;
use rustcta_exchange_gateway::{
    AdapterBackedGateway, AscendexGatewayConfig, BackpackGatewayConfig, BiconomyGatewayConfig,
    BigOneGatewayConfig, BinanceGatewayConfig, BingxGatewayConfig, BitgetGatewayConfig,
    BitkanGatewayConfig, BitunixGatewayConfig, BlofinGatewayConfig, CoinDcxGatewayConfig,
    CoinExGatewayConfig, CoinstoreGatewayConfig, CointrGatewayConfig, CryptoComGatewayConfig,
    DeepcoinGatewayConfig, GateIoGatewayConfig, HashKeyGlobalGatewayConfig, KrakenGatewayConfig,
    KuCoinGatewayConfig, LBankGatewayConfig, MexcGatewayConfig, OkxGatewayConfig,
    OrangeXGatewayConfig, PhemexGatewayConfig, WeexGatewayConfig,
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
    pub account_group: Option<String>,
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
            ("ascendex", "RUSTCTA_ASCENDEX_REST_BASE_URL"),
            ("backpack", "RUSTCTA_BACKPACK_REST_BASE_URL"),
            ("biconomy", "RUSTCTA_BICONOMY_REST_BASE_URL"),
            ("binance", "RUSTCTA_BINANCE_REST_BASE_URL"),
            ("bigone", "RUSTCTA_BIGONE_REST_BASE_URL"),
            ("bigone_spot", "RUSTCTA_BIGONE_SPOT_REST_BASE_URL"),
            ("bigone_contract", "RUSTCTA_BIGONE_CONTRACT_REST_BASE_URL"),
            ("bingx", "RUSTCTA_BINGX_REST_BASE_URL"),
            ("bitget", "RUSTCTA_BITGET_REST_BASE_URL"),
            ("bitkan", "RUSTCTA_BITKAN_REST_BASE_URL"),
            ("bitunix", "RUSTCTA_BITUNIX_REST_BASE_URL"),
            ("bitunix_spot", "RUSTCTA_BITUNIX_SPOT_REST_BASE_URL"),
            ("bitunix_futures", "RUSTCTA_BITUNIX_FUTURES_REST_BASE_URL"),
            ("blofin", "RUSTCTA_BLOFIN_REST_BASE_URL"),
            ("coindcx", "RUSTCTA_COINDCX_REST_BASE_URL"),
            ("coindcx_spot", "RUSTCTA_COINDCX_SPOT_REST_BASE_URL"),
            ("coindcx_futures", "RUSTCTA_COINDCX_FUTURES_REST_BASE_URL"),
            ("coindcx_public", "RUSTCTA_COINDCX_PUBLIC_REST_BASE_URL"),
            ("coinex", "RUSTCTA_COINEX_REST_BASE_URL"),
            ("coinstore", "RUSTCTA_COINSTORE_REST_BASE_URL"),
            ("coinstore_spot", "RUSTCTA_COINSTORE_SPOT_REST_BASE_URL"),
            (
                "coinstore_futures",
                "RUSTCTA_COINSTORE_FUTURES_REST_BASE_URL",
            ),
            ("cointr", "RUSTCTA_COINTR_REST_BASE_URL"),
            ("cryptocom", "RUSTCTA_CRYPTOCOM_REST_BASE_URL"),
            ("deepcoin", "RUSTCTA_DEEPCOIN_REST_BASE_URL"),
            ("gateio", "RUSTCTA_GATEIO_REST_BASE_URL"),
            ("hashkey_global", "RUSTCTA_HASHKEY_GLOBAL_REST_BASE_URL"),
            (
                "hashkey_global_spot",
                "RUSTCTA_HASHKEY_GLOBAL_SPOT_REST_BASE_URL",
            ),
            (
                "hashkey_global_futures",
                "RUSTCTA_HASHKEY_GLOBAL_FUTURES_REST_BASE_URL",
            ),
            ("kucoin", "RUSTCTA_KUCOIN_REST_BASE_URL"),
            ("kraken", "RUSTCTA_KRAKEN_REST_BASE_URL"),
            ("kraken_spot", "RUSTCTA_KRAKEN_SPOT_REST_BASE_URL"),
            ("kraken_futures", "RUSTCTA_KRAKEN_FUTURES_REST_BASE_URL"),
            ("lbank", "RUSTCTA_LBANK_REST_BASE_URL"),
            ("lbank_spot", "RUSTCTA_LBANK_SPOT_REST_BASE_URL"),
            ("lbank_contract", "RUSTCTA_LBANK_CONTRACT_REST_BASE_URL"),
            ("mexc", "RUSTCTA_MEXC_REST_BASE_URL"),
            ("okx", "RUSTCTA_OKX_REST_BASE_URL"),
            ("orangex", "RUSTCTA_ORANGEX_REST_BASE_URL"),
            ("phemex", "RUSTCTA_PHEMEX_REST_BASE_URL"),
            ("weex", "RUSTCTA_WEEX_REST_BASE_URL"),
            ("weex_spot", "RUSTCTA_WEEX_SPOT_REST_BASE_URL"),
            ("weex_contract", "RUSTCTA_WEEX_CONTRACT_REST_BASE_URL"),
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
            let passphrase = match spec.passphrase_keys {
                Some(keys) => first_env(&mut env, keys),
                None => None,
            };
            let token_only_credentials = spec.adapter == "orangex"
                && passphrase.as_ref().is_some_and(|value| !value.is_empty());
            let api_key = match first_env(&mut env, spec.api_key_keys) {
                Some(api_key) => api_key,
                None if token_only_credentials => String::new(),
                None => continue,
            };
            let api_secret = match first_env(&mut env, spec.api_secret_keys) {
                Some(api_secret) => api_secret,
                None if token_only_credentials => String::new(),
                None => continue,
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
                    account_group: match spec.account_group_keys {
                        Some(keys) => first_env(&mut env, keys),
                        None => None,
                    },
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
        "ascendex" | "ascend_ex" => gateway.register_ascendex_adapter(ascendex_config(
            rest_base_urls.get("ascendex"),
            private_credentials.get("ascendex"),
        ))?,
        "backpack" => gateway.register_backpack_adapter(backpack_config(
            rest_base_urls.get("backpack"),
            private_credentials.get("backpack"),
        ))?,
        "biconomy" => gateway.register_biconomy_adapter(biconomy_config(
            rest_base_urls.get("biconomy"),
            private_credentials.get("biconomy"),
        ))?,
        "binance" => gateway.register_binance_adapter(binance_config(
            rest_base_urls.get("binance"),
            private_credentials.get("binance"),
        ))?,
        "bigone" | "big_one" => gateway.register_bigone_adapter(bigone_config(
            rest_base_urls.get("bigone"),
            rest_base_urls.get("bigone_spot"),
            rest_base_urls.get("bigone_contract"),
            private_credentials.get("bigone"),
        ))?,
        "bingx" => gateway.register_bingx_adapter(bingx_config(
            rest_base_urls.get("bingx"),
            private_credentials.get("bingx"),
        ))?,
        "bitget" => gateway.register_bitget_adapter(bitget_config(
            rest_base_urls.get("bitget"),
            private_credentials.get("bitget"),
        ))?,
        "bitkan" => gateway.register_bitkan_adapter(bitkan_config(
            rest_base_urls.get("bitkan"),
            private_credentials.get("bitkan"),
        ))?,
        "bitunix" => gateway.register_bitunix_adapter(bitunix_config(
            rest_base_urls.get("bitunix"),
            rest_base_urls.get("bitunix_spot"),
            rest_base_urls.get("bitunix_futures"),
            private_credentials.get("bitunix"),
        ))?,
        "blofin" | "blo_fin" => gateway.register_blofin_adapter(blofin_config(
            rest_base_urls.get("blofin"),
            private_credentials.get("blofin"),
        ))?,
        "coindcx" | "coin_dcx" => gateway.register_coindcx_adapter(coindcx_config(
            rest_base_urls.get("coindcx"),
            rest_base_urls.get("coindcx_spot"),
            rest_base_urls.get("coindcx_futures"),
            rest_base_urls.get("coindcx_public"),
            private_credentials.get("coindcx"),
        ))?,
        "coinex" => gateway.register_coinex_adapter(coinex_config(
            rest_base_urls.get("coinex"),
            private_credentials.get("coinex"),
        ))?,
        "coinstore" => gateway.register_coinstore_adapter(coinstore_config(
            rest_base_urls.get("coinstore"),
            rest_base_urls.get("coinstore_spot"),
            rest_base_urls.get("coinstore_futures"),
            private_credentials.get("coinstore"),
        ))?,
        "cointr" => gateway.register_cointr_adapter(cointr_config(
            rest_base_urls.get("cointr"),
            private_credentials.get("cointr"),
        ))?,
        "crypto.com" | "crypto_com" | "cryptocom" => {
            gateway.register_cryptocom_adapter(cryptocom_config(
                rest_base_urls.get("cryptocom"),
                private_credentials.get("cryptocom"),
            ))?
        }
        "deepcoin" | "deep_coin" => gateway.register_deepcoin_adapter(deepcoin_config(
            rest_base_urls.get("deepcoin"),
            private_credentials.get("deepcoin"),
        ))?,
        "gate" | "gate.io" | "gateio" => gateway.register_gateio_adapter(gateio_config(
            rest_base_urls.get("gateio"),
            private_credentials.get("gateio"),
        ))?,
        "hashkey" | "hashkey_global" | "hashkey-global" => gateway
            .register_hashkey_global_adapter(hashkey_global_config(
                rest_base_urls.get("hashkey_global"),
                rest_base_urls.get("hashkey_global_spot"),
                rest_base_urls.get("hashkey_global_futures"),
                private_credentials.get("hashkey_global"),
            ))?,
        "kucoin" => gateway.register_kucoin_adapter(kucoin_config(
            rest_base_urls.get("kucoin"),
            private_credentials.get("kucoin"),
        ))?,
        "kraken" => gateway.register_kraken_adapter(kraken_config(
            rest_base_urls.get("kraken"),
            rest_base_urls.get("kraken_spot"),
            rest_base_urls.get("kraken_futures"),
            private_credentials.get("kraken"),
            private_credentials.get("kraken_futures"),
        ))?,
        "lbank" => gateway.register_lbank_adapter(lbank_config(
            rest_base_urls.get("lbank"),
            rest_base_urls.get("lbank_spot"),
            rest_base_urls.get("lbank_contract"),
            private_credentials.get("lbank"),
        ))?,
        "mexc" => gateway.register_mexc_adapter(mexc_config(
            rest_base_urls.get("mexc"),
            private_credentials.get("mexc"),
        ))?,
        "okx" => gateway.register_okx_adapter(okx_config(
            rest_base_urls.get("okx"),
            private_credentials.get("okx"),
        ))?,
        "orange_x" | "orangex" => gateway.register_orangex_adapter(orangex_config(
            rest_base_urls.get("orangex"),
            private_credentials.get("orangex"),
        ))?,
        "phemex" => gateway.register_phemex_adapter(phemex_config(
            rest_base_urls.get("phemex"),
            private_credentials.get("phemex"),
        ))?,
        "weex" => gateway.register_weex_adapter(weex_config(
            rest_base_urls.get("weex"),
            rest_base_urls.get("weex_spot"),
            rest_base_urls.get("weex_contract"),
            private_credentials.get("weex"),
        ))?,
        other => gateway.register_named_adapter(other)?,
    }
    Ok(())
}

fn ascendex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> AscendexGatewayConfig {
    let mut config = AscendexGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.account_group = credentials.account_group.clone();
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = config.account_group.is_some();
    }
    config
}

fn backpack_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BackpackGatewayConfig {
    let mut config = BackpackGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn biconomy_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BiconomyGatewayConfig {
    let mut config = BiconomyGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
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

fn blofin_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BlofinGatewayConfig {
    let mut config = BlofinGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.passphrase = credentials.passphrase.clone();
        config.enabled_private_rest = true;
        config.enabled_private_streams = true;
    }
    config
}

fn bigone_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    contract_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BigOneGatewayConfig {
    let mut config = BigOneGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.contract_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.contract_rest_base_url, contract_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn bingx_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BingxGatewayConfig {
    let mut config = BingxGatewayConfig::default();
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

fn bitkan_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitkanGatewayConfig {
    let mut config = BitkanGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
    }
    config
}

fn bitunix_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    futures_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitunixGatewayConfig {
    let mut config = BitunixGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.futures_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.futures_rest_base_url, futures_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn coindcx_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    futures_rest_base_url: Option<String>,
    public_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinDcxGatewayConfig {
    let mut config = CoinDcxGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.futures_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.futures_rest_base_url, futures_rest_base_url);
    apply_rest_base_url(&mut config.public_rest_base_url, public_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
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

fn coinstore_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    futures_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinstoreGatewayConfig {
    let mut config = CoinstoreGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.futures_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.futures_rest_base_url, futures_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn cointr_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CointrGatewayConfig {
    let mut config = CointrGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.passphrase = credentials.passphrase.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn cryptocom_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CryptoComGatewayConfig {
    let mut config = CryptoComGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn deepcoin_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> DeepcoinGatewayConfig {
    let mut config = DeepcoinGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.passphrase = credentials.passphrase.clone();
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

fn hashkey_global_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    futures_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HashKeyGlobalGatewayConfig {
    let mut config = HashKeyGlobalGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.futures_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.futures_rest_base_url, futures_rest_base_url);
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

fn kraken_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    futures_rest_base_url: Option<String>,
    spot_credentials: Option<&ExchangePrivateCredentials>,
    futures_credentials: Option<&ExchangePrivateCredentials>,
) -> KrakenGatewayConfig {
    let mut config = KrakenGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.futures_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.futures_rest_base_url, futures_rest_base_url);
    if let Some(credentials) = spot_credentials {
        config.spot_api_key = Some(credentials.api_key.clone());
        config.spot_api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    if let Some(credentials) = futures_credentials {
        config.futures_api_key = Some(credentials.api_key.clone());
        config.futures_api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn lbank_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    contract_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> LBankGatewayConfig {
    let mut config = LBankGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.contract_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.contract_rest_base_url, contract_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
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

fn orangex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> OrangeXGatewayConfig {
    let mut config = OrangeXGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        if !credentials.api_key.trim().is_empty() {
            config.client_id = Some(credentials.api_key.clone());
            config.api_key = Some(credentials.api_key.clone());
        }
        if !credentials.api_secret.trim().is_empty() {
            config.client_secret = Some(credentials.api_secret.clone());
            config.api_secret = Some(credentials.api_secret.clone());
        }
        config.access_token = credentials.passphrase.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn phemex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> PhemexGatewayConfig {
    let mut config = PhemexGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn weex_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    contract_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> WeexGatewayConfig {
    let mut config = WeexGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.contract_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.contract_rest_base_url, contract_rest_base_url);
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
    account_group_keys: Option<&'static [&'static str]>,
    requires_passphrase: bool,
}

const PRIVATE_CREDENTIAL_SPECS: &[PrivateCredentialSpec] = &[
    PrivateCredentialSpec {
        adapter: "ascendex",
        api_key_keys: &[
            "RUSTCTA_ASCENDEX_API_KEY",
            "ASCENDEX_API_KEY",
            "ASCENDEX_SPOT_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_ASCENDEX_API_SECRET",
            "ASCENDEX_API_SECRET",
            "ASCENDEX_SPOT_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: Some(&["RUSTCTA_ASCENDEX_ACCOUNT_GROUP", "ASCENDEX_ACCOUNT_GROUP"]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "backpack",
        api_key_keys: &["RUSTCTA_BACKPACK_API_KEY", "BACKPACK_API_KEY"],
        api_secret_keys: &["RUSTCTA_BACKPACK_API_SECRET", "BACKPACK_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
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
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bigone",
        api_key_keys: &["RUSTCTA_BIGONE_API_KEY", "BIGONE_API_KEY"],
        api_secret_keys: &["RUSTCTA_BIGONE_API_SECRET", "BIGONE_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "biconomy",
        api_key_keys: &[
            "RUSTCTA_BICONOMY_API_KEY",
            "BICONOMY_API_KEY",
            "BICONOMY_EXCHANGE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BICONOMY_API_SECRET",
            "BICONOMY_API_SECRET",
            "BICONOMY_EXCHANGE_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bingx",
        api_key_keys: &[
            "RUSTCTA_BINGX_API_KEY",
            "BINGX_API_KEY",
            "BINGX_SPOT_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BINGX_API_SECRET",
            "BINGX_API_SECRET",
            "BINGX_SPOT_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
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
        account_group_keys: None,
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "bitkan",
        api_key_keys: &["RUSTCTA_BITKAN_API_KEY", "BITKAN_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITKAN_API_SECRET", "BITKAN_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitunix",
        api_key_keys: &["RUSTCTA_BITUNIX_API_KEY", "BITUNIX_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITUNIX_API_SECRET", "BITUNIX_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "blofin",
        api_key_keys: &["RUSTCTA_BLOFIN_API_KEY", "BLOFIN_API_KEY"],
        api_secret_keys: &["RUSTCTA_BLOFIN_API_SECRET", "BLOFIN_API_SECRET"],
        passphrase_keys: Some(&[
            "RUSTCTA_BLOFIN_API_PASSPHRASE",
            "RUSTCTA_BLOFIN_PASSPHRASE",
            "BLOFIN_API_PASSPHRASE",
            "BLOFIN_PASSPHRASE",
        ]),
        account_group_keys: None,
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "coindcx",
        api_key_keys: &["RUSTCTA_COINDCX_API_KEY", "COINDCX_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINDCX_API_SECRET", "COINDCX_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "coinex",
        api_key_keys: &["RUSTCTA_COINEX_API_KEY", "COINEX_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINEX_API_SECRET", "COINEX_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "coinstore",
        api_key_keys: &["RUSTCTA_COINSTORE_API_KEY", "COINSTORE_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINSTORE_API_SECRET", "COINSTORE_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "cointr",
        api_key_keys: &["RUSTCTA_COINTR_API_KEY", "COINTR_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINTR_API_SECRET", "COINTR_API_SECRET"],
        passphrase_keys: Some(&[
            "RUSTCTA_COINTR_API_PASSPHRASE",
            "RUSTCTA_COINTR_PASSPHRASE",
            "COINTR_API_PASSPHRASE",
            "COINTR_PASSPHRASE",
        ]),
        account_group_keys: None,
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "cryptocom",
        api_key_keys: &[
            "RUSTCTA_CRYPTOCOM_API_KEY",
            "CRYPTOCOM_API_KEY",
            "CRYPTOCOM_EXCHANGE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_CRYPTOCOM_API_SECRET",
            "CRYPTOCOM_API_SECRET",
            "CRYPTOCOM_EXCHANGE_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "deepcoin",
        api_key_keys: &["RUSTCTA_DEEPCOIN_API_KEY", "DEEPCOIN_API_KEY"],
        api_secret_keys: &["RUSTCTA_DEEPCOIN_API_SECRET", "DEEPCOIN_API_SECRET"],
        passphrase_keys: Some(&[
            "RUSTCTA_DEEPCOIN_PASSPHRASE",
            "RUSTCTA_DEEPCOIN_API_PASSPHRASE",
            "DEEPCOIN_PASSPHRASE",
            "DEEPCOIN_API_PASSPHRASE",
        ]),
        account_group_keys: None,
        requires_passphrase: true,
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
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "hashkey_global",
        api_key_keys: &[
            "RUSTCTA_HASHKEY_GLOBAL_API_KEY",
            "HASHKEY_GLOBAL_API_KEY",
            "HASHKEY_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_HASHKEY_GLOBAL_API_SECRET",
            "HASHKEY_GLOBAL_API_SECRET",
            "HASHKEY_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
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
        account_group_keys: None,
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "kraken",
        api_key_keys: &[
            "RUSTCTA_KRAKEN_API_KEY",
            "KRAKEN_SPOT_API_KEY",
            "KRAKEN_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_KRAKEN_API_SECRET",
            "KRAKEN_SPOT_API_SECRET",
            "KRAKEN_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "kraken_futures",
        api_key_keys: &[
            "RUSTCTA_KRAKEN_FUTURES_API_KEY",
            "KRAKEN_FUTURES_API_KEY",
            "RUSTCTA_KRAKEN_API_KEY",
            "KRAKEN_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_KRAKEN_FUTURES_API_SECRET",
            "KRAKEN_FUTURES_API_SECRET",
            "RUSTCTA_KRAKEN_API_SECRET",
            "KRAKEN_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
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
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "lbank",
        api_key_keys: &[
            "RUSTCTA_LBANK_API_KEY",
            "LBANK_SPOT_API_KEY",
            "LBANK_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_LBANK_API_SECRET",
            "LBANK_SPOT_API_SECRET",
            "LBANK_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
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
        account_group_keys: None,
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "orangex",
        api_key_keys: &[
            "RUSTCTA_ORANGEX_CLIENT_ID",
            "ORANGEX_CLIENT_ID",
            "RUSTCTA_ORANGEX_API_KEY",
            "ORANGEX_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_ORANGEX_CLIENT_SECRET",
            "ORANGEX_CLIENT_SECRET",
            "RUSTCTA_ORANGEX_API_SECRET",
            "ORANGEX_API_SECRET",
        ],
        passphrase_keys: Some(&["RUSTCTA_ORANGEX_ACCESS_TOKEN", "ORANGEX_ACCESS_TOKEN"]),
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "phemex",
        api_key_keys: &[
            "RUSTCTA_PHEMEX_API_KEY",
            "PHEMEX_API_KEY",
            "PHEMEX_SPOT_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_PHEMEX_API_SECRET",
            "PHEMEX_API_SECRET",
            "PHEMEX_SPOT_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "weex",
        api_key_keys: &["RUSTCTA_WEEX_API_KEY", "WEEX_API_KEY"],
        api_secret_keys: &["RUSTCTA_WEEX_API_SECRET", "WEEX_API_SECRET"],
        passphrase_keys: Some(&[
            "RUSTCTA_WEEX_API_PASSPHRASE",
            "RUSTCTA_WEEX_PASSPHRASE",
            "WEEX_API_PASSPHRASE",
            "WEEX_PASSPHRASE",
        ]),
        account_group_keys: None,
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
            (
                "RUSTCTA_GATEWAY_ADAPTERS",
                " paper, BINANCE, BingX, gate.io ",
            ),
            ("RUSTCTA_BICONOMY_REST_BASE_URL", "http://127.0.0.1:9014"),
            ("RUSTCTA_BINANCE_REST_BASE_URL", "http://127.0.0.1:9001"),
            ("RUSTCTA_BINGX_REST_BASE_URL", "http://127.0.0.1:9006"),
            ("RUSTCTA_GATEIO_REST_BASE_URL", "http://127.0.0.1:9002"),
            ("RUSTCTA_CRYPTOCOM_REST_BASE_URL", "http://127.0.0.1:9005"),
            (
                "RUSTCTA_KRAKEN_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9003/0",
            ),
            (
                "RUSTCTA_KRAKEN_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9004/derivatives/api/v3",
            ),
            (
                "RUSTCTA_BITUNIX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9012/spot",
            ),
            (
                "RUSTCTA_BITUNIX_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9013/fapi",
            ),
            ("RUSTCTA_PHEMEX_REST_BASE_URL", "http://127.0.0.1:9007"),
            (
                "RUSTCTA_WEEX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9008/spot",
            ),
            (
                "RUSTCTA_WEEX_CONTRACT_REST_BASE_URL",
                "http://127.0.0.1:9009/contract",
            ),
            ("RUSTCTA_COINTR_REST_BASE_URL", "http://127.0.0.1:9015"),
            ("RUSTCTA_ASCENDEX_REST_BASE_URL", "http://127.0.0.1:9010"),
            ("RUSTCTA_ORANGEX_REST_BASE_URL", "http://127.0.0.1:9011"),
            ("RUSTCTA_ASCENDEX_ACCOUNT_GROUP", "42"),
            ("RUSTCTA_ASCENDEX_API_KEY", "ascendex-key"),
            ("RUSTCTA_ASCENDEX_API_SECRET", "ascendex-secret"),
            ("RUSTCTA_BINANCE_API_KEY", "binance-key"),
            ("RUSTCTA_BINANCE_API_SECRET", "binance-secret"),
            ("RUSTCTA_BICONOMY_API_KEY", "biconomy-key"),
            ("RUSTCTA_BICONOMY_API_SECRET", "biconomy-secret"),
            ("RUSTCTA_BINGX_API_KEY", "bingx-key"),
            ("RUSTCTA_BINGX_API_SECRET", "bingx-secret"),
            ("RUSTCTA_BITUNIX_API_KEY", "bitunix-key"),
            ("RUSTCTA_BITUNIX_API_SECRET", "bitunix-secret"),
            ("RUSTCTA_CRYPTOCOM_API_KEY", "cryptocom-key"),
            ("RUSTCTA_CRYPTOCOM_API_SECRET", "cryptocom-secret"),
            ("RUSTCTA_COINTR_API_KEY", "cointr-key"),
            ("RUSTCTA_COINTR_API_SECRET", "cointr-secret"),
            ("RUSTCTA_COINTR_API_PASSPHRASE", "cointr-passphrase"),
            ("RUSTCTA_DEEPCOIN_REST_BASE_URL", "http://127.0.0.1:9007"),
            ("RUSTCTA_DEEPCOIN_API_KEY", "deepcoin-key"),
            ("RUSTCTA_DEEPCOIN_API_SECRET", "deepcoin-secret"),
            ("RUSTCTA_DEEPCOIN_PASSPHRASE", "deepcoin-passphrase"),
            ("RUSTCTA_KRAKEN_API_KEY", "kraken-key"),
            ("RUSTCTA_KRAKEN_API_SECRET", "kraken-secret"),
            ("RUSTCTA_OKX_API_KEY", "okx-key"),
            ("RUSTCTA_OKX_API_SECRET", "okx-secret"),
            ("RUSTCTA_OKX_API_PASSPHRASE", "okx-passphrase"),
            ("RUSTCTA_ORANGEX_CLIENT_ID", "orangex-client"),
            ("RUSTCTA_ORANGEX_CLIENT_SECRET", "orangex-secret"),
            ("RUSTCTA_ORANGEX_ACCESS_TOKEN", "orangex-token"),
            ("RUSTCTA_PHEMEX_API_KEY", "phemex-key"),
            ("RUSTCTA_PHEMEX_API_SECRET", "phemex-secret"),
            ("RUSTCTA_WEEX_API_KEY", "weex-key"),
            ("RUSTCTA_WEEX_API_SECRET", "weex-secret"),
            ("RUSTCTA_WEEX_API_PASSPHRASE", "weex-passphrase"),
        ]));

        assert_eq!(config.bind_addr, "127.0.0.1:0");
        assert_eq!(
            config.adapters,
            vec!["paper", "binance", "bingx", "gate.io"]
        );
        assert_eq!(
            config.rest_base_urls.get("binance").as_deref(),
            Some("http://127.0.0.1:9001")
        );
        assert_eq!(
            config.rest_base_urls.get("biconomy").as_deref(),
            Some("http://127.0.0.1:9014")
        );
        assert_eq!(
            config.rest_base_urls.get("bingx").as_deref(),
            Some("http://127.0.0.1:9006")
        );
        assert_eq!(
            config.rest_base_urls.get("gateio").as_deref(),
            Some("http://127.0.0.1:9002")
        );
        assert_eq!(
            config.rest_base_urls.get("cryptocom").as_deref(),
            Some("http://127.0.0.1:9005")
        );
        assert_eq!(
            config.rest_base_urls.get("deepcoin").as_deref(),
            Some("http://127.0.0.1:9007")
        );
        assert_eq!(
            config.rest_base_urls.get("kraken_spot").as_deref(),
            Some("http://127.0.0.1:9003/0")
        );
        assert_eq!(
            config.rest_base_urls.get("kraken_futures").as_deref(),
            Some("http://127.0.0.1:9004/derivatives/api/v3")
        );
        assert_eq!(
            config.rest_base_urls.get("bitunix_spot").as_deref(),
            Some("http://127.0.0.1:9012/spot")
        );
        assert_eq!(
            config.rest_base_urls.get("bitunix_futures").as_deref(),
            Some("http://127.0.0.1:9013/fapi")
        );
        assert_eq!(
            config.rest_base_urls.get("phemex").as_deref(),
            Some("http://127.0.0.1:9007")
        );
        assert_eq!(
            config.rest_base_urls.get("weex_spot").as_deref(),
            Some("http://127.0.0.1:9008/spot")
        );
        assert_eq!(
            config.rest_base_urls.get("weex_contract").as_deref(),
            Some("http://127.0.0.1:9009/contract")
        );
        assert_eq!(
            config.rest_base_urls.get("ascendex").as_deref(),
            Some("http://127.0.0.1:9010")
        );
        assert_eq!(
            config.rest_base_urls.get("orangex").as_deref(),
            Some("http://127.0.0.1:9011")
        );
        assert_eq!(
            config.rest_base_urls.get("cointr").as_deref(),
            Some("http://127.0.0.1:9015")
        );
        assert!(config.private_credentials.contains("ascendex"));
        assert!(config.private_credentials.contains("binance"));
        assert!(config.private_credentials.contains("biconomy"));
        assert!(config.private_credentials.contains("bingx"));
        assert!(config.private_credentials.contains("bitunix"));
        assert!(config.private_credentials.contains("cointr"));
        assert!(config.private_credentials.contains("cryptocom"));
        assert!(config.private_credentials.contains("deepcoin"));
        assert!(config.private_credentials.contains("kraken"));
        assert!(config.private_credentials.contains("kraken_futures"));
        assert!(config.private_credentials.contains("okx"));
        assert!(config.private_credentials.contains("orangex"));
        assert!(config.private_credentials.contains("phemex"));
        assert!(config.private_credentials.contains("weex"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("ascendex-secret"));
        assert!(!debug.contains("binance-secret"));
        assert!(!debug.contains("biconomy-secret"));
        assert!(!debug.contains("bingx-secret"));
        assert!(!debug.contains("bitunix-secret"));
        assert!(!debug.contains("cointr-passphrase"));
        assert!(!debug.contains("cointr-secret"));
        assert!(!debug.contains("cryptocom-secret"));
        assert!(!debug.contains("deepcoin-secret"));
        assert!(!debug.contains("deepcoin-passphrase"));
        assert!(!debug.contains("kraken-secret"));
        assert!(!debug.contains("okx-passphrase"));
        assert!(!debug.contains("orangex-secret"));
        assert!(!debug.contains("orangex-token"));
        assert!(!debug.contains("phemex-secret"));
        assert!(!debug.contains("weex-passphrase"));
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

    #[tokio::test]
    async fn config_should_wire_bingx_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bingx"),
            ("RUSTCTA_BINGX_REST_BASE_URL", "http://127.0.0.1:9006"),
            ("RUSTCTA_BINGX_API_KEY", "bingx-key"),
            ("RUSTCTA_BINGX_API_SECRET", "bingx-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "bingx-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bingx-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bingx").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_amend_order);
        assert!(response.capabilities[0].supports_order_list);
    }

    #[tokio::test]
    async fn config_should_wire_bigone_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bigone"),
            ("RUSTCTA_BIGONE_SPOT_REST_BASE_URL", "http://127.0.0.1:9020"),
            (
                "RUSTCTA_BIGONE_CONTRACT_REST_BASE_URL",
                "http://127.0.0.1:9021",
            ),
            ("RUSTCTA_BIGONE_API_KEY", "bigone-key"),
            ("RUSTCTA_BIGONE_API_SECRET", "bigone-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "bigone-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bigone-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bigone").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_positions);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_cancel_all_orders);
        assert!(!response.capabilities[0].supports_amend_order);
        assert!(!response.capabilities[0].supports_quote_market_order);
    }

    #[tokio::test]
    async fn config_should_wire_coindcx_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "coindcx"),
            (
                "RUSTCTA_COINDCX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9030",
            ),
            (
                "RUSTCTA_COINDCX_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9031",
            ),
            (
                "RUSTCTA_COINDCX_PUBLIC_REST_BASE_URL",
                "http://127.0.0.1:9032",
            ),
            ("RUSTCTA_COINDCX_API_KEY", "coindcx-key"),
            ("RUSTCTA_COINDCX_API_SECRET", "coindcx-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "coindcx-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("coindcx-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("coindcx").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_positions);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_cancel_all_orders);
        assert!(response.capabilities[0].supports_amend_order);
        assert!(!response.capabilities[0].supports_reduce_only);
    }

    #[tokio::test]
    async fn config_should_wire_ascendex_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "ascendex"),
            ("RUSTCTA_ASCENDEX_REST_BASE_URL", "http://127.0.0.1:9010"),
            ("RUSTCTA_ASCENDEX_ACCOUNT_GROUP", "42"),
            ("RUSTCTA_ASCENDEX_API_KEY", "ascendex-key"),
            ("RUSTCTA_ASCENDEX_API_SECRET", "ascendex-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "ascendex-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("ascendex-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("ascendex").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        let private_streams = response.capabilities[0]
            .private_stream_capabilities
            .as_ref()
            .expect("private stream capabilities");
        assert!(private_streams.supports_orders);
        assert!(private_streams.supports_fills);
        assert!(!private_streams.supports_balances);
        assert!(!private_streams.supports_positions);
        assert!(!private_streams.supports_account);
    }

    #[tokio::test]
    async fn config_should_wire_deepcoin_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "deepcoin"),
            ("RUSTCTA_DEEPCOIN_REST_BASE_URL", "http://127.0.0.1:9007"),
            ("RUSTCTA_DEEPCOIN_API_KEY", "deepcoin-key"),
            ("RUSTCTA_DEEPCOIN_API_SECRET", "deepcoin-secret"),
            ("RUSTCTA_DEEPCOIN_PASSPHRASE", "deepcoin-passphrase"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "deepcoin-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("deepcoin-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("deepcoin").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_amend_order);
        assert!(response.capabilities[0].supports_quote_market_order);
    }

    #[tokio::test]
    async fn config_should_wire_kraken_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "kraken"),
            (
                "RUSTCTA_KRAKEN_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9003/0",
            ),
            (
                "RUSTCTA_KRAKEN_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9004/derivatives/api/v3",
            ),
            ("RUSTCTA_KRAKEN_API_KEY", "kraken-spot-key"),
            ("RUSTCTA_KRAKEN_API_SECRET", "kraken-spot-secret"),
            ("RUSTCTA_KRAKEN_FUTURES_API_KEY", "kraken-futures-key"),
            ("RUSTCTA_KRAKEN_FUTURES_API_SECRET", "kraken-futures-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "kraken-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("kraken-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("kraken").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_quote_market_order);
        assert!(response.capabilities[0].supports_positions);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
    }

    #[tokio::test]
    async fn config_should_wire_bitunix_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bitunix"),
            (
                "RUSTCTA_BITUNIX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9012/spot",
            ),
            (
                "RUSTCTA_BITUNIX_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9013/fapi",
            ),
            ("RUSTCTA_BITUNIX_API_KEY", "bitunix-key"),
            ("RUSTCTA_BITUNIX_API_SECRET", "bitunix-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "bitunix-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bitunix-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bitunix").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_public_streams);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_positions);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_cancel_all_orders);
        assert!(response.capabilities[0].supports_amend_order);
        assert!(response.capabilities[0].supports_quote_market_order);
        let private_streams = response.capabilities[0]
            .private_stream_capabilities
            .as_ref()
            .expect("private stream capabilities");
        assert!(private_streams.supports_orders);
        assert!(private_streams.supports_fills);
        assert!(private_streams.supports_balances);
        assert!(private_streams.supports_positions);
        assert!(private_streams.supports_account);
    }

    #[tokio::test]
    async fn config_should_wire_orangex_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "orangex"),
            ("RUSTCTA_ORANGEX_REST_BASE_URL", "http://127.0.0.1:9011"),
            ("RUSTCTA_ORANGEX_ACCESS_TOKEN", "orangex-token"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "orangex-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("orangex-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("orangex").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_public_streams);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_quote_market_order);
        let private_streams = response.capabilities[0]
            .private_stream_capabilities
            .as_ref()
            .expect("private stream capabilities");
        assert!(private_streams.supports_orders);
        assert!(private_streams.supports_fills);
        assert!(private_streams.supports_balances);
        assert!(private_streams.supports_positions);
        assert!(private_streams.supports_account);
    }

    #[tokio::test]
    async fn config_should_wire_weex_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "weex"),
            (
                "RUSTCTA_WEEX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9008/spot",
            ),
            (
                "RUSTCTA_WEEX_CONTRACT_REST_BASE_URL",
                "http://127.0.0.1:9009/contract",
            ),
            ("RUSTCTA_WEEX_API_KEY", "weex-key"),
            ("RUSTCTA_WEEX_API_SECRET", "weex-secret"),
            ("RUSTCTA_WEEX_API_PASSPHRASE", "weex-passphrase"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "weex-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("weex-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("weex").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_public_streams);
        assert!(response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_recent_fills);
    }

    #[tokio::test]
    async fn config_should_wire_cointr_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "cointr"),
            ("RUSTCTA_COINTR_REST_BASE_URL", "http://127.0.0.1:9015"),
            ("RUSTCTA_COINTR_API_KEY", "cointr-key"),
            ("RUSTCTA_COINTR_API_SECRET", "cointr-secret"),
            ("RUSTCTA_COINTR_API_PASSPHRASE", "cointr-passphrase"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "cointr-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("cointr-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("cointr").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        let capabilities = &response.capabilities[0];
        assert!(capabilities.supports_private_rest);
        assert!(capabilities.supports_public_streams);
        assert!(capabilities.supports_private_streams);
        assert!(capabilities.supports_positions);
        assert!(capabilities.supports_batch_place_order);
        assert!(capabilities.supports_batch_cancel_order);
        assert!(capabilities.supports_recent_fills);
        assert_eq!(
            capabilities.market_types,
            vec![
                rustcta_exchange_api::MarketType::Spot,
                rustcta_exchange_api::MarketType::Perpetual
            ]
        );
    }
}
