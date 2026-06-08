use std::collections::HashMap;

use anyhow::Result;
use rustcta_exchange_gateway::{
    AdapterBackedGateway, AscendexGatewayConfig, AsterGatewayConfig, BackpackGatewayConfig,
    BiconomyGatewayConfig, BigOneGatewayConfig, BinanceCoinMGatewayConfig, BinanceGatewayConfig,
    BingxGatewayConfig, BitbankGatewayConfig, BitfinexGatewayConfig, BitflyerGatewayConfig,
    BitgetGatewayConfig, BithumbGatewayConfig, BitkanGatewayConfig, BitmartGatewayConfig,
    BitsoGatewayConfig, BitstampGatewayConfig, BitunixGatewayConfig, BitvavoGatewayConfig,
    BlofinGatewayConfig, BtcMarketsGatewayConfig, BtcTurkGatewayConfig, BybitGatewayConfig,
    CoinDcxGatewayConfig, CoinExGatewayConfig, CoinbaseExchangeGatewayConfig,
    CoincheckGatewayConfig, CoinoneGatewayConfig, CoinsPhGatewayConfig, CoinspotGatewayConfig,
    CoinstoreGatewayConfig, CointrGatewayConfig, CryptoComGatewayConfig, DeepcoinGatewayConfig,
    DeriveGatewayConfig, DydxGatewayConfig, GateIoGatewayConfig, GeminiGatewayConfig,
    HashKeyGlobalGatewayConfig, HtxGatewayConfig, HuobiGatewayConfig, HyperliquidGatewayConfig,
    IndependentReserveGatewayConfig, IndodaxGatewayConfig, KrakenFuturesGatewayConfig,
    KrakenGatewayConfig, KuCoinFuturesGatewayConfig, KuCoinGatewayConfig, LBankGatewayConfig,
    LunoGatewayConfig, MercadoGatewayConfig, MexcGatewayConfig, OkxGatewayConfig,
    OrangeXGatewayConfig, ParadexGatewayConfig, PhemexGatewayConfig, UpbitGatewayConfig,
    WeexGatewayConfig,
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
            ("aster", "RUSTCTA_ASTER_REST_BASE_URL"),
            ("backpack", "RUSTCTA_BACKPACK_REST_BASE_URL"),
            ("biconomy", "RUSTCTA_BICONOMY_REST_BASE_URL"),
            ("binance", "RUSTCTA_BINANCE_REST_BASE_URL"),
            ("binancecoinm", "RUSTCTA_BINANCECOINM_REST_BASE_URL"),
            ("bigone", "RUSTCTA_BIGONE_REST_BASE_URL"),
            ("bigone_spot", "RUSTCTA_BIGONE_SPOT_REST_BASE_URL"),
            ("bigone_contract", "RUSTCTA_BIGONE_CONTRACT_REST_BASE_URL"),
            ("bingx", "RUSTCTA_BINGX_REST_BASE_URL"),
            ("bitbank", "RUSTCTA_BITBANK_REST_BASE_URL"),
            ("bitbank_public", "RUSTCTA_BITBANK_PUBLIC_REST_BASE_URL"),
            ("bitbank_private", "RUSTCTA_BITBANK_PRIVATE_REST_BASE_URL"),
            ("bitfinex", "RUSTCTA_BITFINEX_REST_BASE_URL"),
            ("bitfinex_public", "RUSTCTA_BITFINEX_PUBLIC_REST_BASE_URL"),
            ("bitfinex_private", "RUSTCTA_BITFINEX_PRIVATE_REST_BASE_URL"),
            ("bitflyer", "RUSTCTA_BITFLYER_REST_BASE_URL"),
            ("bitget", "RUSTCTA_BITGET_REST_BASE_URL"),
            ("bithumb", "RUSTCTA_BITHUMB_REST_BASE_URL"),
            ("bitkan", "RUSTCTA_BITKAN_REST_BASE_URL"),
            ("bitmart", "RUSTCTA_BITMART_REST_BASE_URL"),
            ("bitmart_spot", "RUSTCTA_BITMART_SPOT_REST_BASE_URL"),
            ("bitmart_futures", "RUSTCTA_BITMART_FUTURES_REST_BASE_URL"),
            ("bitso", "RUSTCTA_BITSO_REST_BASE_URL"),
            ("bitstamp", "RUSTCTA_BITSTAMP_REST_BASE_URL"),
            ("bitvavo", "RUSTCTA_BITVAVO_REST_BASE_URL"),
            ("btcturk", "RUSTCTA_BTCTURK_REST_BASE_URL"),
            ("bitunix", "RUSTCTA_BITUNIX_REST_BASE_URL"),
            ("bybit", "RUSTCTA_BYBIT_REST_BASE_URL"),
            ("bitunix_spot", "RUSTCTA_BITUNIX_SPOT_REST_BASE_URL"),
            ("bitunix_futures", "RUSTCTA_BITUNIX_FUTURES_REST_BASE_URL"),
            ("blofin", "RUSTCTA_BLOFIN_REST_BASE_URL"),
            ("btcmarkets", "RUSTCTA_BTCMARKETS_REST_BASE_URL"),
            ("coindcx", "RUSTCTA_COINDCX_REST_BASE_URL"),
            ("coindcx_spot", "RUSTCTA_COINDCX_SPOT_REST_BASE_URL"),
            ("coindcx_futures", "RUSTCTA_COINDCX_FUTURES_REST_BASE_URL"),
            ("coindcx_public", "RUSTCTA_COINDCX_PUBLIC_REST_BASE_URL"),
            (
                "coinbaseexchange",
                "RUSTCTA_COINBASE_EXCHANGE_REST_BASE_URL",
            ),
            ("coinbaseexchange", "RUSTCTA_COINBASEEXCHANGE_REST_BASE_URL"),
            ("coincheck", "RUSTCTA_COINCHECK_REST_BASE_URL"),
            ("coinex", "RUSTCTA_COINEX_REST_BASE_URL"),
            ("coinone", "RUSTCTA_COINONE_REST_BASE_URL"),
            ("coinspot", "RUSTCTA_COINSPOT_REST_BASE_URL"),
            ("coinspot_public", "RUSTCTA_COINSPOT_PUBLIC_REST_BASE_URL"),
            ("coinspot_private", "RUSTCTA_COINSPOT_PRIVATE_REST_BASE_URL"),
            ("coinspot_ro", "RUSTCTA_COINSPOT_READ_ONLY_REST_BASE_URL"),
            ("coinsph", "RUSTCTA_COINSPH_REST_BASE_URL"),
            ("coinstore", "RUSTCTA_COINSTORE_REST_BASE_URL"),
            ("coinstore_spot", "RUSTCTA_COINSTORE_SPOT_REST_BASE_URL"),
            (
                "coinstore_futures",
                "RUSTCTA_COINSTORE_FUTURES_REST_BASE_URL",
            ),
            ("cointr", "RUSTCTA_COINTR_REST_BASE_URL"),
            ("cryptocom", "RUSTCTA_CRYPTOCOM_REST_BASE_URL"),
            ("deepcoin", "RUSTCTA_DEEPCOIN_REST_BASE_URL"),
            ("derive", "RUSTCTA_DERIVE_REST_BASE_URL"),
            ("dydx", "RUSTCTA_DYDX_INDEXER_REST_BASE_URL"),
            ("dydx_node", "RUSTCTA_DYDX_NODE_REST_BASE_URL"),
            ("gateio", "RUSTCTA_GATEIO_REST_BASE_URL"),
            ("gemini", "RUSTCTA_GEMINI_REST_BASE_URL"),
            ("hashkey_global", "RUSTCTA_HASHKEY_GLOBAL_REST_BASE_URL"),
            (
                "hashkey_global_spot",
                "RUSTCTA_HASHKEY_GLOBAL_SPOT_REST_BASE_URL",
            ),
            (
                "hashkey_global_futures",
                "RUSTCTA_HASHKEY_GLOBAL_FUTURES_REST_BASE_URL",
            ),
            ("htx", "RUSTCTA_HTX_REST_BASE_URL"),
            ("htx_spot", "RUSTCTA_HTX_SPOT_REST_BASE_URL"),
            ("htx_linear", "RUSTCTA_HTX_LINEAR_REST_BASE_URL"),
            ("huobi", "RUSTCTA_HUOBI_REST_BASE_URL"),
            ("huobi_spot", "RUSTCTA_HUOBI_SPOT_REST_BASE_URL"),
            ("huobi_linear", "RUSTCTA_HUOBI_LINEAR_REST_BASE_URL"),
            ("hyperliquid", "RUSTCTA_HYPERLIQUID_REST_BASE_URL"),
            (
                "independentreserve",
                "RUSTCTA_INDEPENDENTRESERVE_REST_BASE_URL",
            ),
            ("indodax", "RUSTCTA_INDODAX_REST_BASE_URL"),
            ("indodax_public", "RUSTCTA_INDODAX_PUBLIC_REST_BASE_URL"),
            ("indodax_private", "RUSTCTA_INDODAX_PRIVATE_REST_BASE_URL"),
            ("kucoin", "RUSTCTA_KUCOIN_REST_BASE_URL"),
            ("kucoinfutures", "RUSTCTA_KUCOIN_FUTURES_REST_BASE_URL"),
            ("kraken", "RUSTCTA_KRAKEN_REST_BASE_URL"),
            ("kraken_spot", "RUSTCTA_KRAKEN_SPOT_REST_BASE_URL"),
            ("kraken_futures", "RUSTCTA_KRAKEN_FUTURES_REST_BASE_URL"),
            ("krakenfutures", "RUSTCTA_KRAKEN_FUTURES_REST_BASE_URL"),
            ("lbank", "RUSTCTA_LBANK_REST_BASE_URL"),
            ("lbank_spot", "RUSTCTA_LBANK_SPOT_REST_BASE_URL"),
            ("lbank_contract", "RUSTCTA_LBANK_CONTRACT_REST_BASE_URL"),
            ("luno", "RUSTCTA_LUNO_REST_BASE_URL"),
            ("mercado", "RUSTCTA_MERCADO_REST_BASE_URL"),
            ("mexc", "RUSTCTA_MEXC_REST_BASE_URL"),
            ("okx", "RUSTCTA_OKX_REST_BASE_URL"),
            ("orangex", "RUSTCTA_ORANGEX_REST_BASE_URL"),
            ("paradex", "RUSTCTA_PARADEX_REST_BASE_URL"),
            ("phemex", "RUSTCTA_PHEMEX_REST_BASE_URL"),
            ("upbit", "RUSTCTA_UPBIT_REST_BASE_URL"),
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
            let token_only_credentials = matches!(spec.adapter, "orangex" | "mercado" | "dydx")
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
        "aster" | "asterdex" | "aster_dex" => gateway.register_aster_adapter(aster_config(
            rest_base_urls.get("aster"),
            private_credentials.get("aster"),
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
        "binancecoinm" | "binance_coinm" | "binance-coinm" | "binance_coin_m" => gateway
            .register_binancecoinm_adapter(binancecoinm_config(
                rest_base_urls.get("binancecoinm"),
                private_credentials.get("binancecoinm"),
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
        "bitbank" => gateway.register_bitbank_adapter(bitbank_config(
            rest_base_urls.get("bitbank"),
            rest_base_urls.get("bitbank_public"),
            rest_base_urls.get("bitbank_private"),
            private_credentials.get("bitbank"),
        ))?,
        "bitfinex" => gateway.register_bitfinex_adapter(bitfinex_config(
            rest_base_urls.get("bitfinex"),
            rest_base_urls.get("bitfinex_public"),
            rest_base_urls.get("bitfinex_private"),
            private_credentials.get("bitfinex"),
        ))?,
        "bitflyer" | "bit_flyer" => gateway.register_bitflyer_adapter(bitflyer_config(
            rest_base_urls.get("bitflyer"),
            private_credentials.get("bitflyer"),
        ))?,
        "bitget" => gateway.register_bitget_adapter(bitget_config(
            rest_base_urls.get("bitget"),
            private_credentials.get("bitget"),
        ))?,
        "bithumb" => gateway.register_bithumb_adapter(bithumb_config(
            rest_base_urls.get("bithumb"),
            private_credentials.get("bithumb"),
        ))?,
        "bitkan" => gateway.register_bitkan_adapter(bitkan_config(
            rest_base_urls.get("bitkan"),
            private_credentials.get("bitkan"),
        ))?,
        "bitmart" => gateway.register_bitmart_adapter(bitmart_config(
            rest_base_urls.get("bitmart"),
            rest_base_urls.get("bitmart_spot"),
            rest_base_urls.get("bitmart_futures"),
            private_credentials.get("bitmart"),
        ))?,
        "bybit" => gateway.register_bybit_adapter(bybit_config(
            rest_base_urls.get("bybit"),
            private_credentials.get("bybit"),
        ))?,
        "bitso" => gateway.register_bitso_adapter(bitso_config(
            rest_base_urls.get("bitso"),
            private_credentials.get("bitso"),
        ))?,
        "bitstamp" => gateway.register_bitstamp_adapter(bitstamp_config(
            rest_base_urls.get("bitstamp"),
            private_credentials.get("bitstamp"),
        ))?,
        "bitvavo" => gateway.register_bitvavo_adapter(bitvavo_config(
            rest_base_urls.get("bitvavo"),
            private_credentials.get("bitvavo"),
        ))?,
        "btcturk" | "btc_turk" | "btc-turk" => gateway.register_btcturk_adapter(btcturk_config(
            rest_base_urls.get("btcturk"),
            private_credentials.get("btcturk"),
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
        "btcmarkets" | "btc_markets" => gateway.register_btcmarkets_adapter(btcmarkets_config(
            rest_base_urls.get("btcmarkets"),
            private_credentials.get("btcmarkets"),
        ))?,
        "coindcx" | "coin_dcx" => gateway.register_coindcx_adapter(coindcx_config(
            rest_base_urls.get("coindcx"),
            rest_base_urls.get("coindcx_spot"),
            rest_base_urls.get("coindcx_futures"),
            rest_base_urls.get("coindcx_public"),
            private_credentials.get("coindcx"),
        ))?,
        "coinbaseexchange" | "coinbase_exchange" | "coinbase-exchange" => gateway
            .register_coinbaseexchange_adapter(coinbaseexchange_config(
                rest_base_urls.get("coinbaseexchange"),
                private_credentials.get("coinbaseexchange"),
            ))?,
        "coincheck" => gateway.register_coincheck_adapter(coincheck_config(
            rest_base_urls.get("coincheck"),
            private_credentials.get("coincheck"),
        ))?,
        "coinex" => gateway.register_coinex_adapter(coinex_config(
            rest_base_urls.get("coinex"),
            private_credentials.get("coinex"),
        ))?,
        "coinone" => gateway.register_coinone_adapter(coinone_config(
            rest_base_urls.get("coinone"),
            private_credentials.get("coinone"),
        ))?,
        "coinspot" | "coin_spot" => gateway.register_coinspot_adapter(coinspot_config(
            rest_base_urls.get("coinspot"),
            rest_base_urls.get("coinspot_public"),
            rest_base_urls.get("coinspot_private"),
            rest_base_urls.get("coinspot_ro"),
            private_credentials.get("coinspot"),
        ))?,
        "coinsph" | "coins_ph" | "coins.ph" => gateway.register_coinsph_adapter(coinsph_config(
            rest_base_urls.get("coinsph"),
            private_credentials.get("coinsph"),
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
        "derive" => gateway.register_derive_adapter(derive_config(
            rest_base_urls.get("derive"),
            private_credentials.get("derive"),
        ))?,
        "dydx" | "dydx_v4" | "dydxv4" => gateway.register_dydx_adapter(dydx_config(
            rest_base_urls.get("dydx"),
            rest_base_urls.get("dydx_node"),
            private_credentials.get("dydx"),
        ))?,
        "gate" | "gate.io" | "gateio" => gateway.register_gateio_adapter(gateio_config(
            rest_base_urls.get("gateio"),
            private_credentials.get("gateio"),
        ))?,
        "gemini" => gateway.register_gemini_adapter(gemini_config(
            rest_base_urls.get("gemini"),
            private_credentials.get("gemini"),
        ))?,
        "hashkey" | "hashkey_global" | "hashkey-global" => gateway
            .register_hashkey_global_adapter(hashkey_global_config(
                rest_base_urls.get("hashkey_global"),
                rest_base_urls.get("hashkey_global_spot"),
                rest_base_urls.get("hashkey_global_futures"),
                private_credentials.get("hashkey_global"),
            ))?,
        "htx" => gateway.register_htx_adapter(htx_config(
            rest_base_urls.get("htx"),
            rest_base_urls.get("htx_spot"),
            rest_base_urls.get("htx_linear"),
            private_credentials.get("htx"),
        ))?,
        "huobi" => gateway.register_huobi_adapter(huobi_config(
            rest_base_urls.get("huobi"),
            rest_base_urls.get("huobi_spot"),
            rest_base_urls.get("huobi_linear"),
            private_credentials.get("huobi"),
        ))?,
        "hyperliquid" | "hyper_liquid" => {
            gateway.register_hyperliquid_adapter(hyperliquid_config(
                rest_base_urls.get("hyperliquid"),
                private_credentials.get("hyperliquid"),
            ))?
        }
        "independentreserve" | "independent_reserve" => gateway
            .register_independentreserve_adapter(independentreserve_config(
                rest_base_urls.get("independentreserve"),
                private_credentials.get("independentreserve"),
            ))?,
        "indodax" => gateway.register_indodax_adapter(indodax_config(
            rest_base_urls.get("indodax"),
            rest_base_urls.get("indodax_public"),
            rest_base_urls.get("indodax_private"),
            private_credentials.get("indodax"),
        ))?,
        "kucoin" => gateway.register_kucoin_adapter(kucoin_config(
            rest_base_urls.get("kucoin"),
            private_credentials.get("kucoin"),
        ))?,
        "kucoinfutures" | "kucoin_futures" | "kucoin-futures" => gateway
            .register_kucoinfutures_adapter(kucoinfutures_config(
                rest_base_urls.get("kucoinfutures"),
                private_credentials.get("kucoinfutures"),
            ))?,
        "kraken" => gateway.register_kraken_adapter(kraken_config(
            rest_base_urls.get("kraken"),
            rest_base_urls.get("kraken_spot"),
            rest_base_urls.get("kraken_futures"),
            private_credentials.get("kraken"),
            private_credentials.get("kraken_futures"),
        ))?,
        "krakenfutures" | "kraken_futures" | "kraken-futures" => gateway
            .register_krakenfutures_adapter(krakenfutures_config(
                rest_base_urls.get("krakenfutures"),
                private_credentials.get("krakenfutures"),
            ))?,
        "lbank" => gateway.register_lbank_adapter(lbank_config(
            rest_base_urls.get("lbank"),
            rest_base_urls.get("lbank_spot"),
            rest_base_urls.get("lbank_contract"),
            private_credentials.get("lbank"),
        ))?,
        "luno" => gateway.register_luno_adapter(luno_config(
            rest_base_urls.get("luno"),
            private_credentials.get("luno"),
        ))?,
        "mercado" | "mercadobitcoin" | "mercado_bitcoin" | "mercado-bitcoin" => gateway
            .register_mercado_adapter(mercado_config(
                rest_base_urls.get("mercado"),
                private_credentials.get("mercado"),
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
        "paradex" => gateway.register_paradex_adapter(paradex_config(
            rest_base_urls.get("paradex"),
            private_credentials.get("paradex"),
        ))?,
        "phemex" => gateway.register_phemex_adapter(phemex_config(
            rest_base_urls.get("phemex"),
            private_credentials.get("phemex"),
        ))?,
        "upbit" => gateway.register_upbit_adapter(upbit_config(
            rest_base_urls.get("upbit"),
            private_credentials.get("upbit"),
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

fn aster_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> AsterGatewayConfig {
    let mut config = AsterGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.user_address = Some(credentials.api_key.clone());
        config.signer_private_key = Some(credentials.api_secret.clone());
        config.signer_address = credentials.passphrase.clone();
        config.enabled_private_rest = config.signer_address.is_some();
        config.enabled_private_streams = config.signer_address.is_some();
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

fn binancecoinm_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BinanceCoinMGatewayConfig {
    let mut config = BinanceCoinMGatewayConfig::default();
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

fn btcmarkets_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BtcMarketsGatewayConfig {
    let mut config = BtcMarketsGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
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

fn bitbank_config(
    rest_base_url: Option<String>,
    public_rest_base_url: Option<String>,
    private_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitbankGatewayConfig {
    let mut config = BitbankGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.public_rest_base_url = rest_base_url.clone();
        config.private_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.public_rest_base_url, public_rest_base_url);
    apply_rest_base_url(&mut config.private_rest_base_url, private_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn bitflyer_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitflyerGatewayConfig {
    let mut config = BitflyerGatewayConfig::default();
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

fn bithumb_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BithumbGatewayConfig {
    let mut config = BithumbGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
        config.enabled_private_streams = true;
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

fn bitfinex_config(
    rest_base_url: Option<String>,
    public_rest_base_url: Option<String>,
    private_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitfinexGatewayConfig {
    let mut config = BitfinexGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.public_rest_base_url = rest_base_url.clone();
        config.private_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.public_rest_base_url, public_rest_base_url);
    apply_rest_base_url(&mut config.private_rest_base_url, private_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn bitmart_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    futures_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitmartGatewayConfig {
    let mut config = BitmartGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.futures_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.futures_rest_base_url, futures_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.memo = credentials.passphrase.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn bitso_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitsoGatewayConfig {
    let mut config = BitsoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
    }
    config
}

fn bitstamp_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitstampGatewayConfig {
    let mut config = BitstampGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.subaccount_id = credentials.account_group.clone();
        config.enabled_private_rest = true;
        config.enabled_private_streams = true;
    }
    config
}

fn bitvavo_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitvavoGatewayConfig {
    let mut config = BitvavoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn btcturk_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BtcTurkGatewayConfig {
    let mut config = BtcTurkGatewayConfig::default();
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

fn bybit_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BybitGatewayConfig {
    let mut config = BybitGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
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

fn coinbaseexchange_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinbaseExchangeGatewayConfig {
    let mut config = CoinbaseExchangeGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.api_passphrase = credentials.passphrase.clone();
        config.enabled_private_rest = config.api_passphrase.is_some();
    }
    config
}

fn coincheck_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoincheckGatewayConfig {
    let mut config = CoincheckGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
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

fn coinone_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinoneGatewayConfig {
    let mut config = CoinoneGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.access_token = Some(credentials.api_key.clone());
        config.secret_key = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn coinspot_config(
    rest_base_url: Option<String>,
    public_rest_base_url: Option<String>,
    private_rest_base_url: Option<String>,
    read_only_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinspotGatewayConfig {
    let mut config = CoinspotGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if config.public_rest_base_url == CoinspotGatewayConfig::default().public_rest_base_url {
        config.public_rest_base_url = config.rest_base_url.clone();
    }
    if config.private_rest_base_url == CoinspotGatewayConfig::default().private_rest_base_url {
        config.private_rest_base_url = config.rest_base_url.clone();
    }
    if config.read_only_rest_base_url == CoinspotGatewayConfig::default().read_only_rest_base_url {
        config.read_only_rest_base_url = config.rest_base_url.clone();
    }
    apply_rest_base_url(&mut config.public_rest_base_url, public_rest_base_url);
    apply_rest_base_url(&mut config.private_rest_base_url, private_rest_base_url);
    apply_rest_base_url(&mut config.read_only_rest_base_url, read_only_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn coinsph_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinsPhGatewayConfig {
    let mut config = CoinsPhGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
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

fn dydx_config(
    indexer_rest_base_url: Option<String>,
    node_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> DydxGatewayConfig {
    let mut config = DydxGatewayConfig::default();
    apply_rest_base_url(&mut config.indexer_rest_base_url, indexer_rest_base_url);
    apply_rest_base_url(&mut config.node_rest_base_url, node_rest_base_url);
    if let Some(credentials) = credentials {
        config.wallet_address = Some(credentials.api_key.clone());
        if let Some(subaccount_number) = credentials
            .account_group
            .as_ref()
            .and_then(|value| value.parse().ok())
        {
            config.subaccount_number = subaccount_number;
        }
        config.enabled_private_indexer_rest = true;
        config.enabled_private_streams = true;
        config.enabled_node_private_write = false;
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

fn derive_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> DeriveGatewayConfig {
    let mut config = DeriveGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.session_key = Some(credentials.api_key.clone());
        config.session_secret = Some(credentials.api_secret.clone());
        config.wallet = credentials.passphrase.clone();
        config.subaccount_id = credentials.account_group.clone();
        config.enabled_private_rest = config.subaccount_id.is_some();
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

fn gemini_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> GeminiGatewayConfig {
    let mut config = GeminiGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
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

fn htx_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    linear_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HtxGatewayConfig {
    let mut config = HtxGatewayConfig::default();
    apply_htx_config(
        &mut config,
        rest_base_url,
        spot_rest_base_url,
        linear_rest_base_url,
        credentials,
    );
    config
}

fn huobi_config(
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    linear_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HuobiGatewayConfig {
    let mut config = HuobiGatewayConfig::huobi_legacy();
    apply_htx_config(
        &mut config,
        rest_base_url,
        spot_rest_base_url,
        linear_rest_base_url,
        credentials,
    );
    config
}

fn apply_htx_config(
    config: &mut HtxGatewayConfig,
    rest_base_url: Option<String>,
    spot_rest_base_url: Option<String>,
    linear_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) {
    if let Some(rest_base_url) = rest_base_url {
        config.spot_rest_base_url = rest_base_url.clone();
        config.linear_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.spot_rest_base_url, spot_rest_base_url);
    apply_rest_base_url(&mut config.linear_rest_base_url, linear_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.spot_account_id = credentials.account_group.clone();
        config.enabled_private_rest = true;
    }
}

fn hyperliquid_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HyperliquidGatewayConfig {
    let mut config = HyperliquidGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.account_address = Some(credentials.api_key.clone());
        config.signing_private_key = Some(credentials.api_secret.clone());
        config.vault_address = credentials.passphrase.clone();
        config.enabled_private_rest = true;
        config.enabled_private_streams = true;
    }
    config
}

fn independentreserve_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> IndependentReserveGatewayConfig {
    let mut config = IndependentReserveGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn indodax_config(
    rest_base_url: Option<String>,
    public_rest_base_url: Option<String>,
    private_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> IndodaxGatewayConfig {
    let mut config = IndodaxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    apply_rest_base_url(&mut config.rest_base_url, public_rest_base_url);
    apply_rest_base_url(&mut config.rest_base_url, private_rest_base_url);
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

fn kucoinfutures_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> KuCoinFuturesGatewayConfig {
    let mut config = KuCoinFuturesGatewayConfig::default();
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

fn krakenfutures_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> KrakenFuturesGatewayConfig {
    let mut config = KrakenFuturesGatewayConfig::default();
    apply_rest_base_url(&mut config.futures_rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
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

fn luno_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> LunoGatewayConfig {
    let mut config = LunoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key_id = Some(credentials.api_key.clone());
        config.api_key_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
    }
    config
}

fn mercado_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> MercadoGatewayConfig {
    let mut config = MercadoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.bearer_token = Some(credentials.api_key.clone());
        config.account_id = credentials.passphrase.clone();
        config.api_key = None;
        config.api_secret = None;
        config.enabled_private_rest = false;
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

fn paradex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ParadexGatewayConfig {
    let mut config = ParadexGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.starknet_account = Some(credentials.api_key.clone());
        config.stark_private_key = Some(credentials.api_secret.clone());
        config.jwt_token = credentials.passphrase.clone();
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

fn upbit_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> UpbitGatewayConfig {
    let mut config = UpbitGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
        config.enabled_private_streams = true;
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
        adapter: "aster",
        api_key_keys: &[
            "RUSTCTA_ASTER_USER_ADDRESS",
            "ASTER_USER_ADDRESS",
            "ASTER_ACCOUNT_ADDRESS",
        ],
        api_secret_keys: &[
            "RUSTCTA_ASTER_SIGNER_PRIVATE_KEY",
            "ASTER_SIGNER_PRIVATE_KEY",
            "ASTER_PRIVATE_KEY",
        ],
        passphrase_keys: Some(&["RUSTCTA_ASTER_SIGNER_ADDRESS", "ASTER_SIGNER_ADDRESS"]),
        account_group_keys: None,
        requires_passphrase: true,
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
        adapter: "binancecoinm",
        api_key_keys: &["RUSTCTA_BINANCECOINM_API_KEY", "BINANCE_COINM_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_BINANCECOINM_API_SECRET",
            "BINANCE_COINM_API_SECRET",
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
        adapter: "bitbank",
        api_key_keys: &["RUSTCTA_BITBANK_API_KEY", "BITBANK_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITBANK_API_SECRET", "BITBANK_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitfinex",
        api_key_keys: &["RUSTCTA_BITFINEX_API_KEY", "BITFINEX_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITFINEX_API_SECRET", "BITFINEX_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitflyer",
        api_key_keys: &["RUSTCTA_BITFLYER_API_KEY", "BITFLYER_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITFLYER_API_SECRET", "BITFLYER_API_SECRET"],
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
        adapter: "bithumb",
        api_key_keys: &[
            "RUSTCTA_BITHUMB_API_KEY",
            "BITHUMB_API_KEY",
            "BITHUMB_ACCESS_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BITHUMB_API_SECRET",
            "BITHUMB_API_SECRET",
            "BITHUMB_SECRET_KEY",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
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
        adapter: "bitmart",
        api_key_keys: &["RUSTCTA_BITMART_API_KEY", "BITMART_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITMART_API_SECRET", "BITMART_API_SECRET"],
        passphrase_keys: Some(&[
            "RUSTCTA_BITMART_MEMO",
            "RUSTCTA_BITMART_API_MEMO",
            "BITMART_MEMO",
            "BITMART_API_MEMO",
        ]),
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitso",
        api_key_keys: &["RUSTCTA_BITSO_API_KEY", "BITSO_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITSO_API_SECRET", "BITSO_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitstamp",
        api_key_keys: &["RUSTCTA_BITSTAMP_API_KEY", "BITSTAMP_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITSTAMP_API_SECRET", "BITSTAMP_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: Some(&[
            "RUSTCTA_BITSTAMP_SUBACCOUNT_ID",
            "BITSTAMP_SUBACCOUNT_ID",
            "BITSTAMP_SUBACCOUNT",
        ]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitvavo",
        api_key_keys: &["RUSTCTA_BITVAVO_API_KEY", "BITVAVO_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITVAVO_API_SECRET", "BITVAVO_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "btcturk",
        api_key_keys: &["RUSTCTA_BTCTURK_API_KEY", "BTCTURK_API_KEY"],
        api_secret_keys: &["RUSTCTA_BTCTURK_API_SECRET", "BTCTURK_API_SECRET"],
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
        adapter: "bybit",
        api_key_keys: &["RUSTCTA_BYBIT_API_KEY", "BYBIT_API_KEY"],
        api_secret_keys: &["RUSTCTA_BYBIT_API_SECRET", "BYBIT_API_SECRET"],
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
        adapter: "btcmarkets",
        api_key_keys: &[
            "RUSTCTA_BTCMARKETS_API_KEY",
            "BTCMARKETS_API_KEY",
            "BTC_MARKETS_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BTCMARKETS_API_SECRET",
            "BTCMARKETS_API_SECRET",
            "BTC_MARKETS_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
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
        adapter: "coinbaseexchange",
        api_key_keys: &[
            "RUSTCTA_COINBASE_EXCHANGE_API_KEY",
            "RUSTCTA_COINBASEEXCHANGE_API_KEY",
            "COINBASE_EXCHANGE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_COINBASE_EXCHANGE_API_SECRET",
            "RUSTCTA_COINBASEEXCHANGE_API_SECRET",
            "COINBASE_EXCHANGE_API_SECRET",
        ],
        passphrase_keys: Some(&[
            "RUSTCTA_COINBASE_EXCHANGE_API_PASSPHRASE",
            "RUSTCTA_COINBASEEXCHANGE_API_PASSPHRASE",
            "COINBASE_EXCHANGE_API_PASSPHRASE",
        ]),
        account_group_keys: None,
        requires_passphrase: true,
    },
    PrivateCredentialSpec {
        adapter: "coincheck",
        api_key_keys: &["RUSTCTA_COINCHECK_API_KEY", "COINCHECK_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINCHECK_API_SECRET", "COINCHECK_API_SECRET"],
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
        adapter: "coinone",
        api_key_keys: &[
            "RUSTCTA_COINONE_ACCESS_TOKEN",
            "RUSTCTA_COINONE_API_KEY",
            "COINONE_ACCESS_TOKEN",
            "COINONE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_COINONE_SECRET_KEY",
            "RUSTCTA_COINONE_API_SECRET",
            "COINONE_SECRET_KEY",
            "COINONE_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "coinspot",
        api_key_keys: &["RUSTCTA_COINSPOT_API_KEY", "COINSPOT_API_KEY"],
        api_secret_keys: &["RUSTCTA_COINSPOT_API_SECRET", "COINSPOT_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "coinsph",
        api_key_keys: &[
            "RUSTCTA_COINSPH_API_KEY",
            "COINSPH_API_KEY",
            "COINS_PH_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_COINSPH_API_SECRET",
            "COINSPH_API_SECRET",
            "COINS_PH_API_SECRET",
        ],
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
        adapter: "derive",
        api_key_keys: &["RUSTCTA_DERIVE_SESSION_KEY", "DERIVE_SESSION_KEY"],
        api_secret_keys: &["RUSTCTA_DERIVE_SESSION_SECRET", "DERIVE_SESSION_SECRET"],
        passphrase_keys: Some(&["RUSTCTA_DERIVE_WALLET", "DERIVE_WALLET"]),
        account_group_keys: Some(&["RUSTCTA_DERIVE_SUBACCOUNT_ID", "DERIVE_SUBACCOUNT_ID"]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "dydx",
        api_key_keys: &["RUSTCTA_DYDX_WALLET_ADDRESS", "DYDX_WALLET_ADDRESS"],
        api_secret_keys: &[],
        passphrase_keys: Some(&["RUSTCTA_DYDX_SUBACCOUNT_NUMBER", "DYDX_SUBACCOUNT_NUMBER"]),
        account_group_keys: Some(&["RUSTCTA_DYDX_SUBACCOUNT_NUMBER", "DYDX_SUBACCOUNT_NUMBER"]),
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
        adapter: "gemini",
        api_key_keys: &["RUSTCTA_GEMINI_API_KEY", "GEMINI_API_KEY"],
        api_secret_keys: &["RUSTCTA_GEMINI_API_SECRET", "GEMINI_API_SECRET"],
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
        adapter: "htx",
        api_key_keys: &["RUSTCTA_HTX_API_KEY", "HTX_API_KEY"],
        api_secret_keys: &["RUSTCTA_HTX_API_SECRET", "HTX_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: Some(&["RUSTCTA_HTX_SPOT_ACCOUNT_ID", "HTX_SPOT_ACCOUNT_ID"]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "huobi",
        api_key_keys: &["RUSTCTA_HUOBI_API_KEY", "HUOBI_API_KEY"],
        api_secret_keys: &["RUSTCTA_HUOBI_API_SECRET", "HUOBI_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: Some(&["RUSTCTA_HUOBI_SPOT_ACCOUNT_ID", "HUOBI_SPOT_ACCOUNT_ID"]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "hyperliquid",
        api_key_keys: &[
            "RUSTCTA_HYPERLIQUID_ACCOUNT_ADDRESS",
            "HYPERLIQUID_ACCOUNT_ADDRESS",
            "HYPERLIQUID_WALLET_ADDRESS",
        ],
        api_secret_keys: &["RUSTCTA_HYPERLIQUID_SIGNING_KEY", "HYPERLIQUID_PRIVATE_KEY"],
        passphrase_keys: Some(&[
            "RUSTCTA_HYPERLIQUID_VAULT_ADDRESS",
            "HYPERLIQUID_VAULT_ADDRESS",
        ]),
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "independentreserve",
        api_key_keys: &[
            "RUSTCTA_INDEPENDENTRESERVE_API_KEY",
            "INDEPENDENTRESERVE_API_KEY",
            "INDEPENDENT_RESERVE_API_KEY",
            "IR_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_INDEPENDENTRESERVE_API_SECRET",
            "INDEPENDENTRESERVE_API_SECRET",
            "INDEPENDENT_RESERVE_API_SECRET",
            "IR_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "indodax",
        api_key_keys: &["RUSTCTA_INDODAX_API_KEY", "INDODAX_API_KEY"],
        api_secret_keys: &["RUSTCTA_INDODAX_API_SECRET", "INDODAX_API_SECRET"],
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
        adapter: "kucoinfutures",
        api_key_keys: &[
            "RUSTCTA_KUCOIN_FUTURES_API_KEY",
            "KUCOIN_FUTURES_API_KEY",
            "RUSTCTA_KUCOIN_API_KEY",
            "KUCOIN_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_KUCOIN_FUTURES_API_SECRET",
            "KUCOIN_FUTURES_API_SECRET",
            "RUSTCTA_KUCOIN_API_SECRET",
            "KUCOIN_API_SECRET",
        ],
        passphrase_keys: Some(&[
            "RUSTCTA_KUCOIN_FUTURES_API_PASSPHRASE",
            "KUCOIN_FUTURES_API_PASSPHRASE",
            "RUSTCTA_KUCOIN_API_PASSPHRASE",
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
        adapter: "krakenfutures",
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
        adapter: "paradex",
        api_key_keys: &[
            "RUSTCTA_PARADEX_STARKNET_ACCOUNT",
            "PARADEX_STARKNET_ACCOUNT",
        ],
        api_secret_keys: &[
            "RUSTCTA_PARADEX_STARK_PRIVATE_KEY",
            "PARADEX_STARK_PRIVATE_KEY",
        ],
        passphrase_keys: Some(&["RUSTCTA_PARADEX_JWT", "PARADEX_JWT"]),
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "mercado",
        api_key_keys: &["RUSTCTA_MERCADO_BEARER_TOKEN", "MERCADO_BEARER_TOKEN"],
        api_secret_keys: &[],
        passphrase_keys: Some(&["RUSTCTA_MERCADO_ACCOUNT_ID", "MERCADO_ACCOUNT_ID"]),
        account_group_keys: None,
        requires_passphrase: true,
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
        adapter: "luno",
        api_key_keys: &[
            "RUSTCTA_LUNO_API_KEY_ID",
            "RUSTCTA_LUNO_API_KEY",
            "LUNO_API_KEY_ID",
            "LUNO_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_LUNO_API_KEY_SECRET",
            "RUSTCTA_LUNO_API_SECRET",
            "LUNO_API_KEY_SECRET",
            "LUNO_API_SECRET",
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
        adapter: "upbit",
        api_key_keys: &["RUSTCTA_UPBIT_API_KEY", "UPBIT_API_KEY", "UPBIT_ACCESS_KEY"],
        api_secret_keys: &[
            "RUSTCTA_UPBIT_API_SECRET",
            "UPBIT_API_SECRET",
            "UPBIT_SECRET_KEY",
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
    fn config_should_parse_bitflyer_and_bitbank_without_exposing_secrets() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bitflyer,bitbank"),
            ("RUSTCTA_BITFLYER_REST_BASE_URL", "http://127.0.0.1:9201"),
            (
                "RUSTCTA_BITBANK_PUBLIC_REST_BASE_URL",
                "http://127.0.0.1:9202",
            ),
            (
                "RUSTCTA_BITBANK_PRIVATE_REST_BASE_URL",
                "http://127.0.0.1:9203/v1",
            ),
            ("RUSTCTA_BITFLYER_API_KEY", "bitflyer-key"),
            ("RUSTCTA_BITFLYER_API_SECRET", "bitflyer-secret"),
            ("RUSTCTA_BITBANK_API_KEY", "bitbank-key"),
            ("RUSTCTA_BITBANK_API_SECRET", "bitbank-secret"),
        ]));

        assert_eq!(config.adapters, vec!["bitflyer", "bitbank"]);
        assert_eq!(
            config.rest_base_urls.get("bitflyer").as_deref(),
            Some("http://127.0.0.1:9201")
        );
        assert_eq!(
            config.rest_base_urls.get("bitbank_public").as_deref(),
            Some("http://127.0.0.1:9202")
        );
        assert!(config.private_credentials.contains("bitflyer"));
        assert!(config.private_credentials.contains("bitbank"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bitflyer-secret"));
        assert!(!debug.contains("bitbank-secret"));
    }

    #[test]
    fn config_should_parse_htx_and_huobi_without_exposing_secrets() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "htx,huobi"),
            (
                "RUSTCTA_HTX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9040/spot",
            ),
            (
                "RUSTCTA_HTX_LINEAR_REST_BASE_URL",
                "http://127.0.0.1:9041/linear",
            ),
            (
                "RUSTCTA_HUOBI_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9042/spot",
            ),
            (
                "RUSTCTA_HUOBI_LINEAR_REST_BASE_URL",
                "http://127.0.0.1:9043/linear",
            ),
            ("RUSTCTA_HTX_API_KEY", "htx-key"),
            ("RUSTCTA_HTX_API_SECRET", "htx-secret"),
            ("RUSTCTA_HTX_SPOT_ACCOUNT_ID", "htx-account"),
            ("RUSTCTA_HUOBI_API_KEY", "huobi-key"),
            ("RUSTCTA_HUOBI_API_SECRET", "huobi-secret"),
            ("RUSTCTA_HUOBI_SPOT_ACCOUNT_ID", "huobi-account"),
        ]));

        assert_eq!(config.adapters, vec!["htx", "huobi"]);
        assert_eq!(
            config.rest_base_urls.get("htx_spot").as_deref(),
            Some("http://127.0.0.1:9040/spot")
        );
        assert_eq!(
            config.rest_base_urls.get("htx_linear").as_deref(),
            Some("http://127.0.0.1:9041/linear")
        );
        assert_eq!(
            config.rest_base_urls.get("huobi_spot").as_deref(),
            Some("http://127.0.0.1:9042/spot")
        );
        assert_eq!(
            config.rest_base_urls.get("huobi_linear").as_deref(),
            Some("http://127.0.0.1:9043/linear")
        );
        assert!(config.private_credentials.contains("htx"));
        assert!(config.private_credentials.contains("huobi"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("htx-secret"));
        assert!(!debug.contains("htx-account"));
        assert!(!debug.contains("huobi-secret"));
        assert!(!debug.contains("huobi-account"));
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

    #[test]
    fn bitso_mercado_config_should_parse_redacted_credentials_without_enabling_live_writes() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bitso,mercado"),
            (
                "RUSTCTA_BITSO_REST_BASE_URL",
                "http://127.0.0.1:9101/api/v3",
            ),
            (
                "RUSTCTA_MERCADO_REST_BASE_URL",
                "http://127.0.0.1:9102/api/v4",
            ),
            ("RUSTCTA_BITSO_API_KEY", "bitso-key"),
            ("RUSTCTA_BITSO_API_SECRET", "bitso-secret"),
            ("RUSTCTA_MERCADO_BEARER_TOKEN", "mercado-token"),
            ("RUSTCTA_MERCADO_ACCOUNT_ID", "mercado-account"),
        ]));

        assert_eq!(config.adapters, vec!["bitso", "mercado"]);
        assert_eq!(
            config.rest_base_urls.get("bitso").as_deref(),
            Some("http://127.0.0.1:9101/api/v3")
        );
        assert_eq!(
            config.rest_base_urls.get("mercado").as_deref(),
            Some("http://127.0.0.1:9102/api/v4")
        );
        assert!(config.private_credentials.contains("bitso"));
        assert!(config.private_credentials.contains("mercado"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bitso-secret"));
        assert!(!debug.contains("mercado-token"));
        assert!(!debug.contains("mercado-account"));
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
    async fn config_should_wire_task14_bitvavo_and_gemini_private_adapters() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bitvavo,gemini"),
            ("RUSTCTA_BITVAVO_REST_BASE_URL", "http://127.0.0.1:9140"),
            ("RUSTCTA_BITVAVO_API_KEY", "bitvavo-key"),
            ("RUSTCTA_BITVAVO_API_SECRET", "bitvavo-secret"),
            ("RUSTCTA_GEMINI_REST_BASE_URL", "http://127.0.0.1:9141"),
            ("RUSTCTA_GEMINI_API_KEY", "gemini-key"),
            ("RUSTCTA_GEMINI_API_SECRET", "gemini-secret"),
        ]));
        assert_eq!(config.adapters, vec!["bitvavo", "gemini"]);
        assert_eq!(
            config.rest_base_urls.get("bitvavo").as_deref(),
            Some("http://127.0.0.1:9140")
        );
        assert_eq!(
            config.rest_base_urls.get("gemini").as_deref(),
            Some("http://127.0.0.1:9141")
        );
        assert!(config.private_credentials.contains("bitvavo"));
        assert!(config.private_credentials.contains("gemini"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bitvavo-secret"));
        assert!(!debug.contains("gemini-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "task14-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("task14-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![
                        ExchangeId::new("bitvavo").expect("bitvavo"),
                        ExchangeId::new("gemini").expect("gemini"),
                    ],
                },
            )
            .await
            .expect("capabilities");

        let bitvavo = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "bitvavo")
            .expect("bitvavo capabilities");
        assert!(bitvavo.supports_private_rest);
        assert!(bitvavo.supports_balances);
        assert!(bitvavo.supports_place_order);
        assert!(bitvavo.supports_private_streams);

        let gemini = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "gemini")
            .expect("gemini capabilities");
        assert!(gemini.supports_private_rest);
        assert!(gemini.supports_balances);
        assert!(gemini.supports_place_order);
        assert!(gemini.supports_private_streams);
    }

    #[tokio::test]
    async fn config_should_wire_task11_aster_and_bitstamp_private_adapters() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "aster,bitstamp"),
            ("RUSTCTA_ASTER_REST_BASE_URL", "http://127.0.0.1:9050"),
            (
                "RUSTCTA_ASTER_USER_ADDRESS",
                "0x1111111111111111111111111111111111111111",
            ),
            (
                "RUSTCTA_ASTER_SIGNER_ADDRESS",
                "0x2222222222222222222222222222222222222222",
            ),
            (
                "RUSTCTA_ASTER_SIGNER_PRIVATE_KEY",
                "fixture-aster-private-key",
            ),
            ("RUSTCTA_BITSTAMP_REST_BASE_URL", "http://127.0.0.1:9051"),
            ("RUSTCTA_BITSTAMP_API_KEY", "bitstamp-key"),
            ("RUSTCTA_BITSTAMP_API_SECRET", "bitstamp-secret"),
            ("RUSTCTA_BITSTAMP_SUBACCOUNT_ID", "fixture-subaccount"),
        ]));
        assert_eq!(
            config.rest_base_urls.get("aster").as_deref(),
            Some("http://127.0.0.1:9050")
        );
        assert_eq!(
            config.rest_base_urls.get("bitstamp").as_deref(),
            Some("http://127.0.0.1:9051")
        );
        assert!(config.private_credentials.contains("aster"));
        assert!(config.private_credentials.contains("bitstamp"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fixture-aster-private-key"));
        assert!(!debug.contains("bitstamp-secret"));
        assert!(!debug.contains("fixture-subaccount"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "task11-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("task11-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![
                        ExchangeId::new("aster").expect("aster"),
                        ExchangeId::new("bitstamp").expect("bitstamp"),
                    ],
                },
            )
            .await
            .expect("capabilities");

        let aster = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "aster")
            .expect("aster capabilities");
        assert!(aster.supports_private_rest);
        assert!(aster.supports_private_streams);
        assert!(aster.supports_positions);
        assert!(aster.supports_place_order);
        assert!(!aster.supports_quote_market_order);
        assert!(!aster.supports_amend_order);

        let bitstamp = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "bitstamp")
            .expect("bitstamp capabilities");
        assert!(bitstamp.supports_private_rest);
        assert!(bitstamp.supports_private_streams);
        assert!(bitstamp.supports_balances);
        assert!(bitstamp.supports_fees);
        assert!(bitstamp.supports_place_order);
        assert!(bitstamp.supports_recent_fills);
        assert!(!bitstamp.supports_positions);
    }

    #[tokio::test]
    async fn config_should_wire_btcturk_gateway_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "btcturk"),
            ("RUSTCTA_BTCTURK_REST_BASE_URL", "http://127.0.0.1:9040"),
            ("RUSTCTA_BTCTURK_API_KEY", "btcturk-key"),
            ("RUSTCTA_BTCTURK_API_SECRET", "btcturk-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "btcturk-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("btcturk-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("btcturk").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_luno_gateway_adapter_without_promoting_payment_rails() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "luno"),
            ("RUSTCTA_LUNO_REST_BASE_URL", "http://127.0.0.1:9041"),
            ("RUSTCTA_LUNO_API_KEY_ID", "luno-key"),
            ("RUSTCTA_LUNO_API_KEY_SECRET", "luno-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "luno-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("luno-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("luno").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_task1_bybit_and_binancecoinm_private_gateway_adapters() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bybit,binancecoinm"),
            ("RUSTCTA_BYBIT_REST_BASE_URL", "http://127.0.0.1:9050"),
            ("RUSTCTA_BYBIT_API_KEY", "bybit-key"),
            ("RUSTCTA_BYBIT_API_SECRET", "bybit-secret"),
            (
                "RUSTCTA_BINANCECOINM_REST_BASE_URL",
                "http://127.0.0.1:9051",
            ),
            ("RUSTCTA_BINANCECOINM_API_KEY", "coinm-key"),
            ("RUSTCTA_BINANCECOINM_API_SECRET", "coinm-secret"),
        ]));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bybit-secret"));
        assert!(!debug.contains("coinm-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "task1-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("task1-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![
                        ExchangeId::new("bybit").expect("bybit"),
                        ExchangeId::new("binancecoinm").expect("binancecoinm"),
                    ],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 2);
        assert!(response
            .capabilities
            .iter()
            .any(|capability| capability.exchange.as_str() == "bybit"
                && capability.supports_private_rest
                && capability.supports_public_streams));
        assert!(response
            .capabilities
            .iter()
            .any(|capability| capability.exchange.as_str() == "binancecoinm"
                && capability.supports_private_rest
                && capability.supports_positions));
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
    async fn config_should_wire_krakenfutures_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "krakenfutures"),
            (
                "RUSTCTA_KRAKEN_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9004/derivatives/api/v3",
            ),
            ("RUSTCTA_KRAKEN_FUTURES_API_KEY", "kraken-futures-key"),
            ("RUSTCTA_KRAKEN_FUTURES_API_SECRET", "kraken-futures-secret"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "krakenfutures-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("krakenfutures-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("krakenfutures").expect("exchange")],
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
        assert!(!response.capabilities[0].supports_quote_market_order);
    }

    #[tokio::test]
    async fn config_should_wire_kucoinfutures_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "kucoinfutures"),
            (
                "RUSTCTA_KUCOIN_FUTURES_REST_BASE_URL",
                "http://127.0.0.1:9005",
            ),
            ("RUSTCTA_KUCOIN_FUTURES_API_KEY", "kucoin-futures-key"),
            ("RUSTCTA_KUCOIN_FUTURES_API_SECRET", "kucoin-futures-secret"),
            (
                "RUSTCTA_KUCOIN_FUTURES_API_PASSPHRASE",
                "kucoin-futures-passphrase",
            ),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "kucoinfutures-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("kucoinfutures-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("kucoinfutures").expect("exchange")],
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
        assert!(!response.capabilities[0].supports_quote_market_order);
        assert!(!response.capabilities[0].supports_amend_order);
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
        assert!(!response.capabilities[0].supports_private_streams);
        assert!(response.capabilities[0].supports_batch_place_order);
        assert!(response.capabilities[0].supports_batch_cancel_order);
        assert!(response.capabilities[0].supports_quote_market_order);
        let private_streams = response.capabilities[0]
            .private_stream_capabilities
            .as_ref()
            .expect("private stream capabilities");
        assert!(!private_streams.supports_orders);
        assert!(!private_streams.supports_fills);
        assert!(!private_streams.supports_balances);
        assert!(!private_streams.supports_positions);
        assert!(!private_streams.supports_account);
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

    #[tokio::test]
    async fn config_should_wire_htx_and_huobi_private_gateway_adapters() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "htx,huobi"),
            (
                "RUSTCTA_HTX_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9040/spot",
            ),
            (
                "RUSTCTA_HTX_LINEAR_REST_BASE_URL",
                "http://127.0.0.1:9041/linear",
            ),
            (
                "RUSTCTA_HUOBI_SPOT_REST_BASE_URL",
                "http://127.0.0.1:9042/spot",
            ),
            (
                "RUSTCTA_HUOBI_LINEAR_REST_BASE_URL",
                "http://127.0.0.1:9043/linear",
            ),
            ("RUSTCTA_HTX_API_KEY", "htx-key"),
            ("RUSTCTA_HTX_API_SECRET", "htx-secret"),
            ("RUSTCTA_HTX_SPOT_ACCOUNT_ID", "1001"),
            ("RUSTCTA_HUOBI_API_KEY", "huobi-key"),
            ("RUSTCTA_HUOBI_API_SECRET", "huobi-secret"),
            ("RUSTCTA_HUOBI_SPOT_ACCOUNT_ID", "1002"),
        ]));
        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "htx-huobi-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("htx-huobi-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![
                        ExchangeId::new("htx").expect("htx"),
                        ExchangeId::new("huobi").expect("huobi"),
                    ],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 2);
        assert_htx_like_capabilities(&response, "htx");
        assert_htx_like_capabilities(&response, "huobi");
    }

    fn assert_htx_like_capabilities(
        response: &rustcta_exchange_gateway::GetCapabilitiesResponse,
        exchange: &str,
    ) {
        let capabilities = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == exchange)
            .expect("capabilities");
        assert!(capabilities.supports_private_rest);
        assert!(capabilities.supports_public_streams);
        assert!(capabilities.supports_private_streams);
        assert!(capabilities.supports_balances);
        assert!(capabilities.supports_positions);
        assert!(capabilities.supports_query_order);
        assert!(capabilities.supports_open_orders);
        assert!(capabilities.supports_recent_fills);
        assert_eq!(
            capabilities.market_types,
            vec![
                rustcta_exchange_api::MarketType::Spot,
                rustcta_exchange_api::MarketType::Perpetual
            ]
        );
    }

    #[tokio::test]
    async fn config_should_wire_task8_dex_gateway_adapters_with_redacted_env_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "paradex,derive"),
            ("RUSTCTA_PARADEX_REST_BASE_URL", "http://127.0.0.1:9020"),
            (
                "RUSTCTA_PARADEX_STARKNET_ACCOUNT",
                "fixture-paradex-account",
            ),
            ("RUSTCTA_PARADEX_STARK_PRIVATE_KEY", "fixture-stark-key"),
            ("RUSTCTA_PARADEX_JWT", "fixture.jwt"),
            ("RUSTCTA_DERIVE_REST_BASE_URL", "http://127.0.0.1:9021"),
            ("RUSTCTA_DERIVE_SESSION_KEY", "fixture-session-key"),
            ("RUSTCTA_DERIVE_SESSION_SECRET", "fixture-session-material"),
            ("RUSTCTA_DERIVE_SUBACCOUNT_ID", "fixture-subaccount"),
            ("RUSTCTA_DERIVE_WALLET", "fixture-wallet"),
        ]));
        assert!(format!("{:?}", config.private_credentials).contains("<redacted>"));
        assert!(!format!("{:?}", config.private_credentials).contains("fixture-stark-key"));
        assert!(!format!("{:?}", config.private_credentials).contains("fixture-session-material"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "task8-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("task8-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![
                        ExchangeId::new("paradex").expect("paradex"),
                        ExchangeId::new("derive").expect("derive"),
                    ],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 2);
        let paradex = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "paradex")
            .expect("paradex capabilities");
        assert!(paradex.supports_private_rest);
        assert!(paradex.supports_positions);
        assert!(!paradex.supports_place_order);
        assert!(paradex
            .market_types
            .contains(&rustcta_exchange_api::MarketType::Option));

        let derive = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "derive")
            .expect("derive capabilities");
        assert!(derive.supports_private_rest);
        assert!(derive.supports_balances);
        assert!(derive.supports_positions);
        assert!(!derive.supports_place_order);
        assert!(derive
            .market_types
            .contains(&rustcta_exchange_api::MarketType::Option));
    }

    #[tokio::test]
    async fn config_should_wire_task5_dex_gateway_adapters_with_redacted_env_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "hyperliquid,dydx"),
            ("RUSTCTA_HYPERLIQUID_REST_BASE_URL", "http://127.0.0.1:9030"),
            (
                "RUSTCTA_HYPERLIQUID_ACCOUNT_ADDRESS",
                "0x1111111111111111111111111111111111111111",
            ),
            (
                "RUSTCTA_HYPERLIQUID_SIGNING_KEY",
                "fixture-hyperliquid-private-key",
            ),
            (
                "RUSTCTA_HYPERLIQUID_VAULT_ADDRESS",
                "0x2222222222222222222222222222222222222222",
            ),
            (
                "RUSTCTA_DYDX_INDEXER_REST_BASE_URL",
                "http://127.0.0.1:9031",
            ),
            (
                "RUSTCTA_DYDX_WALLET_ADDRESS",
                "dydx1fixturewalletaddress000000000000000000",
            ),
            ("RUSTCTA_DYDX_SUBACCOUNT_NUMBER", "7"),
        ]));
        assert!(config.private_credentials.contains("hyperliquid"));
        assert!(config.private_credentials.contains("dydx"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fixture-hyperliquid-private-key"));
        assert!(!debug.contains("dydx1fixturewalletaddress"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "task5-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("task5-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![
                        ExchangeId::new("hyperliquid").expect("hyperliquid"),
                        ExchangeId::new("dydx").expect("dydx"),
                    ],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 2);
        let hyperliquid = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "hyperliquid")
            .expect("hyperliquid capabilities");
        assert!(hyperliquid.supports_private_rest);
        assert!(hyperliquid.supports_place_order);
        assert!(hyperliquid
            .market_types
            .contains(&rustcta_exchange_api::MarketType::Perpetual));

        let dydx = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "dydx")
            .expect("dydx capabilities");
        assert!(dydx.supports_private_rest);
        assert!(dydx.supports_positions);
        assert!(!dydx.supports_place_order);
        assert!(dydx
            .market_types
            .contains(&rustcta_exchange_api::MarketType::Perpetual));
    }
}
