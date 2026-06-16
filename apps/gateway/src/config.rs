use std::collections::HashMap;

use anyhow::Result;
use rustcta_exchange_gateway::{
    AarkGatewayConfig, AdapterBackedGateway, AftermathGatewayConfig, AlpacaGatewayConfig,
    ApolloxDexGatewayConfig, ArkhamGatewayConfig, AscendexGatewayConfig, AsterGatewayConfig,
    BackpackGatewayConfig, BiconomyGatewayConfig, BigOneGatewayConfig, BinanceCoinMGatewayConfig,
    BinanceGatewayConfig, BinanceUsGatewayConfig, BingxGatewayConfig, Bit2cGatewayConfig,
    BitbankGatewayConfig, BitbnsGatewayConfig, BitfinexGatewayConfig, BitflyerGatewayConfig,
    BitgetGatewayConfig, BithumbGatewayConfig, BitkanGatewayConfig, BitmartGatewayConfig,
    BitoproGatewayConfig, BitsoGatewayConfig, BitstampGatewayConfig, BittradeGatewayConfig,
    BitunixGatewayConfig, BitvavoGatewayConfig, BlockchainComGatewayConfig, BlofinGatewayConfig,
    BsxGatewayConfig, BtcMarketsGatewayConfig, BtcTurkGatewayConfig, BtcboxGatewayConfig,
    BybitGatewayConfig, BybiteuGatewayConfig, BydfiGatewayConfig, CexGatewayConfig,
    Cod3xGatewayConfig, CoinDcxGatewayConfig, CoinExGatewayConfig, CoinbaseExchangeGatewayConfig,
    CoincheckGatewayConfig, CoinmateGatewayConfig, CoinoneGatewayConfig, CoinsPhGatewayConfig,
    CoinspotGatewayConfig, CoinstoreGatewayConfig, CointrGatewayConfig, CryptoComGatewayConfig,
    CryptomusGatewayConfig, D8xGatewayConfig, DeepcoinGatewayConfig, DeriveChainPerpsGatewayConfig,
    DeriveGatewayConfig, DydxGatewayConfig, EquationGatewayConfig, ExmoGatewayConfig,
    FmfwioGatewayConfig, FoxbitGatewayConfig, GateIoGatewayConfig, GeminiGatewayConfig,
    HashKeyGlobalGatewayConfig, HibachiGatewayConfig, HitbtcGatewayConfig, HollaexGatewayConfig,
    HtxGatewayConfig, HuobiGatewayConfig, HyperliquidGatewayConfig,
    IndependentReserveGatewayConfig, IndodaxGatewayConfig, KrakenFuturesGatewayConfig,
    KrakenGatewayConfig, KuCoinFuturesGatewayConfig, KuCoinGatewayConfig, LBankGatewayConfig,
    LatokenGatewayConfig, LunoGatewayConfig, MangoMarketsGatewayConfig, MercadoGatewayConfig,
    MexcGatewayConfig, ModetradeGatewayConfig, MyOkxGatewayConfig, NdaxGatewayConfig,
    NovadaxGatewayConfig, OkxGatewayConfig, OkxusGatewayConfig, OneTradingGatewayConfig,
    OrangeXGatewayConfig, P2bGatewayConfig, ParadexGatewayConfig, PaymiumGatewayConfig,
    PhemexGatewayConfig, TokocryptoGatewayConfig, UpbitGatewayConfig, WavesExchangeGatewayConfig,
    WeexGatewayConfig, WoofiproGatewayConfig, YobitGatewayConfig, ZaifGatewayConfig,
    ZebpayGatewayConfig, ZetaMarketsGatewayConfig,
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
            ("aark", "RUSTCTA_AARK_REST_BASE_URL"),
            ("aftermath", "RUSTCTA_AFTERMATH_REST_BASE_URL"),
            ("alpaca", "RUSTCTA_ALPACA_REST_BASE_URL"),
            ("alpaca_broker", "RUSTCTA_ALPACA_BROKER_REST_BASE_URL"),
            (
                "alpaca_market_data",
                "RUSTCTA_ALPACA_MARKET_DATA_REST_BASE_URL",
            ),
            ("apollox_dex", "RUSTCTA_APOLLOX_DEX_REST_BASE_URL"),
            ("apollox_dex", "APOLLOX_DEX_REST_BASE_URL"),
            ("arkham", "RUSTCTA_ARKHAM_REST_BASE_URL"),
            ("ascendex", "RUSTCTA_ASCENDEX_REST_BASE_URL"),
            ("aster", "RUSTCTA_ASTER_REST_BASE_URL"),
            ("backpack", "RUSTCTA_BACKPACK_REST_BASE_URL"),
            ("biconomy", "RUSTCTA_BICONOMY_REST_BASE_URL"),
            ("binance", "RUSTCTA_BINANCE_REST_BASE_URL"),
            ("binancecoinm", "RUSTCTA_BINANCECOINM_REST_BASE_URL"),
            ("binanceus", "RUSTCTA_BINANCEUS_REST_BASE_URL"),
            ("binanceus", "RUSTCTA_BINANCE_US_REST_BASE_URL"),
            ("bigone", "RUSTCTA_BIGONE_REST_BASE_URL"),
            ("bigone_spot", "RUSTCTA_BIGONE_SPOT_REST_BASE_URL"),
            ("bigone_contract", "RUSTCTA_BIGONE_CONTRACT_REST_BASE_URL"),
            ("bingx", "RUSTCTA_BINGX_REST_BASE_URL"),
            ("bit2c", "RUSTCTA_BIT2C_REST_BASE_URL"),
            ("bitbank", "RUSTCTA_BITBANK_REST_BASE_URL"),
            ("bitbank_public", "RUSTCTA_BITBANK_PUBLIC_REST_BASE_URL"),
            ("bitbank_private", "RUSTCTA_BITBANK_PRIVATE_REST_BASE_URL"),
            ("bitbns", "RUSTCTA_BITBNS_REST_BASE_URL"),
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
            ("bitopro", "RUSTCTA_BITOPRO_REST_BASE_URL"),
            ("bitso", "RUSTCTA_BITSO_REST_BASE_URL"),
            ("bitstamp", "RUSTCTA_BITSTAMP_REST_BASE_URL"),
            ("bittrade", "RUSTCTA_BITTRADE_REST_BASE_URL"),
            ("bitvavo", "RUSTCTA_BITVAVO_REST_BASE_URL"),
            ("blockchaincom", "RUSTCTA_BLOCKCHAINCOM_REST_BASE_URL"),
            ("blockchaincom", "RUSTCTA_BLOCKCHAIN_COM_REST_BASE_URL"),
            ("btcturk", "RUSTCTA_BTCTURK_REST_BASE_URL"),
            ("btcbox", "RUSTCTA_BTCBOX_REST_BASE_URL"),
            ("bitunix", "RUSTCTA_BITUNIX_REST_BASE_URL"),
            ("bybit", "RUSTCTA_BYBIT_REST_BASE_URL"),
            ("bybiteu", "RUSTCTA_BYBITEU_REST_BASE_URL"),
            ("bybiteu", "RUSTCTA_BYBIT_EU_REST_BASE_URL"),
            ("bydfi", "RUSTCTA_BYDFI_REST_BASE_URL"),
            ("cex", "RUSTCTA_CEX_REST_BASE_URL"),
            ("cex", "RUSTCTA_CEXIO_REST_BASE_URL"),
            ("bitunix_spot", "RUSTCTA_BITUNIX_SPOT_REST_BASE_URL"),
            ("bitunix_futures", "RUSTCTA_BITUNIX_FUTURES_REST_BASE_URL"),
            ("blofin", "RUSTCTA_BLOFIN_REST_BASE_URL"),
            ("bsx", "RUSTCTA_BSX_REST_BASE_URL"),
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
            ("coinmate", "RUSTCTA_COINMATE_REST_BASE_URL"),
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
            ("cod3x", "RUSTCTA_COD3X_DOCS_BASE_URL"),
            ("cod3x", "RUSTCTA_COD3X_REST_BASE_URL"),
            ("cod3x_app", "RUSTCTA_COD3X_APP_BASE_URL"),
            ("cod3x_website", "RUSTCTA_COD3X_WEBSITE_BASE_URL"),
            ("cryptocom", "RUSTCTA_CRYPTOCOM_REST_BASE_URL"),
            ("cryptomus", "RUSTCTA_CRYPTOMUS_REST_BASE_URL"),
            ("d8x", "RUSTCTA_D8X_REST_BASE_URL"),
            ("deepcoin", "RUSTCTA_DEEPCOIN_REST_BASE_URL"),
            ("derive", "RUSTCTA_DERIVE_REST_BASE_URL"),
            (
                "derive_chain_perps",
                "RUSTCTA_DERIVE_CHAIN_PERPS_REST_BASE_URL",
            ),
            (
                "derive_chain_perps_rpc",
                "RUSTCTA_DERIVE_CHAIN_PERPS_RPC_URL",
            ),
            ("dydx", "RUSTCTA_DYDX_INDEXER_REST_BASE_URL"),
            ("dydx_node", "RUSTCTA_DYDX_NODE_REST_BASE_URL"),
            ("equation", "RUSTCTA_EQUATION_DOCS_BASE_URL"),
            ("equation", "RUSTCTA_EQUATION_REST_BASE_URL"),
            ("equation_app", "RUSTCTA_EQUATION_APP_BASE_URL"),
            ("exmo", "RUSTCTA_EXMO_REST_BASE_URL"),
            ("fmfwio", "RUSTCTA_FMFWIO_REST_BASE_URL"),
            ("foxbit", "RUSTCTA_FOXBIT_REST_BASE_URL"),
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
            ("hollaex", "RUSTCTA_HOLLAEX_REST_BASE_URL"),
            ("hibachi", "RUSTCTA_HIBACHI_REST_BASE_URL"),
            ("hibachi", "RUSTCTA_HIBACHI_DATA_REST_BASE_URL"),
            ("hibachi_account", "RUSTCTA_HIBACHI_ACCOUNT_REST_BASE_URL"),
            ("hitbtc", "RUSTCTA_HITBTC_REST_BASE_URL"),
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
            ("latoken", "RUSTCTA_LATOKEN_REST_BASE_URL"),
            ("lbank", "RUSTCTA_LBANK_REST_BASE_URL"),
            ("lbank_spot", "RUSTCTA_LBANK_SPOT_REST_BASE_URL"),
            ("lbank_contract", "RUSTCTA_LBANK_CONTRACT_REST_BASE_URL"),
            ("luno", "RUSTCTA_LUNO_REST_BASE_URL"),
            ("mango_markets", "RUSTCTA_MANGO_MARKETS_SOLANA_RPC_URL"),
            ("mercado", "RUSTCTA_MERCADO_REST_BASE_URL"),
            ("mexc", "RUSTCTA_MEXC_REST_BASE_URL"),
            ("modetrade", "RUSTCTA_MODETRADE_REST_BASE_URL"),
            ("woofipro", "RUSTCTA_WOOFIPRO_REST_BASE_URL"),
            ("myokx", "RUSTCTA_MYOKX_REST_BASE_URL"),
            ("myokx", "MYOKX_REST_BASE_URL"),
            ("ndax", "RUSTCTA_NDAX_REST_BASE_URL"),
            ("novadax", "RUSTCTA_NOVADAX_REST_BASE_URL"),
            ("okx", "RUSTCTA_OKX_REST_BASE_URL"),
            ("okxus", "RUSTCTA_OKXUS_REST_BASE_URL"),
            ("okxus", "RUSTCTA_OKX_US_REST_BASE_URL"),
            ("onetrading", "RUSTCTA_ONETRADING_REST_BASE_URL"),
            ("onetrading", "RUSTCTA_ONE_TRADING_REST_BASE_URL"),
            ("orangex", "RUSTCTA_ORANGEX_REST_BASE_URL"),
            ("p2b", "RUSTCTA_P2B_REST_BASE_URL"),
            ("p2b", "RUSTCTA_P2PB2B_REST_BASE_URL"),
            ("paradex", "RUSTCTA_PARADEX_REST_BASE_URL"),
            ("paymium", "RUSTCTA_PAYMIUM_REST_BASE_URL"),
            ("phemex", "RUSTCTA_PHEMEX_REST_BASE_URL"),
            ("tokocrypto", "RUSTCTA_TOKOCRYPTO_REST_BASE_URL"),
            (
                "tokocrypto_market",
                "RUSTCTA_TOKOCRYPTO_MARKET_REST_BASE_URL",
            ),
            (
                "tokocrypto_nextme",
                "RUSTCTA_TOKOCRYPTO_NEXTME_REST_BASE_URL",
            ),
            ("upbit", "RUSTCTA_UPBIT_REST_BASE_URL"),
            ("wavesexchange", "RUSTCTA_WAVESEXCHANGE_REST_BASE_URL"),
            ("weex", "RUSTCTA_WEEX_REST_BASE_URL"),
            ("weex_spot", "RUSTCTA_WEEX_SPOT_REST_BASE_URL"),
            ("weex_contract", "RUSTCTA_WEEX_CONTRACT_REST_BASE_URL"),
            ("yobit", "RUSTCTA_YOBIT_REST_BASE_URL"),
            ("zaif", "RUSTCTA_ZAIF_REST_BASE_URL"),
            ("zaif_public", "RUSTCTA_ZAIF_PUBLIC_REST_BASE_URL"),
            ("zaif_private", "RUSTCTA_ZAIF_PRIVATE_REST_BASE_URL"),
            ("zeta_markets", "RUSTCTA_ZETA_MARKETS_REST_BASE_URL"),
            ("zeta_markets", "RUSTCTA_ZETAMARKETS_REST_BASE_URL"),
            ("zebpay", "RUSTCTA_ZEBPAY_REST_BASE_URL"),
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
            let token_only_credentials =
                matches!(spec.adapter, "dydx" | "mercado" | "onetrading" | "orangex")
                    && (spec.adapter == "onetrading"
                        || passphrase.as_ref().is_some_and(|value| !value.is_empty()));
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
        "aark" | "aark_digital" | "aark-digital" => gateway.register_aark_adapter(aark_config(
            rest_base_urls.get("aark"),
            private_credentials.get("aark"),
        ))?,
        "aftermath" => {
            gateway.register_aftermath_adapter(aftermath_config(rest_base_urls.get("aftermath")))?
        }
        "alpaca" => gateway.register_alpaca_adapter(alpaca_config(
            rest_base_urls.get("alpaca"),
            rest_base_urls.get("alpaca_broker"),
            rest_base_urls.get("alpaca_market_data"),
            private_credentials.get("alpaca"),
        ))?,
        "apollox_dex" | "apollox-dex" | "apollox" | "apx" => {
            gateway.register_apollox_dex_adapter(apollox_dex_config(
                rest_base_urls.get("apollox_dex"),
                private_credentials.get("apollox_dex"),
            ))?
        }
        "ascendex" | "ascend_ex" => gateway.register_ascendex_adapter(ascendex_config(
            rest_base_urls.get("ascendex"),
            private_credentials.get("ascendex"),
        ))?,
        "arkham" => gateway.register_arkham_adapter(arkham_config(
            rest_base_urls.get("arkham"),
            private_credentials.get("arkham"),
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
        "binanceus" | "binance_us" | "binance-us" => {
            gateway.register_binanceus_adapter(binanceus_config(
                rest_base_urls.get("binanceus"),
                private_credentials.get("binanceus"),
            ))?
        }
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
        "bit2c" | "bit_2c" | "bit2c.co.il" => gateway.register_bit2c_adapter(bit2c_config(
            rest_base_urls.get("bit2c"),
            private_credentials.get("bit2c"),
        ))?,
        "bitbank" => gateway.register_bitbank_adapter(bitbank_config(
            rest_base_urls.get("bitbank"),
            rest_base_urls.get("bitbank_public"),
            rest_base_urls.get("bitbank_private"),
            private_credentials.get("bitbank"),
        ))?,
        "bitbns" => gateway.register_bitbns_adapter(bitbns_config(
            rest_base_urls.get("bitbns"),
            private_credentials.get("bitbns"),
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
        "bitopro" | "bito_pro" | "bito-pro" => gateway.register_bitopro_adapter(bitopro_config(
            rest_base_urls.get("bitopro"),
            private_credentials.get("bitopro"),
        ))?,
        "bybit" => gateway.register_bybit_adapter(bybit_config(
            rest_base_urls.get("bybit"),
            private_credentials.get("bybit"),
        ))?,
        "bybiteu" | "bybit_eu" | "bybit-eu" => {
            gateway.register_bybiteu_adapter(bybiteu_config(rest_base_urls.get("bybiteu")))?
        }
        "bydfi" | "bydfi.com" => gateway.register_bydfi_adapter(bydfi_config(
            rest_base_urls.get("bydfi"),
            private_credentials.get("bydfi"),
        ))?,
        "bitso" => gateway.register_bitso_adapter(bitso_config(
            rest_base_urls.get("bitso"),
            private_credentials.get("bitso"),
        ))?,
        "bitstamp" => gateway.register_bitstamp_adapter(bitstamp_config(
            rest_base_urls.get("bitstamp"),
            private_credentials.get("bitstamp"),
        ))?,
        "bittrade" | "bittradejp" | "bittrade_jp" | "huobi_japan" => gateway
            .register_bittrade_adapter(bittrade_config(
                rest_base_urls.get("bittrade"),
                private_credentials.get("bittrade"),
            ))?,
        "bitvavo" => gateway.register_bitvavo_adapter(bitvavo_config(
            rest_base_urls.get("bitvavo"),
            private_credentials.get("bitvavo"),
        ))?,
        "blockchain.com" | "blockchaincom" | "blockchain_com" | "blockchain-com" => gateway
            .register_blockchaincom_adapter(blockchaincom_config(
                rest_base_urls.get("blockchaincom"),
                private_credentials.get("blockchaincom"),
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
        "bsx" => gateway.register_bsx_adapter(bsx_config(
            rest_base_urls.get("bsx"),
            private_credentials.get("bsx"),
        ))?,
        "btcmarkets" | "btc_markets" => gateway.register_btcmarkets_adapter(btcmarkets_config(
            rest_base_urls.get("btcmarkets"),
            private_credentials.get("btcmarkets"),
        ))?,
        "btcbox" | "btc_box" => gateway.register_btcbox_adapter(btcbox_config(
            rest_base_urls.get("btcbox"),
            private_credentials.get("btcbox"),
        ))?,
        "cex" | "cexio" | "cex.io" => gateway.register_cex_adapter(cex_config(
            rest_base_urls.get("cex"),
            private_credentials.get("cex"),
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
        "coinmate" | "coin_mate" => gateway.register_coinmate_adapter(coinmate_config(
            rest_base_urls.get("coinmate"),
            private_credentials.get("coinmate"),
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
        "cod3x" | "cod3x_ai" | "cod3x-ai" => gateway.register_cod3x_adapter(cod3x_config(
            rest_base_urls.get("cod3x"),
            rest_base_urls.get("cod3x_website"),
            rest_base_urls.get("cod3x_app"),
        ))?,
        "crypto.com" | "crypto_com" | "cryptocom" => {
            gateway.register_cryptocom_adapter(cryptocom_config(
                rest_base_urls.get("cryptocom"),
                private_credentials.get("cryptocom"),
            ))?
        }
        "cryptomus" | "crypto_mus" => gateway.register_cryptomus_adapter(cryptomus_config(
            rest_base_urls.get("cryptomus"),
            private_credentials.get("cryptomus"),
        ))?,
        "d8x" | "d8x_exchange" | "d8x-exchange" => gateway.register_d8x_adapter(d8x_config(
            rest_base_urls.get("d8x"),
            private_credentials.get("d8x"),
        ))?,
        "deepcoin" | "deep_coin" => gateway.register_deepcoin_adapter(deepcoin_config(
            rest_base_urls.get("deepcoin"),
            private_credentials.get("deepcoin"),
        ))?,
        "derive" => gateway.register_derive_adapter(derive_config(
            rest_base_urls.get("derive"),
            private_credentials.get("derive"),
        ))?,
        "derive_chain_perps" | "derive-chain-perps" | "derive_chain" => gateway
            .register_derive_chain_perps_adapter(derive_chain_perps_config(
                rest_base_urls.get("derive_chain_perps"),
                rest_base_urls.get("derive_chain_perps_rpc"),
            ))?,
        "dydx" | "dydx_v4" | "dydxv4" => gateway.register_dydx_adapter(dydx_config(
            rest_base_urls.get("dydx"),
            rest_base_urls.get("dydx_node"),
            private_credentials.get("dydx"),
        ))?,
        "equation" | "equation_dao" | "equationdao" => {
            gateway.register_equation_adapter(equation_config(
                rest_base_urls.get("equation"),
                rest_base_urls.get("equation_app"),
            ))?
        }
        "exmo" => gateway.register_exmo_adapter(exmo_config(
            rest_base_urls.get("exmo"),
            private_credentials.get("exmo"),
        ))?,
        "fmfwio" | "fmfw.io" | "fmfw" => gateway.register_fmfwio_adapter(fmfwio_config(
            rest_base_urls.get("fmfwio"),
            private_credentials.get("fmfwio"),
        ))?,
        "foxbit" | "fox_bit" => gateway.register_foxbit_adapter(foxbit_config(
            rest_base_urls.get("foxbit"),
            private_credentials.get("foxbit"),
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
        "hollaex" | "hollaex_demo" | "hollaex-demo" => {
            gateway.register_hollaex_adapter(hollaex_config(
                rest_base_urls.get("hollaex"),
                private_credentials.get("hollaex"),
            ))?
        }
        "hibachi" => gateway.register_hibachi_adapter(hibachi_config(
            rest_base_urls.get("hibachi"),
            rest_base_urls.get("hibachi_account"),
            private_credentials.get("hibachi"),
        ))?,
        "hitbtc" | "hitbtc.com" | "hit_btc" => gateway.register_hitbtc_adapter(hitbtc_config(
            rest_base_urls.get("hitbtc"),
            private_credentials.get("hitbtc"),
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
        "latoken" => gateway.register_latoken_adapter(latoken_config(
            rest_base_urls.get("latoken"),
            private_credentials.get("latoken"),
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
        "mango_markets" | "mango-markets" | "mango" | "mango_markets_v4" => gateway
            .register_mango_markets_adapter(mango_markets_config(
                rest_base_urls.get("mango_markets"),
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
        "modetrade" | "mode_trade" | "mode-trade" => {
            gateway.register_modetrade_adapter(modetrade_config(
                rest_base_urls.get("modetrade"),
                private_credentials.get("modetrade"),
            ))?
        }
        "woofipro" | "woofi_pro" | "woofi-pro" => {
            gateway.register_woofipro_adapter(woofipro_config(
                rest_base_urls.get("woofipro"),
                private_credentials.get("woofipro"),
            ))?
        }
        "myokx" | "my_okx" | "my-okx" | "okx_eea" | "okx-eea" => {
            gateway.register_myokx_adapter(myokx_config(rest_base_urls.get("myokx")))?
        }
        "ndax" | "ndaxio" | "ndax_io" => gateway.register_ndax_adapter(ndax_config(
            rest_base_urls.get("ndax"),
            private_credentials.get("ndax"),
        ))?,
        "novadax" | "nova_dax" | "nova-dax" => gateway.register_novadax_adapter(novadax_config(
            rest_base_urls.get("novadax"),
            private_credentials.get("novadax"),
        ))?,
        "okx" => gateway.register_okx_adapter(okx_config(
            rest_base_urls.get("okx"),
            private_credentials.get("okx"),
        ))?,
        "okxus" | "okx_us" | "okx-us" => {
            gateway.register_okxus_adapter(okxus_config(rest_base_urls.get("okxus")))?
        }
        "onetrading" | "one_trading" | "one-trading" => {
            gateway.register_onetrading_adapter(onetrading_config(
                rest_base_urls.get("onetrading"),
                private_credentials.get("onetrading"),
            ))?
        }
        "orange_x" | "orangex" => gateway.register_orangex_adapter(orangex_config(
            rest_base_urls.get("orangex"),
            private_credentials.get("orangex"),
        ))?,
        "p2b" | "p2pb2b" | "p2pb2b.com" => gateway.register_p2b_adapter(p2b_config(
            rest_base_urls.get("p2b"),
            private_credentials.get("p2b"),
        ))?,
        "paradex" => gateway.register_paradex_adapter(paradex_config(
            rest_base_urls.get("paradex"),
            private_credentials.get("paradex"),
        ))?,
        "paymium" => gateway.register_paymium_adapter(paymium_config(
            rest_base_urls.get("paymium"),
            private_credentials.get("paymium"),
        ))?,
        "phemex" => gateway.register_phemex_adapter(phemex_config(
            rest_base_urls.get("phemex"),
            private_credentials.get("phemex"),
        ))?,
        "tokocrypto" | "toko_crypto" | "toko-crypto" => {
            gateway.register_tokocrypto_adapter(tokocrypto_config(
                rest_base_urls.get("tokocrypto"),
                rest_base_urls.get("tokocrypto_market"),
                rest_base_urls.get("tokocrypto_nextme"),
                private_credentials.get("tokocrypto"),
            ))?
        }
        "upbit" => gateway.register_upbit_adapter(upbit_config(
            rest_base_urls.get("upbit"),
            private_credentials.get("upbit"),
        ))?,
        "wavesexchange" | "waves_exchange" | "waves.exchange" => gateway
            .register_wavesexchange_adapter(wavesexchange_config(
                rest_base_urls.get("wavesexchange"),
            ))?,
        "weex" => gateway.register_weex_adapter(weex_config(
            rest_base_urls.get("weex"),
            rest_base_urls.get("weex_spot"),
            rest_base_urls.get("weex_contract"),
            private_credentials.get("weex"),
        ))?,
        "yobit" | "yo_bit" | "yobit.net" => gateway.register_yobit_adapter(yobit_config(
            rest_base_urls.get("yobit"),
            private_credentials.get("yobit"),
        ))?,
        "zaif" => gateway.register_zaif_adapter(zaif_config(
            rest_base_urls.get("zaif"),
            rest_base_urls.get("zaif_public"),
            rest_base_urls.get("zaif_private"),
            private_credentials.get("zaif"),
        ))?,
        "zebpay" | "zeb_pay" => gateway.register_zebpay_adapter(zebpay_config(
            rest_base_urls.get("zebpay"),
            private_credentials.get("zebpay"),
        ))?,
        "zeta_markets" | "zetamarkets" | "zeta-markets" => gateway.register_zeta_markets_adapter(
            zeta_markets_config(rest_base_urls.get("zeta_markets")),
        )?,
        other => gateway.register_named_adapter(other)?,
    }
    Ok(())
}

fn aark_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> AarkGatewayConfig {
    let mut config = AarkGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.orderly_key = Some(credentials.api_key.clone());
        config.orderly_secret = Some(credentials.api_secret.clone());
        config.orderly_account_id = credentials.account_group.clone();
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
    config
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

fn arkham_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ArkhamGatewayConfig {
    let mut config = ArkhamGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config
}

fn aftermath_config(rest_base_url: Option<String>) -> AftermathGatewayConfig {
    let mut config = AftermathGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
    config
}

fn alpaca_config(
    rest_base_url: Option<String>,
    broker_rest_base_url: Option<String>,
    market_data_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> AlpacaGatewayConfig {
    let mut config = AlpacaGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.broker_rest_base_url = rest_base_url.clone();
        config.market_data_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.broker_rest_base_url, broker_rest_base_url);
    apply_rest_base_url(
        &mut config.market_data_rest_base_url,
        market_data_rest_base_url,
    );
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = true;
    }
    config
}

fn apollox_dex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ApolloxDexGatewayConfig {
    let mut config = ApolloxDexGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
    config
}

fn d8x_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> D8xGatewayConfig {
    let mut config = D8xGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.wallet_address = Some(credentials.api_key.clone());
        config.signer_private_key = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
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

fn binanceus_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BinanceUsGatewayConfig {
    let mut config = BinanceUsGatewayConfig::default();
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

fn bsx_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BsxGatewayConfig {
    let mut config = BsxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.wallet_address = credentials.account_group.clone();
        config.signer_address = credentials.passphrase.clone();
    }
    config.enabled_private_rest = false;
    config.enabled_private_streams = false;
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

fn btcbox_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BtcboxGatewayConfig {
    let mut config = BtcboxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
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

fn bit2c_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> Bit2cGatewayConfig {
    let mut config = Bit2cGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
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

fn bitbns_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitbnsGatewayConfig {
    let mut config = BitbnsGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = false;
    }
    config
}

fn bittrade_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BittradeGatewayConfig {
    let mut config = BittradeGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
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

fn bitopro_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BitoproGatewayConfig {
    let mut config = BitoproGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.api_identity = credentials.account_group.clone();
        config.enabled_private_rest = config.api_identity.is_some();
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

fn blockchaincom_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BlockchainComGatewayConfig {
    let mut config = BlockchainComGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
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

fn bybiteu_config(rest_base_url: Option<String>) -> BybiteuGatewayConfig {
    let mut config = BybiteuGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    config
}

fn bydfi_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> BydfiGatewayConfig {
    let mut config = BydfiGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn cex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CexGatewayConfig {
    let mut config = CexGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.user_id = credentials.account_group.clone();
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
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

fn coinmate_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CoinmateGatewayConfig {
    let mut config = CoinmateGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.public_key = Some(credentials.api_key.clone());
        config.private_key = Some(credentials.api_secret.clone());
        config.client_id = credentials
            .account_group
            .clone()
            .or_else(|| credentials.passphrase.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
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

fn cod3x_config(
    docs_base_url: Option<String>,
    website_base_url: Option<String>,
    app_base_url: Option<String>,
) -> Cod3xGatewayConfig {
    let mut config = Cod3xGatewayConfig::default();
    apply_rest_base_url(&mut config.docs_base_url, docs_base_url);
    apply_rest_base_url(&mut config.website_base_url, website_base_url);
    apply_rest_base_url(&mut config.app_base_url, app_base_url);
    config.enabled_public_rest = false;
    config.enabled_private_rest = false;
    config.enabled_public_streams = false;
    config.enabled_private_streams = false;
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

fn equation_config(
    docs_base_url: Option<String>,
    app_base_url: Option<String>,
) -> EquationGatewayConfig {
    let mut config = EquationGatewayConfig::default();
    apply_rest_base_url(&mut config.docs_base_url, docs_base_url);
    apply_rest_base_url(&mut config.app_base_url, app_base_url);
    config.enabled_public_rest = false;
    config.enabled_private_rest = false;
    config.enabled_public_streams = false;
    config.enabled_private_streams = false;
    config
}

fn exmo_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ExmoGatewayConfig {
    let mut config = ExmoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = true;
    }
    config
}

fn fmfwio_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> FmfwioGatewayConfig {
    let mut config = FmfwioGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
    }
    config
}

fn hollaex_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HollaexGatewayConfig {
    let mut config = HollaexGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
    }
    config
}

fn foxbit_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> FoxbitGatewayConfig {
    let mut config = FoxbitGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config
}

fn hibachi_config(
    data_rest_base_url: Option<String>,
    account_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HibachiGatewayConfig {
    let mut config = HibachiGatewayConfig::default();
    apply_rest_base_url(&mut config.data_rest_base_url, data_rest_base_url);
    apply_rest_base_url(&mut config.account_rest_base_url, account_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.private_key = Some(credentials.api_secret.clone());
        config.public_key = credentials.passphrase.clone();
        config.account_id = credentials.account_group.clone();
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config
}

fn hitbtc_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> HitbtcGatewayConfig {
    let mut config = HitbtcGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
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

fn cryptomus_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> CryptomusGatewayConfig {
    let mut config = CryptomusGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.user_id = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
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

fn derive_chain_perps_config(
    rest_base_url: Option<String>,
    rpc_url: Option<String>,
) -> DeriveChainPerpsGatewayConfig {
    let mut config = DeriveChainPerpsGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    apply_rest_base_url(&mut config.rpc_url, rpc_url);
    config.enabled = false;
    config.enabled_public_rest = false;
    config.enabled_private_rest = false;
    config.enabled_public_streams = false;
    config.enabled_private_streams = false;
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

fn latoken_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> LatokenGatewayConfig {
    let mut config = LatokenGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
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

fn mango_markets_config(solana_rpc_url: Option<String>) -> MangoMarketsGatewayConfig {
    let mut config = MangoMarketsGatewayConfig::default();
    if let Some(solana_rpc_url) = solana_rpc_url {
        config.solana_rpc_url = solana_rpc_url;
    }
    config.enabled_public_scan = false;
    config.enabled_private_rest = false;
    config.enabled_public_streams = false;
    config.enabled_private_streams = false;
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

fn modetrade_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ModetradeGatewayConfig {
    let mut config = ModetradeGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.orderly_key = Some(credentials.api_key.clone());
        config.orderly_secret = Some(credentials.api_secret.clone());
        config.orderly_account_id = credentials.account_group.clone();
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
    config
}

fn woofipro_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> WoofiproGatewayConfig {
    let mut config = WoofiproGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.orderly_key = Some(credentials.api_key.clone());
        config.orderly_secret = Some(credentials.api_secret.clone());
        config.orderly_account_id = credentials.account_group.clone();
        config.enabled_private_rest = config.private_rest_available();
    }
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
    config.enabled_private_streams = false;
    config
}

fn novadax_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> NovadaxGatewayConfig {
    let mut config = NovadaxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.account_id = credentials.passphrase.clone();
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config
}

fn ndax_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> NdaxGatewayConfig {
    let mut config = NdaxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.passphrase = credentials.passphrase.clone();
        config.user_id = credentials.account_group.clone();
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_rest = true;
    config
}

fn myokx_config(rest_base_url: Option<String>) -> MyOkxGatewayConfig {
    let mut config = MyOkxGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
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

fn okxus_config(rest_base_url: Option<String>) -> OkxusGatewayConfig {
    let mut config = OkxusGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    config
}

fn onetrading_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> OneTradingGatewayConfig {
    let mut config = OneTradingGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_token = Some(credentials.api_key.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
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

fn p2b_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> P2bGatewayConfig {
    let mut config = P2bGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
        config.enabled_private_rest = false;
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

fn paymium_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> PaymiumGatewayConfig {
    let mut config = PaymiumGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_streams = false;
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

fn tokocrypto_config(
    rest_base_url: Option<String>,
    market_rest_base_url: Option<String>,
    nextme_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> TokocryptoGatewayConfig {
    let mut config = TokocryptoGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    apply_rest_base_url(&mut config.market_rest_base_url, market_rest_base_url);
    apply_rest_base_url(&mut config.nextme_rest_base_url, nextme_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
        config.enabled_private_streams = false;
    }
    config.enabled_public_streams = false;
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

fn wavesexchange_config(rest_base_url: Option<String>) -> WavesExchangeGatewayConfig {
    let mut config = WavesExchangeGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
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

fn yobit_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> YobitGatewayConfig {
    let mut config = YobitGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = credentials.api_key.clone();
        config.api_secret = credentials.api_secret.clone();
    }
    config.enabled_private_rest = false;
    config
}

fn zaif_config(
    rest_base_url: Option<String>,
    public_rest_base_url: Option<String>,
    private_rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ZaifGatewayConfig {
    let mut config = ZaifGatewayConfig::default();
    if let Some(rest_base_url) = rest_base_url {
        config.public_rest_base_url = rest_base_url.clone();
        config.private_rest_base_url = rest_base_url;
    }
    apply_rest_base_url(&mut config.public_rest_base_url, public_rest_base_url);
    apply_rest_base_url(&mut config.private_rest_base_url, private_rest_base_url);
    if let Some(credentials) = credentials {
        config.api_key = Some(credentials.api_key.clone());
        config.api_secret = Some(credentials.api_secret.clone());
        config.enabled_private_rest = false;
    }
    config
}

fn zebpay_config(
    rest_base_url: Option<String>,
    credentials: Option<&ExchangePrivateCredentials>,
) -> ZebpayGatewayConfig {
    let mut config = ZebpayGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    if let Some(credentials) = credentials {
        config.client_id = credentials.api_key.clone();
        config.client_secret = credentials.api_secret.clone();
        config.access_token = credentials.passphrase.clone().unwrap_or_default();
        config.enabled_private_rest = false;
    }
    config
}

fn zeta_markets_config(rest_base_url: Option<String>) -> ZetaMarketsGatewayConfig {
    let mut config = ZetaMarketsGatewayConfig::default();
    apply_rest_base_url(&mut config.rest_base_url, rest_base_url);
    config.enabled_public_rest = true;
    config.enabled_public_streams = false;
    config.enabled_private_rest = false;
    config.enabled_private_streams = false;
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
        adapter: "alpaca",
        api_key_keys: &[
            "RUSTCTA_ALPACA_API_KEY",
            "RUSTCTA_ALPACA_API_KEY_ID",
            "ALPACA_API_KEY",
            "APCA_API_KEY_ID",
        ],
        api_secret_keys: &[
            "RUSTCTA_ALPACA_API_SECRET",
            "RUSTCTA_ALPACA_SECRET_KEY",
            "ALPACA_API_SECRET",
            "APCA_API_SECRET_KEY",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "apollox_dex",
        api_key_keys: &[
            "RUSTCTA_APOLLOX_DEX_API_KEY",
            "APOLLOX_DEX_API_KEY",
            "APOLLOX_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_APOLLOX_DEX_API_SECRET",
            "APOLLOX_DEX_API_SECRET",
            "APOLLOX_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "arkham",
        api_key_keys: &["RUSTCTA_ARKHAM_API_KEY", "ARKHAM_API_KEY"],
        api_secret_keys: &["RUSTCTA_ARKHAM_API_SECRET", "ARKHAM_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
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
            "RUSTCTA_BINANCE_FUTURES_API_KEY",
            "RUSTCTA_BINANCE_USDM_API_KEY",
            "BINANCE_FUTURES_API_KEY",
            "BINANCE_USDM_API_KEY",
            "BINANCE_SPOT_API_KEY",
            "BINANCE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BINANCE_API_SECRET",
            "RUSTCTA_BINANCE_FUTURES_API_SECRET",
            "RUSTCTA_BINANCE_USDM_API_SECRET",
            "BINANCE_FUTURES_API_SECRET",
            "BINANCE_USDM_API_SECRET",
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
        adapter: "binanceus",
        api_key_keys: &[
            "RUSTCTA_BINANCEUS_API_KEY",
            "RUSTCTA_BINANCE_US_API_KEY",
            "BINANCEUS_SPOT_API_KEY",
            "BINANCEUS_API_KEY",
            "BINANCE_US_SPOT_API_KEY",
            "BINANCE_US_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BINANCEUS_API_SECRET",
            "RUSTCTA_BINANCE_US_API_SECRET",
            "BINANCEUS_SPOT_API_SECRET",
            "BINANCEUS_API_SECRET",
            "BINANCE_US_SPOT_API_SECRET",
            "BINANCE_US_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "tokocrypto",
        api_key_keys: &[
            "RUSTCTA_TOKOCRYPTO_API_KEY",
            "TOKOCRYPTO_SPOT_API_KEY",
            "TOKOCRYPTO_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_TOKOCRYPTO_API_SECRET",
            "TOKOCRYPTO_SPOT_API_SECRET",
            "TOKOCRYPTO_API_SECRET",
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
        adapter: "bit2c",
        api_key_keys: &["RUSTCTA_BIT2C_API_KEY", "BIT2C_API_KEY"],
        api_secret_keys: &["RUSTCTA_BIT2C_API_SECRET", "BIT2C_API_SECRET"],
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
        adapter: "zaif",
        api_key_keys: &["RUSTCTA_ZAIF_API_KEY", "ZAIF_API_KEY"],
        api_secret_keys: &["RUSTCTA_ZAIF_API_SECRET", "ZAIF_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bitbns",
        api_key_keys: &["RUSTCTA_BITBNS_API_KEY", "BITBNS_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITBNS_API_SECRET", "BITBNS_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "bittrade",
        api_key_keys: &["RUSTCTA_BITTRADE_API_KEY", "BITTRADE_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITTRADE_API_SECRET", "BITTRADE_API_SECRET"],
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
        adapter: "bitopro",
        api_key_keys: &["RUSTCTA_BITOPRO_API_KEY", "BITOPRO_API_KEY"],
        api_secret_keys: &["RUSTCTA_BITOPRO_API_SECRET", "BITOPRO_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: Some(&[
            "RUSTCTA_BITOPRO_API_IDENTITY",
            "BITOPRO_API_IDENTITY",
            "RUSTCTA_BITOPRO_IDENTITY",
            "BITOPRO_IDENTITY",
        ]),
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
        adapter: "blockchaincom",
        api_key_keys: &[
            "RUSTCTA_BLOCKCHAINCOM_API_KEY",
            "RUSTCTA_BLOCKCHAIN_COM_API_KEY",
            "BLOCKCHAINCOM_API_KEY",
            "BLOCKCHAIN_COM_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_BLOCKCHAINCOM_API_SECRET",
            "RUSTCTA_BLOCKCHAIN_COM_API_SECRET",
            "BLOCKCHAINCOM_API_SECRET",
            "BLOCKCHAIN_COM_API_SECRET",
        ],
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
        adapter: "btcbox",
        api_key_keys: &["RUSTCTA_BTCBOX_API_KEY", "BTCBOX_API_KEY"],
        api_secret_keys: &["RUSTCTA_BTCBOX_API_SECRET", "BTCBOX_API_SECRET"],
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
        adapter: "bydfi",
        api_key_keys: &["RUSTCTA_BYDFI_API_KEY", "BYDFI_API_KEY"],
        api_secret_keys: &["RUSTCTA_BYDFI_API_SECRET", "BYDFI_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "cex",
        api_key_keys: &["RUSTCTA_CEX_API_KEY", "CEX_API_KEY", "CEXIO_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_CEX_API_SECRET",
            "CEX_API_SECRET",
            "CEXIO_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: Some(&["RUSTCTA_CEX_USER_ID", "CEX_USER_ID", "CEXIO_USER_ID"]),
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
        adapter: "bsx",
        api_key_keys: &["RUSTCTA_BSX_API_KEY", "BSX_API_KEY"],
        api_secret_keys: &["RUSTCTA_BSX_API_SECRET", "BSX_API_SECRET"],
        passphrase_keys: Some(&["RUSTCTA_BSX_SIGNER_ADDRESS", "BSX_SIGNER_ADDRESS"]),
        account_group_keys: Some(&["RUSTCTA_BSX_WALLET_ADDRESS", "BSX_WALLET_ADDRESS"]),
        requires_passphrase: false,
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
        adapter: "coinmate",
        api_key_keys: &[
            "RUSTCTA_COINMATE_PUBLIC_KEY",
            "RUSTCTA_COINMATE_API_KEY",
            "COINMATE_PUBLIC_KEY",
            "COINMATE_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_COINMATE_PRIVATE_KEY",
            "RUSTCTA_COINMATE_API_SECRET",
            "COINMATE_PRIVATE_KEY",
            "COINMATE_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: Some(&["RUSTCTA_COINMATE_CLIENT_ID", "COINMATE_CLIENT_ID"]),
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
        adapter: "cryptomus",
        api_key_keys: &["RUSTCTA_CRYPTOMUS_API_KEY", "CRYPTOMUS_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_CRYPTOMUS_USER_ID",
            "CRYPTOMUS_USER_ID",
            "RUSTCTA_CRYPTOMUS_API_SECRET",
            "CRYPTOMUS_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "d8x",
        api_key_keys: &["RUSTCTA_D8X_WALLET_ADDRESS", "D8X_WALLET_ADDRESS"],
        api_secret_keys: &["RUSTCTA_D8X_SIGNER_PRIVATE_KEY", "D8X_SIGNER_PRIVATE_KEY"],
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
        adapter: "exmo",
        api_key_keys: &["RUSTCTA_EXMO_API_KEY", "EXMO_SPOT_API_KEY", "EXMO_API_KEY"],
        api_secret_keys: &[
            "RUSTCTA_EXMO_API_SECRET",
            "EXMO_SPOT_API_SECRET",
            "EXMO_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "fmfwio",
        api_key_keys: &["RUSTCTA_FMFWIO_API_KEY", "FMFWIO_API_KEY"],
        api_secret_keys: &["RUSTCTA_FMFWIO_API_SECRET", "FMFWIO_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "hollaex",
        api_key_keys: &["RUSTCTA_HOLLAEX_API_KEY", "HOLLAEX_API_KEY"],
        api_secret_keys: &["RUSTCTA_HOLLAEX_API_SECRET", "HOLLAEX_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "foxbit",
        api_key_keys: &["RUSTCTA_FOXBIT_API_KEY", "FOXBIT_API_KEY"],
        api_secret_keys: &["RUSTCTA_FOXBIT_API_SECRET", "FOXBIT_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "hibachi",
        api_key_keys: &["RUSTCTA_HIBACHI_API_KEY", "HIBACHI_API_KEY"],
        api_secret_keys: &["RUSTCTA_HIBACHI_PRIVATE_KEY", "HIBACHI_PRIVATE_KEY"],
        passphrase_keys: Some(&["RUSTCTA_HIBACHI_PUBLIC_KEY", "HIBACHI_PUBLIC_KEY"]),
        account_group_keys: Some(&["RUSTCTA_HIBACHI_ACCOUNT_ID", "HIBACHI_ACCOUNT_ID"]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "hitbtc",
        api_key_keys: &["RUSTCTA_HITBTC_API_KEY", "HITBTC_API_KEY"],
        api_secret_keys: &["RUSTCTA_HITBTC_API_SECRET", "HITBTC_API_SECRET"],
        passphrase_keys: None,
        account_group_keys: None,
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
        adapter: "aark",
        api_key_keys: &[
            "RUSTCTA_AARK_ORDERLY_KEY",
            "RUSTCTA_AARK_API_KEY",
            "AARK_ORDERLY_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_AARK_ORDERLY_SECRET",
            "RUSTCTA_AARK_API_SECRET",
            "AARK_ORDERLY_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: Some(&["RUSTCTA_AARK_ORDERLY_ACCOUNT_ID", "AARK_ORDERLY_ACCOUNT_ID"]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "modetrade",
        api_key_keys: &[
            "RUSTCTA_MODETRADE_ORDERLY_KEY",
            "RUSTCTA_MODETRADE_API_KEY",
            "MODETRADE_ORDERLY_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_MODETRADE_ORDERLY_SECRET",
            "RUSTCTA_MODETRADE_API_SECRET",
            "MODETRADE_ORDERLY_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: Some(&[
            "RUSTCTA_MODETRADE_ORDERLY_ACCOUNT_ID",
            "MODETRADE_ORDERLY_ACCOUNT_ID",
        ]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "woofipro",
        api_key_keys: &[
            "RUSTCTA_WOOFIPRO_ORDERLY_KEY",
            "RUSTCTA_WOOFIPRO_API_KEY",
            "WOOFIPRO_ORDERLY_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_WOOFIPRO_ORDERLY_SECRET",
            "RUSTCTA_WOOFIPRO_API_SECRET",
            "WOOFIPRO_ORDERLY_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: Some(&[
            "RUSTCTA_WOOFIPRO_ORDERLY_ACCOUNT_ID",
            "WOOFIPRO_ORDERLY_ACCOUNT_ID",
        ]),
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "p2b",
        api_key_keys: &[
            "RUSTCTA_P2B_API_KEY",
            "RUSTCTA_P2B_SPOT_API_KEY",
            "RUSTCTA_P2PB2B_API_KEY",
            "P2B_API_KEY",
            "P2PB2B_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_P2B_API_SECRET",
            "RUSTCTA_P2B_SPOT_API_SECRET",
            "RUSTCTA_P2PB2B_API_SECRET",
            "P2B_API_SECRET",
            "P2PB2B_API_SECRET",
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
        adapter: "paymium",
        api_key_keys: &["RUSTCTA_PAYMIUM_API_KEY", "PAYMIUM_API_KEY"],
        api_secret_keys: &["RUSTCTA_PAYMIUM_API_SECRET", "PAYMIUM_API_SECRET"],
        passphrase_keys: None,
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
        adapter: "novadax",
        api_key_keys: &["RUSTCTA_NOVADAX_API_KEY", "NOVADAX_API_KEY"],
        api_secret_keys: &["RUSTCTA_NOVADAX_API_SECRET", "NOVADAX_API_SECRET"],
        passphrase_keys: Some(&["RUSTCTA_NOVADAX_ACCOUNT_ID", "NOVADAX_ACCOUNT_ID"]),
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "ndax",
        api_key_keys: &["RUSTCTA_NDAX_API_KEY", "NDAX_API_KEY"],
        api_secret_keys: &["RUSTCTA_NDAX_API_SECRET", "NDAX_API_SECRET"],
        passphrase_keys: Some(&[
            "RUSTCTA_NDAX_API_PASSPHRASE",
            "NDAX_API_PASSPHRASE",
            "NDAX_PASSPHRASE",
        ]),
        account_group_keys: Some(&["RUSTCTA_NDAX_USER_ID", "NDAX_USER_ID"]),
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
        adapter: "latoken",
        api_key_keys: &["RUSTCTA_LATOKEN_API_KEY", "LATOKEN_API_KEY"],
        api_secret_keys: &["RUSTCTA_LATOKEN_API_SECRET", "LATOKEN_API_SECRET"],
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
        adapter: "onetrading",
        api_key_keys: &[
            "RUSTCTA_ONETRADING_API_TOKEN",
            "RUSTCTA_ONE_TRADING_API_TOKEN",
            "ONETRADING_API_TOKEN",
            "ONE_TRADING_API_TOKEN",
        ],
        api_secret_keys: &[],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
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
    PrivateCredentialSpec {
        adapter: "yobit",
        api_key_keys: &[
            "RUSTCTA_YOBIT_API_KEY",
            "RUSTCTA_YOBIT_SPOT_API_KEY",
            "YOBIT_API_KEY",
        ],
        api_secret_keys: &[
            "RUSTCTA_YOBIT_API_SECRET",
            "RUSTCTA_YOBIT_SPOT_API_SECRET",
            "YOBIT_API_SECRET",
        ],
        passphrase_keys: None,
        account_group_keys: None,
        requires_passphrase: false,
    },
    PrivateCredentialSpec {
        adapter: "zebpay",
        api_key_keys: &["RUSTCTA_ZEBPAY_CLIENT_ID", "ZEBPAY_CLIENT_ID"],
        api_secret_keys: &["RUSTCTA_ZEBPAY_CLIENT_SECRET", "ZEBPAY_CLIENT_SECRET"],
        passphrase_keys: Some(&["RUSTCTA_ZEBPAY_ACCESS_TOKEN", "ZEBPAY_ACCESS_TOKEN"]),
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
        AccountId, ExchangeId, MarketType, RequestContext, TenantId, EXCHANGE_API_SCHEMA_VERSION,
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
    fn config_should_wire_exmo_private_gateway_adapter_with_redacted_env_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "exmo"),
            ("RUSTCTA_EXMO_REST_BASE_URL", "http://127.0.0.1:9330"),
            ("RUSTCTA_EXMO_API_KEY", "exmo-key"),
            ("RUSTCTA_EXMO_API_SECRET", "exmo-secret"),
        ]));

        assert_eq!(config.adapters, vec!["exmo"]);
        assert_eq!(
            config.rest_base_urls.get("exmo").as_deref(),
            Some("http://127.0.0.1:9330")
        );
        assert!(config.private_credentials.contains("exmo"));
        config.build_gateway().expect("exmo gateway");
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("exmo-key"));
        assert!(!debug.contains("exmo-secret"));
    }

    #[test]
    fn config_should_wire_equation_gateway_adapter_as_scan_only() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "equation"),
            (
                "RUSTCTA_EQUATION_DOCS_BASE_URL",
                "https://docs.equation.org",
            ),
            ("RUSTCTA_EQUATION_APP_BASE_URL", "https://app.equation.org"),
        ]));

        assert_eq!(config.adapters, vec!["equation"]);
        assert_eq!(
            config.rest_base_urls.get("equation").as_deref(),
            Some("https://docs.equation.org")
        );
        assert_eq!(
            config.rest_base_urls.get("equation_app").as_deref(),
            Some("https://app.equation.org")
        );
        assert!(!config.private_credentials.contains("equation"));
        let gateway = config.build_gateway().expect("equation gateway");
        assert_eq!(gateway.adapter_count().expect("count"), 1);
    }

    #[test]
    fn config_should_wire_cod3x_gateway_adapter_as_audit_only() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "cod3x"),
            ("RUSTCTA_COD3X_DOCS_BASE_URL", "https://docs.cod3x.org"),
            ("RUSTCTA_COD3X_WEBSITE_BASE_URL", "https://www.cod3x.org"),
            ("RUSTCTA_COD3X_APP_BASE_URL", "https://app.cod3x.org"),
        ]));

        assert_eq!(config.adapters, vec!["cod3x"]);
        assert_eq!(
            config.rest_base_urls.get("cod3x").as_deref(),
            Some("https://docs.cod3x.org")
        );
        assert_eq!(
            config.rest_base_urls.get("cod3x_website").as_deref(),
            Some("https://www.cod3x.org")
        );
        assert_eq!(
            config.rest_base_urls.get("cod3x_app").as_deref(),
            Some("https://app.cod3x.org")
        );
        assert!(!config.private_credentials.contains("cod3x"));
        let gateway = config.build_gateway().expect("cod3x gateway");
        assert_eq!(gateway.adapter_count().expect("count"), 1);
    }

    #[tokio::test]
    async fn alpaca_config_should_parse_split_rest_urls_and_register_private_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "alpaca"),
            (
                "RUSTCTA_ALPACA_BROKER_REST_BASE_URL",
                "http://127.0.0.1:9310",
            ),
            (
                "RUSTCTA_ALPACA_MARKET_DATA_REST_BASE_URL",
                "http://127.0.0.1:9311",
            ),
            ("RUSTCTA_ALPACA_API_KEY", "alpaca-key"),
            ("RUSTCTA_ALPACA_API_SECRET", "alpaca-secret"),
        ]));

        assert_eq!(config.adapters, vec!["alpaca"]);
        assert_eq!(
            config.rest_base_urls.get("alpaca_broker").as_deref(),
            Some("http://127.0.0.1:9310")
        );
        assert_eq!(
            config.rest_base_urls.get("alpaca_market_data").as_deref(),
            Some("http://127.0.0.1:9311")
        );
        assert!(config.private_credentials.contains("alpaca"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("alpaca-key"));
        assert!(!debug.contains("alpaca-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "alpaca-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("acct-123").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("acct-123").expect("account")),
                        run_id: None,
                        request_id: Some("alpaca-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("alpaca").expect("alpaca")],
                },
            )
            .await
            .expect("capabilities");

        let alpaca = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "alpaca")
            .expect("alpaca capabilities");
        assert!(alpaca.supports_public_rest);
        assert!(alpaca.supports_private_rest);
        assert!(alpaca.supports_place_order);
        assert!(alpaca.market_types.contains(&MarketType::Spot));
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

    #[tokio::test]
    async fn config_should_wire_bitopro_private_gateway_adapter_without_exposing_identity() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bitopro"),
            ("RUSTCTA_BITOPRO_REST_BASE_URL", "http://127.0.0.1:9208/v3"),
            ("RUSTCTA_BITOPRO_API_KEY", "bitopro-key"),
            ("RUSTCTA_BITOPRO_API_SECRET", "bitopro-secret"),
            ("RUSTCTA_BITOPRO_API_IDENTITY", "bitopro-identity"),
        ]));

        assert_eq!(config.adapters, vec!["bitopro"]);
        assert_eq!(
            config.rest_base_urls.get("bitopro").as_deref(),
            Some("http://127.0.0.1:9208/v3")
        );
        assert!(config.private_credentials.contains("bitopro"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bitopro-secret"));
        assert!(!debug.contains("bitopro-identity"));

        let gateway = config.build_gateway().expect("gateway");
        let status = gateway.status().await.expect("status");
        assert!(status
            .exchanges
            .iter()
            .any(|exchange| exchange.exchange == "bitopro"));
    }

    #[test]
    fn config_should_parse_bittrade_without_exposing_secrets() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bittrade"),
            ("RUSTCTA_BITTRADE_REST_BASE_URL", "http://127.0.0.1:9204"),
            ("RUSTCTA_BITTRADE_API_KEY", "bittrade-key"),
            ("RUSTCTA_BITTRADE_API_SECRET", "bittrade-secret"),
        ]));

        assert_eq!(config.adapters, vec!["bittrade"]);
        assert_eq!(
            config.rest_base_urls.get("bittrade").as_deref(),
            Some("http://127.0.0.1:9204")
        );
        assert!(config.private_credentials.contains("bittrade"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bittrade-secret"));
    }

    #[tokio::test]
    async fn config_should_wire_coinmate_scan_only_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "coinmate"),
            (
                "RUSTCTA_COINMATE_REST_BASE_URL",
                "http://127.0.0.1:9216/api",
            ),
            ("RUSTCTA_COINMATE_CLIENT_ID", "fixture-client"),
            ("RUSTCTA_COINMATE_PUBLIC_KEY", "fixture-public"),
            ("RUSTCTA_COINMATE_PRIVATE_KEY", "fixture-private"),
        ]));

        assert_eq!(config.adapters, vec!["coinmate"]);
        assert_eq!(
            config.rest_base_urls.get("coinmate").as_deref(),
            Some("http://127.0.0.1:9216/api")
        );
        assert!(config.private_credentials.contains("coinmate"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fixture-private"));
        assert!(!debug.contains("fixture-client"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "coinmate-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("coinmate-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("coinmate").expect("coinmate")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        let capabilities = &response.capabilities[0];
        assert_eq!(capabilities.exchange.as_str(), "coinmate");
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
        assert!(!capabilities.supports_positions);
    }

    #[tokio::test]
    async fn config_should_wire_cex_public_gateway_adapter_with_redacted_env_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "cex"),
            ("RUSTCTA_CEX_REST_BASE_URL", "http://127.0.0.1:9415/api"),
            ("RUSTCTA_CEX_API_KEY", "fixture-cex-key"),
            ("RUSTCTA_CEX_API_SECRET", "fixture-cex-secret"),
            ("RUSTCTA_CEX_USER_ID", "fixture-cex-user"),
        ]));
        assert_eq!(config.adapters, vec!["cex"]);
        assert_eq!(
            config.rest_base_urls.get("cex").as_deref(),
            Some("http://127.0.0.1:9415/api")
        );
        assert!(config.private_credentials.contains("cex"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fixture-cex-secret"));
        assert!(!debug.contains("fixture-cex-user"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "cex-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("cex-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("cex").expect("cex")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("cex capabilities");
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_paymium_public_gateway_adapter_with_redacted_env_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "paymium"),
            (
                "RUSTCTA_PAYMIUM_REST_BASE_URL",
                "http://127.0.0.1:9433/api/v1",
            ),
            ("RUSTCTA_PAYMIUM_API_KEY", "fixture-paymium-key"),
            ("RUSTCTA_PAYMIUM_API_SECRET", "fixture-paymium-secret"),
        ]));
        assert_eq!(config.adapters, vec!["paymium"]);
        assert_eq!(
            config.rest_base_urls.get("paymium").as_deref(),
            Some("http://127.0.0.1:9433/api/v1")
        );
        assert!(config.private_credentials.contains("paymium"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fixture-paymium-key"));
        assert!(!debug.contains("fixture-paymium-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "paymium-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("paymium-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("paymium").expect("paymium")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("paymium capabilities");
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(capabilities.supports_private_rest);
        assert!(!capabilities.supports_public_streams);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_wavesexchange_public_gateway_adapter_without_private_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "wavesexchange"),
            (
                "RUSTCTA_WAVESEXCHANGE_REST_BASE_URL",
                "http://127.0.0.1:9435",
            ),
            ("RUSTCTA_WAVESEXCHANGE_API_KEY", "ignored-waves-key"),
            ("RUSTCTA_WAVESEXCHANGE_API_SECRET", "ignored-waves-secret"),
        ]));
        assert_eq!(config.adapters, vec!["wavesexchange"]);
        assert_eq!(
            config.rest_base_urls.get("wavesexchange").as_deref(),
            Some("http://127.0.0.1:9435")
        );
        assert!(!config.private_credentials.contains("wavesexchange"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("ignored-waves-key"));
        assert!(!debug.contains("ignored-waves-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "wavesexchange-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("wavesexchange-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("wavesexchange").expect("wavesexchange")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response
            .capabilities
            .first()
            .expect("wavesexchange capabilities");
        assert_eq!(capabilities.exchange.as_str(), "wavesexchange");
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_public_streams);
        assert!(!capabilities.supports_place_order);
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
            ("RUSTCTA_CRYPTOMUS_REST_BASE_URL", "http://127.0.0.1:9016"),
            ("RUSTCTA_CRYPTOMUS_API_KEY", "cryptomus-key"),
            ("RUSTCTA_CRYPTOMUS_USER_ID", "cryptomus-user-id"),
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
        assert_eq!(
            config.rest_base_urls.get("cryptomus").as_deref(),
            Some("http://127.0.0.1:9016")
        );
        assert!(config.private_credentials.contains("ascendex"));
        assert!(config.private_credentials.contains("binance"));
        assert!(config.private_credentials.contains("biconomy"));
        assert!(config.private_credentials.contains("bingx"));
        assert!(config.private_credentials.contains("bitunix"));
        assert!(config.private_credentials.contains("cointr"));
        assert!(config.private_credentials.contains("cryptocom"));
        assert!(config.private_credentials.contains("cryptomus"));
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
        assert!(!debug.contains("cryptomus-user-id"));
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
    async fn myokx_config_should_register_eea_profile_without_private_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "myokx"),
            ("RUSTCTA_MYOKX_REST_BASE_URL", "http://127.0.0.1:9055"),
            ("RUSTCTA_MYOKX_API_KEY", "ignored-myokx-key"),
            ("RUSTCTA_MYOKX_API_SECRET", "ignored-myokx-secret"),
            ("RUSTCTA_MYOKX_API_PASSPHRASE", "ignored-myokx-passphrase"),
        ]));

        assert_eq!(config.adapters, vec!["myokx"]);
        assert_eq!(
            config.rest_base_urls.get("myokx").as_deref(),
            Some("http://127.0.0.1:9055")
        );
        assert!(!config.private_credentials.contains("myokx"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("ignored-myokx-secret"));
        assert!(!debug.contains("ignored-myokx-passphrase"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "myokx-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("myokx-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("myokx").expect("myokx")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("myokx capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("myokx").expect("myokx")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn zaif_config_should_parse_redacted_credentials_and_register_spot_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "zaif"),
            (
                "RUSTCTA_ZAIF_PUBLIC_REST_BASE_URL",
                "http://127.0.0.1:9064/api/1",
            ),
            (
                "RUSTCTA_ZAIF_PRIVATE_REST_BASE_URL",
                "http://127.0.0.1:9065/tapi",
            ),
            ("RUSTCTA_ZAIF_API_KEY", "zaif-key"),
            ("RUSTCTA_ZAIF_API_SECRET", "zaif-secret"),
        ]));

        assert_eq!(config.adapters, vec!["zaif"]);
        assert_eq!(
            config.rest_base_urls.get("zaif_public").as_deref(),
            Some("http://127.0.0.1:9064/api/1")
        );
        assert_eq!(
            config.rest_base_urls.get("zaif_private").as_deref(),
            Some("http://127.0.0.1:9065/tapi")
        );
        assert!(config.private_credentials.contains("zaif"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("zaif-key"));
        assert!(!debug.contains("zaif-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "zaif-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("zaif-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("zaif").expect("zaif")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("zaif capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("zaif").expect("zaif")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn ndax_config_should_parse_redacted_credentials_and_register_scan_only_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "ndax"),
            ("RUSTCTA_NDAX_REST_BASE_URL", "http://127.0.0.1:9056"),
            ("RUSTCTA_NDAX_API_KEY", "ndax-key"),
            ("RUSTCTA_NDAX_API_SECRET", "ndax-secret"),
            ("RUSTCTA_NDAX_API_PASSPHRASE", "ndax-passphrase"),
            ("RUSTCTA_NDAX_USER_ID", "ndax-user"),
        ]));

        assert_eq!(config.adapters, vec!["ndax"]);
        assert_eq!(
            config.rest_base_urls.get("ndax").as_deref(),
            Some("http://127.0.0.1:9056")
        );
        assert!(config.private_credentials.contains("ndax"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("ndax-secret"));
        assert!(!debug.contains("ndax-passphrase"));
        assert!(!debug.contains("ndax-user"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "ndax-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("ndax-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("ndax").expect("ndax")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("ndax capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("ndax").expect("ndax")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn btcbox_config_should_register_scan_only_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "btcbox"),
            (
                "RUSTCTA_BTCBOX_REST_BASE_URL",
                "http://127.0.0.1:9059/api/v1",
            ),
            ("RUSTCTA_BTCBOX_API_KEY", "btcbox-key"),
            ("RUSTCTA_BTCBOX_API_SECRET", "btcbox-secret"),
        ]));

        assert_eq!(config.adapters, vec!["btcbox"]);
        assert_eq!(
            config.rest_base_urls.get("btcbox").as_deref(),
            Some("http://127.0.0.1:9059/api/v1")
        );
        assert!(config.private_credentials.contains("btcbox"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("btcbox-key"));
        assert!(!debug.contains("btcbox-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "btcbox-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("btcbox-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("btcbox").expect("btcbox")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("btcbox capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("btcbox").expect("btcbox")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn novadax_config_should_register_spot_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "novadax"),
            ("RUSTCTA_NOVADAX_REST_BASE_URL", "http://127.0.0.1:9057"),
            ("RUSTCTA_NOVADAX_API_KEY", "novadax-key"),
            ("RUSTCTA_NOVADAX_API_SECRET", "novadax-secret"),
            ("RUSTCTA_NOVADAX_ACCOUNT_ID", "novadax-account"),
        ]));

        assert_eq!(config.adapters, vec!["novadax"]);
        assert_eq!(
            config.rest_base_urls.get("novadax").as_deref(),
            Some("http://127.0.0.1:9057")
        );
        assert!(config.private_credentials.contains("novadax"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("novadax-secret"));
        assert!(!debug.contains("novadax-account"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "novadax-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("novadax-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("novadax").expect("novadax")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("novadax capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("novadax").expect("novadax")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn onetrading_config_should_register_spot_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "onetrading"),
            ("RUSTCTA_ONETRADING_REST_BASE_URL", "http://127.0.0.1:9058"),
            ("RUSTCTA_ONETRADING_API_TOKEN", "onetrading-token"),
        ]));

        assert_eq!(config.adapters, vec!["onetrading"]);
        assert_eq!(
            config.rest_base_urls.get("onetrading").as_deref(),
            Some("http://127.0.0.1:9058")
        );
        assert!(config.private_credentials.contains("onetrading"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("onetrading-token"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "onetrading-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("onetrading-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("onetrading").expect("onetrading")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response
            .capabilities
            .first()
            .expect("onetrading capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("onetrading").expect("onetrading")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn okxus_config_should_register_us_profile_without_private_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "okxus"),
            ("RUSTCTA_OKXUS_REST_BASE_URL", "http://127.0.0.1:9056"),
            ("RUSTCTA_OKXUS_API_KEY", "ignored-okxus-key"),
            ("RUSTCTA_OKXUS_API_SECRET", "ignored-okxus-secret"),
            ("RUSTCTA_OKXUS_API_PASSPHRASE", "ignored-okxus-passphrase"),
        ]));

        assert_eq!(config.adapters, vec!["okxus"]);
        assert_eq!(
            config.rest_base_urls.get("okxus").as_deref(),
            Some("http://127.0.0.1:9056")
        );
        assert!(!config.private_credentials.contains("okxus"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("ignored-okxus-secret"));
        assert!(!debug.contains("ignored-okxus-passphrase"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "okxus-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("okxus-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("okxus").expect("okxus")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response.capabilities.first().expect("okxus capabilities");
        assert_eq!(
            capabilities.exchange,
            ExchangeId::new("okxus").expect("okxus")
        );
        assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_cryptomus_gateway_adapter_without_payment_api_or_live_writes() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "cryptomus"),
            ("RUSTCTA_CRYPTOMUS_REST_BASE_URL", "http://127.0.0.1:9045"),
            ("RUSTCTA_CRYPTOMUS_API_KEY", "cryptomus-key"),
            ("RUSTCTA_CRYPTOMUS_USER_ID", "cryptomus-user-id"),
        ]));

        assert_eq!(config.adapters, vec!["cryptomus"]);
        assert_eq!(
            config.rest_base_urls.get("cryptomus").as_deref(),
            Some("http://127.0.0.1:9045")
        );
        assert!(config.private_credentials.contains("cryptomus"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("cryptomus-user-id"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "cryptomus-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("cryptomus-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("cryptomus").expect("cryptomus")],
                },
            )
            .await
            .expect("capabilities");

        let capabilities = response
            .capabilities
            .first()
            .expect("cryptomus capabilities");
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_symbol_rules);
        assert!(capabilities.supports_order_book_snapshot);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_private_streams);
        assert!(!capabilities.supports_place_order);
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
    async fn config_should_wire_bitbns_scan_only_gateway_adapter_without_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bitbns"),
            ("RUSTCTA_BITBNS_REST_BASE_URL", "http://127.0.0.1:9044"),
            ("RUSTCTA_BITBNS_API_KEY", "bitbns-key"),
            ("RUSTCTA_BITBNS_API_SECRET", "bitbns-secret"),
        ]));

        assert_eq!(config.adapters, vec!["bitbns"]);
        assert_eq!(
            config.rest_base_urls.get("bitbns").as_deref(),
            Some("http://127.0.0.1:9044")
        );
        assert!(config.private_credentials.contains("bitbns"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bitbns-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "bitbns-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bitbns-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bitbns").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_blockchaincom_adapter_without_promoting_private_runtime() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "blockchain.com"),
            (
                "RUSTCTA_BLOCKCHAINCOM_REST_BASE_URL",
                "http://127.0.0.1:9045/v3/exchange",
            ),
            ("RUSTCTA_BLOCKCHAINCOM_API_KEY", "blockchain-key"),
            ("RUSTCTA_BLOCKCHAINCOM_API_SECRET", "blockchain-secret"),
        ]));

        assert_eq!(config.adapters, vec!["blockchain.com"]);
        assert_eq!(
            config.rest_base_urls.get("blockchaincom").as_deref(),
            Some("http://127.0.0.1:9045/v3/exchange")
        );
        assert!(config.private_credentials.contains("blockchaincom"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("blockchain-key"));
        assert!(!debug.contains("blockchain-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "blockchaincom-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("blockchaincom-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("blockchaincom").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_cancel_order);
    }

    #[tokio::test]
    async fn config_should_wire_d8x_without_promoting_contract_writes() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "d8x"),
            ("RUSTCTA_D8X_REST_BASE_URL", "http://127.0.0.1:9059"),
            (
                "RUSTCTA_D8X_WALLET_ADDRESS",
                "0x000000000000000000000000000000000000d8x0",
            ),
            ("RUSTCTA_D8X_SIGNER_PRIVATE_KEY", "fixture-d8x-private-key"),
        ]));

        assert_eq!(config.adapters, vec!["d8x"]);
        assert_eq!(
            config.rest_base_urls.get("d8x").as_deref(),
            Some("http://127.0.0.1:9059")
        );
        assert!(config.private_credentials.contains("d8x"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fixture-d8x-private-key"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "d8x-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("d8x-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("d8x").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_cancel_order);
    }

    #[tokio::test]
    async fn config_should_wire_mango_markets_as_g0_scan_only_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "mango_markets"),
            (
                "RUSTCTA_MANGO_MARKETS_SOLANA_RPC_URL",
                "http://127.0.0.1:8899",
            ),
        ]));

        assert_eq!(config.adapters, vec!["mango_markets"]);
        assert_eq!(
            config.rest_base_urls.get("mango_markets").as_deref(),
            Some("http://127.0.0.1:8899")
        );

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "mango-markets-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("mango-markets-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("mango_markets").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(!response.capabilities[0].supports_public_rest);
        assert!(!response.capabilities[0].supports_symbol_rules);
        assert!(!response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_cancel_order);
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
    async fn config_should_wire_binanceus_private_gateway_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "binanceus"),
            ("RUSTCTA_BINANCEUS_REST_BASE_URL", "http://127.0.0.1:9060"),
            ("RUSTCTA_BINANCEUS_API_KEY", "binanceus-key"),
            ("RUSTCTA_BINANCEUS_API_SECRET", "binanceus-secret"),
        ]));
        assert_eq!(config.adapters, vec!["binanceus"]);
        assert_eq!(
            config.rest_base_urls.get("binanceus").as_deref(),
            Some("http://127.0.0.1:9060")
        );
        assert!(config.private_credentials.contains("binanceus"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("binanceus-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "binanceus-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("binanceus-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("binanceus").expect("binanceus")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(response.capabilities[0].supports_balances);
        assert!(response.capabilities[0].supports_fees);
        assert!(response.capabilities[0].supports_place_order);
        assert!(response.capabilities[0].supports_cancel_order);
        assert!(response.capabilities[0].supports_cancel_all_orders);
        assert!(!response.capabilities[0].supports_positions);
        assert!(!response.capabilities[0].supports_amend_order);
        assert!(!response.capabilities[0].supports_order_list);
    }

    #[tokio::test]
    async fn config_should_wire_tokocrypto_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "tokocrypto"),
            ("RUSTCTA_TOKOCRYPTO_REST_BASE_URL", "http://127.0.0.1:9071"),
            (
                "RUSTCTA_TOKOCRYPTO_MARKET_REST_BASE_URL",
                "http://127.0.0.1:9072",
            ),
            ("RUSTCTA_TOKOCRYPTO_API_KEY", "tokocrypto-key"),
            ("RUSTCTA_TOKOCRYPTO_API_SECRET", "tokocrypto-secret"),
        ]));
        assert_eq!(config.adapters, vec!["tokocrypto"]);
        assert_eq!(
            config.rest_base_urls.get("tokocrypto").as_deref(),
            Some("http://127.0.0.1:9071")
        );
        assert_eq!(
            config.rest_base_urls.get("tokocrypto_market").as_deref(),
            Some("http://127.0.0.1:9072")
        );
        assert!(config.private_credentials.contains("tokocrypto"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("tokocrypto-key"));
        assert!(!debug.contains("tokocrypto-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "tokocrypto-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("tokocrypto-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("tokocrypto").expect("tokocrypto")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
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
        assert!(aster.supports_amend_order);

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
    async fn config_should_wire_arkham_gateway_adapter_with_redacted_env_credentials() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "arkham"),
            ("RUSTCTA_ARKHAM_REST_BASE_URL", "http://127.0.0.1:9042"),
            ("RUSTCTA_ARKHAM_API_KEY", "arkham-key"),
            ("RUSTCTA_ARKHAM_API_SECRET", "arkham-secret"),
        ]));
        assert_eq!(
            config.rest_base_urls.get("arkham").as_deref(),
            Some("http://127.0.0.1:9042")
        );
        assert!(config.private_credentials.contains("arkham"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("arkham-key"));
        assert!(!debug.contains("arkham-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);

        let response = client
            .get_capabilities(
                "arkham-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("arkham-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("arkham").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_private_streams);
    }

    #[tokio::test]
    async fn config_should_wire_p2b_gateway_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "p2b"),
            ("RUSTCTA_P2B_REST_BASE_URL", "http://127.0.0.1:9044"),
            ("RUSTCTA_P2B_API_KEY", "p2b-key"),
            ("RUSTCTA_P2B_API_SECRET", "p2b-secret"),
        ]));
        assert_eq!(config.adapters, vec!["p2b"]);
        assert_eq!(
            config.rest_base_urls.get("p2b").as_deref(),
            Some("http://127.0.0.1:9044")
        );
        assert!(config.private_credentials.contains("p2b"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("p2b-key"));
        assert!(!debug.contains("p2b-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "p2b-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("p2b-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("p2b").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_private_streams);
    }

    #[tokio::test]
    async fn config_should_wire_aftermath_gateway_adapter_as_scan_only() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "aftermath"),
            ("RUSTCTA_AFTERMATH_REST_BASE_URL", "http://127.0.0.1:9043"),
        ]));
        assert_eq!(
            config.rest_base_urls.get("aftermath").as_deref(),
            Some("http://127.0.0.1:9043")
        );
        assert!(!config.private_credentials.contains("aftermath"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "aftermath-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("aftermath-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("aftermath").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(response.capabilities[0]
            .market_types
            .contains(&MarketType::Perpetual));
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_private_streams);
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
    async fn config_should_wire_bit2c_gateway_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bit2c"),
            ("RUSTCTA_BIT2C_REST_BASE_URL", "http://127.0.0.1:9042"),
            ("RUSTCTA_BIT2C_API_KEY", "bit2c-key"),
            ("RUSTCTA_BIT2C_API_SECRET", "bit2c-secret"),
        ]));
        assert_eq!(
            config.rest_base_urls.get("bit2c").as_deref(),
            Some("http://127.0.0.1:9042")
        );
        assert!(config.private_credentials.contains("bit2c"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bit2c-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "bit2c-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bit2c-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bit2c").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_public_streams);
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
    async fn config_should_wire_latoken_gateway_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "latoken"),
            ("RUSTCTA_LATOKEN_REST_BASE_URL", "http://127.0.0.1:9046"),
            ("RUSTCTA_LATOKEN_API_KEY", "latoken-key"),
            ("RUSTCTA_LATOKEN_API_SECRET", "latoken-secret"),
        ]));

        assert_eq!(config.adapters, vec!["latoken"]);
        assert_eq!(
            config.rest_base_urls.get("latoken").as_deref(),
            Some("http://127.0.0.1:9046")
        );
        assert!(config.private_credentials.contains("latoken"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("latoken-key"));
        assert!(!debug.contains("latoken-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "latoken-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("latoken-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("latoken").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(response.capabilities[0].supports_public_streams);
    }

    #[tokio::test]
    async fn config_should_wire_aark_without_promoting_orderly_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "aark"),
            ("RUSTCTA_AARK_REST_BASE_URL", "http://127.0.0.1:9048"),
            ("RUSTCTA_AARK_ORDERLY_ACCOUNT_ID", "acct-aark-fixture"),
            ("RUSTCTA_AARK_ORDERLY_KEY", "aark-orderly-key"),
            ("RUSTCTA_AARK_ORDERLY_SECRET", "aark-orderly-secret"),
        ]));

        assert_eq!(config.adapters, vec!["aark"]);
        assert_eq!(
            config.rest_base_urls.get("aark").as_deref(),
            Some("http://127.0.0.1:9048")
        );
        assert!(config.private_credentials.contains("aark"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("aark-orderly-key"));
        assert!(!debug.contains("aark-orderly-secret"));
        assert!(!debug.contains("acct-aark-fixture"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "aark-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("aark-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("aark").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(!response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_public_streams);
    }

    #[tokio::test]
    async fn config_should_wire_apollox_dex_without_promoting_private_runtime() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "apollox_dex"),
            (
                "RUSTCTA_APOLLOX_DEX_REST_BASE_URL",
                "http://127.0.0.1:9062/fapi",
            ),
            ("RUSTCTA_APOLLOX_DEX_API_KEY", "apollox-key"),
            ("RUSTCTA_APOLLOX_DEX_API_SECRET", "apollox-secret"),
        ]));

        assert_eq!(config.adapters, vec!["apollox_dex"]);
        assert_eq!(
            config.rest_base_urls.get("apollox_dex").as_deref(),
            Some("http://127.0.0.1:9062/fapi")
        );
        assert!(config.private_credentials.contains("apollox_dex"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("apollox-key"));
        assert!(!debug.contains("apollox-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "apollox-dex-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("apollox-dex-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("apollox_dex").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert_eq!(
            response.capabilities[0].market_types,
            vec![MarketType::Perpetual]
        );
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_private_streams);
    }

    #[tokio::test]
    async fn config_should_wire_modetrade_without_promoting_orderly_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "modetrade"),
            ("RUSTCTA_MODETRADE_REST_BASE_URL", "http://127.0.0.1:9047"),
            ("RUSTCTA_MODETRADE_ORDERLY_ACCOUNT_ID", "acct-mode-fixture"),
            ("RUSTCTA_MODETRADE_ORDERLY_KEY", "mode-orderly-key"),
            ("RUSTCTA_MODETRADE_ORDERLY_SECRET", "mode-orderly-secret"),
        ]));

        assert_eq!(config.adapters, vec!["modetrade"]);
        assert_eq!(
            config.rest_base_urls.get("modetrade").as_deref(),
            Some("http://127.0.0.1:9047")
        );
        assert!(config.private_credentials.contains("modetrade"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("mode-orderly-key"));
        assert!(!debug.contains("mode-orderly-secret"));
        assert!(!debug.contains("acct-mode-fixture"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "modetrade-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("modetrade-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("modetrade").expect("exchange")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        assert!(response.capabilities[0].supports_public_rest);
        assert!(response.capabilities[0].supports_symbol_rules);
        assert!(!response.capabilities[0].supports_order_book_snapshot);
        assert!(!response.capabilities[0].supports_private_rest);
        assert!(!response.capabilities[0].supports_place_order);
        assert!(!response.capabilities[0].supports_public_streams);
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
    async fn config_should_wire_bybiteu_profile_with_private_trading_disabled() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bybiteu"),
            ("RUSTCTA_BYBITEU_REST_BASE_URL", "http://127.0.0.1:9060"),
        ]));
        let debug = format!("{config:?}");
        assert!(debug.contains("bybiteu"));
        assert!(!debug.contains("secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "bybiteu-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bybiteu-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bybiteu").expect("bybiteu")],
                },
            )
            .await
            .expect("capabilities");

        assert_eq!(response.capabilities.len(), 1);
        let capabilities = &response.capabilities[0];
        assert_eq!(capabilities.exchange.as_str(), "bybiteu");
        assert!(capabilities.supports_public_rest);
        assert!(capabilities.supports_public_streams);
        assert!(!capabilities.supports_private_rest);
        assert!(!capabilities.supports_place_order);
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

    #[tokio::test]
    async fn config_should_wire_fmfwio_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "fmfwio"),
            (
                "RUSTCTA_FMFWIO_REST_BASE_URL",
                "http://127.0.0.1:9060/api/3",
            ),
            ("RUSTCTA_FMFWIO_API_KEY", "fmfwio-key"),
            ("RUSTCTA_FMFWIO_API_SECRET", "fmfwio-secret"),
        ]));

        assert_eq!(config.adapters, vec!["fmfwio"]);
        assert_eq!(
            config.rest_base_urls.get("fmfwio").as_deref(),
            Some("http://127.0.0.1:9060/api/3")
        );
        assert!(config.private_credentials.contains("fmfwio"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("fmfwio-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "fmfwio-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("fmfwio-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("fmfwio").expect("fmfwio")],
                },
            )
            .await
            .expect("capabilities");

        let fmfwio = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "fmfwio")
            .expect("fmfwio capabilities");
        assert!(fmfwio.supports_public_rest);
        assert!(fmfwio.supports_symbol_rules);
        assert!(fmfwio.supports_order_book_snapshot);
        assert!(!fmfwio.supports_private_rest);
        assert!(!fmfwio.supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_hollaex_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "hollaex"),
            ("RUSTCTA_HOLLAEX_REST_BASE_URL", "http://127.0.0.1:9061/v2"),
            ("RUSTCTA_HOLLAEX_API_KEY", "hollaex-key"),
            ("RUSTCTA_HOLLAEX_API_SECRET", "hollaex-secret"),
        ]));

        assert_eq!(config.adapters, vec!["hollaex"]);
        assert_eq!(
            config.rest_base_urls.get("hollaex").as_deref(),
            Some("http://127.0.0.1:9061/v2")
        );
        assert!(config.private_credentials.contains("hollaex"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("hollaex-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "hollaex-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("hollaex-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("hollaex").expect("hollaex")],
                },
            )
            .await
            .expect("capabilities");

        let hollaex = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "hollaex")
            .expect("hollaex capabilities");
        assert!(hollaex.supports_public_rest);
        assert!(hollaex.supports_symbol_rules);
        assert!(hollaex.supports_order_book_snapshot);
        assert!(!hollaex.supports_private_rest);
        assert!(!hollaex.supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_foxbit_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "foxbit"),
            (
                "RUSTCTA_FOXBIT_REST_BASE_URL",
                "http://127.0.0.1:9061/rest/v3",
            ),
            ("RUSTCTA_FOXBIT_API_KEY", "foxbit-key"),
            ("RUSTCTA_FOXBIT_API_SECRET", "foxbit-secret"),
        ]));

        assert_eq!(config.adapters, vec!["foxbit"]);
        assert_eq!(
            config.rest_base_urls.get("foxbit").as_deref(),
            Some("http://127.0.0.1:9061/rest/v3")
        );
        assert!(config.private_credentials.contains("foxbit"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("foxbit-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "foxbit-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("foxbit-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("foxbit").expect("foxbit")],
                },
            )
            .await
            .expect("capabilities");

        let foxbit = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "foxbit")
            .expect("foxbit capabilities");
        assert!(foxbit.supports_public_rest);
        assert!(foxbit.supports_symbol_rules);
        assert!(foxbit.supports_order_book_snapshot);
        assert!(!foxbit.supports_private_rest);
        assert!(!foxbit.supports_place_order);
    }

    #[tokio::test]
    async fn bsx_config_should_register_public_perp_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bsx"),
            ("RUSTCTA_BSX_REST_BASE_URL", "http://127.0.0.1:9064"),
            ("RUSTCTA_BSX_API_KEY", "bsx-api-key"),
            ("RUSTCTA_BSX_API_SECRET", "bsx-api-secret"),
            (
                "RUSTCTA_BSX_WALLET_ADDRESS",
                "0x1111111111111111111111111111111111111111",
            ),
            (
                "RUSTCTA_BSX_SIGNER_ADDRESS",
                "0x2222222222222222222222222222222222222222",
            ),
        ]));

        assert_eq!(config.adapters, vec!["bsx"]);
        assert_eq!(
            config.rest_base_urls.get("bsx").as_deref(),
            Some("http://127.0.0.1:9064")
        );
        assert!(config.private_credentials.contains("bsx"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bsx-api-secret"));
        assert!(!debug.contains("0x1111111111111111111111111111111111111111"));
        assert!(!debug.contains("0x2222222222222222222222222222222222222222"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "bsx-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bsx-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bsx").expect("bsx")],
                },
            )
            .await
            .expect("capabilities");

        let bsx = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "bsx")
            .expect("bsx capabilities");
        assert_eq!(bsx.market_types, vec![MarketType::Perpetual]);
        assert!(bsx.supports_public_rest);
        assert!(bsx.supports_symbol_rules);
        assert!(bsx.supports_order_book_snapshot);
        assert!(!bsx.supports_private_rest);
        assert!(!bsx.supports_place_order);
        assert!(!bsx.supports_private_streams);
    }

    #[tokio::test]
    async fn config_should_wire_hibachi_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "hibachi"),
            (
                "RUSTCTA_HIBACHI_DATA_REST_BASE_URL",
                "http://127.0.0.1:9062/data",
            ),
            (
                "RUSTCTA_HIBACHI_ACCOUNT_REST_BASE_URL",
                "http://127.0.0.1:9063/account",
            ),
            ("RUSTCTA_HIBACHI_API_KEY", "hibachi-key"),
            ("RUSTCTA_HIBACHI_PRIVATE_KEY", "hibachi-private-key"),
            ("RUSTCTA_HIBACHI_PUBLIC_KEY", "hibachi-public-key"),
            ("RUSTCTA_HIBACHI_ACCOUNT_ID", "42"),
        ]));

        assert_eq!(config.adapters, vec!["hibachi"]);
        assert_eq!(
            config.rest_base_urls.get("hibachi").as_deref(),
            Some("http://127.0.0.1:9062/data")
        );
        assert_eq!(
            config.rest_base_urls.get("hibachi_account").as_deref(),
            Some("http://127.0.0.1:9063/account")
        );
        assert!(config.private_credentials.contains("hibachi"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("hibachi-key"));
        assert!(!debug.contains("hibachi-private-key"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "hibachi-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("hibachi-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("hibachi").expect("hibachi")],
                },
            )
            .await
            .expect("capabilities");

        let hibachi = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "hibachi")
            .expect("hibachi capabilities");
        assert!(hibachi.supports_public_rest);
        assert!(hibachi.supports_symbol_rules);
        assert!(hibachi.supports_order_book_snapshot);
        assert!(hibachi.supports_fees);
        assert!(!hibachi.supports_private_rest);
        assert!(!hibachi.supports_place_order);
    }

    #[tokio::test]
    async fn config_should_wire_hitbtc_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "hitbtc"),
            (
                "RUSTCTA_HITBTC_REST_BASE_URL",
                "http://127.0.0.1:9061/api/3",
            ),
            ("RUSTCTA_HITBTC_API_KEY", "hitbtc-key"),
            ("RUSTCTA_HITBTC_API_SECRET", "hitbtc-secret"),
        ]));

        assert_eq!(config.adapters, vec!["hitbtc"]);
        assert_eq!(
            config.rest_base_urls.get("hitbtc").as_deref(),
            Some("http://127.0.0.1:9061/api/3")
        );
        assert!(config.private_credentials.contains("hitbtc"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("hitbtc-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "hitbtc-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("hitbtc-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("hitbtc").expect("hitbtc")],
                },
            )
            .await
            .expect("capabilities");

        let hitbtc = response
            .capabilities
            .iter()
            .find(|capabilities| capabilities.exchange.as_str() == "hitbtc")
            .expect("hitbtc capabilities");
        assert!(hitbtc.supports_public_rest);
        assert!(hitbtc.supports_symbol_rules);
        assert!(hitbtc.supports_order_book_snapshot);
        assert!(!hitbtc.supports_private_rest);
        assert!(!hitbtc.supports_place_order);
    }

    #[tokio::test]
    async fn yobit_config_should_register_scan_only_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "yobit"),
            ("RUSTCTA_YOBIT_REST_BASE_URL", "http://127.0.0.1:9071"),
            ("RUSTCTA_YOBIT_API_KEY", "yobit-key"),
            ("RUSTCTA_YOBIT_API_SECRET", "yobit-secret"),
        ]));

        assert_eq!(config.adapters, vec!["yobit"]);
        assert_eq!(
            config.rest_base_urls.get("yobit").as_deref(),
            Some("http://127.0.0.1:9071")
        );
        assert!(config.private_credentials.contains("yobit"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("yobit-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "yobit-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("yobit-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("yobit").expect("yobit")],
                },
            )
            .await
            .expect("capabilities");

        let yobit = &response.capabilities[0];
        assert_eq!(yobit.exchange.as_str(), "yobit");
        assert_eq!(yobit.market_types, vec![MarketType::Spot]);
        assert!(yobit.supports_public_rest);
        assert!(yobit.supports_symbol_rules);
        assert!(yobit.supports_order_book_snapshot);
        assert!(yobit.supports_private_rest);
        assert!(!yobit.supports_place_order);
    }

    #[tokio::test]
    async fn bydfi_config_should_parse_redacted_credentials_and_register_adapter() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "bydfi"),
            ("RUSTCTA_BYDFI_REST_BASE_URL", "http://127.0.0.1:9070/api"),
            ("RUSTCTA_BYDFI_API_KEY", "bydfi-key"),
            ("RUSTCTA_BYDFI_API_SECRET", "bydfi-secret"),
        ]));

        assert_eq!(
            config.rest_base_urls.get("bydfi").as_deref(),
            Some("http://127.0.0.1:9070/api")
        );
        assert!(config.private_credentials.contains("bydfi"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bydfi-secret"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "bydfi-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("bydfi-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("bydfi").expect("bydfi")],
                },
            )
            .await
            .expect("capabilities");
        let bydfi = &response.capabilities[0];
        assert_eq!(bydfi.exchange.as_str(), "bydfi");
        assert!(bydfi.supports_private_rest);
        assert!(bydfi
            .market_types
            .contains(&rustcta_exchange_api::MarketType::Perpetual));
    }

    #[tokio::test]
    async fn zebpay_config_should_register_scan_only_adapter_without_promoting_private_rest() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "zebpay"),
            ("RUSTCTA_ZEBPAY_REST_BASE_URL", "http://127.0.0.1:9072"),
            ("RUSTCTA_ZEBPAY_CLIENT_ID", "zebpay-client-id"),
            ("RUSTCTA_ZEBPAY_CLIENT_SECRET", "zebpay-client-secret"),
            ("RUSTCTA_ZEBPAY_ACCESS_TOKEN", "zebpay-access-token"),
        ]));

        assert_eq!(config.adapters, vec!["zebpay"]);
        assert_eq!(
            config.rest_base_urls.get("zebpay").as_deref(),
            Some("http://127.0.0.1:9072")
        );
        assert!(config.private_credentials.contains("zebpay"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("zebpay-client-secret"));
        assert!(!debug.contains("zebpay-access-token"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "zebpay-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("zebpay-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("zebpay").expect("zebpay")],
                },
            )
            .await
            .expect("capabilities");

        let zebpay = &response.capabilities[0];
        assert_eq!(zebpay.exchange.as_str(), "zebpay");
        assert_eq!(zebpay.market_types, vec![MarketType::Spot]);
        assert!(zebpay.supports_public_rest);
        assert!(zebpay.supports_symbol_rules);
        assert!(zebpay.supports_order_book_snapshot);
        assert!(!zebpay.supports_private_rest);
        assert!(!zebpay.supports_place_order);
        assert!(!zebpay.supports_public_streams);
    }

    #[tokio::test]
    async fn zeta_markets_config_should_register_scan_only_adapter_without_private_runtime() {
        let config = GatewayAppConfig::from_env_reader(env_from(&[
            ("RUSTCTA_GATEWAY_ADAPTERS", "zeta_markets"),
            (
                "RUSTCTA_ZETA_MARKETS_REST_BASE_URL",
                "http://127.0.0.1:9073",
            ),
        ]));

        assert_eq!(config.adapters, vec!["zeta_markets"]);
        assert_eq!(
            config.rest_base_urls.get("zeta_markets").as_deref(),
            Some("http://127.0.0.1:9073")
        );
        assert!(!config.private_credentials.contains("zeta_markets"));

        let gateway = Arc::new(config.build_gateway().expect("gateway"));
        let client = InProcessGatewayClient::new(gateway);
        let response = client
            .get_capabilities(
                "zeta-markets-capabilities".to_string(),
                TenantId::new("tenant").expect("tenant"),
                Some(AccountId::new("account").expect("account")),
                GetCapabilitiesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: RequestContext {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                        account_id: Some(AccountId::new("account").expect("account")),
                        run_id: None,
                        request_id: Some("zeta-markets-capabilities".to_string()),
                        requested_at: chrono::Utc::now(),
                    },
                    exchanges: vec![ExchangeId::new("zeta_markets").expect("zeta_markets")],
                },
            )
            .await
            .expect("capabilities");

        let zeta_markets = &response.capabilities[0];
        assert_eq!(zeta_markets.exchange.as_str(), "zeta_markets");
        assert_eq!(zeta_markets.market_types, vec![MarketType::Perpetual]);
        assert!(zeta_markets.supports_public_rest);
        assert!(zeta_markets.supports_symbol_rules);
        assert!(zeta_markets.supports_order_book_snapshot);
        assert!(!zeta_markets.supports_private_rest);
        assert!(!zeta_markets.supports_place_order);
        assert!(!zeta_markets.supports_public_streams);
    }
}
