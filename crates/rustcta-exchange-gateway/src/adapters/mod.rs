// Adapter modules intentionally keep offline request-spec, signing, stream and
// parser helpers next to each venue implementation even when only fixture tests
// call them today.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    CancelOrderRequest, ExchangeApiError, ExchangeClient, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeId;

use crate::{
    ensure_secret_free_serializable, BookSubscriptionAck, CredentialBoundary, GatewayError,
    GatewayExchangeStatus, GatewayIdentity, GatewayMode, GatewayProtocolRequest,
    GatewayProtocolResponse, GatewayRequestPayload, GatewayResponsePayload, GatewayStatus,
    GetCapabilitiesResponse, GetStatusResponse, LocalGateway, PrivateSubscriptionAck,
    SubscribeBooksResponse, SubscribePrivateResponse, GATEWAY_API_VERSION,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

pub mod apex;
pub mod ascendex;
pub mod aster;
pub mod backpack;
pub mod biconomy;
pub mod bigone;
pub mod binance;
pub mod binancecoinm;
pub mod bingx;
pub mod bitbank;
pub mod bitfinex;
pub mod bitflyer;
pub mod bitget;
pub mod bithumb;
pub mod bitkan;
pub mod bitmart;
pub mod bitmex;
pub mod bitrue;
pub mod bitso;
pub mod bitstamp;
pub mod bitunix;
pub mod bitvavo;
pub mod blofin;
pub mod btcmarkets;
pub mod btcturk;
pub mod bullish;
pub mod bybit;
pub mod coinbase;
pub mod coinbaseexchange;
pub mod coincheck;
pub mod coindcx;
pub mod coinex;
pub mod coinone;
pub mod coinsph;
pub mod coinspot;
pub mod coinstore;
pub mod cointr;
pub mod coinw;
pub mod cryptocom;
pub mod deepcoin;
pub mod delta;
pub mod deribit;
pub mod derive;
pub mod digifinex;
pub mod dydx;
pub mod gateio;
pub mod gemini;
pub mod grvt;
pub mod hashkey_global;
pub mod htx;
pub mod huobi;
pub mod hyperliquid;
pub mod independentreserve;
pub mod indodax;
pub mod kraken;
pub mod krakenfutures;
pub mod kucoin;
pub mod kucoinfutures;
pub mod lbank;
pub mod lighter;
pub mod luno;
pub mod mercado;
pub mod mexc;
pub mod okx;
pub mod orangex;
pub mod oxfun;
pub mod pacifica;
pub mod paper;
pub mod paradex;
pub mod phemex;
pub mod poloniex;
pub mod tapbit;
pub mod toobit;
pub mod upbit;
pub mod weex;
pub mod whitebit;
pub mod woo;
pub mod xt;

pub use apex::ApexGatewayConfig;
pub use ascendex::AscendexGatewayConfig;
pub use aster::AsterGatewayConfig;
pub use backpack::BackpackGatewayConfig;
pub use biconomy::BiconomyGatewayConfig;
pub use bigone::BigOneGatewayConfig;
pub use binance::BinanceGatewayConfig;
pub use binancecoinm::BinanceCoinMGatewayConfig;
pub use bingx::BingxGatewayConfig;
pub use bitbank::BitbankGatewayConfig;
pub use bitfinex::BitfinexGatewayConfig;
pub use bitflyer::BitflyerGatewayConfig;
pub use bitget::BitgetGatewayConfig;
pub use bithumb::BithumbGatewayConfig;
pub use bitkan::BitkanGatewayConfig;
pub use bitmart::BitmartGatewayConfig;
pub use bitmex::BitmexGatewayConfig;
pub use bitrue::BitrueGatewayConfig;
pub use bitso::BitsoGatewayConfig;
pub use bitstamp::BitstampGatewayConfig;
pub use bitunix::BitunixGatewayConfig;
pub use bitvavo::BitvavoGatewayConfig;
pub use blofin::BlofinGatewayConfig;
pub use btcmarkets::BtcMarketsGatewayConfig;
pub use btcturk::BtcTurkGatewayConfig;
pub use bullish::BullishGatewayConfig;
pub use bybit::BybitGatewayConfig;
pub use coinbase::CoinbaseGatewayConfig;
pub use coinbaseexchange::CoinbaseExchangeGatewayConfig;
pub use coincheck::CoincheckGatewayConfig;
pub use coindcx::CoinDcxGatewayConfig;
pub use coinex::CoinExGatewayConfig;
pub use coinone::CoinoneGatewayConfig;
pub use coinsph::CoinsPhGatewayConfig;
pub use coinspot::CoinspotGatewayConfig;
pub use coinstore::CoinstoreGatewayConfig;
pub use cointr::CointrGatewayConfig;
pub use coinw::CoinwGatewayConfig;
pub use cryptocom::CryptoComGatewayConfig;
pub use deepcoin::DeepcoinGatewayConfig;
pub use delta::DeltaGatewayConfig;
pub use deribit::DeribitGatewayConfig;
pub use derive::DeriveGatewayConfig;
pub use digifinex::DigiFinexGatewayConfig;
pub use dydx::DydxGatewayConfig;
pub use gateio::GateIoGatewayConfig;
pub use gemini::GeminiGatewayConfig;
pub use grvt::GrvtGatewayConfig;
pub use hashkey_global::HashKeyGlobalGatewayConfig;
pub use htx::HtxGatewayConfig;
pub use huobi::HuobiGatewayConfig;
pub use hyperliquid::HyperliquidGatewayConfig;
pub use independentreserve::IndependentReserveGatewayConfig;
pub use indodax::IndodaxGatewayConfig;
pub use kraken::KrakenGatewayConfig;
pub use krakenfutures::KrakenFuturesGatewayConfig;
pub use kucoin::KuCoinGatewayConfig;
pub use kucoinfutures::KuCoinFuturesGatewayConfig;
pub use lbank::LBankGatewayConfig;
pub use lighter::LighterGatewayConfig;
pub use luno::LunoGatewayConfig;
pub use mercado::MercadoGatewayConfig;
pub use mexc::MexcGatewayConfig;
pub use okx::OkxGatewayConfig;
pub use orangex::OrangeXGatewayConfig;
pub use oxfun::OxfunGatewayConfig;
pub use pacifica::PacificaGatewayConfig;
pub use paradex::ParadexGatewayConfig;
pub use phemex::PhemexGatewayConfig;
pub use poloniex::PoloniexGatewayConfig;
pub use tapbit::TapbitGatewayConfig;
pub use toobit::ToobitGatewayConfig;
pub use upbit::UpbitGatewayConfig;
pub use weex::WeexGatewayConfig;
pub use whitebit::WhiteBitGatewayConfig;
pub use woo::WooGatewayConfig;
pub use xt::XtGatewayConfig;

#[cfg(test)]
mod paper_tests;

#[async_trait]
pub trait GatewayAdapter: ExchangeClient {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus;
}

#[derive(Clone)]
pub struct AdapterBackedGateway {
    identity: GatewayIdentity,
    adapters: Arc<RwLock<HashMap<ExchangeId, Arc<dyn GatewayAdapter>>>>,
}

impl AdapterBackedGateway {
    pub fn new(gateway_id: impl Into<String>) -> Self {
        Self {
            identity: GatewayIdentity {
                gateway_id: gateway_id.into(),
                mode: GatewayMode::Local,
                credential_boundary: CredentialBoundary::GatewayOnly,
                started_at: Utc::now(),
            },
            adapters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn paper_only(gateway_id: impl Into<String>) -> Result<Self, GatewayError> {
        let gateway = Self::new(gateway_id);
        gateway.register_paper_adapter()?;
        Ok(gateway)
    }

    pub fn with_named_adapters(
        gateway_id: impl Into<String>,
        adapters: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, GatewayError> {
        let gateway = Self::new(gateway_id);
        for adapter in adapters {
            gateway.register_named_adapter(adapter.as_ref())?;
        }
        Ok(gateway)
    }

    pub fn register_named_adapter(&self, adapter: &str) -> Result<(), GatewayError> {
        let adapter = adapter.trim().to_ascii_lowercase();
        match adapter.as_str() {
            "" => Ok(()),
            "apex" | "apexpro" | "apex_pro" => self.register_apex_public_adapter(None),
            "paper" => self.register_paper_adapter(),
            "ascendex" | "ascend_ex" => self.register_ascendex_public_adapter(None),
            "aster" | "asterdex" | "aster_dex" => self.register_aster_public_adapter(None),
            "backpack" => self.register_backpack_public_adapter(None),
            "biconomy" => self.register_biconomy_public_adapter(None),
            "binance" => self.register_binance_public_adapter(None),
            "binancecoinm" | "binance_coinm" | "binance-coinm" | "binance_coin_m" => {
                self.register_binancecoinm_public_adapter(None)
            }
            "bigone" | "big_one" => self.register_bigone_public_adapter(None),
            "bingx" => self.register_bingx_public_adapter(None),
            "bitbank" => self.register_bitbank_public_adapter(None),
            "bitflyer" | "bit_flyer" => self.register_bitflyer_public_adapter(None),
            "bitfinex" => self.register_bitfinex_public_adapter(None),
            "bitunix" => self.register_bitunix_public_adapter(None),
            "bybit" => self.register_bybit_public_adapter(None),
            "bitkan" => self.register_bitkan_public_adapter(None),
            "bitmart" => self.register_bitmart_public_adapter(None),
            "bitso" => self.register_bitso_public_adapter(None),
            "bitstamp" => self.register_bitstamp_public_adapter(None),
            "bitvavo" => self.register_bitvavo_public_adapter(None),
            "bitrue" => self.register_bitrue_public_adapter(None),
            "btcturk" | "btc_turk" | "btc-turk" => self.register_btcturk_public_adapter(None),
            "bitmex" => self.register_bitmex_public_adapter(None),
            "bitget" => self.register_bitget_public_adapter(None),
            "bithumb" => self.register_bithumb_public_adapter(None),
            "blofin" | "blo_fin" => self.register_blofin_public_adapter(None),
            "btcmarkets" | "btc_markets" => self.register_btcmarkets_public_adapter(None),
            "bullish" => self.register_bullish_public_adapter(None),
            "coinbase" => self.register_coinbase_public_adapter(None),
            "coinbaseexchange" | "coinbase_exchange" | "coinbase-exchange" => {
                self.register_coinbaseexchange_public_adapter(None)
            }
            "coincheck" => self.register_coincheck_public_adapter(None),
            "coindcx" | "coin_dcx" => self.register_coindcx_public_adapter(None),
            "coinstore" => self.register_coinstore_public_adapter(None),
            "cointr" => self.register_cointr_public_adapter(None),
            "coinw" => self.register_coinw_public_adapter(None),
            "coinex" => self.register_coinex_public_adapter(None),
            "coinone" => self.register_coinone_public_adapter(None),
            "coinspot" | "coin_spot" => self.register_coinspot_public_adapter(None),
            "coinsph" | "coins_ph" | "coins.ph" => self.register_coinsph_public_adapter(None),
            "crypto.com" | "cryptocom" | "crypto_com" => {
                self.register_cryptocom_public_adapter(None)
            }
            "deepcoin" | "deep_coin" => self.register_deepcoin_public_adapter(None),
            "delta" | "delta_exchange" | "delta-exchange" => {
                self.register_delta_public_adapter(None)
            }
            "deribit" => self.register_deribit_public_adapter(None),
            "derive" => self.register_derive_public_adapter(None),
            "digifinex" | "digi_finex" => self.register_digifinex_public_adapter(None),
            "dydx" | "dydx_v4" | "dydxv4" => self.register_dydx_public_adapter(None),
            "gate" | "gate.io" | "gateio" => self.register_gateio_public_adapter(None),
            "gemini" => self.register_gemini_public_adapter(None),
            "grvt" => self.register_grvt_public_adapter(None),
            "hashkey" | "hashkey_global" | "hashkey-global" => {
                self.register_hashkey_global_public_adapter(None)
            }
            "hyperliquid" | "hyper_liquid" => self.register_hyperliquid_public_adapter(None),
            "htx" => self.register_htx_public_adapter(None),
            "huobi" => self.register_huobi_public_adapter(None),
            "independentreserve" | "independent_reserve" => {
                self.register_independentreserve_public_adapter(None)
            }
            "indodax" => self.register_indodax_public_adapter(None),
            "kucoin" => self.register_kucoin_public_adapter(None),
            "kucoinfutures" | "kucoin_futures" | "kucoin-futures" => {
                self.register_kucoinfutures_public_adapter(None)
            }
            "kraken" => self.register_kraken_public_adapter(None),
            "krakenfutures" | "kraken_futures" | "kraken-futures" => {
                self.register_krakenfutures_public_adapter(None)
            }
            "lbank" => self.register_lbank_public_adapter(None),
            "lighter" => self.register_lighter_public_adapter(None),
            "luno" => self.register_luno_public_adapter(None),
            "mercado" | "mercadobitcoin" | "mercado_bitcoin" | "mercado-bitcoin" => {
                self.register_mercado_public_adapter(None)
            }
            "mexc" => self.register_mexc_public_adapter(None),
            "okx" => self.register_okx_public_adapter(None),
            "orange_x" | "orangex" => self.register_orangex_public_adapter(None),
            "ox.fun" | "ox_fun" | "oxfun" => self.register_oxfun_public_adapter(None),
            "pacifica" => self.register_pacifica_public_adapter(None),
            "paradex" => self.register_paradex_public_adapter(None),
            "phemex" => self.register_phemex_public_adapter(None),
            "poloniex" => self.register_poloniex_public_adapter(None),
            "tapbit" => self.register_tapbit_public_adapter(None),
            "toobit" => self.register_toobit_public_adapter(None),
            "upbit" => self.register_upbit_public_adapter(None),
            "weex" => self.register_weex_public_adapter(None),
            "whitebit" | "white_bit" => self.register_whitebit_public_adapter(None),
            "woo" | "woo_x" | "woox" => self.register_woo_public_adapter(None),
            "xt" | "xt.com" | "xtcom" => self.register_xt_public_adapter(None),
            other => Err(GatewayError::UnsupportedOperation {
                operation: format!("unknown gateway adapter {other}"),
            }),
        }
    }

    pub fn register_paper_adapter(&self) -> Result<(), GatewayError> {
        self.register_adapter(Arc::new(paper::PaperGatewayAdapter::default_paper()?))
    }

    pub fn register_apex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = apex::ApexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_apex_adapter(config)
    }

    pub fn register_apex_adapter(
        &self,
        config: apex::ApexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            apex::ApexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_ascendex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = ascendex::AscendexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_ascendex_adapter(config)
    }

    pub fn register_ascendex_adapter(
        &self,
        config: ascendex::AscendexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            ascendex::AscendexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_aster_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = aster::AsterGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_aster_adapter(config)
    }

    pub fn register_aster_adapter(
        &self,
        config: aster::AsterGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            aster::AsterGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_backpack_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = backpack::BackpackGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_backpack_adapter(config)
    }

    pub fn register_backpack_adapter(
        &self,
        config: backpack::BackpackGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            backpack::BackpackGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_biconomy_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = biconomy::BiconomyGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_biconomy_adapter(config)
    }

    pub fn register_biconomy_adapter(
        &self,
        config: biconomy::BiconomyGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            biconomy::BiconomyGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_binance_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = binance::BinanceGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_binance_adapter(config)
    }

    pub fn register_binance_adapter(
        &self,
        config: binance::BinanceGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            binance::BinanceGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_binancecoinm_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = binancecoinm::BinanceCoinMGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_binancecoinm_adapter(config)
    }

    pub fn register_binancecoinm_adapter(
        &self,
        config: binancecoinm::BinanceCoinMGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = binancecoinm::BinanceCoinMGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bigone_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bigone::BigOneGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.contract_rest_base_url = rest_base_url;
        }
        self.register_bigone_adapter(config)
    }

    pub fn register_bigone_adapter(
        &self,
        config: bigone::BigOneGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bigone::BigOneGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bingx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bingx::BingxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bingx_adapter(config)
    }

    pub fn register_bingx_adapter(
        &self,
        config: bingx::BingxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bingx::BingxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitbank_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitbank::BitbankGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.public_rest_base_url = rest_base_url;
        }
        self.register_bitbank_adapter(config)
    }

    pub fn register_bitbank_adapter(
        &self,
        config: bitbank::BitbankGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitbank::BitbankGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitflyer_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitflyer::BitflyerGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitflyer_adapter(config)
    }

    pub fn register_bitflyer_adapter(
        &self,
        config: bitflyer::BitflyerGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitflyer::BitflyerGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitrue_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitrue::BitrueGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_bitrue_adapter(config)
    }

    pub fn register_bitrue_adapter(
        &self,
        config: bitrue::BitrueGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitrue::BitrueGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitmex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitmex::BitmexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitmex_adapter(config)
    }

    pub fn register_bitmex_adapter(
        &self,
        config: bitmex::BitmexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitmex::BitmexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitunix_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitunix::BitunixGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_bitunix_adapter(config)
    }

    pub fn register_bitunix_adapter(
        &self,
        config: bitunix::BitunixGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitunix::BitunixGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bybit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bybit::BybitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bybit_adapter(config)
    }

    pub fn register_bybit_adapter(
        &self,
        config: bybit::BybitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bybit::BybitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitfinex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitfinex::BitfinexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.public_rest_base_url = rest_base_url.clone();
            config.private_rest_base_url = rest_base_url;
        }
        self.register_bitfinex_adapter(config)
    }

    pub fn register_bitfinex_adapter(
        &self,
        config: bitfinex::BitfinexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitfinex::BitfinexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitmart_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitmart::BitmartGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_bitmart_adapter(config)
    }

    pub fn register_bitmart_adapter(
        &self,
        config: bitmart::BitmartGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitmart::BitmartGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitkan_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitkan::BitkanGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitkan_adapter(config)
    }

    pub fn register_bitkan_adapter(
        &self,
        config: bitkan::BitkanGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitkan::BitkanGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitso_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitso::BitsoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitso_adapter(config)
    }

    pub fn register_bitso_adapter(
        &self,
        config: bitso::BitsoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitso::BitsoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitstamp_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitstamp::BitstampGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitstamp_adapter(config)
    }

    pub fn register_bitstamp_adapter(
        &self,
        config: bitstamp::BitstampGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitstamp::BitstampGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitvavo_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitvavo::BitvavoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitvavo_adapter(config)
    }

    pub fn register_bitvavo_adapter(
        &self,
        config: bitvavo::BitvavoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitvavo::BitvavoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_btcturk_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = btcturk::BtcTurkGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_btcturk_adapter(config)
    }

    pub fn register_btcturk_adapter(
        &self,
        config: btcturk::BtcTurkGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            btcturk::BtcTurkGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_blofin_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = blofin::BlofinGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_blofin_adapter(config)
    }

    pub fn register_blofin_adapter(
        &self,
        config: blofin::BlofinGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            blofin::BlofinGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_btcmarkets_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = btcmarkets::BtcMarketsGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_btcmarkets_adapter(config)
    }

    pub fn register_btcmarkets_adapter(
        &self,
        config: btcmarkets::BtcMarketsGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = btcmarkets::BtcMarketsGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bullish_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bullish::BullishGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bullish_adapter(config)
    }

    pub fn register_bullish_adapter(
        &self,
        config: bullish::BullishGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bullish::BullishGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitget_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitget::BitgetGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitget_adapter(config)
    }

    pub fn register_bitget_adapter(
        &self,
        config: bitget::BitgetGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitget::BitgetGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bithumb_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bithumb::BithumbGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bithumb_adapter(config)
    }

    pub fn register_bithumb_adapter(
        &self,
        config: bithumb::BithumbGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bithumb::BithumbGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinbase_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinbase::CoinbaseGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.international_rest_base_url = rest_base_url;
        }
        self.register_coinbase_adapter(config)
    }

    pub fn register_coinbase_adapter(
        &self,
        config: coinbase::CoinbaseGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinbase::CoinbaseGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinbaseexchange_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinbaseexchange::CoinbaseExchangeGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinbaseexchange_adapter(config)
    }

    pub fn register_coinbaseexchange_adapter(
        &self,
        config: coinbaseexchange::CoinbaseExchangeGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = coinbaseexchange::CoinbaseExchangeGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coincheck_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coincheck::CoincheckGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coincheck_adapter(config)
    }

    pub fn register_coincheck_adapter(
        &self,
        config: coincheck::CoincheckGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = coincheck::CoincheckGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coindcx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coindcx::CoinDcxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_coindcx_adapter(config)
    }

    pub fn register_coindcx_adapter(
        &self,
        config: coindcx::CoinDcxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coindcx::CoinDcxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_cointr_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = cointr::CointrGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_cointr_adapter(config)
    }

    pub fn register_cointr_adapter(
        &self,
        config: cointr::CointrGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            cointr::CointrGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinw_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinw::CoinwGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinw_adapter(config)
    }

    pub fn register_coinw_adapter(
        &self,
        config: coinw::CoinwGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinw::CoinwGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinex::CoinExGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinex_adapter(config)
    }

    pub fn register_coinex_adapter(
        &self,
        config: coinex::CoinExGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinex::CoinExGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinone_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinone::CoinoneGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinone_adapter(config)
    }

    pub fn register_coinone_adapter(
        &self,
        config: coinone::CoinoneGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinone::CoinoneGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinspot_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinspot::CoinspotGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url.clone();
            config.public_rest_base_url = rest_base_url.clone();
            config.private_rest_base_url = rest_base_url.clone();
            config.read_only_rest_base_url = rest_base_url;
        }
        self.register_coinspot_adapter(config)
    }

    pub fn register_coinspot_adapter(
        &self,
        config: coinspot::CoinspotGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinspot::CoinspotGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinsph_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinsph::CoinsPhGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinsph_adapter(config)
    }

    pub fn register_coinsph_adapter(
        &self,
        config: coinsph::CoinsPhGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinsph::CoinsPhGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinstore_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinstore::CoinstoreGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_coinstore_adapter(config)
    }

    pub fn register_coinstore_adapter(
        &self,
        config: coinstore::CoinstoreGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = coinstore::CoinstoreGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_cryptocom_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = cryptocom::CryptoComGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_cryptocom_adapter(config)
    }

    pub fn register_cryptocom_adapter(
        &self,
        config: cryptocom::CryptoComGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = cryptocom::CryptoComGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_deepcoin_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = deepcoin::DeepcoinGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_deepcoin_adapter(config)
    }

    pub fn register_deepcoin_adapter(
        &self,
        config: deepcoin::DeepcoinGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            deepcoin::DeepcoinGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_delta_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = delta::DeltaGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_delta_adapter(config)
    }

    pub fn register_delta_adapter(
        &self,
        config: delta::DeltaGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            delta::DeltaGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_deribit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = deribit::DeribitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_deribit_adapter(config)
    }

    pub fn register_deribit_adapter(
        &self,
        config: deribit::DeribitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            deribit::DeribitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_derive_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = derive::DeriveGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_derive_adapter(config)
    }

    pub fn register_derive_adapter(
        &self,
        config: derive::DeriveGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            derive::DeriveGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_kucoin_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = kucoin::KuCoinGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_kucoin_adapter(config)
    }

    pub fn register_kucoin_adapter(
        &self,
        config: kucoin::KuCoinGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            kucoin::KuCoinGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_kucoinfutures_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = kucoinfutures::KuCoinFuturesGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_kucoinfutures_adapter(config)
    }

    pub fn register_kucoinfutures_adapter(
        &self,
        config: kucoinfutures::KuCoinFuturesGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = kucoinfutures::KuCoinFuturesGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_kraken_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = kraken::KrakenGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_kraken_adapter(config)
    }

    pub fn register_kraken_adapter(
        &self,
        config: kraken::KrakenGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            kraken::KrakenGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_krakenfutures_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = krakenfutures::KrakenFuturesGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_krakenfutures_adapter(config)
    }

    pub fn register_krakenfutures_adapter(
        &self,
        config: krakenfutures::KrakenFuturesGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = krakenfutures::KrakenFuturesGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_lbank_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = lbank::LBankGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.contract_rest_base_url = rest_base_url;
        }
        self.register_lbank_adapter(config)
    }

    pub fn register_lbank_adapter(
        &self,
        config: lbank::LBankGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            lbank::LBankGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_lighter_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = lighter::LighterGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_lighter_adapter(config)
    }

    pub fn register_lighter_adapter(
        &self,
        config: lighter::LighterGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            lighter::LighterGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_luno_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = luno::LunoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_luno_adapter(config)
    }

    pub fn register_luno_adapter(
        &self,
        config: luno::LunoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            luno::LunoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_mercado_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = mercado::MercadoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_mercado_adapter(config)
    }

    pub fn register_mercado_adapter(
        &self,
        config: mercado::MercadoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            mercado::MercadoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_mexc_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = mexc::MexcGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_mexc_adapter(config)
    }

    pub fn register_mexc_adapter(
        &self,
        config: mexc::MexcGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            mexc::MexcGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_gateio_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = gateio::GateIoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_gateio_adapter(config)
    }

    pub fn register_gateio_adapter(
        &self,
        config: gateio::GateIoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            gateio::GateIoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_gemini_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = gemini::GeminiGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_gemini_adapter(config)
    }

    pub fn register_gemini_adapter(
        &self,
        config: gemini::GeminiGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            gemini::GeminiGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_grvt_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = grvt::GrvtGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.market_data_rest_base_url = rest_base_url.clone();
            config.trading_rest_base_url = rest_base_url;
        }
        self.register_grvt_adapter(config)
    }

    pub fn register_grvt_adapter(
        &self,
        config: grvt::GrvtGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            grvt::GrvtGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_htx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = htx::HtxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.linear_rest_base_url = rest_base_url;
        }
        self.register_htx_adapter(config)
    }

    pub fn register_htx_adapter(&self, config: htx::HtxGatewayConfig) -> Result<(), GatewayError> {
        let adapter = htx::HtxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_huobi_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = htx::HtxGatewayConfig::huobi_legacy();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.linear_rest_base_url = rest_base_url;
        }
        self.register_huobi_adapter(config)
    }

    pub fn register_huobi_adapter(
        &self,
        mut config: htx::HtxGatewayConfig,
    ) -> Result<(), GatewayError> {
        config.set_huobi_legacy_profile();
        let adapter = htx::HtxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_indodax_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = indodax::IndodaxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_indodax_adapter(config)
    }

    pub fn register_indodax_adapter(
        &self,
        config: indodax::IndodaxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            indodax::IndodaxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_digifinex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = digifinex::DigiFinexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.swap_rest_base_url = rest_base_url;
        }
        self.register_digifinex_adapter(config)
    }

    pub fn register_digifinex_adapter(
        &self,
        config: digifinex::DigiFinexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = digifinex::DigiFinexGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_dydx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = dydx::DydxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.indexer_rest_base_url = rest_base_url;
        }
        self.register_dydx_adapter(config)
    }

    pub fn register_dydx_adapter(
        &self,
        config: dydx::DydxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            dydx::DydxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_hashkey_global_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = hashkey_global::HashKeyGlobalGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_hashkey_global_adapter(config)
    }

    pub fn register_hashkey_global_adapter(
        &self,
        config: hashkey_global::HashKeyGlobalGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = hashkey_global::HashKeyGlobalGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_independentreserve_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = independentreserve::IndependentReserveGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_independentreserve_adapter(config)
    }

    pub fn register_independentreserve_adapter(
        &self,
        config: independentreserve::IndependentReserveGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = independentreserve::IndependentReserveGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_hyperliquid_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = hyperliquid::HyperliquidGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_hyperliquid_adapter(config)
    }

    pub fn register_hyperliquid_adapter(
        &self,
        config: hyperliquid::HyperliquidGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = hyperliquid::HyperliquidGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_okx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = okx::OkxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_okx_adapter(config)
    }

    pub fn register_okx_adapter(&self, config: okx::OkxGatewayConfig) -> Result<(), GatewayError> {
        let adapter = okx::OkxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_orangex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = orangex::OrangeXGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_orangex_adapter(config)
    }

    pub fn register_orangex_adapter(
        &self,
        config: orangex::OrangeXGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            orangex::OrangeXGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_oxfun_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = oxfun::OxfunGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_oxfun_adapter(config)
    }

    pub fn register_oxfun_adapter(
        &self,
        config: oxfun::OxfunGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            oxfun::OxfunGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_pacifica_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = pacifica::PacificaGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_pacifica_adapter(config)
    }

    pub fn register_pacifica_adapter(
        &self,
        config: pacifica::PacificaGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            pacifica::PacificaGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_paradex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = paradex::ParadexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_paradex_adapter(config)
    }

    pub fn register_paradex_adapter(
        &self,
        config: paradex::ParadexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            paradex::ParadexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_phemex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = phemex::PhemexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_phemex_adapter(config)
    }

    pub fn register_phemex_adapter(
        &self,
        config: phemex::PhemexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            phemex::PhemexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_poloniex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = poloniex::PoloniexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_poloniex_adapter(config)
    }

    pub fn register_poloniex_adapter(
        &self,
        config: poloniex::PoloniexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            poloniex::PoloniexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_tapbit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = tapbit::TapbitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.swap_rest_base_url = rest_base_url;
        }
        self.register_tapbit_adapter(config)
    }

    pub fn register_tapbit_adapter(
        &self,
        config: tapbit::TapbitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            tapbit::TapbitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_toobit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = toobit::ToobitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_toobit_adapter(config)
    }

    pub fn register_toobit_adapter(
        &self,
        config: toobit::ToobitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            toobit::ToobitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_upbit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = upbit::UpbitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_upbit_adapter(config)
    }

    pub fn register_upbit_adapter(
        &self,
        config: upbit::UpbitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            upbit::UpbitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_whitebit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = whitebit::WhiteBitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_whitebit_adapter(config)
    }

    pub fn register_whitebit_adapter(
        &self,
        config: whitebit::WhiteBitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            whitebit::WhiteBitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_weex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = weex::WeexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.contract_rest_base_url = rest_base_url;
        }
        self.register_weex_adapter(config)
    }

    pub fn register_weex_adapter(
        &self,
        config: weex::WeexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            weex::WeexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_woo_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = woo::WooGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_woo_adapter(config)
    }

    pub fn register_woo_adapter(&self, config: woo::WooGatewayConfig) -> Result<(), GatewayError> {
        let adapter = woo::WooGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_xt_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = xt::XtGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.spot_rest_base_url = rest_base_url.clone();
            config.futures_rest_base_url = rest_base_url;
        }
        self.register_xt_adapter(config)
    }

    pub fn register_xt_adapter(&self, config: xt::XtGatewayConfig) -> Result<(), GatewayError> {
        let adapter = xt::XtGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_adapter(&self, adapter: Arc<dyn GatewayAdapter>) -> Result<(), GatewayError> {
        let exchange = adapter.exchange();
        let mut adapters = self
            .adapters
            .write()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?;
        if adapters.contains_key(&exchange) {
            return Err(GatewayError::Rejected(format!(
                "gateway adapter already registered for {exchange}"
            )));
        }
        adapters.insert(exchange, adapter);
        Ok(())
    }

    pub fn adapter_count(&self) -> Result<usize, GatewayError> {
        Ok(self
            .adapters
            .read()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?
            .len())
    }

    fn adapter_for(&self, exchange: &ExchangeId) -> Result<Arc<dyn GatewayAdapter>, GatewayError> {
        self.adapters
            .read()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?
            .get(exchange)
            .cloned()
            .ok_or_else(|| GatewayError::UnsupportedOperation {
                operation: format!("no gateway adapter registered for {exchange}"),
            })
    }
}

#[async_trait]
impl LocalGateway for AdapterBackedGateway {
    async fn status(&self) -> Result<GatewayStatus, GatewayError> {
        let adapters = self
            .adapters
            .read()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?;
        let exchanges = adapters
            .values()
            .map(|adapter| adapter.gateway_exchange_status())
            .collect();
        Ok(GatewayStatus {
            api_version: GATEWAY_API_VERSION.to_string(),
            identity: self.identity.clone(),
            exchanges,
        })
    }

    async fn handle_typed(
        &self,
        request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError> {
        request.validate()?;
        ensure_secret_free_serializable(&request, "request")?;

        let request_id = request.request_id.clone();
        let operation = request.operation;
        let payload = match request.payload {
            GatewayRequestPayload::GetStatus(request) => {
                let mut status = self.status().await?;
                if !request.include_exchanges {
                    status.exchanges.clear();
                }
                GatewayResponsePayload::Status(GetStatusResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    status,
                })
            }
            GatewayRequestPayload::GetCapabilities(request) => {
                let capabilities = if request.exchanges.is_empty() {
                    self.adapters
                        .read()
                        .map_err(|_| {
                            GatewayError::Rejected("adapter registry lock poisoned".to_string())
                        })?
                        .values()
                        .map(|adapter| adapter.capabilities())
                        .collect()
                } else {
                    let mut capabilities = Vec::with_capacity(request.exchanges.len());
                    for exchange in request.exchanges {
                        capabilities.push(self.adapter_for(&exchange)?.capabilities());
                    }
                    capabilities
                };
                GatewayResponsePayload::Capabilities(GetCapabilitiesResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    capabilities,
                })
            }
            GatewayRequestPayload::GetBalances(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::Balances(
                    adapter
                        .get_balances(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetPositions(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::Positions(
                    adapter
                        .get_positions(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetOrderBook(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::OrderBook(
                    adapter
                        .get_order_book(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetSymbolRules(request) => {
                let exchange = request
                    .symbols
                    .first()
                    .map(|symbol| symbol.exchange.clone())
                    .ok_or_else(|| {
                        GatewayError::Rejected(
                            "get_symbol_rules requires at least one symbol scope".to_string(),
                        )
                    })?;
                let adapter = self.adapter_for(&exchange)?;
                GatewayResponsePayload::SymbolRules(
                    adapter
                        .get_symbol_rules(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetFees(request) => {
                let exchange = request
                    .symbols
                    .first()
                    .map(|symbol| symbol.exchange.clone())
                    .ok_or_else(|| {
                        GatewayError::Rejected(
                            "get_fees requires at least one symbol scope".to_string(),
                        )
                    })?;
                let adapter = self.adapter_for(&exchange)?;
                GatewayResponsePayload::Fees(
                    adapter
                        .get_fees(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::PlaceOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::PlaceOrder(
                    adapter
                        .place_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::PlaceQuoteMarketOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::PlaceOrder(
                    adapter
                        .place_quote_market_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::CancelOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::CancelOrder(
                    adapter
                        .cancel_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::AmendOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::AmendOrder(
                    adapter
                        .amend_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::PlaceOrderList(request) => {
                let adapter = self.adapter_for(&request.symbol().exchange)?;
                GatewayResponsePayload::OrderList(
                    adapter
                        .place_order_list(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::BatchPlaceOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::BatchPlaceOrders(
                    adapter
                        .batch_place_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::BatchCancelOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::BatchCancelOrders(
                    adapter
                        .batch_cancel_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::CancelAllOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::CancelAllOrders(
                    adapter
                        .cancel_all_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::QueryOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::QueryOrder(
                    adapter
                        .query_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetOpenOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::OpenOrders(
                    adapter
                        .get_open_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetRecentFills(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::RecentFills(
                    adapter
                        .get_recent_fills(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::SubscribeBooks(request) => {
                if request.subscriptions.is_empty() {
                    return Err(GatewayError::Rejected(
                        "subscribe_books requires at least one subscription".to_string(),
                    ));
                }

                let mut subscriptions = Vec::with_capacity(request.subscriptions.len());
                for subscription in request.subscriptions {
                    let adapter = self.adapter_for(&subscription.symbol.exchange)?;
                    let subscription_id = adapter
                        .subscribe_public_stream(subscription.clone())
                        .await
                        .map_err(exchange_api_error_to_gateway)?;
                    subscriptions.push(BookSubscriptionAck {
                        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                        subscription_id,
                        exchange: subscription.symbol.exchange,
                        market_type: subscription.symbol.market_type,
                        canonical_symbol: subscription.symbol.canonical_symbol,
                        exchange_symbol: subscription.symbol.exchange_symbol,
                        kind: subscription.kind,
                        subscribed_at: Utc::now(),
                    });
                }

                GatewayResponsePayload::BooksSubscribed(SubscribeBooksResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    subscriptions,
                })
            }
            GatewayRequestPayload::SubscribePrivate(request) => {
                let mut subscriptions = Vec::with_capacity(request.subscriptions.len());
                for subscription in request.subscriptions {
                    let adapter = self.adapter_for(&subscription.exchange)?;
                    let capabilities = adapter.capabilities().private_stream_capabilities;
                    let subscription_id = adapter
                        .subscribe_private_stream(subscription.clone())
                        .await
                        .map_err(exchange_api_error_to_gateway)?;
                    subscriptions.push(PrivateSubscriptionAck {
                        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                        subscription_id,
                        exchange: subscription.exchange,
                        market_type: subscription.market_type,
                        account_id: subscription.account_id,
                        kind: subscription.kind,
                        capabilities,
                        subscribed_at: Utc::now(),
                    });
                }

                GatewayResponsePayload::PrivateSubscribed(SubscribePrivateResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    subscriptions,
                })
            }
        };

        let response = GatewayProtocolResponse::accepted(request_id, operation, payload);
        ensure_secret_free_serializable(&response, "response")?;
        Ok(response)
    }
}

fn exchange_api_error_to_gateway(error: ExchangeApiError) -> GatewayError {
    GatewayError::from(error)
}

pub(crate) fn missing_order_identity(request: &CancelOrderRequest) -> bool {
    request.client_order_id.is_none() && request.exchange_order_id.is_none()
}

pub(crate) fn response_metadata(
    exchange: ExchangeId,
    request_id: Option<String>,
) -> rustcta_exchange_api::ResponseMetadata {
    rustcta_exchange_api::ResponseMetadata {
        request_id,
        ..rustcta_exchange_api::ResponseMetadata::new(exchange, Utc::now())
    }
}

pub(crate) fn ensure_exchange_api_schema(schema_version: u16) -> Result<(), ExchangeApiError> {
    if schema_version != EXCHANGE_API_SCHEMA_VERSION {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "unsupported exchange schema_version {}, expected {}",
                schema_version, EXCHANGE_API_SCHEMA_VERSION
            ),
        });
    }
    Ok(())
}
