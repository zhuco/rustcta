// Adapter modules intentionally keep offline request-spec, signing, stream and
// parser helpers next to each venue implementation even when only fixture tests
// call them today.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountControlCapabilities, CancelOrderRequest, ClosePositionRequest, ClosePositionResponse,
    CountdownCancelAllRequest, CountdownCancelAllResponse, ExchangeApiError, ExchangeApiResult,
    ExchangeClient, FundingRatesRequest, FundingRatesResponse, PlaceOrderRequest,
    PlaceOrderResponse, SetLeverageRequest, SetLeverageResponse, SetMarginModeRequest,
    SetMarginModeResponse, SetPositionModeRequest, SetPositionModeResponse,
    SymbolAccountConfigRequest, SymbolAccountConfigResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeId, MarketType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce,
};

use crate::{
    ensure_secret_free_serializable, BookSubscriptionAck, CredentialBoundary, GatewayError,
    GatewayExchangeStatus, GatewayIdentity, GatewayMode, GatewayProtocolRequest,
    GatewayProtocolResponse, GatewayRequestPayload, GatewayResponsePayload, GatewayStatus,
    GetCapabilitiesResponse, GetStatusResponse, LocalGateway, PrivateSubscriptionAck,
    SubscribeBooksResponse, SubscribePrivateResponse, GATEWAY_API_VERSION,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

pub mod aark;
pub mod aftermath;
pub mod alpaca;
pub mod apex;
pub mod apollox_dex;
pub mod arkham;
pub mod ascendex;
pub mod aster;
pub mod backpack;
pub mod bequant;
pub mod biconomy;
pub mod bigone;
pub mod binance;
pub mod binancecoinm;
pub mod binanceus;
pub mod bingx;
pub mod bit2c;
pub mod bitbank;
pub mod bitbns;
pub mod bitfinex;
pub mod bitflyer;
pub mod bitget;
pub mod bithumb;
pub mod bitkan;
pub mod bitmart;
pub mod bitmex;
pub mod bitopro;
pub mod bitrue;
pub mod bitso;
pub mod bitstamp;
pub mod bitteam;
pub mod bittrade;
pub mod bitunix;
pub mod bitvavo;
pub mod blockchaincom;
pub mod blofin;
pub mod bsx;
pub mod btcbox;
pub mod btcmarkets;
pub mod btcturk;
pub mod bullish;
pub mod bybit;
pub mod bybiteu;
pub mod bydfi;
pub mod cex;
pub mod cod3x;
pub mod coinbase;
pub mod coinbaseexchange;
pub mod coincheck;
pub mod coindcx;
pub mod coinex;
pub mod coinmate;
pub mod coinmetro;
pub mod coinone;
pub mod coinsph;
pub mod coinspot;
pub mod coinstore;
pub mod cointr;
pub mod coinw;
pub mod cryptocom;
pub mod cryptomus;
pub mod d8x;
pub mod deepcoin;
pub mod delta;
pub mod deribit;
pub mod derive;
pub mod derive_chain_perps;
pub mod digifinex;
pub mod dydx;
pub mod equation;
pub mod exmo;
pub mod fmfwio;
pub mod foxbit;
pub mod gateio;
pub mod gemini;
pub mod grvt;
pub mod hashkey_global;
pub mod hibachi;
pub mod hitbtc;
pub mod hollaex;
pub mod htx;
pub mod huobi;
pub mod hyperliquid;
pub mod independentreserve;
pub mod indodax;
pub mod kcex;
pub mod kraken;
pub mod krakenfutures;
pub mod kucoin;
pub mod kucoinfutures;
pub mod latoken;
pub mod lbank;
pub mod lighter;
pub mod luno;
pub mod mango_markets;
pub mod mercado;
pub mod mexc;
pub mod modetrade;
pub mod myokx;
pub mod ndax;
pub mod novadax;
pub mod okx;
pub mod okxus;
pub mod onetrading;
pub mod orangex;
pub mod oxfun;
pub mod p2b;
pub mod pacifica;
pub mod paper;
pub mod paradex;
pub mod paymium;
pub mod phemex;
pub mod poloniex;
pub mod tapbit;
pub mod tokocrypto;
pub mod toobit;
pub mod upbit;
pub mod wavesexchange;
pub mod weex;
pub mod whitebit;
pub mod woo;
pub mod woofipro;
pub mod xt;
pub mod yobit;
pub mod zaif;
pub mod zebpay;
pub mod zeta_markets;

pub use aark::AarkGatewayConfig;
pub use aftermath::AftermathGatewayConfig;
pub use alpaca::AlpacaGatewayConfig;
pub use apex::ApexGatewayConfig;
pub use apollox_dex::ApolloxDexGatewayConfig;
pub use arkham::ArkhamGatewayConfig;
pub use ascendex::AscendexGatewayConfig;
pub use aster::AsterGatewayConfig;
pub use backpack::BackpackGatewayConfig;
pub use bequant::BequantGatewayConfig;
pub use biconomy::BiconomyGatewayConfig;
pub use bigone::BigOneGatewayConfig;
pub use binance::BinanceGatewayConfig;
pub use binancecoinm::BinanceCoinMGatewayConfig;
pub use binanceus::BinanceUsGatewayConfig;
pub use bingx::BingxGatewayConfig;
pub use bit2c::Bit2cGatewayConfig;
pub use bitbank::BitbankGatewayConfig;
pub use bitbns::BitbnsGatewayConfig;
pub use bitfinex::BitfinexGatewayConfig;
pub use bitflyer::BitflyerGatewayConfig;
pub use bitget::BitgetGatewayConfig;
pub use bithumb::BithumbGatewayConfig;
pub use bitkan::BitkanGatewayConfig;
pub use bitmart::BitmartGatewayConfig;
pub use bitmex::BitmexGatewayConfig;
pub use bitopro::BitoproGatewayConfig;
pub use bitrue::BitrueGatewayConfig;
pub use bitso::BitsoGatewayConfig;
pub use bitstamp::BitstampGatewayConfig;
pub use bitteam::BitteamGatewayConfig;
pub use bittrade::BittradeGatewayConfig;
pub use bitunix::BitunixGatewayConfig;
pub use bitvavo::BitvavoGatewayConfig;
pub use blockchaincom::BlockchainComGatewayConfig;
pub use blofin::BlofinGatewayConfig;
pub use bsx::BsxGatewayConfig;
pub use btcbox::BtcboxGatewayConfig;
pub use btcmarkets::BtcMarketsGatewayConfig;
pub use btcturk::BtcTurkGatewayConfig;
pub use bullish::BullishGatewayConfig;
pub use bybit::BybitGatewayConfig;
pub use bybiteu::BybiteuGatewayConfig;
pub use bydfi::BydfiGatewayConfig;
pub use cex::CexGatewayConfig;
pub use cod3x::Cod3xGatewayConfig;
pub use coinbase::CoinbaseGatewayConfig;
pub use coinbaseexchange::CoinbaseExchangeGatewayConfig;
pub use coincheck::CoincheckGatewayConfig;
pub use coindcx::CoinDcxGatewayConfig;
pub use coinex::CoinExGatewayConfig;
pub use coinmate::CoinmateGatewayConfig;
pub use coinmetro::CoinmetroGatewayConfig;
pub use coinone::CoinoneGatewayConfig;
pub use coinsph::CoinsPhGatewayConfig;
pub use coinspot::CoinspotGatewayConfig;
pub use coinstore::CoinstoreGatewayConfig;
pub use cointr::CointrGatewayConfig;
pub use coinw::CoinwGatewayConfig;
pub use cryptocom::CryptoComGatewayConfig;
pub use cryptomus::CryptomusGatewayConfig;
pub use d8x::D8xGatewayConfig;
pub use deepcoin::DeepcoinGatewayConfig;
pub use delta::DeltaGatewayConfig;
pub use deribit::DeribitGatewayConfig;
pub use derive::DeriveGatewayConfig;
pub use derive_chain_perps::DeriveChainPerpsGatewayConfig;
pub use digifinex::DigiFinexGatewayConfig;
pub use dydx::DydxGatewayConfig;
pub use equation::EquationGatewayConfig;
pub use exmo::ExmoGatewayConfig;
pub use fmfwio::FmfwioGatewayConfig;
pub use foxbit::FoxbitGatewayConfig;
pub use gateio::GateIoGatewayConfig;
pub use gemini::GeminiGatewayConfig;
pub use grvt::GrvtGatewayConfig;
pub use hashkey_global::HashKeyGlobalGatewayConfig;
pub use hibachi::HibachiGatewayConfig;
pub use hitbtc::HitbtcGatewayConfig;
pub use hollaex::HollaexGatewayConfig;
pub use htx::HtxGatewayConfig;
pub use huobi::HuobiGatewayConfig;
pub use hyperliquid::HyperliquidGatewayConfig;
pub use independentreserve::IndependentReserveGatewayConfig;
pub use indodax::IndodaxGatewayConfig;
pub use kcex::KcexGatewayConfig;
pub use kraken::KrakenGatewayConfig;
pub use krakenfutures::KrakenFuturesGatewayConfig;
pub use kucoin::KuCoinGatewayConfig;
pub use kucoinfutures::KuCoinFuturesGatewayConfig;
pub use latoken::LatokenGatewayConfig;
pub use lbank::LBankGatewayConfig;
pub use lighter::LighterGatewayConfig;
pub use luno::LunoGatewayConfig;
pub use mango_markets::MangoMarketsGatewayConfig;
pub use mercado::MercadoGatewayConfig;
pub use mexc::MexcGatewayConfig;
pub use modetrade::ModetradeGatewayConfig;
pub use myokx::MyOkxGatewayConfig;
pub use ndax::NdaxGatewayConfig;
pub use novadax::NovadaxGatewayConfig;
pub use okx::OkxGatewayConfig;
pub use okxus::OkxusGatewayConfig;
pub use onetrading::OneTradingGatewayConfig;
pub use orangex::OrangeXGatewayConfig;
pub use oxfun::OxfunGatewayConfig;
pub use p2b::P2bGatewayConfig;
pub use pacifica::PacificaGatewayConfig;
pub use paradex::ParadexGatewayConfig;
pub use paymium::PaymiumGatewayConfig;
pub use phemex::PhemexGatewayConfig;
pub use poloniex::PoloniexGatewayConfig;
pub use tapbit::TapbitGatewayConfig;
pub use tokocrypto::TokocryptoGatewayConfig;
pub use toobit::ToobitGatewayConfig;
pub use upbit::UpbitGatewayConfig;
#[allow(unused_imports)]
pub use wavesexchange::WavesExchangeGatewayConfig;
pub use weex::WeexGatewayConfig;
pub use whitebit::WhiteBitGatewayConfig;
pub use woo::WooGatewayConfig;
pub use woofipro::WoofiproGatewayConfig;
pub use xt::XtGatewayConfig;
pub use yobit::YobitGatewayConfig;
pub use zaif::ZaifGatewayConfig;
pub use zebpay::ZebpayGatewayConfig;
pub use zeta_markets::ZetaMarketsGatewayConfig;

#[cfg(test)]
mod paper_tests;

#[async_trait]
pub trait GatewayAdapter: ExchangeClient {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus;

    fn account_control_capabilities(&self) -> AccountControlCapabilities {
        AccountControlCapabilities::unsupported(self.exchange())
    }

    async fn get_symbol_account_config(
        &self,
        _request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "get_symbol_account_config",
        })
    }

    async fn set_leverage(
        &self,
        _request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "set_leverage",
        })
    }

    async fn set_position_mode(
        &self,
        _request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "set_position_mode",
        })
    }

    async fn set_margin_mode(
        &self,
        _request: SetMarginModeRequest,
    ) -> ExchangeApiResult<SetMarginModeResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "set_margin_mode",
        })
    }

    async fn close_position(
        &self,
        _request: ClosePositionRequest,
    ) -> ExchangeApiResult<ClosePositionResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "close_position",
        })
    }

    async fn set_countdown_cancel_all(
        &self,
        _request: CountdownCancelAllRequest,
    ) -> ExchangeApiResult<CountdownCancelAllResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "set_countdown_cancel_all",
        })
    }

    async fn get_funding_rates(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        ExchangeClient::get_funding_rates(self, request).await
    }
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
            "aark" | "aark_digital" | "aark-digital" => self.register_aark_public_adapter(None),
            "apex" | "apexpro" | "apex_pro" => self.register_apex_public_adapter(None),
            "aftermath" => self.register_aftermath_public_adapter(None),
            "alpaca" => self.register_alpaca_public_adapter(None),
            "apollox_dex" | "apollox-dex" | "apollox" | "apx" => {
                self.register_apollox_dex_public_adapter(None)
            }
            "arkham" => self.register_arkham_public_adapter(None),
            "paper" => self.register_paper_adapter(),
            "ascendex" | "ascend_ex" => self.register_ascendex_public_adapter(None),
            "aster" | "asterdex" | "aster_dex" => self.register_aster_public_adapter(None),
            "backpack" => self.register_backpack_public_adapter(None),
            "bequant" | "be_quant" => self.register_bequant_public_adapter(None),
            "biconomy" => self.register_biconomy_public_adapter(None),
            "binance" => self.register_binance_public_adapter(None),
            "binancecoinm" | "binance_coinm" | "binance-coinm" | "binance_coin_m" => {
                self.register_binancecoinm_public_adapter(None)
            }
            "binanceus" | "binance_us" | "binance-us" => {
                self.register_binanceus_public_adapter(None)
            }
            "bigone" | "big_one" => self.register_bigone_public_adapter(None),
            "bingx" => self.register_bingx_public_adapter(None),
            "bit2c" | "bit_2c" | "bit2c.co.il" => self.register_bit2c_public_adapter(None),
            "bitbank" => self.register_bitbank_public_adapter(None),
            "bitflyer" | "bit_flyer" => self.register_bitflyer_public_adapter(None),
            "bitfinex" => self.register_bitfinex_public_adapter(None),
            "bitunix" => self.register_bitunix_public_adapter(None),
            "bybit" => self.register_bybit_public_adapter(None),
            "bybiteu" | "bybit_eu" | "bybit-eu" => self.register_bybiteu_public_adapter(None),
            "bydfi" | "bydfi.com" => self.register_bydfi_public_adapter(None),
            "bitkan" => self.register_bitkan_public_adapter(None),
            "bitmart" => self.register_bitmart_public_adapter(None),
            "bitopro" | "bito_pro" | "bito-pro" => self.register_bitopro_public_adapter(None),
            "bitso" => self.register_bitso_public_adapter(None),
            "bitstamp" => self.register_bitstamp_public_adapter(None),
            "bitteam" | "bit_team" | "bit.team" => self.register_bitteam_public_adapter(None),
            "bittrade" | "bittradejp" | "bittrade_jp" | "huobi_japan" => {
                self.register_bittrade_public_adapter(None)
            }
            "bitvavo" => self.register_bitvavo_public_adapter(None),
            "blockchain.com" | "blockchaincom" | "blockchain_com" | "blockchain-com" => {
                self.register_blockchaincom_public_adapter(None)
            }
            "bitrue" => self.register_bitrue_public_adapter(None),
            "btcturk" | "btc_turk" | "btc-turk" => self.register_btcturk_public_adapter(None),
            "bitmex" => self.register_bitmex_public_adapter(None),
            "bitget" => self.register_bitget_public_adapter(None),
            "bithumb" => self.register_bithumb_public_adapter(None),
            "blofin" | "blo_fin" => self.register_blofin_public_adapter(None),
            "bsx" => self.register_bsx_public_adapter(None),
            "btcbox" | "btc_box" => self.register_btcbox_public_adapter(None),
            "btcmarkets" | "btc_markets" => self.register_btcmarkets_public_adapter(None),
            "bullish" => self.register_bullish_public_adapter(None),
            "cex" | "cexio" | "cex.io" => self.register_cex_public_adapter(None),
            "coinbase" => self.register_coinbase_public_adapter(None),
            "coinbaseexchange" | "coinbase_exchange" | "coinbase-exchange" => {
                self.register_coinbaseexchange_public_adapter(None)
            }
            "coincheck" => self.register_coincheck_public_adapter(None),
            "coinmate" | "coin_mate" => self.register_coinmate_public_adapter(None),
            "coindcx" | "coin_dcx" => self.register_coindcx_public_adapter(None),
            "coinmetro" | "coin_metro" => self.register_coinmetro_public_adapter(None),
            "coinstore" => self.register_coinstore_public_adapter(None),
            "cointr" => self.register_cointr_public_adapter(None),
            "coinw" => self.register_coinw_public_adapter(None),
            "cod3x" | "cod3x_ai" | "cod3x-ai" => self.register_cod3x_public_adapter(None),
            "coinex" => self.register_coinex_public_adapter(None),
            "coinone" => self.register_coinone_public_adapter(None),
            "coinspot" | "coin_spot" => self.register_coinspot_public_adapter(None),
            "coinsph" | "coins_ph" | "coins.ph" => self.register_coinsph_public_adapter(None),
            "crypto.com" | "cryptocom" | "crypto_com" => {
                self.register_cryptocom_public_adapter(None)
            }
            "cryptomus" | "crypto_mus" => self.register_cryptomus_public_adapter(None),
            "d8x" | "d8x_exchange" | "d8x-exchange" => self.register_d8x_public_adapter(None),
            "deepcoin" | "deep_coin" => self.register_deepcoin_public_adapter(None),
            "delta" | "delta_exchange" | "delta-exchange" => {
                self.register_delta_public_adapter(None)
            }
            "deribit" => self.register_deribit_public_adapter(None),
            "derive" => self.register_derive_public_adapter(None),
            "derive_chain_perps" | "derive-chain-perps" | "derive_chain" => {
                self.register_derive_chain_perps_public_adapter(None)
            }
            "digifinex" | "digi_finex" => self.register_digifinex_public_adapter(None),
            "dydx" | "dydx_v4" | "dydxv4" => self.register_dydx_public_adapter(None),
            "equation" | "equation_dao" | "equationdao" => {
                self.register_equation_public_adapter(None)
            }
            "exmo" => self.register_exmo_public_adapter(None),
            "fmfwio" | "fmfw.io" | "fmfw" => self.register_fmfwio_public_adapter(None),
            "foxbit" | "fox_bit" => self.register_foxbit_public_adapter(None),
            "gate" | "gate.io" | "gateio" => self.register_gateio_public_adapter(None),
            "gemini" => self.register_gemini_public_adapter(None),
            "grvt" => self.register_grvt_public_adapter(None),
            "hashkey" | "hashkey_global" | "hashkey-global" => {
                self.register_hashkey_global_public_adapter(None)
            }
            "hollaex" | "hollaex_demo" | "hollaex-demo" => {
                self.register_hollaex_public_adapter(None)
            }
            "hibachi" => self.register_hibachi_public_adapter(None),
            "hitbtc" | "hitbtc.com" | "hit_btc" => self.register_hitbtc_public_adapter(None),
            "hyperliquid" | "hyper_liquid" => self.register_hyperliquid_public_adapter(None),
            "htx" => self.register_htx_public_adapter(None),
            "huobi" => self.register_huobi_public_adapter(None),
            "independentreserve" | "independent_reserve" => {
                self.register_independentreserve_public_adapter(None)
            }
            "indodax" => self.register_indodax_public_adapter(None),
            "kcex" | "kc_ex" | "kc-ex" => self.register_kcex_public_adapter(None),
            "kucoin" => self.register_kucoin_public_adapter(None),
            "kucoinfutures" | "kucoin_futures" | "kucoin-futures" => {
                self.register_kucoinfutures_public_adapter(None)
            }
            "latoken" => self.register_latoken_public_adapter(None),
            "kraken" => self.register_kraken_public_adapter(None),
            "krakenfutures" | "kraken_futures" | "kraken-futures" => {
                self.register_krakenfutures_public_adapter(None)
            }
            "lbank" => self.register_lbank_public_adapter(None),
            "lighter" => self.register_lighter_public_adapter(None),
            "luno" => self.register_luno_public_adapter(None),
            "mango_markets" | "mango-markets" | "mango" | "mango_markets_v4" => {
                self.register_mango_markets_public_adapter(None)
            }
            "mercado" | "mercadobitcoin" | "mercado_bitcoin" | "mercado-bitcoin" => {
                self.register_mercado_public_adapter(None)
            }
            "mexc" => self.register_mexc_public_adapter(None),
            "modetrade" | "mode_trade" | "mode-trade" => {
                self.register_modetrade_public_adapter(None)
            }
            "myokx" | "my_okx" | "my-okx" | "okx_eea" | "okx-eea" => {
                self.register_myokx_public_adapter(None)
            }
            "ndax" | "ndaxio" | "ndax_io" => self.register_ndax_public_adapter(None),
            "novadax" | "nova_dax" | "nova-dax" => self.register_novadax_public_adapter(None),
            "okx" => self.register_okx_public_adapter(None),
            "okxus" | "okx_us" | "okx-us" => self.register_okxus_public_adapter(None),
            "onetrading" | "one_trading" | "one-trading" => {
                self.register_onetrading_public_adapter(None)
            }
            "orange_x" | "orangex" => self.register_orangex_public_adapter(None),
            "ox.fun" | "ox_fun" | "oxfun" => self.register_oxfun_public_adapter(None),
            "p2b" | "p2pb2b" | "p2pb2b.com" => self.register_p2b_public_adapter(None),
            "pacifica" => self.register_pacifica_public_adapter(None),
            "paradex" => self.register_paradex_public_adapter(None),
            "paymium" => self.register_paymium_public_adapter(None),
            "phemex" => self.register_phemex_public_adapter(None),
            "poloniex" => self.register_poloniex_public_adapter(None),
            "tapbit" => self.register_tapbit_public_adapter(None),
            "tokocrypto" | "toko_crypto" | "toko-crypto" => {
                self.register_tokocrypto_public_adapter(None)
            }
            "toobit" => self.register_toobit_public_adapter(None),
            "upbit" => self.register_upbit_public_adapter(None),
            "wavesexchange" | "waves_exchange" | "waves.exchange" => {
                self.register_wavesexchange_public_adapter(None)
            }
            "weex" => self.register_weex_public_adapter(None),
            "whitebit" | "white_bit" => self.register_whitebit_public_adapter(None),
            "woo" | "woo_x" | "woox" => self.register_woo_public_adapter(None),
            "woofipro" | "woofi_pro" | "woofi-pro" => self.register_woofipro_public_adapter(None),
            "xt" | "xt.com" | "xtcom" => self.register_xt_public_adapter(None),
            "yobit" | "yo_bit" | "yobit.net" => self.register_yobit_public_adapter(None),
            "zaif" => self.register_zaif_public_adapter(None),
            "zebpay" | "zeb_pay" => self.register_zebpay_public_adapter(None),
            "zeta_markets" | "zetamarkets" | "zeta-markets" => {
                self.register_zeta_markets_public_adapter(None)
            }
            other => Err(GatewayError::UnsupportedOperation {
                operation: format!("unknown gateway adapter {other}"),
            }),
        }
    }

    pub fn register_paper_adapter(&self) -> Result<(), GatewayError> {
        self.register_adapter(Arc::new(paper::PaperGatewayAdapter::default_paper()?))
    }

    pub fn register_aark_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = aark::AarkGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_aark_adapter(config)
    }

    pub fn register_aark_adapter(
        &self,
        config: aark::AarkGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            aark::AarkGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
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

    pub fn register_apollox_dex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = apollox_dex::ApolloxDexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_apollox_dex_adapter(config)
    }

    pub fn register_apollox_dex_adapter(
        &self,
        config: apollox_dex::ApolloxDexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = apollox_dex::ApolloxDexGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_aftermath_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = aftermath::AftermathGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_aftermath_adapter(config)
    }

    pub fn register_aftermath_adapter(
        &self,
        config: aftermath::AftermathGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = aftermath::AftermathGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_alpaca_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = alpaca::AlpacaGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.broker_rest_base_url = rest_base_url.clone();
            config.market_data_rest_base_url = rest_base_url;
        }
        self.register_alpaca_adapter(config)
    }

    pub fn register_alpaca_adapter(
        &self,
        config: alpaca::AlpacaGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            alpaca::AlpacaGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_d8x_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = d8x::D8xGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_d8x_adapter(config)
    }

    pub fn register_d8x_adapter(&self, config: d8x::D8xGatewayConfig) -> Result<(), GatewayError> {
        let adapter = d8x::D8xGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_arkham_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = arkham::ArkhamGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_arkham_adapter(config)
    }

    pub fn register_arkham_adapter(
        &self,
        config: arkham::ArkhamGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            arkham::ArkhamGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_bequant_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bequant::BequantGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bequant_adapter(config)
    }

    pub fn register_bequant_adapter(
        &self,
        config: bequant::BequantGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bequant::BequantGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_binanceus_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = binanceus::BinanceUsGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_binanceus_adapter(config)
    }

    pub fn register_binanceus_adapter(
        &self,
        config: binanceus::BinanceUsGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = binanceus::BinanceUsGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_bit2c_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bit2c::Bit2cGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bit2c_adapter(config)
    }

    pub fn register_bit2c_adapter(
        &self,
        config: bit2c::Bit2cGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bit2c::Bit2cGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_bitbns_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitbns::BitbnsGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitbns_adapter(config)
    }

    pub fn register_bitbns_adapter(
        &self,
        config: bitbns::BitbnsGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitbns::BitbnsGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bittrade_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bittrade::BittradeGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bittrade_adapter(config)
    }

    pub fn register_bittrade_adapter(
        &self,
        config: bittrade::BittradeGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bittrade::BittradeGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_bybiteu_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bybiteu::BybiteuGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bybiteu_adapter(config)
    }

    pub fn register_bybiteu_adapter(
        &self,
        config: bybiteu::BybiteuGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = bybiteu::new_adapter(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bydfi_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bydfi::BydfiGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bydfi_adapter(config)
    }

    pub fn register_bydfi_adapter(
        &self,
        config: bydfi::BydfiGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bydfi::BydfiGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_kcex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = kcex::KcexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_kcex_adapter(config)
    }

    pub fn register_kcex_adapter(
        &self,
        config: kcex::KcexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            kcex::KcexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitopro_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitopro::BitoproGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitopro_adapter(config)
    }

    pub fn register_bitopro_adapter(
        &self,
        config: bitopro::BitoproGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitopro::BitoproGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_bitteam_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitteam::BitteamGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitteam_adapter(config)
    }

    pub fn register_bitteam_adapter(
        &self,
        config: bitteam::BitteamGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitteam::BitteamGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_blockchaincom_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = blockchaincom::BlockchainComGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_blockchaincom_adapter(config)
    }

    pub fn register_blockchaincom_adapter(
        &self,
        config: blockchaincom::BlockchainComGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = blockchaincom::BlockchainComGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_bsx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bsx::BsxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bsx_adapter(config)
    }

    pub fn register_bsx_adapter(&self, config: bsx::BsxGatewayConfig) -> Result<(), GatewayError> {
        let adapter = bsx::BsxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_btcbox_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = btcbox::BtcboxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_btcbox_adapter(config)
    }

    pub fn register_btcbox_adapter(
        &self,
        config: btcbox::BtcboxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            btcbox::BtcboxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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
        config.enabled_public_streams = true;
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

    pub fn register_cex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = cex::CexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_cex_adapter(config)
    }

    pub fn register_cex_adapter(&self, config: cex::CexGatewayConfig) -> Result<(), GatewayError> {
        let adapter = cex::CexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_coinmate_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinmate::CoinmateGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinmate_adapter(config)
    }

    pub fn register_coinmate_adapter(
        &self,
        config: coinmate::CoinmateGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinmate::CoinmateGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_coinmetro_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinmetro::CoinmetroGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinmetro_adapter(config)
    }

    pub fn register_coinmetro_adapter(
        &self,
        config: coinmetro::CoinmetroGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = coinmetro::CoinmetroGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_cryptomus_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = cryptomus::CryptomusGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_cryptomus_adapter(config)
    }

    pub fn register_cryptomus_adapter(
        &self,
        config: cryptomus::CryptomusGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = cryptomus::CryptomusGatewayAdapter::new(config)
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

    pub fn register_derive_chain_perps_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = derive_chain_perps::DeriveChainPerpsGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_derive_chain_perps_adapter(config)
    }

    pub fn register_derive_chain_perps_adapter(
        &self,
        config: derive_chain_perps::DeriveChainPerpsGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = derive_chain_perps::DeriveChainPerpsGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_latoken_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = latoken::LatokenGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_latoken_adapter(config)
    }

    pub fn register_latoken_adapter(
        &self,
        config: latoken::LatokenGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            latoken::LatokenGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_mango_markets_public_adapter(
        &self,
        solana_rpc_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = mango_markets::MangoMarketsGatewayConfig::default();
        if let Some(solana_rpc_url) = solana_rpc_url {
            config.solana_rpc_url = solana_rpc_url;
        }
        self.register_mango_markets_adapter(config)
    }

    pub fn register_mango_markets_adapter(
        &self,
        config: mango_markets::MangoMarketsGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = mango_markets::MangoMarketsGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_modetrade_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = modetrade::ModetradeGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_modetrade_adapter(config)
    }

    pub fn register_modetrade_adapter(
        &self,
        config: modetrade::ModetradeGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = modetrade::ModetradeGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_novadax_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = novadax::NovadaxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_novadax_adapter(config)
    }

    pub fn register_novadax_adapter(
        &self,
        config: novadax::NovadaxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            novadax::NovadaxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_ndax_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = ndax::NdaxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_ndax_adapter(config)
    }

    pub fn register_ndax_adapter(
        &self,
        config: ndax::NdaxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            ndax::NdaxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_equation_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = equation::EquationGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.docs_base_url = rest_base_url;
        }
        self.register_equation_adapter(config)
    }

    pub fn register_equation_adapter(
        &self,
        config: equation::EquationGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            equation::EquationGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_cod3x_public_adapter(
        &self,
        docs_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = cod3x::Cod3xGatewayConfig::default();
        if let Some(docs_base_url) = docs_base_url {
            config.docs_base_url = docs_base_url;
        }
        self.register_cod3x_adapter(config)
    }

    pub fn register_cod3x_adapter(
        &self,
        config: cod3x::Cod3xGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            cod3x::Cod3xGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_exmo_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = exmo::ExmoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_exmo_adapter(config)
    }

    pub fn register_exmo_adapter(
        &self,
        config: exmo::ExmoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            exmo::ExmoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_hollaex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = hollaex::HollaexGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_hollaex_adapter(config)
    }

    pub fn register_hollaex_adapter(
        &self,
        config: hollaex::HollaexGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            hollaex::HollaexGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_hibachi_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = hibachi::HibachiGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.data_rest_base_url = rest_base_url;
        }
        self.register_hibachi_adapter(config)
    }

    pub fn register_hibachi_adapter(
        &self,
        config: hibachi::HibachiGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            hibachi::HibachiGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_hitbtc_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = hitbtc::HitbtcGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_hitbtc_adapter(config)
    }

    pub fn register_hitbtc_adapter(
        &self,
        config: hitbtc::HitbtcGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            hitbtc::HitbtcGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_fmfwio_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = fmfwio::FmfwioGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_fmfwio_adapter(config)
    }

    pub fn register_fmfwio_adapter(
        &self,
        config: fmfwio::FmfwioGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            fmfwio::FmfwioGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_foxbit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = foxbit::FoxbitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_foxbit_adapter(config)
    }

    pub fn register_foxbit_adapter(
        &self,
        config: foxbit::FoxbitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            foxbit::FoxbitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_okxus_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = okxus::OkxusGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_okxus_adapter(config)
    }

    pub fn register_okxus_adapter(
        &self,
        config: okxus::OkxusGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = okxus::new_adapter(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_onetrading_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = onetrading::OneTradingGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_onetrading_adapter(config)
    }

    pub fn register_onetrading_adapter(
        &self,
        config: onetrading::OneTradingGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = onetrading::OneTradingGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_myokx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = myokx::MyOkxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_myokx_adapter(config)
    }

    pub fn register_myokx_adapter(
        &self,
        config: myokx::MyOkxGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = myokx::new_adapter(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_p2b_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = p2b::P2bGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_p2b_adapter(config)
    }

    pub fn register_p2b_adapter(&self, config: p2b::P2bGatewayConfig) -> Result<(), GatewayError> {
        let adapter = p2b::P2bGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_paymium_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = paymium::PaymiumGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_paymium_adapter(config)
    }

    pub fn register_paymium_adapter(
        &self,
        config: paymium::PaymiumGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            paymium::PaymiumGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_tokocrypto_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = tokocrypto::TokocryptoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_tokocrypto_adapter(config)
    }

    pub fn register_tokocrypto_adapter(
        &self,
        config: tokocrypto::TokocryptoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = tokocrypto::TokocryptoGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_wavesexchange_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = wavesexchange::WavesExchangeGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_wavesexchange_adapter(config)
    }

    pub fn register_wavesexchange_adapter(
        &self,
        config: wavesexchange::WavesExchangeGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = wavesexchange::WavesExchangeGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_woofipro_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = woofipro::WoofiproGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_woofipro_adapter(config)
    }

    pub fn register_woofipro_adapter(
        &self,
        config: woofipro::WoofiproGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            woofipro::WoofiproGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
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

    pub fn register_yobit_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = yobit::YobitGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_yobit_adapter(config)
    }

    pub fn register_yobit_adapter(
        &self,
        config: yobit::YobitGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            yobit::YobitGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_zaif_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = zaif::ZaifGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.public_rest_base_url = rest_base_url;
        }
        self.register_zaif_adapter(config)
    }

    pub fn register_zaif_adapter(
        &self,
        config: zaif::ZaifGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            zaif::ZaifGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_zebpay_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = zebpay::ZebpayGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_zebpay_adapter(config)
    }

    pub fn register_zebpay_adapter(
        &self,
        config: zebpay::ZebpayGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            zebpay::ZebpayGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_zeta_markets_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = zeta_markets::ZetaMarketsGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_zeta_markets_adapter(config)
    }

    pub fn register_zeta_markets_adapter(
        &self,
        config: zeta_markets::ZetaMarketsGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter = zeta_markets::ZetaMarketsGatewayAdapter::new(config)
            .map_err(exchange_api_error_to_gateway)?;
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
            GatewayRequestPayload::GetFundingRates(request) => {
                let exchange = request
                    .symbols
                    .first()
                    .map(|symbol| symbol.exchange.clone())
                    .ok_or_else(|| {
                        GatewayError::Rejected(
                            "get_funding_rates requires at least one symbol scope".to_string(),
                        )
                    })?;
                let adapter = self.adapter_for(&exchange)?;
                GatewayResponsePayload::FundingRates(
                    GatewayAdapter::get_funding_rates(adapter.as_ref(), request)
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
            GatewayRequestPayload::GetSymbolAccountConfig(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::SymbolAccountConfig(
                    adapter
                        .get_symbol_account_config(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::SetLeverage(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::SetLeverage(
                    adapter
                        .set_leverage(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::SetPositionMode(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::SetPositionMode(
                    adapter
                        .set_position_mode(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::SetMarginMode(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::SetMarginMode(
                    adapter
                        .set_margin_mode(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::ClosePosition(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::ClosePosition(
                    adapter
                        .close_position(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::SetCountdownCancelAll(request) => {
                let exchange = request
                    .symbol
                    .as_ref()
                    .map(|symbol| symbol.exchange.clone())
                    .unwrap_or_else(|| request.exchange.clone());
                let adapter = self.adapter_for(&exchange)?;
                GatewayResponsePayload::CountdownCancelAll(
                    adapter
                        .set_countdown_cancel_all(request)
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
                if request.subscriptions.is_empty() {
                    return Err(GatewayError::Rejected(
                        "subscribe_private requires at least one subscription".to_string(),
                    ));
                }

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

pub(crate) fn close_position_order_request(
    request: &ClosePositionRequest,
) -> ExchangeApiResult<PlaceOrderRequest> {
    ensure_exchange_api_schema(request.schema_version)?;
    if !matches!(
        request.symbol.market_type,
        MarketType::Perpetual | MarketType::Futures
    ) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "close_position requires a perpetual or futures symbol".to_string(),
        });
    }
    let client_order_id = non_empty_close_field("client_order_id", &request.client_order_id)?;
    let quantity = non_empty_close_field("quantity", &request.quantity)?;
    if request
        .max_slippage_pct
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "composed_close_position.max_slippage_pct",
        });
    }
    let side = match request.position_side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net | PositionSide::None => {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "close_position requires long or short position_side, got {:?}",
                    request.position_side
                ),
            });
        }
    };
    let post_only = request.order_type == OrderType::PostOnly
        || matches!(request.time_in_force, TimeInForce::GTX);
    Ok(PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: request.context.clone(),
        symbol: request.symbol.clone(),
        client_order_id: Some(client_order_id),
        side,
        position_side: Some(request.position_side),
        order_type: request.order_type,
        time_in_force: Some(request.time_in_force),
        quantity,
        price: request.price.clone(),
        quote_quantity: None,
        reduce_only: true,
        post_only,
    })
}

pub(crate) fn close_position_response_from_place_order(
    request: &ClosePositionRequest,
    response: PlaceOrderResponse,
) -> ClosePositionResponse {
    let status = response.order.status;
    ClosePositionResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response.metadata,
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: response.order.exchange_order_id,
        accepted: !matches!(status, OrderStatus::Rejected),
        status: Some(status),
        message: Some("submitted reduce-only close order".to_string()),
    }
}

fn non_empty_close_field(field: &str, value: &str) -> ExchangeApiResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("close_position requires non-empty {field}"),
        });
    }
    Ok(trimmed.to_string())
}
