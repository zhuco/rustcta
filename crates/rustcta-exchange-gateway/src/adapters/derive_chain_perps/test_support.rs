#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

pub fn derive_chain_perps_exchange_id() -> ExchangeId {
    ExchangeId::new("derive_chain_perps").expect("exchange")
}

pub fn derive_chain_perps_context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

pub fn derive_chain_perps_symbol(market_type: MarketType) -> SymbolScope {
    SymbolScope {
        exchange: derive_chain_perps_exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("ETH", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(
            derive_chain_perps_exchange_id(),
            market_type,
            "ETH-USD",
        )
        .expect("symbol"),
    }
}
