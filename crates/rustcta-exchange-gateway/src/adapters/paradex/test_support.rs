#![cfg(test)]

use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

pub fn exchange_id() -> ExchangeId {
    ExchangeId::new("paradex").expect("paradex exchange")
}

pub fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("paper-subaccount").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

pub fn symbol(market_type: MarketType, venue_symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, venue_symbol)
            .expect("exchange symbol"),
    }
}
