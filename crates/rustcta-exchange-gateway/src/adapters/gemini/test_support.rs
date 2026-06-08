use chrono::Utc;
use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

pub fn exchange_id() -> ExchangeId {
    ExchangeId::new("gemini").expect("valid exchange id")
}

pub fn btc_usd_symbol() -> SymbolScope {
    let exchange = exchange_id();
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("valid symbol")),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Spot, "btcusd")
            .expect("valid exchange symbol"),
    }
}

pub fn context() -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(TenantId::new("tenant-a").expect("valid tenant"));
    context.account_id = Some(AccountId::new("acct-a").expect("valid account"));
    context.request_id = Some("req-a".to_string());
    context.schema_version = EXCHANGE_API_SCHEMA_VERSION;
    context
}
