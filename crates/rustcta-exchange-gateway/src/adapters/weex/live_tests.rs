use rustcta_exchange_api::{
    BalancesRequest, ExchangeClient, FeesRequest, OpenOrdersRequest, OrderBookRequest,
    SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{WeexGatewayAdapter, WeexGatewayConfig};

#[tokio::test]
#[ignore]
async fn weex_live_readonly_should_cover_public_and_authenticated_baseline() {
    let Some(api_key) = non_empty_env("WEEX_API_KEY") else {
        eprintln!("WEEX_API_KEY missing; skipping WEEX authenticated live readonly test");
        return;
    };
    let Some(api_secret) = non_empty_env("WEEX_API_SECRET") else {
        eprintln!("WEEX_API_SECRET missing; skipping WEEX authenticated live readonly test");
        return;
    };
    let Some(passphrase) = non_empty_env("WEEX_API_PASSPHRASE") else {
        eprintln!("WEEX_API_PASSPHRASE missing; skipping WEEX authenticated live readonly test");
        return;
    };

    let adapter = WeexGatewayAdapter::new(WeexGatewayConfig {
        api_key: Some(api_key),
        api_secret: Some(api_secret),
        passphrase: Some(passphrase),
        enabled_private_rest: true,
        ..WeexGatewayConfig::default()
    })
    .expect("adapter");

    let spot_symbol = spot_symbol_scope();
    let perp_symbol = perp_symbol_scope();

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("weex-live-rules"),
            symbols: vec![spot_symbol.clone(), perp_symbol.clone()],
        })
        .await
        .expect("live symbol rules");
    assert!(rules.rules.iter().any(|rule| rule.symbol == spot_symbol));
    assert!(rules.rules.iter().any(|rule| rule.symbol == perp_symbol));

    let spot_book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("weex-live-spot-book"),
            symbol: spot_symbol.clone(),
            depth: Some(5),
        })
        .await
        .expect("live spot book");
    assert!(!spot_book.order_book.bids.is_empty());
    assert!(!spot_book.order_book.asks.is_empty());

    let perp_book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("weex-live-perp-book"),
            symbol: perp_symbol.clone(),
            depth: Some(5),
        })
        .await
        .expect("live perp book");
    assert!(!perp_book.order_book.bids.is_empty());
    assert!(!perp_book.order_book.asks.is_empty());

    adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("weex-live-fees"),
            symbols: vec![spot_symbol.clone(), perp_symbol],
        })
        .await
        .expect("live fees");

    adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("weex-live-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("live spot balances");

    adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("weex-live-open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol),
            page: None,
        })
        .await
        .expect("live spot open orders");
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
