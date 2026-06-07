use rustcta_exchange_api::{
    BalancesRequest, ExchangeClient, PrivateStreamKind, PrivateStreamSubscription,
    SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};

use super::test_support::{context, exchange_id, symbol_scope};
use super::{WhiteBitGatewayAdapter, WhiteBitGatewayConfig};

#[tokio::test]
#[ignore = "requires WHITEBIT_LIVE_DRY_RUN=1; optional WHITEBIT_API_KEY/WHITEBIT_API_SECRET for private readonly probes"]
async fn whitebit_live_dry_run_should_probe_readonly_rest_and_private_ws_auth() {
    if std::env::var("WHITEBIT_LIVE_DRY_RUN").ok().as_deref() != Some("1") {
        eprintln!("set WHITEBIT_LIVE_DRY_RUN=1 to run the WhiteBIT readonly live dry-run probe");
        return;
    }

    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig::default()).expect("adapter");
    let context = context("whitebit-live-dry-run");
    let exchange = exchange_id();
    let symbol = symbol_scope();

    let symbol_rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            symbols: vec![symbol.clone()],
        })
        .await
        .expect("public symbol rules");
    assert!(
        symbol_rules.rules.iter().any(|rule| rule.symbol == symbol),
        "expected public symbol rules for BTC_USDT"
    );

    let order_book = adapter
        .get_order_book(rustcta_exchange_api::OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            symbol: symbol.clone(),
            depth: Some(5),
        })
        .await
        .expect("public order book");
    assert!(
        !order_book.order_book.bids.is_empty() || !order_book.order_book.asks.is_empty(),
        "expected non-empty BTC_USDT book"
    );

    if !adapter.capabilities().supports_private_rest {
        eprintln!(
            "WHITEBIT_API_KEY/WHITEBIT_API_SECRET not set or private REST disabled; skipping private readonly probes"
        );
        return;
    }

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            exchange: exchange.clone(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("private spot balances");
    assert_eq!(balances.metadata.exchange, exchange);

    let session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context,
            exchange,
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("whitebit-live-dry-run").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private websocket auth session");
    assert!(session.initial_requests().len() >= 2);
}
