use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::public_subscription_spec;
use super::{ZaifGatewayAdapter, ZaifGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("zaif").expect("exchange")
}

fn context(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol_scope() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btc_jpy")
            .expect("symbol"),
    }
}

#[test]
fn zaif_parser_fixtures_should_cover_pairs_and_depth() {
    let pairs: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/currency_pairs_success.json"
    ))
    .expect("pairs");
    let rules = parse_symbol_rules(&exchange_id(), &pairs).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc_jpy");
    assert_eq!(rules[0].price_increment.as_deref(), Some("5"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].price_precision, Some(0));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/depth_btc_jpy.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.best_bid().unwrap().price, 6500000.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 6500500.0);
}

#[tokio::test]
async fn zaif_adapter_capabilities_should_be_spot_only_with_write_boundary() {
    let adapter = ZaifGatewayAdapter::new(ZaifGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_private_streams);

    let err = adapter
        .get_positions(rustcta_exchange_api::PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbols: Vec::new(),
        })
        .await
        .expect_err("positions unsupported");
    assert!(format!("{err:?}").contains("zaif.positions"));
}

#[test]
fn zaif_public_ws_subscription_should_use_pair_query() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec =
        public_subscription_spec(&subscription, "wss://ws.zaif.jp/stream").expect("subscription");
    assert_eq!(spec.url, "wss://ws.zaif.jp/stream?currency_pair=btc_jpy");
    assert_eq!(spec.subscribe_payload["currency_pair"], "btc_jpy");
}
