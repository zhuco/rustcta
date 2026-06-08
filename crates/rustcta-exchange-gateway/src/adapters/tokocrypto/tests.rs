use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};

use super::parser::{market_data_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{private_subscribe_payload, public_subscription_spec};
use super::{TokocryptoGatewayAdapter, TokocryptoGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("tokocrypto").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC_USDT")
            .expect("symbol"),
    }
}

#[test]
fn tokocrypto_parser_fixtures_should_cover_symbols_and_depth() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/symbols_success.json"
    ))
    .expect("symbols");
    let rules = parse_symbol_rules(&exchange_id(), &symbols).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC_USDT");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01000000"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00001000"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("10.00000000"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/depth_success.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.sequence, Some(1027024));
    assert_eq!(snapshot.best_bid().unwrap().price, 4.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 4.000002);
    assert_eq!(market_data_symbol("BTC_USDT").unwrap(), "BTCUSDT");
}

#[tokio::test]
async fn tokocrypto_adapter_capabilities_should_be_spot_public_with_offline_private_boundary() {
    let adapter =
        TokocryptoGatewayAdapter::new(TokocryptoGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);

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
    assert!(format!("{err:?}").contains("tokocrypto.positions"));
}

#[test]
fn tokocrypto_public_ws_subscription_should_use_payload_fixture() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec = public_subscription_spec(&subscription, "wss://stream-cloud.tokocrypto.site/stream")
        .expect("subscription");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/ws/public_depth_subscribe.json"
    ))
    .expect("ws fixture");
    assert_eq!(fixture["stream"].as_str(), Some(spec.stream.as_str()));
    assert_eq!(spec.subscribe_payload, fixture["subscribe_payload"]);
    assert_eq!(spec.unsubscribe_payload, fixture["unsubscribe_payload"]);
    assert_eq!(
        private_subscribe_payload("fixture-listen-token")["params"][0],
        "fixture-listen-token"
    );
}

#[test]
fn tokocrypto_named_registration_should_accept_aliases() {
    let gateway = AdapterBackedGateway::new("tokocrypto-test");
    gateway
        .register_named_adapter("toko_crypto")
        .expect("register alias");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}
