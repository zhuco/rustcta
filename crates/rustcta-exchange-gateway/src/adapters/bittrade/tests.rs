use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};

use super::parser::{parse_orderbook_response, parse_symbol_rules};
use super::streams::{public_pong_payload, public_subscription_spec};
use super::{BittradeGatewayAdapter, BittradeGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bittrade").expect("exchange")
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
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btcjpy")
            .expect("symbol"),
    }
}

#[test]
fn bittrade_parser_fixtures_should_cover_symbols_and_depth() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/symbols_success.json"
    ))
    .expect("symbols");
    let rules = parse_symbol_rules(&exchange_id(), &symbols).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btcjpy");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "JPY");
    assert_eq!(rules[0].price_increment.as_deref(), Some("1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("2"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/depth_success.json"
    ))
    .expect("depth");
    let snapshot =
        parse_orderbook_response(&exchange_id(), symbol_scope(), Some(1), &depth).expect("book");
    assert_eq!(snapshot.sequence, Some(100883718161));
    assert_eq!(snapshot.best_bid().unwrap().price, 4_860_380.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 4_860_850.0);
    assert_eq!(snapshot.bids.len(), 1);
}

#[tokio::test]
async fn bittrade_adapter_capabilities_should_be_spot_public_with_offline_private_boundary() {
    let adapter = BittradeGatewayAdapter::new(BittradeGatewayConfig::default()).expect("adapter");
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
    assert!(format!("{err:?}").contains("bittrade.positions"));
}

#[test]
fn bittrade_public_ws_subscription_should_use_official_topics() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec =
        public_subscription_spec(&subscription, "wss://api-cloud.bittrade.co.jp/ws").expect("sub");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/ws/public_depth_subscribe.json"
    ))
    .expect("ws fixture");
    assert_eq!(spec.topic, "market.btcjpy.depth.step0");
    assert_eq!(spec.subscribe_payload["sub"], "market.btcjpy.depth.step0");
    assert_eq!(fixture["topic"].as_str(), Some(spec.topic.as_str()));
    assert_eq!(spec.subscribe_payload, fixture["subscribe_payload"]);
    assert_eq!(spec.unsubscribe_payload, fixture["unsubscribe_payload"]);
    assert_eq!(public_pong_payload(18212558000)["pong"], 18212558000_i64);
}

#[test]
fn bittrade_private_ws_fixture_should_keep_sanitized_order_shape() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/ws/private_order_push.json"
    ))
    .expect("private ws fixture");
    assert_eq!(fixture["ch"], "orders#btcjpy");
    assert_eq!(fixture["data"]["eventType"], "creation");
    assert_eq!(fixture["data"]["clientOrderId"], "fixture-client-order-id");
}

#[test]
fn bittrade_named_registration_should_accept_aliases() {
    let gateway = AdapterBackedGateway::new("bittrade-test");
    gateway
        .register_named_adapter("huobi_japan")
        .expect("register alias");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}
