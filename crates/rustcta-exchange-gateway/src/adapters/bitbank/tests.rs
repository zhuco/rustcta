use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::public_subscription_spec;
use super::{BitbankGatewayAdapter, BitbankGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitbank").expect("exchange")
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
fn bitbank_parser_fixtures_should_cover_pairs_and_depth() {
    let pairs: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/pairs_success.json"
    ))
    .expect("pairs");
    let rules = parse_symbol_rules(&exchange_id(), &pairs).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc_jpy");
    assert_eq!(rules[0].price_increment.as_deref(), Some("1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/depth_success.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.sequence, Some(123456));
    assert_eq!(snapshot.best_bid().unwrap().price, 2999000.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 3001000.0);
}

#[tokio::test]
async fn bitbank_adapter_capabilities_should_be_spot_only_with_explicit_write_boundary() {
    let adapter = BitbankGatewayAdapter::new(BitbankGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
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
    assert!(format!("{err:?}").contains("bitbank.positions"));
}

#[test]
fn bitbank_public_ws_subscription_should_use_socket_io_rooms() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec = public_subscription_spec(&subscription, "wss://stream.bitbank.cc/socket.io/")
        .expect("subscription");
    assert_eq!(spec.room, "depth_diff_btc_jpy");
    assert_eq!(spec.subscribe_payload[0], "join-room");
}
