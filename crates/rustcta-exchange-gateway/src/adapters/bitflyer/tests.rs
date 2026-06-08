use rustcta_exchange_api::{
    ExchangeClient, PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::public_subscription_spec;
use super::{BitflyerGatewayAdapter, BitflyerGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitflyer").expect("exchange")
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

fn symbol_scope(product_code: &str, market_type: MarketType) -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, product_code)
            .expect("symbol"),
    }
}

#[test]
fn bitflyer_parser_fixtures_should_cover_markets_and_board() {
    let markets: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/markets_success.json"
    ))
    .expect("markets");
    let rules = parse_symbol_rules(&exchange_id(), &markets).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.exchange_symbol.symbol == "BTC_JPY"));
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.exchange_symbol.symbol == "FX_BTC_JPY"));

    let board: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitflyer/orderbook_success.json"
    ))
    .expect("board");
    let snapshot = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope("BTC_JPY", MarketType::Spot),
        &board,
    )
    .expect("snapshot");
    assert_eq!(snapshot.best_bid().unwrap().price, 29999.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 30001.0);
}

#[tokio::test]
async fn bitflyer_adapter_capabilities_and_unsupported_boundaries_should_be_explicit() {
    let adapter = BitflyerGatewayAdapter::new(BitflyerGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.market_types.contains(&MarketType::Spot));
    assert!(capabilities.market_types.contains(&MarketType::Margin));

    let err = adapter
        .get_fees(rustcta_exchange_api::FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTC_JPY", MarketType::Spot)],
        })
        .await
        .expect_err("fees unsupported");
    assert!(format!("{err:?}").contains("bitflyer.get_fees"));
}

#[test]
fn bitflyer_public_ws_subscription_should_use_lightning_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope("BTC_JPY", MarketType::Spot),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec =
        public_subscription_spec(&subscription, "wss://ws.lightstream.bitflyer.com/json-rpc")
            .expect("subscription");
    assert_eq!(spec.channel, "lightning_board_BTC_JPY");
    assert_eq!(spec.subscribe_payload["method"], "subscribe");
}

#[test]
fn bitflyer_public_request_paths_are_documented() {
    let _ = json!({
        "markets": "/v1/getmarkets",
        "board": "/v1/getboard?product_code=BTC_JPY"
    });
}
