use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::parser::{luno_symbol, parse_luno_order_book, parse_luno_symbol_rules};
use super::private::place_order_request_spec_fixture;
use super::signing::luno_basic_auth_header;
use super::streams::{luno_stream_url, luno_ws_auth_payload};
use super::{LunoGatewayAdapter, LunoGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("luno").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "ZAR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "XBTZAR")
            .expect("symbol"),
    }
}

#[test]
fn parser_should_cover_multi_region_fiat_markets_and_orderbook() {
    let tickers: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/tickers.json"
    ))
    .expect("fixture");
    let rules = parse_luno_symbol_rules(exchange_id(), &[], &tickers).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "ZAR"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "MYR"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "NGN"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "IDR"));

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/orderbook_top.json"
    ))
    .expect("fixture");
    let snapshot = parse_luno_order_book(&symbol(), &orderbook).expect("orderbook");
    assert_eq!(snapshot.asks[0].price, 1_201_000.0);
    assert_eq!(luno_symbol(&symbol()), "XBTZAR");
}

#[test]
fn request_spec_and_signing_should_match_basic_auth_boundary() {
    let spec = place_order_request_spec_fixture();
    assert_eq!(spec["path"], "/api/1/postorder");
    assert_eq!(spec["auth"], "http_basic_api_key");
    assert_eq!(spec["form"]["pair"], "XBTZAR");

    let header = luno_basic_auth_header("key-id", "key-secret").expect("header");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/signing_vectors/basic_auth.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["authorization"], header);
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter = LunoGatewayAdapter::new(LunoGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("1200000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "luno.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn websocket_helpers_should_cover_pair_scoped_streams() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert_eq!(
        luno_stream_url(
            "wss://ws.luno.com/api/1/stream",
            &subscription.symbol.exchange_symbol.symbol
        ),
        "wss://ws.luno.com/api/1/stream/XBTZAR"
    );
    let auth = luno_ws_auth_payload("key-id", "key-secret");
    assert_eq!(auth["api_key_id"], "key-id");
}
