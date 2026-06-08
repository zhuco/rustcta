use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;

use super::parser::{
    bitso_book, bitso_canonical_pair, parse_order_ack_id, parse_order_book_shape,
    parse_symbol_rules,
};
use super::private::{
    bitso_cancel_order_path, bitso_place_order_body, create_order_request_spec_fixture,
};
use super::private_parser::{parse_balance_assets, parse_fill_ids, parse_open_order_ids};
use super::signing::{bitso_authorization_header, bitso_hmac_signature, bitso_signature_payload};
use super::streams::{bitso_public_subscribe_payload, bitso_reconnect_policy_ms};
use super::{BitsoGatewayAdapter, BitsoGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitso").expect("exchange")
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

fn symbol(symbol: &str) -> SymbolScope {
    let (base, quote) = bitso_canonical_pair(symbol).expect("canonical pair");
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, symbol)
            .expect("symbol"),
    }
}

#[test]
fn symbol_normalization_should_cover_latin_america_fiat_quotes() {
    assert_eq!(bitso_book("BTC/MXN"), "btc_mxn");
    assert_eq!(bitso_book("btc-brl"), "btc_brl");
    assert_eq!(
        bitso_canonical_pair("ETHARS").expect("ars"),
        ("ETH".to_string(), "ARS".to_string())
    );
    assert_eq!(
        bitso_canonical_pair("BTC/BRL").expect("brl"),
        ("BTC".to_string(), "BRL".to_string())
    );
}

#[test]
fn request_spec_and_signing_should_stay_secret_free() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/api/v3/orders");
    assert_eq!(spec["body"]["book"], "btc_mxn");
    assert_eq!(
        spec["headers"]["Authorization"],
        "Bitso <key>:<nonce>:<signature>"
    );

    let payload = bitso_signature_payload(
        1_700_000_000_000,
        "post",
        "/api/v3/orders",
        r#"{"book":"btc_mxn","side":"buy"}"#,
    );
    let signature = bitso_hmac_signature("test-secret", &payload).expect("signature");
    assert_eq!(signature.len(), 64);
    assert_eq!(
        bitso_authorization_header("key", 1_700_000_000_000, &signature),
        format!("Bitso key:1700000000000:{signature}")
    );

    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/signing_vectors/rest_place_order.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["payload"], payload);
}

#[test]
fn place_order_body_should_use_major_minor_and_fiat_book_names() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC/MXN"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("650000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = bitso_place_order_body(&request).expect("body");
    assert_eq!(body["book"], "btc_mxn");
    assert_eq!(body["major"], "0.01");
    assert_eq!(body["price"], "650000");
    assert_eq!(body["time_in_force"], "goodtillcancelled");

    let mut market = request;
    market.order_type = OrderType::Market;
    market.price = None;
    market.quote_quantity = Some("1000".to_string());
    let body = bitso_place_order_body(&market).expect("market");
    assert_eq!(body["minor"], "1000");
    assert!(body.get("price").is_none());
}

#[test]
fn fixtures_should_parse_orderbook_order_ack_and_boundary() {
    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/orderbook_btc_mxn.json"
    ))
    .expect("orderbook");
    let (bids, asks, sequence) = parse_order_book_shape(&order_book).expect("shape");
    assert_eq!((bids, asks), (1, 1));
    assert_eq!(sequence.as_deref(), Some("27214"));

    let order_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/order_ack.json"
    ))
    .expect("ack");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("oid"),
        "qlbga6b600n3xta7"
    );

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["funding_operations"], "unsupported");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");

    let books: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/available_books_success.json"
    ))
    .expect("books");
    let rules = parse_symbol_rules(&exchange_id(), &[], &books).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].quote_asset, "MXN");
}

#[test]
fn private_read_fixtures_should_cover_balances_open_orders_and_fills() {
    let balances: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/balances.json"
    ))
    .expect("balances");
    assert_eq!(
        parse_balance_assets(&balances).expect("assets"),
        vec!["BTC", "MXN"]
    );

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/open_orders.json"
    ))
    .expect("open orders");
    assert_eq!(
        parse_open_order_ids(&open_orders).expect("orders"),
        vec!["offline-order-1"]
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/fills.json"
    ))
    .expect("fills");
    assert_eq!(parse_fill_ids(&fills).expect("fills"), vec!["1000001"]);
    assert_eq!(
        bitso_cancel_order_path("offline-order-1"),
        "/orders/offline-order-1"
    );
}

#[test]
fn websocket_helpers_should_map_bitso_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC/MXN"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = bitso_public_subscribe_payload(&subscription);
    assert_eq!(payload["action"], "subscribe");
    assert_eq!(payload["book"], "btc_mxn");
    assert_eq!(payload["type"], "orders");
    assert_eq!(bitso_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = BitsoGatewayAdapter::new(BitsoGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC/MXN"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("650000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitso.place_order_offline_request_spec_only"
        }
    ));
}
