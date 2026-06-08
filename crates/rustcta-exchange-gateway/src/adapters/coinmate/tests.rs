use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PrivateStreamKind, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;

use super::parser::{
    coinmate_canonical_pair, coinmate_pair, parse_order_ack_id, parse_order_book_shape,
    parse_symbol_rules,
};
use super::private::{
    coinmate_cancel_all_form, coinmate_cancel_order_form, coinmate_history_form,
    coinmate_limit_order_form, coinmate_limit_order_path, redacted_auth_fields,
};
use super::private_parser::{parse_balance_assets, parse_fill_ids, parse_open_order_ids};
use super::signing::{
    coinmate_hmac_signature, coinmate_signature_payload, coinmate_signed_auth_fields,
};
use super::streams::{
    coinmate_private_subscribe_payload, coinmate_public_subscribe_payload,
    coinmate_reconnect_policy_ms, coinmate_unsubscribe_payload, parse_ws_event_type,
    parse_ws_order_book_shape,
};
use super::{CoinmateGatewayAdapter, CoinmateGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("coinmate").expect("exchange")
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
    let (base, quote) = coinmate_canonical_pair(symbol).expect("canonical pair");
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, symbol)
            .expect("symbol"),
    }
}

#[test]
fn symbol_normalization_should_cover_eur_and_czk_markets() {
    assert_eq!(coinmate_pair("btc/eur"), "BTC_EUR");
    assert_eq!(coinmate_pair("ETH-CZK"), "ETH_CZK");
    assert_eq!(
        coinmate_canonical_pair("BTCEUR").expect("eur"),
        ("BTC".to_string(), "EUR".to_string())
    );
    assert_eq!(
        coinmate_canonical_pair("BTC_CZK").expect("czk"),
        ("BTC".to_string(), "CZK".to_string())
    );
}

#[test]
fn request_spec_and_signing_should_stay_secret_free() {
    let payload = coinmate_signature_payload("1700000000000", "fixture-client", "fixture-public");
    assert_eq!(
        payload,
        "1700000000000fixture-clientfixture-public".to_string()
    );
    let signature = coinmate_hmac_signature("fixture-private", &payload).expect("signature");
    assert_eq!(
        signature,
        "07EA1665FD22A32D201A27B76AD8DC90A32D42E93DB39A0926893F3B07843798"
    );
    let auth = coinmate_signed_auth_fields(
        "fixture-client",
        "fixture-public",
        "fixture-private",
        "1700000000000",
    )
    .expect("auth fields");
    assert_eq!(
        auth.get("clientId").map(String::as_str),
        Some("fixture-client")
    );
    assert_eq!(
        auth.get("signature").map(String::as_str),
        Some(signature.as_str())
    );

    let vector: crate::signing_spec::SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/signing_vectors/rest_form_auth.json"
    ))
    .expect("signing vector");
    vector.verify().expect("standard signing vector verifies");

    let place: crate::request_spec::RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/request_specs/place_order_limit_buy.json"
    ))
    .expect("place spec");
    assert_eq!(place.method, "POST");
    assert_eq!(place.path, "/buyLimit");
    assert_eq!(
        place.body.as_ref().expect("body")["currencyPair"],
        "BTC_EUR"
    );
    assert_eq!(place.body.as_ref().expect("body")["postOnly"], true);

    let cancel: crate::request_spec::RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/request_specs/cancel_order.json"
    ))
    .expect("cancel spec");
    assert_eq!(cancel.path, "/cancelOrder");
    assert_eq!(
        cancel.body.as_ref().expect("body")["orderId"],
        "offline-order-1"
    );

    let redacted = redacted_auth_fields();
    assert_eq!(redacted["signature"], "<redacted:signature>");
}

#[test]
fn place_cancel_and_history_forms_should_match_coinmate_fields() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC_EUR"),
        client_order_id: Some("123456".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::PostOnly,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("50000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    };
    let body = coinmate_limit_order_form(&request).expect("body");
    assert_eq!(coinmate_limit_order_path(OrderSide::Buy), "/buyLimit");
    assert_eq!(body["currencyPair"], "BTC_EUR");
    assert_eq!(body["amount"], "0.01");
    assert_eq!(body["price"], "50000");
    assert_eq!(body["postOnly"], true);
    assert_eq!(body["clientOrderId"], "123456");

    let mut ioc = request;
    ioc.order_type = OrderType::Limit;
    ioc.time_in_force = Some(TimeInForce::IOC);
    ioc.post_only = false;
    let body = coinmate_limit_order_form(&ioc).expect("ioc body");
    assert_eq!(body["immediateOrCancel"], true);

    assert_eq!(
        coinmate_cancel_order_form("offline-order-1")["orderId"],
        "offline-order-1"
    );
    assert_eq!(
        coinmate_cancel_all_form(Some("btc/eur"))["currencyPair"],
        "BTC_EUR"
    );
    assert_eq!(coinmate_history_form("btc_eur", Some(100))["limit"], 100);
}

#[test]
fn fixtures_should_parse_public_and_private_shapes() {
    let trading_pairs: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/trading_pairs.json"
    ))
    .expect("trading pairs");
    let rules = parse_symbol_rules(&exchange_id(), &[], &trading_pairs).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].quote_asset, "EUR");
    assert_eq!(rules[0].price_precision, Some(2));

    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/orderbook_btc_eur.json"
    ))
    .expect("orderbook");
    assert_eq!(parse_order_book_shape(&order_book).expect("shape"), (1, 1));

    let order_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/order_ack.json"
    ))
    .expect("ack");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("order id"),
        "offline-order-1"
    );

    let balances: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/balances.json"
    ))
    .expect("balances");
    assert_eq!(
        parse_balance_assets(&balances).expect("assets"),
        vec!["BTC", "EUR"]
    );

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/open_orders.json"
    ))
    .expect("open orders");
    assert_eq!(
        parse_open_order_ids(&open_orders).expect("orders"),
        vec!["offline-order-1"]
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/fills.json"
    ))
    .expect("fills");
    assert_eq!(parse_fill_ids(&fills).expect("fills"), vec!["fill-1"]);

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["funding_operations"], "unsupported");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
}

#[test]
fn websocket_helpers_should_map_v2_channels_and_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC_EUR"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = coinmate_public_subscribe_payload(&subscription);
    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["data"]["channel"], "order_book-BTC_EUR");
    assert_eq!(
        coinmate_unsubscribe_payload("order_book-BTC_EUR")["event"],
        "unsubscribe"
    );
    let private = coinmate_private_subscribe_payload(
        PrivateStreamKind::Orders,
        "fixture-client",
        "fixture-public",
        "fixture-private",
        "1700000000000",
        Some("BTC_EUR"),
    )
    .expect("private subscribe");
    assert_eq!(
        private["data"]["channel"],
        "private-open_orders-fixture-client-BTC_EUR"
    );
    assert_eq!(
        private["data"]["signature"],
        "07EA1665FD22A32D201A27B76AD8DC90A32D42E93DB39A0926893F3B07843798"
    );
    assert_eq!(coinmate_reconnect_policy_ms(), (30_000, 45_000, 60_000));

    let ws_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/ws/public_order_book.json"
    ))
    .expect("ws book");
    assert_eq!(parse_ws_event_type(&ws_book).as_deref(), Some("data"));
    assert_eq!(parse_ws_order_book_shape(&ws_book).expect("shape"), (1, 1));

    let ws_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/ws/subscribe_success.json"
    ))
    .expect("ws ack");
    assert_eq!(
        parse_ws_event_type(&ws_ack).as_deref(),
        Some("subscribe_success")
    );
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = CoinmateGatewayAdapter::new(CoinmateGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_positions);

    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC_EUR"),
        client_order_id: Some("123456".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("50000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "coinmate.place_order_offline_request_spec_only"
        }
    ));
}
