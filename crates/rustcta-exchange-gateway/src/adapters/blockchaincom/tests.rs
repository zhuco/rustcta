use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, RequestContext,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::json;

use super::parser::{
    blockchaincom_symbol, parse_blockchaincom_balances, parse_blockchaincom_fees,
    parse_blockchaincom_fills, parse_blockchaincom_order_book, parse_blockchaincom_order_list,
    parse_blockchaincom_symbol_rules,
};
use super::private::place_order_request_spec_fixture;
use super::signing::{blockchaincom_token_headers, blockchaincom_ws_auth_payload};
use super::streams::{
    blockchaincom_heartbeat_subscribe_payload, blockchaincom_private_subscribe_payload,
    blockchaincom_public_subscribe_payload, blockchaincom_public_unsubscribe_payload,
    blockchaincom_reconnect_policy_ms, blockchaincom_sequence_gap_requires_restart,
};
use super::{BlockchainComGatewayAdapter, BlockchainComGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("blockchaincom").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC-USD")
            .expect("symbol"),
    }
}

#[test]
fn parser_should_cover_symbols_and_l2_order_book() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/symbols.json"
    ))
    .expect("symbols fixture");
    let rules = parse_blockchaincom_symbol_rules(exchange_id(), &[], &symbols).expect("rules");
    let btc_usd = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-USD")
        .expect("BTC-USD rules");
    assert_eq!(btc_usd.base_asset, "BTC");
    assert_eq!(btc_usd.quote_asset, "USD");
    assert_eq!(btc_usd.price_increment.as_deref(), Some("10"));
    assert_eq!(btc_usd.quantity_increment.as_deref(), Some("0.00000005"));
    assert_eq!(btc_usd.min_quantity.as_deref(), Some("0.5"));
    assert!(btc_usd.supports_market_orders);

    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/orderbook_l2.json"
    ))
    .expect("book fixture");
    let snapshot = parse_blockchaincom_order_book(&symbol(), Some(2), &book).expect("book");
    assert_eq!(snapshot.sequence, Some(110));
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.best_bid().expect("bid").price, 8723.45);
    assert_eq!(snapshot.best_ask().expect("ask").price, 8729.0);
    assert_eq!(blockchaincom_symbol(&symbol()), "BTC-USD");
}

#[test]
fn private_parser_should_cover_account_orders_fees_and_fills() {
    let accounts: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/accounts.json"
    ))
    .expect("accounts fixture");
    let balances = parse_blockchaincom_balances(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &accounts,
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");
    assert_eq!(balances[0].balances[0].available, 0.45);

    let orders: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/orders.json"
    ))
    .expect("orders fixture");
    let parsed_orders =
        parse_blockchaincom_order_list(&exchange_id(), Some(&symbol()), &orders).expect("orders");
    assert_eq!(
        parsed_orders[0].client_order_id.as_deref(),
        Some("rustcta-1")
    );
    assert_eq!(parsed_orders[0].filled_quantity, "0.003");

    let fees: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/fees.json"
    ))
    .expect("fees fixture");
    let parsed_fees = parse_blockchaincom_fees(&exchange_id(), &[symbol()], &fees).expect("fees");
    assert_eq!(parsed_fees[0].maker_rate, "0.0014");

    let fills: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/fills.json"
    ))
    .expect("fills fixture");
    let parsed_fills = parse_blockchaincom_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(&symbol()),
        &fills,
    )
    .expect("fills");
    assert_eq!(parsed_fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(parsed_fills[0].fee_asset.as_deref(), Some("USD"));
}

#[test]
fn request_spec_and_token_auth_should_match_official_boundary() {
    let spec: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/request_specs/place_order_limit.json"
    ))
    .expect("request spec");
    let actual = ActualHttpRequest::new("POST", "/orders")
        .with_headers([
            ("Accept".to_string(), "application/json".to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("X-API-Token".to_string(), "fixture-api-token".to_string()),
        ])
        .with_body(Some(json!({
            "clOrdId": "rustcta-fixture-0001",
            "symbol": "BTC-USD",
            "side": "BUY",
            "ordType": "LIMIT",
            "timeInForce": "GTC",
            "orderQty": "0.01",
            "price": "65000"
        })));
    spec.assert_matches(&actual).expect("request spec match");

    let fixture = place_order_request_spec_fixture();
    assert_eq!(fixture["auth"], "api_key_header");
    let headers = blockchaincom_token_headers("fixture-api-token").expect("headers");
    assert_eq!(
        headers.get("X-API-Token").map(String::as_str),
        Some("fixture-api-token")
    );

    let auth_payload = blockchaincom_ws_auth_payload("fixture-api-secret").expect("auth");
    let auth_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/signing_vectors/token_auth.json"
    ))
    .expect("auth fixture");
    assert_eq!(auth_fixture["algorithm"], "none_api_token_header");
    assert_eq!(auth_payload["channel"], "auth");
    assert_eq!(auth_payload["token"], "fixture-api-secret");
}

#[tokio::test]
async fn private_write_should_remain_offline_request_spec_only() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("rustcta-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn websocket_helpers_should_cover_public_private_and_resync_boundaries() {
    let public = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = blockchaincom_public_subscribe_payload(&public);
    assert_eq!(payload["action"], "subscribe");
    assert_eq!(payload["channel"], "l2");
    assert_eq!(payload["symbol"], "BTC-USD");
    assert_eq!(
        blockchaincom_public_unsubscribe_payload(&public)["action"],
        "unsubscribe"
    );
    assert_eq!(
        blockchaincom_heartbeat_subscribe_payload()["channel"],
        "heartbeat"
    );

    let private = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        account_id: AccountId::new("account").expect("account"),
        market_type: Some(MarketType::Spot),
        kind: PrivateStreamKind::Orders,
    };
    let private_payload =
        blockchaincom_private_subscribe_payload(&private, "fixture-api-secret").expect("private");
    assert_eq!(private_payload["auth"]["channel"], "auth");
    assert_eq!(private_payload["subscribe"]["channel"], "trading");
    assert!(blockchaincom_sequence_gap_requires_restart(10, 12));
    assert!(!blockchaincom_sequence_gap_requires_restart(10, 11));
    assert_eq!(blockchaincom_reconnect_policy_ms(), (5_000, 15_000, 60_000));
}

#[test]
fn capabilities_should_expose_public_spot_and_keep_private_runtime_closed() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_private_streams);
}
