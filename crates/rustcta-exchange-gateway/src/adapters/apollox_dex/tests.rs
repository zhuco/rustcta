use std::collections::BTreeMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolRulesRequest, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{apollox_parse_public_depth_event, apollox_public_unsubscribe_payload};
use super::streams::{apollox_public_subscribe_payload, apollox_ws_policy_ms};
use super::{private, signing, ApolloxDexGatewayAdapter, ApolloxDexGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("apollox_dex").expect("exchange")
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
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "exchange_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/exchange_info.json"
        ),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/apollox_dex/orderbook.json")
        }
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/unsupported_boundary.json"
        ),
        "signing_vector" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/signing_vectors/place_order_hmac.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/ws/public_depth_subscribe.json"
        ),
        "ws_unsubscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/ws/public_depth_unsubscribe.json"
        ),
        "ws_depth_event" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/ws/public_depth_event.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "exchange_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/exchange_info.json"
        ),
        "depth" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/depth.json"
            )
        }
        "place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/place_order.json"
        ),
        "open_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/open_orders.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_apollox_perpetual_exchange_info() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("exchange_info")).expect("rules");

    assert_eq!(rules.len(), 2);
    let btc = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTCUSDT")
        .expect("btc rule");
    assert_eq!(btc.symbol.market_type, MarketType::Perpetual);
    assert_eq!(btc.base_asset, "BTC");
    assert_eq!(btc.quote_asset, "USDT");
    assert_eq!(btc.price_increment.as_deref(), Some("0.10"));
    assert_eq!(btc.quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(btc.min_quantity.as_deref(), Some("0.001"));
    assert_eq!(btc.min_notional.as_deref(), Some("5"));
    assert_eq!(btc.price_precision, Some(1));
    assert_eq!(btc.quantity_precision, Some(3));
    assert!(btc.supports_post_only);
    assert!(btc.supports_reduce_only);
}

#[test]
fn parser_should_parse_apollox_orderbook_snapshot() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert_eq!(snapshot.sequence, Some(987654321));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_enable_public_rest_but_keep_private_runtime_closed() {
    let mut config = ApolloxDexGatewayConfig::default();
    config.api_key = Some("test-key".to_string());
    config.api_secret = Some("test-secret".to_string());
    config.enabled_private_rest = true;
    config.enabled_public_streams = true;
    let adapter = ApolloxDexGatewayAdapter::new(config).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_symbol_rules"
            && endpoint.path.as_deref() == Some("/fapi/v1/exchangeInfo")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["v1_orderbook_perp"], true);
    assert_eq!(boundary["v2_onchain_trading_enabled"], false);
    assert_eq!(boundary["private_write_runtime_enabled"], false);
}

#[tokio::test]
async fn private_order_methods_should_return_spec_only_boundary() {
    let adapter =
        ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("cli-apollox-place".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000.5".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter
        .place_order(request)
        .await
        .expect_err("place order unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::PLACE_ORDER_UNSUPPORTED
        }
    ));
}

#[test]
fn request_specs_and_signing_vector_should_match_offline_hmac_contract() {
    request_spec("exchange_info")
        .assert_matches(&ActualHttpRequest::new("GET", "/fapi/v1/exchangeInfo"))
        .expect("exchangeInfo spec");
    request_spec("depth")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/depth").with_query([
                ("symbol".to_string(), "BTCUSDT".to_string()),
                ("limit".to_string(), "100".to_string()),
            ]),
        )
        .expect("depth spec");

    let params = [
        ("symbol".to_string(), "BTCUSDT".to_string()),
        ("side".to_string(), "BUY".to_string()),
        ("type".to_string(), "LIMIT".to_string()),
        ("timeInForce".to_string(), "GTC".to_string()),
        ("quantity".to_string(), "0.01".to_string()),
        ("price".to_string(), "65000.5".to_string()),
        ("reduceOnly".to_string(), "false".to_string()),
        (
            "newClientOrderId".to_string(),
            "cli-apollox-place".to_string(),
        ),
        ("recvWindow".to_string(), "5000".to_string()),
        ("timestamp".to_string(), "1700000000000".to_string()),
    ];
    let (query, signature) =
        signing::apollox_signed_query("test-secret", params.clone()).expect("signature");
    let signing_vector = fixture("signing_vector");
    assert_eq!(
        query,
        signing_vector["canonical_query"].as_str().expect("query")
    );
    assert_eq!(
        signature,
        signing_vector["signature"].as_str().expect("signature")
    );

    let mut actual_query = params.into_iter().collect::<BTreeMap<_, _>>();
    actual_query.insert("signature".to_string(), signature);
    request_spec("place_order")
        .assert_matches(
            &ActualHttpRequest::new("POST", "/fapi/v1/order")
                .with_query(actual_query)
                .with_headers([
                    ("X-MBX-APIKEY".to_string(), "test-key".to_string()),
                    (
                        "Content-Type".to_string(),
                        "application/x-www-form-urlencoded".to_string(),
                    ),
                ]),
        )
        .expect("place order spec");

    request_spec("open_orders")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/openOrders")
                .with_query([
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("timestamp".to_string(), "1700000000000".to_string()),
                    ("signature".to_string(), "fixture-signature".to_string()),
                ])
                .with_headers([("X-MBX-APIKEY".to_string(), "test-key".to_string())]),
        )
        .expect("open orders spec");
}

#[test]
fn websocket_helpers_should_build_binance_style_subscriptions() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        apollox_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        apollox_public_unsubscribe_payload(&subscription),
        fixture("ws_unsubscribe")
    );
    let snapshot =
        apollox_parse_public_depth_event(&exchange_id(), symbol(), &fixture("ws_depth_event"))
            .expect("ws depth");
    assert_eq!(snapshot.sequence, Some(987654322));
    assert_eq!(snapshot.best_bid().expect("bid").price, 65010.0);
    assert_eq!(apollox_ws_policy_ms(), (180_000, 600_000, 86_400_000));
}

#[test]
fn public_requests_validate_exchange_and_market_type() {
    let adapter =
        ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig::default()).expect("adapter");
    let request = SymbolRulesRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("symbols"),
        symbols: vec![SymbolScope {
            market_type: MarketType::Spot,
            ..symbol()
        }],
    };
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let error = runtime
        .block_on(adapter.get_symbol_rules(request))
        .expect_err("spot unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "apollox_dex.unsupported_market_type"
        }
    ));

    assert_eq!(
        json!({
            "exchange": "apollox_dex",
            "scope": "v1_orderbook_perp"
        })["exchange"],
        "apollox_dex"
    );
}
