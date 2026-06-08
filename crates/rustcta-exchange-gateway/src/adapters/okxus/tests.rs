use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, OrderBookRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, RequestContext,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    okxus_heartbeat_policy, okxus_private_login_payload, okxus_private_subscribe_payload,
    okxus_public_subscribe_payload, okxus_public_unsubscribe_payload,
    parse_okxus_orderbook_ws_message,
};
use super::{new_adapter, OkxusGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn okxus_config_should_map_to_okx_us_profile() {
    let config = OkxusGatewayConfig::default();
    assert_eq!(config.rest_base_url, "https://us.okx.com");
    assert_eq!(config.public_ws_url, "wss://wsus.okx.com:8443/ws/v5/public");
    assert_eq!(
        config.private_ws_url,
        "wss://wsus.okx.com:8443/ws/v5/private"
    );

    let okx_profile = config.into_okx_config();
    assert_eq!(okx_profile.exchange_id, "okxus");
    assert_eq!(okx_profile.rest_base_url, "https://us.okx.com");
    assert!(!okx_profile.enabled_private_rest);
    assert!(okx_profile.api_key.is_none());
    assert!(okx_profile.api_secret.is_none());
    assert!(okx_profile.passphrase.is_none());
    assert_eq!(
        okx_profile.unsupported_market_type_operation,
        "okxus.non_spot_market_type"
    );
}

#[test]
fn okxus_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["okxus"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn okxus_capabilities_should_keep_private_trading_disabled() {
    let adapter = new_adapter(OkxusGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.exchange, exchange_id());
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
}

#[tokio::test]
async fn okxus_private_rest_should_remain_unsupported_without_us_scope_audit() {
    let adapter = new_adapter(OkxusGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USD".to_string()],
        })
        .await
        .expect_err("private rest must stay disabled");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "okxus.get_balances"
        }
    ));
}

#[tokio::test]
async fn okxus_non_spot_market_type_should_be_unsupported() {
    let adapter = new_adapter(OkxusGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(MarketType::Perpetual, "BTC-USDT-SWAP"),
            depth: Some(5),
        })
        .await
        .expect_err("non spot must be unsupported");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "okxus.non_spot_market_type"
        }
    ));
}

#[test]
fn okxus_public_rest_fixtures_should_parse_with_us_exchange_id() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id(), &instruments).expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange, exchange_id());
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC-USD");
    assert_eq!(rules[0].quote_asset, "USD");

    let book_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope(MarketType::Spot, "BTC-USD"),
        &book_fixture,
    )
    .expect("order book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.exchange_symbol.unwrap().symbol, "BTC-USD");
    assert_eq!(book.bids[0].quantity, 0.75);
    assert!(book.exchange_timestamp.is_some());
}

#[test]
fn okxus_boundary_and_ws_fixtures_should_be_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["exchange"], "okxus");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert_eq!(boundary["rest_base_url"], "https://us.okx.com");

    let request_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/request_specs/private_rest_unsupported.json"
    ))
    .expect("request spec fixture");
    assert_eq!(request_spec["support"], "unsupported");
    assert!(request_spec["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let signing_vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/signing_vectors/rest_hmac_shape.json"
    ))
    .expect("signing fixture");
    assert_eq!(signing_vector["credential_values"], serde_json::json!({}));

    let public_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/ws_public_orderbook.json"
    ))
    .expect("public ws fixture");
    assert_eq!(public_ws["payload"]["args"][0]["channel"], "books5");
    assert_eq!(public_ws["payload"]["args"][0]["instId"], "BTC-USD");

    let private_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/ws_private_order_unsupported.json"
    ))
    .expect("private ws fixture");
    assert_eq!(private_ws["support"], "unsupported_pending_us_scope_audit");
}

#[test]
fn okxus_ws_payloads_and_parser_should_use_us_hosts_and_okx_shapes() {
    let public_subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(MarketType::Spot, "BTC-USD"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let subscribe = okxus_public_subscribe_payload(&public_subscription).expect("subscribe");
    assert_eq!(subscribe["op"], "subscribe");
    assert_eq!(subscribe["args"][0]["channel"], "books5");
    assert_eq!(subscribe["args"][0]["instId"], "BTC-USD");

    let unsubscribe = okxus_public_unsubscribe_payload(&public_subscription).expect("unsubscribe");
    assert_eq!(unsubscribe["op"], "unsubscribe");

    let login = okxus_private_login_payload(
        "<redacted-key>",
        "<redacted-passphrase>",
        "1710000000",
        "<redacted-signature>",
    );
    assert_eq!(login["op"], "login");
    assert_eq!(login["args"][0]["apiKey"], "<redacted-key>");

    let private_subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let private_subscribe =
        okxus_private_subscribe_payload(&private_subscription).expect("private subscribe");
    assert_eq!(private_subscribe["args"][0]["channel"], "orders");
    assert_eq!(private_subscribe["args"][0]["instType"], "SPOT");
    assert_eq!(okxus_heartbeat_policy()["client_pong"], "pong");

    let ws_orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/ws_public_orderbook_snapshot.json"
    ))
    .expect("ws orderbook fixture");
    let book = parse_okxus_orderbook_ws_message(
        &exchange_id(),
        symbol_scope(MarketType::Spot, "BTC-USD"),
        &ws_orderbook,
    )
    .expect("ws book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.bids[0].quantity, 0.75);
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("okxus").unwrap()
}

fn symbol_scope(market_type: MarketType, symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, symbol).unwrap(),
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").unwrap()),
        account_id: Some(AccountId::new("account").unwrap()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}
