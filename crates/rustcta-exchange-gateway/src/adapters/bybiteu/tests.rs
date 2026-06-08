use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PublicStreamKind, PublicStreamSubscription, RequestContext,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::{new_adapter, BybiteuGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn bybiteu_config_should_map_to_bybit_eu_profile() {
    let config = BybiteuGatewayConfig::default();
    assert_eq!(config.rest_base_url, "https://api.bybit.eu");
    assert_eq!(
        config.public_ws_url,
        "wss://stream.bybit.eu/v5/public/linear"
    );

    let bybit_profile = config.into_bybit_config();
    assert_eq!(bybit_profile.exchange_id, "bybiteu");
    assert_eq!(bybit_profile.rest_base_url, "https://api.bybit.eu");
    assert_eq!(
        bybit_profile.public_ws_url,
        "wss://stream.bybit.eu/v5/public/linear"
    );
    assert!(!bybit_profile.enabled_private_rest);
    assert_eq!(
        bybit_profile.unsupported_market_type_operation,
        "bybiteu.unsupported_market_type"
    );
}

#[test]
fn bybiteu_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["bybiteu"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn bybiteu_capabilities_should_keep_private_trading_disabled() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.exchange, exchange_id());
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual, MarketType::Futures]
    );
}

#[tokio::test]
async fn bybiteu_public_ws_subscription_should_use_eu_host_and_exchange_id() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let ack = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope("BTCUSDT"),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect("public ws");

    assert_eq!(
        ack,
        "bybiteu:wss://stream.bybit.eu/v5/public/linear:orderbook.50.BTCUSDT"
    );
}

#[tokio::test]
async fn bybiteu_private_rest_should_remain_unsupported_without_broker_audit() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_balances(rustcta_exchange_api::BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect_err("private rest must stay disabled");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.get_balances"
        }
    ));
}

#[test]
fn bybiteu_public_rest_fixtures_should_parse_with_eu_exchange_id() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id(), MarketType::Perpetual, &instruments)
        .expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange, exchange_id());
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");

    let book_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol_scope("BTCUSDT"), &book_fixture)
        .expect("order book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.sequence, Some(100));
    assert_eq!(book.bids[0].quantity, 1.2);
}

#[test]
fn bybiteu_boundary_and_ws_fixtures_should_be_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["exchange"], "bybiteu");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert_eq!(boundary["rest_base_url"], "https://api.bybit.eu");

    let request_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/request_specs/private_rest_unsupported.json"
    ))
    .expect("request spec fixture");
    assert_eq!(request_spec["support"], "unsupported");
    assert!(request_spec["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let signing_vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/signing_vectors/rest_hmac_shape.json"
    ))
    .expect("signing fixture");
    assert_eq!(signing_vector["credential_values"], serde_json::json!({}));

    let public_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/ws_public_orderbook.json"
    ))
    .expect("public ws fixture");
    assert_eq!(public_ws["topic"], "orderbook.50.BTCUSDT");
    assert_eq!(public_ws["type"], "snapshot");

    let funding_history: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/funding_history.json"
    ))
    .expect("funding history fixture");
    assert_eq!(funding_history["result"]["category"], "linear");

    let open_interest: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/open_interest.json"
    ))
    .expect("open interest fixture");
    assert_eq!(open_interest["result"]["symbol"], "BTCUSDT");

    let private_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/ws_private_order_unsupported.json"
    ))
    .expect("private ws fixture");
    assert_eq!(
        private_ws["support"],
        "unsupported_without_broker_scope_audit"
    );
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bybiteu").unwrap()
}

fn symbol_scope(symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, symbol).unwrap(),
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
