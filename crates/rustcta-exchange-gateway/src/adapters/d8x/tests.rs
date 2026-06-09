use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PositionsRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{parse_orderbook_snapshot, parse_position_source_boundary, parse_symbol_rules};
use super::streams::{
    d8x_private_auth_payload, d8x_public_subscribe_payload, d8x_reconnect_policy_ms,
};
use super::transport::D8xRest;
use super::{private, signing, D8xGatewayAdapter, D8xGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("d8x").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC-USD-USDC")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "contracts" => include_str!("../../../../../tests/fixtures/exchanges/d8x/contracts.json"),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/orderbook.json")
        }
        "unsupported_boundary" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/unsupported_boundary.json")
        }
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/error_response.json")
        }
        "missing_required_fields" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/missing_required_fields.json")
        }
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/d8x/signing_vectors/evm_contract_boundary.json"
        ),
        "positions_source" => include_str!(
            "../../../../../tests/fixtures/exchanges/d8x/request_specs/get_positions_account_source.json"
        ),
        "ws_subscribe" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/d8x/ws/public_orderbook_subscribe.json"
            )
        }
        "ws_private_auth" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/ws/private_auth_payload.json")
        }
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "contracts" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/request_specs/contracts.json")
        }
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/d8x/request_specs/orderbook.json")
        }
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_validate_position_source_boundary_fixture() {
    let audit = parse_position_source_boundary(&exchange_id(), &fixture("positions_source"))
        .expect("audit");

    assert_eq!(audit.boundary, "wallet_contract_indexer_source_only");
    assert!(audit
        .required_sources
        .iter()
        .any(|source| source == "wallet_address"));
    assert!(audit
        .position_fields
        .iter()
        .any(|field| field == "funding_accrual"));
    assert!(audit
        .reconciliation_required
        .iter()
        .any(|gap| gap == "block number and reorg handling"));
}

#[test]
fn parser_should_parse_d8x_contracts_as_perpetual_symbol_rules() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("contracts")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-USD-USDC")
        .expect("btc rule");
    assert_eq!(rule.symbol.market_type, MarketType::Perpetual);
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USD");
    assert_eq!(rule.price_increment.as_deref(), Some("0.1"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.001"));
    assert_eq!(rule.min_notional.as_deref(), Some("10"));
    assert_eq!(rule.price_precision, Some(1));
    assert_eq!(rule.quantity_precision, Some(3));
    assert!(!rule.supports_market_orders);
}

#[test]
fn parser_should_parse_public_orderbook_snapshot_fixture() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_expose_public_market_data_but_no_private_runtime() {
    let adapter = D8xGatewayAdapter::new(D8xGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/coingecko/orderbook/{ticker_id}")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
}

#[tokio::test]
async fn get_positions_should_return_wallet_indexer_source_boundary() {
    let adapter = D8xGatewayAdapter::new(D8xGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![symbol().exchange_symbol],
        })
        .await
        .expect_err("positions boundary");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::POSITIONS_UNSUPPORTED
        }
    ));
}

#[tokio::test]
async fn private_writes_should_return_contract_boundary() {
    let adapter = D8xGatewayAdapter::new(D8xGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("offline-fixture".to_string()),
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

    let place_error = adapter
        .place_order(order.clone())
        .await
        .expect_err("unsupported place");
    assert!(matches!(
        place_error,
        ExchangeApiError::Unsupported {
            operation: private::PLACE_ORDER_UNSUPPORTED
        }
    ));

    let batch_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect_err("unsupported batch");
    assert!(matches!(
        batch_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_PLACE_UNSUPPORTED
        }
    ));
}

#[test]
fn request_specs_should_match_public_market_data_boundaries() {
    let contracts = ActualHttpRequest::new("GET", "/coingecko/contracts")
        .with_query([("chain_id".to_string(), "1101".to_string())]);
    request_spec("contracts")
        .assert_matches(&contracts)
        .expect("contracts spec");

    let orderbook = ActualHttpRequest::new("GET", D8xRest::orderbook_spec_path("BTC-USD-USDC"))
        .with_query([("chain_id".to_string(), "1101".to_string())]);
    request_spec("orderbook")
        .assert_matches(&orderbook)
        .expect("orderbook spec");

    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["canonical_payload"],
        signing::d8x_contract_call_boundary(
            1101,
            "0xaB7794EcD2c8e9Decc6B577864b40eBf9204720f",
            "trade"
        )
    );
    assert_eq!(
        signing::d8x_signing_boundary(),
        signing::D8X_SIGNING_BOUNDARY
    );
}

#[test]
fn websocket_helpers_should_build_fixture_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        d8x_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        d8x_private_auth_payload(
            "0x000000000000000000000000000000000000d8x0",
            "fixture-signature"
        ),
        fixture("ws_private_auth")
    );
    assert_eq!(d8x_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    let unsupported_spec = serde_json::from_str::<Value>(include_str!(
        "../../../../../tests/fixtures/exchanges/d8x/request_specs/place_order_unsupported.json"
    ))
    .expect("unsupported request spec");

    assert_eq!(empty["contracts"].as_array().expect("contracts").len(), 0);
    assert!(error.get("error").is_some());
    assert!(missing.get("ticker_id").is_none());
    assert_eq!(unsupported_spec["trade_enabled"], false);
    assert_eq!(
        unsupported_spec["expected_error"],
        json!(private::PLACE_ORDER_UNSUPPORTED)
    );
}
