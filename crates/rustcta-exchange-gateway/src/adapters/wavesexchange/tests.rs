use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rule};
use super::streams::{
    wavesexchange_public_subscribe_payload, wavesexchange_public_unsubscribe_payload,
    wavesexchange_reconnect_policy_ms,
};
use super::{private, signing, WavesExchangeGatewayAdapter, WavesExchangeGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

const PRICE_ASSET: &str = "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p";

fn exchange_id() -> ExchangeId {
    ExchangeId::new("wavesexchange").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("WAVES", "USDN").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id(),
            MarketType::Spot,
            format!("WAVES-{PRICE_ASSET}"),
        )
        .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "market_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/market_info.json"
        ),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/wavesexchange/orderbook.json")
        }
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/unsupported_boundary.json"
        ),
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/wavesexchange/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/wavesexchange/error_response.json")
        }
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/signing_vectors/waves_transaction_unsupported.json"
        ),
        "ws_subscribe_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/ws/updates_subscribe_orderbook.json"
        ),
        "ws_unsubscribe_orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/ws/updates_unsubscribe_orderbook.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "market_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/request_specs/markets.json"
        ),
        "orderbook" => include_str!(
            "../../../../../tests/fixtures/exchanges/wavesexchange/request_specs/orderbook.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_spot_market_info() {
    let rule = parse_symbol_rule(&exchange_id(), symbol(), &fixture("market_info")).expect("rule");

    assert_eq!(rule.symbol.market_type, MarketType::Spot);
    assert_eq!(
        rule.symbol.exchange_symbol.symbol,
        format!("WAVES-{PRICE_ASSET}")
    );
    assert_eq!(rule.base_asset, "WAVES");
    assert_eq!(rule.quote_asset, "USDN");
    assert_eq!(rule.price_increment.as_deref(), Some("0.00000001"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.00000001"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.001"));
    assert_eq!(rule.price_precision, Some(8));
    assert_eq!(rule.quantity_precision, Some(8));
}

#[test]
fn parser_should_parse_matcher_orderbook_snapshot() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 2.515);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 125.75);
    assert_eq!(snapshot.best_ask().expect("ask").price, 2.525);
    assert_eq!(snapshot.sequence, Some(321));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_expose_public_spot_scan_only_surface() {
    let adapter =
        WavesExchangeGatewayAdapter::new(WavesExchangeGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
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
            && endpoint.path.as_deref() == Some("/matcher/orderbook/{amountAsset}/{priceAsset}")));
}

#[tokio::test]
async fn private_writes_should_return_waves_signature_boundary() {
    let adapter =
        WavesExchangeGatewayAdapter::new(WavesExchangeGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "1".to_string(),
        price: Some("2.50".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter
        .place_order(order.clone())
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
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
        .expect_err("unsupported");
    assert!(matches!(
        batch_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_PLACE_UNSUPPORTED
        }
    ));
}

#[test]
fn request_specs_should_match_public_rest_builders() {
    let market_info = ActualHttpRequest::new(
        "GET",
        format!("/matcher/orderbook/WAVES/{PRICE_ASSET}/info"),
    );
    request_spec("market_info")
        .assert_matches(&market_info)
        .expect("market info request spec");

    let orderbook =
        ActualHttpRequest::new("GET", format!("/matcher/orderbook/WAVES/{PRICE_ASSET}"))
            .with_query([("depth".to_string(), "100".to_string())]);
    request_spec("orderbook")
        .assert_matches(&orderbook)
        .expect("orderbook request spec");
}

#[test]
fn websocket_helpers_should_build_offline_payload_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        wavesexchange_public_subscribe_payload(&subscription),
        fixture("ws_subscribe_orderbook")
    );
    assert_eq!(
        wavesexchange_public_unsubscribe_payload(&subscription),
        fixture("ws_unsubscribe_orderbook")
    );
    assert_eq!(
        wavesexchange_reconnect_policy_ms(),
        (30_000, 45_000, 60_000)
    );
}

#[test]
fn boundary_fixtures_should_keep_waves_trading_disabled() {
    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["market_type"], "spot");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);

    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    assert!(empty.as_array().is_some_and(Vec::is_empty));
    assert_eq!(error["error"], "Invalid request");
    assert!(missing.get("symbol").is_some());

    let signing_vector = fixture("signing_boundary");
    assert_eq!(signing_vector["supported"], false);
    assert_eq!(
        signing_vector["expected_error"],
        private::PLACE_ORDER_UNSUPPORTED
    );
    assert_eq!(
        signing::wavesexchange_private_write_boundary(),
        signing::WAVESEXCHANGE_PRIVATE_WRITE_BOUNDARY
    );
}
