use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, OrderListConditionalLeg, OrderListLegType, OrderListRequest,
    PlaceOrderRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Map};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::gemini_order_payload;
use super::signing::{private_payload, sign_private_request};
use super::{GeminiGatewayAdapter, GeminiGatewayConfig};

fn gemini_exchange() -> ExchangeId {
    ExchangeId::new("gemini").expect("exchange")
}

fn gemini_symbol() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: gemini_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: rustcta_types::ExchangeSymbol::new(
            gemini_exchange(),
            MarketType::Spot,
            "btcusd",
        )
        .expect("symbol"),
    }
}

#[test]
fn gemini_signing_should_build_payload_and_signature() {
    let payload = private_payload("/v1/order/new", "1710000000000000", Map::new());
    assert!(payload.contains("\"request\":\"/v1/order/new\""));
    let signed = sign_private_request(
        "test-secret",
        "/v1/order/new",
        "1710000000000000",
        Map::new(),
    );
    assert!(!signed.payload_base64.is_empty());
    assert_eq!(signed.signature_hex.len(), 96);
}

#[test]
fn gemini_parser_should_normalize_symbol_details_and_book() {
    let exchange = gemini_exchange();
    let rules = parse_symbol_rules(
        &exchange,
        &json!([
            {
                "symbol": "btcusd",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "tick_size": "0.00000001",
                "quote_increment": "0.01",
                "min_order_size": "0.00001"
            }
        ]),
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    let snapshot = parse_orderbook_snapshot(
        &exchange,
        rules[0].symbol.clone(),
        &json!({
            "bids": [{"price": "30000.00", "amount": "0.5"}],
            "asks": [{"price": "30010.00", "amount": "0.4"}],
            "timestampms": 1710000000000i64
        }),
    )
    .expect("book");
    assert_eq!(snapshot.bids[0].quantity, 0.5);
}

#[test]
fn gemini_order_payload_should_map_limit_order_options() {
    let payload = gemini_order_payload(&PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: gemini_symbol(),
        client_order_id: Some("client-1".to_string()),
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: Some(rustcta_types::TimeInForce::IOC),
        quantity: "0.01".to_string(),
        price: Some("30000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    })
    .expect("payload");
    assert_eq!(payload["symbol"], "btcusd");
    assert!(payload["options"].as_array().expect("options").len() >= 2);
}

#[test]
fn gemini_unsupported_boundary_should_reject_market_surfaces() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/gemini/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["funding_enabled"], false);
    assert!(boundary["unsupported_operations"]
        .as_array()
        .expect("operations")
        .iter()
        .any(|operation| operation == "travel_rule"));

    let adapter = GeminiGatewayAdapter::new(GeminiGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_fees);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_quote_market_order);
    assert!(!capabilities
        .capabilities_v2
        .batch_place_orders
        .support
        .is_supported());
    assert!(!capabilities
        .capabilities_v2
        .batch_cancel_orders
        .support
        .is_supported());
    assert!(!capabilities
        .supports_order_types
        .contains(&rustcta_types::OrderType::Market));
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["runtime_error"],
        "gemini.amend_order_unsupported"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["place_order_list"]["runtime_error"],
        "gemini.order_list_unsupported"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["runtime_error"],
        "gemini.batch_place_orders_composed_not_exposed"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_cancel_orders"]["runtime_error"],
        "gemini.batch_cancel_orders_composed_not_exposed"
    );

    let error = gemini_order_payload(&PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: gemini_symbol(),
        client_order_id: None,
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Market,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: None,
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    })
    .expect_err("market unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "gemini.market_order_unsupported"
        }
    ));
}

#[tokio::test]
async fn gemini_advanced_order_surfaces_should_remain_explicitly_unsupported() {
    let adapter = GeminiGatewayAdapter::new(GeminiGatewayConfig::default()).expect("adapter");
    let context = rustcta_exchange_api::RequestContext::new(Utc::now());
    let symbol = gemini_symbol();

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            symbol: symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.2".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "gemini.amend_order_unsupported"
        }
    ));

    let list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            symbol: symbol.clone(),
            list_client_order_id: Some("oco-1".to_string()),
            side: rustcta_types::OrderSide::Sell,
            quantity: "0.1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("120".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("90".to_string()),
                stop_price: Some("95".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        list_error,
        ExchangeApiError::Unsupported {
            operation: "gemini.order_list_unsupported"
        }
    ));

    let place_order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context.clone(),
        symbol: symbol.clone(),
        client_order_id: Some("batch-place-1".to_string()),
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: None,
        quantity: "0.1".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            exchange: gemini_exchange(),
            orders: vec![place_order],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "gemini.batch_place_orders_composed_not_exposed"
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            exchange: gemini_exchange(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context,
                symbol,
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            }],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "gemini.batch_cancel_orders_composed_not_exposed"
        }
    ));
}
