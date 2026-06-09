use std::collections::BTreeMap;

use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest, OrderBookRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderSide, OrderType,
};
use serde_json::json;

use super::parser::{coin_param_from_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::private::private_post_request_spec;
use super::signing::{canonical_form, md5_hex, sign_canonical_payload};
use super::streams::btcbox_rest_reconciliation_fallback;
use super::test_support::spawn_rest_server;
use super::transport::classify_error;
use super::{BtcboxGatewayAdapter, BtcboxGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("btcbox").expect("exchange")
}

fn context(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol_scope() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btc_jpy")
            .expect("symbol"),
    }
}

#[test]
fn btcbox_parser_fixtures_should_cover_tickers_and_depth() {
    let tickers: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/tickers_success.json"
    ))
    .expect("tickers");
    let rules = parse_symbol_rules(&exchange_id(), &tickers).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.exchange_symbol.symbol == "btc_jpy"));
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.exchange_symbol.symbol == "trx_jpy"));
    assert!(!rules[0].supports_market_orders);

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/depth_btc_success.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.best_bid().unwrap().price, 5_000_000.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 5_001_000.0);
}

#[test]
fn btcbox_order_book_should_reject_ticker_only_depth_symbols() {
    assert_eq!(coin_param_from_symbol("eth_jpy").unwrap(), "eth");
    let err = coin_param_from_symbol("trx_jpy").expect_err("trx depth unverified");
    assert!(matches!(
        err,
        ExchangeApiError::Unsupported {
            operation: "btcbox.order_book_unverified_for_ticker_only_symbol"
        }
    ));
}

#[tokio::test]
async fn btcbox_capabilities_should_be_spot_readback_only_with_explicit_private_boundary() {
    let adapter = BtcboxGatewayAdapter::new(BtcboxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);

    let err = adapter
        .get_balances(rustcta_exchange_api::BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("balances unsupported");
    assert!(format!("{err:?}").contains("btcbox.get_balances"));

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["scan_only"], false);
    assert_eq!(boundary["readback_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(
        boundary["private_rest_runtime_enabled"],
        json!("readback_only")
    );
    assert_eq!(boundary["private_rest_readback_enabled"], true);
    assert_eq!(
        boundary["advanced_orders"]["unsupported_operations"],
        json!([
            "amend_order",
            "place_order_list",
            "batch_place_orders",
            "batch_cancel_orders",
            "cancel_all_orders"
        ])
    );
}

#[tokio::test]
async fn btcbox_advanced_order_surfaces_should_remain_explicitly_unsupported() {
    let adapter = BtcboxGatewayAdapter::new(BtcboxGatewayConfig::default()).expect("adapter");
    let symbol = symbol_scope();
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-item"),
        symbol: symbol.clone(),
        client_order_id: Some("client-batch-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("5000000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("12345".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.amend_order.unsupported"
        }
    ));

    let list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol.clone(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("5100000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("4900000".to_string()),
                stop_price: Some("4950000".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        list_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.order_list.unsupported"
        }
    ));

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.batch_place_orders.unsupported"
        }
    ));

    let cancel = CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel-item"),
        symbol: symbol.clone(),
        client_order_id: None,
        exchange_order_id: Some("12345".to_string()),
    };
    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![cancel],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.batch_cancel_orders.unsupported"
        }
    ));

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol),
        })
        .await
        .expect_err("cancel-all unsupported");
    assert!(matches!(
        cancel_all_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.cancel_all_orders.unsupported"
        }
    ));
}

#[test]
fn btcbox_named_registration_should_accept_primary_aliases() {
    AdapterBackedGateway::with_named_adapters("btcbox-test", ["btcbox"]).expect("btcbox");
    AdapterBackedGateway::with_named_adapters("btcbox-alias-test", ["btc_box"])
        .expect("btc_box alias");
}

#[tokio::test]
async fn btcbox_public_rest_should_route_tickers_and_depth() {
    let tickers: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/tickers_success.json"
    ))
    .expect("tickers");
    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/depth_btc_success.json"
    ))
    .expect("depth");
    let (base_url, seen) = spawn_rest_server(vec![tickers, depth]).await;
    let adapter = BtcboxGatewayAdapter::new(BtcboxGatewayConfig {
        rest_base_url: base_url,
        ..BtcboxGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");
    assert_eq!(rules.rules.len(), 1);
    assert_eq!(rules.rules[0].symbol.exchange_symbol.symbol, "btc_jpy");

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.bids.len(), 2);
    assert_eq!(book.order_book.asks.len(), 2);

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/tickers");
    assert_eq!(seen[1].method, "GET");
    assert_eq!(seen[1].path, "/depth");
    assert_eq!(seen[1].query.get("coin").map(String::as_str), Some("btc"));
}

#[tokio::test]
async fn btcbox_private_readback_should_route_trade_view_and_trade_list_when_enabled() {
    let trade_view: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/trade_view_success.json"
    ))
    .expect("trade view");
    let trade_list: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/trade_list_success.json"
    ))
    .expect("trade list");
    let (base_url, seen) = spawn_rest_server(vec![trade_view, trade_list]).await;
    let adapter = BtcboxGatewayAdapter::new(BtcboxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("btcbox-public-key".to_string()),
        api_secret: Some("btcbox-private-key".to_string()),
        enabled_private_rest: true,
        ..BtcboxGatewayConfig::default()
    })
    .expect("adapter");

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect("query order");
    let order = query.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.filled_quantity, "0.01");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 2);
    assert_eq!(open.orders[1].side, OrderSide::Sell);

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/trade_view");
    assert!(seen[0].body.contains("coin=btc"));
    assert!(seen[0].body.contains("id=12345"));
    assert!(seen[0].body.contains("key=btcbox-public-key"));
    assert!(seen[0].body.contains("signature="));
    assert_eq!(seen[1].method, "POST");
    assert_eq!(seen[1].path, "/trade_list");
    assert!(seen[1].body.contains("since=0"));
}

#[tokio::test]
async fn btcbox_private_readback_should_stay_guarded_without_env_flag() {
    let adapter = BtcboxGatewayAdapter::new(BtcboxGatewayConfig {
        api_key: Some("btcbox-public-key".to_string()),
        api_secret: Some("btcbox-private-key".to_string()),
        enabled_private_rest: false,
        ..BtcboxGatewayConfig::default()
    })
    .expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect_err("guarded private rest");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.query_order"
        }
    ));
}

#[tokio::test]
async fn btcbox_streams_should_stay_unsupported_with_rest_boundary() {
    let adapter = BtcboxGatewayAdapter::new(BtcboxGatewayConfig::default()).expect("adapter");
    let public_error = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect_err("public ws unsupported");
    assert!(matches!(
        public_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.public_streams.unsupported_no_official_ws"
        }
    ));

    let private_error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private ws unsupported");
    assert!(matches!(
        private_error,
        ExchangeApiError::Unsupported {
            operation: "btcbox.private_streams.unsupported_no_official_ws"
        }
    ));
    assert!(btcbox_rest_reconciliation_fallback().contains("REST-only"));
}

#[test]
fn btcbox_signing_vector_should_match_offline_request_spec() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/signing_vectors/private_form_hmac_sha256.json"
    ))
    .expect("vector");
    assert_eq!(
        md5_hex(vector["private_key"].as_str().unwrap()),
        vector["md5_private_key"].as_str().unwrap()
    );
    assert_eq!(
        sign_canonical_payload(
            vector["private_key"].as_str().unwrap(),
            vector["canonical"].as_str().unwrap(),
        )
        .unwrap(),
        vector["expected_signature"].as_str().unwrap()
    );
}

#[test]
fn btcbox_private_request_specs_should_be_offline_only() {
    let cases = vec![
        (
            "get_balances",
            "balance",
            include_str!(
                "../../../../../tests/fixtures/exchanges/btcbox/request_specs/balance.json"
            ),
            vec![("coin", "btc"), ("nonce", "1700000000012")],
        ),
        (
            "get_open_orders",
            "trade_list",
            include_str!(
                "../../../../../tests/fixtures/exchanges/btcbox/request_specs/trade_list.json"
            ),
            vec![("coin", "btc"), ("nonce", "1700000000012"), ("since", "0")],
        ),
        (
            "query_order",
            "trade_view",
            include_str!(
                "../../../../../tests/fixtures/exchanges/btcbox/request_specs/trade_view.json"
            ),
            vec![("coin", "btc"), ("id", "12345"), ("nonce", "1700000000012")],
        ),
        (
            "cancel_order",
            "trade_cancel",
            include_str!(
                "../../../../../tests/fixtures/exchanges/btcbox/request_specs/trade_cancel.json"
            ),
            vec![("coin", "btc"), ("id", "12345"), ("nonce", "1700000000012")],
        ),
        (
            "place_order",
            "trade_add",
            include_str!(
                "../../../../../tests/fixtures/exchanges/btcbox/request_specs/trade_add_limit.json"
            ),
            vec![
                ("amount", "0.01"),
                ("coin", "btc"),
                ("nonce", "1700000000012"),
                ("price", "5000000"),
                ("type", "buy"),
            ],
        ),
    ];

    for (operation, endpoint, fixture, fields) in cases {
        let fixture: serde_json::Value = serde_json::from_str(fixture).expect("request spec");
        let params = fields
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>();
        let spec = private_post_request_spec(
            operation,
            endpoint,
            "btcbox-public-key",
            "btcbox-private-key",
            params,
        )
        .expect("spec");
        assert_eq!(spec["network"], json!("offline_request_spec_only"));
        assert_eq!(spec["operation"], fixture["operation"]);
        assert_eq!(spec["path"], fixture["path"]);
        assert_eq!(spec["canonical"], fixture["canonical"]);
        assert_eq!(spec["body"], fixture["body"]);
        assert_eq!(spec["expected_signature"], fixture["expected_signature"]);
    }

    let mut canonical_params = BTreeMap::new();
    canonical_params.insert("amount".to_string(), "0.01".to_string());
    canonical_params.insert("coin".to_string(), "btc".to_string());
    canonical_params.insert("key".to_string(), "btcbox-public-key".to_string());
    canonical_params.insert("nonce".to_string(), "1700000000012".to_string());
    canonical_params.insert("price".to_string(), "5000000".to_string());
    canonical_params.insert("type".to_string(), "buy".to_string());
    assert_eq!(
        canonical_form(&canonical_params),
        "amount=0.01&coin=btc&key=btcbox-public-key&nonce=1700000000012&price=5000000&type=buy"
    );
}

#[test]
fn btcbox_service_suspended_error_should_classify_exchange_unavailable() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcbox/error_service_suspended.json"
    ))
    .expect("service suspended");
    assert_eq!(
        classify_error(
            fixture["code"].as_i64().unwrap().to_string().as_str(),
            fixture["message"].as_str().unwrap(),
            200,
        ),
        ExchangeErrorClass::ExchangeUnavailable
    );
}
