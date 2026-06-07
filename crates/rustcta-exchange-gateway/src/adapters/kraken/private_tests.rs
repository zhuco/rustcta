use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchExecutionMode, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, FeesRequest, PageCursor, PageRequest, PlaceOrderRequest, PositionsRequest,
    QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::json;

use super::test_support::{
    context, perp_symbol_scope, private_config, spawn_rest_server, spot_symbol_scope,
};
use super::KrakenGatewayAdapter;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "balance.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/balance.json")
        }
        "balance_empty.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/balance_empty.json")
        }
        "error.json" => include_str!("../../../../../tests/fixtures/exchanges/kraken/error.json"),
        "fills.json" => include_str!("../../../../../tests/fixtures/exchanges/kraken/fills.json"),
        "fills_empty.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/fills_empty.json")
        }
        "fills_missing_symbol.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/kraken/fills_missing_symbol.json")
        }
        "request_specs/spot_place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kraken/request_specs/spot_place_order_limit.json"
        ),
        "request_specs/futures_batch_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kraken/request_specs/futures_batch_order.json"
        ),
        _ => panic!("unknown kraken fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[test]
fn kraken_capabilities_v2_should_declare_batches_pagination_streams_and_endpoints() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");
    let capabilities = adapter.capabilities();
    let v2 = capabilities.capabilities_v2;

    assert!(matches!(v2.public_rest, CapabilitySupport::Native));
    assert!(matches!(v2.private_rest, CapabilitySupport::Native));
    assert_eq!(v2.batch_place_orders.mode, BatchExecutionMode::Native);
    assert_eq!(v2.batch_place_orders.max_items, Some(15));
    assert!(v2.batch_place_orders.same_symbol_required);
    assert_eq!(v2.batch_cancel_orders.max_items, Some(50));
    assert!(v2.batch_cancel_orders.supports_partial_failure);
    assert!(v2.fills_history.supports_since);
    assert!(v2.fills_history.supports_cursor);
    assert_eq!(v2.fills_history.max_limit, Some(1000));
    assert!(v2.stream_runtime.public_private_separate_connections);
    assert!(
        v2.stream_runtime
            .orderbook_requires_snapshot_after_reconnect
    );
    assert_eq!(v2.stream_runtime.heartbeat_policy.ping_interval_ms, 30_000);
    assert!(v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.path.as_deref() == Some("/0/private/AddOrderBatch")));
    assert!(v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "subscribe_private_stream"
            && endpoint.transport == rustcta_exchange_api::EndpointTransport::WebSocket));
}

#[test]
fn kraken_request_spec_fixtures_should_cover_private_writes() {
    let spot = fixture("request_specs/spot_place_order_limit.json");
    assert_eq!(spot["operation"], "kraken.place_order.spot");
    assert_eq!(spot["method"], "POST");
    assert_eq!(spot["path"], "/0/private/AddOrder");
    assert_eq!(spot["form"]["pair"], "XBTUSD");
    assert_eq!(spot["form"]["cl_ord_id"], "krk-client-1");

    let futures = fixture("request_specs/futures_batch_order.json");
    assert_eq!(futures["operation"], "kraken.batch_place_orders.perpetual");
    assert_eq!(futures["method"], "POST");
    assert_eq!(futures["path"], "/derivatives/api/v3/batchorder");
    assert!(futures["form"]["batchOrder"]
        .as_str()
        .is_some_and(|batch| batch.contains("\"order\":\"send\"")));
}

#[test]
fn kraken_private_parser_fixtures_should_cover_success_empty_error_and_missing_fields() {
    let balances = super::private_parser::parse_balances(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &fixture("balance.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");
    assert_eq!(balances[0].balances[0].locked, 0.25);

    let empty_balances = super::private_parser::parse_balances(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &fixture("balance_empty.json"),
    )
    .expect("empty balances");
    assert!(empty_balances[0].balances.is_empty());

    let fills = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&spot_symbol_scope()),
        &fixture("fills.json"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].canonical_symbol.as_str(), "BTC/USD");
    assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));

    let empty_fills = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&spot_symbol_scope()),
        &fixture("fills_empty.json"),
    )
    .expect("empty fills");
    assert!(empty_fills.is_empty());

    let missing_symbol_fills = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        None,
        &fixture("fills_missing_symbol.json"),
    )
    .expect("missing symbol is skipped");
    assert!(missing_symbol_fills.is_empty());

    let error = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&spot_symbol_scope()),
        &fixture("error.json"),
    )
    .unwrap_err();
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

fn limit_order(
    request_id: &str,
    market_type: MarketType,
    client_order_id: &str,
) -> PlaceOrderRequest {
    let symbol = match market_type {
        MarketType::Spot => spot_symbol_scope(),
        MarketType::Perpetual => perp_symbol_scope(),
        _ => unreachable!("test only uses kraken-supported market types"),
    };
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol,
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("37500".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

#[tokio::test]
async fn kraken_spot_place_order_should_send_signed_add_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": [],
        "result": {"txid": ["OABCDEF-12345-67890"]}
    })])
    .await;
    let adapter =
        KrakenGatewayAdapter::new(private_config(format!("{base_url}/0"))).expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("krk-client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.01".to_string(),
            price: Some("37500".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");

    assert_eq!(
        response.order.exchange_order_id.as_deref(),
        Some("OABCDEF-12345-67890")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/0/private/AddOrder");
    assert_eq!(
        request.headers.get("api-key").map(String::as_str),
        Some("key")
    );
    assert!(request.headers.get("api-sign").is_some());
    assert_eq!(request.form.get("pair").map(String::as_str), Some("XBTUSD"));
    assert_eq!(
        request.form.get("ordertype").map(String::as_str),
        Some("limit")
    );
    assert_eq!(
        request.form.get("cl_ord_id").map(String::as_str),
        Some("krk-client-1")
    );
}

#[tokio::test]
async fn kraken_quote_market_buy_should_set_viqc_flag() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": [],
        "result": {"txid": ["OQUOTE-12345-67890"]}
    })])
    .await;
    let adapter =
        KrakenGatewayAdapter::new(private_config(format!("{base_url}/0"))).expect("adapter");

    adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("quote-client".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "100".to_string(),
        })
        .await
        .expect("quote");

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/0/private/AddOrder");
    assert_eq!(request.form.get("oflags").map(String::as_str), Some("viqc"));
    assert_eq!(request.form.get("volume").map(String::as_str), Some("100"));
}

#[tokio::test]
async fn kraken_spot_batch_place_and_cancel_should_use_private_batch_endpoints() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "error": [],
            "result": {"orders": [
                {"txid": "OSPOT-BATCH-1"},
                {"txid": "OSPOT-BATCH-2"}
            ]}
        }),
        json!({"error": [], "result": {"count": 2}}),
    ])
    .await;
    let adapter =
        KrakenGatewayAdapter::new(private_config(format!("{base_url}/0"))).expect("adapter");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-place"),
            exchange: super::test_support::exchange_id(),
            orders: vec![
                limit_order("spot-batch-place-1", MarketType::Spot, "spot-batch-1"),
                limit_order("spot-batch-place-2", MarketType::Spot, "spot-batch-2"),
            ],
        })
        .await
        .expect("batch place");

    assert_eq!(placed.orders.len(), 2);
    assert_eq!(
        placed.orders[0].exchange_order_id.as_deref(),
        Some("OSPOT-BATCH-1")
    );

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-cancel"),
            exchange: super::test_support::exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-batch-cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("OSPOT-BATCH-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-batch-cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("OSPOT-BATCH-2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");

    assert_eq!(cancelled.cancelled_count, 2);
    assert_eq!(cancelled.orders.len(), 2);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/0/private/AddOrderBatch");
    assert!(requests[0].form.contains_key("orders"));
    assert!(requests[0]
        .form
        .get("orders")
        .is_some_and(|orders| orders.contains("ordertype")));
    assert_eq!(requests[1].path, "/0/private/CancelOrderBatch");
    assert!(requests[1].form.contains_key("orders"));
}

#[tokio::test]
async fn kraken_futures_batch_place_and_cancel_should_use_batchorder_endpoint() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "success",
            "batchStatus": [
                {"sendStatus": {"order_id": "FUT-BATCH-1"}},
                {"sendStatus": {"order_id": "FUT-BATCH-2"}}
            ]
        }),
        json!({"result": "success", "batchStatus": [{"status": "success"}, {"status": "success"}]}),
    ])
    .await;
    let mut config = private_config(format!("{base_url}/0"));
    config.futures_rest_base_url = format!("{base_url}/derivatives/api/v3");
    let adapter = KrakenGatewayAdapter::new(config).expect("adapter");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-batch-place"),
            exchange: super::test_support::exchange_id(),
            orders: vec![
                limit_order(
                    "futures-batch-place-1",
                    MarketType::Perpetual,
                    "fut-batch-1",
                ),
                limit_order(
                    "futures-batch-place-2",
                    MarketType::Perpetual,
                    "fut-batch-2",
                ),
            ],
        })
        .await
        .expect("futures batch place");

    assert_eq!(placed.orders.len(), 2);
    assert_eq!(
        placed.orders[0].exchange_order_id.as_deref(),
        Some("FUT-BATCH-1")
    );

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-batch-cancel"),
            exchange: super::test_support::exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("futures-batch-cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("FUT-BATCH-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("futures-batch-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("FUT-BATCH-2".to_string()),
                },
            ],
        })
        .await
        .expect("futures batch cancel");

    assert_eq!(cancelled.cancelled_count, 2);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/derivatives/api/v3/batchorder");
    assert!(requests[0].form.contains_key("batchOrder"));
    assert!(requests[0]
        .form
        .get("batchOrder")
        .is_some_and(|batch| batch.contains("%22order%22%3A%22send%22")));
    assert_eq!(requests[1].path, "/derivatives/api/v3/batchorder");
    assert!(requests[1]
        .form
        .get("batchOrder")
        .is_some_and(|batch| batch.contains("%22order%22%3A%22cancel%22")));
}

#[tokio::test]
async fn kraken_cancel_query_and_open_orders_should_use_private_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"error": [], "result": {"count": 1}}),
        json!({"error": [], "result": {
            "OABCDEF-12345-67890": {
                "status": "open",
                "descr": {"pair": "XBT/USD", "type": "buy", "ordertype": "limit", "price": "37500"},
                "vol": "0.01",
                "vol_exec": "0"
            }
        }}),
        json!({"error": [], "result": {"open": {
            "OABCDEF-12345-67890": {
                "status": "open",
                "descr": {"pair": "XBT/USD", "type": "buy", "ordertype": "limit", "price": "37500"},
                "vol": "0.01",
                "vol_exec": "0"
            }
        }}}),
    ])
    .await;
    let adapter =
        KrakenGatewayAdapter::new(private_config(format!("{base_url}/0"))).expect("adapter");

    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("OABCDEF-12345-67890".to_string()),
        })
        .await
        .expect("cancel");
    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("OABCDEF-12345-67890".to_string()),
        })
        .await
        .expect("query");
    let open = adapter
        .get_open_orders(rustcta_exchange_api::OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("open");

    assert!(query.order.is_some());
    assert_eq!(open.orders.len(), 1);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/0/private/CancelOrder");
    assert_eq!(requests[1].path, "/0/private/QueryOrders");
    assert_eq!(requests[2].path, "/0/private/OpenOrders");
}

#[tokio::test]
async fn kraken_balances_fees_fills_and_positions_should_parse_private_responses() {
    let (base_url, _seen) = spawn_rest_server(vec![
        json!({"error": [], "result": {"XXBT": {"balance": "1.5", "hold_trade": "0.25"}}}),
        json!({"error": [], "result": {"fees": {"XBTUSD": {"fee": "0.26", "fee_maker": "0.16"}}}}),
        json!({"error": [], "result": {"trades": {
            "T1": {"pair": "XBT/USD", "type": "buy", "price": "37500", "vol": "0.01", "fee": "0.1", "time": "1700000000.0"}
        }}}),
        json!({"result": "success", "openPositions": [{
            "symbol": "PF_XBTUSDT",
            "side": "long",
            "size": "2",
            "price": "37000",
            "markPrice": "37100"
        }]}),
    ])
    .await;
    let adapter =
        KrakenGatewayAdapter::new(private_config(format!("{base_url}/0"))).expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec![],
        })
        .await
        .expect("balances");
    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("fees");
    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: None,
            page: None,
        })
        .await
        .expect("fills");
    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");

    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].locked, 0.25);
    assert_eq!(fees.fees[0].maker_rate, "0.0016");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(positions.positions.len(), 1);
}

#[tokio::test]
async fn kraken_recent_fills_should_send_spot_and_futures_pagination_params() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"error": [], "result": {"trades": {}}}),
        json!({"result": "success", "fills": []}),
    ])
    .await;
    let mut config = private_config(format!("{base_url}/0"));
    config.futures_rest_base_url = format!("{base_url}/derivatives/api/v3");
    let adapter = KrakenGatewayAdapter::new(config).expect("adapter");

    adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-fills-page"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("OABCDEF-12345-67890".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: None,
            page: Some(PageRequest::next_page(
                Some(200),
                PageCursor::Offset { offset: 50 },
            )),
        })
        .await
        .expect("spot fills");
    adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-fills-page"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: Some("2026-06-08T00:00:00Z".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("futures fills");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/0/private/TradesHistory");
    assert_eq!(
        requests[0].form.get("pair").map(String::as_str),
        Some("XBTUSD")
    );
    assert_eq!(
        requests[0].form.get("txid").map(String::as_str),
        Some("OABCDEF-12345-67890")
    );
    assert_eq!(
        requests[0].form.get("count").map(String::as_str),
        Some("200")
    );
    assert_eq!(requests[0].form.get("ofs").map(String::as_str), Some("50"));
    assert_eq!(requests[1].path, "/derivatives/api/v3/fills");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("PF_XBTUSDT")
    );
    assert_eq!(
        requests[1].query.get("count").map(String::as_str),
        Some("25")
    );
    assert_eq!(
        requests[1].query.get("lastFillTime").map(String::as_str),
        Some("2026-06-08T00%3A00%3A00Z")
    );
}

#[tokio::test]
async fn kraken_spot_symbol_scoped_cancel_all_should_be_unsupported() {
    let adapter = KrakenGatewayAdapter::new(private_config("http://127.0.0.1:9/0".to_string()))
        .expect("adapter");

    let err = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
        })
        .await
        .unwrap_err();

    assert!(matches!(err, ExchangeApiError::Unsupported { .. }));
}
