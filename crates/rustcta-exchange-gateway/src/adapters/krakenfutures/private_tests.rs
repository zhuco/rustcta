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
use super::KrakenFuturesGatewayAdapter;

macro_rules! include_json {
    ($path:literal) => {
        serde_json::from_str(include_str!($path)).expect("json fixture")
    };
}

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "balance.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/balance.json")
        }
        "balance_empty.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/balance_empty.json")
        }
        "error.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/error.json")
        }
        "fills.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/fills.json")
        }
        "fills_empty.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/fills_empty.json")
        }
        "fills_missing_symbol.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/fills_missing_symbol.json")
        }
        "positions.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/krakenfutures/positions.json")
        }
        "request_specs/futures_place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/krakenfutures/request_specs/futures_place_order_limit.json"
        ),
        "request_specs/futures_batch_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/krakenfutures/request_specs/futures_batch_order.json"
        ),
        "request_specs/futures_batch_cancel.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/krakenfutures/request_specs/futures_batch_cancel.json"
        ),
        _ => panic!("unknown krakenfutures fixture {name}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[test]
fn krakenfutures_capabilities_v2_should_declare_futures_endpoints() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");
    let capabilities = adapter.capabilities();
    let v2 = capabilities.capabilities_v2;

    assert!(matches!(v2.public_rest, CapabilitySupport::Native));
    assert!(matches!(v2.private_rest, CapabilitySupport::Native));
    assert_eq!(v2.batch_place_orders.mode, BatchExecutionMode::Native);
    assert_eq!(v2.batch_place_orders.max_items, Some(15));
    assert!(v2.batch_place_orders.same_symbol_required);
    assert_eq!(v2.batch_cancel_orders.max_items, Some(50));
    assert!(v2.stream_runtime.public_private_separate_connections);
    assert!(v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "query_order"
            && endpoint.path.as_deref() == Some("/derivatives/api/v3/orders")));
    assert!(!v2.endpoints.iter().any(|endpoint| endpoint
        .path
        .as_deref()
        .is_some_and(|path| path.contains("/0/"))));
}

#[test]
fn krakenfutures_request_spec_fixtures_should_cover_private_writes() {
    let place = fixture("request_specs/futures_place_order_limit.json");
    assert_eq!(place["operation"], "krakenfutures.place_order.perpetual");
    assert_eq!(place["path"], "/derivatives/api/v3/sendorder");
    assert_eq!(place["form"]["cliOrdId"], "client-1");

    let batch_place = fixture("request_specs/futures_batch_order.json");
    assert_eq!(
        batch_place["operation"],
        "krakenfutures.batch_place_orders.perpetual"
    );
    assert!(batch_place["form"]["batchOrder"]
        .as_str()
        .is_some_and(|batch| batch.contains("\"params\"")));

    let batch_cancel = fixture("request_specs/futures_batch_cancel.json");
    assert_eq!(
        batch_cancel["operation"],
        "krakenfutures.batch_cancel_orders.perpetual"
    );
    assert!(batch_cancel["form"]["batchOrder"]
        .as_str()
        .is_some_and(|batch| batch.contains("\"cancel\"")));
}

#[test]
fn krakenfutures_private_parser_fixtures_should_cover_futures_shapes() {
    let balances = super::private_parser::parse_balances(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Perpetual,
        &[],
        &fixture("balance.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "USDT");
    assert_eq!(balances[0].balances[0].locked, 250.0);

    let empty_balances = super::private_parser::parse_balances(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Perpetual,
        &[],
        &fixture("balance_empty.json"),
    )
    .expect("empty balances");
    assert!(empty_balances[0].balances.is_empty());

    let fills = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&perp_symbol_scope()),
        &fixture("fills.json"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].canonical_symbol.as_str(), "BTC/USDT");
    assert_eq!(fills[0].client_order_id.as_deref(), Some("client-1"));

    let empty_fills = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&perp_symbol_scope()),
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

    let positions = super::private_parser::parse_positions(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        &[],
        &fixture("positions.json"),
    )
    .expect("positions");
    assert_eq!(positions[0].canonical_symbol.as_str(), "BTC/USDT");

    let error = super::private_parser::parse_recent_fills(
        &super::test_support::exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&perp_symbol_scope()),
        &fixture("error.json"),
    )
    .unwrap_err();
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

fn limit_order(request_id: &str, client_order_id: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol: perp_symbol_scope(),
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
async fn krakenfutures_spot_private_requests_should_be_unsupported() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");

    let err = adapter
        .place_order(PlaceOrderRequest {
            symbol: spot_symbol_scope(),
            ..limit_order("spot-place", "spot-client")
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ExchangeApiError::Unsupported { .. }));

    let err = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-cancel-all"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn krakenfutures_place_and_cancel_should_use_futures_private_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"result": "success", "sendStatus": {"order_id": "FUT-ORDER-1"}}),
        json!({"result": "success", "status": "cancelled"}),
    ])
    .await;
    let adapter = KrakenFuturesGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let placed = adapter
        .place_order(limit_order("place", "client-1"))
        .await
        .expect("place");
    assert_eq!(
        placed.order.exchange_order_id.as_deref(),
        Some("FUT-ORDER-1")
    );

    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("FUT-ORDER-1".to_string()),
        })
        .await
        .expect("cancel");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/sendorder");
    assert_eq!(
        requests[0].form.get("symbol").map(String::as_str),
        Some("PF_XBTUSDT")
    );
    assert_eq!(
        requests[0].form.get("cliOrdId").map(String::as_str),
        Some("client-1")
    );
    assert_eq!(requests[1].path, "/cancelorder");
    assert_eq!(
        requests[1].form.get("order_id").map(String::as_str),
        Some("FUT-ORDER-1")
    );
}

#[tokio::test]
async fn krakenfutures_batch_place_and_cancel_should_use_batchorder_endpoint() {
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
    let adapter = KrakenFuturesGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: super::test_support::exchange_id(),
            orders: vec![
                limit_order("batch-place-1", "fut-batch-1"),
                limit_order("batch-place-2", "fut-batch-2"),
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: super::test_support::exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("FUT-BATCH-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("FUT-BATCH-2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");

    assert_eq!(cancelled.cancelled_count, 2);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/batchorder");
    assert!(requests[0]
        .form
        .get("batchOrder")
        .is_some_and(|batch| batch.contains("%22order%22%3A%22send%22")));
    assert_eq!(requests[1].path, "/batchorder");
    assert!(requests[1]
        .form
        .get("batchOrder")
        .is_some_and(|batch| batch.contains("%22order%22%3A%22cancel%22")));
}

#[tokio::test]
async fn krakenfutures_readbacks_should_use_futures_private_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"result": "success", "accounts": {"USDT": {"balance": "1500", "hold_trade": "250"}}}),
        json!({"result": "success", "openPositions": [{
            "symbol": "PF_XBTUSDT", "side": "long", "size": "2", "price": "37000"
        }]}),
        include_json!("../../../../../tests/fixtures/exchanges/krakenfutures/order_query.json"),
        include_json!("../../../../../tests/fixtures/exchanges/krakenfutures/open_orders.json"),
        json!({"result": "success", "fills": [{
            "fill_id": "fill-1", "symbol": "PF_XBTUSDT", "side": "buy", "price": "37500", "size": "0.01"
        }]}),
    ])
    .await;
    let adapter = KrakenFuturesGatewayAdapter::new(private_config(base_url)).expect("adapter");

    adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec![],
        })
        .await
        .expect("balances");
    adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");
    adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("FUT-QUERY-1".to_string()),
        })
        .await
        .expect("query");
    adapter
        .get_open_orders(rustcta_exchange_api::OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("FUT-ORDER-1".to_string()),
            from_trade_id: Some("2026-06-08T00:00:00Z".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: Some(PageRequest::next_page(
                Some(25),
                PageCursor::Token {
                    token: "2026-06-08T00:00:00Z".to_string(),
                },
            )),
        })
        .await
        .expect("fills");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/accounts");
    assert_eq!(requests[1].path, "/openpositions");
    assert_eq!(requests[2].path, "/orders");
    assert_eq!(
        requests[2].query.get("orderIds").map(String::as_str),
        Some("FUT-QUERY-1")
    );
    assert_eq!(requests[3].path, "/openorders");
    assert_eq!(requests[4].path, "/fills");
    assert_eq!(
        requests[4].query.get("symbol").map(String::as_str),
        Some("PF_XBTUSDT")
    );
}

#[tokio::test]
async fn krakenfutures_quote_market_and_fees_remain_offline_boundaries() {
    let adapter =
        KrakenFuturesGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
            .expect("adapter");

    let quote_err = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("quote-client".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "100".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(quote_err, ExchangeApiError::Unsupported { .. }));

    let fee_err = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect_err("fees source boundary only");
    assert!(matches!(
        fee_err,
        ExchangeApiError::Unsupported {
            operation: "krakenfutures.fees_source_boundary_only"
        }
    ));
}
