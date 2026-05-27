use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy)]
struct ExchangeFixtureSpec {
    exchange: &'static str,
    market_metadata_file: &'static str,
}

const EXCHANGE_FIXTURES: &[ExchangeFixtureSpec] = &[
    ExchangeFixtureSpec {
        exchange: "binance",
        market_metadata_file: "instruments.json",
    },
    ExchangeFixtureSpec {
        exchange: "okx",
        market_metadata_file: "instruments.json",
    },
    ExchangeFixtureSpec {
        exchange: "bitget",
        market_metadata_file: "contracts.json",
    },
    ExchangeFixtureSpec {
        exchange: "gate",
        market_metadata_file: "contracts.json",
    },
];

const COMMON_FIXTURE_FILES: &[&str] = &[
    "orderbook.json",
    "funding.json",
    "order_ack.json",
    "fill.json",
    "position.json",
    "balance.json",
    "error.json",
];

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("exchanges")
}

fn fixture_path(exchange: &str, file_name: &str) -> PathBuf {
    fixtures_root().join(exchange).join(file_name)
}

fn fixture_files(spec: ExchangeFixtureSpec) -> Vec<&'static str> {
    let mut files = Vec::with_capacity(COMMON_FIXTURE_FILES.len() + 1);
    files.push(spec.market_metadata_file);
    files.extend(COMMON_FIXTURE_FILES.iter().copied());
    files
}

fn read_fixture_json(exchange: &str, file_name: &str) -> Value {
    let path = fixture_path(exchange, file_name);
    let bytes = fs::read(&path)
        .unwrap_or_else(|err| panic!("failed to read fixture {}: {err}", path.display()));

    serde_json::from_slice(&bytes)
        .unwrap_or_else(|err| panic!("failed to parse fixture {}: {err}", path.display()))
}

fn json_at<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value
        .pointer(pointer)
        .unwrap_or_else(|| panic!("missing JSON field at pointer {pointer}"))
}

fn array_at<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    json_at(value, pointer)
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| panic!("expected JSON array at pointer {pointer}"))
}

fn first_array_item<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    array_at(value, pointer)
        .first()
        .unwrap_or_else(|| panic!("expected non-empty JSON array at pointer {pointer}"))
}

fn assert_has_path(value: &Value, pointer: &str) {
    assert!(
        !json_at(value, pointer).is_null(),
        "JSON pointer {pointer} should not be null"
    );
}

fn assert_has_paths(value: &Value, pointers: &[&str]) {
    for pointer in pointers {
        assert_has_path(value, pointer);
    }
}

fn assert_string_at(value: &Value, pointer: &str, expected: &str) {
    assert_eq!(
        json_at(value, pointer).as_str(),
        Some(expected),
        "unexpected string at JSON pointer {pointer}"
    );
}

fn assert_array_contains_string(value: &Value, pointer: &str, expected: &str) {
    assert!(
        array_at(value, pointer)
            .iter()
            .any(|item| item.as_str() == Some(expected)),
        "array at JSON pointer {pointer} should contain {expected}"
    );
}

fn assert_price_size_level(value: &Value, pointer: &str) {
    let level = array_at(value, pointer);
    assert!(
        level.len() >= 2,
        "level at JSON pointer {pointer} should contain price and size"
    );
    assert!(
        level[0].is_string() || level[0].is_number(),
        "level price at JSON pointer {pointer} should be string or number"
    );
    assert!(
        level[1].is_string() || level[1].is_number(),
        "level size at JSON pointer {pointer} should be string or number"
    );
}

fn find_object_by_string_field<'a>(
    items: &'a [Value],
    field_name: &str,
    expected: &str,
) -> &'a Value {
    items
        .iter()
        .find(|item| item.get(field_name).and_then(Value::as_str) == Some(expected))
        .unwrap_or_else(|| panic!("missing object with {field_name}={expected}"))
}

fn assert_binance_key_fields() {
    let instruments = read_fixture_json("binance", "instruments.json");
    let symbol = first_array_item(&instruments, "/symbols");
    assert_has_paths(
        symbol,
        &[
            "/symbol",
            "/contractType",
            "/status",
            "/quoteAsset",
            "/marginAsset",
            "/filters",
            "/timeInForce",
        ],
    );
    assert_string_at(symbol, "/contractType", "PERPETUAL");
    assert_string_at(symbol, "/quoteAsset", "USDT");
    assert_string_at(symbol, "/marginAsset", "USDT");
    assert_string_at(symbol, "/status", "TRADING");
    assert_array_contains_string(symbol, "/timeInForce", "GTX");
    assert_array_contains_string(symbol, "/timeInForce", "IOC");

    let filters = array_at(symbol, "/filters");
    let price_filter = find_object_by_string_field(filters, "filterType", "PRICE_FILTER");
    let lot_size = find_object_by_string_field(filters, "filterType", "LOT_SIZE");
    let min_notional = find_object_by_string_field(filters, "filterType", "MIN_NOTIONAL");
    assert_has_path(price_filter, "/tickSize");
    assert_has_paths(lot_size, &["/minQty", "/stepSize"]);
    assert_has_path(min_notional, "/notional");

    let orderbook = read_fixture_json("binance", "orderbook.json");
    assert_has_paths(&orderbook, &["/lastUpdateId", "/E", "/T", "/bids", "/asks"]);
    assert_price_size_level(&orderbook, "/bids/0");
    assert_price_size_level(&orderbook, "/asks/0");

    let funding = read_fixture_json("binance", "funding.json");
    assert_has_paths(
        first_array_item(&funding, ""),
        &["/symbol", "/fundingRate", "/fundingTime", "/markPrice"],
    );

    let order_ack = read_fixture_json("binance", "order_ack.json");
    assert_has_paths(
        &order_ack,
        &[
            "/clientOrderId",
            "/orderId",
            "/symbol",
            "/status",
            "/side",
            "/positionSide",
            "/timeInForce",
            "/reduceOnly",
        ],
    );

    let fill = read_fixture_json("binance", "fill.json");
    assert_has_paths(
        first_array_item(&fill, ""),
        &[
            "/id",
            "/orderId",
            "/symbol",
            "/price",
            "/qty",
            "/commission",
            "/commissionAsset",
            "/positionSide",
            "/maker",
        ],
    );

    let position = read_fixture_json("binance", "position.json");
    assert_has_paths(
        first_array_item(&position, ""),
        &[
            "/symbol",
            "/positionAmt",
            "/entryPrice",
            "/markPrice",
            "/unRealizedProfit",
            "/liquidationPrice",
            "/leverage",
            "/marginType",
            "/positionSide",
        ],
    );

    let balance = read_fixture_json("binance", "balance.json");
    assert_has_paths(
        first_array_item(&balance, ""),
        &[
            "/asset",
            "/balance",
            "/crossWalletBalance",
            "/availableBalance",
            "/marginAvailable",
        ],
    );

    let error = read_fixture_json("binance", "error.json");
    assert_has_paths(&error, &["/code", "/msg"]);
}

fn assert_okx_key_fields() {
    let instruments = read_fixture_json("okx", "instruments.json");
    assert_string_at(&instruments, "/code", "0");
    let instrument = first_array_item(&instruments, "/data");
    assert_has_paths(
        instrument,
        &[
            "/instType",
            "/instId",
            "/settleCcy",
            "/ctVal",
            "/ctValCcy",
            "/tickSz",
            "/lotSz",
            "/minSz",
            "/ctType",
            "/state",
        ],
    );
    assert_string_at(instrument, "/instType", "SWAP");
    assert_string_at(instrument, "/settleCcy", "USDT");
    assert_string_at(instrument, "/ctType", "linear");
    assert_string_at(instrument, "/state", "live");

    let orderbook = read_fixture_json("okx", "orderbook.json");
    assert_string_at(&orderbook, "/code", "0");
    assert_has_paths(
        first_array_item(&orderbook, "/data"),
        &["/asks", "/bids", "/ts"],
    );
    assert_price_size_level(&orderbook, "/data/0/bids/0");
    assert_price_size_level(&orderbook, "/data/0/asks/0");

    let funding = read_fixture_json("okx", "funding.json");
    assert_has_paths(
        first_array_item(&funding, "/data"),
        &[
            "/instType",
            "/instId",
            "/fundingRate",
            "/fundingTime",
            "/nextFundingTime",
        ],
    );

    let order_ack = read_fixture_json("okx", "order_ack.json");
    assert_has_paths(
        first_array_item(&order_ack, "/data"),
        &["/clOrdId", "/ordId", "/sCode", "/sMsg"],
    );

    let fill = read_fixture_json("okx", "fill.json");
    assert_has_paths(
        first_array_item(&fill, "/data"),
        &[
            "/instId", "/tradeId", "/ordId", "/clOrdId", "/fillPx", "/fillSz", "/side", "/posSide",
            "/feeCcy", "/fee", "/ts",
        ],
    );

    let position = read_fixture_json("okx", "position.json");
    assert_has_paths(
        first_array_item(&position, "/data"),
        &[
            "/instId", "/mgnMode", "/posSide", "/pos", "/avgPx", "/upl", "/liqPx", "/lever",
            "/margin", "/markPx",
        ],
    );

    let balance = read_fixture_json("okx", "balance.json");
    let account = first_array_item(&balance, "/data");
    assert_has_paths(account, &["/totalEq", "/adjEq", "/details"]);
    assert_has_paths(
        first_array_item(account, "/details"),
        &["/ccy", "/eq", "/availEq", "/cashBal"],
    );

    let error = read_fixture_json("okx", "error.json");
    assert_has_paths(&error, &["/code", "/msg", "/data"]);
    assert_has_paths(first_array_item(&error, "/data"), &["/sCode", "/sMsg"]);
}

fn assert_bitget_key_fields() {
    let contracts = read_fixture_json("bitget", "contracts.json");
    assert_string_at(&contracts, "/code", "00000");
    assert_string_at(&contracts, "/request/productType", "USDT-FUTURES");
    let contract = first_array_item(&contracts, "/data");
    assert_has_paths(
        contract,
        &[
            "/symbol",
            "/baseCoin",
            "/quoteCoin",
            "/supportMarginCoins",
            "/minTradeNum",
            "/priceEndStep",
            "/pricePlace",
            "/sizeMultiplier",
            "/symbolType",
            "/minTradeUSDT",
            "/maxMarketOrderQty",
            "/maxOrderQty",
            "/maxPositionNum",
            "/symbolStatus",
        ],
    );
    assert_string_at(contract, "/quoteCoin", "USDT");
    assert_string_at(contract, "/symbolType", "perpetual");
    assert_string_at(contract, "/symbolStatus", "normal");
    assert_array_contains_string(contract, "/supportMarginCoins", "USDT");

    let orderbook = read_fixture_json("bitget", "orderbook.json");
    assert_string_at(&orderbook, "/code", "00000");
    assert_string_at(&orderbook, "/request/productType", "USDT-FUTURES");
    assert_has_paths(
        &orderbook,
        &["/data/asks", "/data/bids", "/data/ts", "/data/precision"],
    );
    assert_price_size_level(&orderbook, "/data/bids/0");
    assert_price_size_level(&orderbook, "/data/asks/0");

    let funding = read_fixture_json("bitget", "funding.json");
    assert_has_paths(
        first_array_item(&funding, "/data"),
        &[
            "/symbol",
            "/fundingRate",
            "/fundingRateInterval",
            "/nextUpdate",
            "/minFundingRate",
            "/maxFundingRate",
        ],
    );

    let order_ack = read_fixture_json("bitget", "order_ack.json");
    assert_has_paths(
        &order_ack,
        &[
            "/request/productType",
            "/request/marginCoin",
            "/request/side",
            "/request/tradeSide",
            "/request/force",
            "/request/clientOid",
            "/data/clientOid",
            "/data/orderId",
        ],
    );

    let fill = read_fixture_json("bitget", "fill.json");
    assert_has_paths(
        first_array_item(&fill, "/data/fillList"),
        &[
            "/tradeId",
            "/orderId",
            "/clientOid",
            "/symbol",
            "/productType",
            "/marginCoin",
            "/price",
            "/size",
            "/fee",
            "/side",
            "/tradeSide",
        ],
    );

    let position = read_fixture_json("bitget", "position.json");
    assert_has_paths(
        first_array_item(&position, "/data"),
        &[
            "/symbol",
            "/marginCoin",
            "/holdSide",
            "/available",
            "/total",
            "/leverage",
            "/averageOpenPrice",
            "/marginMode",
            "/holdMode",
            "/unrealizedPL",
            "/liquidationPrice",
            "/markPrice",
        ],
    );

    let balance = read_fixture_json("bitget", "balance.json");
    assert_has_paths(
        first_array_item(&balance, "/data"),
        &[
            "/marginCoin",
            "/available",
            "/equity",
            "/usdtEquity",
            "/unrealizedPL",
        ],
    );

    let error = read_fixture_json("bitget", "error.json");
    assert_has_paths(&error, &["/code", "/msg", "/requestTime"]);
}

fn assert_gate_key_fields() {
    let contracts = read_fixture_json("gate", "contracts.json");
    let contract = first_array_item(&contracts, "");
    assert_has_paths(
        contract,
        &[
            "/name",
            "/type",
            "/quanto_multiplier",
            "/order_price_round",
            "/order_size_min",
            "/order_size_max",
            "/funding_rate_indicative",
            "/funding_next_apply",
            "/funding_interval",
            "/in_delisting",
            "/trade_status",
        ],
    );
    assert_string_at(contract, "/name", "BTC_USDT");
    assert_string_at(contract, "/trade_status", "tradable");

    let orderbook = read_fixture_json("gate", "orderbook.json");
    assert_has_paths(
        &orderbook,
        &["/id", "/current", "/update", "/bids", "/asks"],
    );
    assert_has_paths(first_array_item(&orderbook, "/bids"), &["/p", "/s"]);
    assert_has_paths(first_array_item(&orderbook, "/asks"), &["/p", "/s"]);

    let funding = read_fixture_json("gate", "funding.json");
    assert_has_paths(first_array_item(&funding, ""), &["/t", "/r"]);

    let order_ack = read_fixture_json("gate", "order_ack.json");
    assert_has_paths(
        &order_ack,
        &[
            "/id",
            "/contract",
            "/size",
            "/left",
            "/price",
            "/tif",
            "/text",
            "/is_reduce_only",
            "/status",
            "/pos_margin_mode",
        ],
    );

    let fill = read_fixture_json("gate", "fill.json");
    assert_has_paths(
        first_array_item(&fill, ""),
        &[
            "/id",
            "/contract",
            "/order_id",
            "/size",
            "/price",
            "/role",
            "/fee",
            "/fee_currency",
            "/text",
        ],
    );

    let position = read_fixture_json("gate", "position.json");
    assert_has_paths(
        first_array_item(&position, ""),
        &[
            "/contract",
            "/size",
            "/leverage",
            "/risk_limit",
            "/maintenance_rate",
            "/value",
            "/margin",
            "/entry_price",
            "/liq_price",
            "/mark_price",
            "/unrealised_pnl",
            "/mode",
        ],
    );

    let balance = read_fixture_json("gate", "balance.json");
    assert_has_paths(
        &balance,
        &[
            "/currency",
            "/total",
            "/unrealised_pnl",
            "/order_margin",
            "/available",
            "/in_dual_mode",
            "/position_mode",
        ],
    );

    let error = read_fixture_json("gate", "error.json");
    assert_has_paths(&error, &["/label", "/message"]);
}

#[test]
fn cross_exchange_fixtures_should_exist_for_supported_exchanges() {
    for spec in EXCHANGE_FIXTURES {
        let exchange_dir = fixtures_root().join(spec.exchange);
        assert!(
            exchange_dir.is_dir(),
            "missing fixture directory {}",
            exchange_dir.display()
        );

        for file_name in fixture_files(*spec) {
            let path = fixture_path(spec.exchange, file_name);
            assert!(path.is_file(), "missing fixture file {}", path.display());
        }
    }
}

#[test]
fn cross_exchange_fixtures_should_be_parseable_json() {
    for spec in EXCHANGE_FIXTURES {
        for file_name in fixture_files(*spec) {
            read_fixture_json(spec.exchange, file_name);
        }
    }
}

#[test]
fn cross_exchange_fixtures_should_expose_exchange_specific_key_fields() {
    assert_binance_key_fields();
    assert_okx_key_fields();
    assert_bitget_key_fields();
    assert_gate_key_fields();
}
