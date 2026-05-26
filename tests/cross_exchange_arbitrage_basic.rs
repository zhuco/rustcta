const CROSS_ARB_SQL: &str = include_str!("../sql/cross_exchange_arbitrage.sql");

#[derive(Debug, Clone)]
struct SyntheticBookEvent {
    exchange: &'static str,
    canonical_symbol: &'static str,
    best_bid: f64,
    best_bid_qty: f64,
    best_ask: f64,
    best_ask_qty: f64,
    recv_age_ms: u64,
    sequence: u64,
    previous_sequence: Option<u64>,
}

impl SyntheticBookEvent {
    fn quality_flags(&self, stale_after_ms: u64) -> Vec<&'static str> {
        let mut flags = Vec::new();

        if self.best_bid <= 0.0
            || self.best_bid_qty <= 0.0
            || self.best_ask <= 0.0
            || self.best_ask_qty <= 0.0
        {
            flags.push("invalid_level");
        }

        if self.best_bid >= self.best_ask {
            flags.push("crossed");
        }

        if self.recv_age_ms > stale_after_ms {
            flags.push("stale");
        }

        if let Some(previous_sequence) = self.previous_sequence {
            if self.sequence != previous_sequence + 1 {
                flags.push("sequence_gap");
            }
        }

        flags
    }

    fn executable_qty(&self) -> f64 {
        self.best_bid_qty.min(self.best_ask_qty)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrphanRecoveryAction {
    None,
    PauseNewEntries,
    EmergencyClose,
    AlertOnly,
}

fn orphan_recovery_action(
    maker_filled: bool,
    hedge_filled: bool,
    exposure_usdt: f64,
    max_orphan_exposure_usdt: f64,
) -> OrphanRecoveryAction {
    if !maker_filled || hedge_filled {
        return OrphanRecoveryAction::None;
    }

    if exposure_usdt > max_orphan_exposure_usdt {
        OrphanRecoveryAction::EmergencyClose
    } else if exposure_usdt > 0.0 {
        OrphanRecoveryAction::PauseNewEntries
    } else {
        OrphanRecoveryAction::AlertOnly
    }
}

fn required_tables() -> [&'static str; 11] {
    [
        "cross_arb_market_books_sampled",
        "cross_arb_funding_snapshots",
        "cross_arb_opportunities",
        "cross_arb_signals",
        "cross_arb_execution_commands",
        "cross_arb_order_events",
        "cross_arb_bundle_events",
        "cross_arb_reconcile_reports",
        "cross_arb_route_health_events",
        "cross_arb_risk_events",
        "cross_arb_alerts",
    ]
}

#[test]
fn cross_exchange_arbitrage_sql_should_define_required_tables() {
    for table in required_tables() {
        let create_statement = format!("CREATE TABLE IF NOT EXISTS rustcta.{table}");
        assert!(
            CROSS_ARB_SQL.contains(&create_statement),
            "missing table definition for {table}"
        );
    }
}

#[test]
fn cross_exchange_arbitrage_sql_should_capture_dashboard_read_model_fields() {
    for field in [
        "runtime_mode",
        "canonical_symbol",
        "exchange",
        "route",
        "accepted",
        "reject_reason",
        "risk_flags",
        "bundle_id",
        "severity",
        "recommended_action",
        "alert_required",
        "next_status",
        "reconnect_count",
        "sequence_gap_count",
    ] {
        assert!(
            CROSS_ARB_SQL.contains(field),
            "SQL schema should include dashboard field {field}"
        );
    }
}

#[test]
fn cross_exchange_arbitrage_sql_should_be_offline_clickhouse_schema() {
    assert!(CROSS_ARB_SQL.contains("ENGINE = MergeTree"));
    assert!(CROSS_ARB_SQL.contains("PARTITION BY toYYYYMMDD(event_ts)"));
    assert!(CROSS_ARB_SQL.contains("TTL event_ts + INTERVAL"));
    assert!(
        !CROSS_ARB_SQL.to_ascii_lowercase().contains("clickhouse://"),
        "schema must not require a real ClickHouse connection"
    );
}

#[test]
fn cross_exchange_arbitrage_fixture_should_mark_synthetic_orderbook_quality() {
    let healthy = SyntheticBookEvent {
        exchange: "binance",
        canonical_symbol: "BTC/USDT",
        best_bid: 100.0,
        best_bid_qty: 2.0,
        best_ask: 100.5,
        best_ask_qty: 1.5,
        recv_age_ms: 50,
        sequence: 42,
        previous_sequence: Some(41),
    };

    assert_eq!(healthy.quality_flags(1_000), Vec::<&str>::new());
    assert_eq!(healthy.executable_qty(), 1.5);
    assert_eq!(healthy.exchange, "binance");
    assert_eq!(healthy.canonical_symbol, "BTC/USDT");
}

#[test]
fn cross_exchange_arbitrage_fixture_should_detect_stale_quote() {
    let stale = SyntheticBookEvent {
        exchange: "okx",
        canonical_symbol: "ETH/USDT",
        best_bid: 2_000.0,
        best_bid_qty: 3.0,
        best_ask: 2_001.0,
        best_ask_qty: 3.0,
        recv_age_ms: 2_500,
        sequence: 8,
        previous_sequence: Some(7),
    };

    assert_eq!(stale.quality_flags(1_000), vec!["stale"]);
}

#[test]
fn cross_exchange_arbitrage_fixture_should_detect_sequence_gap() {
    let gap = SyntheticBookEvent {
        exchange: "bitget",
        canonical_symbol: "SOL/USDT",
        best_bid: 150.0,
        best_bid_qty: 10.0,
        best_ask: 150.1,
        best_ask_qty: 9.0,
        recv_age_ms: 20,
        sequence: 105,
        previous_sequence: Some(100),
    };

    assert_eq!(gap.quality_flags(1_000), vec!["sequence_gap"]);
}

#[test]
fn cross_exchange_arbitrage_fixture_should_reject_depth_insufficient() {
    let thin_book = SyntheticBookEvent {
        exchange: "gate",
        canonical_symbol: "WIF/USDT",
        best_bid: 2.10,
        best_bid_qty: 15.0,
        best_ask: 2.11,
        best_ask_qty: 12.0,
        recv_age_ms: 25,
        sequence: 77,
        previous_sequence: Some(76),
    };

    let requested_qty = 20.0;

    assert!(thin_book.quality_flags(1_000).is_empty());
    assert!(thin_book.executable_qty() < requested_qty);
}

#[test]
fn cross_exchange_arbitrage_fixture_should_model_orphan_recovery() {
    assert_eq!(
        orphan_recovery_action(true, false, 125.0, 100.0),
        OrphanRecoveryAction::EmergencyClose
    );
    assert_eq!(
        orphan_recovery_action(true, false, 25.0, 100.0),
        OrphanRecoveryAction::PauseNewEntries
    );
    assert_eq!(
        orphan_recovery_action(true, true, 125.0, 100.0),
        OrphanRecoveryAction::None
    );
}
