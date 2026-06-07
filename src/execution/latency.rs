use serde::{Deserialize, Serialize};
use std::time::Instant;

pub const LATENCY_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyMarkKind {
    MarketReceive,
    StrategyReceive,
    Decision,
    RiskPass,
    Submit,
    Ack,
    PrivateUpdate,
    Fill,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyMark {
    pub kind: LatencyMarkKind,
    pub monotonic_ns: u128,
}

impl LatencyMark {
    pub fn new(kind: LatencyMarkKind, monotonic_ns: u128) -> Self {
        Self { kind, monotonic_ns }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpportunityLatencyTrace {
    pub schema_version: u16,
    pub market_receive_monotonic_ns: Option<u128>,
    pub strategy_receive_monotonic_ns: Option<u128>,
    pub decision_monotonic_ns: Option<u128>,
    pub risk_pass_monotonic_ns: Option<u128>,
    pub submit_monotonic_ns: Option<u128>,
    pub ack_monotonic_ns: Option<u128>,
    pub private_update_monotonic_ns: Option<u128>,
    pub fill_monotonic_ns: Option<u128>,
    pub reconcile_monotonic_ns: Option<u128>,
}

impl Default for OpportunityLatencyTrace {
    fn default() -> Self {
        Self {
            schema_version: LATENCY_SCHEMA_VERSION,
            market_receive_monotonic_ns: None,
            strategy_receive_monotonic_ns: None,
            decision_monotonic_ns: None,
            risk_pass_monotonic_ns: None,
            submit_monotonic_ns: None,
            ack_monotonic_ns: None,
            private_update_monotonic_ns: None,
            fill_monotonic_ns: None,
            reconcile_monotonic_ns: None,
        }
    }
}

impl OpportunityLatencyTrace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_mark(&mut self, mark: LatencyMark) {
        match mark.kind {
            LatencyMarkKind::MarketReceive => {
                self.market_receive_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::StrategyReceive => {
                self.strategy_receive_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::Decision => {
                self.decision_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::RiskPass => {
                self.risk_pass_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::Submit => {
                self.submit_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::Ack => {
                self.ack_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::PrivateUpdate => {
                self.private_update_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::Fill => {
                self.fill_monotonic_ns = Some(mark.monotonic_ns);
            }
            LatencyMarkKind::Reconcile => {
                self.reconcile_monotonic_ns = Some(mark.monotonic_ns);
            }
        }
    }

    pub fn duration_ms(&self, from: LatencyMarkKind, to: LatencyMarkKind) -> Option<f64> {
        let from = self.mark_value(from)?;
        let to = self.mark_value(to)?;
        (to >= from).then_some((to - from) as f64 / 1_000_000.0)
    }

    pub fn market_to_decision_ms(&self) -> Option<f64> {
        self.duration_ms(LatencyMarkKind::MarketReceive, LatencyMarkKind::Decision)
    }

    pub fn decision_to_submit_ms(&self) -> Option<f64> {
        self.duration_ms(LatencyMarkKind::Decision, LatencyMarkKind::Submit)
    }

    pub fn submit_to_ack_ms(&self) -> Option<f64> {
        self.duration_ms(LatencyMarkKind::Submit, LatencyMarkKind::Ack)
    }

    pub fn submit_to_private_update_ms(&self) -> Option<f64> {
        self.duration_ms(LatencyMarkKind::Submit, LatencyMarkKind::PrivateUpdate)
    }

    pub fn submit_to_fill_ms(&self) -> Option<f64> {
        self.duration_ms(LatencyMarkKind::Submit, LatencyMarkKind::Fill)
    }

    pub fn fill_to_reconcile_ms(&self) -> Option<f64> {
        self.duration_ms(LatencyMarkKind::Fill, LatencyMarkKind::Reconcile)
    }

    fn mark_value(&self, kind: LatencyMarkKind) -> Option<u128> {
        match kind {
            LatencyMarkKind::MarketReceive => self.market_receive_monotonic_ns,
            LatencyMarkKind::StrategyReceive => self.strategy_receive_monotonic_ns,
            LatencyMarkKind::Decision => self.decision_monotonic_ns,
            LatencyMarkKind::RiskPass => self.risk_pass_monotonic_ns,
            LatencyMarkKind::Submit => self.submit_monotonic_ns,
            LatencyMarkKind::Ack => self.ack_monotonic_ns,
            LatencyMarkKind::PrivateUpdate => self.private_update_monotonic_ns,
            LatencyMarkKind::Fill => self.fill_monotonic_ns,
            LatencyMarkKind::Reconcile => self.reconcile_monotonic_ns,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyClock {
    origin: Instant,
}

impl LatencyClock {
    pub fn start() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    pub fn mark(&self, kind: LatencyMarkKind) -> LatencyMark {
        LatencyMark::new(kind, self.origin.elapsed().as_nanos())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyEndpointKind {
    PublicWs,
    PrivateWs,
    RestOrderAck,
    CancelAck,
    Resync,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LatencySeriesKey {
    pub region: String,
    pub exchange: String,
    pub endpoint: LatencyEndpointKind,
}

impl LatencySeriesKey {
    pub fn new(
        region: impl Into<String>,
        exchange: impl Into<String>,
        endpoint: LatencyEndpointKind,
    ) -> Self {
        Self {
            region: region.into(),
            exchange: exchange.into(),
            endpoint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub schema_version: u16,
    pub sample_count: usize,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub p999_ms: Option<f64>,
    pub max_ms: Option<f64>,
}

impl LatencySummary {
    pub fn from_samples(samples_ms: &[f64]) -> Self {
        let mut samples = samples_ms
            .iter()
            .copied()
            .filter(|sample| sample.is_finite() && *sample >= 0.0)
            .collect::<Vec<_>>();
        samples.sort_by(|left, right| {
            left.partial_cmp(right)
                .expect("finite latency samples should compare")
        });
        Self {
            schema_version: LATENCY_SCHEMA_VERSION,
            sample_count: samples.len(),
            p50_ms: percentile(&samples, 0.50),
            p95_ms: percentile(&samples, 0.95),
            p99_ms: percentile(&samples, 0.99),
            p999_ms: percentile(&samples, 0.999),
            max_ms: samples.last().copied(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyMatrixRow {
    pub schema_version: u16,
    pub key: LatencySeriesKey,
    pub summary: LatencySummary,
}

impl LatencyMatrixRow {
    pub fn new(key: LatencySeriesKey, samples_ms: &[f64]) -> Self {
        Self {
            schema_version: LATENCY_SCHEMA_VERSION,
            key,
            summary: LatencySummary::from_samples(samples_ms),
        }
    }
}

fn percentile(sorted_samples: &[f64], percentile: f64) -> Option<f64> {
    if sorted_samples.is_empty() {
        return None;
    }
    let rank = ((sorted_samples.len() - 1) as f64 * percentile).ceil() as usize;
    sorted_samples.get(rank).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_marks_should_chain_opportunity_lifecycle() {
        let mut trace = OpportunityLatencyTrace::new();
        for (kind, monotonic_ns) in [
            (LatencyMarkKind::MarketReceive, 1_000_000),
            (LatencyMarkKind::StrategyReceive, 1_500_000),
            (LatencyMarkKind::Decision, 2_000_000),
            (LatencyMarkKind::RiskPass, 2_500_000),
            (LatencyMarkKind::Submit, 4_000_000),
            (LatencyMarkKind::Ack, 9_000_000),
            (LatencyMarkKind::PrivateUpdate, 11_000_000),
            (LatencyMarkKind::Fill, 13_000_000),
            (LatencyMarkKind::Reconcile, 20_000_000),
        ] {
            trace.record_mark(LatencyMark::new(kind, monotonic_ns));
        }

        assert_eq!(trace.market_to_decision_ms(), Some(1.0));
        assert_eq!(trace.decision_to_submit_ms(), Some(2.0));
        assert_eq!(trace.submit_to_ack_ms(), Some(5.0));
        assert_eq!(trace.submit_to_private_update_ms(), Some(7.0));
        assert_eq!(trace.submit_to_fill_ms(), Some(9.0));
        assert_eq!(trace.fill_to_reconcile_ms(), Some(7.0));
    }

    #[test]
    fn latency_clock_should_emit_in_process_monotonic_marks() {
        let clock = LatencyClock::start();
        let first = clock.mark(LatencyMarkKind::MarketReceive);
        let second = clock.mark(LatencyMarkKind::Decision);

        assert_eq!(first.kind, LatencyMarkKind::MarketReceive);
        assert_eq!(second.kind, LatencyMarkKind::Decision);
        assert!(second.monotonic_ns >= first.monotonic_ns);
    }

    #[test]
    fn latency_summary_should_expose_tail_percentiles() {
        let samples = (1..=1000).map(|value| value as f64).collect::<Vec<_>>();
        let summary = LatencySummary::from_samples(&samples);

        assert_eq!(summary.sample_count, 1000);
        assert_eq!(summary.p50_ms, Some(501.0));
        assert_eq!(summary.p95_ms, Some(951.0));
        assert_eq!(summary.p99_ms, Some(991.0));
        assert_eq!(summary.p999_ms, Some(1000.0));
        assert_eq!(summary.max_ms, Some(1000.0));
    }

    #[test]
    fn latency_matrix_row_should_keep_region_exchange_endpoint_dimensions() {
        let row = LatencyMatrixRow::new(
            LatencySeriesKey::new("tokyo", "binance", LatencyEndpointKind::PublicWs),
            &[10.0, 20.0, 30.0],
        );

        assert_eq!(row.key.region, "tokyo");
        assert_eq!(row.key.exchange, "binance");
        assert_eq!(row.key.endpoint, LatencyEndpointKind::PublicWs);
        assert_eq!(row.summary.p95_ms, Some(30.0));
    }
}
