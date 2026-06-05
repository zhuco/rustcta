//! Opportunity recording for detection-only cross-exchange arbitrage.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use super::config::DetectionRecorderConfig;
use super::types::{DetectionMetrics, OpportunityRecord};

#[derive(Debug)]
pub struct OpportunityRecorder {
    config: DetectionRecorderConfig,
    records: Vec<OpportunityRecord>,
    metrics: DetectionMetrics,
}

impl OpportunityRecorder {
    pub fn new(config: DetectionRecorderConfig) -> Self {
        Self {
            config,
            records: Vec::new(),
            metrics: DetectionMetrics::default(),
        }
    }

    pub fn record(&mut self, opportunity: OpportunityRecord) -> std::io::Result<()> {
        self.metrics.observe_opportunity(&opportunity);
        if self.config.enabled {
            self.append_jsonl(&opportunity)?;
        }
        self.records.push(opportunity);
        Ok(())
    }

    pub fn metrics(&self) -> &DetectionMetrics {
        &self.metrics
    }

    pub fn records(&self) -> &[OpportunityRecord] {
        &self.records
    }

    fn append_jsonl(&self, opportunity: &OpportunityRecord) -> std::io::Result<()> {
        let path = PathBuf::from(&self.config.jsonl_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let line = serde_json::to_string(opportunity)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        writeln!(file, "{line}")?;
        Ok(())
    }
}
