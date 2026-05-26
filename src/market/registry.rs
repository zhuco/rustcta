use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::{ExchangeId, MarketCapabilities, MarketDataAdapter};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAdapterMetadata {
    pub exchange: ExchangeId,
    pub name: String,
    pub capabilities: MarketCapabilities,
    pub protocol_notes: Vec<String>,
}

impl MarketAdapterMetadata {
    pub fn new(
        exchange: ExchangeId,
        name: impl Into<String>,
        capabilities: MarketCapabilities,
    ) -> Self {
        Self {
            exchange,
            name: name.into(),
            capabilities,
            protocol_notes: Vec::new(),
        }
    }

    pub fn with_protocol_notes(
        mut self,
        notes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.protocol_notes = notes.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Default)]
pub struct MarketAdapterRegistry {
    metadata: HashMap<ExchangeId, MarketAdapterMetadata>,
    adapters: HashMap<ExchangeId, Arc<dyn MarketDataAdapter>>,
}

impl MarketAdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_metadata(&mut self, metadata: MarketAdapterMetadata) {
        self.metadata.insert(metadata.exchange.clone(), metadata);
    }

    pub fn register_adapter(
        &mut self,
        name: impl Into<String>,
        adapter: Arc<dyn MarketDataAdapter>,
    ) {
        let exchange = adapter.exchange();
        let metadata = MarketAdapterMetadata::new(exchange.clone(), name, adapter.capabilities());
        self.metadata.insert(exchange.clone(), metadata);
        self.adapters.insert(exchange, adapter);
    }

    pub fn metadata(&self, exchange: &ExchangeId) -> Option<&MarketAdapterMetadata> {
        self.metadata.get(exchange)
    }

    pub fn adapter(&self, exchange: &ExchangeId) -> Option<Arc<dyn MarketDataAdapter>> {
        self.adapters.get(exchange).cloned()
    }

    pub fn contains_exchange(&self, exchange: &ExchangeId) -> bool {
        self.metadata.contains_key(exchange)
    }

    pub fn exchanges(&self) -> Vec<ExchangeId> {
        self.metadata.keys().cloned().collect()
    }

    pub fn all_metadata(&self) -> Vec<&MarketAdapterMetadata> {
        self.metadata.values().collect()
    }

    pub fn len(&self) -> usize {
        self.metadata.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metadata.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_should_query_metadata_by_exchange() {
        let mut registry = MarketAdapterRegistry::new();
        registry.register_metadata(MarketAdapterMetadata::new(
            ExchangeId::Binance,
            "binance-usdm-market",
            MarketCapabilities::new(true, true, true, true, true),
        ));

        let info = registry
            .metadata(&ExchangeId::Binance)
            .expect("registered adapter metadata");

        assert_eq!(info.name, "binance-usdm-market");
        assert!(info.capabilities.supports_orderbook5);
        assert!(registry.contains_exchange(&ExchangeId::Binance));
        assert!(!registry.contains_exchange(&ExchangeId::Okx));
    }
}
