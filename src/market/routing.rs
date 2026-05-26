use serde::{Deserialize, Serialize};

use super::{ExchangeId, RouteHealth, RouteStatus, RouteType};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteEndpoint {
    pub exchange: ExchangeId,
    pub route_type: RouteType,
    pub endpoint: String,
    pub priority: u16,
}

impl RouteEndpoint {
    pub fn new(
        exchange: ExchangeId,
        route_type: RouteType,
        endpoint: impl Into<String>,
        priority: u16,
    ) -> Self {
        Self {
            exchange,
            route_type,
            endpoint: endpoint.into(),
            priority,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteSet {
    pub exchange: ExchangeId,
    pub route_type: RouteType,
    pub routes: Vec<RouteHealth>,
    pub active_index: Option<usize>,
}

impl RouteSet {
    pub fn new(exchange: ExchangeId, route_type: RouteType, endpoints: Vec<String>) -> Self {
        let routes = endpoints
            .into_iter()
            .map(|endpoint| RouteHealth::new(exchange.clone(), route_type, endpoint))
            .collect::<Vec<_>>();
        let active_index = (!routes.is_empty()).then_some(0);

        Self {
            exchange,
            route_type,
            routes,
            active_index,
        }
    }

    pub fn active(&self) -> Option<&RouteHealth> {
        self.active_index.and_then(|idx| self.routes.get(idx))
    }

    pub fn active_mut(&mut self) -> Option<&mut RouteHealth> {
        self.active_index.and_then(|idx| self.routes.get_mut(idx))
    }

    pub fn failover(&mut self) -> Option<&RouteHealth> {
        let next = self
            .routes
            .iter()
            .enumerate()
            .filter(|(idx, route)| {
                Some(*idx) != self.active_index && route.status != RouteStatus::Offline
            })
            .min_by_key(|(idx, _)| *idx)
            .map(|(idx, _)| idx);

        self.active_index = next;
        self.active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_set_should_failover_to_next_non_offline_route() {
        let mut routes = RouteSet::new(
            ExchangeId::Okx,
            RouteType::MarketWs,
            vec!["primary".to_string(), "backup".to_string()],
        );
        routes.active_mut().expect("active").mark_offline("down");

        let active = routes.failover().expect("backup");

        assert_eq!(active.endpoint, "backup");
    }
}
