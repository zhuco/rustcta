#![cfg_attr(not(test), allow(dead_code))]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParadexRestRoute {
    pub method: &'static str,
    pub path: &'static str,
    pub auth: &'static str,
}

pub const AUTH_ROUTE: ParadexRestRoute = ParadexRestRoute {
    method: "POST",
    path: "/auth",
    auth: "starknet_headers",
};
