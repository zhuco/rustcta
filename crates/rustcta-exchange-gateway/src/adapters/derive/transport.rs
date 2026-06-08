#![cfg_attr(not(test), allow(dead_code))]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeriveJsonRpcRoute {
    pub method: &'static str,
    pub auth: &'static str,
}

pub const PRIVATE_ORDER_ROUTE: DeriveJsonRpcRoute = DeriveJsonRpcRoute {
    method: "private/order",
    auth: "session_key",
};
