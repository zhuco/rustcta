use axum::http::HeaderMap;

use super::models::MonitoringConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDecision {
    Allowed,
    Unauthorized,
}

pub fn authorize(headers: &HeaderMap, config: &MonitoringConfig) -> AuthDecision {
    if !config.require_token {
        return AuthDecision::Allowed;
    }
    let Ok(expected) = std::env::var(&config.token_env) else {
        return AuthDecision::Unauthorized;
    };
    if expected.is_empty() {
        return AuthDecision::Unauthorized;
    }
    let Some(header) = headers.get(axum::http::header::AUTHORIZATION) else {
        return AuthDecision::Unauthorized;
    };
    let Ok(value) = header.to_str() else {
        return AuthDecision::Unauthorized;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return AuthDecision::Unauthorized;
    };
    if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        AuthDecision::Allowed
    } else {
        AuthDecision::Unauthorized
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}
