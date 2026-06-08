use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestSpec {
    pub exchange: String,
    pub operation: String,
    #[serde(default)]
    pub product: Option<String>,
    #[serde(default)]
    pub transport: Option<String>,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub auth: Option<RequestAuth>,
    #[serde(default)]
    pub query: BTreeMap<String, FieldExpectation>,
    #[serde(default)]
    pub headers: BTreeMap<String, FieldExpectation>,
    #[serde(default)]
    pub body: Option<Value>,
    #[serde(default)]
    pub body_contains: Option<Value>,
    #[serde(default)]
    pub strict_query: bool,
    #[serde(default)]
    pub strict_headers: bool,
    #[serde(default)]
    pub strict_body: bool,
    #[serde(default)]
    pub forbid_query_values_containing: Vec<String>,
    #[serde(default)]
    pub forbid_header_values_containing: Vec<String>,
    #[serde(default)]
    pub signing_vector: Option<String>,
}

impl Default for RequestSpec {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            operation: String::new(),
            product: None,
            transport: None,
            method: String::new(),
            path: String::new(),
            auth: None,
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            body: None,
            body_contains: None,
            strict_query: false,
            strict_headers: false,
            strict_body: false,
            forbid_query_values_containing: Vec::new(),
            forbid_header_values_containing: Vec::new(),
            signing_vector: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestAuth {
    None,
    ApiKey,
    Signed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldExpectation {
    #[serde(rename = "match", default)]
    pub match_kind: FieldMatch,
    #[serde(default)]
    pub value: Option<String>,
}

impl<'de> Deserialize<'de> for FieldExpectation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum FieldExpectationWire {
            ExactString(String),
            Object {
                #[serde(rename = "match", default)]
                match_kind: FieldMatch,
                #[serde(default)]
                value: Option<String>,
            },
        }

        match FieldExpectationWire::deserialize(deserializer)? {
            FieldExpectationWire::ExactString(value) => Ok(Self {
                match_kind: FieldMatch::Exact,
                value: Some(value),
            }),
            FieldExpectationWire::Object { match_kind, value } => Ok(Self { match_kind, value }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FieldMatch {
    #[default]
    Exact,
    Present,
    NonEmpty,
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActualHttpRequest {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}

impl ActualHttpRequest {
    pub fn new(method: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            ..Self::default()
        }
    }

    pub fn with_query(mut self, query: impl IntoIterator<Item = (String, String)>) -> Self {
        self.query = query.into_iter().collect();
        self
    }

    pub fn with_headers(mut self, headers: impl IntoIterator<Item = (String, String)>) -> Self {
        self.headers = headers
            .into_iter()
            .map(|(key, value)| (normalize_header_name(&key), value))
            .collect();
        self
    }

    pub fn with_body(mut self, body: Option<Value>) -> Self {
        self.body = body;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestSpecError {
    pub operation: String,
    pub mismatches: Vec<String>,
}

impl fmt::Display for RequestSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} request spec mismatch: {}",
            self.operation,
            self.mismatches.join("; ")
        )
    }
}

impl std::error::Error for RequestSpecError {}

impl RequestSpec {
    pub fn assert_matches(&self, actual: &ActualHttpRequest) -> Result<(), RequestSpecError> {
        let mut mismatches = Vec::new();

        if !self.method.eq_ignore_ascii_case(&actual.method) {
            mismatches.push(format!(
                "method expected {} got {}",
                self.method, actual.method
            ));
        }
        if self.path != actual.path {
            mismatches.push(format!("path expected {} got {}", self.path, actual.path));
        }

        compare_fields("query", &self.query, &actual.query, &mut mismatches);
        if self.strict_query {
            compare_extra_fields("query", &self.query, &actual.query, &mut mismatches);
        }
        let expected_headers = self
            .headers
            .iter()
            .map(|(key, value)| (normalize_header_name(key), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let actual_headers = actual
            .headers
            .iter()
            .map(|(key, value)| (normalize_header_name(key), value.clone()))
            .collect::<BTreeMap<_, _>>();
        compare_fields(
            "header",
            &expected_headers,
            &actual_headers,
            &mut mismatches,
        );
        if self.strict_headers {
            compare_extra_fields(
                "header",
                &expected_headers,
                &actual_headers,
                &mut mismatches,
            );
        }

        if let Some(expected_body) = &self.body {
            match actual.body.as_ref() {
                Some(actual_body) if expected_body == actual_body => {}
                Some(actual_body) => mismatches.push(format!(
                    "body expected {} got {}",
                    expected_body, actual_body
                )),
                None => mismatches.push("body expected JSON but was absent".to_string()),
            }
        } else if self.strict_body && actual.body.is_some() {
            mismatches.push("body expected absent but got JSON".to_string());
        }
        if let Some(expected_subset) = &self.body_contains {
            match actual.body.as_ref() {
                Some(actual_body) if json_contains(actual_body, expected_subset) => {}
                Some(actual_body) => mismatches.push(format!(
                    "body did not contain expected subset {} in {}",
                    expected_subset, actual_body
                )),
                None => {
                    mismatches.push("body subset expected JSON but body was absent".to_string())
                }
            }
        }

        for forbidden in &self.forbid_query_values_containing {
            if actual
                .query
                .values()
                .any(|value| value.contains(forbidden.as_str()))
            {
                mismatches.push(format!(
                    "query value contained forbidden fragment {forbidden:?}"
                ));
            }
        }
        for forbidden in &self.forbid_header_values_containing {
            if actual
                .headers
                .values()
                .any(|value| value.contains(forbidden.as_str()))
            {
                mismatches.push(format!(
                    "header value contained forbidden fragment {forbidden:?}"
                ));
            }
        }

        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(RequestSpecError {
                operation: self.operation.clone(),
                mismatches,
            })
        }
    }
}

fn compare_fields(
    scope: &str,
    expected: &BTreeMap<String, FieldExpectation>,
    actual: &BTreeMap<String, String>,
    mismatches: &mut Vec<String>,
) {
    for (key, expectation) in expected {
        let actual_value = actual.get(key);
        match expectation.match_kind {
            FieldMatch::Exact => match (expectation.value.as_deref(), actual_value) {
                (Some(expected_value), Some(actual_value)) if expected_value == actual_value => {}
                (Some(expected_value), Some(actual_value)) => mismatches.push(format!(
                    "{scope} {key} expected {expected_value:?} got {actual_value:?}"
                )),
                (Some(expected_value), None) => mismatches.push(format!(
                    "{scope} {key} expected {expected_value:?} but was absent"
                )),
                (None, Some(_)) => {}
                (None, None) => {
                    mismatches.push(format!("{scope} {key} expected exact field but was absent"))
                }
            },
            FieldMatch::Present => {
                if actual_value.is_none() {
                    mismatches.push(format!("{scope} {key} expected present but was absent"));
                }
            }
            FieldMatch::NonEmpty => match actual_value {
                Some(value) if !value.is_empty() => {}
                Some(_) => mismatches.push(format!("{scope} {key} expected non-empty value")),
                None => mismatches.push(format!("{scope} {key} expected present but was absent")),
            },
            FieldMatch::Absent => {
                if let Some(value) = actual_value {
                    mismatches.push(format!("{scope} {key} expected absent but got {value:?}"));
                }
            }
        }
    }
}

fn compare_extra_fields(
    scope: &str,
    expected: &BTreeMap<String, FieldExpectation>,
    actual: &BTreeMap<String, String>,
    mismatches: &mut Vec<String>,
) {
    for key in actual.keys() {
        if !expected.contains_key(key) {
            mismatches.push(format!("{scope} {key} was not declared in strict spec"));
        }
    }
}

fn normalize_header_name(key: &str) -> String {
    key.to_ascii_lowercase()
}

fn json_contains(actual: &Value, expected_subset: &Value) -> bool {
    match (actual, expected_subset) {
        (Value::Object(actual), Value::Object(expected)) => {
            expected.iter().all(|(key, expected)| {
                actual
                    .get(key)
                    .is_some_and(|actual| json_contains(actual, expected))
            })
        }
        (Value::Array(actual), Value::Array(expected)) => {
            expected.len() <= actual.len()
                && expected
                    .iter()
                    .enumerate()
                    .all(|(index, expected)| json_contains(&actual[index], expected))
        }
        _ => actual == expected_subset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_spec_should_match_query_headers_and_body_subset() {
        let spec = RequestSpec {
            exchange: "okx".to_string(),
            operation: "place_order".to_string(),
            product: Some("spot".to_string()),
            transport: Some("rest".to_string()),
            method: "POST".to_string(),
            path: "/api/v5/trade/order".to_string(),
            auth: Some(RequestAuth::Signed),
            query: BTreeMap::new(),
            headers: BTreeMap::from([
                (
                    "OK-ACCESS-KEY".to_string(),
                    FieldExpectation {
                        match_kind: FieldMatch::Exact,
                        value: Some("key".to_string()),
                    },
                ),
                (
                    "OK-ACCESS-SIGN".to_string(),
                    FieldExpectation {
                        match_kind: FieldMatch::NonEmpty,
                        value: None,
                    },
                ),
            ]),
            body: None,
            body_contains: Some(serde_json::json!({
                "instId": "BTC-USDT",
                "side": "buy"
            })),
            strict_query: false,
            strict_headers: false,
            strict_body: false,
            forbid_query_values_containing: Vec::new(),
            forbid_header_values_containing: vec!["secret".to_string()],
            signing_vector: None,
        };
        let actual = ActualHttpRequest::new("POST", "/api/v5/trade/order")
            .with_headers([
                ("ok-access-key".to_string(), "key".to_string()),
                ("ok-access-sign".to_string(), "signature".to_string()),
            ])
            .with_body(Some(serde_json::json!({
                "instId": "BTC-USDT",
                "side": "buy",
                "sz": "0.02"
            })));

        spec.assert_matches(&actual).expect("request matches");
    }

    #[test]
    fn request_spec_should_report_mismatches() {
        let spec = RequestSpec {
            exchange: "binance".to_string(),
            operation: "query_order".to_string(),
            product: Some("spot".to_string()),
            transport: Some("rest".to_string()),
            method: "GET".to_string(),
            path: "/api/v3/order".to_string(),
            auth: Some(RequestAuth::Signed),
            query: BTreeMap::from([(
                "signature".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::Present,
                    value: None,
                },
            )]),
            headers: BTreeMap::new(),
            body: None,
            body_contains: None,
            strict_query: false,
            strict_headers: false,
            strict_body: false,
            forbid_query_values_containing: Vec::new(),
            forbid_header_values_containing: Vec::new(),
            signing_vector: None,
        };
        let actual = ActualHttpRequest::new("POST", "/api/v3/order");

        let error = spec.assert_matches(&actual).expect_err("mismatch");
        assert!(error.to_string().contains("method expected GET"));
        assert!(error
            .to_string()
            .contains("query signature expected present"));
    }
}
