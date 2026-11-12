//! The Open Service Broker API (2.17), as a platform speaks it to the brokers
//! registered (`crate::brokers`): the catalog, checked as Cloud Controller
//! checks it; an instance's provision, update and deprovision, a binding's
//! bind and unbind, each with `accepts_incomplete=true`; the polling of their
//! last operation; the fetch of an instance or a binding.
//!
//! Every request has the broker's basic authentication, the API version, an
//! identity of its own and, when a user asked for it, the user's
//! (`X-Broker-API-Originating-Identity`). It takes [`TIMEOUT`] at most, as on
//! CC; no redirect is followed.

use std::time::Duration;

use base64::Engine as _;
use http::{HeaderMap, Method};
use serde_json::{Map, Value, json};

use crate::store::{Broker, OperationState};

/// The version of the API in `X-Broker-API-Version`.
pub const API_VERSION: &str = "2.17";
/// How long a request to a broker takes at most: CC's
/// `broker_client_timeout_seconds`.
pub const TIMEOUT: Duration = Duration::from_secs(60);
/// The largest catalog.
const MAX_CATALOG: usize = 16 << 20;
/// The largest body of any other answer: a binding's credentials among them.
const MAX_BODY: usize = 1 << 20;
/// The longest `operation`, as the specification bounds it.
const MAX_OPERATION: usize = 10_000;
/// The most of a body not understood an error quotes.
const QUOTED: usize = 512;

/// Where a broker is and how it is authenticated.
#[derive(Clone)]
pub struct Endpoint {
    pub url: String,
    pub username: String,
    pub password: String,
}

impl From<&Broker> for Endpoint {
    fn from(broker: &Broker) -> Endpoint {
        Endpoint {
            url: broker.url.clone(),
            username: broker.username.clone(),
            password: broker.password.clone(),
        }
    }
}

/// What the broker answered of a provision, an update, a deprovision, a
/// bind or an unbind.
#[derive(Debug, PartialEq)]
pub enum Answer {
    /// 200 or 201: done, the body said.
    Done(Map<String, Value>),
    /// 202: under way, polled with its `operation`.
    Accepted {
        operation: Option<String>,
        body: Map<String, Value>,
    },
    /// 410: there is none (a deprovision's or an unbind's success).
    Gone,
}

/// Why a request failed.
#[derive(Debug, PartialEq)]
pub struct Refusal {
    /// As the user reads it.
    pub description: String,
    /// The status the broker answered with; None: it answered none.
    pub status: Option<u16>,
    /// Whether the broker may have done it all the same (no answer in
    /// time, a 5xx, a body not understood): a provision or a bind is to be
    /// undone (orphan mitigation).
    pub unknown: bool,
}

impl Refusal {
    fn invalid(what: impl std::fmt::Display) -> Refusal {
        Refusal {
            description: format!("The service broker returned an invalid response: {what}"),
            status: None,
            unknown: true,
        }
    }
}

/// What a poll of a last operation answered.
#[derive(Debug, PartialEq)]
pub struct Polled {
    /// None: 410, the resource is gone.
    pub state: Option<OperationState>,
    pub description: Option<String>,
    /// The broker's `Retry-After`.
    pub retry_after: Option<Duration>,
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
}

impl Client {
    pub fn new() -> Result<Client, String> {
        let http = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("the brokers' client: {e}"))?;
        Ok(Client { http })
    }

    /// The broker's catalog, checked ([`check_catalog`]): each offering
    /// less its plans, and its plans.
    pub async fn catalog(&self, broker: &Endpoint) -> Result<Vec<CatalogOffering>, String> {
        let (status, _, body) = self
            .send(
                broker,
                Method::GET,
                "/v2/catalog",
                &[],
                None,
                None,
                MAX_CATALOG,
            )
            .await
            .map_err(|refusal| refusal.description)?;
        if status != 200 {
            return Err(failed(status, &body).description);
        }
        let catalog: Value = serde_json::from_slice(&body).map_err(|e| {
            format!("The service broker returned an invalid response: the catalog is no JSON: {e}")
        })?;
        check_catalog(&catalog)
    }

    /// A provision, an update, a deprovision, a bind or an unbind: `path`
    /// with `query` (`accepts_incomplete=true` added), `body` as JSON.
    pub async fn request(
        &self,
        broker: &Endpoint,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        user: Option<&str>,
    ) -> Result<Answer, Refusal> {
        let mut query = query.to_vec();
        query.push(("accepts_incomplete", "true"));
        let (status, _, bytes) = self
            .send(broker, method, path, &query, body, user, MAX_BODY)
            .await?;
        match status {
            200 | 201 | 204 => object(&bytes).map(Answer::Done),
            202 => {
                let body = object(&bytes)?;
                let operation = match body.get("operation") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(operation)) if operation.len() <= MAX_OPERATION => {
                        Some(operation.clone())
                    }
                    Some(_) => {
                        return Err(Refusal::invalid(format!(
                            "operation is a string of {MAX_OPERATION} characters at most"
                        )));
                    }
                };
                Ok(Answer::Accepted { operation, body })
            }
            410 => Ok(Answer::Gone),
            _ => Err(failed(status, &bytes)),
        }
    }

    /// The last operation at `path` (`.../last_operation`).
    pub async fn poll(
        &self,
        broker: &Endpoint,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<Polled, Refusal> {
        let (status, headers, bytes) = self
            .send(broker, Method::GET, path, query, None, None, MAX_BODY)
            .await?;
        let retry_after = headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        match status {
            200 => {
                let body = object(&bytes)?;
                let state = match body.get("state").and_then(Value::as_str) {
                    Some("in progress") => OperationState::InProgress,
                    Some("succeeded") => OperationState::Succeeded,
                    Some("failed") => OperationState::Failed,
                    other => {
                        return Err(Refusal::invalid(format!(
                            "state is in progress, succeeded or failed, not {}",
                            other.map_or("absent".into(), |state| format!("'{state}'"))
                        )));
                    }
                };
                Ok(Polled {
                    state: Some(state),
                    description: body
                        .get("description")
                        .and_then(Value::as_str)
                        .map(String::from),
                    retry_after,
                })
            }
            410 => Ok(Polled {
                state: None,
                description: None,
                retry_after,
            }),
            _ => Err(failed(status, &bytes)),
        }
    }

    /// An instance or a binding as the broker has it (`GET path`).
    pub async fn fetch(
        &self,
        broker: &Endpoint,
        path: &str,
    ) -> Result<Map<String, Value>, Refusal> {
        let (status, _, bytes) = self
            .send(broker, Method::GET, path, &[], None, None, MAX_BODY)
            .await?;
        match status {
            200 => object(&bytes),
            _ => Err(failed(status, &bytes)),
        }
    }

    /// Sends a request: the status, the headers and the body, `limit`
    /// bytes at most.
    #[allow(clippy::too_many_arguments)]
    async fn send(
        &self,
        broker: &Endpoint,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        user: Option<&str>,
        limit: usize,
    ) -> Result<(u16, HeaderMap, Vec<u8>), Refusal> {
        let mut url = format!("{}{path}", broker.url.trim_end_matches('/'));
        let named = format!("{method} {url}");
        if !query.is_empty() {
            let encoded = form_urlencoded::Serializer::new(String::new())
                .extend_pairs(query)
                .finish();
            url = format!("{url}?{encoded}");
        }
        let mut request = self
            .http
            .request(method, &url)
            .basic_auth(&broker.username, Some(&broker.password))
            .header("X-Broker-API-Version", API_VERSION)
            .header("X-Broker-API-Request-Identity", crate::store::guid());
        if let Some(user) = user {
            let identity = base64::engine::general_purpose::STANDARD
                .encode(json!({"user_id": user}).to_string());
            request = request.header(
                "X-Broker-API-Originating-Identity",
                format!("cloudfoundry {identity}"),
            );
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let unreached = |e: reqwest::Error| {
            if e.is_timeout() {
                Refusal {
                    description: format!("The request to the service broker timed out: {named}"),
                    status: None,
                    unknown: true,
                }
            } else {
                Refusal {
                    description: format!(
                        "The service broker could not be reached: {named}: {}",
                        cause(&e)
                    ),
                    status: None,
                    // refused before it was sent, the broker did nothing
                    unknown: !e.is_connect(),
                }
            }
        };
        let mut response = request.send().await.map_err(unreached)?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(unreached)? {
            if bytes.len() + chunk.len() > limit {
                return Err(Refusal::invalid(format!(
                    "{named}: the body is larger than {} KiB",
                    limit >> 10
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok((status, headers, bytes))
    }
}

/// The innermost cause of `e`: reqwest's own says little.
fn cause(e: &(dyn std::error::Error + 'static)) -> String {
    let mut inner = e;
    while let Some(source) = inner.source() {
        inner = source;
    }
    inner.to_string()
}

/// A body that is a JSON object; an empty one is an empty object.
fn object(bytes: &[u8]) -> Result<Map<String, Value>, Refusal> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(Map::new());
    }
    match serde_json::from_slice(bytes) {
        Ok(Value::Object(body)) => Ok(body),
        _ => Err(Refusal::invalid(format!(
            "expected a JSON object in the body, got {}",
            quoted(bytes)
        ))),
    }
}

/// An answer of a status that is no success: the broker's `description`
/// when it gives one. A 5xx or a 408 leaves its outcome unknown.
fn failed(status: u16, bytes: &[u8]) -> Refusal {
    let described = serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|body| {
            let description = body.get("description").and_then(Value::as_str);
            let error = body.get("error").and_then(Value::as_str);
            match (error, description) {
                (Some(error), Some(description)) => Some(format!("{error}: {description}")),
                (None, Some(text)) | (Some(text), None) => Some(text.to_string()),
                (None, None) => None,
            }
        });
    Refusal {
        description: match described {
            Some(description) => {
                format!("Service broker error (status {status}): {description}")
            }
            None => format!(
                "The service broker returned an invalid response: status {status}, body {}",
                quoted(bytes)
            ),
        },
        status: Some(status),
        unknown: status >= 500 || status == 408,
    }
}

fn quoted(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut end = text.len().min(QUOTED);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let ellipsis = if end < text.len() { "..." } else { "" };
    format!("{:?}{ellipsis}", &text[..end])
}

/// An offering of a catalog, less its plans, and its plans: the objects as
/// the broker gave them.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogOffering {
    pub offering: Map<String, Value>,
    pub plans: Vec<Map<String, Value>>,
}

/// Checks a catalog as CC does: each offering has an `id`, a `name`, a
/// `description`, `bindable` and plans, unique ids and names; each plan an
/// `id`, a `name` and a `description`, unique ids, names unique in its
/// offering; the optional fields of their types. Every problem is listed.
pub fn check_catalog(catalog: &Value) -> Result<Vec<CatalogOffering>, String> {
    let Some(services) = catalog.get("services").and_then(Value::as_array) else {
        return Err("Service broker catalog is invalid: services is an array".into());
    };
    let mut problems = Vec::new();
    let mut offerings = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    let mut plan_ids = std::collections::BTreeSet::new();
    for (at, service) in services.iter().enumerate() {
        let Some(service) = service.as_object() else {
            problems.push(format!("service #{}: an object", at + 1));
            continue;
        };
        let named = match service.get("name").and_then(Value::as_str) {
            Some(name) if !name.is_empty() => format!("service {name}"),
            _ => format!("service #{}", at + 1),
        };
        let mut problem = |what: String| problems.push(format!("{named}: {what}"));
        for key in ["id", "name"] {
            match service.get(key).and_then(Value::as_str) {
                Some(value) if !value.trim().is_empty() => {}
                _ => problem(format!("{key} is a string, not empty")),
            }
        }
        if !service.get("description").is_some_and(Value::is_string) {
            problem("description is a string".into());
        }
        if !service.get("bindable").is_some_and(Value::is_boolean) {
            problem("bindable is true or false".into());
        }
        if let Some(id) = service.get("id").and_then(Value::as_str)
            && !ids.insert(id.to_string())
        {
            problem(format!("the id {id} is another service's too"));
        }
        if let Some(name) = service.get("name").and_then(Value::as_str)
            && !names.insert(name.to_string())
        {
            problem(format!("the name {name} is another service's too"));
        }
        for key in [
            "plan_updateable",
            "instances_retrievable",
            "bindings_retrievable",
            "allow_context_updates",
        ] {
            if service.get(key).is_some_and(|value| !value.is_boolean()) {
                problem(format!("{key} is true or false"));
            }
        }
        for key in ["tags", "requires"] {
            if let Some(value) = service.get(key)
                && !strings(value)
            {
                problem(format!("{key} is an array of strings"));
            }
        }
        if service
            .get("metadata")
            .is_some_and(|value| !value.is_object())
        {
            problem("metadata is an object".into());
        }
        let plans = match service.get("plans").and_then(Value::as_array) {
            Some(plans) if !plans.is_empty() => plans.as_slice(),
            _ => {
                problem("plans is an array of one plan at least".into());
                &[]
            }
        };
        let mut plan_names = std::collections::BTreeSet::new();
        let mut checked = Vec::new();
        for (at, plan) in plans.iter().enumerate() {
            let Some(plan) = plan.as_object() else {
                problem(format!("plan #{}: an object", at + 1));
                continue;
            };
            let plan_named = match plan.get("name").and_then(Value::as_str) {
                Some(name) if !name.is_empty() => format!("plan {name}"),
                _ => format!("plan #{}", at + 1),
            };
            let mut plan_problem = |what: String| problem(format!("{plan_named}: {what}"));
            for key in ["id", "name"] {
                match plan.get(key).and_then(Value::as_str) {
                    Some(value) if !value.trim().is_empty() => {}
                    _ => plan_problem(format!("{key} is a string, not empty")),
                }
            }
            if !plan.get("description").is_some_and(Value::is_string) {
                plan_problem("description is a string".into());
            }
            if let Some(id) = plan.get("id").and_then(Value::as_str)
                && !plan_ids.insert(id.to_string())
            {
                plan_problem(format!("the id {id} is another plan's too"));
            }
            if let Some(name) = plan.get("name").and_then(Value::as_str)
                && !plan_names.insert(name.to_string())
            {
                plan_problem(format!(
                    "the name {name} is another plan's of the service too"
                ));
            }
            for key in ["free", "bindable", "plan_updateable"] {
                if plan.get(key).is_some_and(|value| !value.is_boolean()) {
                    plan_problem(format!("{key} is true or false"));
                }
            }
            for key in ["metadata", "schemas"] {
                if plan.get(key).is_some_and(|value| !value.is_object()) {
                    plan_problem(format!("{key} is an object"));
                }
            }
            if plan
                .get("maximum_polling_duration")
                .is_some_and(|value| !value.is_u64())
            {
                plan_problem("maximum_polling_duration is a number of seconds".into());
            }
            if let Some(info) = plan.get("maintenance_info")
                && !info
                    .get("version")
                    .is_some_and(|version| version.as_str().is_some_and(semantic_version))
            {
                plan_problem("maintenance_info.version is a semantic version".into());
            }
            checked.push(plan.clone());
        }
        let mut offering = service.clone();
        offering.remove("plans");
        offerings.push(CatalogOffering {
            offering,
            plans: checked,
        });
    }
    if !problems.is_empty() {
        return Err(format!(
            "Service broker catalog is invalid:\n{}",
            problems.join("\n")
        ));
    }
    Ok(offerings)
}

fn strings(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|values| values.iter().all(Value::is_string))
}

/// `MAJOR.MINOR.PATCH`, a pre-release and build metadata after.
fn semantic_version(version: &str) -> bool {
    let core = version.split(['-', '+']).next().unwrap_or_default();
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog checked lists every problem, each offering and plan named;
    /// a good one comes back as offerings less their plans, and the plans.
    #[test]
    fn a_catalog_is_checked_as_cc_checks_it() {
        let good = json!({"services": [{
            "id": "s1", "name": "db", "description": "a database", "bindable": true,
            "tags": ["sql"], "plan_updateable": true,
            "plans": [
                {"id": "p1", "name": "small", "description": "1 GB",
                 "maintenance_info": {"version": "1.2.3-rc.1+b5"}},
                {"id": "p2", "name": "large", "description": "8 GB", "free": false,
                 "maximum_polling_duration": 600},
            ],
        }]});
        let checked = check_catalog(&good).unwrap();
        assert_eq!(checked.len(), 1);
        assert_eq!(checked[0].offering["name"], "db");
        assert!(!checked[0].offering.contains_key("plans"));
        assert_eq!(checked[0].plans.len(), 2);

        let bad = json!({"services": [
            {"id": "s1", "name": "db", "description": "x", "bindable": "yes",
             "tags": [1], "plans": [
                {"id": "p1", "name": "small", "description": "x"},
                {"id": "p1", "name": "small", "description": "x", "free": "no",
                 "maintenance_info": {"version": "1.2"}},
             ]},
            {"id": "s1", "name": "db", "description": "y", "bindable": true, "plans": []},
            {"name": "", "description": "z", "bindable": true, "plans": [{"name": "p"}]},
        ]});
        let e = check_catalog(&bad).unwrap_err();
        for problem in [
            "service db: bindable is true or false",
            "service db: tags is an array of strings",
            "service db: plan small: the id p1 is another plan's too",
            "service db: plan small: the name small is another plan's of the service too",
            "service db: plan small: free is true or false",
            "service db: plan small: maintenance_info.version is a semantic version",
            "service db: the id s1 is another service's too",
            "service db: the name db is another service's too",
            "service db: plans is an array of one plan at least",
            "service #3: id is a string, not empty",
            "service #3: plan p: id is a string, not empty",
            "service #3: plan p: description is a string",
        ] {
            assert!(e.contains(problem), "{problem}: {e}");
        }
        assert!(check_catalog(&json!({})).is_err());
    }

    /// An error answered names the broker's description, or quotes the body
    /// (bounded); a 5xx and a 408 leave the outcome unknown, a 4xx not.
    #[test]
    fn a_failure_says_what_the_broker_said() {
        let refused = failed(422, br#"{"error":"ConcurrencyError","description":"busy"}"#);
        assert_eq!(
            refused.description,
            "Service broker error (status 422): ConcurrencyError: busy"
        );
        assert!(!refused.unknown);
        let broken = failed(500, "x".repeat(QUOTED * 2).as_bytes());
        assert!(broken.unknown);
        assert!(
            broken.description.ends_with("...") && broken.description.len() < QUOTED + 100,
            "{}",
            broken.description
        );
        assert!(failed(408, b"").unknown);
        assert_eq!(object(b" ").unwrap(), Map::new());
        assert!(object(b"[1]").unwrap_err().unknown);
    }
}
