//! The wicket's app API (its console, `--console`): an app is hosts served
//! by one component in one sandbox, put and removed by its owner; its
//! component is uploaded by digest first.
//!
//! ```text
//! GET    /api/apps[?after=<revision>]   the apps; held until they change with `after`
//! PUT    /api/components/<sha256>       keeps the body under its digest
//! PUT    /api/apps/<name>               serves the app of the body
//! DELETE /api/apps/<name>?owner=<owner> removes the app
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A PUT may compile the component: generous.
const EDIT_TIMEOUT: Duration = Duration::from_secs(120);
/// A listing with `after` is held 25s at most.
const LIST_TIMEOUT: Duration = Duration::from_secs(40);

pub struct Wicket {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
    pub owner: String,
}

/// The body of an app's PUT, and an app as listed.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AppBody {
    pub owner: String,
    pub hosts: Vec<String>,
    pub component: ComponentRef,
    pub sandbox: Sandbox,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ComponentRef {
    pub sha256: String,
    pub name: String,
}

/// The sandbox: the `--wasm` route options in JSON, and the keyvalue map. A
/// key left out takes the wicket's default; a listing names every one.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Sandbox {
    #[serde(default)]
    pub wasi: Vec<String>,
    pub budget_ms: Option<u64>,
    pub memory_mib: Option<u64>,
    #[serde(default)]
    pub pool: u64,
    pub instances: Option<u64>,
    #[serde(default)]
    pub keyvalue: BTreeMap<String, String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Listing {
    pub revision: u64,
    pub generation: u64,
    pub apps: Vec<ListedApp>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedApp {
    pub name: String,
    #[serde(flatten)]
    pub app: AppBody,
    pub deployed: String,
}

#[derive(Deserialize, Debug)]
pub struct Edited {
    pub generation: u64,
    pub message: String,
}

/// Why the wicket did not do it.
#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    /// The wicket answered no (4xx): its reason, the host of the route that
    /// did not load, the components it has no upload of.
    Refused {
        status: u16,
        error: String,
        host: Option<String>,
        missing: Vec<String>,
    },
    /// No answer, or none to act on (5xx).
    Unreachable(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Refused { error, .. } => f.write_str(error),
            Failure::Unreachable(error) => write!(f, "the wicket is unreachable: {error}"),
        }
    }
}

#[derive(Deserialize)]
struct ErrorBody {
    error: String,
    host: Option<String>,
    #[serde(default)]
    missing: Vec<String>,
}

impl Wicket {
    pub fn new(base: &str, token: Option<String>, owner: &str) -> Wicket {
        Wicket {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("a plain client"),
            base: base.trim_end_matches('/').to_string(),
            token,
            owner: owner.to_string(),
        }
    }

    /// The apps; with `after` the current revision, once they changed (25s
    /// at most).
    pub async fn list(&self, after: Option<u64>) -> Result<Listing, Failure> {
        let url = match after {
            Some(after) => format!("{}/api/apps?after={after}", self.base),
            None => format!("{}/api/apps", self.base),
        };
        let request = self.client.get(url).timeout(LIST_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body).map_err(|e| Failure::Unreachable(format!("the listing: {e}")))
    }

    pub async fn put(&self, name: &str, app: &AppBody) -> Result<Edited, Failure> {
        let request = self
            .client
            .put(format!("{}/api/apps/{name}", self.base))
            .timeout(EDIT_TIMEOUT)
            .json(app);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body).map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    pub async fn upload(&self, sha256: &str, component: Vec<u8>) -> Result<(), Failure> {
        let request = self
            .client
            .put(format!("{}/api/components/{sha256}", self.base))
            .timeout(EDIT_TIMEOUT)
            .header("content-type", "application/wasm")
            .body(component);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Removes the app; None when the wicket has none of the name.
    pub async fn delete(&self, name: &str) -> Result<Option<Edited>, Failure> {
        let owner: String = form_urlencoded::byte_serialize(self.owner.as_bytes()).collect();
        let request = self
            .client
            .delete(format!("{}/api/apps/{name}?owner={owner}", self.base))
            .timeout(EDIT_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    async fn send(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> Result<(reqwest::StatusCode, bytes::Bytes), Failure> {
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| Failure::Unreachable(e.to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| Failure::Unreachable(e.to_string()))?;
        if status.is_server_error() {
            return Err(Failure::Unreachable(format!(
                "{status}: {}",
                error_text(&body)
            )));
        }
        Ok((status, body))
    }
}

fn refused(status: reqwest::StatusCode, body: &[u8]) -> Failure {
    match serde_json::from_slice::<ErrorBody>(body) {
        Ok(error) => Failure::Refused {
            status: status.as_u16(),
            error: error.error,
            host: error.host,
            missing: error.missing,
        },
        Err(_) => Failure::Refused {
            status: status.as_u16(),
            error: format!("{status}: {}", String::from_utf8_lossy(body)),
            host: None,
            missing: Vec::new(),
        },
    }
}

fn error_text(body: &[u8]) -> String {
    serde_json::from_slice::<ErrorBody>(body)
        .map(|error| error.error)
        .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned())
}
