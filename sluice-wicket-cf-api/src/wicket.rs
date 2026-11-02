//! The wicket's app API (its console, `--console`): an app is hosts served
//! by one component in one sandbox, put and removed by its owner; its
//! component is uploaded by digest first.
//!
//! ```text
//! GET    /api/apps[?after=<revision>]          the apps, the capabilities and the keyvalue
//!                                              stores; held until they change with `after`
//! PUT    /api/components/<sha256>              keeps the body under its digest
//! PUT    /api/images                           a Firecracker wicket: makes an ext4 image of
//!                                              an OCI image, kept under its digest
//! PUT    /api/apps/<name>                      serves the app of the body
//! DELETE /api/apps/<name>?owner=<owner>        removes the app
//! DELETE /api/keyvalue/<store>/<namespace>     deletes the data of a namespace no route maps
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A PUT may compile the component: generous.
const EDIT_TIMEOUT: Duration = Duration::from_secs(120);
/// A rootfs made of an image, or uploaded (4 GiB at most): minutes, the wicket's own bound of
/// a make a little within it.
const IMAGE_TIMEOUT: Duration = Duration::from_secs(660);
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
    /// Opaque to the wicket, listed as put: the snapshot's
    /// (`crate::env::tag`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ComponentRef {
    pub sha256: String,
    pub name: String,
}

/// The sandbox: the route options in JSON, as the wicket runs apps: the `--wasm`
/// options, the keyvalue map and the env; or the `--vm` options and the env. A key left
/// out takes the wicket's default; a listing names every one the wicket has, the env by
/// the names only.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Sandbox {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wasi: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instances: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keyvalue: BTreeMap<String, String>,
    /// A Firecracker wicket's: how a connection gets its VM.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// A Firecracker wicket's: a VM's memory in MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem: Option<u64>,
    /// A Firecracker wicket's: its pool's VMs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vms: Option<u64>,
    /// Put, never read back: the app's tag tells what the wicket serves.
    #[serde(skip_deserializing, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// What a wicket runs apps as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Runtime {
    /// wasm components: a wicket whose listing names none is one.
    Wasm,
    /// ext4 images in microVMs.
    Firecracker,
}

impl Runtime {
    /// The stack of its apps.
    pub fn stack(self) -> &'static str {
        match self {
            Runtime::Wasm => crate::store::STACK,
            Runtime::Firecracker => crate::store::ROOTFS_STACK,
        }
    }
}

/// An image the wicket made of an OCI image: what an app names.
#[derive(Deserialize, Debug)]
pub struct MadeImage {
    /// The reference, pinned to its manifest's digest.
    pub image: String,
    pub name: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Listing {
    pub revision: u64,
    pub generation: u64,
    /// `firecracker`, or none from a wasm wicket.
    #[serde(default)]
    pub runtime: Option<String>,
    /// What a sandbox may hold: the capabilities, in the wicket's order.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub keyvalue_stores: Vec<ListedStore>,
    pub apps: Vec<ListedApp>,
}

impl Listing {
    pub fn runtime(&self) -> Runtime {
        match self.runtime.as_deref() {
            Some("firecracker") => Runtime::Firecracker,
            _ => Runtime::Wasm,
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ListedStore {
    pub name: String,
    /// `sqlite` or `redis`.
    #[serde(rename = "type")]
    pub kind: String,
    pub default: bool,
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

impl Failure {
    /// Whether a removal the wicket answered so is to be tried again: anything but another
    /// owner's app, which stays that way.
    pub fn retried(&self) -> bool {
        !matches!(self, Failure::Refused { status: 409, .. })
    }
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

    /// Uploads the component in `file` as it is read: it may be larger than this process's
    /// memory.
    pub async fn upload(&self, sha256: &str, file: tokio::fs::File) -> Result<(), Failure> {
        let bytes = file
            .metadata()
            .await
            .map_err(|e| Failure::Unreachable(format!("the component: {e}")))?
            .len();
        let request = self
            .client
            .put(format!("{}/api/components/{sha256}", self.base))
            .timeout(IMAGE_TIMEOUT)
            .header("content-type", "application/wasm")
            .header("content-length", bytes)
            .body(file);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Has the wicket make an ext4 image of the OCI image `image`, with the
    /// `(username, password)` of a private one.
    pub async fn make_image(
        &self,
        image: &str,
        credentials: Option<(&str, &str)>,
    ) -> Result<MadeImage, Failure> {
        let mut body = serde_json::json!({"image": image});
        if let Some((username, password)) = credentials {
            body["username"] = username.into();
            body["password"] = password.into();
        }
        let request = self
            .client
            .put(format!("{}/api/images", self.base))
            .timeout(IMAGE_TIMEOUT)
            .json(&body);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body).map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
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

    /// Deletes the data of the keyvalue namespace: how many keys it held.
    /// Refused (409) while a route maps it.
    pub async fn purge(&self, store: &str, namespace: &str) -> Result<u64, Failure> {
        // no space in a store's name nor in `cf/<guid>`: form encoding is
        // path encoding here
        let place: String =
            form_urlencoded::byte_serialize(format!("{store}/{namespace}").as_bytes()).collect();
        let request = self
            .client
            .delete(format!("{}/api/keyvalue/{place}", self.base))
            .timeout(EDIT_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        #[derive(Deserialize)]
        struct Purged {
            keys: u64,
        }
        serde_json::from_slice::<Purged>(&body)
            .map(|purged| purged.keys)
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
