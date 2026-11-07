//! The wicket's app API (its console, `--console`): an app is hosts served
//! by one component in one sandbox, put and removed by its owner; its
//! component is uploaded by digest first.
//!
//! ```text
//! GET    /api/apps[?after=<revision>]          the apps, the capabilities and the keyvalue
//!                                              stores; held until they change with `after`
//! PUT    /api/components/<sha256>              keeps the body under its digest
//! PUT    /api/files/<sha256>                   a wasm wicket: unpacks the body, a tar of an
//!                                              app's files, under its digest
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
const EDIT_TIMEOUT: Duration = Duration::from_secs(240);
/// A rootfs made of an image, or uploaded (4 GiB at most): minutes, the wicket's own bound of
/// a make a little within it.
const IMAGE_TIMEOUT: Duration = Duration::from_secs(660);
/// A listing with `after` is held 25s at most.
const LIST_TIMEOUT: Duration = Duration::from_secs(40);
/// One without is answered at once: `cf app` does not wait long to show the wicket away.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Wicket {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
    pub owner: String,
}

/// The body of an app's PUT, and an app as listed: `S` its sandbox, as a wicket of a runtime has
/// it ([`Sandbox`]), or as listed before the listing's runtime tells which.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AppBody<S = Sandbox> {
    pub owner: String,
    pub hosts: Vec<String>,
    pub component: ComponentRef,
    /// A static site's files, read-only at `/` for its component.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<FilesRef>,
    pub sandbox: S,
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FilesRef {
    pub sha256: String,
}

/// The sandbox: the route options in JSON, named as the wicket's route options are, as the
/// wicket of each runtime runs apps. A key left out takes the wicket's default; a listing names
/// the keys as [`WasmSandbox`] and [`VmSandbox`] say, the env by the names only.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Sandbox {
    Wasm(WasmSandbox),
    Vm(VmSandbox),
}

/// A wasm wicket's: the `--wasm` options, the keyvalue map and the env. Listed, every key is
/// named but `instances` and `keep-ms`, unset.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct WasmSandbox {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wasi: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    /// The instances kept warm.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm: Option<u64>,
    /// The instances alive at once at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instances: Option<u64>,
    /// How long an instance beyond `warm` serving nothing is kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_ms: Option<u64>,
    /// The requests an instance (a store) takes at once.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keyvalue: BTreeMap<String, String>,
    /// Put, never read back: the app's tag tells what the wicket serves.
    #[serde(skip_deserializing, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// A Firecracker wicket's: the `--vm` options and the env. Listed, `mode` and `memory-mib` are
/// named, the rest away from their defaults.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct VmSandbox {
    /// How a connection gets its VM.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// A VM's memory in MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    /// The VMs alive at once at most: a pool's size, the VMs booted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instances: Option<u64>,
    /// The VMs kept ahead of the connections.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm: Option<u64>,
    /// How long a VM serving nothing is kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_ms: Option<u64>,
    /// The connections a VM serves at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uses: Option<u64>,
    /// How long a VM serves at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifetime_ms: Option<u64>,
    /// The connections a pool's VM holds before the pool grows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    /// How long a VM's process may take to listen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boot_ms: Option<u64>,
    /// The destinations its VMs may connect to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress: Vec<String>,
    /// Put, never read back: the app's tag tells what the wicket serves.
    #[serde(skip_deserializing, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl AppBody<serde_json::Value> {
    /// The app as a wicket of `runtime` lists it.
    pub fn typed(self, runtime: Runtime) -> serde_json::Result<AppBody> {
        Ok(AppBody {
            owner: self.owner,
            hosts: self.hosts,
            component: self.component,
            files: self.files,
            sandbox: match runtime {
                Runtime::Wasm => Sandbox::Wasm(serde_json::from_value(self.sandbox)?),
                Runtime::Firecracker => Sandbox::Vm(serde_json::from_value(self.sandbox)?),
            },
            tag: self.tag,
        })
    }
}

#[cfg(test)]
impl Sandbox {
    pub fn wasm(&self) -> &WasmSandbox {
        match self {
            Sandbox::Wasm(sandbox) => sandbox,
            Sandbox::Vm(_) => panic!("a VM's sandbox"),
        }
    }

    pub fn wasm_mut(&mut self) -> &mut WasmSandbox {
        match self {
            Sandbox::Wasm(sandbox) => sandbox,
            Sandbox::Vm(_) => panic!("a VM's sandbox"),
        }
    }

    pub fn vm(&self) -> &VmSandbox {
        match self {
            Sandbox::Vm(sandbox) => sandbox,
            Sandbox::Wasm(_) => panic!("a component's sandbox"),
        }
    }

    pub fn vm_mut(&mut self) -> &mut VmSandbox {
        match self {
            Sandbox::Vm(sandbox) => sandbox,
            Sandbox::Wasm(_) => panic!("a component's sandbox"),
        }
    }
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

/// The wicket's apps: `S` their sandboxes, as [`AppBody`]'s.
#[derive(Deserialize, Clone, Debug)]
pub struct Listing<S = Sandbox> {
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
    pub apps: Vec<ListedApp<S>>,
}

impl<S> Listing<S> {
    pub fn runtime(&self) -> Runtime {
        match self.runtime.as_deref() {
            Some("firecracker") => Runtime::Firecracker,
            _ => Runtime::Wasm,
        }
    }
}

impl Listing<serde_json::Value> {
    /// The listing, its apps' sandboxes those of its runtime.
    fn typed(self) -> serde_json::Result<Listing> {
        let runtime = self.runtime();
        let apps = self
            .apps
            .into_iter()
            .map(|listed| listed.typed(runtime))
            .collect::<serde_json::Result<_>>()?;
        Ok(Listing {
            revision: self.revision,
            generation: self.generation,
            runtime: self.runtime,
            capabilities: self.capabilities,
            keyvalue_stores: self.keyvalue_stores,
            apps,
        })
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
pub struct ListedApp<S = Sandbox> {
    pub name: String,
    #[serde(flatten)]
    pub app: AppBody<S>,
    pub deployed: String,
    /// The instances alive, lowest index first.
    pub alive: Vec<ListedInstance>,
    /// The VMs it keeps that are down, each starting again.
    #[serde(default)]
    pub crashed: Vec<ListedCrashed>,
    /// Why it is short of VMs: one it keeps went down or does not start, or none is alive and
    /// their restores fail.
    #[serde(default)]
    pub down: Option<ListedDown>,
}

impl ListedApp<serde_json::Value> {
    /// The app as a wicket of `runtime` lists it.
    pub fn typed(self, runtime: Runtime) -> serde_json::Result<ListedApp> {
        Ok(ListedApp {
            name: self.name,
            app: self.app.typed(runtime)?,
            deployed: self.deployed,
            alive: self.alive,
            crashed: self.crashed,
            down: self.down,
        })
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedCrashed {
    /// The wicket's index, as the live access log names it.
    pub index: u64,
    pub why: String,
    /// Since when it goes on, in seconds since the epoch.
    pub since: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedDown {
    pub why: String,
    /// Since when it goes on, in seconds since the epoch.
    pub since: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedInstance {
    /// The wicket's index, as the live access log names it.
    pub index: u64,
    /// Its start, in seconds since the epoch.
    pub since: u64,
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
    /// did not load, the components it has no upload of, whether the app's VMs did not start.
    Refused {
        status: u16,
        error: String,
        host: Option<String>,
        missing: Vec<String>,
        did_not_start: bool,
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
    #[serde(default)]
    did_not_start: bool,
}

impl Wicket {
    pub fn new(base: &str, token: Option<String>, owner: &str) -> Wicket {
        Wicket {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                // let go of before its console closes it: no request lands on a closing one
                .pool_idle_timeout(Duration::from_secs(30))
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
        let (url, timeout) = match after {
            Some(after) => (
                format!("{}/api/apps?after={after}", self.base),
                LIST_TIMEOUT,
            ),
            None => (format!("{}/api/apps", self.base), READ_TIMEOUT),
        };
        let request = self.client.get(url).timeout(timeout);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice::<Listing<serde_json::Value>>(&body)
            .and_then(Listing::typed)
            .map_err(|e| Failure::Unreachable(format!("the listing: {e}")))
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
        self.upload_as("components", "application/wasm", sha256, file)
            .await
    }

    /// Uploads a static site's files, the tar in `file`, as [`Wicket::upload`] a component.
    pub async fn upload_files(&self, sha256: &str, file: tokio::fs::File) -> Result<(), Failure> {
        self.upload_as("files", "application/x-tar", sha256, file)
            .await
    }

    /// PUTs `file`, of `content_type`, to `/api/<kind>/<sha256>`.
    async fn upload_as(
        &self,
        kind: &str,
        content_type: &str,
        sha256: &str,
        file: tokio::fs::File,
    ) -> Result<(), Failure> {
        let bytes = file
            .metadata()
            .await
            .map_err(|e| Failure::Unreachable(format!("the upload of {kind}: {e}")))?
            .len();
        let request = self
            .client
            .put(format!("{}/api/{kind}/{sha256}", self.base))
            .timeout(IMAGE_TIMEOUT)
            .header("content-type", content_type)
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
            did_not_start: error.did_not_start,
        },
        Err(_) => Failure::Refused {
            status: status.as_u16(),
            error: format!("{status}: {}", String::from_utf8_lossy(body)),
            host: None,
            missing: Vec::new(),
            did_not_start: false,
        },
    }
}

fn error_text(body: &[u8]) -> String {
    serde_json::from_slice::<ErrorBody>(body)
        .map(|error| error.error)
        .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned())
}
