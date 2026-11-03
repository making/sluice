//! The console (`--console <host:port>`): a page and a JSON API over the
//! routes. A rootfs image uploaded -- or dropped on the page -- is deployed as
//! a route at once, without a restart: the console hands the edit to the
//! reloader (`routes::Reloader::edit`), which starts the routes' VMs as they
//! would be with it and keeps them only when they started, the console's
//! routes in its directory (`deployed`). A controller deploys apps the same
//! way: hosts served by an image it uploaded by digest before, in one set of
//! options with an env.
//!
//! ```text
//! GET    /api/routes[?after=<revision>]         the routes, their VMs and the budget; held
//!                                               until they change when `after` is the
//!                                               current revision
//! PUT    /api/routes/<host>?name=..&<options>   deploys the body, an ext4 image, to the host
//! PATCH  /api/routes/<host>?<options>           gives the console's route other options
//! PATCH  /api/routes/<host>?host=<to>           moves the console's route to another host
//! DELETE /api/routes/<host>                     removes the console's route
//! PUT    /api/components/<sha256>               keeps the body, an ext4 image, under its digest
//! PUT    /api/images                            makes an ext4 image of the OCI image the body
//!                                               (JSON) names (`--guest`), kept under its digest
//! GET    /api/apps[?after=<revision>]           the apps, as `/api/routes`
//! PUT    /api/apps/<name>                       serves the app the body (JSON) has
//! DELETE /api/apps/<name>?owner=<owner>         removes the app
//! GET    /api/disks                             the apps' disks, which outlive them
//! DELETE /api/disks/<name>?owner=<owner>        removes the disk of an app that was
//! ```
//!
//! The options are the `--vm` route's (`mode`, `vcpus`, `mem`, `warm`, `vms`,
//! `egress`, ...), all of them: an option left out takes its default. The
//! image is received to the console's directory as it comes, hashed on the
//! way (images run to hundreds of MiB), and kept under its digest. An app's
//! sandbox is the same in JSON, with its env (its process's environment
//! variables, which may hold secrets: the listings name them, never their
//! values; an app's `tag`, opaque to the console, tells its owner what it put),
//! and its `disk` (`disk`).
//!
//! The credentials, the page's shared files and the directory's layout are
//! `sluice-wicket-console`'s.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::request::Parts;
use http::{Method, StatusCode};
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sluice_wicket_console::env::Env;
use sluice_wicket_console::http::{
    Edited, Intake, Received, Refusal, Response, error, json, pairs, read, receive,
};
use sluice_wicket_console::serve::{self, Service};
use sluice_wicket_console::status::{Alive, FailureView, Status, alive, settled};
use sluice_wicket_console::store::{
    check_digest, check_host, check_hosts, check_name, check_tag, rfc3339,
};
use sluice_wicket_tunnel::routes::Routes;
use tokio::sync::{mpsc, oneshot, watch};

use crate::capacity::Capacity;
use crate::config::Origin;
use crate::deployed::{self, AppSpec};
use crate::image::{self, Images};
use crate::route::RouteSpec;
use crate::routes::{Edit, Outcome, Refused, Request, Table};

/// Largest image an upload may carry.
pub const MAX_ROOTFS_BYTES: u64 = 4 << 30;

/// Where the ext2/3/4 superblock's magic is, and what it is (little-endian
/// 0xEF53).
const MAGIC_AT: usize = 1024 + 56;
const MAGIC: [u8; 2] = [0x53, 0xEF];

/// Largest app (its JSON) a PUT may carry: an env of the most an env holds
/// fits, escaped.
const MAX_APP_BYTES: usize = 1 << 20;

/// What the listings say the wicket runs: what its apps' sandboxes hold.
const RUNTIME: &str = "firecracker";

const INDEX: &str = include_str!("console/index.html");
const SCRIPT: &str = include_str!("console/console.js");

/// What the console serves and edits.
pub struct Console {
    pub routes: Arc<Routes<Table>>,
    pub status: watch::Receiver<Status>,
    /// To the reloader, which applies the edits.
    pub requests: mpsc::Sender<Request>,
    /// The VMs' budget, which the page shows.
    pub capacity: Arc<Capacity>,
    /// The console's directory, its images' paths relative to it.
    pub dir: PathBuf,
    /// Where an upload is received: the images' directory.
    pub intake: PathBuf,
    /// With `--guest`: what `PUT /api/images` makes images with.
    pub images: Option<Images>,
    /// Jail mode: a route may take `cpu`.
    pub jailed: bool,
    /// What the page shows besides the routes.
    pub server: Option<String>,
    pub listen: Option<SocketAddr>,
}

impl Service for Console {
    const INDEX: &'static str = INDEX;
    const SCRIPT: &'static str = SCRIPT;
    const DRAIN_LIMIT: u64 = MAX_ROOTFS_BYTES;

    async fn handle(&self, request: serve::Request<'_>) -> Response {
        let serve::Request { parts, body, user } = request;
        let (method, path, query) = (&parts.method, parts.uri.path(), parts.uri.query());
        if (method, path) == (&Method::GET, "/api/routes") {
            let status = settled(&self.status, query).await;
            return json(StatusCode::OK, &self.listing(&status, user));
        }
        if (method, path) == (&Method::GET, "/api/apps") {
            let status = settled(&self.status, query).await;
            return json(StatusCode::OK, &self.apps(&status));
        }
        let mut take = || body.take().expect("not read yet");
        if path == "/api/images" {
            return match *method {
                Method::PUT => self.make_image(parts, take()).await,
                _ => error(StatusCode::METHOD_NOT_ALLOWED, "/api/images takes PUT"),
            };
        }
        if let Some(host) = path.strip_prefix("/api/routes/") {
            return match *method {
                Method::PUT => self.deploy(host, parts, take()).await,
                Method::PATCH => self.patch(host, query).await,
                Method::DELETE => match check_host(host) {
                    Ok(host) => self.edit(Edit::Remove { host }).await,
                    Err(e) => error(StatusCode::BAD_REQUEST, e),
                },
                _ => error(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "a route takes PUT, PATCH or DELETE",
                ),
            };
        }
        if (method, path) == (&Method::GET, "/api/disks") {
            return json(StatusCode::OK, &self.disks());
        }
        if let Some(name) = path.strip_prefix("/api/disks/") {
            return match *method {
                Method::DELETE => self.delete_disk(name, query).await,
                _ => error(StatusCode::METHOD_NOT_ALLOWED, "a disk takes DELETE"),
            };
        }
        if let Some(name) = path.strip_prefix("/api/apps/") {
            return match *method {
                Method::PUT => self.put_app(name, parts, take()).await,
                Method::DELETE => self.delete_app(name, query).await,
                _ => error(StatusCode::METHOD_NOT_ALLOWED, "an app takes PUT or DELETE"),
            };
        }
        if let Some(sha256) = path.strip_prefix("/api/components/") {
            return match *method {
                Method::PUT => self.upload(sha256, parts, take()).await,
                _ => error(StatusCode::METHOD_NOT_ALLOWED, "an image takes PUT"),
            };
        }
        error(StatusCode::NOT_FOUND, format!("nothing at {path}"))
    }
}

impl Console {
    fn listing(&self, status: &Status, user: Option<String>) -> Listing {
        let table = self.routes.current();
        let routes = table
            .served()
            .map(|(up, origin, route)| {
                let spec = route.spec();
                let rootfs = &spec.vm.rootfs;
                let (path, sha256) = match origin {
                    // its path below the directory names its digest
                    Origin::Console { .. } | Origin::Api { .. } => {
                        let relative = rootfs.strip_prefix(&self.dir).unwrap_or(rootfs);
                        (relative.display().to_string(), kept_digest(relative))
                    }
                    _ => (rootfs.display().to_string(), None),
                };
                let vms = route.vms();
                RouteView {
                    host: up.host.clone(),
                    origin: origin.name(),
                    deployed: match origin {
                        Origin::Console { deployed } | Origin::Api { deployed, .. } => {
                            Some(deployed.clone())
                        }
                        _ => None,
                    },
                    app: match origin {
                        Origin::Api { name, .. } => Some(name.clone()),
                        _ => None,
                    },
                    owner: match origin {
                        Origin::Api { owner, .. } => Some(owner.clone()),
                        _ => None,
                    },
                    env: spec.vm.env.names(),
                    target: up.target_url.clone(),
                    rootfs: RootfsView {
                        name: rootfs
                            .file_name()
                            .map(|name| name.to_string_lossy().to_string())
                            .unwrap_or_default(),
                        path,
                        sha256,
                        bytes: std::fs::metadata(rootfs).map_or(0, |metadata| metadata.len()),
                    },
                    options: spec.options().into_iter().collect(),
                    mode: spec.mode.name(),
                    mem_mib: spec.vm.mem_mib,
                    vcpus: spec.vm.vcpus,
                    vms: VmsView {
                        alive: vms.alive,
                        shared: vms.shared,
                        warm: vms.warm,
                        kept: vms.kept,
                        pooled: vms.pooled,
                        starting: vms.starting,
                        connections: vms.connections,
                        restarts: vms.restarts,
                        down: route.down(),
                    },
                    snapshot: route.taken().map(|taken| SnapshotView {
                        memory_bytes: taken.memory,
                        booted_ms: taken.booted.as_millis() as u64,
                        taken_ms: taken.taken.as_millis() as u64,
                        at: rfc3339(taken.at),
                    }),
                }
            })
            .collect();
        let (vms, memory) = self.capacity.usage();
        Listing {
            revision: status.revision,
            generation: table.generation(),
            build: crate::describe_build(),
            user,
            server: self.server.clone(),
            listen: self.listen.map(|listen| listen.to_string()),
            max_rootfs_bytes: MAX_ROOTFS_BYTES,
            runtime: RUNTIME,
            jailed: self.jailed,
            budget: BudgetView {
                max_vms: self.capacity.max_vms(),
                max_memory_mib: self.capacity.max_memory(),
                vms,
                memory_mib: memory,
            },
            reload_failure: status.failure_view(),
            routes,
        }
    }

    /// The apps the table serves, in the order put, each as its PUT has it --
    /// but the env, named only.
    fn apps(&self, status: &Status) -> AppsView {
        let table = self.routes.current();
        let mut apps: Vec<AppView> = Vec::new();
        for (up, origin, route) in table.served() {
            let Origin::Api {
                name,
                owner,
                tag,
                deployed,
            } = origin
            else {
                continue;
            };
            if let Some(app) = apps.iter_mut().find(|app| app.name == *name) {
                app.hosts.push(up.host.clone());
                continue;
            }
            let rootfs = &route.vm().rootfs;
            let relative = rootfs.strip_prefix(&self.dir).unwrap_or(rootfs);
            apps.push(AppView {
                name: name.clone(),
                owner: owner.clone(),
                hosts: vec![up.host.clone()],
                component: ComponentRef {
                    sha256: kept_digest(relative).unwrap_or_default(),
                    name: rootfs
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                        .unwrap_or_default(),
                },
                sandbox: sandbox_view(&route.spec()),
                tag: tag.clone(),
                deployed: deployed.clone(),
                alive: alive(route.alive()),
                down: route.down(),
            });
        }
        AppsView {
            revision: status.revision,
            generation: table.generation(),
            runtime: RUNTIME,
            capabilities: [],
            keyvalue_stores: [],
            apps,
        }
    }

    /// Keeps the body under its digest `sha256` for an app to name. The digest
    /// is checked before the image is read.
    async fn upload(&self, sha256: &str, parts: &Parts, body: Incoming) -> Response {
        let sha256 = match check_digest(sha256) {
            Ok(sha256) => sha256,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let received = match self.image(parts, body).await {
            Ok(received) => received,
            Err(refused) => return refused.into(),
        };
        let bytes = received.bytes;
        let upload = Edit::Upload {
            sha256: sha256.clone(),
            received,
        };
        match self.request(upload).await {
            Ok(_) => json(StatusCode::OK, &Uploaded { sha256, bytes }),
            Err(refused) => refused.into(),
        }
    }

    /// Makes the image the body names, kept as if uploaded: its digest is what an app
    /// names. Held until it is made: minutes, for a large one.
    async fn make_image(&self, parts: &Parts, body: Incoming) -> Response {
        let Some(images) = &self.images else {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "this wicket makes no images: it needs --guest, /init of the images",
            );
        };
        let too_large = format!("a request is {} KiB at most", MAX_APP_BYTES >> 10);
        let body = match read(parts, body, MAX_APP_BYTES, &too_large).await {
            Ok(body) => body,
            Err(refused) => return refused.into(),
        };
        let body = match serde_json::from_slice::<ImageBody>(&body) {
            Ok(body) => body,
            Err(e) => return error(StatusCode::BAD_REQUEST, format!("the request: {e}")),
        };
        let credentials = match (body.username, body.password) {
            (Some(username), Some(password)) => Some((username, password)),
            (None, None) => None,
            _ => {
                return error(StatusCode::BAD_REQUEST, "username and password go together");
            }
        };
        let request = image::Request {
            image: body.image,
            credentials,
            port: body.port,
            restore: body.restore,
        };
        let built = match images.build(request).await {
            Ok(built) => built,
            Err(e) => return error(StatusCode::UNPROCESSABLE_ENTITY, e),
        };
        let view = ImageView {
            image: built.image,
            name: built.name,
            sha256: built.received.sha256.clone(),
            bytes: built.received.bytes,
        };
        let upload = Edit::Upload {
            sha256: view.sha256.clone(),
            received: built.received,
        };
        match self.request(upload).await {
            Ok(_) => json(StatusCode::OK, &view),
            Err(refused) => refused.into(),
        }
    }

    /// Serves the app `name` as the body has it; every check of its own before
    /// the reloader's, so a malformed app is a 400 first.
    async fn put_app(&self, name: &str, parts: &Parts, body: Incoming) -> Response {
        let too_large = format!("an app is {} KiB at most", MAX_APP_BYTES >> 10);
        let body = match read(parts, body, MAX_APP_BYTES, &too_large).await {
            Ok(body) => body,
            Err(refused) => return refused.into(),
        };
        let app = serde_json::from_slice::<AppBody>(&body)
            .map_err(|e| format!("the app: {e}"))
            .and_then(|app| app_spec(name, app));
        match app {
            Ok(app) => {
                let name = name.to_string();
                self.edit(Edit::PutApp { name, app }).await
            }
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    async fn delete_app(&self, name: &str, query: Option<&str>) -> Response {
        match owner_of(name, query) {
            Ok(owner) => {
                let name = name.to_string();
                self.edit(Edit::DeleteApp { name, owner }).await
            }
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// The disks kept in the console's directory, and whether an app has each.
    fn disks(&self) -> DisksView {
        let table = self.routes.current();
        let apps: Vec<(&str, &str)> = table
            .served()
            .filter_map(|(_, origin, _)| match origin {
                Origin::Api { name, owner, .. } => Some((owner.as_str(), name.as_str())),
                _ => None,
            })
            .collect();
        DisksView {
            disks: deployed::disks(&self.dir, |owner, name| apps.contains(&(owner, name))),
        }
    }

    async fn delete_disk(&self, name: &str, query: Option<&str>) -> Response {
        let edit = owner_of(name, query).map(|owner| Edit::DeleteDisk {
            name: name.to_string(),
            owner,
        });
        match edit {
            Ok(edit) => match self.request(edit).await {
                Ok(_) => json(
                    StatusCode::OK,
                    &Edited {
                        generation: self.routes.current().generation(),
                        message: format!("removed the disk of {name}"),
                    },
                ),
                Err(refused) => refused.into(),
            },
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// The options are checked before the image is read: a refusal skips the
    /// upload of a client expecting `100 Continue`.
    async fn deploy(&self, host: &str, parts: &Parts, body: Incoming) -> Response {
        let host = match check_host(host) {
            Ok(host) => host,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let mut name = None;
        let mut given = Vec::new();
        for (key, value) in pairs(parts.uri.query()) {
            match key.as_str() {
                "name" => name = Some(value),
                _ => given.push((key, value)),
            }
        }
        let options = match deployed::options(&host, &given) {
            Ok(options) => options,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let received = match self.image(parts, body).await {
            Ok(received) => received,
            Err(refused) => return refused.into(),
        };
        self.edit(Edit::Deploy {
            host,
            name: name.unwrap_or_default(),
            received,
            options,
        })
        .await
    }

    /// The ext4 image of a request's body, received to the images' directory.
    async fn image(&self, parts: &Parts, body: Incoming) -> Result<Received, Refusal> {
        let too_large = format!("an image is {} GiB at most", MAX_ROOTFS_BYTES >> 30);
        let intake = Intake {
            dir: &self.intake,
            limit: MAX_ROOTFS_BYTES,
            too_large: &too_large,
            empty: "no image: send the ext4 file as the request body",
            head: MAGIC_AT + MAGIC.len(),
            check: |head: &[u8]| match head.get(MAGIC_AT..) {
                Some(magic) if magic == MAGIC => Ok(()),
                _ => Err("not an ext4 image (no ext2/3/4 superblock magic at byte 1080)".into()),
            },
        };
        receive(parts, body, intake).await
    }

    /// `host=<to>` alone moves the route; any other query is its whole set of
    /// options. Never both: options left out would read as the defaults.
    async fn patch(&self, host: &str, query: Option<&str>) -> Response {
        let host = match check_host(host) {
            Ok(host) => host,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let pairs = pairs(query);
        if let Some((_, to)) = pairs.iter().find(|(key, _)| key == "host") {
            if pairs.len() > 1 {
                return error(
                    StatusCode::BAD_REQUEST,
                    "host= moves the route as it is: its options take a PATCH of their own",
                );
            }
            return match check_host(to) {
                Ok(to) => self.edit(Edit::Move { host, to }).await,
                Err(e) => error(StatusCode::BAD_REQUEST, e),
            };
        }
        match deployed::options(&host, &pairs) {
            Ok(options) => self.edit(Edit::Options { host, options }).await,
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// Has the reloader apply `edit`, and answers its outcome.
    async fn edit(&self, edit: Edit) -> Response {
        match self.request(edit).await {
            Ok(Outcome { generation, change }) => json(
                StatusCode::OK,
                &Edited {
                    generation,
                    message: change.map_or_else(|| "no change".into(), |change| change.to_string()),
                },
            ),
            Err(refused) => refused.into(),
        }
    }

    /// Has the reloader apply `edit`: its outcome, or the answer refusing it.
    async fn request(&self, edit: Edit) -> Result<Outcome, Refusal> {
        let draining = || {
            Refusal::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "the wicket is draining: its routes change no more",
            )
        };
        let (reply, outcome) = oneshot::channel();
        if self.requests.send(Request { edit, reply }).await.is_err() {
            return Err(draining());
        }
        match outcome.await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(refused)) => {
                let status = match refused {
                    Refused::Invalid(_) => StatusCode::BAD_REQUEST,
                    Refused::Unknown(_) => StatusCode::NOT_FOUND,
                    Refused::Taken(_) | Refused::Missing(_) => StatusCode::CONFLICT,
                    Refused::Rejected(_) | Refused::DidNotStart(_) => {
                        StatusCode::UNPROCESSABLE_ENTITY
                    }
                    Refused::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let mut refusal = Refusal::new(status, refused.to_string());
                match refused {
                    Refused::Missing(sha256) => refusal.view.missing = vec![sha256],
                    Refused::DidNotStart(_) => refusal.view.did_not_start = true,
                    _ => {}
                }
                Err(refusal)
            }
            Err(_) => Err(draining()),
        }
    }
}

/// The owner `query` names of the app `name`, both checked.
fn owner_of(name: &str, query: Option<&str>) -> Result<String, String> {
    let owner = pairs(query)
        .into_iter()
        .find(|(key, _)| key == "owner")
        .map(|(_, owner)| owner);
    check_name("app", name).and_then(|()| match owner {
        Some(owner) => check_name("owner", &owner).map(|()| owner),
        None => Err("owner=<owner> names the app's owner: a controller removes its own".into()),
    })
}

/// The digest a kept image's path (`rootfs/<sha256>/<name>`) names.
fn kept_digest(relative: &Path) -> Option<String> {
    relative
        .parent()
        .and_then(|parent| parent.file_name())
        .map(|digest| digest.to_string_lossy().to_string())
}

/// The app `name` of `body`, checked.
fn app_spec(name: &str, body: AppBody) -> Result<AppSpec, String> {
    check_name("app", name)?;
    check_name("owner", &body.owner)?;
    let sha256 =
        check_digest(&body.component.sha256).map_err(|e| format!("component.sha256: {e}"))?;
    if let Some(tag) = &body.tag {
        check_tag(tag)?;
    }
    let hosts = check_hosts(&body.hosts)?;
    let mut sandbox = body.sandbox;
    let env = match sandbox.remove("env") {
        None => BTreeMap::new(),
        Some(env) => serde_json::from_value(env)
            .map_err(|_| "sandbox.env: a map of name -> value, strings".to_string())?,
    };
    let env = Env::new(env).map_err(|e| format!("sandbox.env: {e}"))?;
    let pairs = sandbox
        .into_iter()
        .map(|(key, value)| {
            let value = match &value {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                Value::Array(rules) => {
                    let rules: Option<Vec<&str>> = rules.iter().map(Value::as_str).collect();
                    rules
                        .ok_or_else(|| format!("sandbox.{key}: a list of strings"))?
                        .join(",")
                }
                _ => return Err(format!("sandbox.{key}: a string, a number or a list")),
            };
            Ok((key, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let options = deployed::options(&hosts[0], &pairs).map_err(|e| format!("sandbox: {e}"))?;
    Ok(AppSpec {
        owner: body.owner,
        hosts,
        sha256,
        file: body.component.name,
        options,
        env,
        tag: body.tag,
    })
}

/// A route's sandbox as an app's listing has it, a body its PUT takes but the
/// env: its mode, vCPUs and memory, the other options set, the env's names.
fn sandbox_view(spec: &RouteSpec) -> Map<String, Value> {
    let mut sandbox = Map::new();
    sandbox.insert("mode".into(), spec.mode.name().into());
    sandbox.insert("vcpus".into(), spec.vm.vcpus.into());
    sandbox.insert("mem".into(), spec.vm.mem_mib.into());
    for (key, value) in spec.options() {
        let value = match key {
            "mode" => continue,
            "egress" => value.split(',').collect::<Vec<_>>().into(),
            "disk" => value.into(),
            _ => value.parse::<u64>().map_or(Value::Null, Value::from),
        };
        sandbox.insert(key.into(), value);
    }
    if !spec.vm.env.is_empty() {
        sandbox.insert("env".into(), spec.vm.env.names().into());
    }
    sandbox
}

/// What `GET /api/disks` answers.
#[derive(Serialize)]
struct DisksView {
    disks: Vec<deployed::KeptDisk>,
}

/// What `PUT /api/images` takes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageBody {
    /// `[registry/]repository[:tag][@sha256:<digest>]`.
    image: String,
    username: Option<String>,
    password: Option<String>,
    /// The process's port; left out: the one the image exposes, else 8080.
    port: Option<u16>,
    /// The restore hook's argv, run in each clone before it serves.
    #[serde(default)]
    restore: Vec<String>,
}

/// What it answers: the reference pinned to its manifest's digest, and what an app
/// names.
#[derive(Serialize)]
struct ImageView {
    image: String,
    name: String,
    sha256: String,
    bytes: u64,
}

#[derive(Serialize)]
struct Uploaded {
    sha256: String,
    bytes: u64,
}

/// The apps. No capability or keyvalue store to grant: what a wasm wicket's
/// listing offers its apps.
#[derive(Serialize)]
struct AppsView {
    revision: u64,
    generation: u64,
    runtime: &'static str,
    capabilities: [&'static str; 0],
    keyvalue_stores: [&'static str; 0],
    apps: Vec<AppView>,
}

/// An app as listed: what its PUT takes -- but the env, named only -- its name,
/// since when it serves this image, these options and this env, and what of it is up.
#[derive(Serialize)]
struct AppView {
    name: String,
    owner: String,
    hosts: Vec<String>,
    component: ComponentRef,
    sandbox: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
    deployed: String,
    /// Its VMs alive, lowest index first.
    alive: Vec<Alive>,
    /// Why it is short of the VMs it keeps ([`Route::down`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    down: Option<String>,
}

/// The body of an app's PUT.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppBody {
    owner: String,
    hosts: Vec<String>,
    component: ComponentRef,
    /// The `--vm` route's options (a number, a string, `egress` a list) and
    /// `env`, a map.
    #[serde(default)]
    sandbox: Map<String, Value>,
    /// Opaque to the console, kept and listed as given ([`check_tag`]).
    tag: Option<String>,
}

/// An image uploaded by digest, and the file name it is kept as.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ComponentRef {
    sha256: String,
    name: String,
}

#[derive(Serialize)]
struct Listing {
    revision: u64,
    generation: u64,
    build: String,
    /// Who is signed in through the provider, for the page.
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    server: Option<String>,
    listen: Option<String>,
    max_rootfs_bytes: u64,
    runtime: &'static str,
    /// Jail mode: a route may take `cpu`.
    jailed: bool,
    budget: BudgetView,
    reload_failure: Option<FailureView>,
    routes: Vec<RouteView>,
}

/// The VMs' budget and what of it is taken.
#[derive(Serialize)]
struct BudgetView {
    max_vms: usize,
    /// None: no memory budget.
    max_memory_mib: Option<u64>,
    vms: usize,
    memory_mib: u64,
}

#[derive(Serialize)]
struct RouteView {
    host: String,
    origin: &'static str,
    deployed: Option<String>,
    /// An app's route: the app and its owner.
    app: Option<String>,
    owner: Option<String>,
    /// The names of its process's environment variables, never their values.
    env: Vec<String>,
    target: String,
    rootfs: RootfsView,
    /// The options set to other than their defaults, as a PATCH takes them.
    options: BTreeMap<&'static str, String>,
    mode: &'static str,
    mem_mib: u32,
    vcpus: u32,
    vms: VmsView,
    /// Cloned modes: the template's snapshot.
    snapshot: Option<SnapshotView>,
}

#[derive(Serialize)]
struct RootfsView {
    name: String,
    /// The console's: relative to its directory; else absolute.
    path: String,
    /// The console's: the digest its path names.
    sha256: Option<String>,
    bytes: u64,
}

#[derive(Serialize)]
struct VmsView {
    alive: usize,
    shared: bool,
    warm: usize,
    kept: usize,
    pooled: usize,
    starting: usize,
    connections: u64,
    restarts: u64,
    down: Option<String>,
}

#[derive(Serialize)]
struct SnapshotView {
    memory_bytes: u64,
    booted_ms: u64,
    taken_ms: u64,
    at: String,
}
