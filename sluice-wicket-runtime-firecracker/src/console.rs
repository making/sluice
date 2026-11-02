//! The console (`--console <host:port>`): a page and a JSON API over the
//! routes. A rootfs image uploaded -- or dropped on the page -- is deployed as
//! a route at once, without a restart: the console hands the edit to the
//! reloader (`routes::Reloader::edit`), which starts the routes' VMs as they
//! would be with it and keeps them only when they started, the console's
//! routes in its directory (`deployed`).
//!
//! ```text
//! GET    /api/routes[?after=<revision>]         the routes, their VMs and the budget; held
//!                                               until they change when `after` is the
//!                                               current revision
//! PUT    /api/routes/<host>?name=..&<options>   deploys the body, an ext4 image, to the host
//! PATCH  /api/routes/<host>?<options>           gives the console's route other options
//! PATCH  /api/routes/<host>?host=<to>           moves the console's route to another host
//! DELETE /api/routes/<host>                     removes the console's route
//! ```
//!
//! The options are the `--vm` route's (`mode`, `vcpus`, `mem`, `warm`, `vms`,
//! `egress`, ...), all of them: an option left out takes its default. The
//! image is received to the console's directory as it comes, hashed on the
//! way (images run to hundreds of MiB), and kept under its digest.
//!
//! The credentials, the page's shared files and the directory's layout are
//! `sluice-wicket-console`'s.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use http::request::Parts;
use http::{Method, StatusCode};
use hyper::body::Incoming;
use serde::Serialize;
use sluice_wicket_console::http::{Edited, Intake, Refusal, Response, error, json, pairs, receive};
use sluice_wicket_console::serve::{self, Service};
use sluice_wicket_console::status::{FailureView, Status, settled};
use sluice_wicket_console::store::{check_host, rfc3339};
use sluice_wicket_tunnel::routes::Routes;
use tokio::sync::{mpsc, oneshot, watch};

use crate::capacity::Capacity;
use crate::config::Origin;
use crate::deployed;
use crate::routes::{Edit, Outcome, Refused, Request, Table};

/// Largest image an upload may carry.
pub const MAX_ROOTFS_BYTES: u64 = 4 << 30;

/// Where the ext2/3/4 superblock's magic is, and what it is (little-endian
/// 0xEF53).
const MAGIC_AT: usize = 1024 + 56;
const MAGIC: [u8; 2] = [0x53, 0xEF];

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
        let Some(host) = path.strip_prefix("/api/routes/") else {
            return error(StatusCode::NOT_FOUND, format!("nothing at {path}"));
        };
        match *method {
            Method::PUT => {
                let body = body.take().expect("not read yet");
                self.deploy(host, parts, body).await
            }
            Method::PATCH => self.patch(host, query).await,
            Method::DELETE => match check_host(host) {
                Ok(host) => self.edit(Edit::Remove { host }).await,
                Err(e) => error(StatusCode::BAD_REQUEST, e),
            },
            _ => error(
                StatusCode::METHOD_NOT_ALLOWED,
                "a route takes PUT, PATCH or DELETE",
            ),
        }
    }
}

impl Console {
    fn listing(&self, status: &Status, user: Option<String>) -> Listing {
        let table = self.routes.current();
        let routes = table
            .served()
            .map(|(up, origin, route)| {
                let rootfs = &route.spec.vm.rootfs;
                let (path, sha256) = match origin {
                    // its path below the directory names its digest
                    Origin::Console { .. } => {
                        let relative = rootfs.strip_prefix(&self.dir).unwrap_or(rootfs);
                        let digest = relative
                            .parent()
                            .and_then(|parent| parent.file_name())
                            .map(|digest| digest.to_string_lossy().to_string());
                        (relative.display().to_string(), digest)
                    }
                    _ => (rootfs.display().to_string(), None),
                };
                let vms = route.vms();
                RouteView {
                    host: up.host.clone(),
                    origin: origin.name(),
                    deployed: match origin {
                        Origin::Console { deployed } => Some(deployed.clone()),
                        _ => None,
                    },
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
                    options: route.spec.options().into_iter().collect(),
                    mode: route.spec.mode.name(),
                    mem_mib: route.spec.vm.mem_mib,
                    vcpus: route.spec.vm.vcpus,
                    vms: VmsView {
                        alive: vms.alive,
                        shared: vms.shared,
                        warm: vms.warm,
                        kept: vms.kept,
                        pooled: vms.pooled,
                        starting: vms.starting,
                        connections: vms.connections,
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
        let received = match receive(parts, body, intake).await {
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
        let draining = || {
            Refusal::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "the wicket is draining: its routes change no more",
            )
            .into()
        };
        let (reply, outcome) = oneshot::channel();
        if self.requests.send(Request { edit, reply }).await.is_err() {
            return draining();
        }
        match outcome.await {
            Ok(Ok(Outcome { generation, change })) => json(
                StatusCode::OK,
                &Edited {
                    generation,
                    message: change.map_or_else(|| "no change".into(), |change| change.to_string()),
                },
            ),
            Ok(Err(refused)) => {
                let status = match refused {
                    Refused::Invalid(_) => StatusCode::BAD_REQUEST,
                    Refused::Unknown(_) => StatusCode::NOT_FOUND,
                    Refused::Taken(_) => StatusCode::CONFLICT,
                    Refused::Rejected(_) => StatusCode::UNPROCESSABLE_ENTITY,
                    Refused::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
                };
                error(status, refused.to_string())
            }
            Err(_) => draining(),
        }
    }
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
}

#[derive(Serialize)]
struct SnapshotView {
    memory_bytes: u64,
    booted_ms: u64,
    taken_ms: u64,
    at: String,
}
