//! The console (`--console <host:port>`): a page and a JSON API over the
//! routes. A component uploaded -- or dropped on the page -- is deployed as a
//! route at once, without a restart: the console hands the edit to the
//! reloader (`routes::Reloader::apply`), which reloads the routes with it and
//! keeps them only when they loaded, the console's routes in its directory
//! (`deployed`). A controller deploys apps the same way: hosts served by a
//! component it uploaded by digest before, in one sandbox.
//!
//! ```text
//! GET    /api/routes[?after=<revision>]         the routes; held until they change when
//!                                               `after` is the current revision
//! PUT    /api/routes/<host>?name=..&<options>   deploys the body, a component, to the host
//! PATCH  /api/routes/<host>?<options>           gives the console's route another sandbox
//! PATCH  /api/routes/<host>?host=<to>           moves the console's route to another host
//! DELETE /api/routes/<host>                     removes the console's route
//! PUT    /api/components/<sha256>               keeps the body, a component, under its digest
//! PUT    /api/files/<sha256>                    unpacks the body, a tar, under its digest
//! GET    /api/apps[?after=<revision>]           the apps, as `/api/routes`
//! PUT    /api/apps/<name>                       serves the app the body (JSON) has
//! DELETE /api/apps/<name>?owner=<owner>         removes the app
//! GET    /api/keyvalue/<store>/<namespace>      the routes mapping a namespace
//! DELETE /api/keyvalue/<store>/<namespace>      deletes the data of a namespace no route maps
//! PUT    /api/evacuated/<owner>                 a wicket leaving: the owner moved what of its
//!                                               apps it could (`sluice_wicket_console::leaving`)
//! PUT    /api/confirmed/<owner>                 the owner's apps the body names are as it wants
//!                                               them: held, served (`sluice_wicket_console::held`)
//! ```
//!
//! The options are the `--wasm` route's (`budget-ms`, `concurrency`, `instances`, `keep-ms`,
//! `memory-mib`, `warm`, `wasi`), the whole sandbox: an option left out takes
//! its default. An app's sandbox is the same in JSON, the options' names its keys (a
//! sandbox in a listing too), with its keyvalue map
//! and its env (the guest's environment variables, which may hold secrets:
//! the listings name them, never their values; an app's `tag`, opaque to the
//! console, tells its owner what it put); the apps' listing names what a
//! sandbox may hold (the capabilities and the keyvalue stores), as the
//! routes' does. An app's files, a tar uploaded by digest too, are its
//! instances' read-only at `/` (`files`). The apps' listing names the wicket too -- its id, kept
//! in the console's directory, its flume node, its build -- and its budget: `--max-instances` and
//! the slots the routes' pools hold for good; an edit needing more is refused `no_room`. Both
//! listings say when the wicket is leaving: told to stop, it waits for the apps of the API to be
//! moved elsewhere (`--evacuation-grace`). An app put with `held-ms` is held at each start of the
//! wicket, listed so, until its owner confirms it, puts it again or removes it, or `held-ms`
//! passed: its owner may have moved, changed or removed it while the wicket was away.
//!
//! The credentials, the page's shared files and the persistence layout are
//! `sluice-wicket-console`'s.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::request::Parts;
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use sluice_wicket_console::held::{self, Held};
use sluice_wicket_console::http::{
    Body, Edited, Intake, Refusal, Response, error, json, pairs, read, receive,
};
use sluice_wicket_console::identity::{Budget, Identity};
use sluice_wicket_console::leaving::{self, LeavingView};
use sluice_wicket_console::serve::{self, Service};
use sluice_wicket_console::status::{Alive, Status, alive, settled};
use sluice_wicket_console::store;
use tokio::sync::{mpsc, oneshot, watch};

use crate::deployed::{self, AppSpec};
use crate::env::Env;
use crate::files;
use crate::keyvalue::{Place, Stores};
use crate::routes::{Edit, Origin, Outcome, Refused, Request, Routes};
use crate::wasm_host::{Caps, Limits, Sandbox};

/// Largest component an upload may carry.
pub const MAX_COMPONENT_BYTES: usize = 128 << 20;

/// Largest app (its JSON) a PUT may carry: an env of the most an env holds
/// fits, escaped.
const MAX_APP_BYTES: usize = 1 << 20;

const INDEX: &str = include_str!("console/index.html");
const SCRIPT: &str = include_str!("console/console.js");

/// What the console serves and edits.
pub struct Console {
    pub routes: Arc<Routes>,
    /// What the listings show besides the routes, the wicket leaving among it.
    pub status: watch::Sender<Status>,
    /// To the reloader, which applies the edits.
    pub requests: mpsc::Sender<Request>,
    /// The keyvalue stores an app's map may name.
    pub stores: Arc<Stores>,
    /// What the page shows besides the routes.
    pub server: Option<String>,
    pub listen: Option<SocketAddr>,
    /// The console's directory, the apps' files below it.
    pub dir: PathBuf,
    /// Which wicket this is, as the apps' listing says.
    pub identity: Identity,
    /// `--max-instances`, which the apps' listing names as the budget.
    pub max_instances: usize,
    /// The apps held until their owners confirm them.
    pub held: Arc<Held>,
}

impl Service for Console {
    const INDEX: &'static str = INDEX;
    const SCRIPT: &'static str = SCRIPT;
    const DRAIN_LIMIT: u64 = files::MAX_BYTES;

    async fn handle(&self, request: serve::Request<'_>) -> Response {
        self.route(request.parts, request.body, request.user).await
    }
}

impl Console {
    /// Answers the request of `parts`, taking `body` when it reads it.
    async fn route(
        &self,
        parts: &Parts,
        body: &mut Option<Body>,
        user: Option<String>,
    ) -> Response {
        let (method, path, query) = (&parts.method, parts.uri.path(), parts.uri.query());
        let mut take = || body.take().expect("not read yet");
        match (method, path) {
            (&Method::GET, "/api/routes") => {
                let status = settled(&self.status, query).await;
                json(StatusCode::OK, &self.listing(&status, user))
            }
            (&Method::GET, "/api/apps") => {
                let status = settled(&self.status, query).await;
                json(StatusCode::OK, &self.apps(&status))
            }
            _ => {
                if let Some(host) = path.strip_prefix("/api/routes/") {
                    match *method {
                        Method::PUT => self.deploy(host, parts, take()).await,
                        Method::PATCH => self.patch(host, query).await,
                        Method::DELETE => self.remove(host).await,
                        _ => error(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "a route takes PUT, PATCH or DELETE",
                        ),
                    }
                } else if let Some(name) = path.strip_prefix("/api/apps/") {
                    match *method {
                        Method::PUT => self.put_app(name, parts, take()).await,
                        Method::DELETE => self.delete_app(name, query).await,
                        _ => error(StatusCode::METHOD_NOT_ALLOWED, "an app takes PUT or DELETE"),
                    }
                } else if let Some(sha256) = path.strip_prefix("/api/components/") {
                    match *method {
                        Method::PUT => self.upload(sha256, parts, take()).await,
                        _ => error(StatusCode::METHOD_NOT_ALLOWED, "a component takes PUT"),
                    }
                } else if let Some(sha256) = path.strip_prefix("/api/files/") {
                    match *method {
                        Method::PUT => self.upload_files(sha256, parts, take()).await,
                        _ => error(StatusCode::METHOD_NOT_ALLOWED, "files take PUT"),
                    }
                } else if let Some(owner) = path.strip_prefix("/api/evacuated/") {
                    match *method {
                        Method::PUT => leaving::evacuated(&self.status, owner),
                        _ => error(StatusCode::METHOD_NOT_ALLOWED, "an owner's word takes PUT"),
                    }
                } else if let Some(owner) = path.strip_prefix("/api/confirmed/") {
                    match *method {
                        Method::PUT => held::confirmed(&self.held, owner, parts, take()).await,
                        _ => error(StatusCode::METHOD_NOT_ALLOWED, "an owner's word takes PUT"),
                    }
                } else if let Some(place) = path.strip_prefix("/api/keyvalue/") {
                    match *method {
                        Method::GET => self.mapping(place),
                        Method::DELETE => self.purge(place).await,
                        _ => error(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "a keyvalue namespace takes GET or DELETE",
                        ),
                    }
                } else {
                    error(StatusCode::NOT_FOUND, format!("nothing at {path}"))
                }
            }
        }
    }

    fn listing(&self, status: &Status, user: Option<String>) -> Listing {
        let table = self.routes.current();
        let routes = table
            .advertised
            .iter()
            .filter_map(|up| {
                let handler = table.handler(&up.target_url)?;
                let (origin, deployed, app, owner) = match table.origins.get(&up.host) {
                    Some(Origin::Console { deployed }) => {
                        ("console", Some(deployed.clone()), None, None)
                    }
                    Some(Origin::Api {
                        name,
                        owner,
                        deployed,
                        ..
                    }) => (
                        "api",
                        Some(deployed.clone()),
                        Some(name.clone()),
                        Some(owner.clone()),
                    ),
                    Some(Origin::Config) => ("config", None, None, None),
                    _ => ("flag", None, None, None),
                };
                let loaded = handler.loaded();
                Some(RouteView {
                    held: table.held_app(&up.host).is_some(),
                    host: up.host.clone(),
                    origin,
                    deployed,
                    app,
                    owner,
                    target: up.target_url.clone(),
                    component: ComponentView {
                        name: loaded.label().to_string(),
                        sha256: loaded.digest().to_string(),
                        bytes: loaded.size(),
                    },
                    sandbox: SandboxView::of(&handler.sandbox()),
                })
            })
            .collect();
        let defaults = Limits::default();
        Listing {
            revision: status.revision,
            generation: table.generation,
            build: crate::describe_build(),
            user,
            server: self.server.clone(),
            listen: self.listen.map(|listen| listen.to_string()),
            max_component_bytes: MAX_COMPONENT_BYTES,
            capabilities: &Caps::NAMES,
            defaults: Defaults {
                budget_ms: defaults.budget.as_millis(),
                memory_mib: defaults.memory >> 20,
            },
            keyvalue_stores: self.store_views(),
            reload_failure: status.failure_view(),
            leaving: status.leaving_view(),
            routes,
        }
    }

    /// The apps the table serves, in the order put, each as its PUT has it
    /// -- but the env, named only.
    fn apps(&self, status: &Status) -> AppsView {
        let table = self.routes.current();
        let mut apps: Vec<AppView> = Vec::new();
        for up in &table.advertised {
            let Some(Origin::Api {
                name,
                owner,
                tag,
                held_ms,
                deployed,
            }) = table.origins.get(&up.host)
            else {
                continue;
            };
            let taken = self.routes.taken(up);
            if let Some(app) = apps.iter_mut().find(|app| app.name == *name) {
                app.hosts.push(up.host.clone());
                app.advertised &= taken;
                continue;
            }
            let Some(handler) = table.handler(&up.target_url) else {
                continue;
            };
            let sandbox = handler.sandbox();
            let files = sandbox.files.as_ref().and_then(|files| {
                let sha256 = files.file_name()?.to_str()?.to_string();
                Some(FilesRef { sha256 })
            });
            apps.push(AppView {
                name: name.clone(),
                owner: owner.clone(),
                hosts: vec![up.host.clone()],
                component: ComponentRef {
                    sha256: handler.loaded().digest().to_string(),
                    name: handler.loaded().label().to_string(),
                },
                files,
                sandbox: AppSandboxView {
                    body: SandboxBody::of(&sandbox),
                    env: sandbox.env.names(),
                },
                tag: tag.clone(),
                held_ms: *held_ms,
                deployed: deployed.clone(),
                alive: alive(handler.alive()),
                advertised: taken,
                held: table.holds(name),
            });
        }
        AppsView {
            revision: status.revision,
            generation: table.generation,
            leaving: status.leaving_view(),
            wicket: self.identity.clone(),
            budget: Budget {
                max_instances: self.max_instances,
                max_memory_mib: None,
                instances: table.pooled(),
                memory_mib: None,
            },
            capabilities: &Caps::NAMES,
            keyvalue_stores: self.store_views(),
            apps,
        }
    }

    /// The keyvalue stores an app's map may name.
    fn store_views(&self) -> Vec<StoreView> {
        self.stores
            .described()
            .into_iter()
            .map(|(name, kind, default, shared)| StoreView {
                name: name.to_string(),
                kind,
                default,
                shared,
            })
            .collect()
    }

    async fn deploy(&self, host: &str, parts: &Parts, body: Body) -> Response {
        let component = match component(parts, body).await {
            Ok(component) => component,
            Err(refused) => return refused.into(),
        };
        let host = match store::check_host(host) {
            Ok(host) => host,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let (name, sandbox) = match options(&pairs(parts.uri.query()), true) {
            Ok(options) => options,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        if let Err(refused) = wasm(&component) {
            return refused.into();
        }
        self.edit(Edit::Deploy {
            host,
            name: name.unwrap_or_default(),
            component,
            sandbox,
        })
        .await
    }

    /// `host=<to>` alone moves the route; any other query is its whole
    /// sandbox. Never both: a sandbox left out would read as the defaults.
    async fn patch(&self, host: &str, query: Option<&str>) -> Response {
        let host = match store::check_host(host) {
            Ok(host) => host,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let pairs = pairs(query);
        if let Some((_, to)) = pairs.iter().find(|(key, _)| key == "host") {
            if pairs.len() > 1 {
                return error(
                    StatusCode::BAD_REQUEST,
                    "host= moves the route as it is: a sandbox takes a PATCH of its own",
                );
            }
            return match store::check_host(to) {
                Ok(to) => self.edit(Edit::Move { host, to }).await,
                Err(e) => error(StatusCode::BAD_REQUEST, e),
            };
        }
        match options(&pairs, false) {
            Ok((_, sandbox)) => self.edit(Edit::Sandbox { host, sandbox }).await,
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    async fn remove(&self, host: &str) -> Response {
        match store::check_host(host) {
            Ok(host) => self.edit(Edit::Remove { host }).await,
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// Keeps the body under its digest `sha256` for an app to name.
    async fn upload(&self, sha256: &str, parts: &Parts, body: Body) -> Response {
        let component = match component(parts, body).await {
            Ok(component) => component,
            Err(refused) => return refused.into(),
        };
        let sha256 = match store::check_digest(sha256) {
            Ok(sha256) => sha256,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        if let Err(refused) = wasm(&component) {
            return refused.into();
        }
        let bytes = component.len();
        let upload = Edit::Upload {
            sha256: sha256.clone(),
            component,
        };
        match self.request(upload).await {
            Ok(_) => json(StatusCode::OK, &Uploaded { sha256, bytes }),
            Err(refused) => refused.into(),
        }
    }

    /// Unpacks the body, a tar, as the files of the digest `sha256` for an
    /// app to name; on a blocking thread, as large as it may be.
    async fn upload_files(&self, sha256: &str, parts: &Parts, body: Body) -> Response {
        let sha256 = match store::check_digest(sha256) {
            Ok(sha256) => sha256,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let kept = self.dir.join(files::FILES);
        let too_large = format!("files are a tar of {} MiB at most", files::MAX_BYTES >> 20);
        let intake = Intake {
            dir: &kept,
            limit: files::MAX_BYTES,
            too_large: &too_large,
            empty: "no files: send a tar as the request body",
            head: 0,
            check: |_: &[u8]| Ok(()),
        };
        let received = match receive(parts, body, intake).await {
            Ok(received) => received,
            Err(refused) => return refused.into(),
        };
        if received.sha256 != sha256 {
            return error(
                StatusCode::BAD_REQUEST,
                format!("the body's sha256 is {}, not {sha256}", received.sha256),
            );
        }
        let bytes = received.bytes as usize;
        match tokio::task::spawn_blocking(move || files::keep(&kept, received)).await {
            Ok(Ok(())) => {
                println!("console: files {sha256}: {bytes} bytes kept");
                json(StatusCode::OK, &Uploaded { sha256, bytes })
            }
            Ok(Err(e)) => error(StatusCode::BAD_REQUEST, format!("the files: {e}")),
            Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, format!("the files: {e}")),
        }
    }

    /// Serves the app `name` as the body has it; every check of its own
    /// before the reloader's, so a malformed app is a 400 first.
    async fn put_app(&self, name: &str, parts: &Parts, body: Body) -> Response {
        let too_large = format!("an app is {} KiB at most", MAX_APP_BYTES >> 10);
        let body = match read(parts, body, MAX_APP_BYTES, &too_large).await {
            Ok(body) => body,
            Err(refused) => return refused.into(),
        };
        let app = serde_json::from_slice::<AppBody>(&body)
            .map_err(|e| format!("the app: {e}"))
            .and_then(|app| self.app_spec(name, app));
        match app {
            Ok(app) => {
                let name = name.to_string();
                let put = self.request(Edit::PutApp {
                    name: name.clone(),
                    app,
                });
                // as its owner wants it
                edited(put.await.inspect(|_| self.held.release(&name)))
            }
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// The app `name` of `body`, checked; its keyvalue map names the stores.
    fn app_spec(&self, name: &str, body: AppBody) -> Result<AppSpec, String> {
        store::check_name("app", name)?;
        store::check_name("owner", &body.owner)?;
        let sha256 = store::check_digest(&body.component.sha256)
            .map_err(|e| format!("component.sha256: {e}"))?;
        let files = body
            .files
            .map(|files| {
                store::check_digest(&files.sha256).map_err(|e| format!("files.sha256: {e}"))
            })
            .transpose()?;
        if let Some(tag) = &body.tag {
            store::check_tag(tag)?;
        }
        if let Some(held_ms) = body.held_ms {
            held::check_held_ms(held_ms)?;
        }
        Ok(AppSpec {
            owner: body.owner,
            hosts: store::check_hosts(&body.hosts)?,
            sha256,
            file: body.component.name,
            files,
            sandbox: body.sandbox.sandbox(&self.stores)?,
            tag: body.tag,
            held_ms: body.held_ms,
        })
    }

    async fn delete_app(&self, name: &str, query: Option<&str>) -> Response {
        let owner = pairs(query)
            .into_iter()
            .find(|(key, _)| key == "owner")
            .map(|(_, owner)| owner);
        let checked = store::check_name("app", name).and_then(|()| match owner {
            Some(owner) => store::check_name("owner", &owner).map(|()| owner),
            None => Err("owner=<owner> names the app's owner: a controller removes its own".into()),
        });
        match checked {
            Ok(owner) => {
                let name = name.to_string();
                let removal = self.request(Edit::DeleteApp {
                    name: name.clone(),
                    owner,
                });
                edited(removal.await.inspect(|_| self.held.release(&name)))
            }
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// Has the reloader apply `edit`, and answers its outcome.
    async fn edit(&self, edit: Edit) -> Response {
        edited(self.request(edit).await)
    }

    /// The routes mapping the keyvalue namespace `place` ([`place_of`]), a
    /// route being drained included: what a purge waits for.
    fn mapping(&self, place: &str) -> Response {
        let place = match place_of(place) {
            Ok(place) => place,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        if let Err(e) = self.stores.known(&place.store) {
            let named = format!("{}/{}", place.store, place.namespace);
            return error(StatusCode::NOT_FOUND, format!("{named}: {e}"));
        }
        let mapped_by = self.routes.current().mapping(&place);
        json(
            StatusCode::OK,
            &Mapping {
                store: place.store,
                namespace: place.namespace,
                mapped_by,
            },
        )
    }

    /// Deletes the data of the keyvalue namespace `place` ([`place_of`]).
    async fn purge(&self, place: &str) -> Response {
        let place = match place_of(place) {
            Ok(place) => place,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let (reply, purged) = oneshot::channel();
        let request = Request::Purge {
            place: place.clone(),
            reply,
        };
        match self.reloader(request, purged).await {
            Ok(keys) => json(
                StatusCode::OK,
                &Purged {
                    store: place.store,
                    namespace: place.namespace,
                    keys,
                },
            ),
            Err(refused) => refused.into(),
        }
    }

    /// Has the reloader apply `edit`: its outcome, or the answer refusing it.
    async fn request(&self, edit: Edit) -> Result<Outcome, Refusal> {
        let (reply, outcome) = oneshot::channel();
        let edit = Box::new(edit);
        self.reloader(Request::Edit { edit, reply }, outcome).await
    }

    /// Sends `request` to the reloader: what it answers on `answer`, or the
    /// answer refusing it.
    async fn reloader<T>(
        &self,
        request: Request,
        answer: oneshot::Receiver<Result<T, Refused>>,
    ) -> Result<T, Refusal> {
        let draining = || {
            Refusal::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "the wicket is draining: its routes change no more",
            )
        };
        if self.requests.send(request).await.is_err() {
            return Err(draining());
        }
        match answer.await {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(refused)) => {
                let status = match refused {
                    Refused::Invalid(_) => StatusCode::BAD_REQUEST,
                    Refused::Unknown(_) => StatusCode::NOT_FOUND,
                    Refused::Taken(_) | Refused::Missing(_) => StatusCode::CONFLICT,
                    Refused::Rejected(_) => StatusCode::UNPROCESSABLE_ENTITY,
                    Refused::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let mut refusal = Refusal::new(status, refused.to_string());
                match refused {
                    Refused::Rejected(unloaded) => {
                        refusal.view.host = unloaded.host;
                        refusal.view.no_room = unloaded.no_room;
                    }
                    Refused::Missing(missing) => refusal.view.missing = missing,
                    _ => {}
                }
                Err(refusal)
            }
            Err(_) => Err(draining()),
        }
    }
}

/// The answer of an edit's outcome.
fn edited(outcome: Result<Outcome, Refusal>) -> Response {
    match outcome {
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

/// The component an upload carries, read before anything is answered: a
/// refusal answered before would be lost to the reset of the connection
/// (see [`drain`]).
async fn component(parts: &Parts, body: Body) -> Result<Bytes, Refusal> {
    let too_large = format!("a component is {} MiB at most", MAX_COMPONENT_BYTES >> 20);
    read(parts, body, MAX_COMPONENT_BYTES, &too_large).await
}

/// Refuses an upload that is no wasm binary.
fn wasm(component: &Bytes) -> Result<(), Refusal> {
    if component.is_empty() {
        return Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            "no component: send the .wasm file as the request body",
        ));
    }
    if !component.starts_with(b"\0asm") {
        return Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            "not a wasm binary (it does not start with \\0asm)",
        ));
    }
    Ok(())
}

/// The name (with `named`) and the sandbox of an edit's query: the `--wasm`
/// route options.
fn options(pairs: &[(String, String)], named: bool) -> Result<(Option<String>, Sandbox), String> {
    let mut name = None;
    let mut options = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        if named && key == "name" {
            name = Some(value.clone());
        } else {
            options.push((key.as_str(), value.as_str()));
        }
    }
    Ok((name, Sandbox::from_pairs(options)?))
}

#[derive(Serialize)]
struct Uploaded {
    sha256: String,
    bytes: usize,
}

#[derive(Serialize)]
struct Purged {
    store: String,
    namespace: String,
    /// The keys the namespace held.
    keys: u64,
}

#[derive(Serialize)]
struct Mapping {
    store: String,
    namespace: String,
    /// The hosts whose routes map it, "a route being drained" for one no
    /// longer advertised.
    mapped_by: Vec<String>,
}

/// The keyvalue namespace of a path's `<store>/<namespace>`, percent-decoded:
/// the namespace may hold `/`.
fn place_of(place: &str) -> Result<Place, String> {
    let place = percent_encoding::percent_decode_str(place).decode_utf8_lossy();
    match place.split_once('/') {
        Some((store, namespace)) if !store.is_empty() && !namespace.is_empty() => Ok(Place {
            store: store.to_string(),
            namespace: namespace.to_string(),
        }),
        _ => Err(format!("'{place}' is no keyvalue <store>/<namespace>")),
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
    max_component_bytes: usize,
    capabilities: &'static [&'static str],
    defaults: Defaults,
    keyvalue_stores: Vec<StoreView>,
    reload_failure: Option<sluice_wicket_console::status::FailureView>,
    /// Told to stop, until when it serves on at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    leaving: Option<LeavingView>,
    routes: Vec<RouteView>,
}

/// The defaults of a sandbox's limits, keyed as a sandbox is.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct Defaults {
    budget_ms: u128,
    memory_mib: usize,
}

#[derive(Serialize)]
struct StoreView {
    name: String,
    #[serde(rename = "type")]
    kind: &'static str,
    default: bool,
    /// Which store it is when the wickets of other hosts may reach it too
    /// (none of the host's own): what tells two wickets' stores one.
    shared: Option<String>,
}

#[derive(Serialize)]
struct RouteView {
    /// An app's route held until its owner confirms it: neither advertised nor served.
    held: bool,
    host: String,
    origin: &'static str,
    deployed: Option<String>,
    /// An app's route: the app and its owner.
    app: Option<String>,
    owner: Option<String>,
    target: String,
    component: ComponentView,
    sandbox: SandboxView,
}

#[derive(Serialize)]
struct ComponentView {
    name: String,
    sha256: String,
    bytes: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct SandboxView {
    wasi: Vec<&'static str>,
    budget_ms: u128,
    memory_mib: usize,
    warm: usize,
    instances: Option<usize>,
    keep_ms: Option<u128>,
    concurrency: usize,
    keyvalue: Vec<BucketView>,
    /// The names of the guest's environment variables, never their values.
    env: Vec<String>,
}

impl SandboxView {
    fn of(sandbox: &Sandbox) -> SandboxView {
        SandboxView {
            wasi: sandbox.caps.names(),
            budget_ms: sandbox.limits.budget.as_millis(),
            memory_mib: sandbox.limits.memory >> 20,
            warm: sandbox.warm,
            instances: sandbox.instances,
            keep_ms: sandbox.keep.map(|keep| keep.as_millis()),
            concurrency: sandbox.concurrency,
            keyvalue: sandbox
                .keyvalue
                .iter()
                .map(|(identifier, place)| BucketView {
                    identifier: identifier.clone(),
                    store: place.store.clone(),
                    namespace: place.namespace.clone(),
                })
                .collect(),
            env: sandbox.env.names(),
        }
    }
}

#[derive(Serialize)]
struct BucketView {
    identifier: String,
    store: String,
    namespace: String,
}

/// The apps, the wicket and its budget, and what their sandboxes may hold.
#[derive(Serialize)]
struct AppsView {
    revision: u64,
    generation: u64,
    /// Told to stop, until when it serves on at most, while its apps are moved elsewhere.
    #[serde(skip_serializing_if = "Option::is_none")]
    leaving: Option<LeavingView>,
    wicket: Identity,
    budget: Budget,
    capabilities: &'static [&'static str],
    keyvalue_stores: Vec<StoreView>,
    apps: Vec<AppView>,
}

/// An app as listed: what its PUT takes -- but the env, named only -- its
/// name, since when it serves this component and sandbox, and what of it is up.
#[derive(Serialize)]
struct AppView {
    name: String,
    owner: String,
    hosts: Vec<String>,
    component: ComponentRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    files: Option<FilesRef>,
    sandbox: AppSandboxView,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
    #[serde(rename = "held-ms", skip_serializing_if = "Option::is_none")]
    held_ms: Option<u64>,
    deployed: String,
    /// Its instances alive, lowest index first.
    alive: Vec<Alive>,
    /// Whether the server of every tunnel stream took its routes as they are: it sends them
    /// connections. So in local mode.
    advertised: bool,
    /// Whether it is held until its owner confirms it: neither advertised nor served.
    held: bool,
}

/// An app's sandbox as listed: the env's names in place of the env, left
/// out without one (the listing of an app without an env is its PUT).
#[derive(Serialize)]
struct AppSandboxView {
    #[serde(flatten)]
    body: SandboxBody,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    env: Vec<String>,
}

/// The body of an app's PUT.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppBody {
    owner: String,
    hosts: Vec<String>,
    component: ComponentRef,
    /// Its files, a tar uploaded by digest: read-only at `/`.
    files: Option<FilesRef>,
    #[serde(default)]
    sandbox: SandboxBody,
    /// Opaque to the console, kept and listed as given
    /// ([`store::check_tag`]).
    tag: Option<String>,
    /// How long a start of the wicket holds it at most, until its owner confirms it.
    #[serde(rename = "held-ms")]
    held_ms: Option<u64>,
}

/// A component uploaded by digest, and the file name it is kept as.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ComponentRef {
    sha256: String,
    name: String,
}

/// An app's files, uploaded by digest.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FilesRef {
    sha256: String,
}

/// An app's sandbox: the `--wasm` route options, its keyvalue map
/// (`identifier -> store/namespace`) and its env (`name -> value`). A key
/// left out takes its default; a listing names every one but the env
/// ([`AppSandboxView`]).
#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct SandboxBody {
    #[serde(default)]
    wasi: Vec<String>,
    budget_ms: Option<u64>,
    memory_mib: Option<u64>,
    #[serde(default)]
    warm: usize,
    instances: Option<usize>,
    keep_ms: Option<u64>,
    concurrency: Option<usize>,
    #[serde(default)]
    keyvalue: BTreeMap<String, String>,
    #[serde(default, skip_serializing)]
    env: BTreeMap<String, String>,
}

impl SandboxBody {
    /// Without the env: its values are never listed.
    fn of(sandbox: &Sandbox) -> SandboxBody {
        SandboxBody {
            wasi: sandbox.caps.names().into_iter().map(String::from).collect(),
            budget_ms: Some(sandbox.limits.budget.as_millis() as u64),
            memory_mib: Some((sandbox.limits.memory >> 20) as u64),
            warm: sandbox.warm,
            instances: sandbox.instances,
            keep_ms: sandbox.keep.map(|keep| keep.as_millis() as u64),
            concurrency: Some(sandbox.concurrency),
            keyvalue: deployed::map_specs(&sandbox.keyvalue),
            env: BTreeMap::new(),
        }
    }

    /// The sandbox, its map naming `stores`; errors name the key.
    fn sandbox(self, stores: &Stores) -> Result<Sandbox, String> {
        let mut sandbox = Sandbox {
            caps: Caps::parse(&self.wasi.join(",")).map_err(|e| format!("sandbox.wasi: {e}"))?,
            warm: self.warm,
            ..Sandbox::default()
        };
        match self.budget_ms {
            Some(0) => return Err("sandbox.budget-ms: must be at least 1".into()),
            Some(budget) => sandbox.limits.budget = Duration::from_millis(budget),
            None => {}
        }
        match self.memory_mib {
            Some(0) => return Err("sandbox.memory-mib: must be at least 1".into()),
            Some(memory) => {
                sandbox.limits.memory = memory
                    .checked_mul(1 << 20)
                    .and_then(|bytes| usize::try_from(bytes).ok())
                    .ok_or_else(|| format!("sandbox.memory-mib: too large '{memory}'"))?;
            }
            None => {}
        }
        sandbox.instances = match self.instances {
            Some(0) => return Err("sandbox.instances: must be at least 1".into()),
            cap => cap,
        };
        sandbox.keep = match self.keep_ms {
            Some(0) => return Err("sandbox.keep-ms: must be at least 1".into()),
            keep => keep.map(Duration::from_millis),
        };
        sandbox.concurrency = match self.concurrency {
            Some(0) => return Err("sandbox.concurrency: must be at least 1".into()),
            concurrency => concurrency.unwrap_or(1),
        };
        sandbox.check().map_err(|e| format!("sandbox: {e}"))?;
        sandbox.env = Env::new(self.env).map_err(|e| format!("sandbox.env: {e}"))?;
        sandbox.keyvalue = deployed::app_map(sandbox.caps, self.keyvalue)?;
        for place in sandbox.keyvalue.values() {
            stores
                .known(&place.store)
                .map_err(|e| format!("sandbox.keyvalue: {e}"))?;
        }
        Ok(sandbox)
    }
}
