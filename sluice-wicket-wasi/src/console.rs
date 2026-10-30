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
//! GET    /api/apps[?after=<revision>]           the apps, as `/api/routes`
//! PUT    /api/apps/<name>                       serves the app the body (JSON) has
//! DELETE /api/apps/<name>?owner=<owner>         removes the app
//! DELETE /api/keyvalue/<store>/<namespace>      deletes the data of a namespace no route maps
//! ```
//!
//! The options are the `--wasm` route's (`budget-ms`, `instances`,
//! `memory-mib`, `pool`, `wasi`), the whole sandbox: an option left out takes
//! its default. An app's sandbox is the same in JSON, with its keyvalue map
//! and its env (the guest's environment variables, which may hold secrets:
//! the listings name them, never their values; an app's `tag`, opaque to the
//! console, tells its owner what it put); the apps' listing names what a
//! sandbox may hold (the capabilities and the keyvalue stores), as the
//! routes' does.
//!
//! With `--console-token` every request needs the token, as the password of
//! HTTP Basic (any user name: the browser asks for it) or as a Bearer token.
//! With `--oidc-issuer` a browser signs in through the OpenID Provider
//! instead (`/login`, `/oauth2/callback`, `/logout`): the session is a
//! cookie sealed with a key drawn at startup, good for [`SESSION_MAX_AGE`].
//! Without either the console listens on a loopback address only and
//! answers only requests for an IP address or `localhost`, so a DNS
//! rebinding page cannot reach it. Edits take no simple method -- neither a
//! form nor `sendBeacon` can send one -- and one from another origin is
//! refused.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use http::request::Parts;
use http::{HeaderValue, Method, StatusCode, header};
use http_body_util::{BodyExt as _, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use sluice_oidc::{Provider, Signer};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

use crate::deployed::{self, AppSpec, rfc3339};
use crate::env::Env;
use crate::keyvalue::{Place, Stores};
use crate::routes::{Edit, Origin, Outcome, Refused, Request, Routes, Status};
use crate::wasm_host::{Caps, Limits, Sandbox};

/// Largest component an upload may carry.
pub const MAX_COMPONENT_BYTES: usize = 128 << 20;

/// Largest app (its JSON) a PUT may carry: an env of the most an env holds
/// fits, escaped.
const MAX_APP_BYTES: usize = 1 << 20;

/// How long `GET /api/routes?after=<the current revision>` waits for a change.
const LONG_POLL: Duration = Duration::from_secs(25);

/// Connections open at once; one beyond waits in the backlog.
const MAX_CONNECTIONS: usize = 64;

/// How long a browser stays signed in.
const SESSION_MAX_AGE: Duration = Duration::from_secs(8 * 3600);

const SESSION_COOKIE: &str = "sluice-wicket-session";

const INDEX: &str = include_str!("console/index.html");
const CSS: &str = include_str!("console/console.css");
const JS: &str = include_str!("console/console.js");
const OVERPASS: &[u8] = include_bytes!("console/fonts/overpass.woff2");
const OVERPASS_MONO: &[u8] = include_bytes!("console/fonts/overpass-mono.woff2");

/// The page loads its own files only, and runs no inline script or style.
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
                   img-src 'self' data:; connect-src 'self'; base-uri 'none'; \
                   form-action 'none'; frame-ancestors 'none'";

type Response = hyper::Response<Full<Bytes>>;

/// What the console serves and edits.
pub struct Console {
    pub routes: Arc<Routes>,
    pub status: watch::Receiver<Status>,
    /// To the reloader, which applies the edits.
    pub requests: mpsc::Sender<Request>,
    /// The keyvalue stores an app's map may name.
    pub stores: Arc<Stores>,
    /// None: no credentials asked, on a loopback address.
    pub token: Option<String>,
    /// The browsers' sign-in through the OpenID Provider; None: the token.
    pub sign_in: Option<SignIn>,
    /// What the page shows besides the routes.
    pub server: Option<String>,
    pub listen: Option<SocketAddr>,
}

/// The sign-in through the OpenID Provider (`--oidc-issuer`).
pub struct SignIn {
    pub provider: Provider,
    /// Seals the sessions; drawn at startup: a restart signs every browser
    /// out.
    pub signer: Signer,
}

/// What a session cookie says.
#[derive(Serialize, Deserialize)]
struct Session {
    user: String,
    /// Seconds since the epoch.
    exp: u64,
}

/// Binds `address`; `authenticated` (a token or a provider) or a loopback
/// address only, checked before anything listens.
pub async fn bind(address: &str, authenticated: bool) -> Result<TcpListener, String> {
    let failed = |e: std::io::Error| format!("--console {address}: {e}");
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(address)
        .await
        .map_err(failed)?
        .collect();
    if !authenticated && resolved.iter().any(|at| !at.ip().is_loopback()) {
        return Err(format!(
            "--console {address}: a console on a non-loopback address needs --console-token or --oidc-issuer"
        ));
    }
    TcpListener::bind(&resolved[..]).await.map_err(failed)
}

/// Serves the console for good, at most [`MAX_CONNECTIONS`] connections at
/// once.
pub async fn run(listener: TcpListener, console: Arc<Console>) {
    let open = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    loop {
        let permit = open.clone().acquire_owned().await.expect("never closed");
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // e.g. out of file descriptors: retried, not fatal
                eprintln!("--console: accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let console = console.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let service = hyper::service::service_fn(move |request| {
                let console = console.clone();
                async move { Ok::<_, Infallible>(console.handle(request).await) }
            });
            // a client leaving mid-request is no news
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                .await;
        });
    }
}

impl Console {
    async fn handle(&self, request: hyper::Request<Incoming>) -> Response {
        let (parts, body) = request.into_parts();
        let mut body = Some(body);
        let mut response = match (&self.sign_in, parts.uri.path()) {
            // the way in: before any refusal
            (Some(sign_in), "/login" | "/oauth2/callback" | "/logout") => {
                self.sign_in_route(sign_in, &parts).await
            }
            _ => match self.refusal(&parts) {
                Some(refused) => refused,
                None => self.route(&parts, &mut body).await,
            },
        };
        if let Some(body) = body {
            drain(body).await;
        }
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(CSP),
        );
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        headers.insert(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        );
        response
    }

    /// Answers the request of `parts`, taking `body` when it reads it.
    async fn route(&self, parts: &Parts, body: &mut Option<Incoming>) -> Response {
        let (method, path, query) = (&parts.method, parts.uri.path(), parts.uri.query());
        let mut take = || body.take().expect("not read yet");
        match (method, path) {
            (&Method::GET, "/") => asset("text/html; charset=utf-8", INDEX.as_bytes()),
            (&Method::GET, "/console.css") => asset("text/css; charset=utf-8", CSS.as_bytes()),
            (&Method::GET, "/console.js") => asset("text/javascript; charset=utf-8", JS.as_bytes()),
            (&Method::GET, "/fonts/overpass.woff2") => asset("font/woff2", OVERPASS),
            (&Method::GET, "/fonts/overpass-mono.woff2") => asset("font/woff2", OVERPASS_MONO),
            (&Method::GET, "/api/routes") => {
                let status = self.settled(query).await;
                json(
                    StatusCode::OK,
                    &self.listing(&status, self.session(&parts.headers)),
                )
            }
            (&Method::GET, "/api/apps") => {
                let status = self.settled(query).await;
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
                } else if let Some(place) = path.strip_prefix("/api/keyvalue/") {
                    match *method {
                        Method::DELETE => self.purge(place).await,
                        _ => error(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "a keyvalue namespace takes DELETE",
                        ),
                    }
                } else {
                    error(StatusCode::NOT_FOUND, format!("nothing at {path}"))
                }
            }
        }
    }

    /// The answer refusing a request without the token or a session; without
    /// either configured, one for a host name but localhost (DNS rebinding);
    /// and an edit from another origin. None: the request is admitted.
    fn refusal(&self, parts: &Parts) -> Option<Response> {
        let headers = &parts.headers;
        let host = headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or_default();
        let token = self
            .token
            .as_deref()
            .is_some_and(|token| authorized(headers.get(header::AUTHORIZATION), token));
        match (&self.token, &self.sign_in) {
            (None, None) if !local_authority(host) => {
                return Some(error(
                    StatusCode::FORBIDDEN,
                    "without --console-token or --oidc-issuer the console answers requests for \
                     an IP address or localhost only",
                ));
            }
            (None, None) => {}
            _ if token || self.session(headers).is_some() => {}
            (token, sign_in) => {
                // a browser's page: to the sign-in; an API request: 401
                let page = sign_in.is_some()
                    && parts.method == Method::GET
                    && !parts.uri.path().starts_with("/api/")
                    && headers
                        .get(header::ACCEPT)
                        .and_then(|accept| accept.to_str().ok())
                        .is_some_and(|accept| accept.contains("text/html"));
                if page {
                    return Some(redirect("/login", None));
                }
                let mut response = error(
                    StatusCode::UNAUTHORIZED,
                    if sign_in.is_some() {
                        "the console needs a sign-in (/login), or its token"
                    } else {
                        "the console needs its token"
                    },
                );
                // the browser's prompt, where the token is the way in
                if token.is_some() && sign_in.is_none() {
                    response.headers_mut().insert(
                        header::WWW_AUTHENTICATE,
                        HeaderValue::from_static(
                            "Basic realm=\"sluice-wicket-wasi console\", charset=\"UTF-8\"",
                        ),
                    );
                }
                return Some(response);
            }
        }
        let edit = !matches!(parts.method, Method::GET | Method::HEAD);
        let foreign = headers.get(header::ORIGIN).is_some_and(|origin| {
            let authority = origin
                .to_str()
                .ok()
                .and_then(|origin| origin.split_once("://"))
                .map(|(_, authority)| authority);
            !authority.is_some_and(|authority| authority.eq_ignore_ascii_case(host))
        });
        (edit && foreign).then(|| {
            error(
                StatusCode::FORBIDDEN,
                "an edit from another origin is refused",
            )
        })
    }

    /// Who the session cookie of `headers` names, when it is sealed with the
    /// key and not expired.
    fn session(&self, headers: &http::HeaderMap) -> Option<String> {
        let signer = &self.sign_in.as_ref()?.signer;
        let sealed = headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|cookies| cookies.to_str().ok())
            .flat_map(|cookies| cookies.split(';'))
            .map(str::trim)
            .find_map(|cookie| {
                cookie
                    .strip_prefix(SESSION_COOKIE)
                    .and_then(|rest| rest.strip_prefix('='))
            })?;
        let session: Session = serde_json::from_slice(&signer.verify(sealed)?).ok()?;
        (session.exp > sluice_oidc::epoch_secs()).then_some(session.user)
    }

    /// `/login`, `/oauth2/callback` and `/logout`: the sign-in through the
    /// provider, the session set by the callback.
    async fn sign_in_route(&self, sign_in: &SignIn, parts: &Parts) -> Response {
        let headers = &parts.headers;
        let host = headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or("localhost");
        // the console speaks plain HTTP: https is a proxy's ahead of it
        let https = headers
            .get("x-forwarded-proto")
            .and_then(|proto| proto.to_str().ok())
            .is_some_and(|proto| proto == "https");
        let callback = format!(
            "{}://{host}/oauth2/callback",
            if https { "https" } else { "http" }
        );
        let query: Vec<(String, String)> =
            form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        let q = |key: &str| {
            query
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, value)| value.as_str())
        };
        if parts.method != Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED, "the sign-in takes GET");
        }
        match parts.uri.path() {
            "/login" => match sign_in.provider.begin(&callback).await {
                Ok(location) => redirect(&location, None),
                Err(e) => page(
                    StatusCode::BAD_GATEWAY,
                    "The provider is unreachable",
                    format!("<p>{}</p>", escape(&e)),
                ),
            },
            "/logout" => redirect("/", Some(cookie("", 0, https))),
            _ => {
                if let Some(error) = q("error") {
                    return page(
                        StatusCode::BAD_REQUEST,
                        "The provider refused the sign-in",
                        format!(
                            "<p>{}: {}</p>",
                            escape(error),
                            escape(q("error_description").unwrap_or("no reason given"))
                        ),
                    );
                }
                let (Some(code), Some(state)) = (q("code"), q("state")) else {
                    return page(
                        StatusCode::BAD_REQUEST,
                        "Not a callback",
                        "<p>The provider sent no code and state: <a href=\"/login\">sign in again</a>.</p>".into(),
                    );
                };
                match sign_in.provider.finish(code, state, &callback).await {
                    Ok(identity) => {
                        let session = Session {
                            user: identity.user_name,
                            exp: sluice_oidc::epoch_secs() + SESSION_MAX_AGE.as_secs(),
                        };
                        let sealed = sign_in
                            .signer
                            .sign(&serde_json::to_vec(&session).expect("plain data"));
                        redirect("/", Some(cookie(&sealed, SESSION_MAX_AGE.as_secs(), https)))
                    }
                    Err(e) if e.starts_with("the claim ") => page(
                        StatusCode::FORBIDDEN,
                        "Not allowed",
                        format!(
                            "<p>{}</p><p><a href=\"/login\">Sign in again</a></p>",
                            escape(&e)
                        ),
                    ),
                    Err(e) => page(
                        StatusCode::BAD_REQUEST,
                        "The sign-in failed",
                        format!(
                            "<p>{}</p><p><a href=\"/login\">Sign in again</a></p>",
                            escape(&e)
                        ),
                    ),
                }
            }
        }
    }

    /// The status once the routes moved past `after` of `query` (the current
    /// revision) or [`LONG_POLL`] passed; at once without `after`.
    async fn settled(&self, query: Option<&str>) -> Status {
        let after = form_urlencoded::parse(query.unwrap_or_default().as_bytes())
            .find(|(key, _)| key == "after")
            .and_then(|(_, value)| value.parse::<u64>().ok());
        let mut status = self.status.clone();
        if let Some(after) = after {
            let changed = status.wait_for(|status| status.revision != after);
            let ended = matches!(tokio::time::timeout(LONG_POLL, changed).await, Ok(Err(_)));
            if ended {
                // the reloader ended (the drain): nothing changes from now on
                tokio::time::sleep(LONG_POLL).await;
            }
        }
        status.borrow().clone()
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
                    sandbox: SandboxView::of(handler.sandbox()),
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
            reload_failure: status.failure.as_ref().map(|failure| FailureView {
                reason: failure.reason.clone(),
                error: failure.error.clone(),
                at: rfc3339(failure.at),
            }),
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
                deployed,
            }) = table.origins.get(&up.host)
            else {
                continue;
            };
            if let Some(app) = apps.iter_mut().find(|app| app.name == *name) {
                app.hosts.push(up.host.clone());
                continue;
            }
            let Some(handler) = table.handler(&up.target_url) else {
                continue;
            };
            apps.push(AppView {
                name: name.clone(),
                owner: owner.clone(),
                hosts: vec![up.host.clone()],
                component: ComponentRef {
                    sha256: handler.loaded().digest().to_string(),
                    name: handler.loaded().label().to_string(),
                },
                sandbox: AppSandboxView {
                    body: SandboxBody::of(handler.sandbox()),
                    env: handler.sandbox().env.names(),
                },
                tag: tag.clone(),
                deployed: deployed.clone(),
            });
        }
        AppsView {
            revision: status.revision,
            generation: table.generation,
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
            .map(|(name, kind, default)| StoreView {
                name: name.to_string(),
                kind,
                default,
            })
            .collect()
    }

    async fn deploy(&self, host: &str, parts: &Parts, body: Incoming) -> Response {
        let component = match component(parts, body).await {
            Ok(component) => component,
            Err(refused) => return refused.into(),
        };
        let host = match deployed::check_host(host) {
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
        let host = match deployed::check_host(host) {
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
            return match deployed::check_host(to) {
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
        match deployed::check_host(host) {
            Ok(host) => self.edit(Edit::Remove { host }).await,
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// Keeps the body under its digest `sha256` for an app to name.
    async fn upload(&self, sha256: &str, parts: &Parts, body: Incoming) -> Response {
        let component = match component(parts, body).await {
            Ok(component) => component,
            Err(refused) => return refused.into(),
        };
        let sha256 = match deployed::check_digest(sha256) {
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

    /// Serves the app `name` as the body has it; every check of its own
    /// before the reloader's, so a malformed app is a 400 first.
    async fn put_app(&self, name: &str, parts: &Parts, body: Incoming) -> Response {
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
                self.edit(Edit::PutApp { name, app }).await
            }
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        }
    }

    /// The app `name` of `body`, checked; its keyvalue map names the stores.
    fn app_spec(&self, name: &str, body: AppBody) -> Result<AppSpec, String> {
        deployed::check_name("app", name)?;
        deployed::check_name("owner", &body.owner)?;
        let sha256 = deployed::check_digest(&body.component.sha256)
            .map_err(|e| format!("component.sha256: {e}"))?;
        if let Some(tag) = &body.tag {
            deployed::check_tag(tag)?;
        }
        Ok(AppSpec {
            owner: body.owner,
            hosts: deployed::check_hosts(&body.hosts)?,
            sha256,
            file: body.component.name,
            sandbox: body.sandbox.sandbox(&self.stores)?,
            tag: body.tag,
        })
    }

    async fn delete_app(&self, name: &str, query: Option<&str>) -> Response {
        let owner = pairs(query)
            .into_iter()
            .find(|(key, _)| key == "owner")
            .map(|(_, owner)| owner);
        let checked = deployed::check_name("app", name).and_then(|()| match owner {
            Some(owner) => deployed::check_name("owner", &owner).map(|()| owner),
            None => Err("owner=<owner> names the app's owner: a controller removes its own".into()),
        });
        match checked {
            Ok(owner) => {
                let name = name.to_string();
                self.edit(Edit::DeleteApp { name, owner }).await
            }
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

    /// Deletes the data of the keyvalue namespace `place`
    /// (`<store>/<namespace>`, percent-decoded: the namespace may hold `/`).
    async fn purge(&self, place: &str) -> Response {
        let place = percent_encoding::percent_decode_str(place).decode_utf8_lossy();
        let place = match place.split_once('/') {
            Some((store, namespace)) if !store.is_empty() && !namespace.is_empty() => Place {
                store: store.to_string(),
                namespace: namespace.to_string(),
            },
            _ => {
                return error(
                    StatusCode::BAD_REQUEST,
                    format!("'{place}' is no keyvalue <store>/<namespace>"),
                );
            }
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
                    Refused::Rejected(unloaded) => refusal.view.host = unloaded.host,
                    Refused::Missing(sha256) => refusal.view.missing = vec![sha256],
                    _ => {}
                }
                Err(refusal)
            }
            Err(_) => Err(draining()),
        }
    }
}

/// The component an upload carries, read before anything is answered: a
/// refusal answered before would be lost to the reset of the connection
/// (see [`drain`]).
async fn component(parts: &Parts, body: Incoming) -> Result<Bytes, Refusal> {
    let too_large = format!("a component is {} MiB at most", MAX_COMPONENT_BYTES >> 20);
    read(parts, body, MAX_COMPONENT_BYTES, &too_large).await
}

/// The body of a request, `limit` bytes at most (else 413, `too_large`).
async fn read(
    parts: &Parts,
    body: Incoming,
    limit: usize,
    too_large: &str,
) -> Result<Bytes, Refusal> {
    let declared = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|length| length.to_str().ok())
        .and_then(|length| length.parse::<u64>().ok());
    if declared.is_some_and(|length| length > limit as u64) {
        return Err(Refusal::new(StatusCode::PAYLOAD_TOO_LARGE, too_large));
    }
    match Limited::new(body, limit).collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(e) if e.is::<LengthLimitError>() => {
            Err(Refusal::new(StatusCode::PAYLOAD_TOO_LARGE, too_large))
        }
        Err(e) => Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            format!("the upload failed: {e}"),
        )),
    }
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

/// Reads `body` to its end, [`MAX_COMPONENT_BYTES`] at most, and drops it.
/// hyper reads no further than a request's answer, and a connection closed
/// with bytes unread is reset, which may lose the answer to the client: a
/// refusal of an upload would read as a network error.
async fn drain(mut body: Incoming) {
    let mut read = 0;
    while let Some(Ok(frame)) = body.frame().await {
        read += frame.data_ref().map_or(0, Bytes::len);
        if read > MAX_COMPONENT_BYTES {
            return;
        }
    }
}

/// The pairs of an edit's query, url-decoded.
fn pairs(query: Option<&str>) -> Vec<(String, String)> {
    form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .into_owned()
        .collect()
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

/// Whether `authorization` carries `token`: as the password of HTTP Basic
/// (any user name) or as a Bearer token. Compared in a time that does not tell
/// where they differ.
fn authorized(authorization: Option<&HeaderValue>, token: &str) -> bool {
    let Some((scheme, credentials)) = authorization
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().split_once(' '))
    else {
        return false;
    };
    let given = if scheme.eq_ignore_ascii_case("bearer") {
        credentials.trim().as_bytes().to_vec()
    } else if scheme.eq_ignore_ascii_case("basic") {
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(credentials.trim())
        else {
            return false;
        };
        match decoded.iter().position(|&byte| byte == b':') {
            Some(colon) => decoded[colon + 1..].to_vec(),
            None => return false,
        }
    } else {
        return false;
    };
    given.len() == token.len()
        && given
            .iter()
            .zip(token.as_bytes())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

/// Whether `authority` (a `Host` header) names an IP address or localhost.
fn local_authority(authority: &str) -> bool {
    let Ok(authority) = authority.parse::<http::uri::Authority>() else {
        return false;
    };
    let host = authority.host();
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.parse::<std::net::IpAddr>().is_ok() || host.eq_ignore_ascii_case("localhost")
}

/// A redirect to `location`, setting `cookie` when there is one.
fn redirect(location: &str, cookie: Option<String>) -> Response {
    let mut response = hyper::Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(cookie) = cookie {
        response = response.header(header::SET_COOKIE, cookie);
    }
    response
        .body(Full::new(Bytes::new()))
        .expect("a plain response")
}

/// The session cookie holding `sealed` for `max_age` seconds (0: cleared).
fn cookie(sealed: &str, max_age: u64, https: bool) -> String {
    format!(
        "{SESSION_COOKIE}={sealed}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{}",
        if https { "; Secure" } else { "" }
    )
}

/// A page of the sign-in: `title` and the `body` fragment (the page's own
/// style is behind the sign-in).
fn page(status: StatusCode, title: &str, body: String) -> Response {
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::from(format!(
            "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{title} - Sluice wicket</title></head><body><h1>{title}</h1>{body}</body></html>"
        ))))
        .expect("a plain response")
}

/// `text` safe inside an HTML element.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn asset(content_type: &'static str, body: &'static [u8]) -> Response {
    hyper::Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        // they change with the binary: revalidated, so an upgrade shows
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Full::new(Bytes::from_static(body)))
        .expect("a valid response")
}

fn json(status: StatusCode, body: &impl Serialize) -> Response {
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(body).expect("serializable"),
        )))
        .expect("a valid response")
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    Refusal::new(status, message).into()
}

/// The answer refusing a request, before it is one.
struct Refusal {
    status: StatusCode,
    view: ErrorView,
}

impl Refusal {
    fn new(status: StatusCode, message: impl Into<String>) -> Refusal {
        Refusal {
            status,
            view: ErrorView {
                error: message.into(),
                host: None,
                missing: Vec::new(),
            },
        }
    }
}

impl From<Refusal> for Response {
    fn from(refusal: Refusal) -> Response {
        json(refusal.status, &refusal.view)
    }
}

#[derive(Serialize)]
struct ErrorView {
    error: String,
    /// The host of the route that did not load (422).
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    /// The digests of the components an app named and no upload kept (409).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    missing: Vec<String>,
}

#[derive(Serialize)]
struct Edited {
    generation: u64,
    message: String,
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
    reload_failure: Option<FailureView>,
    routes: Vec<RouteView>,
}

#[derive(Serialize)]
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
}

#[derive(Serialize)]
struct FailureView {
    reason: String,
    error: String,
    at: String,
}

#[derive(Serialize)]
struct RouteView {
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
struct SandboxView {
    wasi: Vec<&'static str>,
    budget_ms: u128,
    memory_mib: usize,
    pool: usize,
    instances: Option<usize>,
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
            pool: sandbox.pool,
            instances: sandbox.instances,
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

/// The apps, and what their sandboxes may hold.
#[derive(Serialize)]
struct AppsView {
    revision: u64,
    generation: u64,
    capabilities: &'static [&'static str],
    keyvalue_stores: Vec<StoreView>,
    apps: Vec<AppView>,
}

/// An app as listed: what its PUT takes -- but the env, named only -- its
/// name, and since when it serves this component and sandbox.
#[derive(Serialize)]
struct AppView {
    name: String,
    owner: String,
    hosts: Vec<String>,
    component: ComponentRef,
    sandbox: AppSandboxView,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
    deployed: String,
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
    #[serde(default)]
    sandbox: SandboxBody,
    /// Opaque to the console, kept and listed as given
    /// ([`deployed::check_tag`]).
    tag: Option<String>,
}

/// A component uploaded by digest, and the file name it is kept as.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ComponentRef {
    sha256: String,
    name: String,
}

/// An app's sandbox: the `--wasm` route options, its keyvalue map
/// (`identifier -> store/namespace`) and its env (`name -> value`). A key
/// left out takes its default; a listing names every one but the env
/// ([`AppSandboxView`]).
#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
struct SandboxBody {
    #[serde(default)]
    wasi: Vec<String>,
    budget_ms: Option<u64>,
    memory_mib: Option<u64>,
    #[serde(default)]
    pool: usize,
    instances: Option<usize>,
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
            pool: sandbox.pool,
            instances: sandbox.instances,
            keyvalue: deployed::map_specs(&sandbox.keyvalue),
            env: BTreeMap::new(),
        }
    }

    /// The sandbox, its map naming `stores`; errors name the key.
    fn sandbox(self, stores: &Stores) -> Result<Sandbox, String> {
        let mut sandbox = Sandbox {
            caps: Caps::parse(&self.wasi.join(",")).map_err(|e| format!("sandbox.wasi: {e}"))?,
            pool: self.pool,
            ..Sandbox::default()
        };
        match self.budget_ms {
            Some(0) => return Err("sandbox.budget_ms: must be at least 1".into()),
            Some(budget) => sandbox.limits.budget = Duration::from_millis(budget),
            None => {}
        }
        match self.memory_mib {
            Some(0) => return Err("sandbox.memory_mib: must be at least 1".into()),
            Some(memory) => {
                sandbox.limits.memory = memory
                    .checked_mul(1 << 20)
                    .and_then(|bytes| usize::try_from(bytes).ok())
                    .ok_or_else(|| format!("sandbox.memory_mib: too large '{memory}'"))?;
            }
            None => {}
        }
        sandbox.instances = match self.instances {
            Some(0) => return Err("sandbox.instances: must be at least 1".into()),
            cap => cap,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The token is the HTTP Basic password, under any user name, or a Bearer
    /// token; without a token the console listens on loopback only and
    /// answers only a request for an IP address or localhost.
    #[tokio::test]
    async fn the_token_and_the_local_authorities() {
        let header = |value: String| HeaderValue::from_str(&value).unwrap();
        let basic = |credentials: &str| {
            header(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(credentials)
            ))
        };
        for given in [
            basic("anyone:s3cret"),
            basic(":s3cret"),
            header("bearer s3cret".into()),
        ] {
            assert!(authorized(Some(&given), "s3cret"), "{given:?}");
        }
        for given in [
            basic("s3cret:"),
            basic("s3cret"),
            header("Basic !!".into()),
            header("Bearer s3cre".into()),
            header("Bearer s3crets".into()),
            header("Digest s3cret".into()),
        ] {
            assert!(!authorized(Some(&given), "s3cret"), "{given:?}");
        }
        assert!(!authorized(None, "s3cret"));

        // refused before anything listens: no socket on these addresses
        for exposed in ["0.0.0.0:0", "[::]:0", "192.0.2.1:9090"] {
            let refused = bind(exposed, false).await.unwrap_err();
            assert!(
                refused.ends_with("needs --console-token or --oidc-issuer"),
                "{refused}"
            );
        }
        for loopback in ["127.0.0.1:0", "localhost:0"] {
            bind(loopback, false).await.unwrap();
        }

        for local in [
            "127.0.0.1:9090",
            "[::1]:9090",
            "localhost:9090",
            "LOCALHOST",
            "10.0.0.1",
        ] {
            assert!(local_authority(local), "{local}");
        }
        for foreign in ["rebound.example:9090", "localhost.example", "wicket", ""] {
            assert!(!local_authority(foreign), "{foreign}");
        }
    }
}
