//! The console (`--console <host:port>`): a page and a JSON API over the
//! routes. A component uploaded -- or dropped on the page -- is deployed as a
//! route at once, without a restart: the console hands the edit to the
//! reloader (`routes::Reloader::apply`), which reloads the routes with it and
//! keeps them only when they loaded, the console's routes in its directory
//! (`deployed`).
//!
//! ```text
//! GET    /api/routes[?after=<revision>]         the routes; held until they change when
//!                                               `after` is the current revision
//! PUT    /api/routes/<host>?name=..&<options>   deploys the body, a component, to the host
//! PATCH  /api/routes/<host>?<options>           gives the console's route another sandbox
//! PATCH  /api/routes/<host>?host=<to>           moves the console's route to another host
//! DELETE /api/routes/<host>                     removes the console's route
//! ```
//!
//! The options are the `--wasm` route's (`budget-ms`, `instances`,
//! `memory-mib`, `pool`, `wasi`), the whole sandbox: an option left out takes
//! its default.
//!
//! With `--console-token` every request needs the token, as the password of
//! HTTP Basic (any user name: the browser asks for it) or as a Bearer token.
//! Without one the console listens on a loopback address only and answers
//! only requests for an IP address or `localhost`, so a DNS rebinding page
//! cannot reach it. Edits take no simple method -- neither a form nor
//! `sendBeacon` can send one -- and one from another origin is refused.

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
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

use crate::deployed::{self, rfc3339};
use crate::routes::{Edit, Origin, Outcome, Refused, Request, Routes, Status};
use crate::wasm_host::{Caps, Limits, Sandbox};

/// Largest component an upload may carry.
pub const MAX_COMPONENT_BYTES: usize = 128 << 20;

/// How long `GET /api/routes?after=<the current revision>` waits for a change.
const LONG_POLL: Duration = Duration::from_secs(25);

/// Connections open at once; one beyond waits in the backlog.
const MAX_CONNECTIONS: usize = 64;

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
    /// None: no credentials asked, on a loopback address.
    pub token: Option<String>,
    /// What the page shows besides the routes.
    pub server: Option<String>,
    pub listen: Option<SocketAddr>,
}

/// Binds `address`; without a token, a loopback address only, checked before
/// anything listens.
pub async fn bind(address: &str, token: bool) -> Result<TcpListener, String> {
    let failed = |e: std::io::Error| format!("--console {address}: {e}");
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(address)
        .await
        .map_err(failed)?
        .collect();
    if !token && resolved.iter().any(|at| !at.ip().is_loopback()) {
        return Err(format!(
            "--console {address}: a console on a non-loopback address needs --console-token"
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
        let mut response = match self.refusal(&parts) {
            Some(refused) => refused,
            None => self.route(&parts, &mut body).await,
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
        match (method, path) {
            (&Method::GET, "/") => asset("text/html; charset=utf-8", INDEX.as_bytes()),
            (&Method::GET, "/console.css") => asset("text/css; charset=utf-8", CSS.as_bytes()),
            (&Method::GET, "/console.js") => asset("text/javascript; charset=utf-8", JS.as_bytes()),
            (&Method::GET, "/fonts/overpass.woff2") => asset("font/woff2", OVERPASS),
            (&Method::GET, "/fonts/overpass-mono.woff2") => asset("font/woff2", OVERPASS_MONO),
            (&Method::GET, "/api/routes") => self.list(query).await,
            _ => match path.strip_prefix("/api/routes/") {
                Some(host) if method == Method::PUT => {
                    let body = body.take().expect("not read yet");
                    self.deploy(host, parts, body).await
                }
                Some(host) if method == Method::PATCH => self.patch(host, query).await,
                Some(host) if method == Method::DELETE => self.remove(host).await,
                Some(_) => error(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "a route takes PUT, PATCH or DELETE",
                ),
                None => error(StatusCode::NOT_FOUND, format!("nothing at {path}")),
            },
        }
    }

    /// The answer refusing a request without the token; without a token, one
    /// for a host name but localhost (DNS rebinding); and an edit from another
    /// origin. None: the request is admitted.
    fn refusal(&self, parts: &Parts) -> Option<Response> {
        let headers = &parts.headers;
        let host = headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or_default();
        match &self.token {
            Some(token) if !authorized(headers.get(header::AUTHORIZATION), token) => {
                let mut response = error(StatusCode::UNAUTHORIZED, "the console needs its token");
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static(
                        "Basic realm=\"sluice-wicket-wasi console\", charset=\"UTF-8\"",
                    ),
                );
                return Some(response);
            }
            None if !local_authority(host) => {
                return Some(error(
                    StatusCode::FORBIDDEN,
                    "without --console-token the console answers requests for an IP address \
                     or localhost only",
                ));
            }
            _ => {}
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

    /// The routes; with `after` the current revision, once they changed or
    /// [`LONG_POLL`] passed.
    async fn list(&self, query: Option<&str>) -> Response {
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
        let status = status.borrow().clone();
        json(StatusCode::OK, &self.listing(&status))
    }

    fn listing(&self, status: &Status) -> Listing {
        let table = self.routes.current();
        let routes = table
            .advertised
            .iter()
            .filter_map(|up| {
                let handler = table.handler(&up.target_url)?;
                let (origin, deployed) = match table.origins.get(&up.host) {
                    Some(Origin::Console { deployed }) => ("console", Some(deployed.clone())),
                    Some(Origin::Config) => ("config", None),
                    _ => ("flag", None),
                };
                let loaded = handler.loaded();
                Some(RouteView {
                    host: up.host.clone(),
                    origin,
                    deployed,
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
            server: self.server.clone(),
            listen: self.listen.map(|listen| listen.to_string()),
            max_component_bytes: MAX_COMPONENT_BYTES,
            capabilities: &Caps::NAMES,
            defaults: Defaults {
                budget_ms: defaults.budget.as_millis(),
                memory_mib: defaults.memory >> 20,
            },
            reload_failure: status.failure.as_ref().map(|failure| FailureView {
                reason: failure.reason.clone(),
                error: failure.error.clone(),
                at: rfc3339(failure.at),
            }),
            routes,
        }
    }

    /// Reads the component before anything else: a refusal answered before
    /// would be lost to the reset of the connection (see [`drain`]).
    async fn deploy(&self, host: &str, parts: &Parts, body: Incoming) -> Response {
        let too_large = || {
            error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("a component is {} MiB at most", MAX_COMPONENT_BYTES >> 20),
            )
        };
        let declared = parts
            .headers
            .get(header::CONTENT_LENGTH)
            .and_then(|length| length.to_str().ok())
            .and_then(|length| length.parse::<u64>().ok());
        if declared.is_some_and(|length| length > MAX_COMPONENT_BYTES as u64) {
            return too_large();
        }
        let component = match Limited::new(body, MAX_COMPONENT_BYTES).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) if e.is::<LengthLimitError>() => return too_large(),
            Err(e) => return error(StatusCode::BAD_REQUEST, format!("the upload failed: {e}")),
        };
        let host = match deployed::check_host(host) {
            Ok(host) => host,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let (name, sandbox) = match options(&pairs(parts.uri.query()), true) {
            Ok(options) => options,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        if component.is_empty() {
            return error(
                StatusCode::BAD_REQUEST,
                "no component: send the .wasm file as the request body",
            );
        }
        if !component.starts_with(b"\0asm") {
            return error(
                StatusCode::BAD_REQUEST,
                "not a wasm binary (it does not start with \\0asm)",
            );
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

    /// Has the reloader apply `edit`, and answers its outcome.
    async fn edit(&self, edit: Edit) -> Response {
        let draining = || {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "the wicket is draining: its routes change no more",
            )
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
    json(
        status,
        &ErrorView {
            error: message.into(),
        },
    )
}

#[derive(Serialize)]
struct ErrorView {
    error: String,
}

#[derive(Serialize)]
struct Edited {
    generation: u64,
    message: String,
}

#[derive(Serialize)]
struct Listing {
    revision: u64,
    generation: u64,
    build: String,
    server: Option<String>,
    listen: Option<String>,
    max_component_bytes: usize,
    capabilities: &'static [&'static str],
    defaults: Defaults,
    reload_failure: Option<FailureView>,
    routes: Vec<RouteView>,
}

#[derive(Serialize)]
struct Defaults {
    budget_ms: u128,
    memory_mib: usize,
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
        }
    }
}

#[derive(Serialize)]
struct BucketView {
    identifier: String,
    store: String,
    namespace: String,
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
            assert!(refused.ends_with("needs --console-token"), "{refused}");
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
