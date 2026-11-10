//! The HTTP server: the subset of the Cloud Controller v3 API the cf CLI
//! uses, the UAA endpoints (`/uaa`) and log-cache's read API (`/api/v1`) on
//! one origin, in CC's shapes (an error is `{"errors": [{"code", "title",
//! "detail"}]}`). Every request but the root document, `/v3/info` and the
//! UAA endpoints needs a token of [`Uaa`]; with a [`Provider`], the browser
//! pages `/uaa/passcode` and `/uaa/callback` of `cf login --sso`.
//!
//! Routes and `instances` apply at once: an edit a wicket refuses is
//! answered 422 with its reason and changes nothing. The droplet and the
//! rest of the sandbox, the service bindings' grants and the environment
//! variables included, apply at a start, restart or deployment: the running
//! app is the snapshot taken then, and the outcome shows in the process
//! stats and the log. The services are in [`crate::services`], the
//! environment variables in [`crate::env`], the security groups, which
//! apply at a start too, in [`crate::security_groups`].

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::join_all;
use http::header::{HeaderMap, HeaderValue};
use http::{Method, StatusCode, header};
use http_body_util::{BodyExt as _, Full, Limited};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sluice_wicket_console::http::asset;
use sluice_wicket_server::accept::{self, Slot};
use sluice_wicket_server::http::{self as connection, Body};
use tokio::net::TcpListener;

use crate::controller::{Controller, Held};
use crate::deployments;
use crate::env;
use crate::logs::{self, Logs};
use crate::manifest;
use crate::oidc::Provider;
use crate::registry::{Reference, Registry};
use crate::roles::{Access, Need};
use crate::security_groups;
use crate::services;
use crate::staging::{self, Hashed};
use crate::store::{
    BUDGET_ANNOTATION, Blob, Build, Deployment, DockerImage, JobError, Lifecycle, Outcome, Package,
    Running, Spool, State, Store, epoch_secs, guid, now,
};
use crate::uaa::{self, Uaa};
use crate::views;
use crate::wicket::{AppBody, Failure, ListedApp, Runtime, Wicket};

/// Largest package upload, and component pulled.
pub(crate) const MAX_UPLOAD_BYTES: usize = 160 << 20;
/// Largest body of any other request.
const MAX_BODY_BYTES: usize = 1 << 20;
/// Connections open at once; one beyond takes the place of one without an
/// authenticated request, or waits in the backlog for one.
pub(crate) const MAX_CONNECTIONS: usize = 64;
/// How long a build's request waits for its staging: a longer pull goes on,
/// the CLI polling the build (every 3s).
const STAGING_WAIT: std::time::Duration = std::time::Duration::from_secs(15);

pub(crate) type Response = hyper::Response<Full<Bytes>>;
pub(crate) type Reply = Result<Response, ApiError>;

/// The shares an edit put before its commit ([`Api::try_shares`]).
#[derive(Default)]
struct Tried {
    /// The wicket and the app of each: what goes back on a refusal.
    put: Vec<(Arc<Wicket>, String)>,
    /// Their puts, held until the edit is committed or gone back on.
    held: Vec<Held>,
}

pub struct Api {
    pub store: Arc<Store>,
    pub uaa: Uaa,
    /// Who is an admin, and the roles the provider's groups give.
    pub policy: crate::roles::Policy,
    /// Who may edit the security groups.
    pub security_group_editors: crate::security_groups::Editors,
    /// Who may edit the service brokers.
    pub service_broker_editors: crate::security_groups::Editors,
    /// The brokers' client, and their operations under way.
    pub osb: crate::osb::Client,
    pub operations: Arc<crate::operations::Operations>,
    /// The OpenID Provider users log in through; None: any credentials.
    pub provider: Option<Provider>,
    /// Whether the listener serves TLS: the scheme of the links.
    pub tls: bool,
    pub logs: Arc<Logs>,
    pub controller: Arc<Controller>,
    /// Where docker packages' images are pulled from.
    pub registry: Registry,
    /// What `/v3/info` names as the build.
    pub build: String,
}

/// An error in CC's shape.
#[derive(Clone, Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: u32,
    pub title: &'static str,
    pub detail: String,
}

impl ApiError {
    fn internal(e: &tokio::task::JoinError) -> ApiError {
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: 10001,
            title: "UnknownError",
            detail: format!("the request failed: {e}"),
        }
    }

    pub fn not_found(what: &str) -> ApiError {
        ApiError {
            status: StatusCode::NOT_FOUND,
            code: 10010,
            title: "CF-ResourceNotFound",
            detail: format!("{what} not found"),
        }
    }

    pub fn unprocessable(detail: impl Into<String>) -> ApiError {
        ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: 10008,
            title: "CF-UnprocessableEntity",
            detail: detail.into(),
        }
    }

    pub fn bad_request(detail: impl Into<String>) -> ApiError {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            code: 1001,
            title: "CF-MessageParseError",
            detail: detail.into(),
        }
    }

    pub(crate) fn unknown(method: &Method, path: &str) -> ApiError {
        ApiError {
            status: StatusCode::NOT_FOUND,
            code: 10000,
            title: "CF-NotFound",
            detail: format!("Unknown request: {method} {path}"),
        }
    }

    fn unauthenticated() -> ApiError {
        ApiError {
            status: StatusCode::UNAUTHORIZED,
            code: 10002,
            title: "CF-NotAuthenticated",
            detail: "Authentication error".into(),
        }
    }

    fn invalid_token(why: &str) -> ApiError {
        ApiError {
            status: StatusCode::UNAUTHORIZED,
            code: 1000,
            title: "CF-InvalidAuthToken",
            detail: format!("Invalid Auth Token: {why}"),
        }
    }

    pub(crate) fn not_authorized(why: &str) -> ApiError {
        ApiError {
            status: StatusCode::FORBIDDEN,
            code: 10003,
            title: "CF-NotAuthorized",
            detail: format!("You are not authorized to perform the requested action: {why}"),
        }
    }

    fn too_large(limit: usize) -> ApiError {
        ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: 10013,
            title: "CF-AppBitsUploadInvalid",
            detail: format!("the body is {} MiB at most", limit >> 20),
        }
    }

    /// `wicket`'s refusal, as the user reads it.
    fn refused(wicket: &Wicket, failure: &Failure) -> ApiError {
        ApiError::unprocessable(format!("{} refused it: {failure}", wicket.label))
    }

    fn job_error(&self) -> JobError {
        JobError {
            code: self.code,
            title: self.title.into(),
            detail: self.detail.clone(),
        }
    }
}

impl From<ApiError> for Response {
    fn from(error: ApiError) -> Response {
        json(
            error.status,
            &json!({"errors": [{"code": error.code, "title": error.title, "detail": error.detail}]}),
        )
    }
}

impl From<String> for ApiError {
    fn from(detail: String) -> ApiError {
        ApiError::unprocessable(detail)
    }
}

/// A request, its body read: an upload's to a file.
pub(crate) struct Req {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) query: Vec<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
    /// An upload's body, in a file.
    spool: Option<Spool>,
    /// Where the CLI reaches this API: the links name it.
    pub(crate) base: String,
    /// What the holder of its token may; why not without a valid one.
    access: Result<Access, ApiError>,
}

impl Req {
    pub(crate) fn q(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, value)| value.as_str())
    }

    pub(crate) fn json<T: DeserializeOwned>(&self) -> Result<T, ApiError> {
        serde_json::from_slice(&self.body)
            .map_err(|e| ApiError::bad_request(format!("Request invalid due to parse error: {e}")))
    }

    /// What the holder of its token may.
    pub(crate) fn access(&self) -> Result<&Access, ApiError> {
        self.access.as_ref().map_err(ApiError::clone)
    }

    fn header(&self, name: header::HeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

/// Serves the API on `listener` for good, [`MAX_CONNECTIONS`] at once; with
/// `tls`, each connection's handshake first.
pub async fn run(listener: TcpListener, tls: Option<tokio_rustls::TlsAcceptor>, api: Arc<Api>) {
    accept::run(listener, MAX_CONNECTIONS, "--listen", |stream, _, slot| {
        let api = api.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let Some(acceptor) = tls else {
                return serve_connection(stream, slot, api).await;
            };
            // a client quiet in its handshake is no client hyper bounds yet
            slot.claim().yields();
            let handshake = tokio::time::timeout(connection::QUIET, acceptor.accept(stream));
            let handshake = tokio::select! {
                handshake = handshake => handshake,
                () = slot.asked() => return,
            };
            match handshake {
                Ok(Ok(stream)) => serve_connection(stream, slot, api).await,
                Ok(Err(e)) => eprintln!("tls handshake failed: {e}"),
                Err(e) => eprintln!("tls handshake failed: {e}"),
            }
        });
    })
    .await
}

async fn serve_connection<S>(stream: S, slot: Slot, api: Arc<Api>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    connection::serve(stream, slot, |request| {
        let api = api.clone();
        async move {
            // in a task of its own: an edit goes on when its client gives up (a start may take
            // the health check's whole timeout), the state and the wicket's answer kept
            let handled = tokio::spawn(async move { api.handle(request).await }).await;
            handled.unwrap_or_else(|e| ApiError::internal(&e).into())
        }
    })
    .await
}

impl Api {
    async fn handle(&self, request: hyper::Request<Body>) -> Response {
        let (parts, body) = request.into_parts();
        let path = parts.uri.path().to_string();
        let access = self.token(&parts.headers).map(|token| {
            let identity = self.uaa.identity(&token);
            self.store
                .read(|state| Access::of(state, &token, identity.as_ref(), &self.policy))
        });
        let authenticated = access.is_ok();
        if authenticated {
            body.admit();
        }
        // a package to a file as it comes: it may be larger than this process's memory; and
        // only from whoever may leave one, the others refused below, their body unread.
        // Any other body is small
        let (body, spool) = if path.ends_with("/upload") {
            if authenticated {
                match self.spooled(body).await {
                    Ok(spool) => (Bytes::new(), Some(spool)),
                    Err(error) => return error.into(),
                }
            } else {
                (Bytes::new(), None)
            }
        } else {
            match Limited::new(body, MAX_BODY_BYTES).collect().await {
                Ok(collected) => (collected.to_bytes(), None),
                Err(e)
                    if e.downcast_ref::<http_body_util::LengthLimitError>()
                        .is_some() =>
                {
                    return ApiError::too_large(MAX_BODY_BYTES).into();
                }
                Err(e) => return ApiError::bad_request(format!("the body: {e}")).into(),
            }
        };
        let host = parts
            .headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or("127.0.0.1")
            .to_string();
        let forwarded_https = parts
            .headers
            .get("x-forwarded-proto")
            .and_then(|proto| proto.to_str().ok())
            .is_some_and(|proto| proto == "https");
        let scheme = if self.tls || forwarded_https {
            "https"
        } else {
            "http"
        };
        let req = Req {
            method: parts.method,
            query: form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect(),
            headers: parts.headers,
            body,
            spool,
            base: format!("{scheme}://{host}"),
            path,
            access,
        };
        let response = match self.dispatch(&req).await {
            Ok(response) => response,
            Err(error) => error.into(),
        };
        println!(
            "{} {} {}{}",
            response.status().as_u16(),
            req.method,
            req.path,
            req.q("names")
                .or_else(|| req.q("hosts"))
                .map_or(String::new(), |name| format!(" ({name})"))
        );
        response
    }

    async fn dispatch(&self, req: &Req) -> Reply {
        let segments: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
        let base = req.base.as_str();
        match (&req.method, segments.as_slice()) {
            (&Method::GET, []) => Ok(json(StatusCode::OK, &views::root(base))),
            (&Method::GET, ["v3"]) => Ok(json(StatusCode::OK, &views::v3(base))),
            (&Method::GET, ["v3", "info"]) => {
                let stacks = self.controller.stacks().await;
                Ok(json(
                    StatusCode::OK,
                    &views::info(base, &self.build, &stacks),
                ))
            }
            (&Method::GET, ["uaa", "login"]) | (&Method::GET, ["login"]) => Ok(json(
                StatusCode::OK,
                &uaa::login_view(&format!("{base}/uaa")),
            )),
            (&Method::POST, ["uaa", "oauth", "token"]) | (&Method::POST, ["oauth", "token"]) => {
                let basic = req
                    .header(header::AUTHORIZATION)
                    .and_then(|value| value.strip_prefix("Basic "))
                    .and_then(|encoded| {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD
                            .decode(encoded.trim())
                            .ok()
                    })
                    .and_then(|decoded| String::from_utf8(decoded).ok())
                    .and_then(|pair| {
                        pair.split_once(':')
                            .map(|(client, secret)| (client.to_string(), secret.to_string()))
                    });
                let issuer = format!("{base}/uaa/oauth/token");
                match self.uaa.grant(&req.body, basic, &issuer) {
                    Ok(tokens) => Ok(json(StatusCode::OK, &tokens)),
                    Err(e) => Ok(json(
                        StatusCode::UNAUTHORIZED,
                        &json!({"error": "unauthorized", "error_description": e}),
                    )),
                }
            }
            (&Method::GET, ["uaa", "token_keys"]) => Ok(json(StatusCode::OK, &json!({"keys": []}))),
            // the pages' style, the wickets' consoles'
            (&Method::GET, ["uaa", "console.css"]) => Ok(asset(
                "text/css; charset=utf-8",
                sluice_wicket_console::serve::CSS.as_bytes(),
            )),
            (&Method::GET, ["uaa", "fonts", "overpass.woff2"]) => {
                Ok(asset("font/woff2", sluice_wicket_console::serve::OVERPASS))
            }
            (&Method::GET, ["uaa", "fonts", "overpass-mono.woff2"]) => Ok(asset(
                "font/woff2",
                sluice_wicket_console::serve::OVERPASS_MONO,
            )),
            (&Method::GET, ["uaa", "passcode"]) => {
                let Some(provider) = &self.provider else {
                    return Ok(page(
                        StatusCode::NOT_FOUND,
                        "No OpenID Provider",
                        "<p>This API has no OpenID Provider configured: <code>cf login</code> takes any credentials.</p>".into(),
                    ));
                };
                match provider.begin(&format!("{base}/uaa/callback")).await {
                    Ok(location) => Ok(redirect(&location)),
                    Err(e) => Ok(page(
                        StatusCode::BAD_GATEWAY,
                        "The provider is unreachable",
                        format!("<p>{}</p>", escape(&e)),
                    )),
                }
            }
            (&Method::GET, ["uaa", "callback"]) => {
                let Some(provider) = &self.provider else {
                    return Err(ApiError::unknown(&req.method, &req.path));
                };
                if let Some(error) = req.q("error") {
                    return Ok(page(
                        StatusCode::BAD_REQUEST,
                        "The provider refused the sign-in",
                        format!(
                            "<p>{}: {}</p>",
                            escape(error),
                            escape(req.q("error_description").unwrap_or("no reason given"))
                        ),
                    ));
                }
                let (Some(code), Some(state)) = (req.q("code"), req.q("state")) else {
                    return Ok(page(
                        StatusCode::BAD_REQUEST,
                        "Not a callback",
                        "<p>The provider sent no code and state: start again at <a href=\"passcode\">passcode</a>.</p>".into(),
                    ));
                };
                match provider
                    .finish(code, state, &format!("{base}/uaa/callback"))
                    .await
                {
                    Ok(identity) => {
                        let user = identity.user_name.clone();
                        // what the groups give: who sees less than expected tells why
                        let groups = self
                            .store
                            .read(|state| self.policy.explain_groups(state, &identity.claims))
                            .map(|groups| format!("<p class=\"hint\">{}</p>", escape(&groups)))
                            .unwrap_or_default();
                        match self.uaa.passcode(identity) {
                            Ok(passcode) => Ok(page(
                                StatusCode::OK,
                                "Temporary Authentication Code",
                                format!(
                                    "<p>Signed in as <b>{}</b>. Paste this code into <code>cf login --sso</code> (or <code>cf login --sso-passcode {passcode}</code>) within {} minutes; it works once.</p><p class=\"passcode\"><code id=\"passcode\">{passcode}</code> <button type=\"button\" id=\"copy\" class=\"button button--primary\">Copy</button></p>{groups}{COPY_SCRIPT}",
                                    escape(&user),
                                    uaa::PASSCODE_TTL.as_secs() / 60
                                ),
                            )),
                            Err(e) => Ok(page(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "No code",
                                format!("<p>{}</p>", escape(&e)),
                            )),
                        }
                    }
                    Err(e) => {
                        let status = if e.starts_with("the claim ") {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        Ok(page(
                            status,
                            if status == StatusCode::FORBIDDEN {
                                "Not allowed"
                            } else {
                                "The sign-in failed"
                            },
                            format!(
                                "<p>{}</p><p><a href=\"passcode\">Start again</a></p>",
                                escape(&e)
                            ),
                        ))
                    }
                }
            }
            _ => {
                self.authenticate(req)?;
                match segments.as_slice() {
                    // the CLI's check of a client given a role (`cf set-org-role --client`)
                    ["uaa", "oauth", "clients", id] if req.method == Method::GET => {
                        match self.uaa.knows_client(id) {
                            true => Ok(json(StatusCode::OK, &json!({"client_id": id}))),
                            false => Ok(json(
                                StatusCode::NOT_FOUND,
                                &json!({"error": "not_found", "error_description": "no such client"}),
                            )),
                        }
                    }
                    ["api", "v1", rest @ ..] => self.log_cache(req, rest),
                    ["v3", rest @ ..] => self.v3(req, rest).await,
                    _ => Err(ApiError::unknown(&req.method, &req.path)),
                }
            }
        }
    }

    fn authenticate(&self, req: &Req) -> Result<(), ApiError> {
        req.access().map(|_| ())
    }

    /// The claims of the request's token.
    pub(crate) fn claims(&self, req: &Req) -> Result<uaa::Claims, ApiError> {
        self.token(&req.headers)
    }

    fn token(&self, headers: &HeaderMap) -> Result<uaa::Claims, ApiError> {
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                value
                    .strip_prefix("bearer ")
                    .or_else(|| value.strip_prefix("Bearer "))
            })
            .ok_or_else(ApiError::unauthenticated)?;
        self.uaa
            .verify(token.trim())
            .map_err(|e| ApiError::invalid_token(&e))
    }

    /// `body` in a file, [`MAX_UPLOAD_BYTES`] at most.
    async fn spooled(&self, mut body: Body) -> Result<Spool, ApiError> {
        use std::io::Write as _;
        let (spool, mut file) = self.store.spool()?;
        let failed = |e: std::io::Error| ApiError::from(format!("{}: {e}", spool.path().display()));
        let mut bytes = 0;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| ApiError::bad_request(format!("the body: {e}")))?;
            let Some(data) = frame.data_ref() else {
                continue;
            };
            bytes += data.len();
            if bytes > MAX_UPLOAD_BYTES {
                return Err(ApiError::too_large(MAX_UPLOAD_BYTES));
            }
            file.write_all(data).map_err(failed)?;
        }
        Ok(spool)
    }

    fn log_cache(&self, req: &Req, segments: &[&str]) -> Reply {
        match (&req.method, segments) {
            (&Method::GET, ["info"]) => Ok(json(
                StatusCode::OK,
                &json!({"version": "3.0.0", "vm_uptime": epoch_secs().to_string()}),
            )),
            (&Method::GET, ["read", app]) => {
                // an app it does not see has no lines for it: neither does one deleted
                let access = req.access()?;
                let seen = self.store.read(|state| {
                    state
                        .app(app)
                        .is_some_and(|found| access.can(state, &found.space, Need::Read))
                });
                if !seen {
                    return Ok(json(StatusCode::OK, &logs::envelopes(app, &[])));
                }
                let number = |key: &str| req.q(key).and_then(|value| value.parse::<u128>().ok());
                let lines = self.logs.read(
                    app,
                    number("start_time").unwrap_or(0),
                    number("end_time"),
                    req.q("limit")
                        .and_then(|limit| limit.parse().ok())
                        .unwrap_or(100),
                    req.q("descending") == Some("true"),
                );
                Ok(json(StatusCode::OK, &logs::envelopes(app, &lines)))
            }
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        }
    }

    async fn v3(&self, req: &Req, segments: &[&str]) -> Reply {
        let base = req.base.as_str();
        let store = &self.store;
        let access = req.access()?;
        let listing = |items: Vec<Value>, included: Option<Value>| {
            Ok(json(
                StatusCode::OK,
                &views::page(
                    base,
                    &req.path,
                    &req.query,
                    views::select(items, &req.query),
                    included,
                ),
            ))
        };
        let empty = || listing(Vec::new(), None);
        // whether the holder reads what the space `space` holds
        let reads = |state: &State, space: &str| access.can(state, space, Need::Read);
        // the apps of `app`'s guid a resource names, read
        let app_read = |state: &State, app: &str| {
            state
                .app(app)
                .is_some_and(|found| reads(state, &found.space))
        };
        match (&req.method, segments) {
            // --- a space's manifest and security groups; the organizations, the spaces, the
            // roles, the users (`crate::orgs`)
            (
                &Method::GET,
                [
                    "spaces",
                    guid,
                    kind @ ("running_security_groups" | "staging_security_groups"),
                ],
            ) => {
                store.read(|state| access.check(state, guid, Need::Read, "Space"))?;
                self.security_groups_of_space(req, guid, *kind == "running_security_groups")
            }
            (&Method::POST, ["spaces", guid, "actions", "apply_manifest"]) => {
                self.apply_manifest(req, guid).await
            }
            (&Method::POST, ["spaces", guid, "manifest_diff"]) => self.manifest_diff(req, guid),
            (
                _,
                [
                    "organizations" | "organization_quotas" | "spaces" | "roles" | "users",
                    ..,
                ],
            ) => self.orgs(req, segments).await,
            // --- the domains, the stack
            (&Method::GET, ["domains"]) => listing(
                store.read(|state| {
                    state
                        .domains
                        .iter()
                        .map(|domain| views::domain(base, domain))
                        .collect()
                }),
                None,
            ),
            (&Method::GET, ["domains", guid]) => store
                .read(|state| state.domain(guid).map(|domain| views::domain(base, domain)))
                .map_or_else(
                    || Err(ApiError::not_found("Domain")),
                    |domain| Ok(json(StatusCode::OK, &domain)),
                ),
            (&Method::POST, ["domains"]) => Err(ApiError::unprocessable(
                "private domains are not supported: the shared domains are the cf-api's --domain",
            )),
            (&Method::DELETE, ["domains", _]) => Err(ApiError::unprocessable(
                "the shared domains are the cf-api's --domain",
            )),
            (&Method::GET, ["stacks"]) => {
                let mut stacks = views::stacks(base, &self.controller.stacks().await);
                if let Some(names) = req.q("names") {
                    let named = |stack: &Value| {
                        let name = stack["name"].as_str().unwrap_or_default();
                        names.split(',').any(|wanted| wanted == name)
                    };
                    stacks.retain(named);
                }
                listing(stacks, None)
            }
            (&Method::GET, ["stacks", guid]) => {
                let stacks = views::stacks(base, &self.controller.stacks().await);
                let found = stacks.into_iter().find(|stack| stack["guid"] == **guid);
                found.map_or_else(
                    || Err(ApiError::not_found("Stack")),
                    |stack| Ok(json(StatusCode::OK, &stack)),
                )
            }
            (&Method::GET, ["feature_flags"]) => listing(
                ["service_instance_sharing", "diego_docker"]
                    .iter()
                    .map(|name| feature_flag(name))
                    .collect(),
                None,
            ),
            (&Method::GET, ["feature_flags", name]) => {
                Ok(json(StatusCode::OK, &feature_flag(name)))
            }
            // --- apps
            (&Method::GET, ["apps"]) => store.read(|state| {
                let apps: Vec<&crate::store::App> = state
                    .apps
                    .iter()
                    .filter(|app| reads(state, &app.space))
                    .collect();
                listing(
                    apps.iter()
                        .map(|app| views::app(base, state, app))
                        .collect(),
                    views::spaces_included(
                        base,
                        state,
                        &req.query,
                        apps.iter().map(|app| app.space.as_str()),
                    ),
                )
            }),
            (&Method::POST, ["apps"]) => self.create_app(req).await,
            (&Method::GET, ["apps", guid]) => self.app_view(req, guid),
            (&Method::PATCH, ["apps", guid]) => self.patch_app(req, guid).await,
            (&Method::DELETE, ["apps", guid]) => self.delete_app(req, guid).await,
            (&Method::GET, ["apps", guid, "processes"]) => {
                let app = self.app_as(req, guid, Need::Read)?;
                listing(vec![views::process(base, &app)], None)
            }
            (&Method::GET, ["apps", guid, "processes", "web"]) => {
                let app = self.app_as(req, guid, Need::Read)?;
                Ok(json(StatusCode::OK, &views::process(base, &app)))
            }
            (&Method::GET, ["apps", guid, "processes", "web", "stats"]) => {
                self.app_as(req, guid, Need::Read)?;
                self.stats(guid).await
            }
            (&Method::POST, ["apps", guid, "processes", "web", "actions", "scale"]) => {
                self.scale(req, guid).await
            }
            (&Method::GET, ["apps", guid, "processes", kind])
            | (&Method::GET, ["apps", guid, "processes", kind, "stats"])
            | (&Method::POST, ["apps", guid, "processes", kind, "actions", "scale"]) => {
                self.app_as(req, guid, Need::Read)?;
                Err(ApiError::not_found(&format!("Process ({kind})")))
            }
            (&Method::GET, ["apps", guid, "routes"]) => {
                self.app_as(req, guid, Need::Read)?;
                listing(
                    store.read(|state| {
                        state
                            .routes_of(guid)
                            .map(|route| views::route(base, state, route))
                            .collect()
                    }),
                    None,
                )
            }
            (&Method::GET, ["apps", guid, "packages"]) => {
                self.app_as(req, guid, Need::Read)?;
                listing(
                    store.read(|state| {
                        state
                            .packages
                            .iter()
                            .filter(|package| package.app == *guid)
                            .map(|package| views::package(base, package))
                            .collect()
                    }),
                    None,
                )
            }
            (&Method::GET, ["apps", guid, "droplets"]) => {
                self.app_as(req, guid, Need::Read)?;
                listing(
                    store.read(|state| {
                        state
                            .droplets
                            .iter()
                            .filter(|droplet| droplet.app == *guid)
                            .map(|droplet| views::droplet(base, droplet))
                            .collect()
                    }),
                    None,
                )
            }
            (&Method::GET, ["apps", guid, "droplets", "current"]) => {
                let app = self.app_as(req, guid, Need::Read)?;
                let current = app
                    .current_droplet
                    .as_deref()
                    .ok_or_else(|| ApiError::not_found("Droplet"))?;
                store
                    .read(|state| {
                        state
                            .droplet(current)
                            .map(|droplet| views::droplet(base, droplet))
                    })
                    .map_or_else(
                        || Err(ApiError::not_found("Droplet")),
                        |droplet| Ok(json(StatusCode::OK, &droplet)),
                    )
            }
            (&Method::GET, ["apps", guid, "relationships", "current_droplet"]) => {
                let app = self.app_as(req, guid, Need::Read)?;
                Ok(json(
                    StatusCode::OK,
                    &json!({"data": app.current_droplet.as_ref().map(|guid| json!({"guid": guid})), "links": {}}),
                ))
            }
            (&Method::PATCH, ["apps", guid, "relationships", "current_droplet"]) => {
                self.set_current_droplet(req, guid).await
            }
            (
                &Method::POST,
                [
                    "apps",
                    guid,
                    "actions",
                    action @ ("start" | "stop" | "restart"),
                ],
            ) => self.lifecycle(req, guid, action).await,
            (&Method::GET, ["apps", guid, "env"]) => {
                let app = self.app_as(req, guid, Need::Develop)?;
                Ok(json(
                    StatusCode::OK,
                    &store.read(|state| views::env(base, state, &app)),
                ))
            }
            (&Method::GET, ["apps", guid, "environment_variables"]) => {
                let app = self.app_as(req, guid, Need::Develop)?;
                Ok(json(
                    StatusCode::OK,
                    &views::environment_variables(base, &app),
                ))
            }
            (&Method::PATCH, ["apps", guid, "environment_variables"]) => {
                self.set_env(req, guid).await
            }
            (&Method::GET, ["apps", guid, "manifest"]) => {
                let app = self.app_as(req, guid, Need::Develop)?;
                Ok(text(
                    StatusCode::OK,
                    "application/x-yaml",
                    store.read(|state| views::manifest(state, &app)),
                ))
            }
            (&Method::GET, ["apps", guid, "features"]) => {
                self.app_as(req, guid, Need::Read)?;
                Ok(json(
                    StatusCode::OK,
                    &json!({"resources": [{"name": "ssh", "description": "", "enabled": false}, {"name": "revisions", "description": "", "enabled": false}], "pagination": {"total_results": 2, "total_pages": 1}}),
                ))
            }
            (&Method::GET, ["apps", guid, "features", name]) => {
                self.app_as(req, guid, Need::Read)?;
                Ok(json(
                    StatusCode::OK,
                    &json!({"name": name, "description": "", "enabled": false}),
                ))
            }
            (&Method::GET, ["apps", guid, "ssh_enabled"]) => {
                self.app_as(req, guid, Need::Read)?;
                Ok(json(
                    StatusCode::OK,
                    &json!({"enabled": false, "reason": "ssh is not supported: a wasm component has no shell"}),
                ))
            }
            (&Method::GET, ["apps", guid, "sidecars"])
            | (&Method::GET, ["apps", guid, "revisions"])
            | (&Method::GET, ["apps", guid, "revisions", "deployed"])
            | (&Method::GET, ["apps", guid, "tasks"]) => {
                self.app_as(req, guid, Need::Read)?;
                empty()
            }
            (&Method::POST, ["apps", guid, "tasks"]) => {
                self.app_as(req, guid, Need::Read)?;
                Err(ApiError::unprocessable(
                    "tasks are not supported: a wasm component serves requests only",
                ))
            }
            (&Method::POST, ["apps", guid, "sidecars"]) => {
                self.app_as(req, guid, Need::Read)?;
                Err(ApiError::unprocessable("sidecars are not supported"))
            }
            // --- processes
            (&Method::GET, ["processes"]) => listing(
                store.read(|state| {
                    state
                        .apps
                        .iter()
                        .filter(|app| reads(state, &app.space))
                        .map(|app| views::process(base, app))
                        .collect()
                }),
                None,
            ),
            (&Method::GET, ["processes", guid]) => {
                let app = self.app_of_process(req, guid, Need::Read)?;
                Ok(json(StatusCode::OK, &views::process(base, &app)))
            }
            (&Method::PATCH, ["processes", guid]) => self.patch_process(req, guid).await,
            (&Method::GET, ["processes", guid, "stats"]) => {
                let app = self.app_of_process(req, guid, Need::Read)?;
                self.stats(&app.guid).await
            }
            (&Method::GET, ["processes", guid, "sidecars"]) => {
                self.app_of_process(req, guid, Need::Read)?;
                empty()
            }
            (&Method::POST, ["processes", guid, "actions", "scale"]) => {
                let app = self.app_of_process(req, guid, Need::Read)?;
                self.scale(req, &app.guid).await
            }
            // --- packages, builds, droplets
            (&Method::GET, ["packages"]) => listing(
                store.read(|state| {
                    state
                        .packages
                        .iter()
                        .filter(|package| app_read(state, &package.app))
                        .map(|package| views::package(base, package))
                        .collect()
                }),
                None,
            ),
            (&Method::POST, ["packages"]) => self.create_package(req),
            (&Method::GET, ["packages", guid]) => {
                let package = self.package_as(req, guid, Need::Read)?;
                Ok(json(StatusCode::OK, &views::package(base, &package)))
            }
            (&Method::POST, ["packages", guid, "upload"]) => self.upload(req, guid).await,
            (&Method::GET, ["packages", guid, "droplets"]) => {
                self.package_as(req, guid, Need::Read)?;
                listing(
                    store.read(|state| {
                        state
                            .droplets
                            .iter()
                            .filter(|droplet| droplet.package == *guid)
                            .map(|droplet| views::droplet(base, droplet))
                            .collect()
                    }),
                    None,
                )
            }
            (&Method::DELETE, ["packages", guid]) => {
                self.package_as(req, guid, Need::Operate)?;
                let found = store.update(|state| {
                    let found = state.package(guid).is_some();
                    state.packages.retain(|package| package.guid != *guid);
                    found
                });
                if !found {
                    return Err(ApiError::not_found("Package"));
                }
                store.delete(&[crate::store::Blob::Package(guid.to_string())]);
                self.job(req, "package.delete", Vec::new(), None)
            }
            (&Method::POST, ["resource_matches"]) => {
                Ok(json(StatusCode::CREATED, &json!({"resources": []})))
            }
            (&Method::GET, ["builds"]) => listing(
                store.read(|state| {
                    state
                        .builds
                        .iter()
                        .filter(|build| app_read(state, &build.app))
                        .map(|build| views::build(base, state, build))
                        .collect()
                }),
                None,
            ),
            (&Method::POST, ["builds"]) => self.create_build(req).await,
            (&Method::GET, ["builds", guid]) => store
                .read(|state| {
                    state
                        .build(guid)
                        .filter(|build| app_read(state, &build.app))
                        .map(|build| views::build(base, state, build))
                })
                .map_or_else(
                    || Err(ApiError::not_found("Build")),
                    |build| Ok(json(StatusCode::OK, &build)),
                ),
            (&Method::GET, ["droplets"]) => listing(
                store.read(|state| {
                    state
                        .droplets
                        .iter()
                        .filter(|droplet| app_read(state, &droplet.app))
                        .map(|droplet| views::droplet(base, droplet))
                        .collect()
                }),
                None,
            ),
            (&Method::GET, ["droplets", guid]) => store
                .read(|state| {
                    state
                        .droplet(guid)
                        .filter(|droplet| app_read(state, &droplet.app))
                        .map(|droplet| views::droplet(base, droplet))
                })
                .map_or_else(
                    || Err(ApiError::not_found("Droplet")),
                    |droplet| Ok(json(StatusCode::OK, &droplet)),
                ),
            // --- routes
            (&Method::GET, ["routes"]) => {
                let include = req.q("include").unwrap_or_default();
                store.read(|state| {
                    let included = include.contains("domain").then(|| {
                        json!({"domains": state.domains.iter().map(|domain| views::domain(base, domain)).collect::<Vec<_>>()})
                    });
                    let routes = state.routes.iter().filter(|route| reads(state, &route.space));
                    listing(routes.map(|route| views::route(base, state, route)).collect(), included)
                })
            }
            (&Method::POST, ["routes"]) => self.create_route(req),
            (&Method::GET, ["routes", guid]) | (&Method::PATCH, ["routes", guid]) => {
                let route = self.route_as(req, guid, Need::Read)?;
                Ok(json(
                    StatusCode::OK,
                    &store.read(|state| views::route(base, state, &route)),
                ))
            }
            (&Method::DELETE, ["routes", guid]) => self.delete_route(req, guid).await,
            (&Method::GET, ["routes", guid, "destinations"]) => {
                let route = self.route_as(req, guid, Need::Read)?;
                Ok(json(
                    StatusCode::OK,
                    &json!({"destinations": views::destinations(&route), "links": {"self": {"href": format!("{base}/v3/routes/{guid}/destinations")}, "route": {"href": format!("{base}/v3/routes/{guid}")}}}),
                ))
            }
            (&Method::POST, ["routes", guid, "destinations"]) => {
                self.map_route(req, guid, false).await
            }
            (&Method::PATCH, ["routes", guid, "destinations"]) => {
                self.map_route(req, guid, true).await
            }
            (&Method::DELETE, ["routes", guid, "destinations", destination]) => {
                self.unmap_route(req, guid, destination).await
            }
            // --- deployments, jobs
            (&Method::GET, ["deployments"]) => listing(
                store.read(|state| {
                    state
                        .deployments
                        .iter()
                        .filter(|deployment| app_read(state, &deployment.app))
                        .map(|deployment| views::deployment(base, state, deployment))
                        .collect()
                }),
                None,
            ),
            (&Method::POST, ["deployments"]) => self.create_deployment(req).await,
            (&Method::GET, ["deployments", guid]) => store
                .read(|state| {
                    state
                        .deployment(guid)
                        .filter(|deployment| app_read(state, &deployment.app))
                        .map(|deployment| views::deployment(base, state, deployment))
                })
                .map_or_else(
                    || Err(ApiError::not_found("Deployment")),
                    |deployment| Ok(json(StatusCode::OK, &deployment)),
                ),
            (&Method::POST, ["deployments", guid, "actions", "cancel"]) => {
                self.cancel_deployment(req, guid).await
            }
            (&Method::POST, ["deployments", guid, "actions", "continue"]) => {
                self.continue_deployment(req, guid).await
            }
            // a job's guid is its asker's alone to know: no space hides it
            (&Method::GET, ["jobs", guid]) => store
                .read(|state| state.job(guid).map(|job| views::job(base, job)))
                .map_or_else(
                    || Err(ApiError::not_found("Job")),
                    |job| Ok(json(StatusCode::OK, &job)),
                ),
            // --- services
            (
                _,
                [
                    "service_brokers"
                    | "service_offerings"
                    | "service_plans"
                    | "service_instances"
                    | "service_credential_bindings",
                    ..,
                ],
            ) => self.services(req, segments).await,
            (_, ["security_groups", ..]) => self.security_groups(req, segments),
            (&Method::POST, ["service_route_bindings"]) => {
                Err(ApiError::unprocessable("route services are not supported"))
            }
            // --- what there is none of
            (&Method::GET, ["buildpacks"])
            | (&Method::GET, ["service_route_bindings"])
            | (&Method::GET, ["audit_events"])
            | (&Method::GET, ["isolation_segments"])
            | (&Method::GET, ["space_quotas"])
            | (&Method::GET, ["tasks"])
            | (&Method::GET, ["sidecars"])
            | (&Method::GET, ["revisions"]) => empty(),
            (&Method::GET, ["environment_variable_groups", _]) => {
                Ok(json(StatusCode::OK, &json!({"var": {}, "links": {}})))
            }
            (&Method::PATCH, ["environment_variable_groups", _]) => Err(ApiError::unprocessable(
                "environment variable groups are not supported: set an app's own (cf set-env)",
            )),
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        }
    }

    // --- lookups

    /// The app `guid`, as the state has it now.
    fn app(&self, guid: &str) -> Result<crate::store::App, ApiError> {
        self.store
            .read(|state| state.app(guid).cloned())
            .ok_or_else(|| ApiError::not_found("App"))
    }

    /// The app `guid`, if the request's holder may `need` of its space.
    pub(crate) fn app_as(
        &self,
        req: &Req,
        guid: &str,
        need: Need,
    ) -> Result<crate::store::App, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let app = state.app(guid).ok_or_else(|| ApiError::not_found("App"))?;
            access.check(state, &app.space, need, "App")?;
            Ok(app.clone())
        })
    }

    /// The app of the process `process`, as [`Api::app_as`].
    fn app_of_process(
        &self,
        req: &Req,
        process: &str,
        need: Need,
    ) -> Result<crate::store::App, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let app = state
                .apps
                .iter()
                .find(|app| app.process.guid == process)
                .ok_or_else(|| ApiError::not_found("Process"))?;
            access.check(state, &app.space, need, "Process")?;
            Ok(app.clone())
        })
    }

    /// The app of the deployment `guid`, as [`Api::app_as`].
    fn deployment_as(&self, req: &Req, guid: &str, need: Need) -> Result<String, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let deployment = state.deployment(guid);
            let app = deployment.and_then(|deployment| state.app(&deployment.app));
            let app = app.ok_or_else(|| ApiError::not_found("Deployment"))?;
            access.check(state, &app.space, need, "Deployment")?;
            Ok(app.guid.clone())
        })
    }

    /// The package `guid`, if the request's holder may `need` of its app's space.
    fn package_as(&self, req: &Req, guid: &str, need: Need) -> Result<Package, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let package = state
                .package(guid)
                .ok_or_else(|| ApiError::not_found("Package"))?;
            let space = state.app(&package.app).map_or("", |app| app.space.as_str());
            access.check(state, space, need, "Package")?;
            Ok(package.clone())
        })
    }

    /// The route `guid`, if the request's holder may `need` of its space.
    fn route_as(&self, req: &Req, guid: &str, need: Need) -> Result<crate::store::Route, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let route = state
                .route(guid)
                .ok_or_else(|| ApiError::not_found("Route"))?;
            access.check(state, &route.space, need, "Route")?;
            Ok(route.clone())
        })
    }

    fn app_view(&self, req: &Req, guid: &str) -> Reply {
        let app = self.app_as(req, guid, Need::Read)?;
        Ok(json(StatusCode::OK, &self.app_shown(req, &app)))
    }

    /// `app` as `/v3/apps` shows it.
    fn app_shown(&self, req: &Req, app: &crate::store::App) -> Value {
        self.store.read(|state| views::app(&req.base, state, app))
    }

    /// The process stats, of the instances alive as each wicket lists them at this request: its
    /// long poll answers when its apps change, not when an instance comes or goes. None read of
    /// a wicket known away, or not serving its share.
    async fn stats(&self, guid: &str) -> Reply {
        let app = self.app(guid)?;
        let owner = self.controller.owner();
        let serving: Vec<&Arc<Wicket>> = self.store.read(|state| {
            let shares = Controller::desired(state, &app, owner);
            let wickets = self.controller.wickets.iter();
            wickets
                .filter(|wicket| shares.contains_key(&wicket.name))
                .filter(|wicket| {
                    matches!(state.outcome(guid, &wicket.name), Some(Outcome::Running))
                })
                .collect()
        });
        let listed = join_all(serving.into_iter().map(|wicket| async move {
            let listing = wicket.list(None).await.ok()?;
            let mut apps = listing.apps.into_iter();
            let listed = apps.find(|listed| listed.name == guid && listed.app.owner == owner)?;
            Some((wicket.name.clone(), listed))
        }))
        .await;
        let listed: BTreeMap<String, ListedApp> = listed.into_iter().flatten().collect();
        Ok(json(
            StatusCode::OK,
            &self
                .store
                .read(|state| views::stats(state, &app, owner, &listed)),
        ))
    }

    /// A job done (or failed with `errors`), answered 202 with its location.
    pub(crate) fn job(
        &self,
        req: &Req,
        operation: &str,
        errors: Vec<JobError>,
        resource: Option<(String, String)>,
    ) -> Reply {
        let guid = self
            .store
            .update(|state| state.add_job(operation, errors, resource));
        Ok(self.located(req, &guid))
    }

    /// 202, the job `guid` located.
    pub(crate) fn located(&self, req: &Req, guid: &str) -> Response {
        let mut response = json(StatusCode::ACCEPTED, &Value::Null);
        *response.body_mut() = Full::new(Bytes::new());
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(&format!("{}/v3/jobs/{guid}", req.base))
                .expect("a plain location"),
        );
        response
    }

    /// Changes the state as `change` says, once the wickets took the apps
    /// `guids` as they would be: a refusal changes nothing. A wicket
    /// unreachable, the change is kept: it is put when the wicket is back.
    async fn edit_at_once(
        &self,
        guids: &[String],
        change: &(dyn Fn(&mut State) -> Result<(), ApiError> + Sync),
    ) -> Result<(), ApiError> {
        let _apps = self.controller.hold_all(guids).await;
        let trial = || {
            self.store.read(|state| {
                let mut trial = state.clone();
                change(&mut trial)?;
                Ok::<_, ApiError>(self.shares(&trial, guids))
            })
        };
        let tried = self.try_placed(trial).await?;
        let committed = self.store.update(|state| change(state));
        if committed.is_err() {
            self.go_back(&tried).await;
        }
        committed
    }

    /// The shares of the apps `guids` in `trial`, by wicket name.
    fn shares(&self, trial: &State, guids: &[String]) -> Vec<(String, BTreeMap<String, AppBody>)> {
        let owner = self.controller.owner();
        let shares = |guid: &String| {
            let app = trial.app(guid);
            app.map(|app| Controller::desired(trial, app, owner))
                .unwrap_or_default()
        };
        guids
            .iter()
            .map(|guid| (guid.clone(), shares(guid)))
            .collect()
    }

    /// [`Api::try_shares`] of the shares `trial` makes, made again while a wicket refusing one for
    /// room comes to have none: its indices go to a wicket with room (`crate::placement`).
    async fn try_placed(
        &self,
        trial: impl Fn() -> Result<Vec<(String, BTreeMap<String, AppBody>)>, ApiError>,
    ) -> Result<Tried, ApiError> {
        let full = || self.store.read(|state| state.wickets.full.len());
        for _ in 0..self.controller.wickets.len() {
            let before = full();
            match self.try_shares(&trial()?).await {
                Err(_) if full() > before => continue,
                tried => return tried,
            }
        }
        self.try_shares(&trial()?).await
    }

    /// Puts each app's shares as `trial` has them, on every wicket whose share it changes, a
    /// wicket at a time: a refusal puts those put before back as the state has them, and is
    /// answered. What was put is held until the edit is committed, or gone back on.
    async fn try_shares(
        &self,
        trial: &[(String, BTreeMap<String, AppBody>)],
    ) -> Result<Tried, ApiError> {
        let mut tried = Tried::default();
        for (guid, shares) in trial {
            for wicket in &self.controller.wickets {
                let desired = shares.get(&wicket.name);
                // as it was, it is not tried: nothing changes there, and a refusal of it is
                // known, which may have taken the health check's whole timeout (the app held,
                // its shares stay as they are)
                let unchanged = self.store.read(|state| {
                    let share = self.controller.desired_on(state, guid, &wicket.name);
                    share.as_ref() == desired
                });
                if unchanged {
                    continue;
                }
                let share = self.controller.hold_share(guid, &wicket.name).await;
                match self.trial_put(wicket, guid, desired).await {
                    Ok(()) | Err(Failure::Unreachable(_)) => {
                        tried.put.push((wicket.clone(), guid.clone()));
                        tried.held.push(share);
                    }
                    Err(refused) => {
                        self.go_back(&tried).await;
                        return Err(ApiError::refused(wicket, &refused));
                    }
                }
            }
        }
        Ok(tried)
    }

    /// Puts the shares `tried` put back as the state has them.
    async fn go_back(&self, tried: &Tried) {
        for (wicket, guid) in &tried.put {
            let _ = self.controller.sync_held(wicket, guid).await;
        }
    }

    /// Puts an app's share on `wicket` as an edit would leave it, the share held. One the wicket
    /// is not known to serve (it refused it) has nothing to break: refused it goes through, or
    /// what would fix the app (a push of another image, more memory) could not be made; but for
    /// room, which another wicket may have.
    async fn trial_put(
        &self,
        wicket: &Wicket,
        guid: &str,
        desired: Option<&AppBody>,
    ) -> Result<(), Failure> {
        let serving = self
            .store
            .read(|state| matches!(state.outcome(guid, &wicket.name), Some(Outcome::Running)));
        match self.controller.apply(wicket, guid, desired).await {
            Err(Failure::Refused { no_room: false, .. }) if !serving => Ok(()),
            put => put,
        }
    }

    // --- the manifest

    async fn apply_manifest(&self, req: &Req, space: &str) -> Reply {
        let access = req.access()?;
        self.store
            .read(|state| access.check(state, space, Need::Develop, "Space"))?;
        let manifest = match manifest::parse(
            std::str::from_utf8(&req.body)
                .map_err(|_| ApiError::bad_request("the manifest is not UTF-8"))?,
        ) {
            Ok(manifest) => manifest,
            Err(e) => {
                return self.job(
                    req,
                    "space.apply_manifest",
                    vec![ApiError::unprocessable(e).job_error()],
                    None,
                );
            }
        };
        // a stack it pins is one the wickets run
        self.controller.stacks().await;
        let named: Vec<String> = self.store.read(|state| {
            let named = manifest.applications.iter();
            named
                .filter_map(|spec| state.app_named(space, spec.name.trim()))
                .map(|app| app.guid.clone())
                .collect()
        });
        let _apps = self.controller.hold_all(&named).await;
        let guids = {
            let mut trial = self.store.read(State::clone);
            match manifest::apply(&mut trial, space, &manifest) {
                Ok(guids) => guids,
                Err(e) => {
                    return self.job(
                        req,
                        "space.apply_manifest",
                        vec![ApiError::unprocessable(e).job_error()],
                        None,
                    );
                }
            }
        };
        // the routes apply at once: a started app is put as it would be
        let trial = || {
            // of the state as it is: a wicket refusing a share for room fills
            let mut trial = self.store.read(State::clone);
            manifest::apply(&mut trial, space, &manifest).map_err(ApiError::unprocessable)?;
            Ok(self.shares(&trial, &guids))
        };
        let tried = match self.try_placed(trial).await {
            Ok(tried) => tried,
            Err(refused) => {
                return self.job(req, "space.apply_manifest", vec![refused.job_error()], None);
            }
        };
        // made on the state as it is by now: all of it or, the other edits since refusing it,
        // none. An app new since the trial was not put: it is no started one
        let committed = self.store.update(|state| {
            if state.space(space).is_none() {
                return Err("the space is gone".to_string());
            }
            let mut next = state.clone();
            let guids = manifest::apply(&mut next, space, &manifest)?;
            // the bindings `services:` made: a broker's is its broker's to make
            let bound: Vec<(String, String, Option<String>)> = next
                .bindings
                .iter()
                .filter(|binding| state.binding(&binding.guid).is_none())
                .filter_map(|binding| {
                    let instance = next.instance(&binding.instance)?;
                    let brokered = binding.operation.is_some().then(|| binding.guid.clone());
                    Some((binding.app.clone(), instance.name.clone(), brokered))
                })
                .collect();
            *state = next;
            Ok::<_, String>((guids, bound))
        });
        let (guids, bound) = match committed {
            Ok(committed) => committed,
            Err(e) => {
                self.go_back(&tried).await;
                return self.job(
                    req,
                    "space.apply_manifest",
                    vec![ApiError::unprocessable(e).job_error()],
                    None,
                );
            }
        };
        let mut warnings = Vec::new();
        for guid in &guids {
            self.logs.add(guid, "API", "Applied the manifest");
            let Some(app) = self.store.read(|state| state.app(guid).cloned()) else {
                continue;
            };
            self.annotations_waiting(&app);
            let waits = manifest::check_running(&app, app.process.instances).err();
            warnings.extend(manifest::push_warning(&app));
            if let Some(waits) = waits {
                self.logs.add(
                    guid,
                    "API",
                    format!("The instances wait for the next start: {waits}"),
                );
            }
        }
        let mut brokered = Vec::new();
        for (app, instance, by_broker) in bound {
            match by_broker {
                // logged when the broker made it
                Some(binding) => brokered.push(binding),
                None => self.logs.add(
                    &app,
                    "API",
                    format!("Bound service instance {instance}: applies at the next start, restart or restage"),
                ),
            }
        }
        let user = self.claims(req)?.user_id;
        let job = self.store.update(|state| {
            let job = state.add_waiting_job(
                "space.apply_manifest",
                brokered.clone(),
                Some(("space".into(), format!("/v3/spaces/{space}"))),
            );
            for binding in &brokered {
                if let Some(operation) = state
                    .binding_mut(binding)
                    .and_then(|binding| binding.operation.as_mut())
                {
                    operation.job = Some(job.clone());
                    operation.user = Some(user.clone());
                }
            }
            job
        });
        self.operations.kick();
        Ok(warned(self.located(req, &job), &warnings))
    }

    fn manifest_diff(&self, req: &Req, space: &str) -> Reply {
        let access = req.access()?;
        self.store
            .read(|state| access.check(state, space, Need::Develop, "Space"))?;
        let manifest = manifest::parse(
            std::str::from_utf8(&req.body)
                .map_err(|_| ApiError::bad_request("the manifest is not UTF-8"))?,
        )?;
        let diff: Vec<Value> = self.store.read(|state| {
            manifest
                .applications
                .iter()
                .enumerate()
                .filter(|(_, spec)| state.app_named(space, spec.name.trim()).is_none())
                .map(|(i, spec)| json!({"op": "add", "path": format!("/applications/{i}"), "value": {"name": spec.name}}))
                .collect()
        });
        Ok(json(StatusCode::CREATED, &json!({"diff": diff})))
    }

    // --- apps

    async fn create_app(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let name = body["name"]
            .as_str()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| ApiError::unprocessable("Name must be a string"))?;
        let env = match &body["environment_variables"] {
            Value::Null => Default::default(),
            Value::Object(var) => env::merged(&Default::default(), var)?,
            _ => {
                return Err(ApiError::unprocessable(
                    "Environment variables must be an object",
                ));
            }
        };
        let lifecycle = lifecycle(&body)?;
        let space = body["relationships"]["space"]["data"]["guid"]
            .as_str()
            .unwrap_or_default();
        let access = req.access()?;
        // a stack it pins is one the wickets run
        self.controller.stacks().await;
        let view = self.store.update(|state| {
            space_to_develop(access, state, space)?;
            if state.app_named(space, name).is_some() {
                return Err(ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    code: 10016,
                    title: "CF-UniquenessError",
                    detail: format!("App with the name '{name}' already exists."),
                });
            }
            let pin = pin(state, &body)?.flatten();
            let guid = state.create_app(name, space).guid.clone();
            let app = state.app_mut(&guid).expect("just created");
            app.env = env;
            app.lifecycle = lifecycle.unwrap_or_default();
            app.pin = pin;
            let runtime = state.stack_of(state.app(&guid).expect("just created"));
            let app = state.app_mut(&guid).expect("just created");
            if let Err(refused) = apply_metadata(app, &body["metadata"], runtime) {
                state.apps.retain(|app| app.guid != guid);
                return Err(refused);
            }
            let app = state.app_mut(&guid).expect("just created");
            if let Some(buildpacks) = body["lifecycle"]["data"]["buildpacks"].as_array() {
                app.buildpacks = buildpacks
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
            }
            let app = state.app(&guid).expect("just created");
            Ok(views::app(&req.base, state, app))
        })?;
        Ok(json(StatusCode::CREATED, &view))
    }

    /// Renames the app, sets its lifecycle and merges its metadata: the annotations of how many
    /// instances and how long at once, once the wicket took them (`manifest::live_lifetime`).
    async fn patch_app(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        let lifecycle = lifecycle(&body)?;
        let space = self.app_as(req, guid, Need::Develop)?.space;
        // a stack it pins is one the wickets run
        self.controller.stacks().await;
        let change = |state: &mut State| {
            if let Some(name) = body["name"].as_str()
                && state
                    .app_named(&space, name)
                    .is_some_and(|other| other.guid != guid)
            {
                return Err(ApiError::unprocessable(format!(
                    "App with the name '{name}' already exists."
                )));
            }
            let pin = pin(state, &body)?;
            let app = state
                .app_mut(guid)
                .ok_or_else(|| ApiError::not_found("App"))?;
            let pinned = app.pin;
            app.pin = pin.unwrap_or(pinned);
            let runtime = state.stack_of(state.app(guid).expect("found above"));
            let app = state.app_mut(guid).expect("found above");
            // first: refused, it changes nothing
            if let Err(refused) = apply_metadata(app, &body["metadata"], runtime) {
                app.pin = pinned;
                return Err(refused);
            }
            manifest::take_annotations(app);
            if let Some(name) = body["name"].as_str() {
                app.name = name.to_string();
            }
            if let Some(lifecycle) = lifecycle {
                app.lifecycle = lifecycle;
            }
            if let Some(buildpacks) = body["lifecycle"]["data"]["buildpacks"].as_array() {
                app.buildpacks = buildpacks
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
            }
            app.updated_at = now();
            Ok(())
        };
        self.edit_at_once(&[guid.to_string()], &change).await?;
        let app = self.app(guid)?;
        if body["metadata"]["annotations"].is_object() {
            self.annotations_waiting(&app);
        }
        Ok(json(StatusCode::OK, &self.app_shown(req, &app)))
    }

    /// Logs why the running `app` takes none of its annotations until its next start.
    fn annotations_waiting(&self, app: &crate::store::App) {
        if let Some(Err(waits)) = manifest::live_lifetime(app) {
            self.logs.add(
                &app.guid,
                "API",
                format!("The annotations wait for the next start: {waits}"),
            );
        }
    }

    /// Merges the body's `var` into the app's environment variables (a null
    /// removes one); the guest gets them at its next start, as on CF. The
    /// log names no value.
    async fn set_env(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        let var = body["var"]
            .as_object()
            .ok_or_else(|| ApiError::unprocessable("Var must be an object"))?;
        self.app_as(req, guid, Need::Develop)?;
        let app = self.store.update(|state| {
            let app = state
                .app_mut(guid)
                .ok_or_else(|| ApiError::not_found("App"))?;
            app.env = env::merged(&app.env, var)?;
            app.updated_at = now();
            Ok::<_, ApiError>(app.clone())
        })?;
        self.logs.add(
            guid,
            "API",
            format!(
                "Updated app with guid {guid} ({{\"environment_variables\"=>\"[PRIVATE DATA HIDDEN]\"}})"
            ),
        );
        Ok(json(
            StatusCode::OK,
            &views::environment_variables(&req.base, &app),
        ))
    }

    /// Deletes the app; its bindings to brokers' instances first, as their brokers unbind
    /// them: the job settles once they are unbound and the app deleted, and fails, the app
    /// left, if one is not.
    async fn delete_app(&self, req: &Req, guid: &str) -> Reply {
        let app = self.app_as(req, guid, Need::Develop)?;
        let user = self.claims(req)?.user_id;
        let job = self.store.update(|state| {
            let brokered: Vec<String> = state
                .bindings_of(guid)
                .filter(|binding| {
                    state
                        .instance(&binding.instance)
                        .is_some_and(crate::store::Instance::brokered)
                })
                .map(|binding| binding.guid.clone())
                .collect();
            if brokered.is_empty() {
                return Ok(None);
            }
            if let Some(busy) = state
                .bindings_of(guid)
                .find(|binding| brokered.contains(&binding.guid) && binding.busy())
            {
                let instance = state
                    .instance(&busy.instance)
                    .map_or("", |instance| instance.name.as_str());
                return Err(ApiError {
                    status: StatusCode::CONFLICT,
                    code: 60016,
                    title: "CF-AsyncServiceInstanceOperationInProgress",
                    detail: format!(
                        "An operation for the service binding between app {} and service instance {instance} is in progress.",
                        app.name
                    ),
                });
            }
            let mut waits = brokered.clone();
            waits.push(guid.to_string());
            let job = state.add_waiting_job("app.delete", waits, None);
            for binding in &brokered {
                let binding = state.binding_mut(binding).expect("listed above");
                binding.operation = Some(crate::store::Operation::new(
                    crate::store::OperationKind::Delete,
                    Some(job.clone()),
                    Some(user.clone()),
                ));
            }
            Ok(Some((job, brokered)))
        })?;
        let Some((job, unbound)) = job else {
            self.controller.remove(guid, &app.name).await;
            return self.job(req, "app.delete", Vec::new(), None);
        };
        self.operations.kick();
        let (operations, controller, store) = (
            self.operations.clone(),
            self.controller.clone(),
            self.store.clone(),
        );
        let (guid, located) = (guid.to_string(), job.clone());
        tokio::spawn(async move {
            operations
                .until(|state| {
                    state
                        .job(&job)
                        .is_none_or(|job| !job.waits.iter().any(|wait| unbound.contains(wait)))
                })
                .await;
            let unbound =
                store.read(|state| state.job(&job).is_some_and(|job| job.errors.is_empty()));
            if unbound {
                controller.remove(&guid, &app.name).await;
            }
            store.update(|state| state.settle(&job, &guid, None));
        });
        Ok(self.located(req, &located))
    }

    /// The command and the health checks, a check refused when the app as annotated would not take
    /// it (`manifest::check_readiness`, `manifest::check_liveness`); both taken at the next start,
    /// as on CF (`cf set-health-check` asks for a restart). Of a health check's `data`, what it
    /// names alone changes (`cf set-health-check` names no `timeout`), and a type but `http` takes
    /// no endpoint.
    async fn patch_process(&self, req: &Req, process: &str) -> Reply {
        let body: Value = req.json()?;
        let app = self.app_of_process(req, process, Need::Operate)?;
        let view = self.store.update(|state| {
            let runtime = state.stack_of(&app);
            let app = state
                .app_mut(&app.guid)
                .ok_or_else(|| ApiError::not_found("Process"))?;
            let given = &body["readiness_health_check"];
            if given.is_object() {
                let mut readiness = app.process.readiness_health_check.clone();
                if let Some(kind) = given["type"].as_str() {
                    readiness.kind = kind.to_string();
                }
                if let Some(data) = given["data"].as_object() {
                    let seconds = |key: &str| data.get(key).and_then(Value::as_u64);
                    readiness.invocation_timeout = seconds("invocation_timeout");
                    readiness.interval = seconds("interval");
                    readiness.endpoint = data
                        .get("endpoint")
                        .and_then(Value::as_str)
                        .map(String::from);
                }
                let lifetime =
                    manifest::check_annotations(&app.annotations, app.process.instances, runtime)?;
                manifest::check_readiness(&readiness, &lifetime, runtime)?;
                app.process.readiness_health_check = readiness;
            }
            let given = &body["health_check"];
            if given.is_object() {
                let mut liveness = app.process.health_check.clone();
                if let Some(kind) = given["type"].as_str() {
                    liveness.kind = kind.to_string();
                    if kind != "http" {
                        liveness.endpoint = None;
                    }
                }
                if let Some(data) = given["data"].as_object() {
                    let seconds = |key: &str| data.get(key).map(Value::as_u64);
                    if let Some(timeout) = seconds("timeout") {
                        liveness.timeout = timeout;
                    }
                    if let Some(timeout) = seconds("invocation_timeout") {
                        liveness.invocation_timeout = timeout;
                    }
                    if let Some(interval) = seconds("interval") {
                        liveness.interval = interval;
                    }
                    if let Some(endpoint) = data.get("endpoint") {
                        liveness.endpoint = endpoint.as_str().map(String::from);
                    }
                }
                let lifetime =
                    manifest::check_annotations(&app.annotations, app.process.instances, runtime)?;
                manifest::check_liveness(&liveness, &lifetime, runtime)?;
                app.process.health_check = liveness;
            }
            if let Some(command) = body.get("command") {
                app.process.command = command
                    .as_str()
                    .map(String::from)
                    .filter(|command| !command.is_empty());
            }
            app.process.updated_at = now();
            Ok::<_, ApiError>(views::process(&req.base, app))
        })?;
        Ok(json(StatusCode::OK, &view))
    }

    /// `instances` applies at once, refused if the app as it runs or as annotated cannot
    /// take it (`manifest::check_scale`); memory and disk at the next start (the CLI
    /// restarts the app for `-m`).
    async fn scale(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        self.app_as(req, guid, Need::Operate)?;
        let instances = body
            .get("instances")
            .map(|value| {
                value
                    .as_u64()
                    .ok_or_else(|| ApiError::unprocessable("Instances must be a number from 0"))
            })
            .transpose()?;
        let memory = body
            .get("memory_in_mb")
            .map(|value| {
                value
                    .as_u64()
                    .filter(|memory| *memory > 0)
                    .ok_or_else(|| ApiError::unprocessable("Memory must be a number from 1"))
            })
            .transpose()?;
        let disk = body
            .get("disk_in_mb")
            .map(|value| {
                value
                    .as_u64()
                    .filter(|disk| *disk > 0)
                    .ok_or_else(|| ApiError::unprocessable("Disk must be a number from 1"))
            })
            .transpose()?;
        let log_rate = body
            .get("log_rate_limit_in_bytes_per_second")
            .and_then(Value::as_i64);
        let guid = guid.to_string();
        let change = {
            let guid = guid.clone();
            move |state: &mut State| {
                let runtime = state.app(&guid).and_then(|app| state.stack_of(app));
                let app = state
                    .app_mut(&guid)
                    .ok_or_else(|| ApiError::not_found("App"))?;
                if let Some(instances) = instances {
                    manifest::check_scale(app, instances, runtime)?;
                    app.process.instances = instances;
                }
                if let Some(memory) = memory {
                    app.process.memory_mb = memory;
                }
                if let Some(disk) = disk {
                    app.process.disk_mb = disk;
                }
                if let Some(log_rate) = log_rate {
                    app.process.log_rate_limit = log_rate;
                }
                app.process.updated_at = now();
                Ok(())
            }
        };
        self.edit_at_once(std::slice::from_ref(&guid), &change)
            .await?;
        self.logs
            .add(&guid, "API", format!("Scaled the web process: {body}"));
        let app = self.app(&guid)?;
        let warning = instances.and_then(|_| manifest::scale_warning(&app));
        let scaled = json(StatusCode::ACCEPTED, &views::process(&req.base, &app));
        Ok(warned(scaled, warning.as_slice()))
    }

    async fn set_current_droplet(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        let droplet = body["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("data.guid names the droplet"))?
            .to_string();
        self.app_as(req, guid, Need::Operate)?;
        let _app = self.controller.hold(guid).await;
        let blobs = self.store.update(|state| {
            match state.droplet(&droplet) {
                Some(found) if found.app == guid => {}
                Some(_) => return Err(ApiError::unprocessable("Unable to assign current droplet. Ensure the droplet exists and belongs to this app.")),
                None => return Err(ApiError::not_found("Droplet")),
            }
            Ok(state.set_current_droplet(guid, &droplet))
        })?;
        self.store.delete(&blobs);
        self.logs
            .add(guid, "API", format!("Set the current droplet to {droplet}"));
        Ok(json(
            StatusCode::OK,
            &json!({
                "data": {"guid": droplet},
                "links": {
                    "self": {"href": format!("{}/v3/apps/{guid}/relationships/current_droplet", req.base)},
                    "related": {"href": format!("{}/v3/apps/{guid}/droplets/current", req.base)},
                },
            }),
        ))
    }

    /// A start (and a restart) takes the snapshot the wicket gets: the
    /// current droplet, the memory, the annotations, the bindings' grants
    /// and the environment; a stop removes the app from the wicket. Either
    /// supersedes a deployment under way, every share put at once. The
    /// outcome is in the stats and the log.
    async fn lifecycle(&self, req: &Req, guid: &str, action: &str) -> Reply {
        self.app_as(req, guid, Need::Operate)?;
        let _app = self.controller.hold(guid).await;
        let (app, superseded) = self.store.update(|state| {
            let running = if action == "stop" {
                None
            } else {
                Some(snapshot(state, guid, &req.base)?)
            };
            state.app(guid).ok_or_else(|| ApiError::not_found("App"))?;
            let superseded = state.supersede(guid);
            let app = state.app_mut(guid).expect("found above");
            match running {
                Some(running) => {
                    app.running = Some(running);
                    app.started = true;
                }
                None => app.started = false,
            }
            app.updated_at = now();
            Ok::<_, ApiError>((app.clone(), superseded))
        })?;
        if let Some(line) = superseded {
            self.logs.add(guid, "API", line);
        }
        self.logs.add(
            guid,
            "API",
            format!(
                "Updated app with guid {guid} ({{\"state\"=>\"{}\"}})",
                if app.started { "STARTED" } else { "STOPPED" }
            ),
        );
        self.controller.sync(guid).await;
        Ok(json(StatusCode::OK, &self.app_shown(req, &app)))
    }

    // --- packages, builds

    /// A bits package awaits its upload; a docker one, its image and the
    /// credentials it is pulled with, is ready at once: the app's older
    /// packages go.
    fn create_package(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let docker = match body["type"].as_str().unwrap_or("bits") {
            "bits" => None,
            "docker" => Some(docker_image(&body["data"])?),
            other => {
                return Err(ApiError::unprocessable(format!(
                    "a package is of the type bits or docker, not {other}"
                )));
            }
        };
        let app = body["relationships"]["app"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.app names the app"))?;
        self.app_as(req, app, Need::Operate)?;
        let image = docker.as_ref().map(|docker| docker.image.clone());
        let (view, blobs) = self.store.update(|state| {
            let at = now();
            let ready = docker.is_some();
            let package = Package {
                guid: guid(),
                app: app.to_string(),
                state: if ready { "READY" } else { "AWAITING_UPLOAD" }.into(),
                sha256: None,
                docker,
                created_at: at.clone(),
                updated_at: at,
            };
            let view = views::package(&req.base, &package);
            state.packages.push(package);
            let blobs = if ready {
                state.keep_newest_package(app)
            } else {
                Vec::new()
            };
            (view, blobs)
        });
        self.store.delete(&blobs);
        if let Some(image) = image {
            self.logs
                .add(app, "API", format!("Created a docker package of {image}"));
        }
        Ok(json(StatusCode::CREATED, &view))
    }

    /// The `bits` part of the upload is the zip, copied on a blocking thread: a VM of one vCPU
    /// has one worker, which its API and its watch run on. The app's older packages go, and an
    /// upload to one that went is refused.
    async fn upload(&self, req: &Req, guid: &str) -> Reply {
        let package = self.package_as(req, guid, Need::Operate)?;
        if package.docker.is_some() {
            return Err(ApiError::unprocessable(
                "a docker package takes no upload: its image is pulled at staging",
            ));
        }
        let replaced = || ApiError::unprocessable("a newer package of the app replaced this one");
        let newest = |state: &State| state.is_newest_package(guid, &package.app);
        if !self.store.read(newest) {
            return Err(replaced());
        }
        let content_type = req
            .header(header::CONTENT_TYPE)
            .unwrap_or_default()
            .to_string();
        let spool = req
            .spool
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("no body"))?;
        let body = std::fs::File::open(spool.path())
            .map_err(|e| ApiError::from(format!("{}: {e}", spool.path().display())))?;
        let copied = {
            let (store, guid) = (self.store.clone(), guid.to_string());
            tokio::task::spawn_blocking(move || {
                store.make_package(&guid, |file| {
                    let mut hashed = Hashed::new(file);
                    let bytes =
                        crate::multipart::copy_part(&content_type, body, "bits", &mut hashed)?
                            .ok_or("the upload has no 'bits' part")?;
                    Ok((bytes, hashed.finish()))
                })
            })
            .await
            .map_err(|e| ApiError::from(format!("the upload: {e}")))?
        };
        let (bytes, sha256) = copied.map_err(ApiError::bad_request)?;
        let updated = self.store.update(|state| {
            if !newest(state) {
                return None;
            }
            let blobs = state.keep_newest_package(&package.app);
            let package = state
                .packages
                .iter_mut()
                .find(|package| package.guid == guid)?;
            package.state = "READY".into();
            package.sha256 = Some(sha256);
            package.updated_at = now();
            Some((views::package(&req.base, package), blobs))
        });
        let Some((view, blobs)) = updated else {
            self.store.delete(&[Blob::Package(guid.to_string())]);
            return Err(replaced());
        };
        self.store.delete(&blobs);
        self.logs.add(
            &package.app,
            "API",
            format!("Uploaded a package of {bytes} bytes"),
        );
        Ok(json(StatusCode::OK, &view))
    }

    /// Stages the package: the droplet is the component picked from its zip
    /// or pulled from its image (`staging::run`). Answered once staged, or
    /// STAGING after [`STAGING_WAIT`], the staging going on.
    async fn create_build(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let package_guid = body["package"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("package.guid names the package"))?;
        let package = self
            .store
            .read(|state| state.package(package_guid).cloned())
            .ok_or_else(|| ApiError::unprocessable("Unable to use package. Ensure that the package exists and you have access to it."))?;
        let app = self.app_as(req, &package.app, Need::Operate)?;
        if package.state != "READY" {
            return Err(ApiError::unprocessable(
                "Only packages with state READY can be staged.",
            ));
        }
        let lifecycle = if package.docker.is_some() {
            Lifecycle::Docker
        } else {
            Lifecycle::Buildpack
        };
        let at = now();
        let build = Build {
            guid: guid(),
            app: app.guid.clone(),
            package: package_guid.to_string(),
            state: "STAGING".into(),
            lifecycle,
            error: None,
            droplet: None,
            created_at: at.clone(),
            updated_at: at,
        };
        let guid = build.guid.clone();
        // the app is what it was staged as last
        self.store.update(|state| {
            if let Some(app) = state.app_mut(&build.app) {
                app.lifecycle = lifecycle;
            }
            state.builds.push(build);
            if state.builds.len() > 1000 {
                state.builds.remove(0);
            }
        });
        let staging = tokio::spawn({
            let (store, logs, registry, controller) = (
                self.store.clone(),
                self.logs.clone(),
                self.registry.clone(),
                self.controller.clone(),
            );
            let (guid, command) = (guid.clone(), app.process.command.clone());
            async move {
                staging::run(
                    &store,
                    &logs,
                    &registry,
                    &controller,
                    &guid,
                    &package,
                    command.as_deref(),
                )
                .await;
            }
        });
        // past the wait, the staging goes on: the CLI polls the build
        let _ = tokio::time::timeout(STAGING_WAIT, staging).await;
        self.store
            .read(|state| {
                state
                    .build(&guid)
                    .map(|build| views::build(&req.base, state, build))
            })
            .map_or_else(
                || Err(ApiError::not_found("Build")),
                |build| Ok(json(StatusCode::CREATED, &build)),
            )
    }

    // --- routes

    fn create_route(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        if body["path"].as_str().is_some_and(|path| !path.is_empty()) {
            return Err(ApiError::unprocessable("a route with a path is not served"));
        }
        if !body["port"].is_null() {
            return Err(ApiError::unprocessable("a route with a port is not served"));
        }
        let domain = body["relationships"]["domain"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.domain names the domain"))?
            .to_string();
        let host = manifest::check_host(body["host"].as_str().unwrap_or_default())?;
        let space = body["relationships"]["space"]["data"]["guid"]
            .as_str()
            .unwrap_or_default();
        let access = req.access()?;
        let view = self.store.update(|state| {
            if state.space(space).is_none() || !access.can(state, space, Need::Read) {
                return Err(ApiError::unprocessable(
                    "Invalid space. Ensure that the space exists and you have access to it.",
                ));
            }
            access.check(state, space, Need::Operate, "Space")?;
            if state.domain(&domain).is_none() {
                return Err(ApiError::unprocessable(
                    "Invalid domain. Ensure that the domain exists and you have access to it.",
                ));
            }
            if state.route_at(&host, &domain).is_some() {
                return Err(ApiError::unprocessable("Route already exists."));
            }
            let route = state.create_route(&host, &domain, space).clone();
            Ok(views::route(&req.base, state, &route))
        })?;
        Ok(json(StatusCode::CREATED, &view))
    }

    async fn delete_route(&self, req: &Req, guid: &str) -> Reply {
        self.route_as(req, guid, Need::Operate)?;
        let _route = self.controller.hold(guid).await;
        let route = self
            .store
            .read(|state| state.route(guid).cloned())
            .ok_or_else(|| ApiError::not_found("Route"))?;
        if let Some(app) = &route.app {
            let guid = guid.to_string();
            self.edit_at_once(std::slice::from_ref(app), &move |state: &mut State| {
                state.unmap(&guid);
                Ok(())
            })
            .await?;
        }
        self.store
            .update(|state| state.routes.retain(|kept| kept.guid != guid));
        self.controller.forget(guid);
        self.job(req, "route.delete", Vec::new(), None)
    }

    /// Maps the destinations' app (one: a route serves one app) at once;
    /// `replace` (a PATCH) unmaps the app mapped before.
    async fn map_route(&self, req: &Req, guid: &str, replace: bool) -> Reply {
        let body: Value = req.json()?;
        self.route_as(req, guid, Need::Operate)?;
        let _route = self.controller.hold(guid).await;
        let route = self
            .store
            .read(|state| state.route(guid).cloned())
            .ok_or_else(|| ApiError::not_found("Route"))?;
        let destinations = body["destinations"]
            .as_array()
            .ok_or_else(|| ApiError::unprocessable("destinations is a list"))?;
        let mut wanted: Option<String> = None;
        for destination in destinations {
            if !destination["weight"].is_null() {
                return Err(ApiError::unprocessable(
                    "weighted routing is not supported: a route serves one app",
                ));
            }
            if destination["app"]["process"]["type"]
                .as_str()
                .is_some_and(|kind| kind != "web")
            {
                return Err(ApiError::unprocessable(
                    "process types but web are not supported",
                ));
            }
            let app = destination["app"]["guid"]
                .as_str()
                .ok_or_else(|| ApiError::unprocessable("a destination names an app"))?;
            let found = self
                .app_as(req, app, Need::Read)
                .map_err(|_| ApiError::unprocessable(format!("App with guid '{app}' not found")))?;
            if found.space != route.space {
                return Err(ApiError::unprocessable(
                    "Routes destinations must be in either the app's space or the route's shared spaces",
                ));
            }
            if wanted.as_deref().is_some_and(|wanted| wanted != app) {
                return Err(ApiError::unprocessable(
                    "a route serves one app on this platform",
                ));
            }
            wanted = Some(app.to_string());
        }
        let before = route.app.clone();
        if !replace && wanted.is_some() && before.is_some() && before != wanted {
            let url = self.store.read(|state| state.url(&route));
            return Err(ApiError::unprocessable(format!(
                "route {url} serves another app: a route serves one app on this platform"
            )));
        }
        let mut touched: Vec<String> = wanted.iter().cloned().collect();
        if let Some(before) = before.iter().find(|before| wanted.as_ref() != Some(before)) {
            touched.push(before.clone());
        }
        let guid = guid.to_string();
        let change = {
            let guid = guid.clone();
            let wanted = wanted.clone();
            move |state: &mut State| {
                match &wanted {
                    Some(app) => state.map(&guid, app),
                    None => state.unmap(&guid),
                }
                Ok(())
            }
        };
        self.edit_at_once(&touched, &change).await?;
        for app in &touched {
            self.logs.add(
                app,
                "API",
                format!(
                    "Mapped route {}",
                    self.store.read(|state| state.url(&route))
                ),
            );
        }
        let route = self
            .store
            .read(|state| state.route(&guid).cloned())
            .ok_or_else(|| ApiError::not_found("Route"))?;
        Ok(json(
            StatusCode::OK,
            &json!({
                "destinations": views::destinations(&route),
                "links": {"self": {"href": format!("{}/v3/routes/{guid}/destinations", req.base)}, "route": {"href": format!("{}/v3/routes/{guid}", req.base)}},
            }),
        ))
    }

    async fn unmap_route(&self, req: &Req, guid: &str, destination: &str) -> Reply {
        self.route_as(req, guid, Need::Operate)?;
        let _route = self.controller.hold(guid).await;
        let route = self
            .store
            .read(|state| state.route(guid).cloned())
            .ok_or_else(|| ApiError::not_found("Route"))?;
        let app = match (&route.app, &route.destination) {
            (Some(app), Some(mapped)) if mapped == destination => app.clone(),
            _ => {
                return Err(ApiError::unprocessable(
                    "Unable to unmap route from destination. Ensure the route has a destination with this guid.",
                ));
            }
        };
        let guid = guid.to_string();
        let change = {
            let guid = guid.clone();
            move |state: &mut State| {
                state.unmap(&guid);
                Ok(())
            }
        };
        self.edit_at_once(std::slice::from_ref(&app), &change)
            .await?;
        self.logs.add(
            &app,
            "API",
            format!(
                "Unmapped route {}",
                self.store.read(|state| state.url(&route))
            ),
        );
        Ok(no_content())
    }

    // --- deployments

    /// A deployment (`crate::deployments`): the app's new snapshot, of the droplet given (made
    /// current) or the current one, the web process as its options set it, put a wicket's share
    /// at a time while the others serve the snapshot before; at once when the app serves nothing.
    /// Answered as it starts: the CLI polls it, the outcome is in the stats and the log.
    async fn create_deployment(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let app = body["relationships"]["app"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.app names the app"))?
            .to_string();
        self.app_as(req, &app, Need::Operate)?;
        let asked = deployments::Asked::parse(&body).map_err(ApiError::unprocessable)?;
        let _app = self.controller.hold(&app).await;
        let owner = self.controller.owner();
        let (deployment, superseded, blobs, before) = self.store.update(|state| {
            let found = state.app(&app).ok_or_else(|| ApiError::not_found("App"))?;
            let before = Controller::desired(state, found, owner);
            // all of it or none
            let mut next = state.clone();
            let (deployment, superseded, blobs) = deploy_on(&mut next, &app, &asked, &req.base)?;
            *state = next;
            Ok::<_, ApiError>((deployment, superseded, blobs, before))
        })?;
        self.store.delete(&blobs);
        if let Some(line) = superseded {
            self.logs.add(&app, "API", line);
        }
        self.logs.add(
            &app,
            "API",
            format!(
                "Created the {} deployment {} with droplet {}",
                deployment.strategy, deployment.guid, deployment.droplet
            ),
        );
        match deployment.stepping() {
            // what changes at once: the shares of a scale it asked
            true => {
                self.controller.sync_changed(&app, &before).await;
                let controller = self.controller.clone();
                tokio::spawn(controller.deploy(app.clone(), deployment.guid.clone()));
            }
            // the app served nothing: started
            false => self.controller.sync(&app).await,
        }
        let view = self.store.read(|state| {
            let kept = state.deployment(&deployment.guid).unwrap_or(&deployment);
            views::deployment(&req.base, state, kept)
        });
        Ok(json(StatusCode::CREATED, &view))
    }

    /// `cf cancel-deployment`: the app back on its snapshot and droplet before the deployment,
    /// every share put at once, canceling meanwhile (`crate::deployments`).
    async fn cancel_deployment(&self, req: &Req, guid: &str) -> Reply {
        let app = self.deployment_as(req, guid, Need::Operate)?;
        let _app = self.controller.hold(&app).await;
        let canceling = (self.store)
            .update(|state| state.cancel_deployment(guid))
            .map_err(ApiError::unprocessable)?;
        if canceling {
            self.logs
                .add(&app, "API", format!("Canceling the deployment {guid}"));
            self.controller.cancel_held(&app, guid).await;
        }
        self.deployment_view(req, guid)
    }

    /// `cf continue-deployment`: a paused canary's next step, else the rest of it.
    async fn continue_deployment(&self, req: &Req, guid: &str) -> Reply {
        let app = self.deployment_as(req, guid, Need::Operate)?;
        let _app = self.controller.hold(&app).await;
        (self.store)
            .update(|state| state.continue_deployment(guid))
            .map_err(ApiError::unprocessable)?;
        self.logs
            .add(&app, "API", format!("Continuing the deployment {guid}"));
        let controller = self.controller.clone();
        tokio::spawn(controller.deploy(app, guid.to_string()));
        self.deployment_view(req, guid)
    }

    /// The deployment `guid` as it is now.
    fn deployment_view(&self, req: &Req, guid: &str) -> Reply {
        let view = self.store.read(|state| {
            let deployment = state.deployment(guid);
            deployment.map(|deployment| views::deployment(&req.base, state, deployment))
        });
        view.map_or_else(
            || Err(ApiError::not_found("Deployment")),
            |view| Ok(json(StatusCode::OK, &view)),
        )
    }
}

/// Deploys on `state` what `asked` asks of the app `guid`: the droplet given made current, the
/// web process as asked, its snapshot taken ([`State::deploy`]). The deployment, the line saying
/// what it superseded, and the blobs no droplet names any more.
fn deploy_on(
    state: &mut State,
    guid: &str,
    asked: &deployments::Asked,
    base: &str,
) -> Result<(Deployment, Option<String>, Vec<Blob>), ApiError> {
    let app = state.app(guid).ok_or_else(|| ApiError::not_found("App"))?;
    let previous = app.current_droplet.clone();
    let serving = state.serving(app);
    let mut blobs = Vec::new();
    if let Some(droplet) = &asked.droplet {
        match state.droplet(droplet) {
            Some(found) if found.app == guid => {}
            _ => {
                return Err(ApiError::unprocessable(
                    "Unable to use droplet. Ensure the droplet exists and belongs to this app.",
                ));
            }
        }
        blobs = state.set_current_droplet(guid, droplet);
    }
    let app = state.app_mut(guid).expect("found above");
    asked.scaled.apply(app).map_err(ApiError::unprocessable)?;
    let running = snapshot(state, guid, base)?;
    // a step swaps a wicket's share in place, and a wicket runs one stack
    if let Some(serves) = serving
        .values()
        .find(|serves| serves.stack() != running.stack())
    {
        return Err(ApiError::unprocessable(format!(
            "a {} deployment of a droplet of the stack {} is not supported while the app runs \
             one of the stack {}: its instances move to other wickets. cf push without \
             --strategy stops and starts it",
            asked.strategy,
            running.stack().stack(),
            serves.stack().stack()
        )));
    }
    let previous = previous.unwrap_or_else(|| running.droplet.clone());
    let (deployment, superseded) = state.deploy(guid, running, serving, previous, asked);
    Ok((deployment, superseded, blobs))
}

/// The snapshot a start takes: the current droplet, the memory, the
/// annotations, what the service bindings grant, what the security groups
/// let it reach, and the guest's environment (`VCAP_APPLICATION` naming the API at `base`) with a tag of
/// its own.
fn snapshot(state: &State, guid: &str, base: &str) -> Result<Running, ApiError> {
    let app = state.app(guid).ok_or_else(|| ApiError::not_found("App"))?;
    let current = app
        .current_droplet
        .as_deref()
        .ok_or_else(|| ApiError::unprocessable("Assign a droplet before starting this app."))?;
    let droplet = state
        .droplet(current)
        .ok_or_else(|| ApiError::unprocessable("the current droplet is gone: push again"))?;
    let runtime = droplet.stack();
    // every wicket listed what it runs, and none this: nowhere to place it
    let stacks = state.stacks();
    if state.all_listed() && !stacks.contains(&runtime) {
        return Err(ApiError::unprocessable(format!(
            "the droplet is of the stack {}, which no wicket runs: they run {}",
            runtime.stack(),
            crate::wicket::stacks_named(&stacks)
        )));
    }
    let lifetime =
        manifest::check_annotations(&app.annotations, app.process.instances, Some(runtime))?;
    let readiness = app.process.readiness_health_check.clone();
    manifest::check_readiness(&readiness, &lifetime, Some(runtime))?;
    let liveness = app.process.health_check.clone();
    manifest::check_liveness(&liveness, &lifetime, Some(runtime))?;
    let annotation = |key: &str| {
        app.annotations
            .get(key)
            .and_then(|value| value.parse::<u64>().ok())
    };
    let (wasi, keyvalue) = services::grants(state, guid);
    if runtime == Runtime::Firecracker
        && let Some(granted) = services::wasm_only(state, guid)
    {
        return Err(ApiError::unprocessable(format!(
            "the droplet is a rootfs (the stack {}) and the app binds {granted}, which grant a \
             component's sandbox: a VM takes none of them, unbind them (cf unbind-service)",
            runtime.stack()
        )));
    }
    Ok(Running {
        droplet: droplet.guid.clone(),
        sha256: droplet.sha256.clone(),
        made: droplet.made.clone(),
        file: droplet
            .file
            .rsplit('/')
            .next()
            .unwrap_or(&droplet.file)
            .to_string(),
        staged_as: Some(runtime),
        files: droplet.files.as_ref().map(|files| files.sha256.clone()),
        memory_mib: app.process.memory_mb,
        budget_ms: annotation(BUDGET_ANNOTATION),
        lifetime,
        readiness,
        liveness,
        wasi,
        keyvalue,
        env: {
            let env = env::guest(base, state, app);
            env::check_guest(&env).map_err(ApiError::unprocessable)?;
            env
        },
        egress: security_groups::egress(state, &app.space)?,
        tag: Some(env::tag()),
        since: epoch_secs(),
    })
}

/// The pin a body's `lifecycle.data.stack` asks: a stack the wickets run pins the app to it
/// (`cf push -s`), null lifts the pin; None when the body names none.
fn pin(state: &State, body: &Value) -> Result<Option<Option<Runtime>>, ApiError> {
    match body["lifecycle"]["data"].get("stack") {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(stack)) => Ok(Some(Some(state.stack_named(stack)?))),
        Some(_) => Err(ApiError::unprocessable("Stack must be a string")),
    }
}

/// The lifecycle a body's `lifecycle.type` names: an app is a zip's
/// (`buildpack`) or an image's (`docker`).
fn lifecycle(body: &Value) -> Result<Option<Lifecycle>, ApiError> {
    match body["lifecycle"]["type"].as_str() {
        None => Ok(None),
        Some("buildpack") => Ok(Some(Lifecycle::Buildpack)),
        Some("docker") => Ok(Some(Lifecycle::Docker)),
        Some(other) => Err(ApiError::unprocessable(format!(
            "the {other} lifecycle is not supported: buildpack (a zip holding the component) or docker (an image)"
        ))),
    }
}

/// A docker package's `data`: the image, and the credentials of a private
/// one (`--docker-username`, with `CF_DOCKER_PASSWORD`).
fn docker_image(data: &Value) -> Result<DockerImage, ApiError> {
    let text = |key: &str| {
        data[key]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(String::from)
    };
    let image =
        text("image").ok_or_else(|| ApiError::unprocessable("data.image names the image"))?;
    Reference::parse(&image)?;
    let (username, password) = (text("username"), text("password"));
    if username.is_some() != password.is_some() {
        return Err(ApiError::unprocessable(
            "data.username and data.password go together: --docker-username, with CF_DOCKER_PASSWORD",
        ));
    }
    Ok(DockerImage {
        image,
        username,
        password,
    })
}

/// The feature flags the CLI asks for: docker images, no sharing.
/// Checks the space `space` a body names to make something in: CC's 422 when it is none the
/// holder sees, 403 when the holder may not develop in it.
pub(crate) fn space_to_develop(
    access: &Access,
    state: &State,
    space: &str,
) -> Result<(), ApiError> {
    if state.space(space).is_none() || !access.can(state, space, Need::Read) {
        return Err(ApiError::unprocessable(
            "Invalid space. Ensure that the space exists and you have access to it.",
        ));
    }
    access.check(state, space, Need::Develop, "Space")
}

fn feature_flag(name: &str) -> Value {
    json!({"name": name, "enabled": name == "diego_docker", "updated_at": null, "custom_error_message": null, "links": {}})
}

/// Merges `metadata` (labels and annotations; a null value removes the
/// key) into the app, as CC does; refused, it changes nothing.
fn apply_metadata(
    app: &mut crate::store::App,
    metadata: &Value,
    runtime: Option<Runtime>,
) -> Result<(), ApiError> {
    let (mut labels, mut annotations) = (app.labels.clone(), app.annotations.clone());
    for (what, map) in [("labels", &mut labels), ("annotations", &mut annotations)] {
        if let Some(given) = metadata[what].as_object() {
            for (key, value) in given {
                match value {
                    Value::Null => {
                        map.remove(key);
                    }
                    Value::String(text) => {
                        map.insert(key.clone(), text.clone());
                    }
                    other => {
                        map.insert(key.clone(), other.to_string());
                    }
                }
            }
        }
    }
    let lifetime = manifest::check_annotations(&annotations, app.process.instances, runtime)?;
    manifest::check_readiness(&app.process.readiness_health_check, &lifetime, runtime)?;
    manifest::check_liveness(&app.process.health_check, &lifetime, runtime)?;
    app.labels = labels;
    app.annotations = annotations;
    Ok(())
}

/// `response` with `warnings` as the header the CLI prints, each encoded: it splits them at the
/// commas.
fn warned(mut response: Response, warnings: &[String]) -> Response {
    if !warnings.is_empty() {
        let encoded: Vec<String> = warnings
            .iter()
            .map(|warning| form_urlencoded::byte_serialize(warning.as_bytes()).collect())
            .collect();
        response.headers_mut().insert(
            "x-cf-warnings",
            HeaderValue::from_str(&encoded.join(",")).expect("form encoded"),
        );
    }
    response
}

pub(crate) fn json(status: StatusCode, body: &Value) -> Response {
    let body = serde_json::to_vec(body).expect("plain data");
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .expect("a plain response")
}

pub(crate) fn no_content() -> Response {
    hyper::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .expect("a plain response")
}

/// The passcode page's copy button: the clipboard where the page is a secure context (HTTPS,
/// loopback), else the selection copied.
const COPY_SCRIPT: &str = "<script>document.getElementById('copy').addEventListener('click',async function(){const code=document.getElementById('passcode');try{await navigator.clipboard.writeText(code.textContent)}catch(e){const range=document.createRange();range.selectNodeContents(code);const selection=getSelection();selection.removeAllRanges();selection.addRange(range);document.execCommand('copy')}this.textContent='Copied'})</script>";

/// A page of `cf login --sso` (the passcode, the callback's answers): `title` and the `body`
/// fragment, drawn as the wickets' consoles are.
fn page(status: StatusCode, title: &str, body: String) -> Response {
    text(
        status,
        "text/html; charset=utf-8",
        format!(
            "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"color-scheme\" content=\"light dark\"><title>{title} - Sluice cf-api</title><link rel=\"icon\" type=\"image/svg+xml\" href=\"{ICON}\"><link rel=\"stylesheet\" href=\"console.css\"><style>{STYLE}</style></head><body><header class=\"masthead\"><span class=\"brand\">{BRAND}</span></header><main class=\"page\"><section class=\"panel\"><h1 class=\"rule-heading\">{title}</h1>{body}</section></main></body></html>"
        ),
    )
}

/// The brand of the wickets' consoles (their `index.html`), named cf-api.
const BRAND: &str = "<svg class=\"brand__mark\" viewBox=\"0 0 32 32\" aria-hidden=\"true\"><path d=\"M3 9h9M3 23h9M20 9h9M20 23h9\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"/><path d=\"M12 9l4 7M12 23l4-7\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2.5\" stroke-linecap=\"round\"/><path d=\"M2 16h8M22 16h8\" fill=\"none\" stroke=\"var(--water)\" stroke-width=\"1.5\" stroke-dasharray=\"2 3\"/></svg><span class=\"brand__word\">sluice</span><span class=\"brand__sub\">cf-api</span>";

/// Its mark as the favicon, the palette pinned to the light theme (a favicon cannot read CSS
/// variables).
const ICON: &str = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cpath d='M3 9h9M3 23h9M20 9h9M20 23h9' fill='none' stroke='%23143b69' stroke-width='1.5'/%3E%3Cpath d='M12 9l4 7M12 23l4-7' fill='none' stroke='%23143b69' stroke-width='2.5' stroke-linecap='round'/%3E%3Cpath d='M2 16h8M22 16h8' fill='none' stroke='%231b86c2' stroke-width='1.5' stroke-dasharray='2 3'/%3E%3C/svg%3E";

/// What the consoles' style has not: the passcode, a literal value large enough to read off.
const STYLE: &str = ".panel{max-width:44rem}.passcode{display:flex;flex-wrap:wrap;align-items:center;gap:1rem;margin:1.5rem 0}.passcode code{padding:.4rem 1rem .25rem;border:1px solid var(--hair);background:var(--paper-raised);font-size:var(--step-4);letter-spacing:.12em;user-select:all}";

fn redirect(location: &str) -> Response {
    hyper::Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::new()))
        .expect("a plain response")
}

/// `text` safe inside an HTML element.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn text(status: StatusCode, content_type: &str, body: String) -> Response {
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Full::new(Bytes::from(body)))
        .expect("a plain response")
}
