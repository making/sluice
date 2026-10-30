//! The HTTP server: the subset of the Cloud Controller v3 API the cf CLI
//! uses, the UAA endpoints (`/uaa`) and log-cache's read API (`/api/v1`) on
//! one origin, in CC's shapes (an error is `{"errors": [{"code", "title",
//! "detail"}]}`). Every request but the root document, `/v3/info` and the
//! UAA endpoints needs a token of [`Uaa`]; with a [`Provider`], the browser
//! pages `/uaa/passcode` and `/uaa/callback` of `cf login --sso`.
//!
//! Routes and `instances` apply at once: an edit the wicket refuses is
//! answered 422 with its reason and changes nothing. The droplet and the
//! rest of the sandbox, the service bindings' grants and the environment
//! variables included, apply at a start, restart or deployment: the running
//! app is the snapshot taken then, and the outcome shows in the process
//! stats and the log. The services are in [`crate::services`], the
//! environment variables in [`crate::env`].

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{HeaderMap, HeaderValue};
use http::{Method, StatusCode, header};
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::controller::Controller;
use crate::env;
use crate::logs::{self, Logs};
use crate::manifest;
use crate::oidc::Provider;
use crate::services;
use crate::staging;
use crate::store::{
    BUDGET_ANNOTATION, Build, Deployment, Droplet, INSTANCES_ANNOTATION, JobError, Package,
    Running, State, Store, epoch_secs, guid, now,
};
use crate::uaa::{self, Uaa};
use crate::views;
use crate::wicket::Failure;

/// Largest package upload.
const MAX_UPLOAD_BYTES: usize = 160 << 20;
/// Largest body of any other request.
const MAX_BODY_BYTES: usize = 1 << 20;
/// Connections open at once.
const MAX_CONNECTIONS: usize = 64;

pub(crate) type Response = hyper::Response<Full<Bytes>>;
pub(crate) type Reply = Result<Response, ApiError>;

pub struct Api {
    pub store: Arc<Store>,
    pub uaa: Uaa,
    /// The OpenID Provider users log in through; None: any credentials.
    pub provider: Option<Provider>,
    /// Whether the listener serves TLS: the scheme of the links.
    pub tls: bool,
    pub logs: Arc<Logs>,
    pub controller: Arc<Controller>,
    /// What `/v3/info` names as the build.
    pub build: String,
}

/// An error in CC's shape.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: u32,
    pub title: &'static str,
    pub detail: String,
}

impl ApiError {
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

    fn too_large(limit: usize) -> ApiError {
        ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: 10013,
            title: "CF-AppBitsUploadInvalid",
            detail: format!("the body is {} MiB at most", limit >> 20),
        }
    }

    /// The wicket's refusal, as the user reads it.
    fn refused(failure: &Failure) -> ApiError {
        ApiError::unprocessable(format!("the wicket refused it: {failure}"))
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

/// A request read whole.
pub(crate) struct Req {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) query: Vec<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
    /// Where the CLI reaches this API: the links name it.
    pub(crate) base: String,
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

    fn header(&self, name: header::HeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

/// Serves the API on `listener` for good, [`MAX_CONNECTIONS`] at once; with
/// `tls`, each connection's handshake first.
pub async fn run(listener: TcpListener, tls: Option<tokio_rustls::TlsAcceptor>, api: Arc<Api>) {
    let open = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    loop {
        let permit = open.clone().acquire_owned().await.expect("never closed");
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let api = api.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => serve_connection(stream, api).await,
                    Err(e) => eprintln!("tls handshake failed: {e}"),
                },
                None => serve_connection(stream, api).await,
            }
            drop(permit);
        });
    }
}

async fn serve_connection<S>(stream: S, api: Arc<Api>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |request| {
        let api = api.clone();
        async move { Ok::<_, Infallible>(api.handle(request).await) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

impl Api {
    async fn handle(&self, request: hyper::Request<Incoming>) -> Response {
        let (parts, body) = request.into_parts();
        let path = parts.uri.path().to_string();
        let limit = if path.ends_with("/upload") {
            MAX_UPLOAD_BYTES
        } else {
            MAX_BODY_BYTES
        };
        let body = match Limited::new(body, limit).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e)
                if e.downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some() =>
            {
                return ApiError::too_large(limit).into();
            }
            Err(e) => return ApiError::bad_request(format!("the body: {e}")).into(),
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
            base: format!("{scheme}://{host}"),
            path,
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
                Ok(json(StatusCode::OK, &views::info(base, &self.build)))
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
                        match self.uaa.passcode(identity) {
                            Ok(passcode) => Ok(page(
                                StatusCode::OK,
                                "Temporary Authentication Code",
                                format!(
                                    "<p>Signed in as <b>{}</b>. Paste this code into <code>cf login --sso</code> (or <code>cf login --sso-passcode {passcode}</code>) within {} minutes; it works once.</p><p><code id=\"passcode\" class=\"big\">{passcode}</code></p>",
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
                    ["api", "v1", rest @ ..] => self.log_cache(req, rest),
                    ["v3", rest @ ..] => self.v3(req, rest).await,
                    _ => Err(ApiError::unknown(&req.method, &req.path)),
                }
            }
        }
    }

    fn authenticate(&self, req: &Req) -> Result<(), ApiError> {
        let token = req
            .header(header::AUTHORIZATION)
            .and_then(|value| {
                value
                    .strip_prefix("bearer ")
                    .or_else(|| value.strip_prefix("Bearer "))
            })
            .ok_or_else(ApiError::unauthenticated)?;
        self.uaa
            .verify(token.trim())
            .map(|_| ())
            .map_err(|e| ApiError::invalid_token(&e))
    }

    fn log_cache(&self, req: &Req, segments: &[&str]) -> Reply {
        match (&req.method, segments) {
            (&Method::GET, ["info"]) => Ok(json(
                StatusCode::OK,
                &json!({"version": "3.0.0", "vm_uptime": epoch_secs().to_string()}),
            )),
            (&Method::GET, ["read", app]) => {
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
        match (&req.method, segments) {
            // --- the organization, the space, the domains, the stack
            (&Method::GET, ["organizations"]) => {
                listing(store.read(|state| vec![views::org(base, state)]), None)
            }
            (&Method::GET, ["organizations", guid]) => store
                .read(|state| (state.org.guid == *guid).then(|| views::org(base, state)))
                .map_or_else(|| Err(ApiError::not_found("Organization")), |org| Ok(json(StatusCode::OK, &org))),
            (&Method::GET, ["organizations", _, "domains"]) | (&Method::GET, ["domains"]) => {
                listing(store.read(|state| state.domains.iter().map(|domain| views::domain(base, domain)).collect()), None)
            }
            (&Method::GET, ["organizations", _, "domains", "default"]) => store
                .read(|state| state.domains.first().map(|domain| views::domain(base, domain)))
                .map_or_else(|| Err(ApiError::not_found("Domain")), |domain| Ok(json(StatusCode::OK, &domain))),
            (&Method::GET, ["domains", guid]) => store
                .read(|state| state.domain(guid).map(|domain| views::domain(base, domain)))
                .map_or_else(|| Err(ApiError::not_found("Domain")), |domain| Ok(json(StatusCode::OK, &domain))),
            (&Method::POST, ["domains"]) => Err(ApiError::unprocessable(
                "private domains are not supported: the shared domains are the cf-api's --domain",
            )),
            (&Method::DELETE, ["domains", _]) => Err(ApiError::unprocessable(
                "the shared domains are the cf-api's --domain",
            )),
            (&Method::GET, ["spaces"]) => {
                let included = req.q("include").is_some_and(|include| include.contains("organization"));
                store.read(|state| {
                    listing(
                        vec![views::space(base, state)],
                        included.then(|| json!({"organizations": [views::org(base, state)]})),
                    )
                })
            }
            (&Method::GET, ["spaces", guid]) => store
                .read(|state| (state.space.guid == *guid).then(|| views::space(base, state)))
                .map_or_else(|| Err(ApiError::not_found("Space")), |space| Ok(json(StatusCode::OK, &space))),
            (&Method::GET, ["spaces", _, "features", name]) => Ok(json(
                StatusCode::OK,
                &json!({"name": name, "description": "", "enabled": false}),
            )),
            (&Method::GET, ["spaces", _, "features"]) => Ok(json(
                StatusCode::OK,
                &json!({"resources": [{"name": "ssh", "description": "", "enabled": false}], "pagination": {"total_results": 1, "total_pages": 1}}),
            )),
            (&Method::GET, ["spaces", _, "relationships", "isolation_segment"]) => {
                Ok(json(StatusCode::OK, &json!({"data": null, "links": {}})))
            }
            (&Method::GET, ["spaces", _, "running_security_groups"])
            | (&Method::GET, ["spaces", _, "staging_security_groups"]) => empty(),
            (&Method::POST, ["spaces", _, "actions", "apply_manifest"]) => self.apply_manifest(req).await,
            (&Method::POST, ["spaces", _, "manifest_diff"]) => self.manifest_diff(req),
            (&Method::GET, ["stacks"]) => listing(vec![views::stack(base)], None),
            (&Method::GET, ["stacks", _]) => Ok(json(StatusCode::OK, &views::stack(base))),
            (&Method::GET, ["feature_flags"]) => listing(
                ["service_instance_sharing", "diego_docker"]
                    .iter()
                    .map(|name| json!({"name": name, "enabled": false, "updated_at": null, "custom_error_message": null, "links": {}}))
                    .collect(),
                None,
            ),
            (&Method::GET, ["feature_flags", name]) => Ok(json(
                StatusCode::OK,
                &json!({"name": name, "enabled": false, "updated_at": null, "custom_error_message": null, "links": {}}),
            )),
            // --- apps
            (&Method::GET, ["apps"]) => {
                let include = req.q("include").unwrap_or_default();
                store.read(|state| {
                    let mut included = serde_json::Map::new();
                    if include.contains("space") {
                        included.insert("spaces".into(), json!([views::space(base, state)]));
                    }
                    if include.contains("space.organization") {
                        included.insert("organizations".into(), json!([views::org(base, state)]));
                    }
                    listing(
                        state.apps.iter().map(|app| views::app(base, state, app)).collect(),
                        (!included.is_empty()).then_some(Value::Object(included)),
                    )
                })
            }
            (&Method::POST, ["apps"]) => self.create_app(req).await,
            (&Method::GET, ["apps", guid]) => self.app_view(req, guid),
            (&Method::PATCH, ["apps", guid]) => self.patch_app(req, guid).await,
            (&Method::DELETE, ["apps", guid]) => self.delete_app(req, guid).await,
            (&Method::GET, ["apps", guid, "processes"]) => {
                let app = self.app(guid)?;
                listing(store.read(|state| vec![views::process(base, state, &app)]), None)
            }
            (&Method::GET, ["apps", guid, "processes", "web"]) => {
                let app = self.app(guid)?;
                Ok(json(StatusCode::OK, &store.read(|state| views::process(base, state, &app))))
            }
            (&Method::GET, ["apps", guid, "processes", "web", "stats"]) => self.stats(guid),
            (&Method::POST, ["apps", guid, "processes", "web", "actions", "scale"]) => {
                self.scale(req, guid).await
            }
            (&Method::GET, ["apps", _, "processes", kind]) | (&Method::GET, ["apps", _, "processes", kind, "stats"]) | (&Method::POST, ["apps", _, "processes", kind, "actions", "scale"]) => {
                Err(ApiError::not_found(&format!("Process ({kind})")))
            }
            (&Method::GET, ["apps", guid, "routes"]) => {
                self.app(guid)?;
                listing(
                    store.read(|state| state.routes_of(guid).map(|route| views::route(base, state, route)).collect()),
                    None,
                )
            }
            (&Method::GET, ["apps", guid, "packages"]) => {
                self.app(guid)?;
                listing(
                    store.read(|state| state.packages.iter().filter(|package| package.app == *guid).map(|package| views::package(base, package)).collect()),
                    None,
                )
            }
            (&Method::GET, ["apps", guid, "droplets"]) => {
                self.app(guid)?;
                listing(
                    store.read(|state| state.droplets.iter().filter(|droplet| droplet.app == *guid).map(|droplet| views::droplet(base, droplet)).collect()),
                    None,
                )
            }
            (&Method::GET, ["apps", guid, "droplets", "current"]) => {
                let app = self.app(guid)?;
                let current = app.current_droplet.as_deref().ok_or_else(|| ApiError::not_found("Droplet"))?;
                store
                    .read(|state| state.droplet(current).map(|droplet| views::droplet(base, droplet)))
                    .map_or_else(|| Err(ApiError::not_found("Droplet")), |droplet| Ok(json(StatusCode::OK, &droplet)))
            }
            (&Method::GET, ["apps", guid, "relationships", "current_droplet"]) => {
                let app = self.app(guid)?;
                Ok(json(StatusCode::OK, &json!({"data": app.current_droplet.as_ref().map(|guid| json!({"guid": guid})), "links": {}})))
            }
            (&Method::PATCH, ["apps", guid, "relationships", "current_droplet"]) => {
                self.set_current_droplet(req, guid).await
            }
            (&Method::POST, ["apps", guid, "actions", action @ ("start" | "stop" | "restart")]) => {
                self.lifecycle(req, guid, action).await
            }
            (&Method::GET, ["apps", guid, "env"]) => {
                let app = self.app(guid)?;
                Ok(json(StatusCode::OK, &store.read(|state| views::env(base, state, &app))))
            }
            (&Method::GET, ["apps", guid, "environment_variables"]) => {
                let app = self.app(guid)?;
                Ok(json(StatusCode::OK, &views::environment_variables(base, &app)))
            }
            (&Method::PATCH, ["apps", guid, "environment_variables"]) => {
                self.set_env(req, guid).await
            }
            (&Method::GET, ["apps", guid, "manifest"]) => {
                let app = self.app(guid)?;
                Ok(text(StatusCode::OK, "application/x-yaml", store.read(|state| views::manifest(state, &app))))
            }
            (&Method::GET, ["apps", guid, "features"]) => {
                self.app(guid)?;
                Ok(json(StatusCode::OK, &json!({"resources": [{"name": "ssh", "description": "", "enabled": false}, {"name": "revisions", "description": "", "enabled": false}], "pagination": {"total_results": 2, "total_pages": 1}})))
            }
            (&Method::GET, ["apps", guid, "features", name]) => {
                self.app(guid)?;
                Ok(json(StatusCode::OK, &json!({"name": name, "description": "", "enabled": false})))
            }
            (&Method::GET, ["apps", guid, "ssh_enabled"]) => {
                self.app(guid)?;
                Ok(json(StatusCode::OK, &json!({"enabled": false, "reason": "ssh is not supported: a wasm component has no shell"})))
            }
            (&Method::GET, ["apps", guid, "sidecars"]) | (&Method::GET, ["apps", guid, "revisions"]) | (&Method::GET, ["apps", guid, "revisions", "deployed"]) | (&Method::GET, ["apps", guid, "tasks"]) => {
                self.app(guid)?;
                empty()
            }
            (&Method::POST, ["apps", guid, "tasks"]) => {
                self.app(guid)?;
                Err(ApiError::unprocessable("tasks are not supported: a wasm component serves requests only"))
            }
            (&Method::POST, ["apps", guid, "sidecars"]) => {
                self.app(guid)?;
                Err(ApiError::unprocessable("sidecars are not supported"))
            }
            // --- processes
            (&Method::GET, ["processes"]) => listing(
                store.read(|state| state.apps.iter().map(|app| views::process(base, state, app)).collect()),
                None,
            ),
            (&Method::GET, ["processes", guid]) => {
                let app = self.app_of_process(guid)?;
                Ok(json(StatusCode::OK, &store.read(|state| views::process(base, state, &app))))
            }
            (&Method::PATCH, ["processes", guid]) => self.patch_process(req, guid).await,
            (&Method::GET, ["processes", guid, "stats"]) => {
                let app = self.app_of_process(guid)?;
                self.stats(&app.guid)
            }
            (&Method::GET, ["processes", guid, "sidecars"]) => {
                self.app_of_process(guid)?;
                empty()
            }
            (&Method::POST, ["processes", guid, "actions", "scale"]) => {
                let app = self.app_of_process(guid)?;
                self.scale(req, &app.guid).await
            }
            // --- packages, builds, droplets
            (&Method::GET, ["packages"]) => listing(
                store.read(|state| state.packages.iter().map(|package| views::package(base, package)).collect()),
                None,
            ),
            (&Method::POST, ["packages"]) => self.create_package(req),
            (&Method::GET, ["packages", guid]) => store
                .read(|state| state.package(guid).map(|package| views::package(base, package)))
                .map_or_else(|| Err(ApiError::not_found("Package")), |package| Ok(json(StatusCode::OK, &package))),
            (&Method::POST, ["packages", guid, "upload"]) => self.upload(req, guid),
            (&Method::GET, ["packages", guid, "droplets"]) => {
                store.read(|state| state.package(guid).map(|_| ())).ok_or_else(|| ApiError::not_found("Package"))?;
                listing(
                    store.read(|state| state.droplets.iter().filter(|droplet| droplet.package == *guid).map(|droplet| views::droplet(base, droplet)).collect()),
                    None,
                )
            }
            (&Method::DELETE, ["packages", guid]) => {
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
            (&Method::POST, ["resource_matches"]) => Ok(json(StatusCode::CREATED, &json!({"resources": []}))),
            (&Method::GET, ["builds"]) => listing(
                store.read(|state| state.builds.iter().map(|build| views::build(base, state, build)).collect()),
                None,
            ),
            (&Method::POST, ["builds"]) => self.create_build(req),
            (&Method::GET, ["builds", guid]) => store
                .read(|state| state.build(guid).map(|build| views::build(base, state, build)))
                .map_or_else(|| Err(ApiError::not_found("Build")), |build| Ok(json(StatusCode::OK, &build))),
            (&Method::GET, ["droplets"]) => listing(
                store.read(|state| state.droplets.iter().map(|droplet| views::droplet(base, droplet)).collect()),
                None,
            ),
            (&Method::GET, ["droplets", guid]) => store
                .read(|state| state.droplet(guid).map(|droplet| views::droplet(base, droplet)))
                .map_or_else(|| Err(ApiError::not_found("Droplet")), |droplet| Ok(json(StatusCode::OK, &droplet))),
            // --- routes
            (&Method::GET, ["routes"]) => {
                let include = req.q("include").unwrap_or_default();
                store.read(|state| {
                    let included = include.contains("domain").then(|| {
                        json!({"domains": state.domains.iter().map(|domain| views::domain(base, domain)).collect::<Vec<_>>()})
                    });
                    listing(state.routes.iter().map(|route| views::route(base, state, route)).collect(), included)
                })
            }
            (&Method::POST, ["routes"]) => self.create_route(req),
            (&Method::GET, ["routes", guid]) | (&Method::PATCH, ["routes", guid]) => store
                .read(|state| state.route(guid).map(|route| views::route(base, state, route)))
                .map_or_else(|| Err(ApiError::not_found("Route")), |route| Ok(json(StatusCode::OK, &route))),
            (&Method::DELETE, ["routes", guid]) => self.delete_route(req, guid).await,
            (&Method::GET, ["routes", guid, "destinations"]) => {
                let route = store.read(|state| state.route(guid).cloned()).ok_or_else(|| ApiError::not_found("Route"))?;
                Ok(json(StatusCode::OK, &json!({"destinations": views::destinations(&route), "links": {"self": {"href": format!("{base}/v3/routes/{guid}/destinations")}, "route": {"href": format!("{base}/v3/routes/{guid}")}}})))
            }
            (&Method::POST, ["routes", guid, "destinations"]) => self.map_route(req, guid, false).await,
            (&Method::PATCH, ["routes", guid, "destinations"]) => self.map_route(req, guid, true).await,
            (&Method::DELETE, ["routes", guid, "destinations", destination]) => {
                self.unmap_route(req, guid, destination).await
            }
            // --- deployments, jobs
            (&Method::GET, ["deployments"]) => listing(
                store.read(|state| state.deployments.iter().map(|deployment| views::deployment(base, deployment)).collect()),
                None,
            ),
            (&Method::POST, ["deployments"]) => self.create_deployment(req).await,
            (&Method::GET, ["deployments", guid]) => store
                .read(|state| state.deployment(guid).map(|deployment| views::deployment(base, deployment)))
                .map_or_else(|| Err(ApiError::not_found("Deployment")), |deployment| Ok(json(StatusCode::OK, &deployment))),
            (&Method::POST, ["deployments", _, "actions", _]) => Err(ApiError::unprocessable(
                "a deployment is finalized at once: the wicket swaps the app in place",
            )),
            (&Method::GET, ["jobs", guid]) => store
                .read(|state| state.job(guid).map(|job| views::job(base, job)))
                .map_or_else(|| Err(ApiError::not_found("Job")), |job| Ok(json(StatusCode::OK, &job))),
            // --- services
            (_, ["service_brokers" | "service_offerings" | "service_plans" | "service_instances" | "service_credential_bindings", ..]) => {
                self.services(req, segments).await
            }
            (&Method::POST, ["service_route_bindings"]) => {
                Err(ApiError::unprocessable("route services are not supported"))
            }
            // --- what there is none of
            (&Method::GET, ["buildpacks"])
            | (&Method::GET, ["security_groups"])
            | (&Method::GET, ["service_route_bindings"])
            | (&Method::GET, ["audit_events"])
            | (&Method::GET, ["isolation_segments"])
            | (&Method::GET, ["users"])
            | (&Method::GET, ["roles"])
            | (&Method::GET, ["space_quotas"])
            | (&Method::GET, ["organization_quotas"])
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

    fn app(&self, guid: &str) -> Result<crate::store::App, ApiError> {
        self.store
            .read(|state| state.app(guid).cloned())
            .ok_or_else(|| ApiError::not_found("App"))
    }

    fn app_of_process(&self, process: &str) -> Result<crate::store::App, ApiError> {
        self.store
            .read(|state| {
                state
                    .apps
                    .iter()
                    .find(|app| app.process.guid == process)
                    .cloned()
            })
            .ok_or_else(|| ApiError::not_found("Process"))
    }

    fn app_view(&self, req: &Req, guid: &str) -> Reply {
        let app = self.app(guid)?;
        Ok(json(
            StatusCode::OK,
            &self.store.read(|state| views::app(&req.base, state, &app)),
        ))
    }

    fn stats(&self, guid: &str) -> Reply {
        let app = self.app(guid)?;
        let owner = &self.controller.wicket.owner;
        Ok(json(
            StatusCode::OK,
            &self.store.read(|state| views::stats(state, &app, owner)),
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
        let mut response = json(StatusCode::ACCEPTED, &Value::Null);
        *response.body_mut() = Full::new(Bytes::new());
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(&format!("{}/v3/jobs/{guid}", req.base))
                .expect("a plain location"),
        );
        Ok(response)
    }

    /// Changes the state as `change` says, once the wicket took the apps
    /// `guids` as they would be: a refusal changes nothing. The wicket
    /// unreachable, the change is kept: it is put when the wicket is back.
    async fn edit_at_once(
        &self,
        guids: &[String],
        change: &(dyn Fn(&mut State) -> Result<(), ApiError> + Sync),
    ) -> Result<(), ApiError> {
        let _edit = self.controller.edits.lock().await;
        let trial = self.store.read(|state| {
            let mut trial = state.clone();
            change(&mut trial)?;
            Ok::<_, ApiError>(
                guids
                    .iter()
                    .map(|guid| (guid.clone(), self.controller.desired_of(&trial, guid)))
                    .collect::<Vec<_>>(),
            )
        })?;
        let mut applied: Vec<&str> = Vec::new();
        for (guid, desired) in &trial {
            match self.controller.apply(guid, desired.as_ref()).await {
                Ok(()) | Err(Failure::Unreachable(_)) => applied.push(guid),
                Err(refused) => {
                    // the apps put before go back to what the state has
                    for guid in applied {
                        let _ = self.controller.sync(guid).await;
                    }
                    return Err(ApiError::refused(&refused));
                }
            }
        }
        self.store.update(|state| change(state))
    }

    // --- the manifest

    async fn apply_manifest(&self, req: &Req) -> Reply {
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
        let _edit = self.controller.edits.lock().await;
        let (trial, guids) = {
            let mut trial = self.store.read(State::clone);
            match manifest::apply(&mut trial, &manifest) {
                Ok(guids) => (trial, guids),
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
        let mut applied: Vec<&str> = Vec::new();
        for guid in &guids {
            let desired = self.controller.desired_of(&trial, guid);
            if desired.is_none()
                && self
                    .store
                    .read(|state| self.controller.desired_of(state, guid))
                    .is_none()
            {
                continue;
            }
            match self.controller.apply(guid, desired.as_ref()).await {
                Ok(()) | Err(Failure::Unreachable(_)) => applied.push(guid),
                Err(refused) => {
                    for guid in applied {
                        let _ = self.controller.sync(guid).await;
                    }
                    return self.job(
                        req,
                        "space.apply_manifest",
                        vec![ApiError::refused(&refused).job_error()],
                        None,
                    );
                }
            }
        }
        // the bindings `services:` made
        let bound: Vec<(String, String)> = self.store.read(|state| {
            trial
                .bindings
                .iter()
                .filter(|binding| state.binding(&binding.guid).is_none())
                .filter_map(|binding| {
                    let instance = trial.instance(&binding.instance)?;
                    Some((binding.app.clone(), instance.name.clone()))
                })
                .collect()
        });
        let space = self.store.update(|state| {
            let wicket = std::mem::take(&mut state.wicket);
            *state = trial;
            state.wicket = wicket;
            state.space.guid.clone()
        });
        for guid in &guids {
            self.logs.add(guid, "API", "Applied the manifest");
        }
        for (app, instance) in bound {
            self.logs.add(
                &app,
                "API",
                format!("Bound service instance {instance}: applies at the next start, restart or restage"),
            );
        }
        self.job(
            req,
            "space.apply_manifest",
            Vec::new(),
            Some(("space".into(), format!("/v3/spaces/{space}"))),
        )
    }

    fn manifest_diff(&self, req: &Req) -> Reply {
        let manifest = manifest::parse(
            std::str::from_utf8(&req.body)
                .map_err(|_| ApiError::bad_request("the manifest is not UTF-8"))?,
        )?;
        let diff: Vec<Value> = self.store.read(|state| {
            manifest
                .applications
                .iter()
                .enumerate()
                .filter(|(_, spec)| state.app_named(spec.name.trim()).is_none())
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
        if body["lifecycle"]["type"]
            .as_str()
            .is_some_and(|kind| kind == "docker")
        {
            return Err(ApiError::unprocessable(
                "docker images are not supported: push a directory or a zip holding the component",
            ));
        }
        let _edit = self.controller.edits.lock().await;
        let view = self.store.update(|state| {
            if state.app_named(name).is_some() {
                return Err(ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    code: 10016,
                    title: "CF-UniquenessError",
                    detail: format!("App with the name '{name}' already exists."),
                });
            }
            let guid = state.create_app(name).guid.clone();
            let app = state.app_mut(&guid).expect("just created");
            app.env = env;
            apply_metadata(app, &body["metadata"])?;
            if let Some(buildpacks) = body["lifecycle"]["data"]["buildpacks"].as_array() {
                app.buildpacks = buildpacks
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
            }
            if let Some(stack) = body["lifecycle"]["data"]["stack"].as_str() {
                app.stack = stack.to_string();
            }
            let app = app.clone();
            Ok(views::app(&req.base, state, &app))
        })?;
        Ok(json(StatusCode::CREATED, &view))
    }

    async fn patch_app(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        self.app(guid)?;
        let _edit = self.controller.edits.lock().await;
        let view = self.store.update(|state| {
            if let Some(name) = body["name"].as_str()
                && state
                    .app_named(name)
                    .is_some_and(|other| other.guid != guid)
            {
                return Err(ApiError::unprocessable(format!(
                    "App with the name '{name}' already exists."
                )));
            }
            let app = state
                .app_mut(guid)
                .ok_or_else(|| ApiError::not_found("App"))?;
            if let Some(name) = body["name"].as_str() {
                app.name = name.to_string();
            }
            if body["lifecycle"]["type"]
                .as_str()
                .is_some_and(|kind| kind == "docker")
            {
                return Err(ApiError::unprocessable("docker images are not supported"));
            }
            if let Some(buildpacks) = body["lifecycle"]["data"]["buildpacks"].as_array() {
                app.buildpacks = buildpacks
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
            }
            if let Some(stack) = body["lifecycle"]["data"]["stack"].as_str() {
                app.stack = stack.to_string();
            }
            apply_metadata(app, &body["metadata"])?;
            app.updated_at = now();
            let app = app.clone();
            Ok(views::app(&req.base, state, &app))
        })?;
        Ok(json(StatusCode::OK, &view))
    }

    /// Merges the body's `var` into the app's environment variables (a null
    /// removes one); the guest gets them at its next start, as on CF. The
    /// log names no value.
    async fn set_env(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        let var = body["var"]
            .as_object()
            .ok_or_else(|| ApiError::unprocessable("Var must be an object"))?;
        self.app(guid)?;
        let _edit = self.controller.edits.lock().await;
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

    async fn delete_app(&self, req: &Req, guid: &str) -> Reply {
        let app = self.app(guid)?;
        let _edit = self.controller.edits.lock().await;
        // gone from the wicket first; unreachable, the watch removes it later
        let _ = self.controller.apply(guid, None).await;
        let blobs = self.store.update(|state| state.remove_app(guid));
        self.store.delete(&blobs);
        self.logs.forget(guid);
        println!("[{guid}] API: deleted app {}", app.name);
        self.job(req, "app.delete", Vec::new(), None)
    }

    async fn patch_process(&self, req: &Req, process: &str) -> Reply {
        let body: Value = req.json()?;
        let app = self.app_of_process(process)?;
        let _edit = self.controller.edits.lock().await;
        let view = self.store.update(|state| {
            let app = state
                .app_mut(&app.guid)
                .ok_or_else(|| ApiError::not_found("Process"))?;
            if let Some(command) = body.get("command") {
                app.process.command = command
                    .as_str()
                    .map(String::from)
                    .filter(|command| !command.is_empty());
            }
            if let Some(kind) = body["health_check"]["type"].as_str() {
                app.process.health_check.kind = kind.to_string();
            }
            if let Some(data) = body["health_check"]["data"].as_object() {
                app.process.health_check.timeout = data.get("timeout").and_then(Value::as_u64);
                app.process.health_check.invocation_timeout =
                    data.get("invocation_timeout").and_then(Value::as_u64);
                app.process.health_check.endpoint = data
                    .get("endpoint")
                    .and_then(Value::as_str)
                    .map(String::from);
            }
            app.process.updated_at = now();
            let app = app.clone();
            Ok::<_, ApiError>(views::process(&req.base, state, &app))
        })?;
        Ok(json(StatusCode::OK, &view))
    }

    /// `instances` applies at once; memory and disk at the next start (the
    /// CLI restarts the app for `-m`).
    async fn scale(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        self.app(guid)?;
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
                let app = state
                    .app_mut(&guid)
                    .ok_or_else(|| ApiError::not_found("App"))?;
                if let Some(instances) = instances {
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
        Ok(json(
            StatusCode::ACCEPTED,
            &self
                .store
                .read(|state| views::process(&req.base, state, &app)),
        ))
    }

    async fn set_current_droplet(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        let droplet = body["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("data.guid names the droplet"))?
            .to_string();
        self.app(guid)?;
        let _edit = self.controller.edits.lock().await;
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
    /// and the environment; a stop removes the app from the wicket. The
    /// outcome is in the stats and the log.
    async fn lifecycle(&self, req: &Req, guid: &str, action: &str) -> Reply {
        self.app(guid)?;
        let _edit = self.controller.edits.lock().await;
        let app = self.store.update(|state| {
            let running = if action == "stop" {
                None
            } else {
                Some(snapshot(state, guid, &req.base)?)
            };
            let app = state
                .app_mut(guid)
                .ok_or_else(|| ApiError::not_found("App"))?;
            match running {
                Some(running) => {
                    app.running = Some(running);
                    app.started = true;
                }
                None => app.started = false,
            }
            app.updated_at = now();
            Ok::<_, ApiError>(app.clone())
        })?;
        self.logs.add(
            guid,
            "API",
            format!(
                "Updated app with guid {guid} ({{\"state\"=>\"{}\"}})",
                if app.started { "STARTED" } else { "STOPPED" }
            ),
        );
        let _ = self.controller.sync(guid).await;
        Ok(json(
            StatusCode::OK,
            &self.store.read(|state| views::app(&req.base, state, &app)),
        ))
    }

    // --- packages, builds

    fn create_package(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        if body["type"].as_str() == Some("docker") {
            return Err(ApiError::unprocessable(
                "docker images are not supported: push a directory or a zip holding the component",
            ));
        }
        let app = body["relationships"]["app"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.app names the app"))?;
        self.app(app)?;
        let view = self.store.update(|state| {
            let at = now();
            let package = Package {
                guid: guid(),
                app: app.to_string(),
                state: "AWAITING_UPLOAD".into(),
                sha256: None,
                created_at: at.clone(),
                updated_at: at,
            };
            let view = views::package(&req.base, &package);
            state.packages.push(package);
            view
        });
        Ok(json(StatusCode::CREATED, &view))
    }

    /// The `bits` part of the upload is the zip; the app's older packages go.
    fn upload(&self, req: &Req, guid: &str) -> Reply {
        let package = self
            .store
            .read(|state| state.package(guid).cloned())
            .ok_or_else(|| ApiError::not_found("Package"))?;
        let content_type = req.header(header::CONTENT_TYPE).unwrap_or_default();
        let parts =
            crate::multipart::parts(content_type, &req.body).map_err(ApiError::bad_request)?;
        let bits = parts
            .iter()
            .find(|part| part.name == "bits")
            .ok_or_else(|| ApiError::bad_request("the upload has no 'bits' part"))?;
        self.store.write_package(guid, bits.body)?;
        let sha256: String = {
            use sha2::Digest as _;
            sha2::Sha256::digest(bits.body)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        };
        let (view, blobs) = self.store.update(|state| {
            let blobs = state.keep_newest_package(&package.app);
            let package = state
                .packages
                .iter_mut()
                .find(|package| package.guid == guid)
                .expect("the newest");
            package.state = "READY".into();
            package.sha256 = Some(sha256);
            package.updated_at = now();
            (views::package(&req.base, package), blobs)
        });
        self.store.delete(&blobs);
        self.logs.add(
            &package.app,
            "API",
            format!("Uploaded a package of {} bytes", bits.body.len()),
        );
        Ok(json(StatusCode::OK, &view))
    }

    /// Stages the package at once: the droplet is the component picked.
    fn create_build(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let package_guid = body["package"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("package.guid names the package"))?;
        let package = self
            .store
            .read(|state| state.package(package_guid).cloned())
            .ok_or_else(|| ApiError::unprocessable("Unable to use package. Ensure that the package exists and you have access to it."))?;
        let app = self.app(&package.app)?;
        if package.state != "READY" {
            return Err(ApiError::unprocessable(
                "Only packages with state READY can be staged.",
            ));
        }
        self.logs.add(&app.guid, "STG", "Staging with the wasm lifecycle: the web process command names the component, else the package's only .wasm");
        let staged = std::fs::read(self.store.package_path(package_guid))
            .map_err(|e| format!("the package is gone: {e}"))
            .and_then(|zip| staging::stage(&zip, app.process.command.as_deref()));
        let at = now();
        let mut build = Build {
            guid: guid(),
            app: app.guid.clone(),
            package: package_guid.to_string(),
            state: "STAGED".into(),
            error: None,
            droplet: None,
            created_at: at.clone(),
            updated_at: at.clone(),
        };
        match staged {
            Ok(staged) => {
                self.store.write_droplet(&staged.sha256, &staged.bytes)?;
                let droplet = Droplet {
                    guid: guid(),
                    app: app.guid.clone(),
                    package: package_guid.to_string(),
                    sha256: staged.sha256.clone(),
                    file: staged.entry.clone(),
                    bytes: staged.bytes.len() as u64,
                    created_at: at.clone(),
                    updated_at: at,
                };
                self.logs.add(
                    &app.guid,
                    "STG",
                    format!(
                        "Staged {} ({} bytes, sha256 {})",
                        staged.entry,
                        staged.bytes.len(),
                        &staged.sha256[..12]
                    ),
                );
                build.droplet = Some(droplet.guid.clone());
                self.store.update(|state| state.droplets.push(droplet));
            }
            Err(e) => {
                self.logs
                    .add(&app.guid, "STG", format!("Staging failed: {e}"));
                build.state = "FAILED".into();
                build.error = Some(format!("StagingError - {e}"));
            }
        }
        let view = self.store.update(|state| {
            let view = views::build(&req.base, state, &build);
            state.builds.push(build);
            if state.builds.len() > 1000 {
                state.builds.remove(0);
            }
            view
        });
        Ok(json(StatusCode::CREATED, &view))
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
        let view = self.store.update(|state| {
            if state.domain(&domain).is_none() {
                return Err(ApiError::unprocessable(
                    "Invalid domain. Ensure that the domain exists and you have access to it.",
                ));
            }
            if state.route_at(&host, &domain).is_some() {
                return Err(ApiError::unprocessable("Route already exists."));
            }
            let route = state.create_route(&host, &domain).clone();
            Ok(views::route(&req.base, state, &route))
        })?;
        Ok(json(StatusCode::CREATED, &view))
    }

    async fn delete_route(&self, req: &Req, guid: &str) -> Reply {
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
        self.job(req, "route.delete", Vec::new(), None)
    }

    /// Maps the destinations' app (one: a route serves one app) at once;
    /// `replace` (a PATCH) unmaps the app mapped before.
    async fn map_route(&self, req: &Req, guid: &str, replace: bool) -> Reply {
        let body: Value = req.json()?;
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
            self.app(app)
                .map_err(|_| ApiError::unprocessable(format!("App with guid '{app}' not found")))?;
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

    async fn unmap_route(&self, _req: &Req, guid: &str, destination: &str) -> Reply {
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

    /// A rolling deployment: the app is put with the droplet (the current
    /// one, or the one given, made current), and the wicket swaps it in
    /// place. Finalized at once; the outcome is in the stats and the log.
    async fn create_deployment(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let app = body["relationships"]["app"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.app names the app"))?
            .to_string();
        self.app(&app)?;
        let strategy = body["strategy"].as_str().unwrap_or("rolling").to_string();
        if strategy != "rolling" {
            return Err(ApiError::unprocessable(format!(
                "the {strategy} strategy is not supported: rolling swaps the app in place"
            )));
        }
        let given = body["droplet"]["guid"].as_str().map(String::from);
        let _edit = self.controller.edits.lock().await;
        let (deployment, blobs) = self.store.update(|state| {
            let previous = state.app(&app).and_then(|app| app.current_droplet.clone());
            let mut blobs = Vec::new();
            if let Some(droplet) = &given {
                match state.droplet(droplet) {
                    Some(found) if found.app == app => {}
                    _ => return Err(ApiError::unprocessable("Unable to use droplet. Ensure the droplet exists and belongs to this app.")),
                }
                blobs = state.set_current_droplet(&app, droplet);
            }
            let running = snapshot(state, &app, &req.base)?;
            let droplet = running.droplet.clone();
            let kept = state.app_mut(&app).expect("found above");
            kept.running = Some(running);
            kept.started = true;
            kept.updated_at = now();
            let at = now();
            let deployment = Deployment {
                guid: guid(),
                app: app.clone(),
                previous_droplet: previous.unwrap_or_else(|| droplet.clone()),
                droplet,
                strategy,
                created_at: at.clone(),
                updated_at: at,
            };
            state.deployments.push(deployment.clone());
            if state.deployments.len() > 1000 {
                state.deployments.remove(0);
            }
            Ok::<_, ApiError>((deployment, blobs))
        })?;
        self.store.delete(&blobs);
        self.logs.add(
            &app,
            "API",
            format!(
                "Created a rolling deployment with droplet {}",
                deployment.droplet
            ),
        );
        let _ = self.controller.sync(&app).await;
        Ok(json(
            StatusCode::CREATED,
            &views::deployment(&req.base, &deployment),
        ))
    }
}

/// The snapshot a start takes: the current droplet, the memory, the
/// annotations, what the service bindings grant, and the guest's
/// environment (`VCAP_APPLICATION` naming the API at `base`) with a tag of
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
    manifest::check_annotations(&app.annotations)?;
    let annotation = |key: &str| {
        app.annotations
            .get(key)
            .and_then(|value| value.parse::<u64>().ok())
    };
    let (wasi, keyvalue) = services::grants(state, guid);
    Ok(Running {
        droplet: droplet.guid.clone(),
        sha256: droplet.sha256.clone(),
        file: droplet
            .file
            .rsplit('/')
            .next()
            .unwrap_or(&droplet.file)
            .to_string(),
        memory_mib: app.process.memory_mb,
        budget_ms: annotation(BUDGET_ANNOTATION),
        instances: annotation(INSTANCES_ANNOTATION),
        wasi,
        keyvalue,
        env: env::guest(base, state, app),
        tag: Some(env::tag()),
        since: epoch_secs(),
    })
}

/// Merges `metadata` (labels and annotations; a null value removes the
/// key) into the app, as CC does.
fn apply_metadata(app: &mut crate::store::App, metadata: &Value) -> Result<(), ApiError> {
    for (what, map) in [
        ("labels", &mut app.labels),
        ("annotations", &mut app.annotations),
    ] {
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
    manifest::check_annotations(&app.annotations)?;
    Ok(())
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

/// A page of the browser flow: `title` and the `body` fragment.
fn page(status: StatusCode, title: &str, body: String) -> Response {
    text(
        status,
        "text/html; charset=utf-8",
        format!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>{title}</title><style>body{{font-family:system-ui,sans-serif;max-width:40em;margin:3em auto;padding:0 1em;line-height:1.5}}code{{background:#f2f2f2;padding:.1em .3em;border-radius:.2em}}code.big{{font-size:2em;letter-spacing:.1em;display:inline-block;padding:.3em .6em;user-select:all}}</style></head><body><h1>{title}</h1>{body}</body></html>"
        ),
    )
}

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
