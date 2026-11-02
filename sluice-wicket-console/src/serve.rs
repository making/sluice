//! The console's server: HTTP/1.1, at most [`MAX_CONNECTIONS`] connections at
//! once, every request through the [`Gate`]. The page's shared files are
//! served here -- its style (`/console.css`), its fonts and the script
//! helpers its own script imports (`/kit.js`) -- and the wicket's page
//! (`/`, `/console.js`) and API by its [`Service`].

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use http::request::Parts;
use http::{Method, StatusCode};
use hyper::body::Incoming;
use tokio::net::TcpListener;

use crate::gate::Gate;
use crate::http::{Response, asset, drain, error, secure};

/// Connections open at once; one beyond waits in the backlog.
const MAX_CONNECTIONS: usize = 64;

const CSS: &str = include_str!("assets/console.css");
const KIT: &str = include_str!("assets/kit.js");
const OVERPASS: &[u8] = include_bytes!("assets/fonts/overpass.woff2");
const OVERPASS_MONO: &[u8] = include_bytes!("assets/fonts/overpass-mono.woff2");

/// A request the gate admitted.
pub struct Request<'a> {
    pub parts: &'a Parts,
    /// Taken by the service when it reads it; what is left is drained.
    pub body: &'a mut Option<Incoming>,
    /// Who is signed in through the provider.
    pub user: Option<String>,
}

/// A wicket's page and API.
pub trait Service: Send + Sync + 'static {
    /// The page (`/`).
    const INDEX: &'static str;
    /// Its script (`/console.js`), a module importing `/kit.js`.
    const SCRIPT: &'static str;
    /// The most of a body left unread that is drained ([`drain`]): the
    /// largest upload.
    const DRAIN_LIMIT: u64;

    /// Answers an admitted request but the page's files.
    fn handle(&self, request: Request<'_>) -> impl Future<Output = Response> + Send;
}

/// Serves `service` behind `gate` for good.
pub async fn run<S: Service>(listener: TcpListener, gate: Gate, service: Arc<S>) {
    let gate = Arc::new(gate);
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
        let (gate, service) = (gate.clone(), service.clone());
        tokio::spawn(async move {
            let _permit = permit;
            let handler = hyper::service::service_fn(move |request| {
                let (gate, service) = (gate.clone(), service.clone());
                async move { Ok::<_, Infallible>(answer(&gate, &*service, request).await) }
            });
            // a client leaving mid-request is no news
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), handler)
                .await;
        });
    }
}

async fn answer<S: Service>(
    gate: &Gate,
    service: &S,
    request: hyper::Request<Incoming>,
) -> Response {
    let (parts, body) = request.into_parts();
    let mut body = Some(body);
    let mut response = if gate.signs_in(&parts) {
        gate.sign_in_route(&parts).await
    } else {
        match gate.refusal(&parts) {
            Some(refused) => refused,
            None => match (&parts.method, parts.uri.path()) {
                (&Method::GET, "/") => asset("text/html; charset=utf-8", S::INDEX.as_bytes()),
                (&Method::GET, "/console.js") => {
                    asset("text/javascript; charset=utf-8", S::SCRIPT.as_bytes())
                }
                (&Method::GET, "/kit.js") => {
                    asset("text/javascript; charset=utf-8", KIT.as_bytes())
                }
                (&Method::GET, "/console.css") => asset("text/css; charset=utf-8", CSS.as_bytes()),
                (&Method::GET, "/fonts/overpass.woff2") => asset("font/woff2", OVERPASS),
                (&Method::GET, "/fonts/overpass-mono.woff2") => asset("font/woff2", OVERPASS_MONO),
                (_, path) if !path.starts_with("/api/") => {
                    error(StatusCode::NOT_FOUND, format!("nothing at {path}"))
                }
                _ => {
                    let user = gate.session(&parts.headers);
                    service
                        .handle(Request {
                            parts: &parts,
                            body: &mut body,
                            user,
                        })
                        .await
                }
            },
        }
    };
    if let Some(body) = body {
        drain(&parts, body, S::DRAIN_LIMIT).await;
    }
    secure(&mut response);
    response
}
