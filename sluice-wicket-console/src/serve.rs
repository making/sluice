//! The console's server: HTTP/1.1, at most [`MAX_CONNECTIONS`] connections at
//! once, every request through the [`Gate`]. The page's shared files are
//! served here -- its style (`/console.css`), its fonts and the script
//! helpers its own script imports (`/kit.js`) -- and the wicket's page
//! (`/`, `/console.js`) and API by its [`Service`]. A connection's client going
//! quiet gives its slot back (`sluice_wicket_server::http`).

use std::sync::Arc;

use http::request::Parts;
use http::{Method, StatusCode};
use sluice_wicket_server::accept;
use tokio::net::TcpListener;

use crate::gate::Gate;
use crate::http::{Body, Response, asset, drain, error, secure};

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
    pub body: &'a mut Option<Body>,
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
    accept::run(
        listener,
        MAX_CONNECTIONS,
        "--console",
        move |stream, _, slot| {
            let (gate, service) = (gate.clone(), service.clone());
            tokio::spawn(async move {
                let _slot = slot;
                sluice_wicket_server::http::serve(stream, |request| {
                    answer(&gate, &*service, request)
                })
                .await;
            });
        },
    )
    .await
}

async fn answer<S: Service>(gate: &Gate, service: &S, request: hyper::Request<Body>) -> Response {
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

#[cfg(test)]
mod tests {
    use sluice_wicket_server::http::QUIET;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpStream;
    use tokio::time::Instant;

    use super::*;

    struct Page;

    impl Service for Page {
        const INDEX: &'static str = "page";
        const SCRIPT: &'static str = "";
        const DRAIN_LIMIT: u64 = 0;

        async fn handle(&self, _: Request<'_>) -> Response {
            error(StatusCode::NOT_FOUND, "nothing")
        }
    }

    /// Connections left open taking every slot stop no one for longer than the
    /// bound on a quiet client: each is closed, and the next one answered.
    #[tokio::test(start_paused = true)]
    async fn quiet_connections_give_their_slots_back() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let gate = Gate {
            token: None,
            sign_in: None,
            realm: "test",
        };
        tokio::spawn(run(listener, gate, Arc::new(Page)));
        let started = Instant::now();
        let mut quiet = Vec::new();
        for _ in 0..MAX_CONNECTIONS {
            quiet.push(TcpStream::connect(address).await.unwrap());
        }
        let mut next = TcpStream::connect(address).await.unwrap();
        next.write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut status = [0; 12];
        next.read_exact(&mut status).await.unwrap();
        assert_eq!(&status, b"HTTP/1.1 200");
        assert!(started.elapsed() >= QUIET, "{:?}", started.elapsed());
        assert!(started.elapsed() < QUIET * 2, "{:?}", started.elapsed());
        for mut client in quiet {
            assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
        }
    }
}
