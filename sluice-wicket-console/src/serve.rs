//! The console's server: HTTP/1.1, at most [`MAX_CONNECTIONS`] connections at
//! once, every request through the [`Gate`]. The page's shared files are
//! served here -- its style (`/console.css`), its fonts and the script
//! helpers its own script imports (`/kit.js`) -- and the wicket's page
//! (`/`, `/console.js`) and API by its [`Service`]. A connection's client going
//! quiet gives its slot back, and all taken, one without a request the gate
//! admitted gives way (`sluice_wicket_server::http`).

use std::sync::Arc;

use http::request::Parts;
use http::{Method, StatusCode};
use sluice_wicket_server::accept;
use tokio::net::TcpListener;

use crate::gate::Gate;
use crate::http::{Body, Response, asset, drain, error, secure};

/// Connections open at once; one beyond takes the place of one without a request
/// admitted, or waits in the backlog for one.
const MAX_CONNECTIONS: usize = 64;

/// The pages' style, its fonts in `fonts/` next to it: sluice-wicket-cf-api's pages wear it too.
pub const CSS: &str = include_str!("assets/console.css");
const KIT: &str = include_str!("assets/kit.js");
pub const OVERPASS: &[u8] = include_bytes!("assets/fonts/overpass.woff2");
pub const OVERPASS_MONO: &[u8] = include_bytes!("assets/fonts/overpass-mono.woff2");

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
                sluice_wicket_server::http::serve(stream, slot, |request| {
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
    let refusal = (!gate.signs_in(&parts)).then(|| gate.refusal(&parts));
    if let Some(None) = refusal {
        body.admit();
    }
    let mut body = Some(body);
    let mut response = match refusal {
        None => gate.sign_in_route(&parts).await,
        Some(Some(refused)) => refused,
        Some(None) => match (&parts.method, parts.uri.path()) {
            (&Method::GET, "/") => asset("text/html; charset=utf-8", S::INDEX.as_bytes()),
            (&Method::GET, "/console.js") => {
                asset("text/javascript; charset=utf-8", S::SCRIPT.as_bytes())
            }
            (&Method::GET, "/kit.js") => asset("text/javascript; charset=utf-8", KIT.as_bytes()),
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
    use tokio::sync::Notify;
    use tokio::time::{Duration, Instant, sleep};

    use super::*;

    /// How long the [`Page`]'s API takes to answer: past [`QUIET`].
    const HOLD: Duration = Duration::from_secs(90);

    /// Its API tells when a request reached it.
    #[derive(Default)]
    struct Page {
        reached: Notify,
    }

    impl Service for Page {
        const INDEX: &'static str = "page";
        const SCRIPT: &'static str = "";
        const DRAIN_LIMIT: u64 = 1 << 20;

        async fn handle(&self, _: Request<'_>) -> Response {
            self.reached.notify_one();
            sleep(HOLD).await;
            error(StatusCode::NOT_FOUND, "nothing")
        }
    }

    /// Keeps the paused clock still until dropped: a blocking task running stops its
    /// auto-advance, which otherwise leaps to the next timer whenever the runtime waits on a
    /// socket, past [`QUIET`] before the sockets' bytes arrive.
    fn hold_clock() -> std::sync::mpsc::Sender<()> {
        let (release, released) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || released.recv());
        release
    }

    /// Connections left open taking every slot stop no one: the next one is
    /// answered at once, the one waiting longest gone, here a request the gate
    /// refused, its body unfinished. A request it admitted stays, and the others
    /// until the bound on a quiet client.
    #[tokio::test(start_paused = true)]
    async fn connections_give_way_unless_admitted() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let gate = Gate {
            token: Some("token".to_string()),
            sign_in: None,
            realm: "test",
        };
        let page = Arc::new(Page::default());
        tokio::spawn(run(listener, gate, page.clone()));
        let held = hold_clock();
        let started = Instant::now();
        let mut refused = TcpStream::connect(address).await.unwrap();
        refused
            .write_all(b"POST /api/x HTTP/1.1\r\nhost: t\r\ncontent-length: 5\r\n\r\nab")
            .await
            .unwrap();
        let mut admitted = TcpStream::connect(address).await.unwrap();
        admitted
            .write_all(b"GET /api/hold HTTP/1.1\r\nhost: t\r\nauthorization: Bearer token\r\n\r\n")
            .await
            .unwrap();
        // both reach the gate, refused's sent first, and are no longer fresh
        page.reached.notified().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let mut quiet = Vec::new();
        for _ in 2..MAX_CONNECTIONS {
            quiet.push(TcpStream::connect(address).await.unwrap());
        }

        let mut next = TcpStream::connect(address).await.unwrap();
        next.write_all(b"GET / HTTP/1.1\r\nhost: t\r\nauthorization: Bearer token\r\n\r\n")
            .await
            .unwrap();
        let mut status = [0; 12];
        next.read_exact(&mut status).await.unwrap();
        assert_eq!(&status, b"HTTP/1.1 200");
        assert_eq!(refused.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(started.elapsed() < QUIET, "{:?}", started.elapsed());
        drop(held);

        for mut client in quiet {
            assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(started.elapsed() >= QUIET, "{:?}", started.elapsed());
        }
        admitted.read_exact(&mut status).await.unwrap();
        assert_eq!(&status, b"HTTP/1.1 404");
        assert!(started.elapsed() >= HOLD, "{:?}", started.elapsed());
    }
}
