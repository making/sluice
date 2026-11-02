//! Local mode: the routes served over plain TCP (`--listen`), with or
//! without a sluice server. Every connection is served as a tunnel one is
//! (`wasm_host::serve_http`: http/1.1 or h2c sniffed, the same handlers,
//! sandbox, pools and budgets), but each request finds its route by its
//! host: the route of that host, or the sole route whatever the host.

use std::sync::Arc;

use sluice_wicket_tunnel::listener::{self, Replay};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::routes::Routes;
use crate::wasm_host::{self, Handler, Instances};

pub use sluice_wicket_tunnel::listener::bind;

/// Accepts connections for good, at most `max_connections` open at once (see
/// `sluice_wicket_tunnel::listener::run`).
pub async fn run(
    listener: TcpListener,
    routes: Arc<Routes>,
    instances: Arc<Instances>,
    max_connections: usize,
) {
    listener::run(listener, max_connections, |stream, peer, slot| {
        let (routes, instances) = (routes.clone(), instances.clone());
        tokio::spawn(async move {
            let _slot = slot;
            let (io, h2) = sniff(stream).await;
            let route =
                move |request: &hyper::Request<hyper::body::Incoming>| route(&routes, request);
            let label = format!("listener conn {peer}");
            // never gone: a reload changes the routes request by request
            let gone = CancellationToken::new();
            wasm_host::serve_http(io, h2, &instances, route, gone, &label, Some(peer)).await;
        });
    })
    .await
}

/// The handler of `request`'s host (`wasm_host::request_host`).
fn route(
    routes: &Routes,
    request: &hyper::Request<hyper::body::Incoming>,
) -> Result<Arc<Handler>, String> {
    let host = wasm_host::request_host(request);
    routes
        .current()
        .by_host(&host)
        .ok_or_else(|| format!("no wasm route for host '{host}'"))
}

/// Reads until the h2 preface's `PRI` is seen or ruled out, within
/// `wasm_host::SNIFF_TIMEOUT`; true for h2. The bytes read are read again.
async fn sniff(stream: TcpStream) -> (Replay<TcpStream>, bool) {
    const H2: &[u8] = b"PRI";
    let deadline = tokio::time::Instant::now() + wasm_host::SNIFF_TIMEOUT;
    let mut io = Replay::new(stream);
    while io.read_so_far().len() < H2.len() && H2.starts_with(io.read_so_far()) {
        let wanted = H2.len() - io.read_so_far().len();
        match tokio::time::timeout_at(deadline, io.fill(wanted)).await {
            Ok(Ok(read)) if read > 0 => {}
            _ => break,
        }
    }
    let h2 = io.read_so_far() == H2;
    (io, h2)
}
