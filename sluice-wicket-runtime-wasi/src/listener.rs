//! Local mode: the routes served over plain TCP (`--listen`), with or
//! without a sluice server. Every connection is served as a tunnel one is
//! (`wasm_host::serve_http`: http/1.1 or h2c sniffed, the same handlers,
//! sandbox, pools and budgets), but each request finds its route by its
//! host: the route of that host, or the sole route whatever the host
//! (`sluice_wicket_tunnel::routes::Table::sole`).
//!
//! A client's leaving is watched here as the server does for a tunnel
//! connection ([`client_gone`]): hyper's `half_close` does not read between a
//! request's end and its response's end.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use sluice_wicket_server::accept;
use sluice_wicket_tunnel::listener::Replay;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::routes::Routes;
use crate::wasm_host::{self, Accepted, Handler, Instances};

pub use sluice_wicket_tunnel::listener::bind;

/// What a probe writes: an interim response, which a client must take before
/// its response (RFC 9110 15.2).
const PROBE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// How long a request's response head is awaited between probes, as the
/// server's `sluice.limits.half-close-probe-interval` by default.
const PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// The probes a response may be preceded by: clients fail on too many 1xx
/// (`.kb/connection-limits.md`).
const MAX_PROBES: usize = 4;

/// Accepts connections for good, at most `max_connections` open at once (see
/// `sluice_wicket_server::accept::run`).
pub async fn run(
    listener: TcpListener,
    routes: Arc<Routes>,
    instances: Arc<Instances>,
    max_connections: usize,
) {
    accept::run(listener, max_connections, "--listen", |stream, peer, slot| {
        let (routes, instances) = (routes.clone(), instances.clone());
        tokio::spawn(async move {
            let _slot = slot;
            let stream = Arc::new(stream);
            let unflushed = Arc::new(AtomicBool::new(false));
            let (io, h2) = sniff(HyperIo {
                stream: stream.clone(),
                unflushed: unflushed.clone(),
            })
            .await;
            let route =
                move |request: &hyper::Request<hyper::body::Incoming>| route(&routes, request);
            let label = format!("listener conn {peer}");
            // never gone: a reload changes the routes request by request
            let gone = CancellationToken::new();
            let (head_owed, owed) = watch::channel(false);
            let accepted = Some(Accepted { peer, head_owed });
            tokio::select! {
                () = wasm_host::serve_http(io, h2, &instances, route, gone, &label, accepted) => {}
                // dropping hyper drops the request in flight
                () = client_gone(&stream, owed, &unflushed) => {}
            }
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

/// Resolves once the client of a connection is known to be gone while hyper
/// may not be reading it: an error on the socket (the client's reset, a probe
/// answered with one), or a probe not written whole. A client's FIN alone is
/// no departure, it may await its response half-closed (RFC 9112 9.6), and only
/// a write tells: so a client that ended its side while a request `owed` its
/// response's head, with nothing of hyper's on its way, gets a [`PROBE`] after
/// each [`PROBE_INTERVAL`], at most [`MAX_PROBES`]. A response begun takes
/// nothing more: until its next write fails, its budget holds it.
///
/// Polled with hyper, never at once with it: a probe does not land within its
/// writes.
async fn client_gone(stream: &TcpStream, mut owed: watch::Receiver<bool>, unflushed: &AtomicBool) {
    let probing = async {
        // `wait_for` fails only once hyper is done, which ends the caller
        while owed.wait_for(|owed| *owed).await.is_ok() {
            let mut probes = 0;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(PROBE_INTERVAL) => {}
                    _ = owed.wait_for(|owed| !*owed) => break,
                }
                if probes < MAX_PROBES && !unflushed.load(Ordering::Relaxed) && read_closed(stream)
                {
                    match stream.try_write(PROBE) {
                        Ok(written) if written == PROBE.len() => probes += 1,
                        // nothing written: another time
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        // part of one, which no bytes of hyper's may follow, or
                        // a failed write
                        _ => return,
                    }
                }
            }
        }
        std::future::pending().await
    };
    tokio::select! {
        _ = stream.ready(Interest::ERROR) => {}
        () = probing => {}
    }
}

/// Whether the client ended its side, whether or not hyper read that far.
fn read_closed(stream: &TcpStream) -> bool {
    futures_util::FutureExt::now_or_never(stream.ready(Interest::READABLE))
        .is_some_and(|ready| ready.is_ok_and(|ready| ready.is_read_closed()))
}

/// A connection's socket as hyper has it, [`client_gone`] having it too.
struct HyperIo {
    stream: Arc<TcpStream>,
    /// True from a write until its flush: bytes of hyper's may be on their way.
    unflushed: Arc<AtomicBool>,
}

impl AsyncRead for HyperIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            ready!(self.stream.poll_read_ready(cx))?;
            match self.stream.try_read(buf.initialize_unfilled()) {
                Ok(read) => {
                    buf.advance(read);
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncWrite for HyperIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.unflushed.store(true, Ordering::Relaxed);
        loop {
            ready!(self.stream.poll_write_ready(cx))?;
            match self.stream.try_write(buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                written => return Poll::Ready(written),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.unflushed.store(false, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }

    /// Nothing: hyper is done with the connection then, and the socket closes
    /// with it.
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Reads until the h2 preface's `PRI` is seen or ruled out, within
/// `wasm_host::SNIFF_TIMEOUT`; true for h2. The bytes read are read again.
async fn sniff(stream: HyperIo) -> (Replay<HyperIo>, bool) {
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
