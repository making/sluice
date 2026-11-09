//! Local mode: the routes served over plain TCP (`--listen`), with or
//! without a sluice server. Every connection is served as a tunnel one is
//! (`wasm_host::serve_http`: http/1.1 or h2c sniffed, the same handlers,
//! sandbox, pools and budgets), but each request finds its route by its
//! host: the route of that host, or the sole route whatever the host
//! (`sluice_wicket_tunnel::routes::Table::sole`). An app held until its owner
//! confirms it is answered 503, as it is advertised to no server.
//!
//! A client's leaving is watched here as the server does for a tunnel
//! connection ([`client_gone`]): hyper's `half_close` does not read between a
//! request's end and its response's end.
//!
//! Nor does a client hold a connection by going quiet (`.kb/connection-limits.md`):
//! it has [`QUIET`] to go on with a request's head or the next one's
//! (`wasm_host::serve_http`), or to take a write of an answer ([`Unread`]). h2 has
//! neither on its streams, so an h2 connection goes by what its requests wait on
//! ([`h2_quiet`]). A request's body is left to its route: a guest reads it or not
//! as it likes, within `budget-ms`.
//!
//! All the slots taken, a connection with no request in progress gives way
//! ([`serve`]), so no number of connections left open keeps out the next.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use sluice_wicket_server::accept::{self, Slot};
use sluice_wicket_server::http::{QUIET, Unread, Writing};
use sluice_wicket_tunnel::listener::Replay;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::routes::Routes;
use crate::wasm_host::{self, Accepted, H2Requests, Handler, Instances};

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
    accept::run(
        listener,
        max_connections,
        "--listen",
        |stream, peer, slot| {
            let (routes, instances) = (routes.clone(), instances.clone());
            tokio::spawn(async move {
                let stream = Arc::new(stream);
                let unflushed = Arc::new(AtomicBool::new(false));
                let (head_owed, owed) = watch::channel(false);
                let io = HyperIo {
                    stream: stream.clone(),
                    unflushed: unflushed.clone(),
                };
                tokio::select! {
                    () = serve(io, &routes, &instances, peer, head_owed, &slot) => {}
                    // dropping hyper drops the request in flight
                    () = client_gone(&stream, owed, &unflushed) => {}
                }
            });
        },
    )
    .await
}

/// Serves a connection, http/1.1 or h2c sniffed, until it ended, its client left
/// it waiting, or another connection wanted `slot`: only with no request in
/// progress, as every request counts as admitted with no gate before it. An h2
/// client wanted so is told by a GOAWAY, its going not waited for: the slot is
/// free at once, whoever the client is.
async fn serve<S>(
    stream: S,
    routes: &Arc<Routes>,
    instances: &Instances,
    peer: SocketAddr,
    head_owed: watch::Sender<bool>,
    slot: &Slot,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let claim = slot.claim();
    claim.yields();
    let writing = Writing::new(claim.clone());
    let sniffed = tokio::select! {
        sniffed = sniff(Unread::writing(stream, writing.clone())) => sniffed,
        () = slot.asked() => None,
    };
    let Some((io, h2)) = sniffed else {
        return;
    };
    let routes = routes.clone();
    let route = move |request: &hyper::Request<hyper::body::Incoming>| route(&routes, request);
    let label = format!("listener conn {peer}");
    // no reload ends it, changing the routes request by request
    let gone = CancellationToken::new();
    let (h2_requests, requests) = watch::channel(H2Requests::default());
    let accepted = Some(Accepted {
        peer,
        head_owed,
        h2_requests,
        claim: claim.clone(),
        writing,
    });
    let shutdown = gone.clone();
    let quiet = async {
        if h2 {
            h2_quiet(requests, &shutdown).await;
        } else {
            std::future::pending().await
        }
    };
    let asked = async {
        slot.asked().await;
        if h2 {
            shutdown.cancel();
            // hyper's next poll, before this one's end, writes the GOAWAY
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        biased;
        () = wasm_host::serve_http(io, h2, instances, route, gone, &label, accepted) => {}
        () = quiet => {}
        () = asked => {}
    }
}

/// The handler of `request`'s host (`wasm_host::request_host`): Err with the status and why of
/// an unknown host (404) and of an app held until its owner confirms it (503).
fn route(
    routes: &Routes,
    request: &hyper::Request<hyper::body::Incoming>,
) -> Result<Arc<Handler>, (u16, String)> {
    let host = wasm_host::request_host(request);
    let table = routes.current();
    if let Some((name, owner)) = table.held_at(&host) {
        let held = format!("the app '{name}' of {owner} is held until its owner confirms it");
        return Err((503, held));
    }
    table
        .by_host(&host)
        .ok_or_else(|| (404, format!("no wasm route for host '{host}'")))
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

/// Resolves once an h2 client left its connection waiting on it alone: first
/// `shutdown` is cancelled, the client told to go (GOAWAY) when for [`QUIET`] no
/// request of it awaited its guest and none moved on, which an idle connection
/// ends with; a client not going ends the connection after as long again. Its
/// PINGs count for nothing, and a guest's time is bounded by its budget.
async fn h2_quiet(mut requests: watch::Receiver<H2Requests>, shutdown: &CancellationToken) {
    let mut quiet = async || {
        loop {
            // failing only once hyper is done, which ends the caller
            let _ = requests.wait_for(|requests| requests.on_guest == 0).await;
            match tokio::time::timeout(QUIET, requests.changed()).await {
                Err(_) => return,
                Ok(Ok(())) => {}
                Ok(Err(_)) => std::future::pending().await,
            }
        }
    };
    quiet().await;
    shutdown.cancel();
    quiet().await;
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

/// Reads until the h2 preface's `PRI` is seen or ruled out; true for h2. The
/// bytes read are read again. None once that took its client longer than
/// [`QUIET`].
async fn sniff<S: AsyncRead + Unpin>(stream: S) -> Option<(Replay<S>, bool)> {
    const H2: &[u8] = b"PRI";
    let deadline = tokio::time::Instant::now() + QUIET;
    let mut io = Replay::new(stream);
    while io.read_so_far().len() < H2.len() && H2.starts_with(io.read_so_far()) {
        let wanted = H2.len() - io.read_so_far().len();
        match tokio::time::timeout_at(deadline, io.fill(wanted)).await {
            Ok(Ok(read)) if read > 0 => {}
            Ok(_) => break,
            Err(_) => return None,
        }
    }
    let h2 = io.read_so_far() == H2;
    Some((io, h2))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sluice_wicket_server::accept::Open;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
    use tokio::task::JoinHandle;
    use tokio::time::{Instant, sleep, timeout};

    use super::*;
    use crate::wasm_host::tests::{hello, route};
    use crate::wasm_host::{Handler, Limits, Sandbox};

    /// How far from a bound the connection is taken to end.
    const NEAR: Duration = Duration::from_secs(1);
    const ROOMY: usize = 64 * 1024;
    const LOCALHOST: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    const PREFACE: &str = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";

    /// A connection of `buffer` bytes each way to the routes of `handlers`, by
    /// host: its client, and its service, which ends with it.
    fn connection(
        buffer: usize,
        handlers: &HashMap<String, Arc<Handler>>,
    ) -> (DuplexStream, JoinHandle<()>) {
        connection_of(&Open::new(1), buffer, handlers)
    }

    /// As [`connection`], its slot one of `open`'s.
    fn connection_of(
        open: &Arc<Open>,
        buffer: usize,
        handlers: &HashMap<String, Arc<Handler>>,
    ) -> (DuplexStream, JoinHandle<()>) {
        let (client, server) = tokio::io::duplex(buffer);
        let routes = crate::routes::by_host(handlers.clone());
        let open = open.clone();
        let served = tokio::spawn(async move {
            let slot = open.place(LOCALHOST).await;
            let instances = Instances::new(Instances::MAX);
            let peer = "127.0.0.1:1".parse().unwrap();
            let (head_owed, _) = watch::channel(false);
            serve(server, &routes, &instances, peer, head_owed, &slot).await;
        });
        (client, served)
    }

    /// `served` ends after `bound`, and not before.
    async fn ends(served: &mut JoinHandle<()>, bound: Duration, what: &str) {
        let early = timeout(bound - NEAR, &mut *served).await.is_ok();
        assert!(!early, "ended early: {what}");
        timeout(NEAR * 2, &mut *served)
            .await
            .unwrap_or_else(|_| panic!("still open: {what}"))
            .unwrap();
    }

    fn get(host: &str, path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: {host}\r\n\r\n")
    }

    /// What is sent on `client` up to the text `until`.
    async fn read_to(client: &mut DuplexStream, until: &str) -> String {
        let mut read = Vec::new();
        loop {
            let text = String::from_utf8_lossy(&read);
            if text.contains(until) {
                return text.to_string();
            }
            let text = text.to_string();
            assert_ne!(client.read_buf(&mut read).await.unwrap(), 0, "cut: {text}");
        }
    }

    /// A client that stopped at any point before a guest's answer loses its
    /// connection after the bound, as long again for an h2 one not going when
    /// told to; one going on within the bound, however late, is served.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_client_loses_its_connection() {
        let none = HashMap::new();
        let request = get("nowhere", "/");
        for (buffer, sent, bound) in [
            (ROOMY, "", QUIET),
            (ROOMY, "P", QUIET),
            (ROOMY, "GET / HTTP/1.1\r\nhost", QUIET),
            // its answer waits to be read
            (ROOMY, request.as_str(), QUIET),
            (16, request.as_str(), QUIET),
            (ROOMY, "PRI", QUIET * 2),
            (ROOMY, PREFACE, QUIET * 2),
        ] {
            let (mut client, mut served) = connection(buffer, &none);
            let (_reading, mut writing) = tokio::io::split(&mut client);
            writing.write_all(sent.as_bytes()).await.unwrap();
            ends(&mut served, bound, &format!("{buffer}: {sent:?}")).await;
        }

        let (mut client, served) = connection(ROOMY, &none);
        for _ in 0..3 {
            sleep(QUIET - NEAR).await;
            client.write_all(request.as_bytes()).await.unwrap();
            read_to(&mut client, "no wasm route").await;
        }
        assert!(!served.is_finished());

        // a body no one reads goes with its answer
        let (mut client, mut served) = connection(ROOMY, &none);
        let upload = "POST / HTTP/1.1\r\nhost: nowhere\r\ncontent-length: 5\r\n\r\nab";
        client.write_all(upload.as_bytes()).await.unwrap();
        timeout(NEAR, &mut served).await.unwrap().unwrap();
        read_to(&mut client, "no wasm route").await;
    }

    /// An idle h2 connection is told to go after the bound and ends then, its
    /// client's pings counting for nothing; one whose client grants its answer
    /// no window ends after as long again, the pings answered all along.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_h2_connection_ends() {
        let none = HashMap::new();
        let request = || http::Request::get("http://nowhere/").body(()).unwrap();

        let (stream, mut served) = connection(ROOMY, &none);
        let (client, mut idle) = h2::client::handshake(stream).await.unwrap();
        let mut pings = idle.ping_pong().unwrap();
        let idle = tokio::spawn(idle);
        let mut client = client.ready().await.unwrap();
        let (response, _) = client.send_request(request(), true).unwrap();
        let mut body = response.await.unwrap().into_body();
        while let Some(chunk) = body.data().await {
            let _ = body.flow_control().release_capacity(chunk.unwrap().len());
        }
        tokio::spawn(async move {
            while pings.ping(h2::Ping::opaque()).await.is_ok() {
                sleep(NEAR).await;
            }
        });
        ends(&mut served, QUIET, "idle").await;
        idle.await.unwrap().expect("a clean end");

        let (stream, mut served) = connection(ROOMY, &none);
        let (client, unread) = h2::client::Builder::new()
            .initial_window_size(0)
            .handshake::<_, bytes::Bytes>(stream)
            .await
            .unwrap();
        tokio::spawn(unread);
        let mut client = client.ready().await.unwrap();
        let (response, _) = client.send_request(request(), true).unwrap();
        let response = response.await.unwrap();
        assert_eq!(response.status(), 404);
        ends(&mut served, QUIET * 2, "no window").await;
    }

    /// A guest's time is no client's: a request it holds stays past any bound,
    /// and its connection goes on, on http/1.1 and h2, as does an answer read
    /// slowly. A request its client does not finish ends at its route's budget.
    #[tokio::test]
    async fn a_guests_time_is_bounded_by_its_budget() {
        let loaded = hello().await;
        let budgeted = |budget| {
            let limits = Limits {
                budget,
                ..Limits::default()
            };
            route(
                loaded.clone(),
                Sandbox {
                    limits,
                    ..Sandbox::default()
                },
            )
        };
        let budget = Duration::from_secs(5);
        let routes = HashMap::from([
            ("slow".to_string(), budgeted(QUIET * 8)),
            ("short".to_string(), budgeted(budget)),
        ]);
        tokio::time::pause();
        let held = format!("/hold/{}", (QUIET * 3).as_millis());

        let (mut client, served) = connection(ROOMY, &routes);
        for path in [held.as_str(), "/"] {
            client
                .write_all(get("slow", path).as_bytes())
                .await
                .unwrap();
            read_to(&mut client, "hello from wasm").await;
        }
        assert!(!served.is_finished());

        let (stream, served) = connection(ROOMY, &routes);
        let (client, h2) = h2::client::handshake(stream).await.unwrap();
        tokio::spawn(h2);
        let mut client = client.ready().await.unwrap();
        let mut send = async |path: &str| {
            let request = http::Request::get(format!("http://slow{path}")).body(());
            let (response, _) = client.send_request(request.unwrap(), true).unwrap();
            response.await.unwrap()
        };
        assert_eq!(send(&held).await.status(), 200);
        let mut body = send("/stream").await.into_body();
        for _ in 0..4 {
            sleep(QUIET - NEAR).await;
            let mut window = 0;
            while window < 64 * 1024 {
                let chunk = body.data().await.expect("more of it").unwrap();
                window += chunk.len();
                let _ = body.flow_control().release_capacity(chunk.len());
            }
        }
        assert!(!served.is_finished());

        let (mut client, mut served) = connection(ROOMY, &routes);
        let upload = "POST / HTTP/1.1\r\nhost: short\r\ncontent-length: 5\r\n\r\nab";
        client.write_all(upload.as_bytes()).await.unwrap();
        let started = Instant::now();
        read_to(&mut client, "HTTP/1.1 504").await;
        assert!(
            started.elapsed() >= budget - NEAR,
            "{:?}",
            started.elapsed()
        );
        timeout(NEAR, &mut served).await.expect("closed").unwrap();
    }

    /// Whatever its client is at, a connection with no request in progress gives way at once to one
    /// wanting its slot, an h2 one told by a GOAWAY, whether its client answers its PING or not. A
    /// request in progress keeps its slot, on http/1.1 until its answer was written, on h2 until its
    /// stream ended.
    #[tokio::test]
    async fn a_connection_gives_way_with_no_request_in_progress() {
        let limits = Limits {
            budget: QUIET * 8,
            ..Limits::default()
        };
        let sandbox = Sandbox {
            limits,
            ..Sandbox::default()
        };
        let routes = HashMap::from([("slow".to_string(), route(hello().await, sandbox))]);
        tokio::time::pause();
        let held = format!("/hold/{}", (QUIET * 3).as_millis());
        let answered = get("nowhere", "/");

        for sent in ["", "P", "GET / HTTP/1.1\r\nhost", "PRI", answered.as_str()] {
            let open = Open::new(1);
            let (mut client, _served) = connection_of(&open, ROOMY, &routes);
            client.write_all(sent.as_bytes()).await.unwrap();
            if sent == answered {
                read_to(&mut client, "no wasm route").await;
            }
            sleep(NEAR).await;
            timeout(NEAR, open.place(LOCALHOST))
                .await
                .unwrap_or_else(|_| panic!("kept after {sent:?}"));
            let mut rest = Vec::new();
            client.read_to_end(&mut rest).await.unwrap();
        }

        let open = Open::new(1);
        let (mut client, _served) = connection_of(&open, ROOMY, &routes);
        client.write_all(PREFACE.as_bytes()).await.unwrap();
        sleep(NEAR).await;
        timeout(NEAR, open.place(LOCALHOST)).await.expect("kept");
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        let mut frames = &rest[..];
        let mut kinds = Vec::new();
        while let Some(head) = frames.get(..9) {
            kinds.push(head[3]);
            frames = &frames[9 + u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize..];
        }
        assert!(kinds.contains(&0x7), "no GOAWAY: {kinds:?}");

        // an answer not read holds its slot until the bound on that
        let open = Open::new(1);
        let (mut client, _served) = connection_of(&open, 16, &routes);
        client.write_all(answered.as_bytes()).await.unwrap();
        sleep(NEAR).await;
        let mut wanted = Box::pin(open.place(LOCALHOST));
        assert!(timeout(QUIET - NEAR * 2, &mut wanted).await.is_err());
        timeout(NEAR * 2, wanted).await.expect("kept");

        let open = Open::new(1);
        let (mut client, _served) = connection_of(&open, ROOMY, &routes);
        client
            .write_all(get("slow", &held).as_bytes())
            .await
            .unwrap();
        sleep(NEAR).await;
        let mut wanted = Box::pin(open.place(LOCALHOST));
        assert!(timeout(QUIET, &mut wanted).await.is_err());
        read_to(&mut client, "hello from wasm").await;
        timeout(NEAR, wanted).await.expect("kept after its answer");

        let open = Open::new(1);
        let (stream, _served) = connection_of(&open, ROOMY, &routes);
        let (h2, connection) = h2::client::handshake(stream).await.unwrap();
        let connection = tokio::spawn(connection);
        let mut h2 = h2.ready().await.unwrap();
        let request = http::Request::get(format!("http://slow{held}")).body(());
        let (response, _) = h2.send_request(request.unwrap(), true).unwrap();
        sleep(NEAR).await;
        let mut wanted = Box::pin(open.place(LOCALHOST));
        assert!(timeout(QUIET, &mut wanted).await.is_err());
        let mut body = response.await.unwrap().into_body();
        while let Some(chunk) = body.data().await {
            let _ = body.flow_control().release_capacity(chunk.unwrap().len());
        }
        timeout(NEAR, wanted).await.expect("kept after its stream");
        // its answer to the PING behind the GOAWAY finds no one: no clean end to expect
        timeout(NEAR, connection)
            .await
            .expect("ended")
            .unwrap()
            .ok();
    }
}
