//! Local mode: the routes served over plain TCP (`--listen`), with or
//! without a sluice server. Every connection is served as a tunnel one is
//! (`wasm_host::serve_http`: http/1.1 or h2c sniffed, the same handlers,
//! sandbox, pools and budgets), but each request finds its route by its
//! host: the route of that host, or the sole route whatever the host.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::routes::Routes;
use crate::wasm_host::{self, Handler, Instances};

/// Binds `address` (`host:port`).
pub async fn bind(address: &str) -> Result<TcpListener, String> {
    TcpListener::bind(address)
        .await
        .map_err(|e| format!("--listen {address}: {e}"))
}

/// Accepts connections for good, at most `max_connections` open at once (a
/// connection beyond waits in the backlog).
pub async fn run(
    listener: TcpListener,
    routes: Arc<Routes>,
    instances: Arc<Instances>,
    max_connections: usize,
) {
    let open = Arc::new(tokio::sync::Semaphore::new(max_connections));
    loop {
        let permit = open.clone().acquire_owned().await.expect("never closed");
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                // e.g. out of file descriptors: retried, not fatal
                eprintln!("--listen: accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let (routes, instances) = (routes.clone(), instances.clone());
        tokio::spawn(async move {
            let _permit = permit;
            let _ = stream.set_nodelay(true);
            let (io, h2) = Sniffed::read(stream).await;
            let route =
                move |request: &hyper::Request<hyper::body::Incoming>| route(&routes, request);
            let label = format!("listener conn {peer}");
            // never gone: a reload changes the routes request by request
            let gone = CancellationToken::new();
            wasm_host::serve_http(io, h2, &instances, route, gone, &label, Some(peer)).await;
        });
    }
}

/// The handler of `request`'s host (`wasm_host::request_host`).
fn route(
    routes: &Routes,
    request: &hyper::Request<hyper::body::Incoming>,
) -> Result<Arc<Handler>, String> {
    let host = wasm_host::request_host(request);
    routes
        .by_host(&host)
        .ok_or_else(|| format!("no wasm route for host '{host}'"))
}

/// A TCP stream whose first bytes were read to tell h2c from http/1.1, and
/// are read again first.
struct Sniffed {
    prefix: Vec<u8>,
    /// How much of `prefix` was read again.
    replayed: usize,
    stream: TcpStream,
}

impl Sniffed {
    /// Reads until the h2 preface's `PRI` is seen or ruled out, within
    /// `wasm_host::SNIFF_TIMEOUT`; true for h2.
    async fn read(mut stream: TcpStream) -> (Sniffed, bool) {
        const H2: &[u8] = b"PRI";
        let deadline = tokio::time::Instant::now() + wasm_host::SNIFF_TIMEOUT;
        let mut prefix = Vec::with_capacity(H2.len());
        while prefix.len() < H2.len() && H2.starts_with(&prefix) {
            let mut byte = [0u8; 3];
            let wanted = H2.len() - prefix.len();
            match tokio::time::timeout_at(deadline, stream.read(&mut byte[..wanted])).await {
                Ok(Ok(read)) if read > 0 => prefix.extend_from_slice(&byte[..read]),
                _ => break,
            }
        }
        let h2 = prefix == H2;
        let sniffed = Sniffed {
            prefix,
            replayed: 0,
            stream,
        };
        (sniffed, h2)
    }
}

impl AsyncRead for Sniffed {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = &mut *self;
        if this.replayed < this.prefix.len() {
            let rest = &this.prefix[this.replayed..];
            let n = rest.len().min(buf.remaining());
            buf.put_slice(&rest[..n]);
            this.replayed += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Sniffed {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
