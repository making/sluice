//! Hosting of wasi:http 0.3 handler components for `wasm:` upstream routes.
//!
//! The tunnel connection's byte stream is served by hyper (http/1.1 vs h2c
//! sniffed from the first bytes); every request is handled by a fresh
//! instance of the guest component through `wasmtime-wasi-http`'s p3
//! `Service.handle`. Response bodies stream: the store task relays frames to
//! hyper through a bounded channel, so a slow client backpressures the guest
//! instead of buffering the response.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};

use http_body_util::BodyExt as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;

use crate::Ctrl;
use crate::pb::frame::Body;
use crate::pb::{Close, Data, Frame};

/// Upper bound for a single DATA frame emitted by the hyper write path.
const CHUNK: usize = 16 * 1024;

/// Relay slots between the guest's store task and hyper; together with the
/// pipe buffer inside the body conversion this bounds the in-flight response
/// bytes and applies backpressure to the guest.
const RELAY_BUFFER: usize = 2;

/// Sniff timeout for the first bytes of a connection (h2 preface check).
const SNIFF_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Component locators

/// Where a component comes from: the `wasm:` address form minus the prefix.
/// Bare paths and `file://` read from disk; `http://` / `https://` are
/// fetched once at startup. Further schemes slot in here.
pub enum Locator {
    File(PathBuf),
    Http(http::Uri),
}

impl Locator {
    /// `scheme://...` is a locator, anything else a local component path.
    pub fn parse(spec: &str) -> Result<Locator, String> {
        let spec = spec.trim();
        let Some((scheme, rest)) = spec.split_once("://") else {
            return Ok(Locator::File(PathBuf::from(spec)));
        };
        match scheme {
            "file" => Ok(Locator::File(file_url_path(rest))),
            "http" | "https" => http::Uri::from_str(spec)
                .map(Locator::Http)
                .map_err(|e| format!("bad url '{spec}': {e}")),
            other => Err(format!("unsupported wasm locator scheme '{other}://'")),
        }
    }

    /// The `wasm:` target advertised to the server; doubles as the cache key.
    pub async fn target(&self) -> Result<String, String> {
        match self {
            Locator::File(path) => {
                let abs = path
                    .canonicalize()
                    .map_err(|e| format!("wasm {}: {e}", path.display()))?;
                Ok(format!("wasm:file://{}", abs.display()))
            }
            Locator::Http(uri) => Ok(format!("wasm:{uri}")),
        }
    }

    async fn fetch(&self) -> Result<Vec<u8>, String> {
        match self {
            Locator::File(path) => tokio::fs::read(path)
                .await
                .map_err(|e| format!("read {}: {e}", path.display())),
            Locator::Http(uri) => {
                let response = reqwest::get(uri.to_string())
                    .await
                    .map_err(|e| format!("GET {uri}: {e}"))?
                    .error_for_status()
                    .map_err(|e| format!("GET {uri}: {e}"))?;
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| format!("GET {uri}: {e}"))?;
                Ok(bytes.to_vec())
            }
        }
    }
}

/// `file://<path>` rest -> path; only absolute file URLs are expected, but a
/// single-landing path after an authority is tolerated.
fn file_url_path(rest: &str) -> PathBuf {
    if rest.starts_with('/') {
        PathBuf::from(rest)
    } else {
        match rest.find('/') {
            Some(slash) => PathBuf::from(&rest[slash..]),
            None => PathBuf::from(rest),
        }
    }
}

// ---------------------------------------------------------------------------
// Loaded components

pub struct Loaded {
    engine: wasmtime::Engine,
    component: wasmtime::component::Component,
    linker: wasmtime::component::Linker<Ctx>,
}

/// Loads (or fetches from cache) the component behind `locator`.
pub async fn load(locator: &Locator) -> Result<Arc<Loaded>, String> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Loaded>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = locator.target().await?;
    if let Some(loaded) = cache.lock().unwrap().get(&key) {
        return Ok(loaded.clone());
    }
    let bytes = locator.fetch().await?;
    let mut config = wasmtime::Config::new();
    config.wasm_component_model_async(true);
    let engine = wasmtime::Engine::new(&config).map_err(|e| format!("engine: {e}"))?;
    let component = wasmtime::component::Component::new(&engine, &bytes).map_err(|e| {
        format!(
            "component {}: {e}",
            key.strip_prefix("wasm:").unwrap_or(&key)
        )
    })?;
    let mut linker = wasmtime::component::Linker::<Ctx>::new(&engine);
    // p2 hosts the rust std imports of wasm32-wasip2 components; p3 hosts the
    // wasi 0.3 world the guest is written against.
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| format!("linker: {e}"))?;
    wasmtime_wasi::p3::add_to_linker(&mut linker).map_err(|e| format!("linker: {e}"))?;
    wasmtime_wasi_http::p3::add_to_linker(&mut linker).map_err(|e| format!("linker: {e}"))?;
    let loaded = Arc::new(Loaded {
        engine,
        component,
        linker,
    });
    cache.lock().unwrap().insert(key, loaded.clone());
    Ok(loaded)
}

// ---------------------------------------------------------------------------
// Store state

struct NoHooks;

impl wasmtime_wasi_http::WasiHttpHooks for NoHooks {}

struct Ctx {
    table: wasmtime::component::ResourceTable,
    wasi: wasmtime_wasi::WasiCtx,
    http: wasmtime_wasi_http::WasiHttpCtx,
    hooks: NoHooks,
}

impl Ctx {
    fn new() -> Self {
        Self {
            table: wasmtime::component::ResourceTable::default(),
            wasi: wasmtime_wasi::WasiCtxBuilder::new().build(),
            http: wasmtime_wasi_http::WasiHttpCtx::new(),
            hooks: NoHooks,
        }
    }
}

impl wasmtime_wasi::WasiView for Ctx {
    fn ctx(&mut self) -> wasmtime_wasi::WasiCtxView<'_> {
        wasmtime_wasi::WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl wasmtime_wasi_http::WasiHttpView for Ctx {
    fn http(&mut self) -> wasmtime_wasi_http::WasiHttpCtxView<'_> {
        wasmtime_wasi_http::WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.hooks,
        }
    }
}

// ---------------------------------------------------------------------------
// Request handling

/// One relayed body frame, or the error that truncated the body. The error is
/// boxed: it may come from the body conversion (`wasmtime_wasi_http::Error`)
/// or the store event loop (`wasmtime::Error`).
type RelayError = Box<dyn std::error::Error + Send + Sync>;
type RelayFrame = Result<http_body::Frame<bytes::Bytes>, RelayError>;

/// The body hyper consumes: frames relayed from the guest's store task.
type RelayBody = http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, RelayError>;

/// The response head, or a synthesized failure with its message (the body is
/// that single frame).
enum Head {
    Response(http::response::Parts),
    Failure(u16, String),
}

fn plain_response(status: u16, body: String) -> hyper::Response<RelayBody> {
    let frame = Ok(http_body::Frame::data(bytes::Bytes::from(body)));
    hyper::Response::builder()
        .status(status)
        .body(http_body_util::StreamBody::new(tokio_stream::iter([frame])).boxed_unsync())
        .expect("static response")
}

/// Error response synthesized inside the store task.
async fn failed(head_tx: mpsc::Sender<Head>, status: u16, message: String) {
    let _ = head_tx.send(Head::Failure(status, message)).await;
}

/// Forwards guest body frames to hyper until either side ends. A body error
/// is relayed so hyper sees the truncation instead of a silent short body; a
/// gone client just ends the relay and the guest drains through the pipe.
async fn relay(
    mut body: http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, wasmtime_wasi_http::Error>,
    frame_tx: mpsc::Sender<RelayFrame>,
) {
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if frame_tx.send(Ok(frame)).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = frame_tx.send(Err(e.into())).await;
                return;
            }
        }
    }
}

async fn respond(
    loaded: &Loaded,
    req: hyper::Request<hyper::body::Incoming>,
) -> hyper::Response<RelayBody> {
    let mut store = wasmtime::Store::new(&loaded.engine, Ctx::new());
    let service = match wasmtime_wasi_http::p3::bindings::Service::instantiate_async(
        &mut store,
        &loaded.component,
        &loaded.linker,
    )
    .await
    {
        Ok(service) => service,
        Err(e) => return plain_response(500, format!("wasm instantiate failed: {e}")),
    };
    let (req, pump) = wasmtime_wasi_http::p3::Request::from_http(&mut store.data_mut().hooks, req);

    // The response head is handed to hyper as soon as it is known; body frames
    // are relayed by the store task, which must outlive this future for the
    // guest to keep producing while hyper drains at the client's pace.
    let (head_tx, mut head_rx) = mpsc::channel::<Head>(1);
    let (frame_tx, frame_rx) = mpsc::channel::<RelayFrame>(RELAY_BUFFER);

    // A detached task: it ends when the response is fully relayed (or the
    // client disappeared). A store-level failure (e.g. a guest trap) cancels
    // the closure, so the fallback is synthesized here: a pending head wins,
    // a missing one becomes the 500 below, and an already-streaming body is
    // marked truncated via the error frame.
    tokio::spawn(async move {
        let driven = store.run_concurrent({
            let head = head_tx.clone();
            let frames = frame_tx.clone();
            async move |accessor| {
                let response = async {
                    match service.handle(accessor, req).await {
                        Ok(Ok(response)) => {
                            let http = accessor.with(|store| {
                                response.into_http(store, futures_util::future::ready(Ok(())))
                            });
                            match http {
                                Ok(http) => {
                                    let (parts, body) = http.into_parts();
                                    let _ = head.send(Head::Response(parts)).await;
                                    relay(body, frames).await;
                                }
                                Err(e) => {
                                    failed(head, 500, format!("response conversion failed: {e}"))
                                        .await;
                                }
                            }
                        }
                        Ok(Err(code)) => failed(head, 502, format!("wasi error: {code:?}")).await,
                        Err(e) => failed(head, 500, format!("wasm trap: {e}")).await,
                    }
                };
                let ((), ()) = tokio::join!(response, async {
                    // Drains request-body bookkeeping; errors surface in the response path.
                    let _ = pump.await;
                },);
            }
        });
        if let Err(e) = driven.await {
            eprintln!("[wasm] store ended with error: {e}");
            let _ = head_tx.try_send(Head::Failure(500, format!("wasm handler failed: {e}")));
            let _ = frame_tx.try_send(Err(e.into()));
        }
    });

    match head_rx.recv().await {
        Some(Head::Response(parts)) => hyper::Response::from_parts(
            parts,
            http_body_util::StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(frame_rx))
                .boxed_unsync(),
        ),
        Some(Head::Failure(status, message)) => plain_response(status, message),
        // The task died before any head (panic / abort).
        None => plain_response(500, "wasm handler failed".into()),
    }
}

// ---------------------------------------------------------------------------
// Tunnel <-> hyper byte stream adapter

/// Serves one tunnel connection by feeding its frames through hyper and the
/// guest component; http/1.1 and h2c are auto-detected.
pub async fn serve(
    tx: mpsc::Sender<Frame>,
    registry: crate::Registry,
    conn_id: i64,
    component: Arc<Loaded>,
    ctrl: mpsc::Receiver<Ctrl>,
) {
    let mut io = TunnelIo::new(tx, conn_id, ctrl);

    // Sniff the first bytes: HTTP/2 prior-knowledge starts with the
    // (unencrypted) preface "PRI"; everything else is treated as http/1.1.
    let h2 = {
        let deadline = tokio::time::Instant::now() + SNIFF_TIMEOUT;
        while io.buffered() == 0 {
            match tokio::time::timeout_at(deadline, io.fill()).await {
                Ok(true) => {}
                _ => break,
            }
        }
        io.peek_prefix(b"PRI")
    };

    let service = hyper::service::service_fn(move |req| {
        let component = component.clone();
        async move { Ok::<_, std::convert::Infallible>(respond(&component, req).await) }
    });
    let served = if h2 {
        hyper::server::conn::http2::Builder::new(TokioExecutor)
            .serve_connection(hyper_util::rt::TokioIo::new(io), service)
            .await
    } else {
        hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(io), service)
            .await
    };
    if let Err(e) = served {
        eprintln!("[wasm] conn {conn_id} serve error: {e}");
    }
    registry.lock().unwrap().remove(&conn_id);
}

#[derive(Clone)]
struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

/// AsyncRead/AsyncWrite adapter over the tunnel frame channel: reads consume
/// `Ctrl::Payload` frames, writes emit `Data` frames.
struct TunnelIo {
    rx: mpsc::Receiver<Ctrl>,
    sender: PollSender<Frame>,
    conn_id: i64,
    buf: bytes::BytesMut,
    eof: bool,
}

impl TunnelIo {
    fn new(tx: mpsc::Sender<Frame>, conn_id: i64, rx: mpsc::Receiver<Ctrl>) -> Self {
        Self {
            rx,
            sender: PollSender::new(tx),
            conn_id,
            buf: bytes::BytesMut::new(),
            eof: false,
        }
    }

    fn buffered(&self) -> usize {
        self.buf.len()
    }

    fn peek_prefix(&self, prefix: &[u8]) -> bool {
        self.buf.starts_with(prefix)
    }

    /// Pulls one control message into the buffer; false on close/abort.
    async fn fill(&mut self) -> bool {
        match self.rx.recv().await {
            Some(Ctrl::Payload(data)) => {
                self.buf.extend_from_slice(&data);
                true
            }
            Some(Ctrl::WriteClose) => {
                self.eof = true;
                false
            }
            Some(Ctrl::Abort) | None => false,
        }
    }

    fn send_close(&mut self, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        let frame = Frame {
            body: Some(Body::Close(Close {
                conn_id: self.conn_id,
            })),
        };
        match self.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let _ = self.sender.send_item(frame);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::other(e.to_string()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for TunnelIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if !this.buf.is_empty() {
            let n = this.buf.len().min(buf.remaining());
            let data = this.buf.split_to(n);
            buf.put_slice(&data);
            return Poll::Ready(Ok(()));
        }
        if this.eof {
            // Unfilled ReadBuf on return signals EOF to hyper.
            return Poll::Ready(Ok(()));
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(Ctrl::Payload(data))) => {
                this.buf.extend_from_slice(&data);
                let n = this.buf.len().min(buf.remaining());
                let data = this.buf.split_to(n);
                buf.put_slice(&data);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ctrl::WriteClose)) => {
                this.eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ctrl::Abort)) => Poll::Ready(Err(std::io::Error::other(
                "connection aborted by the tunnel peer",
            ))),
            Poll::Ready(None) => {
                this.eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TunnelIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        let n = buf.len().min(CHUNK);
        let frame = Frame {
            body: Some(Body::Data(Data {
                conn_id: this.conn_id,
                payload: buf[..n].to_vec(),
            })),
        };
        match this.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let _ = this.sender.send_item(frame);
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => {
                Poll::Ready(Err(std::io::Error::other(format!("tunnel closed: {e}"))))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Half-close the tunnel direction when hyper is done with the connection.
        self.send_close(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Builds the hello guest (a no-op when fresh) and returns the component
    /// path. The `wasm32-wasip2` artifacts live in the regular target dir, so
    /// repeated runs reuse cargo's incrementality.
    fn hello_wasm() -> std::path::PathBuf {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let output = std::process::Command::new(cargo)
            .args([
                "build",
                "--release",
                "-p",
                "hello",
                "--target",
                "wasm32-wasip2",
            ])
            .current_dir(&root)
            .output()
            .expect("run cargo for the guest build");
        assert!(
            output.status.success(),
            "guest build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let target = std::env::var("CARGO_TARGET_DIR")
            .unwrap_or_else(|_| root.join("target").into_os_string().into_string().unwrap());
        std::path::Path::new(&target).join("wasm32-wasip2/release/hello.wasm")
    }

    /// Drives `serve` over in-memory frame channels with a `GET /stream`
    /// request and collects the response: 10 MiB must arrive progressively
    /// (the guest paces chunks), not as one buffered blob.
    #[tokio::test]
    async fn streams_response_frames_as_the_guest_produces_them() {
        let wasm = hello_wasm();
        let loaded = load(&Locator::File(wasm)).await.unwrap();

        let (tx, mut rx) = mpsc::channel::<Frame>(64);
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<Ctrl>(32);
        let registry = crate::Registry::default();
        tokio::spawn(serve(tx, registry, 1, loaded, ctrl_rx));

        ctrl_tx
            .send(Ctrl::Payload(
                b"GET /stream HTTP/1.1\r\nhost: demo.local\r\nconnection: close\r\n\r\n".to_vec(),
            ))
            .await
            .unwrap();

        let started = Instant::now();
        let mut raw: Vec<u8> = Vec::new();
        let mut head_end = None;
        let mut first_body_at = None;
        let mut closed_at = None;
        let completed = async {
            while closed_at.is_none() {
                let Some(frame) = rx.recv().await else {
                    panic!("serve task ended without Close");
                };
                match frame.body.expect("frame body") {
                    Body::Data(data) => {
                        raw.extend_from_slice(&data.payload);
                        if head_end.is_none() {
                            head_end = raw
                                .windows(4)
                                .position(|w| w == b"\r\n\r\n")
                                .map(|pos| pos + 4);
                        }
                        if let (Some(end), None) = (head_end, first_body_at) {
                            if raw.len() > end {
                                first_body_at = Some(Instant::now() - started);
                            }
                        }
                    }
                    Body::Close(..) => closed_at = Some(Instant::now() - started),
                    _ => {}
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(30), completed)
            .await
            .expect("response did not complete in time");

        let head = std::str::from_utf8(&raw[..head_end.unwrap()]).unwrap();
        assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
        let body = &raw[head_end.unwrap()..];
        assert_eq!(body.len(), 10 * 1024 * 1024, "streamed body length");

        // The guest paces 20 x 512 KiB over ~1s: body bytes must spread over
        // that window instead of appearing at once (the old buffered path).
        let spread = closed_at.unwrap().as_millis() - first_body_at.unwrap().as_millis();
        assert!(spread > 300, "body spread too small: {spread}ms");
    }
}
