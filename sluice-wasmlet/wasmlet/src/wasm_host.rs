//! Hosting of wasi:http 0.3 handler components for `wasm:` upstream routes.
//!
//! The tunnel connection's byte stream is served by hyper (http/1.1 vs h2c
//! sniffed from the first bytes); every request is handled by a fresh
//! instance of the guest component through `wasmtime-wasi-http`'s p3
//! `Service.handle`. Response bodies are buffered (PoC scope).

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

fn plain_response(
    status: u16,
    body: String,
) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
    hyper::Response::builder()
        .status(status)
        .body(http_body_util::Full::new(bytes::Bytes::from(body)))
        .expect("static response")
}

async fn respond(
    loaded: &Loaded,
    req: hyper::Request<hyper::body::Incoming>,
) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
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

    let handled = store.run_concurrent(async |accessor| {
        let (result, ()) = tokio::join!(
            async {
                let outcome = match service.handle(accessor, req).await {
                    Ok(outcome) => outcome,
                    Err(e) => return Err(format!("wasm trap: {e}")),
                };
                let response = match outcome {
                    Ok(response) => response,
                    Err(code) => return Ok(plain_response(502, format!("wasi error: {code:?}"))),
                };
                let http = accessor
                    .with(|store| response.into_http(store, futures_util::future::ready(Ok(()))));
                let http = match http {
                    Ok(http) => http,
                    Err(e) => return Err(format!("response conversion failed: {e}")),
                };
                let (parts, body) = http.into_parts();
                match body.collect().await {
                    Ok(collected) => Ok(hyper::Response::from_parts(
                        parts,
                        http_body_util::Full::new(collected.to_bytes()),
                    )),
                    Err(e) => Err(format!("body collection failed: {e}")),
                }
            },
            async {
                // Drains request-body bookkeeping; errors surface in the response path.
                let _ = pump.await;
            },
        );
        result
    });

    let handled = handled.await;
    match handled {
        Ok(Ok(response)) => response,
        Ok(Err(message)) => plain_response(500, format!("wasm handler failed: {message}")),
        Err(e) => plain_response(500, format!("wasm execution failed: {e}")),
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
