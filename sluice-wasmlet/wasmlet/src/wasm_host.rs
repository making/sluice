//! Hosting of wasi:http 0.3 handler components for `wasm:` upstream routes.
//!
//! The tunnel connection's byte stream is served by hyper (http/1.1 vs h2c
//! sniffed from the first bytes); every request is handled by a fresh
//! instance of the guest component (or, with `pool=N`, a warm one) through
//! `wasmtime-wasi-http`'s p3 `Service.handle`. Instantiation is pre-linked
//! (`InstancePre`) and bounded by per-route epoch budgets and memory caps. Response bodies stream: the
//! store task relays frames to hyper through a bounded channel, so a slow
//! client backpressures the guest instead of buffering the response.
//!
//! The WASI surface follows `wasmtime serve` for p3: a route links http +
//! the p3 cli/clocks/random interfaces, `cli` links the full wasi p3 set, and
//! the network capabilities configure the per-request `WasiCtx`. wasi p2 is
//! unsupported: a p2-importing component is rejected at load. Guest stdout /
//! stderr always reach the process log, line-prefixed.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};

use http_body_util::BodyExt as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::{CancellationToken, DropGuard, PollSender};
use wasmtime::AsContextMut as _;

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

/// Granularity of every request budget: the shared engine's epoch is
/// incremented at this interval, and budgets are denominated in ticks.
const EPOCH_TICK_MS: u64 = 10;

const EPOCH_TICK: std::time::Duration = std::time::Duration::from_millis(EPOCH_TICK_MS);

/// Request budget when the route does not name one. Must cover the slowest
/// legitimate handler, including time it spends suspended (the epoch deadline
/// is absolute, checked at the guest's next yield point).
const DEFAULT_BUDGET_MS: u64 = 10_000;

/// Linear-memory cap per instance when the route does not name one; a guest
/// growing beyond it traps instead of OOM-ing the process.
const DEFAULT_MEMORY_MIB: usize = 256;

/// Longest guest log line buffered on the host; a longer run without newline
/// is emitted in pieces, so a guest cannot grow host memory past its own cap.
const GUEST_LOG_LINE_MAX: usize = 16 * 1024;

// ---------------------------------------------------------------------------
// Engine

/// The engine shared by every component: one epoch ticker drives the
/// per-request deadlines of all stores.
fn engine() -> &'static wasmtime::Engine {
    static ENGINE: OnceLock<wasmtime::Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model_async(true);
        // Epoch deadlines raise `Trap::Interrupt` in guest code; without this
        // a budget could not interrupt a guest between yield points.
        config.epoch_interruption(true);
        let engine = wasmtime::Engine::new(&config).expect("valid engine config");
        // A dedicated thread: guests run on tokio workers and can starve the
        // timer wheel with a busy loop, which would stall their own budget.
        let ticker = engine.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(EPOCH_TICK);
                ticker.increment_epoch();
            }
        });
        engine
    })
}

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
    /// The pre-linked instantiation plan: import resolution and interface
    /// typechecking happen once here, so the per-request `instantiate_async`
    /// skips them. Measured on the hello guest (see the ignored
    /// `instantiation_latency_reference`): ~260us full vs ~200us pre-linked
    /// (dev profile) per instantiation, i.e. the warm path stays far below a
    /// millisecond -- so instances are not pooled by default. `pool=N` opts a
    /// route into `Pool`, which amortizes even that by keeping whole
    /// `(Store, Service)` units warm; pooling assumes stateless guests (see
    /// `Pool`).
    service_pre: wasmtime_wasi_http::p3::bindings::ServicePre<Ctx>,
    /// Short component name (the locator's last path segment) for log prefixes.
    label: String,
}

/// Per-route resource limits, applied to every request's instance.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Limits {
    /// Request budget: the wall-clock deadline of instantiation, handler and
    /// response relay. Running wasm is interrupted at its next yield point
    /// past it by a wasmtime epoch deadline (`Trap::Interrupt`); a request
    /// suspended past it (e.g. on an upstream that never answers) is stopped
    /// by the host watchdog (`supervise`). Either way HTTP 504, or a truncated
    /// body once streaming -- this bounds runaway CPU and hung handlers alike.
    pub budget: std::time::Duration,
    /// Linear-memory cap per instance in bytes; growth beyond it fails and
    /// surfaces as a guest trap (HTTP 500) instead of OOM-ing the process.
    pub memory: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            budget: std::time::Duration::from_millis(DEFAULT_BUDGET_MS),
            memory: DEFAULT_MEMORY_MIB << 20,
        }
    }
}

impl Limits {
    /// Applies one `key=value` route option; false when the key is not a limit.
    fn set(&mut self, key: &str, value: &str) -> Result<bool, String> {
        let number = || -> Result<u64, String> {
            value
                .parse()
                .map_err(|_| format!("{key}: not a number '{value}'"))
        };
        match key {
            "budget-ms" => self.budget = std::time::Duration::from_millis(number()?.max(1)),
            "memory-mib" => self.memory = (number()? as usize) << 20,
            _ => return Ok(false),
        }
        Ok(true)
    }
}

/// Per-route WASI capabilities, named after the `-S` options `wasmtime serve`
/// takes, so a component runs here with the flags it runs with there. None
/// granted is the serve default: http plus the p3 cli / clocks / random
/// interfaces (stdio included).
///
/// - `http` (outgoing requests via `wasi:http/client`) is always linked, as in
///   serve; the name is accepted for parity with `-S http` and changes nothing.
/// - `cli` is linker-level, as in serve: the full wasi p3 surface (sockets
///   and filesystem included) is linked. wasi p2 is never linked: p2 guests
///   are unsupported. A component importing an
///   interface its route does not link fails at startup.
/// - the network capabilities are enforced by `wasmtime-wasi` on every socket
///   call (`WasiCtx`): `tcp` / `udp` allow the protocol, `inherit-network`
///   allows every address (and implies `tcp` + `udp`), `allow-ip-name-lookup`
///   allows `wasi:sockets/ip-name-lookup`. Without `inherit-network` every
///   address is denied. They need `cli`, since sockets are not linked otherwise.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Caps {
    pub cli: bool,
    pub inherit_network: bool,
    pub allow_ip_name_lookup: bool,
    pub tcp: bool,
    pub udp: bool,
}

impl Caps {
    /// Accepted but always granted (see the type doc), so not a flag.
    const HTTP: &'static str = "http";

    /// Capability names in canonical order.
    const NAMES: [&'static str; 5] = [
        "cli",
        "inherit-network",
        "allow-ip-name-lookup",
        "tcp",
        "udp",
    ];

    fn flag(&mut self, name: &str) -> Option<&mut bool> {
        match name {
            "cli" => Some(&mut self.cli),
            "inherit-network" => Some(&mut self.inherit_network),
            "allow-ip-name-lookup" => Some(&mut self.allow_ip_name_lookup),
            "tcp" => Some(&mut self.tcp),
            "udp" => Some(&mut self.udp),
            _ => None,
        }
    }

    /// Parses a comma-separated capability list (`cli,inherit-network`).
    pub fn parse(list: &str) -> Result<Caps, String> {
        let mut caps = Caps::default();
        for name in list
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if name == Caps::HTTP {
                continue;
            }
            let Some(flag) = caps.flag(name) else {
                return Err(format!(
                    "unknown wasi capability '{name}' ({}, {})",
                    Caps::NAMES.join(", "),
                    Caps::HTTP
                ));
            };
            *flag = true;
        }
        let network = caps.inherit_network || caps.allow_ip_name_lookup || caps.tcp || caps.udp;
        if network && !caps.cli {
            return Err(
                "network capabilities need 'cli' (wasi:sockets is linked only with it)".into(),
            );
        }
        Ok(caps)
    }

    /// The granted names in canonical order.
    fn names(&self) -> Vec<&'static str> {
        let mut caps = *self;
        Caps::NAMES
            .into_iter()
            .filter(|name| *caps.flag(name).expect("known name"))
            .collect()
    }

    fn configure(&self, wasi: &mut wasmtime_wasi::WasiCtxBuilder) {
        if self.inherit_network {
            wasi.inherit_network().allow_tcp(true).allow_udp(true);
        }
        if self.allow_ip_name_lookup {
            wasi.allow_ip_name_lookup(true);
        }
        if self.tcp {
            wasi.allow_tcp(true);
        }
        if self.udp {
            wasi.allow_udp(true);
        }
    }
}

/// Everything a route configures for its instances: resource limits, WASI
/// capabilities and instance pooling.
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Sandbox {
    pub limits: Limits,
    pub caps: Caps,
    /// Warm `(Store, Service)` units kept for the route (see `Pool`); 0
    /// instantiates per request.
    pub pool: usize,
}

impl Sandbox {
    /// Parses the route query (`budget-ms=N&memory-mib=N&pool=N&wasi=cli,tcp`).
    pub fn parse(query: &str) -> Result<Sandbox, String> {
        let mut sandbox = Sandbox::default();
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!("expected key=value in '{query}'"));
            };
            if key == "wasi" {
                sandbox.caps = Caps::parse(value)?;
            } else if key == "pool" {
                sandbox.pool = value
                    .parse()
                    .map_err(|_| format!("pool: not a number '{value}'"))?;
            } else if !sandbox.limits.set(key, value)? {
                return Err(format!(
                    "unknown option '{key}' (budget-ms, memory-mib, pool, wasi)"
                ));
            }
        }
        Ok(sandbox)
    }

    /// The canonical query of the non-default settings; empty for defaults.
    /// Part of the advertised target, so every distinct sandbox of one
    /// component is its own dial address.
    pub fn query(&self) -> String {
        let defaults = Limits::default();
        let mut pairs = Vec::new();
        if self.limits.budget != defaults.budget {
            pairs.push(format!("budget-ms={}", self.limits.budget.as_millis()));
        }
        if self.limits.memory != defaults.memory {
            pairs.push(format!("memory-mib={}", self.limits.memory >> 20));
        }
        if self.pool > 0 {
            pairs.push(format!("pool={}", self.pool));
        }
        let caps = self.caps.names();
        if !caps.is_empty() {
            pairs.push(format!("wasi={}", caps.join(",")));
        }
        pairs.join("&")
    }
}

/// Loads (or fetches from cache) the component behind `locator`, linked for
/// the `caps` it runs with (only `cli` changes the linker).
pub async fn load(locator: &Locator, caps: Caps) -> Result<Arc<Loaded>, String> {
    // (target, cli) -> component: `cli` selects the linker
    type Cache = Mutex<HashMap<(String, bool), Arc<Loaded>>>;
    static CACHE: OnceLock<Cache> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let target = locator.target().await?;
    let key = (target, caps.cli);
    if let Some(loaded) = cache.lock().unwrap().get(&key) {
        return Ok(loaded.clone());
    }
    let label = key.0.rsplit('/').next().unwrap_or(&key.0).to_string();
    let bytes = locator.fetch().await?;
    let engine = engine().clone();
    let name = key.0.strip_prefix("wasm:").unwrap_or(&key.0);
    let component = wasmtime::component::Component::new(&engine, &bytes)
        .map_err(|e| format!("component {name}: {e}"))?;
    if let Some(p2) = component
        .component_type()
        .imports(&engine)
        .map(|(import, _)| import)
        .find(|import| import.starts_with("wasi:") && import.contains("@0.2."))
    {
        return Err(format!(
            "component {name} imports {p2}: wasi p2 is unsupported (build for wasm32-wasip3)"
        ));
    }
    let linker = linker(&engine, caps).map_err(|e| format!("linker: {e}"))?;
    let pre = linker.instantiate_pre(&component).map_err(|e| {
        let hint = if caps.cli {
            ""
        } else {
            " (the route may need wasi=cli)"
        };
        format!("pre-instantiate {name}: {e:#}{hint}")
    })?;
    let service_pre =
        wasmtime_wasi_http::p3::bindings::ServicePre::new(pre).map_err(|e| format!("bind: {e}"))?;
    let loaded = Arc::new(Loaded { service_pre, label });
    cache.lock().unwrap().insert(key, loaded.clone());
    Ok(loaded)
}

/// The interfaces a route links, as `wasmtime serve` does for p3: http plus
/// cli / clocks / random, or with `cli` the full wasi p3 set. Unlike serve's
/// `-Scli`, no p2 interface is linked.
fn linker(
    engine: &wasmtime::Engine,
    caps: Caps,
) -> wasmtime::Result<wasmtime::component::Linker<Ctx>> {
    let mut linker = wasmtime::component::Linker::<Ctx>::new(engine);
    if caps.cli {
        wasmtime_wasi::p3::add_to_linker(&mut linker)?;
    } else {
        wasmtime_wasi::p3::clocks::add_to_linker(&mut linker)?;
        wasmtime_wasi::p3::random::add_to_linker(&mut linker)?;
        wasmtime_wasi::p3::cli::add_to_linker(&mut linker)?;
    }
    wasmtime_wasi_http::p3::add_to_linker(&mut linker)?;
    Ok(linker)
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
    limiter: MemoryCap,
}

impl Ctx {
    /// A request's store state; `label` names the component in the guest's
    /// log lines.
    fn new(sandbox: &Sandbox, label: &str) -> Self {
        static REQUESTS: AtomicU64 = AtomicU64::new(1);
        let request = REQUESTS.fetch_add(1, Ordering::Relaxed);
        let mut wasi = wasmtime_wasi::WasiCtxBuilder::new();
        wasi.stdout(GuestLog::new(
            format!("stdout [{label}#{request}] :: "),
            Output::Stdout,
        ));
        wasi.stderr(GuestLog::new(
            format!("stderr [{label}#{request}] :: "),
            Output::Stderr,
        ));
        sandbox.caps.configure(&mut wasi);
        Self {
            table: wasmtime::component::ResourceTable::default(),
            wasi: wasi.build(),
            http: wasmtime_wasi_http::WasiHttpCtx::new(),
            hooks: NoHooks,
            limiter: MemoryCap {
                max: sandbox.limits.memory,
                used: 0,
                grew: false,
            },
        }
    }

    /// Why a pooled unit's store must not serve another request, if so:
    /// guest handles left in the resource table (guest state leaked into the
    /// store), or linear memory that grew past half its cap during the
    /// request -- a guest leaking per request is recycled before it hits the
    /// cap, while one that settled below it stays warm.
    fn spent(&self) -> Option<&'static str> {
        if !self.table.is_empty() {
            Some("resource table left dirty")
        } else if self.limiter.grew && self.limiter.used > self.limiter.max / 2 {
            Some("linear memory grew past half its cap")
        } else {
            None
        }
    }
}

/// The per-store linear-memory cap backing `Limits::memory`: growth past the
/// cap is denied, which the guest sees as a failed allocation / `memory.grow`.
/// Also tracks what the store holds, for `Ctx::spent`.
struct MemoryCap {
    max: usize,
    /// Linear memory the store holds, over all its memories (wasm memory
    /// never shrinks).
    used: usize,
    /// Whether `used` grew since the last `rearm`.
    grew: bool,
}

impl MemoryCap {
    /// Starts a request's growth tracking.
    fn rearm(&mut self) {
        self.grew = false;
    }
}

// the trait itself is `#[async_trait]`, so the impl must match its boxed form
#[async_trait::async_trait]
impl wasmtime::ResourceLimiterAsync for MemoryCap {
    async fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max {
            return Ok(false);
        }
        // creation reports growth from 0, so this sums every memory
        self.used += desired.saturating_sub(current);
        self.grew = true;
        Ok(true)
    }

    async fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

/// Which process stream a guest stream is logged to.
#[derive(Clone, Copy)]
enum Output {
    Stdout,
    Stderr,
}

/// A guest stdout / stderr: whole lines, each prefixed with the component and
/// request, are written to the process stream in one call, so concurrent
/// requests do not interleave mid-line. A trailing partial line is flushed
/// when the request's store goes away, an overlong one at
/// `GUEST_LOG_LINE_MAX`.
#[derive(Clone)]
struct GuestLog {
    state: Arc<GuestLogState>,
}

struct GuestLogState {
    prefix: String,
    output: Output,
    pending: Mutex<Vec<u8>>,
}

impl GuestLog {
    fn new(prefix: String, output: Output) -> Self {
        Self {
            state: Arc::new(GuestLogState {
                prefix,
                output,
                pending: Mutex::new(Vec::new()),
            }),
        }
    }

    fn write(&self, bytes: &[u8]) -> std::io::Result<()> {
        let mut pending = self.state.pending.lock().unwrap();
        pending.extend_from_slice(bytes);
        let end = match pending.iter().rposition(|b| *b == b'\n') {
            Some(newline) => newline + 1,
            None if pending.len() >= GUEST_LOG_LINE_MAX => pending.len(),
            None => return Ok(()),
        };
        let lines: Vec<u8> = pending.drain(..end).collect();
        drop(pending);
        self.state.emit(&lines)
    }
}

impl GuestLogState {
    /// Writes complete lines (the last may lack its newline) with the prefix.
    fn emit(&self, lines: &[u8]) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut out = Vec::with_capacity(lines.len() + 16);
        for line in lines.split_inclusive(|b| *b == b'\n') {
            out.extend_from_slice(self.prefix.as_bytes());
            out.extend_from_slice(line);
        }
        if !out.ends_with(b"\n") {
            out.push(b'\n');
        }
        #[cfg(test)]
        tests::captured_log()
            .lock()
            .unwrap()
            .extend_from_slice(&out);
        match self.output {
            Output::Stdout => std::io::stdout().lock().write_all(&out),
            Output::Stderr => std::io::stderr().lock().write_all(&out),
        }
    }
}

impl Drop for GuestLogState {
    fn drop(&mut self) {
        let pending = std::mem::take(self.pending.get_mut().unwrap_or_else(|e| e.into_inner()));
        if !pending.is_empty() {
            let _ = self.emit(&pending);
        }
    }
}

impl wasmtime_wasi::cli::StdoutStream for GuestLog {
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

impl wasmtime_wasi::cli::IsTerminal for GuestLog {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl AsyncWrite for GuestLog {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.write(buf).map(|()| buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
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

/// Ends a body that may already be streaming with an error frame, so hyper
/// sees the truncation instead of a short body that looks complete. The frame
/// queues behind the relayed ones and drains at the client's pace, hence the
/// detached send; it ends at the latest when the client goes away.
fn truncate(frames: mpsc::Sender<RelayFrame>, error: RelayError) {
    tokio::spawn(async move {
        let _ = frames.send(Err(error)).await;
    });
}

/// Forwards guest body frames to hyper until either side ends. A body error
/// is relayed so hyper sees the truncation instead of a silent short body; a
/// gone client just ends the relay and the guest drains through the pipe.
/// False when the relay ended early (the client disappeared before the body
/// did): the serving store's fate is then uncertain.
async fn relay(
    mut body: http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, wasmtime_wasi_http::Error>,
    frame_tx: mpsc::Sender<RelayFrame>,
) -> bool {
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if frame_tx.send(Ok(frame)).await.is_err() {
                    return false;
                }
            }
            Err(e) => {
                let _ = frame_tx.send(Err(e.into())).await;
                return true;
            }
        }
    }
    true
}

const BUDGET_EXHAUSTED: &str = "wasm budget exhausted";

/// Maps a handler failure to its response: 504 when the epoch budget
/// interrupted the guest, 500 for any other trap or error.
fn failure(e: &wasmtime::Error) -> (u16, String) {
    if e.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::Interrupt) {
        (504, BUDGET_EXHAUSTED.into())
    } else {
        (500, format!("wasm trap: {e}"))
    }
}

/// The epoch ticks for a request budget: granular to the tick, at least one.
fn budget_ticks(budget: std::time::Duration) -> u64 {
    (budget.as_millis() as u64 / EPOCH_TICK_MS).max(1)
}

/// Serves one request on an already-instantiated `service`, within a store
/// closure: re-arms nothing (the caller owns the deadline), converts the
/// request, runs the handler, streams the response, and drains the request
/// body. Reports whether the store came out of it cleanly.
async fn serve_on(
    accessor: &wasmtime::component::Accessor<Ctx>,
    service: &wasmtime_wasi_http::p3::bindings::Service,
    request: hyper::Request<hyper::body::Incoming>,
    head: mpsc::Sender<Head>,
    frames: mpsc::Sender<RelayFrame>,
) -> Outcome {
    let (req, pump) = accessor.with(|mut access| {
        wasmtime_wasi_http::p3::Request::from_http(&mut access.data_mut().hooks, request)
    });

    let served = async {
        match service.handle(accessor, req).await {
            Ok(Ok(response)) => {
                let http = accessor
                    .with(|access| response.into_http(access, futures_util::future::ready(Ok(()))));
                match http {
                    Ok(http) => {
                        let (parts, body) = http.into_parts();
                        let _ = head.send(Head::Response(parts)).await;
                        let ended = relay(body, frames).await;
                        if ended {
                            Outcome::Clean
                        } else {
                            // The relay ended early: the client disappeared.
                            // The store is likely fine, but guest tasks may
                            // still be draining -- recycle the unit.
                            Outcome::Uncertain
                        }
                    }
                    Err(e) => {
                        failed(head, 500, format!("response conversion failed: {e}")).await;
                        Outcome::Uncertain
                    }
                }
            }
            Ok(Err(code)) => {
                failed(head, 502, format!("wasi error: {code:?}")).await;
                // A guest-level error, no trap: the store is unharmed.
                Outcome::Clean
            }
            Err(e) => {
                let (status, message) = failure(&e);
                failed(head, status, message).await;
                Outcome::Uncertain
            }
        }
    };
    let (outcome, ()) = tokio::join!(served, async {
        // Drains request-body bookkeeping; errors surface in the response path.
        let _ = pump.await;
    });
    outcome
}

/// Where a request's response goes: its head, its body frames, and its
/// client, cancelled when hyper drops the response before its head.
#[derive(Clone)]
struct Reply {
    head: mpsc::Sender<Head>,
    frames: mpsc::Sender<RelayFrame>,
    client: CancellationToken,
}

/// The hyper side of a `Reply`.
struct Awaited {
    head: mpsc::Receiver<Head>,
    frames: mpsc::Receiver<RelayFrame>,
    /// Cancels the reply's `client` when dropped before the head arrived.
    client: DropGuard,
}

fn reply() -> (Reply, Awaited) {
    let (head_tx, head_rx) = mpsc::channel::<Head>(1);
    let (frame_tx, frame_rx) = mpsc::channel::<RelayFrame>(RELAY_BUFFER);
    let client = CancellationToken::new();
    let reply = Reply {
        head: head_tx,
        frames: frame_tx,
        client: client.clone(),
    };
    let awaited = Awaited {
        head: head_rx,
        frames: frame_rx,
        client: client.drop_guard(),
    };
    (reply, awaited)
}

impl Awaited {
    /// The hyper response: the head as known, a synthesized failure, or a
    /// generic 500 when none arrived (the serving store died before any
    /// head). Only a client leaving before the head cancels the request:
    /// once the body streams, hyper drops it as soon as a `content-length`
    /// is satisfied, which says nothing about the client. A client leaving
    /// mid-body fails the relay's next send instead (`Outcome::Uncertain`),
    /// and a guest stalled mid-body runs into its budget.
    async fn response(mut self) -> hyper::Response<RelayBody> {
        let head = self.head.recv().await;
        self.client.disarm();
        match head {
            Some(Head::Response(parts)) => hyper::Response::from_parts(
                parts,
                http_body_util::StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(
                    self.frames,
                ))
                .boxed_unsync(),
            ),
            Some(Head::Failure(status, message)) => plain_response(status, message),
            None => plain_response(500, "wasm handler failed".into()),
        }
    }
}

/// The request a store is serving, as its watchdog sees it.
struct InFlight {
    reply: Reply,
    /// Wall-clock end of the request's budget.
    deadline: tokio::time::Instant,
}

/// Why the watchdog stopped a store.
enum Stop {
    /// The in-flight request overran its budget.
    Budget,
    /// The in-flight request's client went away.
    Gone,
}

/// Resolves when the watched request overruns its deadline or loses its
/// client. Every wake re-reads the watched request, so a finished request's
/// deadline or departing client never stops its successor.
async fn watchdog(mut watched: watch::Receiver<Option<InFlight>>) -> Stop {
    let mut open = true;
    loop {
        let current = watched
            .borrow_and_update()
            .as_ref()
            .map(|job| (job.deadline, job.reply.client.clone()));
        let Some((deadline, client)) = current else {
            if !open || watched.changed().await.is_err() {
                // the store side is done: nothing left to watch
                return std::future::pending().await;
            }
            continue;
        };
        if client.is_cancelled() {
            return Stop::Gone;
        }
        if tokio::time::Instant::now() >= deadline {
            return Stop::Budget;
        }
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => {}
            () = client.cancelled() => {}
            changed = watched.changed(), if open => open = changed.is_ok(),
        }
    }
}

/// Drives a store under its watchdog. The watchdog sits outside the store:
/// a guest suspended on a host future (an upstream that never answers) runs
/// no wasm the epoch deadline could interrupt, and wasmtime cannot reliably
/// race a timer inside `run_concurrent`. A store error or an overrun budget
/// fails the in-flight request -- its head, or once streaming its body -- and
/// a gone client just ends it; dropping `driven` drops the store.
async fn supervise(
    label: &str,
    driven: impl Future<Output = wasmtime::Result<()>>,
    watched: watch::Receiver<Option<InFlight>>,
) {
    let (status, message) = tokio::select! {
        driven = driven => match driven {
            Ok(()) => return,
            Err(e) => {
                eprintln!("[wasm] {label} store ended with error: {e}");
                failure(&e)
            }
        },
        stop = watchdog(watched.clone()) => match stop {
            Stop::Budget => {
                eprintln!("[wasm] {label} request overran its budget: store stopped");
                (504, BUDGET_EXHAUSTED.to_string())
            }
            Stop::Gone => return,
        },
    };
    let reply = watched.borrow().as_ref().map(|job| job.reply.clone());
    if let Some(reply) = reply {
        let _ = reply.head.try_send(Head::Failure(
            status,
            format!("wasm handler failed: {message}"),
        ));
        truncate(reply.frames, message.into());
    }
}

async fn respond(
    handler: &Handler,
    request: hyper::Request<hyper::body::Incoming>,
) -> hyper::Response<RelayBody> {
    let (reply, awaited) = reply();
    let request = match &handler.pool {
        Some(pool) => match pool.dispatch(request, &reply) {
            Ok(()) => {
                // The unit's job holds the reply now: a job lost with its
                // unit must close the head channel, so the 500 below wins.
                drop(reply);
                return awaited.response().await;
            }
            Err(request) => request,
        },
        None => request,
    };

    let (loaded, sandbox) = (&handler.loaded, handler.sandbox);
    // The budget covers instantiation, handler and response relay alike.
    let deadline = tokio::time::Instant::now() + sandbox.limits.budget;
    let mut store = wasmtime::Store::new(engine(), Ctx::new(&sandbox, &loaded.label));
    store.limiter_async(|ctx| &mut ctx.limiter);
    // The deadline must exist before any wasm runs: with epoch interruption
    // enabled, a store without one traps on the first observed tick.
    store.set_epoch_deadline(budget_ticks(sandbox.limits.budget));
    let service = match loaded.service_pre.instantiate_async(&mut store).await {
        Ok(service) => service,
        Err(e) => return plain_response(500, format!("wasm instantiate failed: {e}")),
    };

    // The response head is handed to hyper as soon as it is known; body frames
    // are relayed by the store task, which must outlive this future for the
    // guest to keep producing while hyper drains at the client's pace. The
    // task ends when the response is fully relayed or the watchdog stops it.
    let label = loaded.label.clone();
    tokio::spawn(async move {
        let (_watch, watched) = watch::channel(Some(InFlight {
            reply: reply.clone(),
            deadline,
        }));
        let driven = store.run_concurrent(async move |accessor| {
            serve_on(accessor, &service, request, reply.head, reply.frames).await;
        });
        supervise(&label, driven, watched).await;
    });

    awaited.response().await
}

// ---------------------------------------------------------------------------
// Route handlers and the instance pool

/// A route's request handler: the component, the sandbox its instances run
/// in, and the warm-instance pool built from both. The one source of a
/// route's settings, so the cold and the pooled path cannot diverge.
pub struct Handler {
    loaded: Arc<Loaded>,
    sandbox: Sandbox,
    pool: Option<Arc<Pool>>,
}

impl Handler {
    /// Starts the route's pool when its sandbox names one.
    pub fn new(loaded: Arc<Loaded>, sandbox: Sandbox) -> Arc<Handler> {
        let pool = (sandbox.pool > 0).then(|| Pool::new(loaded.clone(), sandbox));
        Arc::new(Handler {
            loaded,
            sandbox,
            pool,
        })
    }
}

/// Whether a unit's store came out of a request reusable.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Outcome {
    /// Normal end: the unit is reusable unless the request left it spent
    /// (`Ctx::spent`).
    Clean,
    /// A trap, a failed conversion or an interrupted relay: unit state is
    /// uncertain, retire it.
    Uncertain,
}

/// One request handed to a pooled unit.
struct Job {
    request: hyper::Request<hyper::body::Incoming>,
    reply: Reply,
}

/// A per-route pool of warm `(Store, Service)` units: each unit is a detached
/// task owning its store for its lifetime, running `run_concurrent` once
/// (which consumes the store) with a per-request job loop inside. A unit
/// serves one request at a time; the pool's size is the warm concurrency, and
/// requests beyond it fall back to the per-request path (`respond`), so the
/// pool only ever improves the warm latency. Measured on the hello guest (the
/// ignored `load_comparison_reference`, sequential requests, dev profile):
/// ~380us cold vs ~120us pooled per request.
///
/// Reuse assumes stateless / reentrant-safe guests: module state legitimately
/// persists across a unit's requests. A guest that misbehaves poisons its own
/// unit only: a trap, a failed conversion, an interrupted relay, an overrun
/// budget, a departed client or a spent store (`Ctx::spent`) retires it, and a
/// retired unit is replaced.
struct Pool {
    loaded: Arc<Loaded>,
    sandbox: Sandbox,
    state: Mutex<PoolState>,
}

struct PoolState {
    /// Senders to idle units; one entry per instantiated unit between
    /// requests.
    idle: Vec<mpsc::Sender<Job>>,
    /// Live unit tasks.
    units: usize,
    /// Units ever spawned; growth beyond the target signals recycling
    /// (diagnostics, and asserted in tests).
    spawned: usize,
    /// Requests served by a pooled unit instead of the per-request path.
    served: usize,
}

impl Pool {
    /// Spawns `sandbox.pool` units. Each instantiates asynchronously and is
    /// offered once ready; until then requests take the per-request path.
    fn new(loaded: Arc<Loaded>, sandbox: Sandbox) -> Arc<Self> {
        let pool = Arc::new(Self {
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                units: sandbox.pool,
                spawned: 0,
                served: 0,
            }),
            loaded,
            sandbox,
        });
        for _ in 0..sandbox.pool {
            Self::spawn_unit(pool.clone());
        }
        pool
    }

    /// Starts one unit, already counted in `units`.
    fn spawn_unit(pool: Arc<Pool>) {
        pool.state.lock().unwrap().spawned += 1;
        tokio::spawn(run_unit(pool));
    }

    /// Hands the request to an idle unit. `Err(request)` when none accepted
    /// it (no stock, or a unit vanished): the caller instantiates per request.
    #[expect(
        clippy::result_large_err,
        reason = "handed back once, on the cold path that instantiates anyway"
    )]
    fn dispatch(
        &self,
        mut request: hyper::Request<hyper::body::Incoming>,
        reply: &Reply,
    ) -> Result<(), hyper::Request<hyper::body::Incoming>> {
        loop {
            let Some(tx) = self.state.lock().unwrap().idle.pop() else {
                return Err(request);
            };
            let job = Job {
                request,
                reply: reply.clone(),
            };
            request = match tx.try_send(job) {
                Ok(()) => {
                    self.state.lock().unwrap().served += 1;
                    return Ok(());
                }
                // A unit died since it went idle; its entry is stale.
                Err(tokio::sync::mpsc::error::TrySendError::Full(job))
                | Err(tokio::sync::mpsc::error::TrySendError::Closed(job)) => job.request,
            };
        }
    }

    // Diagnostics, asserted in tests.
    #[cfg(test)]
    fn units(&self) -> usize {
        self.state.lock().unwrap().units
    }

    /// Units ever spawned.
    #[cfg(test)]
    fn spawned(&self) -> usize {
        self.state.lock().unwrap().spawned
    }

    /// Requests served by the pool.
    #[cfg(test)]
    fn served(&self) -> usize {
        self.state.lock().unwrap().served
    }

    /// Idle units.
    #[cfg(test)]
    fn idle(&self) -> usize {
        self.state.lock().unwrap().idle.len()
    }
}

/// Runs at unit exit, however it exits: frees the slot and, unless the unit
/// never got viable, spawns a replacement while the pool is below target.
struct Retire {
    pool: Arc<Pool>,
    /// False when the unit never served (e.g. its instantiate failed): the
    /// pool shrinks instead of respawning into the same failure.
    replace: bool,
}

impl Drop for Retire {
    fn drop(&mut self) {
        let respawn = {
            let mut state = self.pool.state.lock().unwrap();
            state.units -= 1;
            let respawn = self.replace && state.units < self.pool.sandbox.pool;
            if respawn {
                state.units += 1;
            }
            respawn
        };
        if respawn {
            Pool::spawn_unit(self.pool.clone());
        }
    }
}

/// The body of one pooled unit.
async fn run_unit(pool: Arc<Pool>) {
    let mut retire = Retire {
        pool: pool.clone(),
        replace: true,
    };
    let loaded = pool.loaded.clone();
    let budget = pool.sandbox.limits.budget;
    let mut store = wasmtime::Store::new(engine(), Ctx::new(&pool.sandbox, &loaded.label));
    store.limiter_async(|ctx| &mut ctx.limiter);
    // As in `respond`: the deadline must exist before any wasm runs.
    store.set_epoch_deadline(budget_ticks(budget));
    let service = match loaded.service_pre.instantiate_async(&mut store).await {
        Ok(service) => service,
        Err(e) => {
            eprintln!("[wasm] pooled instance of {} failed: {e}", loaded.label);
            retire.replace = false;
            return;
        }
    };

    // Offered only now: a request never queues behind an instantiate that
    // may fail.
    let (tx, mut rx) = mpsc::channel::<Job>(1);
    pool.state.lock().unwrap().idle.push(tx.clone());

    let (watch, watched) = watch::channel(None);
    let driven = store.run_concurrent(async move |accessor| {
        while let Some(job) = rx.recv().await {
            // The budget covers handler, request body drain and response
            // relay alike; re-armed per request on both clocks. A unit idle
            // past its budget just re-arms on the next job.
            watch.send_replace(Some(InFlight {
                reply: job.reply.clone(),
                deadline: tokio::time::Instant::now() + budget,
            }));
            accessor.with(|mut store| {
                store
                    .as_context_mut()
                    .set_epoch_deadline(budget_ticks(budget));
                store.data_mut().limiter.rearm();
            });

            let outcome = serve_on(
                accessor,
                &service,
                job.request,
                job.reply.head,
                job.reply.frames,
            )
            .await;
            // Releases the watchdog's hold on the reply: the response body
            // ends only once every frame sender is gone.
            watch.send_replace(None);

            let spent = match outcome {
                Outcome::Uncertain => Some("uncertain request"),
                Outcome::Clean => accessor.with(|mut store| store.data_mut().spent()),
            };
            if let Some(reason) = spent {
                eprintln!("[wasm] recycling {} pool unit: {reason}", pool.loaded.label);
                break;
            }
            pool.state.lock().unwrap().idle.push(tx.clone());
        }
    });

    supervise(&loaded.label, driven, watched).await;
    // `retire` drops here: the slot frees and a replacement spawns.
}

// ---------------------------------------------------------------------------
// Tunnel <-> hyper byte stream adapter

/// Serves one tunnel connection by feeding its frames through hyper and the
/// route's handler (shared across connections); http/1.1 and h2c are
/// auto-detected.
pub async fn serve(
    tx: mpsc::Sender<Frame>,
    registry: crate::Registry,
    conn_id: i64,
    handler: Arc<Handler>,
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
        let handler = handler.clone();
        async move { Ok::<_, std::convert::Infallible>(respond(&handler, req).await) }
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

    /// Builds the example guests (a no-op when fresh, serialized across
    /// tests) and returns the path of `name`'s component. The examples are
    /// their own workspace with a pinned nightly toolchain (wasm32-wasip3), so
    /// the build goes through the rustup proxy from that directory -- not
    /// `$CARGO`, and without the `RUSTUP_TOOLCHAIN` the outer build exports.
    fn guest_wasm(name: &str) -> std::path::PathBuf {
        static BUILD: Mutex<()> = Mutex::new(());
        let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("examples");
        let _serialized = BUILD.lock().unwrap_or_else(|e| e.into_inner());
        let output = std::process::Command::new("cargo")
            .args(["build", "--release"])
            .env_remove("RUSTUP_TOOLCHAIN")
            .env_remove("CARGO")
            .current_dir(&examples)
            .output()
            .expect("run cargo (rustup) for the guest build");
        assert!(
            output.status.success(),
            "guest build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let target = std::env::var("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| examples.join("target"));
        target.join(format!("wasm32-wasip3/release/{name}.wasm"))
    }

    fn hello_wasm() -> std::path::PathBuf {
        guest_wasm("hello")
    }

    /// Process-wide copy of every guest log line, so tests can assert on
    /// guest stdio (they filter by their own marker: tests run concurrently).
    pub(super) fn captured_log() -> &'static Mutex<Vec<u8>> {
        static CAPTURED: Mutex<Vec<u8>> = Mutex::new(Vec::new());
        &CAPTURED
    }

    /// Reference latencies for the instantiation paths, backing the decision
    /// recorded on `Loaded`. Run with:
    /// `cargo test -p sluice-wasmlet --bin sluice-wasmlet instantiation_latency -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "reference numbers; run manually with --nocapture"]
    async fn instantiation_latency_reference() {
        let bytes = std::fs::read(hello_wasm()).unwrap();
        let engine = engine().clone();
        let component = wasmtime::component::Component::new(&engine, &bytes).unwrap();
        let linker = linker(&engine, Caps::default()).unwrap();

        // full path: import resolution + instantiation, once per request
        async fn full<'a>(
            linker: &'a wasmtime::component::Linker<Ctx>,
            component: &'a wasmtime::component::Component,
        ) -> wasmtime::Result<()> {
            let mut store =
                wasmtime::Store::new(linker.engine(), Ctx::new(&Sandbox::default(), "bench"));
            store.set_epoch_deadline(u64::MAX / 2);
            let _ = wasmtime_wasi_http::p3::bindings::Service::instantiate_async(
                &mut store, component, linker,
            )
            .await?;
            Ok(())
        }
        // pre-linked path: instantiation only, what `respond` pays
        let pre = linker.instantiate_pre(&component).unwrap();
        async fn pre_linked(pre: &wasmtime::component::InstancePre<Ctx>) -> wasmtime::Result<()> {
            let mut store =
                wasmtime::Store::new(pre.engine(), Ctx::new(&Sandbox::default(), "bench"));
            store.set_epoch_deadline(u64::MAX / 2);
            let _ = pre.instantiate_async(&mut store).await?;
            Ok(())
        }

        let iterations = 200;
        for _ in 0..10 {
            full(&linker, &component).await.unwrap();
            pre_linked(&pre).await.unwrap();
        }
        let started = Instant::now();
        for _ in 0..iterations {
            full(&linker, &component).await.unwrap();
        }
        let full = started.elapsed() / iterations;
        let started = Instant::now();
        for _ in 0..iterations {
            pre_linked(&pre).await.unwrap();
        }
        let pre = started.elapsed() / iterations;
        println!(
            "full instantiate: {full:?} / pre-linked instantiate: {pre:?} (mean of {iterations})"
        );
    }

    /// Reference latency for the cold vs pooled request path, backing the
    /// numbers recorded on `Pool`. End-to-end over the in-process tunnel on
    /// the hello guest, sequential requests. Run with:
    /// `cargo test -p sluice-wasmlet --bin sluice-wasmlet load_comparison -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "reference numbers; run manually with --nocapture"]
    async fn load_comparison_reference() {
        let loaded = hello().await;
        let request = get("/");
        let (warmup, iterations) = (20u32, 200u32);
        let mean = async |handler: &Arc<Handler>| {
            for _ in 0..warmup {
                exchange(handler, &request).await;
            }
            let started = Instant::now();
            for _ in 0..iterations {
                exchange(handler, &request).await;
            }
            started.elapsed() / iterations
        };

        let cold = mean(&Handler::new(loaded.clone(), Sandbox::default())).await;
        let handler = pooled(&loaded, Sandbox::default(), 4);
        restocked(&handler, 4).await;
        let pooled = mean(&handler).await;
        assert_eq!(
            pool(&handler).served(),
            (warmup + iterations) as usize,
            "every measured request pooled"
        );

        println!(
            "cold: {cold:?} / pooled(4): {pooled:?} per request (mean of {iterations}, dev profile)"
        );
    }

    async fn hello() -> Arc<Loaded> {
        load(&Locator::File(hello_wasm()), Caps::default())
            .await
            .unwrap()
    }

    fn get(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
    }

    /// Opens a tunnel connection to `handler` and sends `request`: the
    /// connection's control side and the frames it emits.
    async fn open(
        handler: &Arc<Handler>,
        request: &str,
    ) -> (mpsc::Sender<Ctrl>, mpsc::Receiver<Frame>) {
        let (tx, rx) = mpsc::channel::<Frame>(64);
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<Ctrl>(32);
        let registry = crate::Registry::default();
        tokio::spawn(serve(tx, registry, 1, handler.clone(), ctrl_rx));
        ctrl_tx
            .send(Ctrl::Payload(request.as_bytes().to_vec()))
            .await
            .unwrap();
        (ctrl_tx, rx)
    }

    /// Drives one request through `serve` over in-memory frame channels and
    /// returns the raw response bytes up to the tunnel Close.
    async fn exchange_raw(handler: &Arc<Handler>, request: &str) -> Vec<u8> {
        let (_ctrl, mut rx) = open(handler, request).await;
        let mut raw = Vec::new();
        while let Some(frame) = rx.recv().await {
            match frame.body.expect("frame body") {
                Body::Data(data) => raw.extend_from_slice(&data.payload),
                Body::Close(..) => break,
                _ => {}
            }
        }
        assert!(!raw.is_empty(), "no response bytes");
        raw
    }

    /// `exchange_raw` for text responses.
    async fn exchange(handler: &Arc<Handler>, request: &str) -> String {
        String::from_utf8(exchange_raw(handler, request).await).expect("utf8 response")
    }

    /// A guest overrunning its epoch budget degrades to a clean 504 and the
    /// next request on the same component is unaffected.
    #[tokio::test]
    async fn budget_exhaustion_is_a_504_and_later_requests_keep_serving() {
        let loaded = load(&Locator::File(hello_wasm()), Caps::default())
            .await
            .unwrap();
        let sandbox = Sandbox {
            limits: Limits {
                budget: Duration::from_millis(100),
                ..Limits::default()
            },
            ..Sandbox::default()
        };
        let body = exchange(
            &Handler::new(loaded.clone(), sandbox),
            "GET /spin HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 504"), "head: {body}");
        assert!(body.contains("budget"), "body: {body}");

        let body = exchange(
            &Handler::new(loaded.clone(), Sandbox::default()),
            "GET / HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
    }

    /// A memory-hungry guest capped by the limiter degrades to a clean 5xx
    /// instead of OOM-ing the process, and later requests keep serving.
    #[tokio::test]
    async fn memory_cap_yields_a_clean_error_and_later_requests_keep_serving() {
        let loaded = load(&Locator::File(hello_wasm()), Caps::default())
            .await
            .unwrap();
        // above the guest's instantiation footprint, below its ballooning
        let sandbox = Sandbox {
            limits: Limits {
                memory: 8 << 20,
                ..Limits::default()
            },
            ..Sandbox::default()
        };
        let body = exchange(
            &Handler::new(loaded.clone(), sandbox),
            "GET /balloon HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 5"), "head: {body}");

        let body = exchange(
            &Handler::new(loaded.clone(), sandbox),
            "GET / HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
    }

    /// Drives `serve` over in-memory frame channels with a `GET /stream`
    /// request and collects the response: 10 MiB must arrive progressively
    /// (the guest paces chunks), not as one buffered blob.
    #[tokio::test]
    async fn streams_response_frames_as_the_guest_produces_them() {
        let wasm = hello_wasm();
        let loaded = load(&Locator::File(wasm), Caps::default()).await.unwrap();

        let (tx, mut rx) = mpsc::channel::<Frame>(64);
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<Ctrl>(32);
        let registry = crate::Registry::default();
        tokio::spawn(serve(
            tx,
            registry,
            1,
            Handler::new(loaded, Sandbox::default()),
            ctrl_rx,
        ));

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

    /// Guest stdout and stderr reach the process log as whole lines, each
    /// prefixed with the component and the request -- with no capability, as
    /// under `wasmtime serve`.
    #[tokio::test]
    async fn guest_stdio_lands_in_the_log_with_a_request_prefix() {
        let loaded = load(&Locator::File(hello_wasm()), Caps::default())
            .await
            .unwrap();
        let body = exchange(
            &Handler::new(loaded.clone(), Sandbox::default()),
            "GET /log HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");

        let log = String::from_utf8_lossy(&captured_log().lock().unwrap()).into_owned();
        let line = |stream: &str, text: &str| {
            log.lines()
                .find(|line| {
                    line.starts_with(&format!("{stream} [hello.wasm#")) && line.ends_with(text)
                })
                .map(str::to_string)
        };
        let stdout = line("stdout", "] :: hello stdout: Method::Get /log body_bytes=0")
            .unwrap_or_else(|| panic!("stdout line missing in: {log}"));
        let stderr = line("stderr", "] :: hello stderr: Method::Get /log")
            .unwrap_or_else(|| panic!("stderr line missing in: {log}"));
        // one request, one id
        let id = |line: &str| {
            line.split_once('#')
                .unwrap()
                .1
                .split_once(']')
                .unwrap()
                .0
                .to_string()
        };
        assert_eq!(id(&stdout), id(&stderr));
    }

    /// A guest writing without newlines is emitted in bounded pieces instead
    /// of buffering on the host; the remainder is flushed when the log drops.
    #[test]
    fn guest_log_bounds_an_unterminated_line() {
        let prefix = "stdout [unterminated-test] :: ";
        let log = GuestLog::new(prefix.into(), Output::Stdout);
        log.write(&vec![b'x'; GUEST_LOG_LINE_MAX + 10]).unwrap();
        assert_eq!(log.state.pending.lock().unwrap().len(), 0);
        log.write(b"tail").unwrap();
        drop(log);

        let captured = captured_log().lock().unwrap().clone();
        let lines: Vec<String> = String::from_utf8(captured)
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix(prefix).map(str::to_string))
            .collect();
        assert_eq!(
            lines,
            ["x".repeat(GUEST_LOG_LINE_MAX + 10), "tail".to_string()]
        );
    }

    /// A raw TCP upstream: reads each request head, answers `pong` (HTTP/1.0,
    /// read to EOF).
    async fn pong_upstream() -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !head.ends_with(b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                        }
                    }
                    let _ = socket.write_all(b"HTTP/1.0 200 OK\r\n\r\npong").await;
                });
            }
        });
        addr
    }

    fn relay_request(addr: std::net::SocketAddr) -> String {
        format!("GET /?addr={addr} HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
    }

    /// `cli,inherit-network` lets the guest dial a TCP upstream and compose
    /// its response from the reply.
    #[tokio::test]
    async fn granted_network_lets_the_guest_dial_a_tcp_upstream() {
        let caps = Caps::parse("cli,inherit-network").unwrap();
        let loaded = load(&Locator::File(guest_wasm("relay")), caps)
            .await
            .unwrap();
        let upstream = pong_upstream().await;
        let sandbox = Sandbox {
            caps,
            ..Sandbox::default()
        };
        let body = exchange(
            &Handler::new(loaded.clone(), sandbox),
            &relay_request(upstream),
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert!(
            body.contains(&format!("relayed from {upstream}")),
            "body: {body}"
        );
        assert!(body.contains("HTTP/1.0 200 OK\r\n\r\npong"), "body: {body}");
    }

    /// Without `inherit-network` the socket call is denied by the host: the
    /// guest gets `access-denied` (here: a 502 it renders), the tunnel
    /// connection and later requests are unaffected.
    #[tokio::test]
    async fn ungranted_network_is_denied_and_later_requests_keep_serving() {
        let upstream = pong_upstream().await;
        for granted in ["cli", "cli,tcp"] {
            let caps = Caps::parse(granted).unwrap();
            let loaded = load(&Locator::File(guest_wasm("relay")), caps)
                .await
                .unwrap();
            let sandbox = Sandbox {
                caps,
                ..Sandbox::default()
            };
            let body = exchange(
                &Handler::new(loaded.clone(), sandbox),
                &relay_request(upstream),
            )
            .await;
            assert!(body.starts_with("HTTP/1.1 502"), "{granted}: {body}");
            assert!(body.contains("AccessDenied"), "{granted}: {body}");
        }

        let caps = Caps::parse("cli,inherit-network").unwrap();
        let loaded = load(&Locator::File(guest_wasm("relay")), caps)
            .await
            .unwrap();
        let sandbox = Sandbox {
            caps,
            ..Sandbox::default()
        };
        let body = exchange(
            &Handler::new(loaded.clone(), sandbox),
            &relay_request(upstream),
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
    }

    /// A component importing one wasi interface of version `version`.
    fn importing_environment(version: &str) -> wasmtime::component::Component {
        let wat = format!(
            r#"(component
                 (import "wasi:cli/environment@{version}" (instance
                   (export "get-environment" (func (result (list (tuple string string)))))))
               )"#
        );
        wasmtime::component::Component::new(engine(), wat).unwrap()
    }

    /// wasi p2 is unsupported: no route links it, not even with `cli`, so a
    /// p2 import fails at startup while its p3 counterpart links.
    #[test]
    fn p2_imports_are_never_linked() {
        for caps in [Caps::default(), Caps::parse("cli").unwrap()] {
            let linker = linker(engine(), caps).unwrap();
            let p2 = linker.instantiate_pre(&importing_environment("0.2.6"));
            assert!(p2.is_err(), "{caps:?} linked wasi p2");
            linker
                .instantiate_pre(&importing_environment("0.3.0"))
                .unwrap_or_else(|e| panic!("{caps:?}: {e:#}"));
        }
    }

    /// Loading a p2-importing component names the cause instead of a missing
    /// link (or a misleading `wasi=cli` hint).
    #[tokio::test]
    async fn loading_a_p2_component_is_rejected_as_unsupported() {
        let path =
            std::env::temp_dir().join(format!("sluice-p2-import-{}.wat", std::process::id()));
        std::fs::write(
            &path,
            r#"(component
                 (import "wasi:cli/environment@0.2.6" (instance
                   (export "get-environment" (func (result (list (tuple string string)))))))
               )"#,
        )
        .unwrap();
        for caps in [Caps::default(), Caps::parse("cli").unwrap()] {
            let error = match load(&Locator::File(path.clone()), caps).await {
                Ok(_) => panic!("{caps:?} loaded a p2 component"),
                Err(error) => error,
            };
            assert!(
                error.contains("imports wasi:cli/environment@0.2.6"),
                "{error}"
            );
            assert!(error.contains("wasi p2 is unsupported"), "{error}");
        }
    }

    /// Without `cli` wasi:sockets is not linked: a component importing it
    /// fails at load (startup), as under `wasmtime serve` without `-Scli`.
    #[tokio::test]
    async fn a_socket_importing_component_needs_cli_to_load() {
        let error = match load(&Locator::File(guest_wasm("relay")), Caps::default()).await {
            Ok(_) => panic!("loaded without cli"),
            Err(error) => error,
        };
        assert!(error.contains("wasi:sockets/types@0.3.0"), "{error}");
        assert!(error.contains("wasi=cli"), "{error}");
    }

    fn fetch_request(url: &str) -> String {
        format!("GET /?url={url} HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
    }

    /// Outgoing http (wasi:http/client) needs no capability, as under
    /// `wasmtime serve`: the guest calls the upstream and composes its reply.
    #[tokio::test]
    async fn outgoing_http_needs_no_capability() {
        let loaded = load(&Locator::File(guest_wasm("fetch")), Caps::default())
            .await
            .unwrap();
        let upstream = pong_upstream().await;
        let url = format!("http://{upstream}/ping?x=1");
        let body = exchange(
            &Handler::new(loaded.clone(), Sandbox::default()),
            &fetch_request(&url),
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert!(
            body.contains(&format!("fetched {url} -> 200\n")),
            "body: {body}"
        );
        assert!(body.contains("pong"), "body: {body}");
    }

    /// A failing upstream (here: hang-up before any response) surfaces to the
    /// guest as an `ErrorCode` (rendered as a 502); later requests keep serving.
    #[tokio::test]
    async fn outgoing_http_failure_reaches_the_guest_and_later_requests_keep_serving() {
        let loaded = load(&Locator::File(guest_wasm("fetch")), Caps::default())
            .await
            .unwrap();
        // accepts and hangs up without a response
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hangup = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                drop(socket);
            }
        });
        let url = format!("http://{hangup}/");
        let body = exchange(
            &Handler::new(loaded.clone(), Sandbox::default()),
            &fetch_request(&url),
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 502"), "head: {body}");
        assert!(
            body.contains(&format!("fetch {url} failed: ErrorCode::")),
            "body: {body}"
        );

        let upstream = pong_upstream().await;
        let url = format!("http://{upstream}/");
        let body = exchange(
            &Handler::new(loaded.clone(), Sandbox::default()),
            &fetch_request(&url),
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
    }

    // -- instance pool ------------------------------------------------------

    /// A handler whose route keeps `units` warm instances.
    fn pooled(loaded: &Arc<Loaded>, sandbox: Sandbox, units: usize) -> Arc<Handler> {
        Handler::new(
            loaded.clone(),
            Sandbox {
                pool: units,
                ..sandbox
            },
        )
    }

    fn pool(handler: &Handler) -> &Pool {
        handler.pool.as_deref().expect("a pooled route")
    }

    /// Waits until `idle` units are offered (instantiated and between
    /// requests), so the next request is pooled rather than cold.
    async fn restocked(handler: &Handler, idle: usize) {
        let pool = pool(handler);
        let wait = async {
            while pool.idle() != idle {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .is_err()
        {
            panic!(
                "pool did not restock to {idle} idle: units={} idle={}",
                pool.units(),
                pool.idle()
            );
        }
    }

    /// Accepts connections and never answers them.
    async fn silent_upstream() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        addr
    }

    /// The warm path serves repeated requests through one unit: no
    /// instantiation per request (by construction the unit instantiates once),
    /// no recycling (one spawned unit despite the reuse checks), streaming
    /// included.
    #[tokio::test]
    async fn pooled_units_serve_repeated_requests_without_reinstantiating() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        for path in ["/stream", "/", "/"] {
            // The single unit is busy until its response is fully relayed
            // (the guest's closing trailer trails the last data frame).
            restocked(&handler, 1).await;
            let raw = exchange_raw(&handler, &get(path)).await;
            assert!(
                raw.starts_with(b"HTTP/1.1 200"),
                "head: {}",
                String::from_utf8_lossy(&raw)
            );
        }
        let pool = pool(&handler);
        assert_eq!(pool.served(), 3, "every request via the pool");
        assert_eq!(pool.units(), 1, "the unit stayed viable");
        assert_eq!(pool.spawned(), 1, "no unit was recycled");
    }

    /// A guest panic mid-request retires the unit; the pool replaces it and
    /// later requests on the route keep succeeding.
    #[tokio::test]
    async fn a_panicking_unit_is_replaced_and_requests_keep_serving() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;
        let body = exchange(&handler, &get("/panic")).await;
        assert!(body.starts_with("HTTP/1.1 5"), "head: {body}");

        restocked(&handler, 1).await;
        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(pool(&handler).served(), 2, "both requests pooled");
        assert_eq!(pool(&handler).spawned(), 2, "one replacement");
    }

    /// `/spin` and `/balloon` degrade to clean responses on pooled units
    /// under the route's own limits (a 100ms budget, an 8 MiB cap), and the
    /// replacement keeps serving.
    #[tokio::test]
    async fn pool_degrades_spin_and_balloon_cleanly() {
        let loaded = hello().await;
        let spin = pooled(
            &loaded,
            Sandbox {
                limits: Limits {
                    budget: Duration::from_millis(100),
                    ..Limits::default()
                },
                ..Sandbox::default()
            },
            1,
        );
        let balloon = pooled(
            &loaded,
            Sandbox {
                limits: Limits {
                    memory: 8 << 20,
                    ..Limits::default()
                },
                ..Sandbox::default()
            },
            1,
        );

        restocked(&spin, 1).await;
        let started = Instant::now();
        let body = exchange(&spin, &get("/spin")).await;
        assert!(body.starts_with("HTTP/1.1 504"), "head: {body}");
        assert!(body.contains("budget"), "body: {body}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the route's 100ms budget applied, not the default: {:?}",
            started.elapsed()
        );

        restocked(&balloon, 1).await;
        let body = exchange(&balloon, &get("/balloon")).await;
        assert!(body.starts_with("HTTP/1.1 5"), "head: {body}");

        for handler in [&spin, &balloon] {
            restocked(handler, 1).await;
            let body = exchange(handler, &get("/")).await;
            assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
            assert_eq!(pool(handler).served(), 2, "both requests pooled");
            assert_eq!(pool(handler).spawned(), 2, "one replacement");
        }
    }

    /// Beyond the pool's size requests fall back to per-request instantiation:
    /// a second concurrent connection is served while the single unit streams.
    #[tokio::test]
    async fn pool_exhaustion_falls_back_to_per_request_instantiation() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;

        // occupy the only unit for the ~1s the stream takes
        let streamed = handler.clone();
        let first = tokio::spawn(async move { exchange_raw(&streamed, &get("/stream")).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pool(&handler).idle(), 0, "the unit is busy streaming");

        // no stock: served on the cold path, still a clean 200
        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(
            pool(&handler).served(),
            1,
            "the fallback is not counted as pooled"
        );

        let first = first.await.unwrap();
        assert!(
            first.starts_with(b"HTTP/1.1 200"),
            "head: {}",
            String::from_utf8_lossy(&first)
        );
        assert!(first.len() > 1000, "streaming body arrived");
    }

    /// A job lost by its unit (dropped before it was served) fails with a
    /// 500 instead of leaving its client waiting.
    #[tokio::test]
    async fn a_job_lost_by_its_unit_fails_instead_of_hanging() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;
        // a unit that takes the job and dies with it
        let (tx, mut rx) = mpsc::channel::<Job>(1);
        pool(&handler).state.lock().unwrap().idle.push(tx);
        tokio::spawn(async move { drop(rx.recv().await) });

        let answered =
            tokio::time::timeout(Duration::from_secs(5), exchange(&handler, &get("/"))).await;
        let body = answered.expect("the lost job is answered");
        assert!(body.starts_with("HTTP/1.1 500"), "head: {body}");
    }

    /// A unit is offered only once instantiated: a request never queues
    /// behind an instantiate that fails (here: an instance above its 1 MiB
    /// cap), and a unit that never got viable shrinks the pool instead of
    /// respawning into the same failure.
    #[tokio::test]
    async fn a_unit_is_offered_only_once_instantiated() {
        let sandbox = Sandbox {
            limits: Limits {
                memory: 1 << 20,
                ..Limits::default()
            },
            ..Sandbox::default()
        };
        let handler = pooled(&hello().await, sandbox, 1);
        assert_eq!(pool(&handler).idle(), 0, "not offered before instantiation");

        let shrunk = async {
            while pool(&handler).units() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), shrunk)
            .await
            .expect("the unviable unit is not respawned");
        assert_eq!(pool(&handler).idle(), 0);
        assert_eq!(pool(&handler).spawned(), 1);

        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 500"), "head: {body}");
        assert!(body.contains("instantiate failed"), "body: {body}");
    }

    /// A guest suspended on a host future (an upstream that never answers)
    /// runs no wasm, so the epoch deadline cannot interrupt it: the host
    /// watchdog ends it at the budget with a 504, on both paths, and a pooled
    /// unit is replaced.
    #[tokio::test]
    async fn a_request_suspended_past_its_budget_is_a_504() {
        let loaded = load(&Locator::File(guest_wasm("fetch")), Caps::default())
            .await
            .unwrap();
        let silent = silent_upstream().await;
        let sandbox = Sandbox {
            limits: Limits {
                budget: Duration::from_millis(300),
                ..Limits::default()
            },
            ..Sandbox::default()
        };
        let cold = Handler::new(loaded.clone(), sandbox);
        let warm = pooled(&loaded, sandbox, 1);
        restocked(&warm, 1).await;

        for handler in [&cold, &warm] {
            let started = Instant::now();
            let answered = tokio::time::timeout(
                Duration::from_secs(5),
                exchange(handler, &fetch_request(&format!("http://{silent}/"))),
            )
            .await;
            let body = answered.expect("the budget ends a suspended guest");
            assert!(body.starts_with("HTTP/1.1 504"), "head: {body}");
            assert!(body.contains("budget"), "body: {body}");
            assert!(started.elapsed() < Duration::from_secs(3));
        }

        restocked(&warm, 1).await;
        let upstream = pong_upstream().await;
        let body = exchange(&warm, &fetch_request(&format!("http://{upstream}/"))).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(pool(&warm).served(), 2, "both requests pooled");
        assert_eq!(pool(&warm).spawned(), 2, "the stopped unit was replaced");
    }

    /// A client leaving while its guest is still in the handler cancels the
    /// request: the unit is stopped and replaced at once instead of staying
    /// busy until the budget.
    #[tokio::test]
    async fn a_client_leaving_mid_request_frees_its_unit() {
        let loaded = load(&Locator::File(guest_wasm("fetch")), Caps::default())
            .await
            .unwrap();
        let silent = silent_upstream().await;
        let handler = pooled(&loaded, Sandbox::default(), 1);
        restocked(&handler, 1).await;

        let (ctrl, frames) = open(&handler, &fetch_request(&format!("http://{silent}/"))).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(pool(&handler).idle(), 0, "the unit is busy");
        ctrl.send(Ctrl::Abort).await.unwrap();
        drop(frames);

        let started = Instant::now();
        restocked(&handler, 1).await;
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "freed well before the 10s budget: {:?}",
            started.elapsed()
        );
        assert_eq!(pool(&handler).spawned(), 2, "the unit was replaced");
    }

    /// A client leaving mid-stream retires the unit (its relay was cut short)
    /// and the replacement keeps serving.
    #[tokio::test]
    async fn a_client_leaving_mid_stream_retires_its_unit() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;

        let (ctrl, mut frames) = open(&handler, &get("/stream")).await;
        let mut received = 0;
        while received < 64 * 1024 {
            match frames.recv().await.and_then(|frame| frame.body) {
                Some(Body::Data(data)) => received += data.payload.len(),
                other => panic!("stream ended early: {other:?}"),
            }
        }
        ctrl.send(Ctrl::Abort).await.unwrap();
        drop(frames);

        restocked(&handler, 1).await;
        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(pool(&handler).spawned(), 2, "the unit was replaced");
    }

    /// A guest leaking a host handle leaves the resource table dirty: the
    /// request succeeds, the unit is retired and replaced.
    #[tokio::test]
    async fn a_unit_with_a_dirty_resource_table_is_retired() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;
        let body = exchange(&handler, &get("/leak-handle")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");

        restocked(&handler, 1).await;
        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(pool(&handler).served(), 2, "both requests pooled");
        assert_eq!(pool(&handler).spawned(), 2, "the dirty unit was replaced");
    }

    /// A guest leaking memory on every request is recycled before it reaches
    /// its cap: every request succeeds, as on the cold path, instead of one
    /// failing with a trap whenever a unit fills up.
    #[tokio::test]
    async fn a_unit_leaking_memory_is_recycled_before_its_cap() {
        let sandbox = Sandbox {
            limits: Limits {
                memory: 8 << 20,
                ..Limits::default()
            },
            ..Sandbox::default()
        };
        let handler = pooled(&hello().await, sandbox, 1);
        let requests = 12;
        for _ in 0..requests {
            restocked(&handler, 1).await;
            let body = exchange(&handler, &get("/leak")).await;
            assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        }
        assert_eq!(pool(&handler).served(), requests, "every request pooled");
        assert!(pool(&handler).spawned() > 1, "a filled unit was recycled");
    }
}
