//! Hosting of wasi:http 0.3 handler components for `wasm:` upstream routes.
//!
//! The tunnel connection's byte stream is served by hyper (http/1.1 vs h2c
//! sniffed from the first bytes); every request is handled by a fresh
//! instance of the guest component (or, with `pool=N`, a warm one) through
//! `wasmtime-wasi-http`'s p3 `Service.handle`. Instantiation is pre-linked
//! (`InstancePre`) and bounded by per-route epoch budgets and memory caps,
//! and the instances alive at once by the process-wide `Instances` limit.
//! Response bodies stream: the
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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};

use http_body_util::BodyExt as _;
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::{CancellationToken, DropGuard};
use wasmtime::AsContextMut as _;

use crate::tunnel::TunnelIo;

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
        std::thread::Builder::new()
            .name("wasm-epoch-ticker".into())
            .spawn(move || {
                // Without the ticker no budget ever fires: fail stop.
                run_guarded(
                    || {
                        run_ticker(
                            || ticker.increment_epoch(),
                            EpochSchedule::new(std::time::Instant::now(), EPOCH_TICK),
                            std::time::Instant::now,
                            |until| {
                                std::thread::sleep(
                                    until.saturating_duration_since(std::time::Instant::now()),
                                );
                                true
                            },
                        )
                    },
                    || {
                        eprintln!("[wasm] epoch ticker stopped: aborting");
                        std::process::abort()
                    },
                )
            })
            .expect("spawn epoch ticker");
        engine
    })
}

/// Most ticks one wake-up increments. A ticker that wakes later than this
/// (the process was stopped, e.g. a suspended laptop) forgets the excess
/// instead of replaying it, which would trap every guest whose deadline lies
/// within the stall at once; the cap is the longest lag a budget absorbs.
const MAX_CATCH_UP_TICKS: u64 = 100;

/// The ticker's fixed schedule: tick `n` is due at `start + n * tick`, so a
/// late wake-up does not push the later ticks back.
struct EpochSchedule {
    start: std::time::Instant,
    tick: std::time::Duration,
    done: u64,
}

impl EpochSchedule {
    fn new(start: std::time::Instant, tick: std::time::Duration) -> Self {
        EpochSchedule {
            start,
            tick,
            done: 0,
        }
    }

    /// The ticks to increment at `now`: those elapsed since the last call, at
    /// most `MAX_CATCH_UP_TICKS`; a longer backlog is dropped.
    fn due(&mut self, now: std::time::Instant) -> u64 {
        let elapsed =
            (now.saturating_duration_since(self.start).as_nanos() / self.tick.as_nanos()) as u64;
        let due = elapsed.saturating_sub(self.done);
        if due > MAX_CATCH_UP_TICKS {
            self.done = elapsed;
            MAX_CATCH_UP_TICKS
        } else {
            self.done += due;
            due
        }
    }

    /// When the next tick is due.
    fn next_due(&self) -> std::time::Instant {
        // In u128 nanoseconds: `Duration * u32` would wrap the tick count
        // after 2^32 ticks and put the next tick in the past.
        let nanos = self.tick.as_nanos() * u128::from(self.done + 1);
        self.start + std::time::Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// Calls `increment` (the engine's `increment_epoch`) from `schedule` until `sleep_until` returns false.
/// The clock and the sleep are parameters so tests can stall the thread.
fn run_ticker(
    mut increment: impl FnMut(),
    mut schedule: EpochSchedule,
    mut now: impl FnMut() -> std::time::Instant,
    mut sleep_until: impl FnMut(std::time::Instant) -> bool,
) {
    while sleep_until(schedule.next_due()) {
        for _ in 0..schedule.due(now()) {
            increment();
        }
    }
}

/// Runs `body`; if it panics, runs `on_death` (which should not return).
fn run_guarded(body: impl FnOnce(), on_death: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).is_err() {
        on_death();
    }
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
    /// The route's cap on the instance slots it holds at once, its pooled
    /// units included (`Handler::admit`); None leaves the route bounded by
    /// `Instances` alone.
    pub instances: Option<usize>,
}

impl Sandbox {
    /// Parses the route query
    /// (`budget-ms=N&instances=N&memory-mib=N&pool=N&wasi=cli,tcp`).
    pub fn parse(query: &str) -> Result<Sandbox, String> {
        let mut sandbox = Sandbox::default();
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!("expected key=value in '{query}'"));
            };
            let count = || -> Result<usize, String> {
                value
                    .parse()
                    .map_err(|_| format!("{key}: not a number '{value}'"))
            };
            if key == "wasi" {
                sandbox.caps = Caps::parse(value)?;
            } else if key == "pool" {
                sandbox.pool = count()?;
            } else if key == "instances" {
                sandbox.instances = match count()? {
                    0 => return Err("instances: must be at least 1".into()),
                    cap => Some(cap),
                };
            } else if !sandbox.limits.set(key, value)? {
                return Err(format!(
                    "unknown option '{key}' (budget-ms, instances, memory-mib, pool, wasi)"
                ));
            }
        }
        sandbox.check()?;
        Ok(sandbox)
    }

    /// Rejects a cap below the route's pool: every pooled unit holds one of
    /// the route's slots for its lifetime.
    fn check(&self) -> Result<(), String> {
        match self.instances {
            Some(cap) if cap < self.pool => Err(format!(
                "instances={cap} is below pool={}: every pooled unit holds one of the \
                 route's instance slots",
                self.pool
            )),
            _ => Ok(()),
        }
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
        if let Some(cap) = self.instances {
            pairs.push(format!("instances={cap}"));
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

/// Whether HTTP gives the response no body on the wire: a HEAD's (its
/// `content-length` describes the GET's), an informational status, a 204 or
/// a 304. hyper drops such a body without polling it, so it is neither
/// forwarded -- the guest's is drained instead -- nor read as a departure.
fn bodiless(head_only: bool, status: http::StatusCode) -> bool {
    head_only
        || status.is_informational()
        || status == http::StatusCode::NO_CONTENT
        || status == http::StatusCode::NOT_MODIFIED
}

/// Forwards guest body frames to hyper until either side ends. A body error
/// is relayed so hyper sees the truncation instead of a silent short body; a
/// gone client just ends the relay and the guest drains through the pipe.
/// Without `forward` (a bodiless response) nothing is sent: the frames are
/// consumed and a closed channel is no failure. False when the relay ended
/// early (the client disappeared before the body did): the serving store's
/// fate is then uncertain.
async fn relay(
    mut body: http_body_util::combinators::UnsyncBoxBody<bytes::Bytes, wasmtime_wasi_http::Error>,
    frame_tx: mpsc::Sender<RelayFrame>,
    forward: bool,
) -> bool {
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if forward && frame_tx.send(Ok(frame)).await.is_err() {
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

/// The budget left until `deadline`.
fn remaining(deadline: tokio::time::Instant) -> std::time::Duration {
    deadline.saturating_duration_since(tokio::time::Instant::now())
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
    // Captured before the conversion consumes the request: the drain decision.
    let head_only = request.method() == hyper::Method::HEAD;
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
                        let forward = !bodiless(head_only, parts.status);
                        let _ = head.send(Head::Response(parts)).await;
                        let ended = relay(body, frames, forward).await;
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
/// client, cancelled when hyper drops the response before its head, or its
/// body mid-stream (`Tracked`).
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
    /// The request was a HEAD: whatever the head says, hyper drops the body
    /// unread (`bodiless`).
    head_only: bool,
}

fn reply(head_only: bool) -> (Reply, Awaited) {
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
        head_only,
    };
    (reply, awaited)
}

impl Awaited {
    /// The hyper response: the head as known, a synthesized failure, or a
    /// generic 500 when none arrived (the serving store died before any
    /// head). A client leaving before the head cancels through the guard;
    /// past it, the departure is the tracked body's early drop (`Tracked`) --
    /// hyper's own body drops say nothing by themselves: a satisfied
    /// `content-length`, a trailers-only end, and a bodiless response's
    /// body, dropped unread.
    async fn response(mut self) -> hyper::Response<RelayBody> {
        let head = self.head.recv().await;
        let token = self.client.disarm();
        match head {
            Some(Head::Response(parts)) => {
                let bodiless = bodiless(self.head_only, parts.status);
                let expected = parts
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok());
                let body = Tracked {
                    frames: self.frames,
                    token,
                    sent: 0,
                    expected,
                    bodiless,
                    ended: false,
                };
                hyper::Response::from_parts(parts, body.boxed_unsync())
            }
            Some(Head::Failure(status, message)) => plain_response(status, message),
            None => plain_response(500, "wasm handler failed".into()),
        }
    }
}

/// The relayed body hyper consumes, telling a client's mid-body departure
/// from the drops hyper does on its own: on a connection's end or a reset,
/// which is a departure -- and once a declared `content-length` is satisfied,
/// or the response is bodiless and dropped unread, which is not. An early
/// drop fires the reply's token, and the watchdog reclaims the serving store
/// at once: the relay notices a gone client only at its next frame, which a
/// guest stalled mid-body never produces.
struct Tracked {
    frames: mpsc::Receiver<RelayFrame>,
    token: CancellationToken,
    /// Body bytes handed to hyper so far.
    sent: usize,
    /// The head's `content-length`, when declared.
    expected: Option<usize>,
    /// A bodiless response: hyper drops the body unread, never a departure.
    bodiless: bool,
    /// Whether the stream's end went through: the channel's close, an error
    /// frame, or the trailers -- the terminal frames. Past any of them a
    /// drop is hyper's own.
    ended: bool,
}

impl http_body::Body for Tracked {
    type Data = bytes::Bytes;
    type Error = RelayError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        match this.frames.poll_recv(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if frame.is_trailers() {
                    // The terminal frame; nothing follows but the close.
                    this.ended = true;
                } else if let Some(data) = frame.data_ref() {
                    this.sent += data.len();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.ended = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                this.ended = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        // A satisfied `content-length` leaves at most the trailers, and a
        // bodiless response is dropped unread by design: not departures, the
        // pool must not recycle over either.
        if self.ended || self.bodiless || self.expected.is_some_and(|len| self.sent >= len) {
            return;
        }
        self.token.cancel();
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

/// The `[wasm]` line for a store stopped under a client that went away: a
/// unit retired this way would otherwise churn invisibly, unlike the relay
/// path's recycle.
fn gone_stop(label: &str) {
    let line = format!("[wasm] {label} client went away: store stopped\n");
    #[cfg(test)]
    tests::captured_log()
        .lock()
        .unwrap()
        .extend_from_slice(line.as_bytes());
    eprint!("{line}");
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
            Stop::Gone => {
                gone_stop(label);
                return;
            }
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

/// A handler's stores alive, the most there ever were at once, and how many
/// were ever created.
#[derive(Default)]
struct Gauge {
    live: AtomicUsize,
    peak: AtomicUsize,
    created: AtomicUsize,
}

/// Marks a store on its handler's gauge from before its creation to the end
/// of its owning task: the store is dropped with that task, never past it.
struct LiveStore(Arc<Gauge>);

impl LiveStore {
    /// Marks a store on `gauge`.
    fn mark(gauge: &Arc<Gauge>) -> Self {
        let live = gauge.live.fetch_add(1, Ordering::Relaxed) + 1;
        gauge.peak.fetch_max(live, Ordering::Relaxed);
        gauge.created.fetch_add(1, Ordering::Relaxed);
        Self(gauge.clone())
    }
}

impl Drop for LiveStore {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The body of the 503 answering a request that found no instance slot
/// within its budget.
pub const INSTANCE_LIMIT_REACHED: &str = "wasm instance limit reached";

/// The guest instances alive at once over every route of the process: every
/// store -- a request's, or a pooled unit's for the unit's lifetime -- holds
/// one slot from before its creation until after it was dropped. A request
/// finding none free waits for one within its budget (`Handler::admit`); the
/// semaphore hands them out in request order. A route's `instances` cap
/// bounds its share of them (`Handler::slot`) but reserves none.
pub struct Instances {
    slots: Arc<tokio::sync::Semaphore>,
    max: usize,
}

/// One instance slot -- the process-wide one, plus the route's own under
/// its `instances` cap -- freed when dropped.
struct Slot {
    _route: Option<tokio::sync::OwnedSemaphorePermit>,
    _global: tokio::sync::OwnedSemaphorePermit,
}

impl Instances {
    /// The largest limit the semaphore takes.
    pub const MAX: usize = tokio::sync::Semaphore::MAX_PERMITS;

    pub fn new(max: usize) -> Arc<Instances> {
        Arc::new(Instances {
            slots: Arc::new(tokio::sync::Semaphore::new(max)),
            max,
        })
    }

    /// The slots one connection may hold at once (`Share`), announced as its
    /// h2 `max_concurrent_streams`: half the slots (at least one), so one
    /// connection cannot take every slot.
    pub fn streams_per_connection(&self) -> u32 {
        u32::try_from((self.max / 2).max(1)).unwrap_or(u32::MAX)
    }

    /// A new connection's share of the slots.
    fn share(&self) -> Arc<tokio::sync::Semaphore> {
        Arc::new(tokio::sync::Semaphore::new(
            self.streams_per_connection() as usize
        ))
    }

    /// Slots currently held.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.max - self.slots.available_permits()
    }
}

/// One of a connection's slots: held with the slot of every instance serving
/// the connection, until the request's store (or, on a pooled unit, its job)
/// is gone -- not just its h2 stream, which a reset ends while the instance
/// may still run.
type Share = tokio::sync::OwnedSemaphorePermit;

/// How a request got its instance, or why not.
#[expect(
    clippy::large_enum_variant,
    reason = "returned once per request and destructured at once; boxing would allocate on every request"
)]
enum Admission {
    /// Handed to an idle pooled unit.
    Pooled,
    /// A slot for a per-request instance.
    Cold(Job, Slot),
    /// No share, slot or unit within the budget.
    Full,
}

async fn respond(
    handler: &Handler,
    share: &Arc<tokio::sync::Semaphore>,
    request: hyper::Request<hyper::body::Incoming>,
) -> hyper::Response<RelayBody> {
    // The budget runs from the request's arrival: it covers the wait for an
    // instance, instantiation, handler and response relay alike.
    let deadline = tokio::time::Instant::now() + handler.sandbox.limits.budget;
    let (reply, awaited) = reply(request.method() == hyper::Method::HEAD);
    let (job, slot) = match handler.admit(request, &reply, share, deadline).await {
        Admission::Pooled => {
            // The unit's job holds the reply now: a job lost with its
            // unit must close the head channel, so the 500 below wins.
            drop(reply);
            return awaited.response().await;
        }
        Admission::Cold(job, slot) => (job, slot),
        Admission::Full => return plain_response(503, INSTANCE_LIMIT_REACHED.into()),
    };
    // The job's reply is the one the store task takes: no other head sender
    // may stay here, or a task dying before the head would leave the head
    // channel open, and `Awaited::response` would never fall back to its 500.
    drop(reply);
    let Job {
        request,
        reply,
        share,
        ..
    } = job;

    let (loaded, sandbox) = (&handler.loaded, handler.sandbox);
    // Declared after the slot and share, so dropped before them on every
    // path.
    let live = LiveStore::mark(&handler.live);
    let mut store = wasmtime::Store::new(engine(), Ctx::new(&sandbox, &loaded.label));
    store.limiter_async(|ctx| &mut ctx.limiter);
    // The deadline must exist before any wasm runs: with epoch interruption
    // enabled, a store without one traps on the first observed tick.
    store.set_epoch_deadline(budget_ticks(remaining(deadline)));
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
        // Locals drop in reverse: the store, then its mark, then its slots.
        let _slot = slot;
        let _share = share;
        let _live = live;
        let mut store = store;
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
    /// The process-wide instance slots every store of the route takes.
    instances: Arc<Instances>,
    /// The route's own slots under its `instances` cap, taken with every
    /// process-wide one; None without a cap.
    route: Option<Arc<tokio::sync::Semaphore>>,
    /// Backing for `live_stores`, marked by `LiveStore`.
    live: Arc<Gauge>,
}

impl Handler {
    /// Starts the route's pool when its sandbox names one; its units take
    /// their slots of `instances` (and of the route's cap) for good, so a
    /// pool larger than the slots left, or than the cap, fails here (at
    /// startup).
    pub fn new(
        loaded: Arc<Loaded>,
        sandbox: Sandbox,
        instances: &Arc<Instances>,
    ) -> Result<Arc<Handler>, String> {
        sandbox.check()?;
        let live = Arc::new(Gauge::default());
        let route = sandbox
            .instances
            .map(|cap| Arc::new(tokio::sync::Semaphore::new(cap)));
        let pool = match sandbox.pool {
            0 => None,
            units => {
                let free = instances.slots.available_permits();
                let globals = (0..units)
                    .map(|_| instances.slots.clone().try_acquire_owned().ok())
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        format!(
                            "pool={units} needs {units} instance slots, {free} left of \
                             --max-instances {}",
                            instances.max
                        )
                    })?;
                let slots = globals
                    .into_iter()
                    .map(|global| Slot {
                        _route: route.as_ref().map(|route| {
                            route
                                .clone()
                                .try_acquire_owned()
                                .expect("checked: the cap covers the pool")
                        }),
                        _global: global,
                    })
                    .collect();
                Some(Pool::new(loaded.clone(), sandbox, live.clone(), slots))
            }
        };
        Ok(Arc::new(Handler {
            live,
            loaded,
            sandbox,
            pool,
            instances: instances.clone(),
            route,
        }))
    }

    /// Waits for an instance slot: one of the route's under its cap first,
    /// then a process-wide one. In this order a request held back by its
    /// route's cap waits outside the process-wide queue, holding no slot
    /// another route could use; the route's own slot it may hold meanwhile
    /// only delays the route's later requests, which queue behind it anyway.
    async fn slot(&self) -> Slot {
        let route = match &self.route {
            Some(route) => Some(route.clone().acquire_owned().await.expect("never closed")),
            None => None,
        };
        let global = self
            .instances
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("never closed");
        Slot {
            _route: route,
            _global: global,
        }
    }

    /// The route's slots held under its cap: its pooled units' and its
    /// requests'; 0 without a cap.
    #[cfg(test)]
    fn route_held(&self) -> usize {
        match (&self.route, self.sandbox.instances) {
            (Some(route), Some(cap)) => cap - route.available_permits(),
            _ => 0,
        }
    }

    /// Finds the request an instance: first one of its connection's slots,
    /// then an idle pooled unit, else a slot for a per-request instance
    /// (`slot`: the route's under its cap, then a process-wide one). With
    /// none free it waits until `deadline` -- for the share, then for
    /// whichever comes first of a unit going idle and a slot freeing -- and
    /// keeps its place in the slots' queues meanwhile. While a route's units
    /// hold every one of its slots (a cap equal to the pool), its requests
    /// get none here and wait for a unit.
    async fn admit(
        &self,
        request: hyper::Request<hyper::body::Incoming>,
        reply: &Reply,
        share: &Arc<tokio::sync::Semaphore>,
        deadline: tokio::time::Instant,
    ) -> Admission {
        let expired = tokio::time::sleep_until(deadline);
        tokio::pin!(expired);
        let share = tokio::select! {
            biased;
            share = share.clone().acquire_owned() => share.expect("never closed"),
            () = &mut expired => return Admission::Full,
        };
        let mut job = Job {
            request,
            reply: reply.clone(),
            deadline,
            share,
        };
        let slot = self.slot();
        tokio::pin!(slot);
        loop {
            let Some(pool) = &self.pool else {
                return tokio::select! {
                    biased;
                    slot = &mut slot => Admission::Cold(job, slot),
                    () = &mut expired => Admission::Full,
                };
            };
            // Registered before the dispatch, so a unit offered in between
            // still wakes this request.
            let offered = pool.offered.notified();
            tokio::pin!(offered);
            offered.as_mut().enable();
            job = match pool.dispatch(job) {
                Ok(()) => return Admission::Pooled,
                Err(job) => job,
            };
            tokio::select! {
                biased;
                // a warm unit first: a slot acquired meanwhile stays in the
                // pinned future for the next round, or frees with it
                () = &mut offered => {}
                slot = &mut slot => return Admission::Cold(job, slot),
                () = &mut expired => return Admission::Full,
            }
        }
    }

    /// Stores whose owning task is alive: the pool's units and the
    /// per-request ones. Every reclaim -- a departed client, a stop,
    /// a completion -- is one mark fewer.
    #[cfg(test)]
    pub fn live_stores(&self) -> usize {
        self.live.live.load(Ordering::Relaxed)
    }

    /// The most stores alive at once so far.
    #[cfg(test)]
    fn peak_stores(&self) -> usize {
        self.live.peak.load(Ordering::Relaxed)
    }

    /// Stores ever created.
    #[cfg(test)]
    fn stores_created(&self) -> usize {
        self.live.created.load(Ordering::Relaxed)
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
    /// End of the request's budget, which started at its arrival.
    deadline: tokio::time::Instant,
    /// The connection's slot the request holds while it is served.
    share: Share,
}

/// A per-route pool of warm `(Store, Service)` units: each unit is a detached
/// task owning its store for its lifetime, running `run_concurrent` once
/// (which consumes the store) with a per-request job loop inside. A unit
/// serves one request at a time; the pool's size is the warm concurrency, and
/// requests beyond it fall back to the per-request path (`respond`) while an
/// instance slot is free (within the route's cap, which counts the units), so
/// the pool only ever improves the warm latency.
/// Measured on the hello guest (the ignored `load_comparison_reference`,
/// sequential requests, dev profile): ~380us cold vs ~120us pooled per
/// request. Every unit holds an instance slot for its lifetime, handed on to
/// its replacement.
///
/// Reuse assumes stateless / reentrant-safe guests: module state legitimately
/// persists across a unit's requests. A guest that misbehaves poisons its own
/// unit only: a trap, a failed conversion, an interrupted relay, an overrun
/// budget, a departed client or a spent store (`Ctx::spent`) retires it, and a
/// retired unit is replaced.
struct Pool {
    loaded: Arc<Loaded>,
    sandbox: Sandbox,
    /// The handler's gauge: every unit marks its store on it.
    live: Arc<Gauge>,
    state: Mutex<PoolState>,
    /// Signalled whenever a unit is offered, for requests waiting on an
    /// instance (`Handler::admit`).
    offered: tokio::sync::Notify,
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
    /// Spawns one unit per slot (`sandbox.pool` of them). Each instantiates
    /// asynchronously and is offered once ready; until then requests take
    /// the per-request path.
    fn new(loaded: Arc<Loaded>, sandbox: Sandbox, live: Arc<Gauge>, slots: Vec<Slot>) -> Arc<Self> {
        let pool = Arc::new(Self {
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                units: slots.len(),
                spawned: 0,
                served: 0,
            }),
            live,
            loaded,
            sandbox,
            offered: tokio::sync::Notify::new(),
        });
        for slot in slots {
            Self::spawn_unit(pool.clone(), slot);
        }
        pool
    }

    /// Starts one unit on `slot`, already counted in `units`.
    fn spawn_unit(pool: Arc<Pool>, slot: Slot) {
        pool.state.lock().unwrap().spawned += 1;
        tokio::spawn(run_unit(pool, slot));
    }

    /// Offers an idle unit and wakes a request waiting for an instance.
    fn offer(&self, unit: mpsc::Sender<Job>) {
        self.state.lock().unwrap().idle.push(unit);
        self.offered.notify_one();
    }

    /// Hands the job to an idle unit. `Err(job)` when none accepted it (no
    /// stock, or a unit vanished): the caller instantiates per request.
    #[expect(
        clippy::result_large_err,
        reason = "handed back to the caller, which waits or instantiates anyway"
    )]
    fn dispatch(&self, mut job: Job) -> Result<(), Job> {
        loop {
            let Some(tx) = self.state.lock().unwrap().idle.pop() else {
                return Err(job);
            };
            job = match tx.try_send(job) {
                Ok(()) => {
                    self.state.lock().unwrap().served += 1;
                    return Ok(());
                }
                // A unit died since it went idle; its entry is stale.
                Err(tokio::sync::mpsc::error::TrySendError::Full(job))
                | Err(tokio::sync::mpsc::error::TrySendError::Closed(job)) => job,
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

/// Runs at unit exit, however it exits, after the unit's store is gone:
/// frees the unit's place and, unless the unit never got viable, spawns a
/// replacement on its instance slot while the pool is below target; else the
/// slot frees.
struct Retire {
    pool: Arc<Pool>,
    /// False when the unit never served (e.g. its instantiate failed): the
    /// pool shrinks instead of respawning into the same failure.
    replace: bool,
    /// The unit's instance slot.
    slot: Option<Slot>,
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
            let slot = self.slot.take().expect("a unit holds its slot");
            Pool::spawn_unit(self.pool.clone(), slot);
        }
    }
}

/// The body of one pooled unit, on its instance slot.
async fn run_unit(pool: Arc<Pool>, slot: Slot) {
    // Declared first, so dropped last: after the store.
    let mut retire = Retire {
        pool: pool.clone(),
        replace: true,
        slot: Some(slot),
    };
    let loaded = pool.loaded.clone();
    let budget = pool.sandbox.limits.budget;
    let _live = LiveStore::mark(&pool.live);
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
    pool.offer(tx.clone());

    let (watch, watched) = watch::channel(None);
    let driven = store.run_concurrent(async move |accessor| {
        while let Some(job) = rx.recv().await {
            // The request's budget, from its arrival, covers handler,
            // request body drain and response relay alike; re-armed per
            // request on both clocks. A unit idle past a budget just
            // re-arms on the next job.
            watch.send_replace(Some(InFlight {
                reply: job.reply.clone(),
                deadline: job.deadline,
            }));
            accessor.with(|mut store| {
                store
                    .as_context_mut()
                    .set_epoch_deadline(budget_ticks(remaining(job.deadline)));
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
            pool.offer(tx.clone());
        }
    });

    supervise(&loaded.label, driven, watched).await;
    // `retire` drops here: the slot frees and a replacement spawns.
}

// ---------------------------------------------------------------------------
// Serving a tunnel connection

/// Serves one tunnel connection by feeding its frames through hyper and the
/// route's handler (shared across connections); http/1.1 and h2c are
/// auto-detected. An http/1.1 connection has one request in flight at a
/// time; an h2 one announces `Instances::streams_per_connection` streams,
/// and its requests hold at most that many instances at once.
///
/// The server's CLOSE (the public client's FIN) is a half-close: http/1.1
/// runs with hyper's `half_close`, so a request already received is still
/// answered. With it hyper does not read between a request's end and its
/// response's end, so the connection's failure (the server's ERROR, payload
/// past the window, the stream's end) is watched beside hyper and drops it
/// with the request in flight.
pub async fn serve(mut io: TunnelIo, handler: Arc<Handler>) {
    let conn_id = io.conn_id();
    let ending = io.ending();
    let failed = io.failed();

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

    let streams = handler.instances.streams_per_connection();
    let share = handler.instances.share();
    let service = hyper::service::service_fn(move |req| {
        let (handler, share) = (handler.clone(), share.clone());
        async move { Ok::<_, std::convert::Infallible>(respond(&handler, &share, req).await) }
    });
    let served = async {
        if h2 {
            hyper::server::conn::http2::Builder::new(TokioExecutor)
                .max_concurrent_streams(streams)
                .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                .await
        } else {
            hyper::server::conn::http1::Builder::new()
                .half_close(true)
                .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                .await
        }
    };
    tokio::select! {
        biased;
        served = served => {
            if let Err(e) = served {
                eprintln!("[wasm] conn {conn_id} serve error: {e}");
            }
        }
        // dropping hyper drops the request in flight
        () = failed => {}
    }
    ending.finish();
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

// ---------------------------------------------------------------------------
// Tests

#[cfg(test)]
pub(crate) mod tests {
    // -- epoch ticker ------------------------------------------------------

    const T: Duration = Duration::from_millis(10);

    /// Runs `run_ticker` against a virtual clock: each entry of `wakes` is the
    /// instant (ms after start) the thread actually wakes for one sleep, so a
    /// value past the due time is an oversleep. Returns the increments seen.
    fn ticks_after_wakes(wakes: &[u64]) -> Vec<u64> {
        let start = std::time::Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let mut queue = wakes.iter().copied();
        let clock = std::cell::Cell::new(start);
        let count = std::cell::Cell::new(0u64);
        let mut seen = Vec::new();
        let mut sleeps = 0;
        run_ticker(
            || count.set(count.get() + 1),
            EpochSchedule::new(start, T),
            || clock.get(),
            |_| match queue.next() {
                Some(ms) => {
                    if sleeps > 0 {
                        seen.push(count.get());
                    }
                    sleeps += 1;
                    clock.set(at(ms));
                    true
                }
                None => {
                    seen.push(count.get());
                    false
                }
            },
        );
        seen
    }

    /// A stalled ticker thread catches up: the epoch follows the wall clock
    /// (elapsed / tick) instead of losing the stalled ticks.
    #[test]
    fn a_stalled_ticker_catches_the_epoch_up_to_the_wall_clock() {
        // Wakes: on time (10ms), then stalled 5 ticks (60ms), then on time.
        let seen = ticks_after_wakes(&[10, 60, 70]);
        // seen is sampled before each following wake and at the end.
        assert_eq!(
            seen,
            vec![1, 6, 7],
            "epoch == elapsed / tick after each wake"
        );
    }

    /// Oversleeping every tick by a little does not push later ticks back:
    /// each sleep targets `start + n * tick`, and the count tracks elapsed.
    #[test]
    fn small_oversleeps_do_not_accumulate() {
        let start = std::time::Instant::now();
        let clock = std::cell::Cell::new(start);
        let count = std::cell::Cell::new(0u64);
        let mut targets = Vec::new();
        run_ticker(
            || count.set(count.get() + 1),
            EpochSchedule::new(start, T),
            || clock.get(),
            |target| {
                targets.push(target);
                // Every wake is 3ms late.
                clock.set(target + Duration::from_millis(3));
                targets.len() <= 100
            },
        );
        for (n, target) in targets.iter().enumerate() {
            assert_eq!(*target, start + T * (n as u32 + 1), "target {n}");
        }
        assert_eq!(count.get(), 100);
    }

    /// A stall longer than the cap (a suspended process) replays at most
    /// `MAX_CATCH_UP_TICKS`, drops the rest and resumes on the fixed schedule.
    #[test]
    fn a_huge_backlog_is_capped_and_the_schedule_resumes() {
        let start = std::time::Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let mut s = EpochSchedule::new(start, T);
        assert_eq!(s.due(at(3_600_000)), MAX_CATCH_UP_TICKS);
        // Nothing is owed right after, and the next tick is the next slot.
        assert_eq!(s.due(at(3_600_000)), 0);
        assert_eq!(s.next_due(), at(3_600_010));
        assert_eq!(s.due(at(3_600_010)), 1);
        // A backlog at the cap is replayed in full, not dropped.
        let mut s = EpochSchedule::new(start, T);
        assert_eq!(s.due(at(MAX_CATCH_UP_TICKS * 10)), MAX_CATCH_UP_TICKS);
        assert_eq!(s.due(at(MAX_CATCH_UP_TICKS * 10 + 10)), 1);
    }

    /// Past 2^32 ticks (about 497 days at 10ms) the next tick is still due one
    /// tick ahead, not wrapped back into the past (which would spin the ticker).
    #[test]
    fn the_schedule_stays_ahead_past_two_to_the_32_ticks() {
        let start = std::time::Instant::now();
        let s = EpochSchedule {
            start,
            tick: T,
            done: u64::from(u32::MAX),
        };
        let ticks = u64::from(u32::MAX) + 1;
        assert_eq!(
            s.next_due(),
            start + Duration::from_millis(10 * ticks),
            "tick 2^32 is due at start + 2^32 * tick"
        );
    }

    /// A ticker thread that panics must not die silently: budgets would never
    /// fire again.
    #[test]
    fn a_panicking_ticker_reports_its_death() {
        let died = std::cell::Cell::new(false);
        run_guarded(|| panic!("ticker bug"), || died.set(true));
        assert!(died.get());
        let died = std::cell::Cell::new(false);
        run_guarded(|| (), || died.set(true));
        assert!(!died.get());
    }

    use super::*;
    use crate::pb::frame::Body;
    use crate::tunnel::testing::Peer;
    use std::time::{Duration, Instant};

    /// Builds the example guests (a no-op when fresh, serialized across
    /// tests) and returns the path of `name`'s component. The examples are
    /// their own workspace with a pinned nightly toolchain (wasm32-wasip3), so
    /// the build goes through the rustup proxy from that directory -- not
    /// `$CARGO`, and without the `RUSTUP_TOOLCHAIN` the outer build exports.
    pub(crate) fn guest_wasm(name: &str) -> std::path::PathBuf {
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

    /// A route handler without an instance limit to speak of.
    fn route(loaded: Arc<Loaded>, sandbox: Sandbox) -> Arc<Handler> {
        Handler::new(loaded, sandbox, &Instances::new(Instances::MAX)).unwrap()
    }

    /// Process-wide copy of every log line, guest or host, so tests can
    /// assert on them (they filter by their own marker: tests run
    /// concurrently).
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

        let cold = mean(&route(loaded.clone(), Sandbox::default())).await;
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

    fn head(path: &str) -> String {
        format!("HEAD {path} HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
    }

    /// The raw bytes of a bodiless response: head only, nothing after the
    /// blank line (hyper drops such a body unread).
    fn assert_bare_head(raw: &[u8]) {
        let end = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("a response head")
            + 4;
        assert_eq!(raw.len(), end, "no body follows: {:?}", raw);
    }

    /// Dial address of the test route.
    const ROUTE: &str = "wasm:test";

    /// Opens conn 1 to `handler` through the dispatch, as a server CONNECT
    /// announcing its window, and sends `request`.
    fn open(handler: &Arc<Handler>, request: &str) -> Peer {
        let peer = Peer::new(HashMap::from([(ROUTE.to_string(), handler.clone())]));
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        peer.data(1, request.as_bytes());
        peer
    }

    /// Reads the response bytes up to the tunnel Close.
    async fn response_raw(peer: &mut Peer) -> Vec<u8> {
        let mut raw = Vec::new();
        while let Some(frame) = peer.recv().await {
            match frame.body.expect("frame body") {
                Body::Data(data) => raw.extend_from_slice(&data.payload),
                Body::Close(..) => break,
                _ => {}
            }
        }
        assert!(!raw.is_empty(), "no response bytes");
        raw
    }

    /// Drives one request through `serve` over in-memory frame channels and
    /// returns the raw response bytes up to the tunnel Close.
    async fn exchange_raw(handler: &Arc<Handler>, request: &str) -> Vec<u8> {
        response_raw(&mut open(handler, request)).await
    }

    /// `exchange_raw` for text responses.
    async fn exchange(handler: &Arc<Handler>, request: &str) -> String {
        String::from_utf8(exchange_raw(handler, request).await).expect("utf8 response")
    }

    /// The frames the wasmlet sends until none arrives for `quiet`.
    async fn frames_until_quiet(peer: &mut Peer, quiet: Duration) -> Vec<Body> {
        let mut frames = Vec::new();
        while let Ok(Some(frame)) = tokio::time::timeout(quiet, peer.recv()).await {
            frames.push(frame.body.expect("frame body"));
        }
        frames
    }

    fn data_bytes(frames: &[Body], conn_id: i64) -> Vec<u8> {
        frames
            .iter()
            .filter_map(|body| match body {
                Body::Data(data) if data.conn_id == conn_id => Some(data.payload.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect()
    }

    /// A CONNECT without a window (a server predating per-connection flow
    /// control) is answered with an ERROR naming the server build required,
    /// logged once, and neither served nor given a slot: the only slot
    /// still serves a windowed CONNECT after it.
    #[tokio::test]
    async fn a_connect_without_a_window_is_answered_with_an_error() {
        let handler = route(hello().await, Sandbox::default());
        let limit = crate::tunnel::Limit::new(1);
        let mut peer = Peer::limited(
            HashMap::from([(ROUTE.to_string(), handler.clone())]),
            limit.clone(),
        );
        peer.connect(1, ROUTE, 0);
        // DATA right behind it, as such a server sends: dropped
        peer.data(1, get("/").as_bytes());
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert!(
            matches!(&frames[..], [Body::Error(error)]
                if error.conn_id == 1 && error.message == crate::tunnel::SERVER_WINDOW_REQUIRED),
            "{frames:?}"
        );
        assert!(!peer.registered(1), "not registered");
        assert_eq!(limit.open(), 0, "no slot taken");
        assert_eq!(peer.connections.windowless_rejections(), 1, "logged once");

        peer.connect(2, ROUTE, crate::tunnel::WINDOW);
        peer.data(2, get("/").as_bytes());
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        let response = String::from_utf8(data_bytes(&frames, 2)).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(peer.connections.windowless_rejections(), 1);
    }

    /// `connection: close` ends the connection while the server may still
    /// send: CLOSE, then ERROR, so a server writer waiting for credit is
    /// released. After the server's CLOSE, CLOSE alone.
    #[tokio::test]
    async fn a_windowed_connection_ends_with_error_unless_the_server_closed() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = open(&handler, &get("/"));
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        let tail: Vec<_> = frames
            .iter()
            .filter(|body| !matches!(body, Body::Data(_)))
            .collect();
        assert!(
            matches!(tail[..], [Body::Close(_), Body::Error(error)]
                if error.message == crate::tunnel::CONNECTION_CLOSED),
            "{tail:?}"
        );

        // keep-alive: the response leaves the connection open until the
        // server's CLOSE
        let keep_alive = "GET / HTTP/1.1\r\nhost: t\r\n\r\n";
        let mut peer = open(&handler, keep_alive);
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert!(
            String::from_utf8(data_bytes(&frames, 1))
                .unwrap()
                .starts_with("HTTP/1.1 200")
        );
        assert!(
            frames.iter().all(|body| matches!(body, Body::Data(_))),
            "still open: {frames:?}"
        );
        peer.send(Body::Close(crate::pb::Close { conn_id: 1 }));
        let tail = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert!(matches!(tail[..], [Body::Close(_)]), "{tail:?}");
    }

    /// hyper skips its shutdown when the connection fails; the connection
    /// still ends with a terminal frame, or the server's side of it would
    /// wait forever.
    #[tokio::test]
    async fn a_connection_failing_in_hyper_still_ends_with_a_terminal_frame() {
        fn terminal(frames: &[Body]) -> Vec<String> {
            frames
                .iter()
                .filter_map(|body| match body {
                    Body::Close(_) => Some("CLOSE".to_string()),
                    Body::Error(error) => Some(format!("ERROR {}", error.message)),
                    _ => None,
                })
                .collect()
        }
        let handler = route(hello().await, Sandbox::default());
        // a head cut short by the server's CLOSE: hyper fails the read; the
        // server closed, so CLOSE is owed, not ERROR
        let mut peer = open(&handler, "GET / HTTP/1.1\r\nhost:");
        peer.send(Body::Close(crate::pb::Close { conn_id: 1 }));
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert_eq!(terminal(&frames), vec!["CLOSE"], "{frames:?}");
        // a response body cut short by the budget fails hyper while the
        // server may still send: ERROR alone, so the public peer sees an
        // abort, not a clean end
        let short = route(
            hello().await,
            Sandbox {
                limits: Limits {
                    budget: Duration::from_millis(300),
                    ..Limits::default()
                },
                ..Sandbox::default()
            },
        );
        let mut peer = open(&short, &get("/stream"));
        let frames = frames_until_quiet(&mut peer, Duration::from_secs(1)).await;
        let body = data_bytes(&frames, 1);
        assert!(
            body.len() < 10 * 1024 * 1024,
            "truncated: {} bytes",
            body.len()
        );
        assert_eq!(
            terminal(&frames),
            vec![format!("ERROR {}", crate::tunnel::CONNECTION_CLOSED)]
        );
    }

    /// The response stops at exactly the server's credit and resumes on its
    /// grant: hyper's writes never put more on the wire than granted.
    #[tokio::test]
    async fn a_windowed_response_stops_at_the_servers_credit() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = Peer::new(HashMap::from([(ROUTE.to_string(), handler.clone())]));
        peer.auto_grant = false;
        peer.connect(1, ROUTE, 64 * 1024);
        peer.data(1, get("/stream").as_bytes());

        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert_eq!(
            data_bytes(&frames, 1).len(),
            64 * 1024,
            "the initial credit"
        );

        peer.send(Body::WindowUpdate(crate::pb::WindowUpdate {
            conn_id: 1,
            increment: 100_000,
        }));
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert_eq!(data_bytes(&frames, 1).len(), 100_000, "exactly the grant");
    }

    /// A streaming response that filled its connection's queue delays
    /// another connection's response by one frame per frame of it, not by
    /// its backlog.
    #[tokio::test]
    async fn a_response_backlog_delays_another_connection_by_one_frame_per_frame() {
        use crate::tunnel::DATA_QUEUE_CAPACITY;
        async fn until(what: &str, condition: impl Fn() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !condition() {
                assert!(Instant::now() < deadline, "{what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let handler = route(hello().await, Sandbox::default());
        let mut peer = Peer::new(HashMap::from([(ROUTE.to_string(), handler.clone())]));
        peer.auto_grant = false;
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        peer.data(1, get("/stream").as_bytes());
        until("conn 1 fills its queue", || {
            peer.queued_data(1) == DATA_QUEUE_CAPACITY
        })
        .await;

        peer.connect(2, ROUTE, crate::tunnel::WINDOW);
        peer.data(2, get("/").as_bytes());
        until("conn 2's response is queued to its end", || {
            !peer.registered(2)
        })
        .await;
        assert_eq!(
            peer.queued_data(1),
            DATA_QUEUE_CAPACITY,
            "conn 1 still holds its backlog"
        );

        let mut order = Vec::new();
        while !order
            .iter()
            .any(|(conn_id, kind)| *conn_id == 2 && *kind == "ERROR")
        {
            let body = tokio::time::timeout(Duration::from_secs(1), peer.recv())
                .await
                .expect("frames flow")
                .unwrap()
                .body
                .unwrap();
            order.push(match body {
                Body::Data(data) => (data.conn_id, "DATA"),
                Body::Close(close) => (close.conn_id, "CLOSE"),
                Body::Error(error) => (error.conn_id, "ERROR"),
                other => panic!("unexpected {other:?}"),
            });
        }
        let conn2 = order.iter().filter(|(conn_id, _)| *conn_id == 2).count();
        let conn1 = order.len() - conn2;
        assert!(
            conn1 <= conn2 && conn1 < DATA_QUEUE_CAPACITY,
            "{conn1} of conn 1's queued frames went first: {order:?}"
        );
        let tail: Vec<_> = order.iter().filter(|(conn_id, _)| *conn_id == 2).collect();
        assert!(
            matches!(tail[..], [.., (2, "DATA"), (2, "CLOSE"), (2, "ERROR")]),
            "{tail:?}"
        );
    }

    /// What a credit-respecting server saw while uploading.
    #[derive(Default)]
    struct Upload {
        sent: usize,
        credit: u64,
        grants: usize,
        frames: Vec<Body>,
    }

    /// Uploads `head` plus `body_len` bytes to conn 1 as a server would:
    /// within the credit, which the wasmlet's grants refill. Stops once
    /// everything is sent, the wasmlet fails the connection, or nothing
    /// arrives for `quiet` while the credit is exhausted.
    async fn upload(peer: &mut Peer, head: &str, body_len: usize, quiet: Duration) -> Upload {
        let total = head.len() + body_len;
        let mut payload = head.as_bytes().to_vec();
        payload.resize(total, 0x42);
        let mut upload = Upload {
            credit: u64::from(crate::tunnel::WINDOW),
            ..Default::default()
        };
        while upload.sent < total {
            if upload.credit > 0 {
                let n = (total - upload.sent)
                    .min(64 * 1024)
                    .min(upload.credit as usize);
                peer.data(1, &payload[upload.sent..upload.sent + n]);
                upload.sent += n;
                upload.credit -= n as u64;
                continue;
            }
            let Ok(Some(frame)) = tokio::time::timeout(quiet, peer.recv()).await else {
                break; // no grant: the upload stalls here
            };
            let body = frame.body.expect("frame body");
            match &body {
                Body::WindowUpdate(update) if update.conn_id == 1 => {
                    upload.credit += u64::from(update.increment);
                    upload.grants += 1;
                }
                Body::Error(error) if error.conn_id == 1 => {
                    upload.frames.push(body);
                    break;
                }
                _ => {}
            }
            upload.frames.push(body);
        }
        upload
    }

    /// An upload beyond one window flows as the guest reads, on the grants
    /// hyper's reads produce. The guest stops reading past 1MiB and answers;
    /// the connection ends with ERROR, which releases the server's upload.
    #[tokio::test]
    async fn an_upload_flows_on_grants_and_an_unread_rest_is_released() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = Peer::new(HashMap::from([(ROUTE.to_string(), handler.clone())]));
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        let body_len = 4 * crate::tunnel::WINDOW as usize;
        let head = format!(
            "POST / HTTP/1.1\r\nhost: t\r\ncontent-length: {body_len}\r\nconnection: close\r\n\r\n"
        );

        let upload = tokio::time::timeout(
            Duration::from_secs(10),
            upload(&mut peer, &head, body_len, Duration::from_secs(5)),
        )
        .await
        .expect("the upload ends");

        assert!(upload.grants >= 1, "the guest's reads were granted");
        assert!(
            upload.sent > crate::tunnel::WINDOW as usize,
            "more than one window went out: {}",
            upload.sent
        );
        assert!(
            upload.sent < head.len() + body_len,
            "the rest was never read"
        );
        let response = String::from_utf8(data_bytes(&upload.frames, 1)).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let read: usize = response
            .lines()
            .find_map(|line| line.strip_prefix("body_bytes="))
            .expect("body_bytes")
            .parse()
            .unwrap();
        assert!(read > 1024 * 1024, "the guest read past one window: {read}");
        assert!(
            matches!(upload.frames.last(), Some(Body::Error(error))
                if error.message == crate::tunnel::CONNECTION_CLOSED),
            "the ERROR ended the upload: {:?}",
            upload.frames.last()
        );
    }

    /// One connection's upload stalls (its guest reads no more and the
    /// server has no credit left) while another connection on the same
    /// stream is served at once.
    #[tokio::test]
    async fn a_stalled_upload_leaves_other_connections_served() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = Peer::new(HashMap::from([(ROUTE.to_string(), handler.clone())]));
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        let body_len = 8 * crate::tunnel::WINDOW as usize;
        // the guest reads just past 1MiB, then answers /stall slowly
        let head = format!("POST /stall HTTP/1.1\r\nhost: t\r\ncontent-length: {body_len}\r\n\r\n");
        let stalled = upload(&mut peer, &head, body_len, Duration::from_millis(500)).await;
        assert_eq!(stalled.credit, 0, "the upload waits for credit");
        assert!(stalled.sent < head.len() + body_len);
        assert!(
            !stalled
                .frames
                .iter()
                .any(|body| matches!(body, Body::Close(_) | Body::Error(_))),
            "conn 1 is still open"
        );

        let started = Instant::now();
        peer.connect(2, ROUTE, crate::tunnel::WINDOW);
        peer.data(2, get("/").as_bytes());
        let mut frames = Vec::new();
        let served = async {
            while let Some(frame) = peer.recv().await {
                let body = frame.body.expect("frame body");
                let done = matches!(&body, Body::Close(close) if close.conn_id == 2);
                frames.push(body);
                if done {
                    break;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(3), served)
            .await
            .expect("conn 2 is served while conn 1 stalls");
        assert!(started.elapsed() < Duration::from_secs(3));
        let response = String::from_utf8(data_bytes(&frames, 2)).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(
            !frames.iter().any(|body| matches!(body,
                Body::Close(close) if close.conn_id == 1)),
            "conn 1 still stalls"
        );
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
            &route(loaded.clone(), sandbox),
            "GET /spin HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 504"), "head: {body}");
        assert!(body.contains("budget"), "body: {body}");

        let body = exchange(
            &route(loaded.clone(), Sandbox::default()),
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
            &route(loaded.clone(), sandbox),
            "GET /balloon HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 5"), "head: {body}");

        let body = exchange(
            &route(loaded.clone(), sandbox),
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

        let mut peer = open(
            &route(loaded, Sandbox::default()),
            "GET /stream HTTP/1.1\r\nhost: demo.local\r\nconnection: close\r\n\r\n",
        );

        let started = Instant::now();
        let mut raw: Vec<u8> = Vec::new();
        let mut head_end = None;
        let mut first_body_at = None;
        let mut closed_at = None;
        let completed = async {
            while closed_at.is_none() {
                let Some(frame) = peer.recv().await else {
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
            &route(loaded.clone(), Sandbox::default()),
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
        let body = exchange(&route(loaded.clone(), sandbox), &relay_request(upstream)).await;
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
            let body = exchange(&route(loaded.clone(), sandbox), &relay_request(upstream)).await;
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
        let body = exchange(&route(loaded.clone(), sandbox), &relay_request(upstream)).await;
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
            &route(loaded.clone(), Sandbox::default()),
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
            &route(loaded.clone(), Sandbox::default()),
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
            &route(loaded.clone(), Sandbox::default()),
            &fetch_request(&url),
        )
        .await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
    }

    // -- instance pool ------------------------------------------------------

    /// The tracked body's drop tells a departure from hyper's own drops: an
    /// endless, unsatisfied body fires the token; the stream's end, its
    /// error, a trailer or a satisfied `content-length` does not.
    #[tokio::test]
    async fn tracked_body_drop_tells_a_departure_from_the_benign_drops() {
        fn tracked(
            frames: mpsc::Receiver<RelayFrame>,
            token: &CancellationToken,
            sent: usize,
            expected: Option<usize>,
        ) -> Tracked {
            Tracked {
                frames,
                token: token.clone(),
                sent,
                expected,
                bodiless: false,
                ended: false,
            }
        }

        // the bare mid-body drop: a departure
        let (_, rx) = mpsc::channel::<RelayFrame>(1);
        let token = CancellationToken::new();
        drop(tracked(rx, &token, 0, None));
        assert!(token.is_cancelled(), "an endless body is a departure");

        // a satisfied `content-length`, trailers owed: hyper's own drop
        let (tx, rx) = mpsc::channel::<RelayFrame>(1);
        let token = CancellationToken::new();
        let mut body = tracked(rx, &token, 0, Some(3));
        tx.send(Ok(http_body::Frame::data(bytes::Bytes::from("abc"))))
            .await
            .unwrap();
        body.frame().await.unwrap().expect("the data frame");
        drop(body);
        assert!(!token.is_cancelled(), "a satisfied length is no departure");

        // a trailer: the terminal frame
        let (tx, rx) = mpsc::channel::<RelayFrame>(1);
        let token = CancellationToken::new();
        let mut body = tracked(rx, &token, 0, None);
        tx.send(Ok(http_body::Frame::trailers(http::HeaderMap::new())))
            .await
            .unwrap();
        body.frame().await.unwrap().expect("the trailer frame");
        drop(body);
        assert!(!token.is_cancelled(), "a trailed body is no departure");

        // a body error: the terminal frame
        let (tx, rx) = mpsc::channel::<RelayFrame>(1);
        let token = CancellationToken::new();
        let mut body = tracked(rx, &token, 0, None);
        tx.send(Err("wasm body failed".into())).await.unwrap();
        body.frame().await.expect("the error frame").unwrap_err();
        drop(body);
        assert!(!token.is_cancelled(), "an errored body is no departure");

        // the stream's end
        let (tx, rx) = mpsc::channel::<RelayFrame>(1);
        let token = CancellationToken::new();
        let mut body = tracked(rx, &token, 0, None);
        drop(tx);
        assert!(body.frame().await.is_none(), "the end");
        drop(body);
        assert!(!token.is_cancelled(), "an ended body is no departure");

        // a length declared but not reached: back to a departure
        let (tx, rx) = mpsc::channel::<RelayFrame>(1);
        let token = CancellationToken::new();
        let mut body = tracked(rx, &token, 0, Some(3));
        tx.send(Ok(http_body::Frame::data(bytes::Bytes::from("ab"))))
            .await
            .unwrap();
        body.frame().await.unwrap().expect("the short body frame");
        drop(body);
        assert!(token.is_cancelled(), "a short body is a departure");
    }

    /// A handler whose route keeps `units` warm instances.
    fn pooled(loaded: &Arc<Loaded>, sandbox: Sandbox, units: usize) -> Arc<Handler> {
        route(
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
        let cold = route(loaded.clone(), sandbox);
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

        let peer = open(&handler, &fetch_request(&format!("http://{silent}/")));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(pool(&handler).idle(), 0, "the unit is busy");
        peer.error(1, "public peer left");
        drop(peer);

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

        let mut peer = open(&handler, &get("/stream"));
        let mut received = 0;
        while received < 64 * 1024 {
            match peer.recv().await.and_then(|frame| frame.body) {
                Some(Body::Data(data)) => received += data.payload.len(),
                other => panic!("stream ended early: {other:?}"),
            }
        }
        peer.error(1, "public peer left");
        drop(peer);

        restocked(&handler, 1).await;
        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(pool(&handler).spawned(), 2, "the unit was replaced");
    }

    /// A `/stall` connection past its first body byte: the response head is
    /// out (the tracked body live) and the guest suspended before any further
    /// frame. Returns the connection's sides, for the abort.
    async fn stalled_connection(handler: &Arc<Handler>) -> Peer {
        let mut peer = open(handler, &get("/stall"));
        let head_and_chunk = async {
            let mut raw: Vec<u8> = Vec::new();
            loop {
                let frame = peer.recv().await.expect("the stalled response starts");
                if let Body::Data(data) = frame.body.expect("frame body") {
                    raw.extend_from_slice(&data.payload);
                }
                let Some(head_end) = raw
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|at| at + 4)
                else {
                    continue;
                };
                if raw.len() > head_end {
                    break; // the head is out with a body byte: mid-body now
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), head_and_chunk)
            .await
            .expect("the stalled body starts");
        peer
    }

    /// A client leaving mid-body frees its unit at once, though the guest is
    /// suspended and the relay sees no frame to fail on: the tracked body's
    /// early drop fires, well before the budget.
    #[tokio::test]
    async fn a_client_leaving_mid_body_frees_its_unit_before_the_budget() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;

        let peer = stalled_connection(&handler).await;
        assert_eq!(
            pool(&handler).idle(),
            0,
            "the unit is serving the stalled body"
        );

        let started = Instant::now();
        peer.error(1, "public peer left");
        drop(peer);

        restocked(&handler, 1).await;
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "freed well before the 10s budget: {:?}",
            started.elapsed()
        );
        assert_eq!(pool(&handler).spawned(), 2, "the unit was replaced");

        let log = String::from_utf8_lossy(&captured_log().lock().unwrap()).into_owned();
        assert!(
            log.contains("[wasm] hello.wasm client went away: store stopped"),
            "the stop is logged like any other recycle: {log}"
        );
    }

    /// A HEAD response: hyper drops the declared body unread, so the unit
    /// survives only if that is no departure -- the guest's body is drained,
    /// never forwarded, and no reinstantiation happens.
    #[tokio::test]
    async fn a_head_response_leaves_its_unit_reusable() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        for _ in 0..3 {
            restocked(&handler, 1).await;
            let raw = exchange_raw(&handler, &head("/")).await;
            assert!(
                raw.starts_with(b"HTTP/1.1 200"),
                "head: {}",
                String::from_utf8_lossy(&raw)
            );
            assert!(
                raw.windows(b"text/plain".len())
                    .any(|window| window == b"text/plain"),
                "the head declares a body: {}",
                String::from_utf8_lossy(&raw)
            );
            assert_bare_head(&raw);
        }
        let pool = pool(&handler);
        assert_eq!(pool.served(), 3, "every head via the pool");
        assert_eq!(pool.spawned(), 1, "no unit was recycled");
    }

    /// A 204 and a 304: their body hyper drops unread as well; the unit
    /// survives both, the responses bare heads.
    #[tokio::test]
    async fn a_204_and_a_304_leave_the_unit_reusable() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        for (path, status) in [
            ("/no-content", "HTTP/1.1 204"),
            ("/not-modified", "HTTP/1.1 304"),
        ] {
            restocked(&handler, 1).await;
            let raw = exchange_raw(&handler, &head(path)).await;
            assert!(
                raw.starts_with(status.as_bytes()),
                "{status}: {}",
                String::from_utf8_lossy(&raw)
            );
            assert_bare_head(&raw);
        }
        let pool = pool(&handler);
        assert_eq!(pool.served(), 2, "both via the pool");
        assert_eq!(pool.spawned(), 1, "no unit was recycled");
    }

    /// The same departure on the cold path: the per-request store is freed
    /// well before its budget, and the handler keeps serving.
    #[tokio::test]
    async fn a_client_leaving_mid_body_frees_the_cold_store_before_the_budget() {
        let handler = route(hello().await, Sandbox::default());
        assert_eq!(
            handler.live_stores(),
            0,
            "no store before the first request"
        );

        let peer = stalled_connection(&handler).await;
        assert_eq!(handler.live_stores(), 1, "the request's store is held");

        let started = Instant::now();
        peer.error(1, "public peer left");
        drop(peer);

        let freed = async {
            while handler.live_stores() > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(3), freed)
            .await
            .expect("the store is freed well before the budget");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "freed within the wait: {:?}",
            started.elapsed()
        );

        let body = tokio::time::timeout(Duration::from_secs(5), exchange(&handler, &get("/")))
            .await
            .expect("the handler keeps serving");
        assert!(body.starts_with("HTTP/1.1 200"), "head: {body}");
        assert_eq!(handler.live_stores(), 0, "the follow-up store left too");
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

    /// A CONNECT beyond the connection limit is answered with an ERROR and
    /// never served; the slot frees once the admitted connection ends.
    #[tokio::test]
    async fn a_connect_beyond_the_limit_is_refused_until_a_slot_frees() {
        let handler = route(hello().await, Sandbox::default());
        let limit = crate::tunnel::Limit::new(1);
        let mut peer = Peer::limited(
            HashMap::from([(ROUTE.to_string(), handler.clone())]),
            limit.clone(),
        );
        // admitted, and held open: no request yet
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        assert_eq!(limit.open(), 1);

        peer.connect(2, ROUTE, crate::tunnel::WINDOW);
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(300)).await;
        assert!(
            matches!(&frames[..], [Body::Error(error)]
                if error.conn_id == 2 && error.message == crate::tunnel::LIMIT_REACHED),
            "{frames:?}"
        );
        assert_eq!(limit.open(), 1, "the refused connection holds no slot");

        // the server fails the admitted connection: its slot frees once the
        // connection ended and its CLOSE left
        peer.error(1, "gone");
        let deadline = Instant::now() + Duration::from_secs(5);
        while peer.registered(1) {
            assert!(Instant::now() < deadline, "the connection never ended");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(limit.open(), 1, "held while its CLOSE is queued");
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(300)).await;
        assert!(
            matches!(&frames[..], [Body::Close(close)] if close.conn_id == 1),
            "{frames:?}"
        );
        assert_eq!(limit.open(), 0, "freed once the CLOSE left");

        peer.connect(3, ROUTE, crate::tunnel::WINDOW);
        peer.data(3, get("/").as_bytes());
        let response = String::from_utf8(response_raw(&mut peer).await).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    /// The server's CLOSE: the public client half-closed (a FIN).
    fn half_close(peer: &Peer) {
        peer.send(Body::Close(crate::pb::Close { conn_id: 1 }));
    }

    /// The connection-end frames among `frames`.
    fn terminal_frames(frames: &[Body]) -> Vec<String> {
        frames
            .iter()
            .filter_map(|body| match body {
                Body::Close(_) => Some("CLOSE".to_string()),
                Body::Error(error) => Some(format!("ERROR {}", error.message)),
                _ => None,
            })
            .collect()
    }

    /// Polls `condition` every 5ms until it holds; panics past `within`.
    async fn eventually(what: &str, within: Duration, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + within;
        while !condition() {
            assert!(Instant::now() < deadline, "{what} within {within:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A public client that half-closes after its request (the server's
    /// CLOSE right behind the request) still gets the response.
    #[tokio::test]
    async fn a_request_followed_by_the_servers_half_close_is_answered() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = open(&handler, &get("/"));
        peer.send(Body::Close(crate::pb::Close { conn_id: 1 }));
        let response = String::from_utf8(response_raw(&mut peer).await).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    /// The half-close answered in full, keep-alive or not, and the
    /// connection then ends with CLOSE alone (the server closed: no ERROR
    /// owed) and frees its slot.
    #[tokio::test]
    async fn a_half_closed_connection_gets_the_whole_response_then_closes() {
        let handler = route(hello().await, Sandbox::default());
        let keep_alive = "GET / HTTP/1.1\r\nhost: t\r\n\r\n";
        for request in [get("/"), keep_alive.to_string()] {
            let limit = crate::tunnel::Limit::new(1);
            let mut peer = Peer::limited(
                HashMap::from([(ROUTE.to_string(), handler.clone())]),
                limit.clone(),
            );
            peer.connect(1, ROUTE, crate::tunnel::WINDOW);
            peer.data(1, request.as_bytes());
            half_close(&peer);
            let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
            let response = String::from_utf8(data_bytes(&frames, 1)).unwrap();
            assert!(
                response.starts_with("HTTP/1.1 200")
                    && response.ends_with("body_bytes=0\n\r\n0\r\n\r\n"),
                "{request:?}: {response}"
            );
            assert_eq!(terminal_frames(&frames), vec!["CLOSE"], "{request:?}");
            assert_eq!(limit.open(), 0, "{request:?}: slot freed");
        }
    }

    /// A long streamed response is not cut by the half-close that arrived
    /// before its first byte.
    #[tokio::test]
    async fn a_streamed_response_survives_the_servers_half_close() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = open(&handler, &get("/stream"));
        half_close(&peer);
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        let raw = data_bytes(&frames, 1);
        let head_end = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("a response head")
            + 4;
        assert!(raw.starts_with(b"HTTP/1.1 200"));
        assert_eq!(raw.len() - head_end, 10 * 1024 * 1024);
        assert_eq!(terminal_frames(&frames), vec!["CLOSE"]);
    }

    /// A request body cut short by the half-close ends the connection with
    /// CLOSE and frees its slot: the truncation is no reason to wait.
    #[tokio::test]
    async fn a_request_body_cut_short_by_the_half_close_ends_the_connection() {
        let handler = route(hello().await, Sandbox::default());
        let post = "POST / HTTP/1.1\r\nhost: t\r\ncontent-length: 100\r\n\r\n0123456789";
        let limit = crate::tunnel::Limit::new(1);
        let mut peer = Peer::limited(
            HashMap::from([(ROUTE.to_string(), handler.clone())]),
            limit.clone(),
        );
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        peer.data(1, post.as_bytes());
        half_close(&peer);
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(500)).await;
        assert_eq!(terminal_frames(&frames), vec!["CLOSE"]);
        eventually("the slot frees", Duration::from_secs(1), || {
            limit.open() == 0
        })
        .await;
    }

    /// A half-close is no departure: the stalled request keeps its unit.
    /// The server's ERROR after it (the public client gone) still frees the
    /// unit at once, though hyper is not reading the connection.
    #[tokio::test]
    async fn a_client_leaving_after_its_half_close_frees_its_unit() {
        let handler = pooled(&hello().await, Sandbox::default(), 1);
        restocked(&handler, 1).await;

        let peer = stalled_connection(&handler).await;
        half_close(&peer);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            pool(&handler).idle(),
            0,
            "still serving after the half-close"
        );
        assert!(peer.registered(1), "the connection is still served");

        let started = Instant::now();
        peer.error(1, "public peer left");
        // the peer (and so the stream) stays up: the ERROR alone frees it
        restocked(&handler, 1).await;
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "freed well before the 10s budget: {:?}",
            started.elapsed()
        );
        assert_eq!(pool(&handler).spawned(), 2, "the unit was replaced");
        drop(peer);
    }

    /// A half-closed connection whose guest stalls is held no longer than
    /// the budget: the truncated response ends it with CLOSE.
    #[tokio::test]
    async fn a_half_closed_stalled_request_ends_at_the_budget() {
        let budget = Duration::from_millis(1500);
        let handler = route(
            hello().await,
            Sandbox {
                limits: Limits {
                    budget,
                    ..Limits::default()
                },
                ..Sandbox::default()
            },
        );
        let started = Instant::now();
        let mut peer = stalled_connection(&handler).await;
        half_close(&peer);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.live_stores(), 1, "held after the half-close");
        let end = tokio::time::timeout(budget + Duration::from_secs(2), async {
            loop {
                match peer.recv().await.and_then(|frame| frame.body) {
                    Some(Body::Data(_)) => {}
                    other => return other,
                }
            }
        })
        .await
        .expect("the connection ends at the budget");
        assert!(matches!(end, Some(Body::Close(_))), "{end:?}");
        assert!(
            started.elapsed() >= budget,
            "not before the budget: {:?}",
            started.elapsed()
        );
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(300)).await;
        assert!(frames.is_empty(), "nothing after the CLOSE: {frames:?}");
        assert_eq!(handler.live_stores(), 0, "the store left with the budget");
    }

    /// The server's ERROR alone (the stream staying up) frees a stalled
    /// request's store; it is not the stream's teardown that does it.
    #[tokio::test]
    async fn the_servers_error_alone_frees_a_stalled_store() {
        let handler = route(hello().await, Sandbox::default());
        let peer = stalled_connection(&handler).await;
        assert_eq!(handler.live_stores(), 1, "the request's store is held");
        peer.error(1, "public peer left");
        eventually("the store is freed", Duration::from_secs(3), || {
            handler.live_stores() == 0
        })
        .await;
        eventually("the connection ends", Duration::from_secs(1), || {
            !peer.registered(1)
        })
        .await;
        drop(peer);
    }

    /// The stream ending mid-request frees the stalled request's store
    /// before the budget.
    #[tokio::test]
    async fn the_streams_end_frees_a_stalled_store() {
        let handler = route(hello().await, Sandbox::default());
        let peer = stalled_connection(&handler).await;
        assert_eq!(handler.live_stores(), 1, "the request's store is held");
        peer.connections.teardown();
        eventually("the store is freed", Duration::from_secs(3), || {
            handler.live_stores() == 0
        })
        .await;
        drop(peer);
    }

    /// Payload past the window while hyper is not reading (the request is
    /// complete, its response stalled) fails the connection and frees the
    /// store before the budget.
    #[tokio::test]
    async fn payload_past_the_window_frees_a_stalled_store() {
        let handler = route(hello().await, Sandbox::default());
        let mut peer = stalled_connection(&handler).await;
        peer.data(1, &vec![0x5a; crate::tunnel::WINDOW as usize + 1]);
        eventually("the store is freed", Duration::from_secs(3), || {
            handler.live_stores() == 0
        })
        .await;
        let frames = frames_until_quiet(&mut peer, Duration::from_millis(300)).await;
        assert_eq!(
            terminal_frames(&frames),
            vec![format!("ERROR {}", crate::tunnel::WINDOW_EXCEEDED)],
            "{frames:?}"
        );
    }

    // -- instance limit -----------------------------------------------------

    /// A route on `instances`, with `budget` (the default when None).
    fn limited(
        loaded: &Arc<Loaded>,
        instances: &Arc<Instances>,
        budget: Option<Duration>,
    ) -> Arc<Handler> {
        let mut sandbox = Sandbox::default();
        if let Some(budget) = budget {
            sandbox.limits.budget = budget;
        }
        Handler::new(loaded.clone(), sandbox, instances).unwrap()
    }

    /// Waits for the end of conn 1's response: the HTTP response, or None
    /// when nothing arrives within `within`.
    async fn answered(peer: &mut Peer, within: Duration) -> Option<String> {
        tokio::time::timeout(within, response_raw(peer))
            .await
            .ok()
            .map(|raw| String::from_utf8_lossy(&raw).into_owned())
    }

    #[test]
    fn half_the_instances_are_one_connections_streams() {
        let streams = |max| Instances::new(max).streams_per_connection();
        assert_eq!(streams(1), 1);
        assert_eq!(streams(2), 1);
        assert_eq!(streams(3), 1);
        assert_eq!(streams(64), 32);
        assert_eq!(streams(Instances::MAX), u32::MAX);
    }

    /// With every slot held, a further request waits -- no store is created
    /// for it -- and is served once a slot frees, in arrival order.
    #[tokio::test]
    async fn a_request_beyond_the_instance_limit_waits_for_a_slot() {
        let instances = Instances::new(2);
        let handler = limited(&hello().await, &instances, None);
        let first = stalled_connection(&handler).await;
        let second = stalled_connection(&handler).await;
        assert_eq!(instances.held(), 2, "both slots held");

        // the third holds the slot it gets (a stalled body), the fourth not
        let mut third = open(&handler, &get("/stall"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut fourth = open(&handler, &get("/"));
        let waiting = frames_until_quiet(&mut third, Duration::from_millis(300)).await;
        assert!(waiting.is_empty(), "{waiting:?}");
        assert_eq!(
            answered(&mut fourth, Duration::from_millis(100)).await,
            None
        );
        assert_eq!(
            handler.live_stores(),
            2,
            "no store for the waiting requests"
        );

        first.error(1, "public peer left");
        let head = tokio::time::timeout(Duration::from_secs(3), third.recv())
            .await
            .expect("the first waiter takes the freed slot");
        assert!(
            matches!(head.and_then(|frame| frame.body), Some(Body::Data(data))
                if data.payload.starts_with(b"HTTP/1.1 200")),
            "the third's response starts"
        );
        assert_eq!(
            answered(&mut fourth, Duration::from_millis(300)).await,
            None,
            "the later waiter still waits: one slot freed"
        );

        second.error(1, "public peer left");
        let response = answered(&mut fourth, Duration::from_secs(3)).await;
        assert!(
            response
                .as_deref()
                .is_some_and(|r| r.starts_with("HTTP/1.1 200")),
            "{response:?}"
        );
        assert_eq!(handler.peak_stores(), 2, "never more stores than slots");
        assert_eq!(handler.stores_created(), 4);
        third.error(1, "public peer left");
        eventually("every slot frees", Duration::from_secs(3), || {
            instances.held() == 0
        })
        .await;
    }

    /// A request finding no slot within its budget is answered 503 at the
    /// budget, without ever getting an instance.
    #[tokio::test]
    async fn a_request_finding_no_slot_within_its_budget_is_a_503() {
        let loaded = hello().await;
        let instances = Instances::new(1);
        let holder = limited(&loaded, &instances, None);
        let budget = Duration::from_millis(800);
        let waiter = limited(&loaded, &instances, Some(budget));
        let occupant = stalled_connection(&holder).await;

        let started = Instant::now();
        let mut peer = open(&waiter, &get("/"));
        let response = answered(&mut peer, budget + Duration::from_secs(2))
            .await
            .expect("answered at the budget");
        let elapsed = started.elapsed();
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.contains(INSTANCE_LIMIT_REACHED), "{response}");
        assert!(elapsed >= budget, "not before the budget: {elapsed:?}");
        assert!(elapsed < budget + Duration::from_secs(1), "{elapsed:?}");
        assert_eq!(waiter.stores_created(), 0, "the waiter got no instance");
        assert_eq!(instances.held(), 1, "the occupant still holds its slot");
        drop(occupant);
    }

    /// The wait counts against the request's budget: admitted late -- to a
    /// per-request instance, or to a pooled unit -- the request runs only for
    /// what is left of it.
    #[tokio::test]
    async fn the_wait_for_a_slot_counts_against_the_budget() {
        let loaded = hello().await;
        let budget = Duration::from_millis(1500);
        for pooled in [false, true] {
            let instances = Instances::new(1);
            let mut sandbox = Sandbox::default();
            sandbox.limits.budget = budget;
            let (holder, waiter) = if pooled {
                sandbox.pool = 1;
                let handler = Handler::new(loaded.clone(), sandbox, &instances).unwrap();
                restocked(&handler, 1).await;
                (handler.clone(), handler)
            } else {
                (
                    limited(&loaded, &instances, None),
                    Handler::new(loaded.clone(), sandbox, &instances).unwrap(),
                )
            };
            let occupant = stalled_connection(&holder).await;
            let created = waiter.stores_created();

            let started = Instant::now();
            let mut peer = open(&waiter, &get("/stall"));
            tokio::time::sleep(Duration::from_millis(1000)).await;
            assert_eq!(
                waiter.stores_created(),
                created,
                "pooled {pooled}: still waiting at 1s"
            );
            occupant.error(1, "public peer left");

            // admitted at ~1s: the stalled body ends at the budget from arrival
            let end = tokio::time::timeout(Duration::from_secs(5), async {
                let mut started = false;
                loop {
                    match peer.recv().await.and_then(|frame| frame.body) {
                        Some(Body::Data(_)) => started = true,
                        other => return (started, other),
                    }
                }
            })
            .await
            .expect("the request ends");
            let elapsed = started.elapsed();
            assert!(
                matches!(end, (true, Some(Body::Close(_) | Body::Error(_)))),
                "pooled {pooled}: admitted, then ended: {end:?}"
            );
            assert!(elapsed >= budget, "pooled {pooled}: {elapsed:?}");
            assert!(
                elapsed < budget + Duration::from_millis(700),
                "pooled {pooled}: ended at the budget from its arrival, not a fresh one: \
                 {elapsed:?}"
            );
            if pooled {
                assert_eq!(pool(&waiter).served(), 2, "both requests pooled");
            }
        }
    }

    /// Asserts the only slot of `instances` frees, then serves a follow-up
    /// request on it.
    async fn assert_slot_freed(instances: &Arc<Instances>, loaded: &Arc<Loaded>, what: &str) {
        eventually(what, Duration::from_secs(3), || instances.held() == 0).await;
        let next = limited(loaded, instances, None);
        let body = tokio::time::timeout(Duration::from_secs(5), exchange(&next, &get("/")))
            .await
            .unwrap_or_else(|_| panic!("{what}: the follow-up is served"));
        assert!(body.starts_with("HTTP/1.1 200"), "{what}: {body}");
    }

    /// The slot frees when the request ends by its budget: the watchdog
    /// stopping a suspended guest, and the epoch interrupting a busy one.
    #[tokio::test]
    async fn the_slot_frees_when_the_budget_ends_the_request() {
        let loaded = hello().await;
        for (path, budget) in [("/stall", 300), ("/spin", 100)] {
            let instances = Instances::new(1);
            let handler = limited(&loaded, &instances, Some(Duration::from_millis(budget)));
            let mut peer = open(&handler, &get(path));
            // a spinning guest holds this current-thread runtime until its
            // budget: the request's instance is the precondition seen after
            let frames = frames_until_quiet(&mut peer, Duration::from_secs(1)).await;
            assert_eq!(handler.stores_created(), 1, "{path}: it had an instance");
            assert!(!terminal_frames(&frames).is_empty(), "{path}: ended");
            assert_slot_freed(&instances, &loaded, path).await;
        }
    }

    /// The slot frees when the client leaves (the server's ERROR), also
    /// after a half-close -- which by itself keeps the request and its slot.
    #[tokio::test]
    async fn the_slot_frees_when_the_client_leaves() {
        let loaded = hello().await;
        for half_closed in [false, true] {
            let instances = Instances::new(1);
            let handler = limited(&loaded, &instances, None);
            let peer = stalled_connection(&handler).await;
            if half_closed {
                half_close(&peer);
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert_eq!(instances.held(), 1, "a half-close keeps the slot");
            }
            assert_eq!(instances.held(), 1);
            peer.error(1, "public peer left");
            assert_slot_freed(&instances, &loaded, &format!("half-closed {half_closed}")).await;
            drop(peer);
        }
    }

    /// The slot frees when the guest traps, and when its instantiation
    /// fails.
    #[tokio::test]
    async fn the_slot_frees_on_a_guest_trap_and_a_failed_instantiation() {
        let loaded = hello().await;
        let instances = Instances::new(1);
        let handler = limited(&loaded, &instances, None);
        let body = exchange(&handler, &get("/panic")).await;
        assert!(body.starts_with("HTTP/1.1 500"), "{body}");
        assert_eq!(handler.stores_created(), 1);
        assert_slot_freed(&instances, &loaded, "trap").await;

        let tiny = Handler::new(
            loaded.clone(),
            Sandbox {
                limits: Limits {
                    memory: 1 << 20,
                    ..Limits::default()
                },
                ..Sandbox::default()
            },
            &instances,
        )
        .unwrap();
        let body = exchange(&tiny, &get("/")).await;
        assert!(body.contains("instantiate failed"), "{body}");
        assert_slot_freed(&instances, &loaded, "failed instantiation").await;
    }

    /// A request whose client leaves while it waits gives up its place: it
    /// never takes the slot freed later.
    #[tokio::test]
    async fn a_request_whose_client_leaves_while_it_waits_never_takes_a_slot() {
        let instances = Instances::new(1);
        let handler = limited(&hello().await, &instances, None);
        let occupant = stalled_connection(&handler).await;
        let waiter = open(&handler, &get("/"));
        tokio::time::sleep(Duration::from_millis(200)).await;
        waiter.error(1, "public peer left");
        eventually(
            "the waiter's connection ends",
            Duration::from_secs(1),
            || !waiter.registered(1),
        )
        .await;

        occupant.error(1, "public peer left");
        eventually("the slot frees", Duration::from_secs(3), || {
            instances.held() == 0
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.stores_created(), 1, "only the occupant's store");
        assert_eq!(instances.held(), 0);
    }

    /// Pooled units hold their slots: with every slot pooled, a request
    /// finding no idle unit waits for one -- no per-request store is created
    /// -- and a retired unit's replacement takes over its slot.
    #[tokio::test]
    async fn a_request_waits_for_an_idle_unit_when_the_units_hold_every_slot() {
        let instances = Instances::new(1);
        let handler = Handler::new(
            hello().await,
            Sandbox {
                pool: 1,
                ..Sandbox::default()
            },
            &instances,
        )
        .unwrap();
        restocked(&handler, 1).await;
        assert_eq!(instances.held(), 1, "the idle unit holds the slot");

        let occupant = stalled_connection(&handler).await;
        let mut waiter = open(&handler, &get("/"));
        assert_eq!(
            answered(&mut waiter, Duration::from_millis(300)).await,
            None
        );
        assert_eq!(handler.live_stores(), 1, "no per-request store");

        // the unit retires; its replacement, on the same slot, takes the
        // waiting request once offered
        occupant.error(1, "public peer left");
        let response = answered(&mut waiter, Duration::from_secs(3)).await;
        assert!(
            response
                .as_deref()
                .is_some_and(|r| r.starts_with("HTTP/1.1 200")),
            "{response:?}"
        );
        let pool = pool(&handler);
        assert_eq!(pool.served(), 2, "both requests pooled");
        assert_eq!(pool.spawned(), 2, "one replacement");
        assert_eq!(handler.peak_stores(), 1, "never more stores than slots");
        assert_eq!(instances.held(), 1, "the replacement holds the slot");
    }

    /// A unit that never got viable frees its slot for per-request
    /// instances.
    #[tokio::test]
    async fn an_unviable_unit_frees_its_slot() {
        let instances = Instances::new(1);
        let handler = Handler::new(
            hello().await,
            Sandbox {
                pool: 1,
                limits: Limits {
                    memory: 1 << 20,
                    ..Limits::default()
                },
                ..Sandbox::default()
            },
            &instances,
        )
        .unwrap();
        eventually("the pool shrinks", Duration::from_secs(5), || {
            pool(&handler).units() == 0
        })
        .await;
        assert_slot_freed(&instances, &hello().await, "unviable unit").await;
    }

    /// Pools are carved out of the limit at startup: one larger than the
    /// slots left fails, and takes none.
    #[tokio::test]
    async fn a_pool_larger_than_the_slots_left_fails_at_startup() {
        let loaded = hello().await;
        let instances = Instances::new(3);
        let sandbox = Sandbox {
            pool: 2,
            ..Sandbox::default()
        };
        let first = Handler::new(loaded.clone(), sandbox, &instances).unwrap();
        assert_eq!(instances.held(), 2);
        let error = match Handler::new(loaded.clone(), sandbox, &instances) {
            Ok(_) => panic!("a second pool=2 fit in 1 slot"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            "pool=2 needs 2 instance slots, 1 left of --max-instances 3"
        );
        assert_eq!(instances.held(), 2, "the failed pool took no slot");
        drop(first);
    }

    /// An h2 client on conn 1 of a peer of its own, its bytes pumped
    /// through the peer.
    async fn h2_client(handler: &Arc<Handler>) -> h2::client::SendRequest<bytes::Bytes> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut peer = Peer::new(HashMap::from([(ROUTE.to_string(), handler.clone())]));
        peer.connect(1, ROUTE, crate::tunnel::WINDOW);
        let (io, mut pumped) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                tokio::select! {
                    read = pumped.read(&mut buf) => match read {
                        Ok(0) | Err(_) => break,
                        Ok(n) => peer.data(1, &buf[..n]),
                    },
                    frame = peer.recv() => match frame.and_then(|frame| frame.body) {
                        Some(Body::Data(data)) => {
                            if pumped.write_all(&data.payload).await.is_err() {
                                break;
                            }
                        }
                        Some(Body::Close(_) | Body::Error(_)) | None => break,
                        Some(_) => {}
                    },
                }
            }
        });
        let (client, connection) = h2::client::handshake(io).await.expect("h2 handshake");
        tokio::spawn(connection);
        client
    }

    fn h2_get(path: &str) -> http::Request<()> {
        http::Request::get(format!("http://t{path}"))
            .body(())
            .unwrap()
    }

    /// One h2 connection may have half the slots in flight: its further
    /// streams wait at the client, and another connection is served at once.
    #[tokio::test]
    async fn one_h2_connection_cannot_take_every_slot() {
        let loaded = hello().await;
        let instances = Instances::new(2);
        let handler = limited(&loaded, &instances, None);
        let mut client = h2_client(&handler).await;

        let (first, _) = client.send_request(h2_get("/stall"), true).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the first stream's head")
            .unwrap();
        assert_eq!(first.status(), 200);
        assert_eq!(
            client.current_max_send_streams(),
            1,
            "the connection's SETTINGS allow one stream"
        );
        client = client.ready().await.unwrap();
        let (_second, _) = client.send_request(h2_get("/stall"), true).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(instances.held(), 1, "the connection holds one slot");

        let other = limited(&loaded, &instances, Some(Duration::from_secs(1)));
        let body = exchange(&other, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    }

    /// A reset stream keeps its connection's share until its instance is
    /// gone, not just until the stream ended: a spinning guest runs on to
    /// its budget after the reset, and the connection's next stream waits
    /// for it instead of taking a further slot -- per request and on a
    /// pooled unit. Multi-threaded, as the spin holds its worker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reset_stream_keeps_its_share_until_its_instance_is_gone() {
        let loaded = hello().await;
        for pooled in [false, true] {
            let instances = Instances::new(2);
            let mut sandbox = Sandbox::default();
            sandbox.limits.budget = Duration::from_millis(1500);
            sandbox.pool = usize::from(pooled);
            let spinning = Handler::new(loaded.clone(), sandbox, &instances).unwrap();
            if pooled {
                restocked(&spinning, 1).await;
            }
            let created = spinning.stores_created();
            let mut client = h2_client(&spinning).await;
            let (first, _) = client.send_request(h2_get("/spin"), true).unwrap();
            eventually("the first is served", Duration::from_secs(2), || {
                if pooled {
                    pool(&spinning).served() == 1
                } else {
                    spinning.stores_created() == 1
                }
            })
            .await;
            eventually("the one-stream SETTINGS", Duration::from_secs(1), || {
                client.current_max_send_streams() == 1
            })
            .await;

            // the reset ends the stream; the spinning guest keeps its
            // instance. The second stream comes later, so its budget
            // outlasts the first's: the spin ends on epoch ticks, which lag
            // the wall clock under load (up to ~2s for these 1.5s measured
            // in the parallel suite).
            drop(first);
            tokio::time::sleep(Duration::from_millis(900)).await;
            client = client.ready().await.unwrap();
            let (second, _) = client.send_request(h2_get("/spin"), true).unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(
                spinning.stores_created(),
                created + usize::from(!pooled),
                "pooled {pooled}: the second stream waits for the share"
            );
            assert_eq!(instances.held(), 1, "pooled {pooled}: one slot held");

            let other = limited(&loaded, &instances, Some(Duration::from_secs(1)));
            let body = exchange(&other, &get("/")).await;
            assert!(body.starts_with("HTTP/1.1 200"), "pooled {pooled}: {body}");

            // the first instance gone at its budget, the second stream runs
            // into its own: a 504, not the 503 of a request never admitted
            let second = tokio::time::timeout(Duration::from_secs(3), second)
                .await
                .expect("the second stream is answered")
                .unwrap();
            assert_eq!(second.status(), 504, "pooled {pooled}");
        }
    }

    // -- per-route cap ------------------------------------------------------

    /// A route on `instances` capped at `cap` of the slots, keeping `pool`
    /// units, with `budget` (the default when None).
    fn capped(
        loaded: &Arc<Loaded>,
        instances: &Arc<Instances>,
        cap: usize,
        pool: usize,
        budget: Option<Duration>,
    ) -> Arc<Handler> {
        let mut sandbox = Sandbox {
            instances: Some(cap),
            pool,
            ..Sandbox::default()
        };
        if let Some(budget) = budget {
            sandbox.limits.budget = budget;
        }
        Handler::new(loaded.clone(), sandbox, instances).unwrap()
    }

    /// Asserts `response` is a 200.
    fn assert_ok(response: Option<String>, what: &str) {
        assert!(
            response
                .as_deref()
                .is_some_and(|r| r.starts_with("HTTP/1.1 200")),
            "{what}: {response:?}"
        );
    }

    /// A route at its cap holds back its own further requests, which take no
    /// process-wide slot while they wait: another route is served at once.
    #[tokio::test]
    async fn a_route_at_its_cap_leaves_the_other_routes_their_slots() {
        let loaded = hello().await;
        let instances = Instances::new(2);
        let busy = capped(&loaded, &instances, 1, 0, None);
        let other = limited(&loaded, &instances, Some(Duration::from_secs(1)));
        let occupant = stalled_connection(&busy).await;
        assert_eq!(busy.route_held(), 1, "precondition: the route's slot held");

        let mut waiter = open(&busy, &get("/"));
        assert_eq!(
            answered(&mut waiter, Duration::from_millis(300)).await,
            None,
            "the route's next request waits for its slot"
        );
        assert_eq!(instances.held(), 1, "the waiter takes no process-wide slot");
        assert_eq!(busy.stores_created(), 1, "no store for the waiter");

        // served within its 1s budget, while the occupant holds on for 10s
        let body = exchange(&other, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");

        occupant.error(1, "public peer left");
        assert_ok(
            answered(&mut waiter, Duration::from_secs(3)).await,
            "the waiter takes the route's freed slot",
        );
        assert_eq!(busy.peak_stores(), 1, "never past the cap");
    }

    /// A route capped at its pool's size never instantiates per request: a
    /// request finding every unit busy waits for one, though process-wide
    /// slots are free.
    #[tokio::test]
    async fn a_route_capped_at_its_pool_never_instantiates_per_request() {
        let instances = Instances::new(4);
        let handler = capped(&hello().await, &instances, 1, 1, None);
        restocked(&handler, 1).await;
        assert_eq!(handler.route_held(), 1, "the unit holds the route's slot");

        let occupant = stalled_connection(&handler).await;
        let mut waiter = open(&handler, &get("/"));
        assert_eq!(
            answered(&mut waiter, Duration::from_millis(300)).await,
            None
        );
        assert_eq!(handler.stores_created(), 1, "no per-request store");
        assert_eq!(instances.held(), 1, "process-wide slots stay free");

        occupant.error(1, "public peer left");
        assert_ok(
            answered(&mut waiter, Duration::from_secs(3)).await,
            "the replacement unit serves the waiter",
        );
        let pool = pool(&handler);
        assert_eq!(pool.served(), 2, "both requests pooled");
        assert_eq!(pool.spawned(), 2, "one replacement");
        assert_eq!(handler.stores_created(), 2, "the units' stores only");
        assert_eq!(handler.peak_stores(), 1);
        assert_eq!(handler.route_held(), 1, "the replacement holds the slot");
    }

    /// The cap counts the route's pooled units: with the pool busy, per-request
    /// instances make up the rest of the cap and no more.
    #[tokio::test]
    async fn a_routes_cap_counts_its_pooled_units() {
        let instances = Instances::new(4);
        let handler = capped(&hello().await, &instances, 2, 1, None);
        restocked(&handler, 1).await;
        let pooled = stalled_connection(&handler).await;
        let cold = stalled_connection(&handler).await;
        assert_eq!(pool(&handler).served(), 1, "precondition: one pooled");
        assert_eq!(handler.stores_created(), 2, "precondition: one cold");
        assert_eq!(handler.route_held(), 2);

        let mut waiter = open(&handler, &get("/"));
        assert_eq!(
            answered(&mut waiter, Duration::from_millis(300)).await,
            None
        );
        assert_eq!(handler.stores_created(), 2, "no store past the cap");
        assert_eq!(instances.held(), 2);

        cold.error(1, "public peer left");
        assert_ok(
            answered(&mut waiter, Duration::from_secs(3)).await,
            "the waiter takes the freed cold slot",
        );
        assert_eq!(handler.peak_stores(), 2, "never past the cap");
        drop(pooled);
    }

    /// A request finding no slot of its route within its budget is answered
    /// 503 at the budget, holding no process-wide slot meanwhile.
    #[tokio::test]
    async fn a_request_finding_no_route_slot_within_its_budget_is_a_503() {
        let instances = Instances::new(2);
        let budget = Duration::from_millis(800);
        let handler = capped(&hello().await, &instances, 1, 0, Some(budget));
        // the occupant: the route's only slot, taken as a request would
        let occupant = handler.route.clone().unwrap().try_acquire_owned().unwrap();

        let started = Instant::now();
        let mut peer = open(&handler, &get("/"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(instances.held(), 0, "waiting holds no process-wide slot");
        let response = answered(&mut peer, budget + Duration::from_secs(2))
            .await
            .expect("answered at the budget");
        let elapsed = started.elapsed();
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.contains(INSTANCE_LIMIT_REACHED), "{response}");
        assert!(elapsed >= budget, "not before the budget: {elapsed:?}");
        assert!(elapsed < budget + Duration::from_secs(1), "{elapsed:?}");
        assert_eq!(handler.stores_created(), 0, "never instantiated");
        assert_eq!(handler.route_held(), 1, "only the occupant's slot");
        drop(occupant);
    }

    /// A request whose client leaves while it waits for its route's slot
    /// gives up its place: it never takes the slot freed later.
    #[tokio::test]
    async fn a_request_whose_client_leaves_while_it_waits_for_its_route_never_takes_a_slot() {
        let instances = Instances::new(2);
        let handler = capped(&hello().await, &instances, 1, 0, None);
        let occupant = stalled_connection(&handler).await;
        let waiter = open(&handler, &get("/"));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(handler.stores_created(), 1, "precondition: it waits");
        waiter.error(1, "public peer left");
        eventually(
            "the waiter's connection ends",
            Duration::from_secs(1),
            || !waiter.registered(1),
        )
        .await;

        occupant.error(1, "public peer left");
        eventually("the slots free", Duration::from_secs(3), || {
            handler.route_held() == 0 && instances.held() == 0
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.stores_created(), 1, "only the occupant's store");
        assert_eq!(handler.route_held(), 0);
        assert_eq!(instances.held(), 0);
    }

    /// A request holding its route's slot while it waits for a process-wide
    /// one gives the route's slot back when it gives up: its client leaving,
    /// and its budget running out (503).
    #[tokio::test]
    async fn a_request_giving_up_on_a_process_wide_slot_frees_its_route_slot() {
        let loaded = hello().await;
        let instances = Instances::new(1);
        let holder = limited(&loaded, &instances, None);
        let occupant = stalled_connection(&holder).await;
        let budget = Duration::from_millis(800);
        let handler = capped(&loaded, &instances, 1, 0, Some(budget));

        let waiter = open(&handler, &get("/"));
        eventually(
            "the waiter takes its route's slot",
            Duration::from_secs(1),
            || handler.route_held() == 1,
        )
        .await;
        waiter.error(1, "public peer left");
        eventually(
            "the route's slot frees with the client",
            Duration::from_secs(1),
            || handler.route_held() == 0,
        )
        .await;

        let mut waiter = open(&handler, &get("/"));
        eventually(
            "the waiter takes its route's slot",
            Duration::from_secs(1),
            || handler.route_held() == 1,
        )
        .await;
        let response = answered(&mut waiter, budget + Duration::from_secs(2))
            .await
            .expect("answered at the budget");
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        eventually(
            "the route's slot frees at the budget",
            Duration::from_secs(1),
            || handler.route_held() == 0,
        )
        .await;
        assert_eq!(handler.stores_created(), 0, "never instantiated");
        assert_eq!(instances.held(), 1, "the occupant's slot only");
        drop(occupant);
    }

    /// Asserts `handler`'s route slots and the process-wide ones free.
    async fn assert_route_slots_freed(handler: &Handler, instances: &Instances, what: &str) {
        eventually(what, Duration::from_secs(3), || {
            handler.route_held() == handler.sandbox.pool && instances.held() == handler.sandbox.pool
        })
        .await;
    }

    /// A capped route's slot frees on every end of its request: a guest
    /// trap, the budget (the watchdog and the epoch), the client leaving, a
    /// failed instantiation, and a pooled unit that never got viable.
    #[tokio::test]
    async fn a_routes_slot_frees_on_every_exit() {
        let loaded = hello().await;

        let instances = Instances::new(2);
        let handler = capped(&loaded, &instances, 1, 0, None);
        let body = exchange(&handler, &get("/panic")).await;
        assert!(body.starts_with("HTTP/1.1 500"), "{body}");
        assert_route_slots_freed(&handler, &instances, "trap").await;
        let body = exchange(&handler, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "after the trap: {body}");

        for (path, budget) in [("/stall", 300), ("/spin", 100)] {
            let instances = Instances::new(2);
            let budget = Some(Duration::from_millis(budget));
            let handler = capped(&loaded, &instances, 1, 0, budget);
            let mut peer = open(&handler, &get(path));
            let frames = frames_until_quiet(&mut peer, Duration::from_secs(1)).await;
            assert_eq!(handler.stores_created(), 1, "{path}: it had an instance");
            assert!(!terminal_frames(&frames).is_empty(), "{path}: ended");
            assert_route_slots_freed(&handler, &instances, path).await;
        }

        let instances = Instances::new(2);
        let handler = capped(&loaded, &instances, 1, 0, None);
        let peer = stalled_connection(&handler).await;
        assert_eq!(handler.route_held(), 1, "precondition: held while served");
        peer.error(1, "public peer left");
        assert_route_slots_freed(&handler, &instances, "client gone").await;

        let instances = Instances::new(2);
        let tiny = |pool| {
            let sandbox = Sandbox {
                instances: Some(1),
                pool,
                limits: Limits {
                    memory: 1 << 20,
                    ..Limits::default()
                },
                ..Sandbox::default()
            };
            Handler::new(loaded.clone(), sandbox, &instances).unwrap()
        };
        let handler = tiny(0);
        let body = exchange(&handler, &get("/")).await;
        assert!(body.contains("instantiate failed"), "{body}");
        assert_route_slots_freed(&handler, &instances, "failed instantiation").await;
        let body = exchange(&handler, &get("/")).await;
        assert!(
            body.contains("instantiate failed"),
            "the slot again: {body}"
        );

        let handler = tiny(1);
        eventually("the pool shrinks", Duration::from_secs(5), || {
            pool(&handler).units() == 0
        })
        .await;
        eventually(
            "the unviable unit's slot frees",
            Duration::from_secs(3),
            || handler.route_held() == 0,
        )
        .await;
    }

    /// The connection's share and the route's cap compose: an h2 connection
    /// announces its share of the process-wide slots, and its streams beyond
    /// the route's cap wait for the route's slot without a process-wide one.
    #[tokio::test]
    async fn h2_streams_beyond_the_routes_cap_wait_without_a_slot() {
        let loaded = hello().await;
        let instances = Instances::new(4);
        let handler = capped(&loaded, &instances, 1, 0, None);
        let mut client = h2_client(&handler).await;
        let (first, _) = client.send_request(h2_get("/stall"), true).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the first stream's head")
            .unwrap();
        assert_eq!(first.status(), 200);
        assert_eq!(
            client.current_max_send_streams(),
            2,
            "the connection's share, not the route's cap"
        );
        client = client.ready().await.unwrap();
        let (second, _) = client.send_request(h2_get("/"), true).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handler.stores_created(), 1, "the second stream waits");
        assert_eq!(instances.held(), 1, "holding no process-wide slot");

        let other = limited(&loaded, &instances, Some(Duration::from_secs(1)));
        let body = exchange(&other, &get("/")).await;
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");

        // the first stream's reset ends its store: the second takes the slot
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(3), second)
            .await
            .expect("the second stream is answered")
            .unwrap();
        assert_eq!(second.status(), 200);
    }

    /// A cap below the route's pool fails at startup and takes no slot; one
    /// equal to it, or above `--max-instances`, is accepted.
    #[tokio::test]
    async fn a_cap_below_the_pool_fails_at_startup() {
        let loaded = hello().await;
        let instances = Instances::new(2);
        let sandbox = Sandbox {
            instances: Some(1),
            pool: 2,
            ..Sandbox::default()
        };
        let error = match Handler::new(loaded.clone(), sandbox, &instances) {
            Ok(_) => panic!("pool=2 fit a cap of 1"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            "instances=1 is below pool=2: every pooled unit holds one of the route's \
             instance slots"
        );
        assert_eq!(instances.held(), 0, "the failed route took no slot");

        let at_pool = capped(&loaded, &instances, 2, 2, None);
        assert_eq!(at_pool.route_held(), 2);
        let above = capped(&loaded, &Instances::new(1), 8, 0, None);
        assert_eq!(above.route_held(), 0);
    }
}
