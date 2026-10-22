//! Spike: wasi:keyvalue@0.2.0-draft2 for a wasi:http 0.3 guest on wasmtime 50,
//! behind a swappable `Backend` (redb file here; redis / gemfire later).
//!
//!   cargo run --release -- <guest.wasm> <db file> <path>...
//!
//! Each path is one request on a fresh store, as the wicket's per-request path.
//! A path prefixed with `!` is cancelled after 100ms (the store dropped while
//! a keyvalue call is pending), as the wicket's budget watchdog does.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use redb::{ReadableDatabase, ReadableTable};
use wasmtime::component::{HasSelf, Linker, Resource, ResourceTable};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../wit/keyvalue",
        world: "wasi:keyvalue/imports",
        // The draft2 functions are not `async func`: the guest call blocks
        // (its instance), while the host awaits the backend on the fiber.
        imports: { default: async | trappable },
        with: {
            "wasi:keyvalue/store.bucket": crate::Bucket,
            "wasi:keyvalue/atomics.cas": crate::Cas,
        },
    });
}

use bindings::wasi::keyvalue::{atomics, batch, store};

mod draft;
mod sqlite;

// ---------------------------------------------------------------------------
// The backend abstraction

/// What a backend reports; mapped to `wasi:keyvalue/store.error.other`.
type BackendResult<T> = Result<T, String>;

/// The outcome of a conditional write.
enum Swap {
    Done,
    /// The value changed since it was read: the current one (none = absent).
    Stale(Option<Vec<u8>>),
}

/// One key-value store the host provides. `bucket` is the resolved namespace
/// (never the guest's identifier), so a backend knows nothing about routes.
#[async_trait::async_trait]
trait Backend: Send + Sync + 'static {
    async fn get(&self, bucket: &str, key: &str) -> BackendResult<Option<Vec<u8>>>;
    async fn set(&self, bucket: &str, key: &str, value: Vec<u8>) -> BackendResult<()>;
    async fn delete(&self, bucket: &str, key: &str) -> BackendResult<()>;
    async fn exists(&self, bucket: &str, key: &str) -> BackendResult<bool> {
        Ok(self.get(bucket, key).await?.is_some())
    }
    /// A page of keys after `cursor` (exclusive), and the cursor of the next.
    async fn list_keys(
        &self,
        bucket: &str,
        cursor: Option<String>,
    ) -> BackendResult<(Vec<String>, Option<String>)>;
    /// Atomic; the value is the decimal ASCII of an i64 (as Redis INCRBY).
    async fn increment(&self, bucket: &str, key: &str, delta: i64) -> BackendResult<i64>;
    /// Atomic: writes `value` only while the key still holds `expected`.
    async fn swap(
        &self,
        bucket: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> BackendResult<Swap>;
    // batch: per-key defaults; a networked backend overrides them (MGET, ...)
    async fn get_many(
        &self,
        bucket: &str,
        keys: Vec<String>,
    ) -> BackendResult<Vec<(String, Option<Vec<u8>>)>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(bucket, &key).await?;
            out.push((key, value));
        }
        Ok(out)
    }
}

/// The default backend: one redb file on the host. ACID, ordered keys (the
/// cursor is the last key returned), atomic increment / swap in a write
/// transaction. One process holds the file: sharing stores across wickets
/// is what a networked backend is for.
struct RedbBackend {
    db: Arc<redb::Database>,
}

const PAGE: usize = 2;

impl RedbBackend {
    fn open(path: &str) -> Self {
        Self {
            db: Arc::new(redb::Database::create(path).expect("open redb")),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&redb::Database) -> Result<T, redb::Error> + Send + 'static,
    ) -> BackendResult<T> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())
    }
}

fn table(bucket: &str) -> redb::TableDefinition<'_, &'static str, &'static [u8]> {
    redb::TableDefinition::new(bucket)
}

fn read_value(db: &redb::Database, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, redb::Error> {
    let txn = db.begin_read()?;
    let table = match txn.open_table(table(bucket)) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(table.get(key)?.map(|v| v.value().to_vec()))
}

#[async_trait::async_trait]
impl Backend for RedbBackend {
    async fn get(&self, bucket: &str, key: &str) -> BackendResult<Option<Vec<u8>>> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| read_value(db, &bucket, &key)).await
    }

    async fn set(&self, bucket: &str, key: &str, value: Vec<u8>) -> BackendResult<()> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| {
            let txn = db.begin_write()?;
            txn.open_table(table(&bucket))?.insert(key.as_str(), value.as_slice())?;
            txn.commit()?;
            Ok(())
        })
        .await
    }

    async fn delete(&self, bucket: &str, key: &str) -> BackendResult<()> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| {
            let txn = db.begin_write()?;
            txn.open_table(table(&bucket))?.remove(key.as_str())?;
            txn.commit()?;
            Ok(())
        })
        .await
    }

    async fn list_keys(
        &self,
        bucket: &str,
        cursor: Option<String>,
    ) -> BackendResult<(Vec<String>, Option<String>)> {
        let bucket = bucket.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            let table = match txn.open_table(table(&bucket)) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), None)),
                Err(e) => return Err(e.into()),
            };
            let from = match &cursor {
                Some(c) => Bound::Excluded(c.as_str()),
                None => Bound::Unbounded,
            };
            let mut keys = Vec::new();
            for entry in table.range::<&str>((from, Bound::Unbounded))?.take(PAGE + 1) {
                keys.push(entry?.0.value().to_string());
            }
            let next = if keys.len() > PAGE {
                keys.truncate(PAGE);
                keys.last().cloned()
            } else {
                None
            };
            Ok((keys, next))
        })
        .await
    }

    async fn increment(&self, bucket: &str, key: &str, delta: i64) -> BackendResult<i64> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| {
            let txn = db.begin_write()?;
            let next = {
                let mut table = txn.open_table(table(&bucket))?;
                let current = match table.get(key.as_str())? {
                    Some(v) => std::str::from_utf8(v.value())
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or_else(|| {
                            redb::Error::Corrupted("value is not an integer".into())
                        })?,
                    None => 0,
                };
                let next = current
                    .checked_add(delta)
                    .ok_or_else(|| redb::Error::Corrupted("overflow".into()))?;
                table.insert(key.as_str(), next.to_string().as_bytes())?;
                next
            };
            txn.commit()?;
            Ok(next)
        })
        .await
    }

    async fn swap(
        &self,
        bucket: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> BackendResult<Swap> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| {
            let txn = db.begin_write()?;
            let outcome = {
                let mut table = txn.open_table(table(&bucket))?;
                let current = table.get(key.as_str())?.map(|v| v.value().to_vec());
                if current == expected {
                    table.insert(key.as_str(), value.as_slice())?;
                    Swap::Done
                } else {
                    Swap::Stale(current)
                }
            };
            txn.commit()?;
            Ok(outcome)
        })
        .await
    }
}

/// Network latency stand-in: delays every read.
struct Slow<B>(B);

#[async_trait::async_trait]
impl<B: Backend> Backend for Slow<B> {
    async fn get(&self, bucket: &str, key: &str) -> BackendResult<Option<Vec<u8>>> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.0.get(bucket, key).await
    }
    async fn set(&self, bucket: &str, key: &str, value: Vec<u8>) -> BackendResult<()> {
        self.0.set(bucket, key, value).await
    }
    async fn delete(&self, bucket: &str, key: &str) -> BackendResult<()> {
        self.0.delete(bucket, key).await
    }
    async fn list_keys(
        &self,
        bucket: &str,
        cursor: Option<String>,
    ) -> BackendResult<(Vec<String>, Option<String>)> {
        self.0.list_keys(bucket, cursor).await
    }
    async fn increment(&self, bucket: &str, key: &str, delta: i64) -> BackendResult<i64> {
        self.0.increment(bucket, key, delta).await
    }
    async fn swap(
        &self,
        bucket: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> BackendResult<Swap> {
        self.0.swap(bucket, key, expected, value).await
    }
}

// ---------------------------------------------------------------------------
// The wasi:keyvalue host: identifier -> (backend, namespace)

/// A route's buckets: the identifiers its guest may open.
type Buckets = HashMap<String, (Arc<dyn Backend>, String)>;

pub struct Bucket {
    backend: Arc<dyn Backend>,
    namespace: String,
}

pub struct Cas {
    backend: Arc<dyn Backend>,
    namespace: String,
    key: String,
    current: Option<Vec<u8>>,
}

fn other(e: String) -> store::Error {
    store::Error::Other(e)
}

impl Ctx {
    fn bucket(&self, bucket: &Resource<Bucket>) -> wasmtime::Result<(Arc<dyn Backend>, String)> {
        let bucket = self.table.get(bucket)?;
        Ok((bucket.backend.clone(), bucket.namespace.clone()))
    }
}

impl store::Host for Ctx {
    async fn open(&mut self, identifier: String) -> wasmtime::Result<Result<Resource<Bucket>, store::Error>> {
        let Some((backend, namespace)) = self.buckets.get(&identifier).cloned() else {
            return Ok(Err(store::Error::NoSuchStore));
        };
        Ok(Ok(self.table.push(Bucket { backend, namespace })?))
    }
}

impl store::HostBucket for Ctx {
    async fn get(&mut self, bucket: Resource<Bucket>, key: String) -> wasmtime::Result<Result<Option<Vec<u8>>, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.get(&ns, &key).await.map_err(other))
    }
    async fn set(&mut self, bucket: Resource<Bucket>, key: String, value: Vec<u8>) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.set(&ns, &key, value).await.map_err(other))
    }
    async fn delete(&mut self, bucket: Resource<Bucket>, key: String) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.delete(&ns, &key).await.map_err(other))
    }
    async fn exists(&mut self, bucket: Resource<Bucket>, key: String) -> wasmtime::Result<Result<bool, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.exists(&ns, &key).await.map_err(other))
    }
    async fn list_keys(&mut self, bucket: Resource<Bucket>, cursor: Option<String>) -> wasmtime::Result<Result<store::KeyResponse, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend
            .list_keys(&ns, cursor)
            .await
            .map(|(keys, cursor)| store::KeyResponse { keys, cursor })
            .map_err(other))
    }
    async fn drop(&mut self, bucket: Resource<Bucket>) -> wasmtime::Result<()> {
        self.table.delete(bucket)?;
        Ok(())
    }
}

impl atomics::Host for Ctx {
    async fn increment(&mut self, bucket: Resource<Bucket>, key: String, delta: i64) -> wasmtime::Result<Result<i64, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.increment(&ns, &key, delta).await.map_err(other))
    }
    async fn swap(&mut self, cas: Resource<Cas>, value: Vec<u8>) -> wasmtime::Result<Result<(), atomics::CasError>> {
        let cas = self.table.delete(cas)?;
        match cas
            .backend
            .swap(&cas.namespace, &cas.key, cas.current.clone(), value)
            .await
        {
            Ok(Swap::Done) => Ok(Ok(())),
            Ok(Swap::Stale(current)) => {
                let next = self.table.push(Cas { current, ..cas })?;
                Ok(Err(atomics::CasError::CasFailed(next)))
            }
            Err(e) => Ok(Err(atomics::CasError::StoreError(other(e)))),
        }
    }
}

impl atomics::HostCas for Ctx {
    async fn new(&mut self, bucket: Resource<Bucket>, key: String) -> wasmtime::Result<Result<Resource<Cas>, store::Error>> {
        let (backend, namespace) = self.bucket(&bucket)?;
        let current = match backend.get(&namespace, &key).await {
            Ok(current) => current,
            Err(e) => return Ok(Err(other(e))),
        };
        Ok(Ok(self.table.push(Cas { backend, namespace, key, current })?))
    }
    async fn current(&mut self, cas: Resource<Cas>) -> wasmtime::Result<Result<Option<Vec<u8>>, store::Error>> {
        Ok(Ok(self.table.get(&cas)?.current.clone()))
    }
    async fn drop(&mut self, cas: Resource<Cas>) -> wasmtime::Result<()> {
        self.table.delete(cas)?;
        Ok(())
    }
}

impl batch::Host for Ctx {
    async fn get_many(&mut self, bucket: Resource<Bucket>, keys: Vec<String>) -> wasmtime::Result<Result<Vec<(String, Option<Vec<u8>>)>, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.get_many(&ns, keys).await.map_err(other))
    }
    async fn set_many(&mut self, bucket: Resource<Bucket>, key_values: Vec<(String, Vec<u8>)>) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        for (key, value) in key_values {
            if let Err(e) = backend.set(&ns, &key, value).await {
                return Ok(Err(other(e)));
            }
        }
        Ok(Ok(()))
    }
    async fn delete_many(&mut self, bucket: Resource<Bucket>, keys: Vec<String>) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        for key in keys {
            if let Err(e) = backend.delete(&ns, &key).await {
                return Ok(Err(other(e)));
            }
        }
        Ok(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// Store state and the request driver (as the wicket's)

struct NoHooks;

impl wasmtime_wasi_http::WasiHttpHooks for NoHooks {}

struct Ctx {
    table: ResourceTable,
    wasi: wasmtime_wasi::WasiCtx,
    http: wasmtime_wasi_http::WasiHttpCtx,
    hooks: NoHooks,
    buckets: Arc<Buckets>,
}

impl wasmtime_wasi::WasiView for Ctx {
    fn ctx(&mut self) -> wasmtime_wasi::WasiCtxView<'_> {
        wasmtime_wasi::WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
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

async fn request(
    engine: &wasmtime::Engine,
    pre: &wasmtime_wasi_http::p3::bindings::ServicePre<Ctx>,
    buckets: &Arc<Buckets>,
    path: &str,
) {
    let (cancel, path) = match path.strip_prefix('!') {
        Some(path) => (true, path),
        None => (false, path),
    };
    let mut wasi = wasmtime_wasi::WasiCtxBuilder::new();
    wasi.inherit_stdout().inherit_stderr();
    let mut store = wasmtime::Store::new(
        engine,
        Ctx {
            table: ResourceTable::new(),
            wasi: wasi.build(),
            http: wasmtime_wasi_http::WasiHttpCtx::new(),
            hooks: NoHooks,
            buckets: buckets.clone(),
        },
    );
    let service = pre.instantiate_async(&mut store).await.expect("instantiate");
    let (path, body) = path.split_once('=').unwrap_or((path, ""));
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("http://kv.local/{path}"))
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let start = Instant::now();
    let driven = store.run_concurrent(async move |accessor| {
        let (req, pump) = accessor.with(|mut access| {
            wasmtime_wasi_http::p3::Request::from_http(&mut access.data_mut().hooks, req)
        });
        let response = service.handle(accessor, req).await.expect("handle").expect("ok");
        let http = accessor
            .with(|access| response.into_http(access, std::future::ready(Ok(()))))
            .expect("into_http");
        let mut body = http.into_body();
        let mut out = String::new();
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame.expect("frame").into_data() {
                let text = String::from_utf8_lossy(&data);
                if path.starts_with("concurrent") {
                    out.push_str(&format!("  [host +{:>4}ms] {text}", start.elapsed().as_millis()));
                } else {
                    out.push_str(&text);
                }
            }
        }
        let _ = pump.await;
        out
    });
    if cancel {
        match tokio::time::timeout(Duration::from_millis(100), driven).await {
            Ok(_) => println!("{path}: finished before the cancel"),
            Err(_) => println!("{path}: cancelled after 100ms (store dropped mid-call)"),
        }
        return;
    }
    let out = driven.await.expect("run_concurrent");
    println!(
        "{path} ({}ms, table empty: {}):\n{out}",
        start.elapsed().as_millis(),
        store.data().table.is_empty()
    );
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (wasm, db) = (&args[1], &args[2]);

    let mut config = wasmtime::Config::new();
    config.wasm_component_model_async(true);
    let engine = wasmtime::Engine::new(&config).unwrap();

    let mut linker = Linker::<Ctx>::new(&engine);
    wasmtime_wasi::p3::clocks::add_to_linker(&mut linker).unwrap();
    wasmtime_wasi::p3::random::add_to_linker(&mut linker).unwrap();
    wasmtime_wasi::p3::cli::add_to_linker(&mut linker).unwrap();
    wasmtime_wasi_http::p3::add_to_linker(&mut linker).unwrap();
    bindings::Imports::add_to_linker::<_, HasSelf<Ctx>>(&mut linker, |ctx| ctx).unwrap();
    draft::bindings::Imports::add_to_linker::<_, HasSelf<Ctx>>(&mut linker, |ctx| ctx).unwrap();

    let component = wasmtime::component::Component::from_file(&engine, wasm).unwrap();
    let pre = wasmtime_wasi_http::p3::bindings::ServicePre::new(
        linker.instantiate_pre(&component).unwrap(),
    )
    .unwrap();

    // KV_BACKEND=sqlite picks the SQLite backend, else redb
    let (file, slow): (Arc<dyn Backend>, Arc<dyn Backend>) =
        if std::env::var("KV_BACKEND").as_deref() == Ok("sqlite") {
            let file = Arc::new(sqlite::SqliteBackend::open(db));
            (file.clone(), Arc::new(Slow(sqlite::SqliteBackend { conn: file.conn.clone() })))
        } else {
            let file = RedbBackend::open(db);
            let slow = Arc::new(Slow(RedbBackend { db: file.db.clone() }));
            (Arc::new(file), slow)
        };
    // route demo.local: "" is its own namespace; "slow" the same one, slowed
    let buckets: Arc<Buckets> = Arc::new(HashMap::from([
        (String::new(), (file.clone(), "demo.local".to_string())),
        ("slow".to_string(), (slow, "demo.local".to_string())),
    ]));

    // `a+b` runs a and b at once, each on its own store
    for group in &args[3..] {
        futures::future::join_all(
            group.split('+').map(|path| request(&engine, &pre, &buckets, path)),
        )
        .await;
    }
}
