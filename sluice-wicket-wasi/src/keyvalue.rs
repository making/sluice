//! wasi:keyvalue for the guests of a `wasi=keyvalue` route
//! (`wasi:keyvalue@0.2.0-draft2`: `store`, `atomics`, `batch`; experimental,
//! it follows WASI keyvalue as it moves).
//!
//! The stores behind it are the host's choice and invisible to the guest: a
//! route maps each identifier its guest may `open` to a store and a
//! namespace in it (`BucketMap`, by default `""` -> the default store, the
//! route's host as the namespace), so routes are isolated unless mapped to
//! one namespace. A store is a [`Backend`], shaped by no WIT version and
//! given the resolved namespace, never the guest's identifier; only this file
//! changes with the WASI version.
//!
//! The draft2 functions are not `async func`: a call blocks its instance
//! (other tasks of the same guest included) while the host awaits the store,
//! not the other requests.

mod sqlite;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use wasmtime::component::{HasData, Linker, Resource, ResourceTable};

mod bindings {
    wasmtime::component::bindgen!({
        path: "wit/deps/keyvalue",
        world: "wasi:keyvalue/imports",
        // the functions are plain `func`: the host awaits the store on the
        // guest's fiber
        imports: { default: async | trappable },
        with: {
            "wasi:keyvalue/store.bucket": super::Bucket,
            "wasi:keyvalue/atomics.cas": super::Cas,
        },
    });
}

use bindings::wasi::keyvalue::{atomics, batch, store};

/// The interfaces' prefix and the version linked: a component importing
/// another version fails at startup.
pub const PACKAGE: &str = "wasi:keyvalue/";
pub const VERSION: &str = "@0.2.0-draft2";

/// The most keys one `list-keys` page holds.
const PAGE_KEYS: usize = 100;

/// Default of a store's `max-key-bytes`.
const DEFAULT_MAX_KEY_BYTES: usize = 1 << 10;

/// Default of a store's `max-value-bytes`.
const DEFAULT_MAX_VALUE_BYTES: usize = 1 << 20;

/// Default of a `sqlite` store's `max-namespace-bytes`.
pub const DEFAULT_MAX_NAMESPACE_BYTES: u64 = 64 << 20;

// ---------------------------------------------------------------------------
// Backends

/// Why a backend did not serve a call; both reach the guest as `other`.
#[derive(Debug, PartialEq)]
pub enum Failure {
    /// The call cannot be served as asked (a value that is no integer to
    /// increment): the guest's doing, not logged.
    Rejected(String),
    /// The store failed (I/O, ...): logged with the store and namespace.
    Backend(String),
}

pub type Outcome<T> = Result<T, Failure>;

/// What a conditional write did.
#[derive(Debug, PartialEq)]
pub enum Swap {
    Done,
    /// The key no longer held the expected value: its current one (None:
    /// absent).
    Stale(Option<Vec<u8>>),
}

/// One store of key-value pairs, partitioned into namespaces. The defaulted
/// operations are overridden where the store has a primitive.
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    async fn get(&self, namespace: &str, key: &str) -> Outcome<Option<Vec<u8>>>;

    async fn set(&self, namespace: &str, key: &str, value: Vec<u8>) -> Outcome<()>;

    async fn delete(&self, namespace: &str, key: &str) -> Outcome<()>;

    /// A page of keys after `cursor` (from the start without), and the
    /// cursor of the next page, None after the last. The cursor is the
    /// backend's own.
    async fn list_keys(
        &self,
        namespace: &str,
        cursor: Option<String>,
    ) -> Outcome<(Vec<String>, Option<String>)>;

    /// Writes `value` only while the key holds `expected` (None: absent),
    /// atomically.
    async fn swap(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Outcome<Swap>;

    async fn exists(&self, namespace: &str, key: &str) -> Outcome<bool> {
        Ok(self.get(namespace, key).await?.is_some())
    }

    /// Adds `delta` to the decimal ASCII i64 the key holds (0 when absent)
    /// and returns the sum, atomically.
    async fn increment(&self, namespace: &str, key: &str, delta: i64) -> Outcome<i64> {
        loop {
            let current = self.get(namespace, key).await?;
            let sum = add(key, current.as_deref(), delta)?;
            match self
                .swap(namespace, key, current, sum.to_string().into_bytes())
                .await?
            {
                Swap::Done => return Ok(sum),
                Swap::Stale(_) => continue,
            }
        }
    }

    async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<String>,
    ) -> Outcome<Vec<(String, Option<Vec<u8>>)>> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(namespace, &key).await?;
            values.push((key, value));
        }
        Ok(values)
    }

    async fn set_many(&self, namespace: &str, pairs: Vec<(String, Vec<u8>)>) -> Outcome<()> {
        for (key, value) in pairs {
            self.set(namespace, &key, value).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, namespace: &str, keys: Vec<String>) -> Outcome<()> {
        for key in keys {
            self.delete(namespace, &key).await?;
        }
        Ok(())
    }
}

/// `current` (the decimal ASCII of an i64, None: 0) plus `delta`.
fn add(key: &str, current: Option<&[u8]>, delta: i64) -> Outcome<i64> {
    let current = match current {
        None => 0,
        Some(value) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| {
                Failure::Rejected(format!("the value of '{key}' is not a decimal i64"))
            })?,
    };
    current
        .checked_add(delta)
        .ok_or_else(|| Failure::Rejected(format!("incrementing '{key}' overflows an i64")))
}

// ---------------------------------------------------------------------------
// Stores and the routes' maps

/// One store as configured.
#[derive(Clone, PartialEq, Debug)]
pub struct StoreConfig {
    pub kind: Kind,
    pub limits: StoreLimits,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Kind {
    /// A SQLite file on the host, each namespace in it bounded to
    /// `max_namespace_bytes` of keys and values.
    Sqlite {
        path: PathBuf,
        max_namespace_bytes: u64,
    },
}

/// What one call may write to a store, checked before the store sees it.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct StoreLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
        }
    }
}

/// The store name of the one store there is without a `keyvalue` section.
pub const LOCAL: &str = "local";

/// Where an identifier of a route lives.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Place {
    pub store: String,
    pub namespace: String,
}

/// A route's keyvalue identifiers -> where each lives; empty without the
/// `keyvalue` capability.
pub type BucketMap = BTreeMap<String, Place>;

/// The process's stores, as configured at startup (changes apply on a
/// restart); each is opened when a route first maps to it, so a wicket
/// without a keyvalue route creates no file.
pub struct Stores {
    configs: BTreeMap<String, StoreConfig>,
    default: String,
    opened: tokio::sync::Mutex<HashMap<String, Arc<dyn Backend>>>,
}

impl Stores {
    /// `configs` by name, `default` one of them.
    pub fn new(configs: BTreeMap<String, StoreConfig>, default: String) -> Stores {
        assert!(
            configs.contains_key(&default),
            "the default store is configured"
        );
        Stores {
            configs,
            default,
            opened: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The default store: the one an identifier names without a store.
    #[cfg(test)]
    pub fn default_store(&self) -> &str {
        &self.default
    }

    #[cfg(test)]
    pub fn config(&self, name: &str) -> Option<&StoreConfig> {
        self.configs.get(name)
    }

    /// The map of a route on `host` given none: `""` -> the default store,
    /// the host as the namespace.
    pub fn default_map(&self, host: &str) -> BucketMap {
        BucketMap::from([(
            String::new(),
            Place {
                store: self.default.clone(),
                namespace: host.to_string(),
            },
        )])
    }

    /// `store[/namespace]` of a route on `host`: the namespace defaults to
    /// the host.
    pub fn place(&self, host: &str, spec: &str) -> Result<Place, String> {
        let (store, namespace) = match spec.split_once('/') {
            Some((store, namespace)) => (store, namespace),
            None => (spec, host),
        };
        if !self.configs.contains_key(store) {
            return Err(format!(
                "unknown keyvalue store '{store}' ({})",
                self.configs.keys().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        if namespace.is_empty() {
            return Err(format!("'{spec}': empty namespace"));
        }
        Ok(Place {
            store: store.to_string(),
            namespace: namespace.to_string(),
        })
    }

    /// The buckets of `map`, opening the stores it names that are not yet;
    /// None for an empty map (no `keyvalue` capability).
    pub async fn buckets(&self, map: &BucketMap) -> Result<Option<Arc<Buckets>>, String> {
        if map.is_empty() {
            return Ok(None);
        }
        let mut buckets = HashMap::new();
        for (identifier, place) in map {
            let config = self
                .configs
                .get(&place.store)
                .ok_or_else(|| format!("unknown keyvalue store '{}'", place.store))?;
            let backend = self.open(&place.store, config).await?;
            buckets.insert(
                identifier.clone(),
                Target {
                    backend,
                    store: place.store.as_str().into(),
                    namespace: place.namespace.as_str().into(),
                    limits: config.limits,
                },
            );
        }
        Ok(Some(Arc::new(Buckets(buckets))))
    }

    async fn open(&self, name: &str, config: &StoreConfig) -> Result<Arc<dyn Backend>, String> {
        let mut opened = self.opened.lock().await;
        if let Some(backend) = opened.get(name) {
            return Ok(backend.clone());
        }
        let backend: Arc<dyn Backend> = match &config.kind {
            Kind::Sqlite {
                path,
                max_namespace_bytes,
            } => {
                let (file, max) = (path.clone(), *max_namespace_bytes);
                let sqlite = tokio::task::spawn_blocking(move || sqlite::Sqlite::open(&file, max))
                    .await
                    .map_err(|e| format!("keyvalue store '{name}': {e}"))?
                    .map_err(|e| format!("keyvalue store '{name}': {e}"))?;
                println!("keyvalue store '{name}': sqlite {}", path.display());
                Arc::new(sqlite)
            }
        };
        opened.insert(name.to_string(), backend.clone());
        Ok(backend)
    }
}

/// A store and namespace an identifier resolved to.
#[derive(Clone)]
struct Target {
    backend: Arc<dyn Backend>,
    /// The store's name and the namespace, for the log.
    store: Arc<str>,
    namespace: Arc<str>,
    limits: StoreLimits,
}

impl Target {
    /// `key` within the store's `max-key-bytes`.
    fn key(&self, key: &str) -> Result<(), store::Error> {
        within("key", key.len(), self.limits.max_key_bytes, "max-key-bytes")
    }

    /// `value` within the store's `max-value-bytes`.
    fn value(&self, value: &[u8]) -> Result<(), store::Error> {
        within(
            "value",
            value.len(),
            self.limits.max_value_bytes,
            "max-value-bytes",
        )
    }

    /// The guest's error for `failure` of `call`; a backend failure is
    /// logged.
    fn error(&self, call: &str, failure: Failure) -> store::Error {
        match failure {
            Failure::Rejected(message) => store::Error::Other(message),
            Failure::Backend(message) => {
                let line = format!(
                    "[keyvalue] {}/{} {call} failed: {message}\n",
                    self.store, self.namespace
                );
                #[cfg(test)]
                crate::wasm_host::tests::captured_log()
                    .lock()
                    .unwrap()
                    .extend_from_slice(line.as_bytes());
                eprint!("{line}");
                store::Error::Other(message)
            }
        }
    }
}

fn within(what: &str, len: usize, max: usize, limit: &str) -> Result<(), store::Error> {
    if len > max {
        return Err(store::Error::Other(format!(
            "the {what} is {len} bytes, more than the store's {limit} {max}"
        )));
    }
    Ok(())
}

/// A route's identifiers resolved to their stores.
pub struct Buckets(HashMap<String, Target>);

// ---------------------------------------------------------------------------
// The guest's view

/// The keyvalue state of a store: the route's buckets and the resource
/// table their handles live in.
pub struct KeyValueCtx<'a> {
    pub buckets: Option<&'a Buckets>,
    pub table: &'a mut ResourceTable,
}

struct KeyValue;

impl HasData for KeyValue {
    type Data<'a> = KeyValueCtx<'a>;
}

/// Links `store`, `atomics` and `batch`, served from the view `get` gives.
pub fn add_to_linker<T: Send + 'static>(
    linker: &mut Linker<T>,
    get: fn(&mut T) -> KeyValueCtx<'_>,
) -> wasmtime::Result<()> {
    bindings::Imports::add_to_linker::<T, KeyValue>(linker, get)
}

/// An open bucket: the guest's handle on its identifier's target.
pub struct Bucket(Target);

/// A compare-and-swap in progress: the value read when it began (or when
/// the last swap found it changed); the swap writes only while the key still
/// holds it.
pub struct Cas {
    target: Target,
    key: String,
    current: Option<Vec<u8>>,
}

impl KeyValueCtx<'_> {
    fn target(&self, bucket: &Resource<Bucket>) -> wasmtime::Result<Target> {
        Ok(self.table.get(bucket)?.0.clone())
    }
}

type Answer<T> = wasmtime::Result<Result<T, store::Error>>;

impl store::Host for KeyValueCtx<'_> {
    async fn open(&mut self, identifier: String) -> Answer<Resource<Bucket>> {
        let Some(target) = self.buckets.and_then(|buckets| buckets.0.get(&identifier)) else {
            return Ok(Err(store::Error::NoSuchStore));
        };
        let bucket = Bucket(target.clone());
        Ok(Ok(self.table.push(bucket)?))
    }
}

impl store::HostBucket for KeyValueCtx<'_> {
    async fn get(&mut self, bucket: Resource<Bucket>, key: String) -> Answer<Option<Vec<u8>>> {
        let target = self.target(&bucket)?;
        if let Err(e) = target.key(&key) {
            return Ok(Err(e));
        }
        let got = target.backend.get(&target.namespace, &key).await;
        Ok(got.map_err(|failure| target.error("get", failure)))
    }

    async fn set(&mut self, bucket: Resource<Bucket>, key: String, value: Vec<u8>) -> Answer<()> {
        let target = self.target(&bucket)?;
        if let Err(e) = target.key(&key).and_then(|()| target.value(&value)) {
            return Ok(Err(e));
        }
        let set = target.backend.set(&target.namespace, &key, value).await;
        Ok(set.map_err(|failure| target.error("set", failure)))
    }

    async fn delete(&mut self, bucket: Resource<Bucket>, key: String) -> Answer<()> {
        let target = self.target(&bucket)?;
        if let Err(e) = target.key(&key) {
            return Ok(Err(e));
        }
        let deleted = target.backend.delete(&target.namespace, &key).await;
        Ok(deleted.map_err(|failure| target.error("delete", failure)))
    }

    async fn exists(&mut self, bucket: Resource<Bucket>, key: String) -> Answer<bool> {
        let target = self.target(&bucket)?;
        if let Err(e) = target.key(&key) {
            return Ok(Err(e));
        }
        let exists = target.backend.exists(&target.namespace, &key).await;
        Ok(exists.map_err(|failure| target.error("exists", failure)))
    }

    async fn list_keys(
        &mut self,
        bucket: Resource<Bucket>,
        cursor: Option<String>,
    ) -> Answer<store::KeyResponse> {
        let target = self.target(&bucket)?;
        let listed = target.backend.list_keys(&target.namespace, cursor).await;
        Ok(listed
            .map(|(keys, cursor)| store::KeyResponse { keys, cursor })
            .map_err(|failure| target.error("list-keys", failure)))
    }

    async fn drop(&mut self, bucket: Resource<Bucket>) -> wasmtime::Result<()> {
        self.table.delete(bucket)?;
        Ok(())
    }
}

impl atomics::Host for KeyValueCtx<'_> {
    async fn increment(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
        delta: i64,
    ) -> Answer<i64> {
        let target = self.target(&bucket)?;
        if let Err(e) = target.key(&key) {
            return Ok(Err(e));
        }
        let sum = target
            .backend
            .increment(&target.namespace, &key, delta)
            .await;
        Ok(sum.map_err(|failure| target.error("increment", failure)))
    }

    async fn swap(
        &mut self,
        cas: Resource<Cas>,
        value: Vec<u8>,
    ) -> wasmtime::Result<Result<(), atomics::CasError>> {
        let cas = self.table.delete(cas)?;
        if let Err(e) = cas.target.value(&value) {
            return Ok(Err(atomics::CasError::StoreError(e)));
        }
        let swapped = cas
            .target
            .backend
            .swap(&cas.target.namespace, &cas.key, cas.current.clone(), value)
            .await;
        match swapped {
            Ok(Swap::Done) => Ok(Ok(())),
            Ok(Swap::Stale(current)) => {
                let retry = self.table.push(Cas { current, ..cas })?;
                Ok(Err(atomics::CasError::CasFailed(retry)))
            }
            Err(failure) => Ok(Err(atomics::CasError::StoreError(
                cas.target.error("swap", failure),
            ))),
        }
    }
}

impl atomics::HostCas for KeyValueCtx<'_> {
    async fn new(&mut self, bucket: Resource<Bucket>, key: String) -> Answer<Resource<Cas>> {
        let target = self.target(&bucket)?;
        if let Err(e) = target.key(&key) {
            return Ok(Err(e));
        }
        let current = match target.backend.get(&target.namespace, &key).await {
            Ok(current) => current,
            Err(failure) => return Ok(Err(target.error("cas", failure))),
        };
        let cas = Cas {
            target,
            key,
            current,
        };
        Ok(Ok(self.table.push(cas)?))
    }

    async fn current(&mut self, cas: Resource<Cas>) -> Answer<Option<Vec<u8>>> {
        Ok(Ok(self.table.get(&cas)?.current.clone()))
    }

    async fn drop(&mut self, cas: Resource<Cas>) -> wasmtime::Result<()> {
        self.table.delete(cas)?;
        Ok(())
    }
}

impl batch::Host for KeyValueCtx<'_> {
    async fn get_many(
        &mut self,
        bucket: Resource<Bucket>,
        keys: Vec<String>,
    ) -> Answer<Vec<(String, Option<Vec<u8>>)>> {
        let target = self.target(&bucket)?;
        if let Err(e) = keys.iter().try_for_each(|key| target.key(key)) {
            return Ok(Err(e));
        }
        let values = target.backend.get_many(&target.namespace, keys).await;
        Ok(values.map_err(|failure| target.error("get-many", failure)))
    }

    async fn set_many(
        &mut self,
        bucket: Resource<Bucket>,
        pairs: Vec<(String, Vec<u8>)>,
    ) -> Answer<()> {
        let target = self.target(&bucket)?;
        if let Err(e) = pairs
            .iter()
            .try_for_each(|(key, value)| target.key(key).and_then(|()| target.value(value)))
        {
            return Ok(Err(e));
        }
        let set = target.backend.set_many(&target.namespace, pairs).await;
        Ok(set.map_err(|failure| target.error("set-many", failure)))
    }

    async fn delete_many(&mut self, bucket: Resource<Bucket>, keys: Vec<String>) -> Answer<()> {
        let target = self.target(&bucket)?;
        if let Err(e) = keys.iter().try_for_each(|key| target.key(key)) {
            return Ok(Err(e));
        }
        let deleted = target.backend.delete_many(&target.namespace, keys).await;
        Ok(deleted.map_err(|failure| target.error("delete-many", failure)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RouteSource;
    use crate::routes::Reloader;
    use crate::routes::tests::Scratch;
    use crate::wasm_host::tests::{captured_log, exchange};
    use crate::wasm_host::{Handler, Instances, Sandbox};

    /// A store failing every call.
    struct Failing;

    #[async_trait::async_trait]
    impl Backend for Failing {
        async fn get(&self, _: &str, _: &str) -> Outcome<Option<Vec<u8>>> {
            Err(Failure::Backend("disk on fire".into()))
        }

        async fn set(&self, _: &str, _: &str, _: Vec<u8>) -> Outcome<()> {
            Err(Failure::Backend("disk on fire".into()))
        }

        async fn delete(&self, _: &str, _: &str) -> Outcome<()> {
            Err(Failure::Backend("disk on fire".into()))
        }

        async fn list_keys(
            &self,
            _: &str,
            _: Option<String>,
        ) -> Outcome<(Vec<String>, Option<String>)> {
            Err(Failure::Backend("disk on fire".into()))
        }

        async fn swap(&self, _: &str, _: &str, _: Option<Vec<u8>>, _: Vec<u8>) -> Outcome<Swap> {
            Err(Failure::Backend("disk on fire".into()))
        }
    }

    fn request(method: &str, path: &str, body: &str) -> String {
        format!(
            "{method} {path} HTTP/1.1\r\nhost: t\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// A store is opened (its file created) once a route maps to it. A
    /// route opens the namespaces its map names, a namespace two routes map
    /// to is shared, an identifier the map lacks is no store; cas reads again
    /// after a lost race; list-keys pages; the store's limits reject a key, a
    /// value and a batch past them (the batch as a whole); a value that is no
    /// integer does not increment. The pooled path serves the same. A store
    /// failing is the guest's `other`, logged.
    #[tokio::test]
    async fn a_route_reaches_the_namespaces_its_map_names_within_the_stores_limits() {
        let dir = Scratch::new("keyvalue");
        dir.guest("counter.wasm", "counter");
        let stores = "keyvalue:
  stores:
    local: {type: sqlite, path: kv.db, max-key-bytes: 16, max-value-bytes: 64, max-namespace-bytes: 8Ki}
";
        let path = dir.write("wicket.yaml", format!("{stores}routes: []\n"));
        let mut reloader = Reloader::start(RouteSource::file(&path).unwrap(), Instances::new(8))
            .await
            .unwrap();
        assert!(!dir.0.join("kv.db").exists(), "no route maps to it yet");
        dir.write(
            "wicket.yaml",
            format!(
                "{stores}routes:
  - host: a.local
    component: counter.wasm
    sandbox: {{wasi: [keyvalue], keyvalue: {{'': local/shared, own: local}}}}
  - host: b.local
    component: counter.wasm
    sandbox: {{wasi: [keyvalue], keyvalue: {{'': local/shared}}, pool: 1}}
"
            ),
        );
        reloader.reload(false).await.unwrap();
        assert!(dir.0.join("kv.db").exists());
        let a = reloader.routes().by_host("a.local").unwrap();
        let b = reloader.routes().by_host("b.local").unwrap();
        let get = async |handler, path| exchange(handler, &request("GET", path, "")).await;

        assert!(get(&a, "/").await.contains("hits 1\n"));
        assert!(get(&b, "/").await.contains("hits 2\n"), "shared");
        assert!(
            get(&a, "/?store=own").await.contains("hits 1\n"),
            "a.local's own"
        );
        assert!(
            get(&b, "/?store=own")
                .await
                .contains("open \"own\": Error::NoSuchStore")
        );
        assert!(
            get(&b, "/cas?race")
                .await
                .contains("cas-hits 2 after 2 attempt(s)\n")
        );

        let fill = exchange(&a, &request("POST", "/fill/250?store=own", "")).await;
        assert!(fill.contains("ok\n"), "{fill}");
        let keys = "251 keys in 3 page(s)\n";
        assert!(get(&a, "/keys?store=own").await.contains(keys));
        let over = [
            (
                request("GET", "/kv/a-seventeen-bytes", ""),
                "the key is 17 bytes, more than the store's max-key-bytes 16",
            ),
            (
                request("PUT", "/kv/k", &"v".repeat(65)),
                "the value is 65 bytes, more than the store's max-value-bytes 64",
            ),
            (
                request("POST", "/fill/1000?store=own", ""),
                "more than the store's max-namespace-bytes 8192",
            ),
        ];
        for (request, error) in over {
            let response = exchange(&a, &request).await;
            assert!(response.contains(error), "{response}");
        }
        assert!(
            get(&a, "/keys?store=own").await.contains(keys),
            "none of the batch"
        );

        exchange(&b, &request("PUT", "/kv/hits", "abc")).await;
        assert!(
            get(&a, "/")
                .await
                .contains("Error::Other(\"the value of 'hits' is not a decimal i64\")")
        );

        let failing = Target {
            backend: Arc::new(Failing),
            store: "local".into(),
            namespace: "broken.local".into(),
            limits: StoreLimits::default(),
        };
        let buckets = Buckets(HashMap::from([(String::new(), failing)]));
        let broken = Handler::new(
            a.loaded().clone(),
            Sandbox::default(),
            Some(Arc::new(buckets)),
            &Instances::new(4),
        )
        .unwrap();
        assert!(
            get(&broken, "/")
                .await
                .contains("increment: Error::Other(\"disk on fire\")")
        );
        let logged = String::from_utf8_lossy(&captured_log().lock().unwrap()).into_owned();
        assert!(
            logged.contains("[keyvalue] local/broken.local increment failed: disk on fire\n"),
            "{logged}"
        );
    }
}
