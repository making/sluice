//! The `gemfire` keyvalue store of the wickets: the namespaces of one region of a Tanzu
//! GemFire cluster, shared by the wickets on every host reaching it. The cluster is reached
//! through Tanzu GemFire for gRPC's service (`gemfire.v1.GemFire`) alone, the part of it
//! `proto/gemfire/v1/gemfire.proto` describes: any service answering those methods serves the
//! store, GemFire or not ([`mock`], feature `mock`, is one for the tests).
//!
//! A key is `<namespace>:<key>` (a string key of the region), a `:` or `\` in the namespace
//! escaped by `\`, so no namespace reaches another's keys; a value is bytes, and a key holding
//! a value of another type (another client's) is an error. A swap is `PutIfAbsent` /
//! `ReplaceIf` (the server compares the bytes), the batches are the bulk streams, and the
//! service has no way to list a namespace alone: a `list-keys` page and a purge read the
//! region's key set (`KeySetStream`) and keep the namespace's keys.
//!
//! The servers are named, or found through the locators' xDS ([`Endpoints`]); the calls go
//! round the servers, one failing set aside a while, a call failed over to another when its
//! request never left, or when it may be sent twice (all but the swaps). TLS verifies the
//! servers (and locators) against a CA or the webPKI roots, under a name of their
//! certificates if their addresses are not; a client certificate (mTLS) and a bearer token
//! (a `SecurityManager`) are sent when given. Each call answers within the store's timeout
//! or fails; one timing out has the next call to that server open a new connection.

mod client;
mod discovery;
#[cfg(feature = "mock")]
pub mod mock;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use bytes::Bytes;

use crate::client::Client;
use crate::proto::gemfire::v1::Value;
use crate::proto::gemfire::v1::value::ValueValue;

/// The generated messages and stubs.
pub mod proto {
    pub mod gemfire {
        pub mod v1 {
            tonic::include_proto!("gemfire.v1");
        }
    }
    pub mod envoy {
        pub mod config {
            pub mod core {
                pub mod v3 {
                    tonic::include_proto!("envoy.config.core.v3");
                }
            }
            pub mod endpoint {
                pub mod v3 {
                    tonic::include_proto!("envoy.config.endpoint.v3");
                }
            }
        }
        pub mod service {
            pub mod discovery {
                pub mod v3 {
                    tonic::include_proto!("envoy.service.discovery.v3");
                }
            }
        }
    }
    pub mod google {
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }
    }
}

/// Default of a store's `timeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// The keys one `RemoveStream` of a purge removes.
const PURGE_BATCH: usize = 1000;

/// How a store reaches its cluster.
#[derive(Debug)]
pub struct Settings {
    /// Its log lines begin with it (`keyvalue store 'gemfire'`).
    pub label: String,
    pub endpoints: Endpoints,
    pub region: String,
    /// None: plain TCP.
    pub tls: Option<Tls>,
    /// Sent as `authorization: Bearer <token>` on every call to the servers.
    pub token: Option<String>,
    /// The bound on each call, and on a discovery's connection and first answer.
    pub timeout: Duration,
}

/// The servers, `host:port` each.
#[derive(Clone, Debug, PartialEq)]
pub enum Endpoints {
    /// These, as they are.
    Servers(Vec<String>),
    /// Those the locators name by xDS (EDS `gemfire_grpc`), followed as they change; the
    /// locators are tried in order, the first answering followed.
    Locators(Vec<String>),
}

impl Endpoints {
    pub fn addresses(&self) -> &[String] {
        match self {
            Endpoints::Servers(addresses) | Endpoints::Locators(addresses) => addresses,
        }
    }
}

/// TLS to the servers and the locators.
#[derive(Debug)]
pub struct Tls {
    /// The CA of their certificates (PEM); None: the webPKI roots.
    pub ca_pem: Option<Vec<u8>>,
    /// The client's certificate and key (PEM): mTLS.
    pub client_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// The name verified in their certificates and sent as SNI; None: each address's host.
    pub server_name: Option<String>,
}

/// Why a call failed.
#[derive(Clone, Debug, PartialEq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// What a swap did.
#[derive(Debug, PartialEq)]
pub enum Swap {
    Done,
    /// The key no longer held the value expected: what it holds (None: absent).
    Stale(Option<Vec<u8>>),
}

/// The address `url` names (`grpc://host:port`, `grpcs://host:port`) and whether it is a
/// TLS one.
pub fn address(url: &str) -> Result<(String, bool), String> {
    let (rest, tls) = match (url.strip_prefix("grpcs://"), url.strip_prefix("grpc://")) {
        (Some(rest), _) => (rest, true),
        (_, Some(rest)) => (rest, false),
        _ => return Err(format!("'{url}' is no grpc:// or grpcs:// address")),
    };
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    let authority = match rest.parse::<tonic::codegen::http::uri::Authority>() {
        Ok(authority) if !rest.contains('@') => authority,
        _ => return Err(format!("'{url}' is no grpc(s)://host:port")),
    };
    if authority.port_u16().is_none() {
        return Err(format!("'{url}': no port"));
    }
    Ok((authority.as_str().to_string(), tls))
}

/// Which store the namespaces of `region` reached through `endpoints` are, for the wickets
/// to tell one shared store (`gemfire://<the addresses, sorted>/<region>`): wickets naming
/// one cluster's servers or locators alike list one identity.
pub fn identity(endpoints: &Endpoints, region: &str) -> String {
    let addresses: BTreeSet<&str> = endpoints.addresses().iter().map(String::as_str).collect();
    let addresses: Vec<&str> = addresses.into_iter().collect();
    format!("gemfire://{}/{region}", addresses.join(","))
}

/// One store: a region of the cluster, partitioned into namespaces.
pub struct Store {
    client: Client,
    region: String,
}

impl Store {
    /// A store on `settings`' cluster; it connects on the first call, the discovery (with
    /// locators) at once in the background, on the runtime it is opened on.
    pub fn open(settings: Settings) -> Result<Store, String> {
        let region = settings.region.clone();
        Ok(Store {
            client: Client::open(settings)?,
            region,
        })
    }

    pub async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let stored = self
            .client
            .get(&self.region, &entry(namespace, key))
            .await?;
        stored.map(|value| bytes(key, value)).transpose()
    }

    pub async fn set(&self, namespace: &str, key: &str, value: Vec<u8>) -> Result<(), Error> {
        let full = entry(namespace, key);
        self.client.put(&self.region, &full, value.into()).await
    }

    pub async fn delete(&self, namespace: &str, key: &str) -> Result<(), Error> {
        let full = entry(namespace, key);
        self.client.remove(&self.region, &full).await
    }

    pub async fn exists(&self, namespace: &str, key: &str) -> Result<bool, Error> {
        let full = entry(namespace, key);
        self.client.contains_key(&self.region, &full).await
    }

    /// Writes `value` only while the key holds `expected` (None: absent), atomically.
    pub async fn swap(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<Swap, Error> {
        let (region, full, value) = (&self.region, entry(namespace, key), value.into());
        let done = match expected {
            None => self.client.put_if_absent(region, &full, value).await?,
            Some(expected) => {
                let expected = expected.into();
                self.client
                    .replace_if(region, &full, expected, value)
                    .await?
            }
        };
        if done {
            return Ok(Swap::Done);
        }
        Ok(Swap::Stale(self.get(namespace, key).await?))
    }

    /// The first `page` keys of `namespace` after `cursor` (from the first without), in key
    /// order, and the cursor of the next page (its last key; None after the last page). Each
    /// page reads the region's key set.
    pub async fn list_keys(
        &self,
        namespace: &str,
        cursor: Option<String>,
        page: usize,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let space = space(namespace);
        let (space, cursor) = (space.as_str(), cursor.as_deref());
        let (keys, more) = self
            .client
            .keys(
                &self.region,
                || (BTreeSet::new(), false),
                |(keys, more): &mut (BTreeSet<String>, bool), key| {
                    let Some(key) = key.strip_prefix(space) else {
                        return;
                    };
                    if cursor.is_some_and(|cursor| key <= cursor) {
                        return;
                    }
                    keys.insert(key.to_string());
                    if keys.len() > page {
                        keys.pop_last();
                        *more = true;
                    }
                },
            )
            .await?;
        let next = match more {
            true => keys.last().cloned(),
            false => None,
        };
        Ok((keys.into_iter().collect(), next))
    }

    /// The values of `keys`, in their order, by one bulk read.
    pub async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<String>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let full: Vec<String> = keys.iter().map(|key| entry(namespace, key)).collect();
        let found: HashMap<String, Value> = self.client.get_all(&self.region, &full).await?;
        let mut values = Vec::with_capacity(keys.len());
        for (key, full) in keys.into_iter().zip(full) {
            // a key asked twice is answered twice
            let value = match found.get(&full) {
                Some(value) => Some(bytes(&key, value.clone())?),
                None => None,
            };
            values.push((key, value));
        }
        Ok(values)
    }

    /// One bulk write: not atomic, the pairs before a failure may stay written.
    pub async fn set_many(
        &self,
        namespace: &str,
        pairs: Vec<(String, Vec<u8>)>,
    ) -> Result<(), Error> {
        if pairs.is_empty() {
            return Ok(());
        }
        let entries: Vec<(String, Bytes)> = pairs
            .into_iter()
            .map(|(key, value)| (entry(namespace, &key), value.into()))
            .collect();
        self.client.put_all(&self.region, &entries).await
    }

    /// One bulk removal: not atomic, the keys before a failure may stay removed.
    pub async fn delete_many(&self, namespace: &str, keys: Vec<String>) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let full: Vec<String> = keys.iter().map(|key| entry(namespace, key)).collect();
        self.client.remove_all(&self.region, &full).await
    }

    /// Deletes every key of `namespace`: how many there were. The region's key set is read
    /// once, the namespace's keys removed a batch at a time (not atomic: a write meanwhile
    /// may stay).
    pub async fn purge(&self, namespace: &str) -> Result<u64, Error> {
        let space = space(namespace);
        let space = space.as_str();
        let keys = self
            .client
            .keys(&self.region, Vec::new, |keys: &mut Vec<String>, key| {
                if key.starts_with(space) {
                    keys.push(key);
                }
            })
            .await?;
        for batch in keys.chunks(PURGE_BATCH) {
            self.client.remove_all(&self.region, batch).await?;
        }
        Ok(keys.len() as u64)
    }
}

/// What every key of `namespace` begins with.
fn space(namespace: &str) -> String {
    let mut space = String::with_capacity(namespace.len() + 1);
    for c in namespace.chars() {
        if matches!(c, ':' | '\\') {
            space.push('\\');
        }
        space.push(c);
    }
    space.push(':');
    space
}

/// The region's key of `key` in `namespace`.
fn entry(namespace: &str, key: &str) -> String {
    space(namespace) + key
}

/// The bytes `key` holds: a value of another type is no value of the store's.
fn bytes(key: &str, value: Value) -> Result<Vec<u8>, Error> {
    let kind = match value.value_value {
        Some(ValueValue::Bytes(bytes)) => return Ok(bytes.into()),
        Some(ValueValue::String(_)) => "string",
        Some(ValueValue::Int32(_)) => "int32",
        Some(ValueValue::Int64(_)) => "int64",
        Some(ValueValue::Bool(_)) => "bool",
        Some(ValueValue::Double(_)) => "double",
        Some(ValueValue::Float(_)) => "float",
        Some(ValueValue::Any(_)) => "google.protobuf.Any",
        None => "unknown",
    };
    Err(Error(format!(
        "'{key}' holds a {kind} value, not bytes: another client's"
    )))
}

#[cfg(all(test, feature = "mock"))]
mod tests;
