//! The `gemfire` store: the namespaces of one region of a Tanzu GemFire
//! cluster, shared by the wickets on every host reaching it, through the
//! cluster's gRPC service (`sluice-wicket-keyvalue-gemfire`: any service
//! answering its methods serves the store). A key is `<namespace>:<key>` in
//! the region, a `:` or `\` in the namespace escaped by `\`: no namespace
//! reaches another's keys. A swap is the service's `PutIfAbsent` /
//! `ReplaceIf`, an increment the trait's loop over it, a batch one bulk
//! stream; the service lists no namespace alone, so a `list-keys` page and a
//! purge read the region's key set (they cost with the region).
//!
//! The servers named, or those the locators name (xDS), each call going
//! round them: one failing is set aside a while, a call moves to another
//! when its request never left, or when it may be sent twice (all but a
//! swap). Every call is bounded by the store's `timeout`; one timing out has
//! the next call to that server open a new connection.
//!
//! No namespace quota: the cluster bounds the region; the store's
//! `max-key-bytes` / `max-value-bytes` bound each call (a value within the
//! servers' bound on a message, 4 MiB by default).

use std::path::PathBuf;
use std::time::Duration;

use sluice_wicket_keyvalue_gemfire::{self as cluster, Settings, Store};

pub use sluice_wicket_keyvalue_gemfire::{DEFAULT_TIMEOUT, Endpoints, address, identity};

use super::{Backend, Failure, Outcome, PAGE_KEYS, Swap};

/// A `grpcs://` store's TLS: the CA of the certificates (None: the webPKI
/// roots), the client's certificate and key (mTLS), the name verified in
/// the certificates (None: each address's host).
#[derive(Clone, PartialEq, Debug)]
pub struct Tls {
    pub ca_cert: Option<PathBuf>,
    pub client: Option<(PathBuf, PathBuf)>,
    pub server_name: Option<String>,
}

pub struct Gemfire {
    store: Store,
    endpoints: Endpoints,
    region: String,
}

impl From<cluster::Error> for Failure {
    fn from(e: cluster::Error) -> Self {
        Failure::Backend(e.0)
    }
}

impl Gemfire {
    /// A store of `region` (its TLS files read now); it connects on the
    /// first call, to the locators at once in the background. `label`
    /// begins its log lines.
    pub fn open(
        label: &str,
        endpoints: &Endpoints,
        region: &str,
        tls: &Option<Tls>,
        token: &Option<String>,
        timeout: Duration,
    ) -> Result<Gemfire, String> {
        let read = |key: &str, path: &PathBuf| {
            std::fs::read(path).map_err(|e| format!("{key} {}: {e}", path.display()))
        };
        let tls = match tls {
            Some(tls) => Some(cluster::Tls {
                ca_pem: match &tls.ca_cert {
                    Some(path) => Some(read("ca-cert", path)?),
                    None => None,
                },
                client_pem: match &tls.client {
                    Some((cert, key)) => {
                        Some((read("client-cert", cert)?, read("client-key", key)?))
                    }
                    None => None,
                },
                server_name: tls.server_name.clone(),
            }),
            None => None,
        };
        let store = Store::open(Settings {
            label: label.to_string(),
            endpoints: endpoints.clone(),
            region: region.to_string(),
            tls,
            token: token.clone(),
            timeout,
        })?;
        Ok(Gemfire {
            store,
            endpoints: endpoints.clone(),
            region: region.to_string(),
        })
    }

    /// Where it connects, for the log.
    pub fn describe(&self) -> String {
        let (kind, addresses) = match &self.endpoints {
            Endpoints::Servers(addresses) => ("servers", addresses),
            Endpoints::Locators(addresses) => ("locators", addresses),
        };
        let addresses = addresses.join(", ");
        format!("{kind} {addresses} (region '{}')", self.region)
    }
}

#[async_trait::async_trait]
impl Backend for Gemfire {
    async fn get(&self, namespace: &str, key: &str) -> Outcome<Option<Vec<u8>>> {
        Ok(self.store.get(namespace, key).await?)
    }

    async fn set(&self, namespace: &str, key: &str, value: Vec<u8>) -> Outcome<()> {
        Ok(self.store.set(namespace, key, value).await?)
    }

    async fn delete(&self, namespace: &str, key: &str) -> Outcome<()> {
        Ok(self.store.delete(namespace, key).await?)
    }

    /// A page of the namespace's keys in key order, the cursor the last key
    /// of the page before.
    async fn list_keys(
        &self,
        namespace: &str,
        cursor: Option<String>,
    ) -> Outcome<(Vec<String>, Option<String>)> {
        Ok(self.store.list_keys(namespace, cursor, PAGE_KEYS).await?)
    }

    async fn swap(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Outcome<Swap> {
        Ok(
            match self.store.swap(namespace, key, expected, value).await? {
                cluster::Swap::Done => Swap::Done,
                cluster::Swap::Stale(current) => Swap::Stale(current),
            },
        )
    }

    async fn exists(&self, namespace: &str, key: &str) -> Outcome<bool> {
        Ok(self.store.exists(namespace, key).await?)
    }

    /// One `GetStream`.
    async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<String>,
    ) -> Outcome<Vec<(String, Option<Vec<u8>>)>> {
        Ok(self.store.get_many(namespace, keys).await?)
    }

    /// One `PutStream`: not atomic.
    async fn set_many(&self, namespace: &str, pairs: Vec<(String, Vec<u8>)>) -> Outcome<()> {
        Ok(self.store.set_many(namespace, pairs).await?)
    }

    /// One `RemoveStream`: not atomic.
    async fn delete_many(&self, namespace: &str, keys: Vec<String>) -> Outcome<()> {
        Ok(self.store.delete_many(namespace, keys).await?)
    }

    async fn purge(&self, namespace: &str) -> Outcome<u64> {
        Ok(self.store.purge(namespace).await?)
    }
}
