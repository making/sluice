//! The `redis` store: the keys of a Redis (or Valkey) server, shared by the
//! wickets on every host reaching it. A key is `<prefix><namespace>:<key>`,
//! a `:` or `\` in the namespace escaped by `\`: no namespace reaches
//! another's keys. The operations are the server's own (`INCRBY`, `MGET`,
//! ...), a swap a script, so each is atomic; a purge is `SCAN` + `DEL` of the
//! namespace's keys, a batch at a time.
//!
//! One multiplexed connection per store, speaking RESP3: a server closing it
//! is seen at once and it reconnects in the background. Every call is
//! bounded by the store's `timeout`; a call timing out leaves its connection
//! suspect, so the next call opens a new one (a dropped call harms no
//! other: the connection reads its answer and drops it).
//!
//! No namespace quota: `maxmemory` bounds the server; the store's
//! `max-key-bytes` / `max-value-bytes` bound each call.

use std::sync::Mutex;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{
    Client, Cmd, ConnectionAddr, ErrorKind, FromRedisValue, IntoConnectionInfo as _,
    ProtocolVersion, RedisError, RedisResult, ServerErrorKind, TlsCertificates, Value, cmd,
};

use super::{Backend, Failure, Outcome, PAGE_KEYS, Swap, not_integer, overflows};

/// Default of a `redis` store's `timeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// The keys one `SCAN` of a purge looks at.
const PURGE_SCAN: usize = 1000;

/// Writes ARGV[1] while KEYS[1] holds ARGV[2] (is absent without one):
/// 1 when written, else what it holds (0: nothing).
const SWAP: &str = "local current = redis.call('GET', KEYS[1])
if current == (ARGV[2] or false) then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
return current or 0";

pub struct Redis {
    client: Client,
    timeout: Duration,
    prefix: String,
    /// The connection and its generation, renewed after a timeout.
    connection: Mutex<(u64, ConnectionManager)>,
}

impl From<RedisError> for Failure {
    fn from(e: RedisError) -> Self {
        Failure::Backend(e.to_string())
    }
}

/// Whether a store's `url` is a TLS one (`rediss://`), checked without
/// connecting.
pub fn check(url: &str) -> Result<bool, String> {
    let info = url.into_connection_info().map_err(|e| e.to_string())?;
    Ok(matches!(info.addr(), ConnectionAddr::TcpTls { .. }))
}

impl Redis {
    /// A store on the server at `url` (a `rediss://` one's certificate
    /// verified against `ca_pem`, else the webPKI roots), its keys after
    /// `prefix`; it connects on the first call.
    pub fn open(
        url: &str,
        ca_pem: Option<Vec<u8>>,
        prefix: &str,
        timeout: Duration,
    ) -> Result<Redis, String> {
        let client = client(url, ca_pem).map_err(|e| e.to_string())?;
        let connection = connect(&client, timeout).map_err(|e| e.to_string())?;
        Ok(Redis {
            client,
            timeout,
            prefix: prefix.to_string(),
            connection: Mutex::new((0, connection)),
        })
    }

    /// Where it connects, for the log: `host:port/db` (no credentials).
    pub fn address(&self) -> String {
        let info = self.client.get_connection_info();
        format!("{}/{}", info.addr(), info.redis_settings().db())
    }

    /// The store's key of `key` in `namespace`.
    fn key(&self, namespace: &str, key: &str) -> String {
        self.space(namespace) + key
    }

    /// What every key of `namespace` begins with.
    fn space(&self, namespace: &str) -> String {
        let mut space = self.prefix.clone();
        for c in namespace.chars() {
            if matches!(c, ':' | '\\') {
                space.push('\\');
            }
            space.push(c);
        }
        space.push(':');
        space
    }

    /// The `SCAN` pattern matching the keys of `space` alone: its glob
    /// characters escaped.
    fn pattern(space: &str) -> String {
        let mut pattern = String::with_capacity(space.len() + 1);
        for c in space.chars() {
            if matches!(c, '*' | '?' | '[' | ']' | '\\') {
                pattern.push('\\');
            }
            pattern.push(c);
        }
        pattern.push('*');
        pattern
    }

    /// `command`'s answer within the timeout.
    async fn call<T: FromRedisValue>(&self, command: &Cmd) -> RedisResult<T> {
        let (generation, mut connection) = {
            let current = self.connection.lock().unwrap_or_else(|e| e.into_inner());
            (current.0, current.1.clone())
        };
        match tokio::time::timeout(self.timeout, command.query_async(&mut connection)).await {
            Ok(answer) => answer,
            Err(_) => {
                self.renew(generation);
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("no answer within {:?}", self.timeout),
                )
                .into())
            }
        }
    }

    /// Replaces the connection of `generation` (not one a call timing out at
    /// the same time replaced already).
    fn renew(&self, generation: u64) {
        let mut current = self.connection.lock().unwrap_or_else(|e| e.into_inner());
        if current.0 == generation
            && let Ok(fresh) = connect(&self.client, self.timeout)
        {
            *current = (generation + 1, fresh);
        }
    }
}

/// The client of `url`, speaking RESP3.
fn client(url: &str, ca_pem: Option<Vec<u8>>) -> RedisResult<Client> {
    let info = url.into_connection_info()?;
    let settings = info
        .redis_settings()
        .clone()
        .set_protocol(ProtocolVersion::RESP3);
    let info = info.set_redis_settings(settings);
    match ca_pem {
        Some(pem) => Client::build_with_tls(
            info,
            TlsCertificates {
                client_tls: None,
                root_cert: Some(pem),
            },
        ),
        None => Client::open(info),
    }
}

/// A connection opened on the first call, each attempt bounded by
/// `timeout`; one lost is opened again. Two quick retries: a server that
/// does not take it fails the calls with why (refused, a certificate, ...),
/// not with their timeout, and the next call tries again.
fn connect(client: &Client, timeout: Duration) -> RedisResult<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(timeout))
        .set_number_of_retries(2);
    ConnectionManager::new_lazy_with_config(client.clone(), config)
}

#[async_trait::async_trait]
impl Backend for Redis {
    async fn get(&self, namespace: &str, key: &str) -> Outcome<Option<Vec<u8>>> {
        Ok(self.call(cmd("GET").arg(self.key(namespace, key))).await?)
    }

    async fn set(&self, namespace: &str, key: &str, value: Vec<u8>) -> Outcome<()> {
        Ok(self
            .call(cmd("SET").arg(self.key(namespace, key)).arg(value))
            .await?)
    }

    async fn delete(&self, namespace: &str, key: &str) -> Outcome<()> {
        Ok(self.call(cmd("DEL").arg(self.key(namespace, key))).await?)
    }

    /// One `SCAN` of the namespace's keys, the cursor SCAN's: a page may be
    /// empty before the last, and a key may come twice.
    async fn list_keys(
        &self,
        namespace: &str,
        cursor: Option<String>,
    ) -> Outcome<(Vec<String>, Option<String>)> {
        let start: u64 = match &cursor {
            None => 0,
            Some(cursor) => cursor
                .parse()
                .map_err(|_| Failure::Rejected(format!("'{cursor}' is no cursor of this store")))?,
        };
        let space = self.space(namespace);
        let (next, keys): (u64, Vec<Vec<u8>>) = self
            .call(
                cmd("SCAN")
                    .arg(start)
                    .arg("MATCH")
                    .arg(Redis::pattern(&space))
                    .arg("COUNT")
                    .arg(PAGE_KEYS),
            )
            .await?;
        let keys = keys
            .into_iter()
            .filter_map(|key| Some(String::from_utf8(key).ok()?.strip_prefix(&space)?.into()))
            .collect();
        Ok((keys, (next != 0).then(|| next.to_string())))
    }

    async fn swap(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Outcome<Swap> {
        let mut command = cmd("EVAL");
        command
            .arg(SWAP)
            .arg(1)
            .arg(self.key(namespace, key))
            .arg(value)
            .arg(expected);
        match self.call(&command).await? {
            Value::Int(1) => Ok(Swap::Done),
            Value::Int(0) => Ok(Swap::Stale(None)),
            Value::BulkString(current) => Ok(Swap::Stale(Some(current))),
            other => Err(Failure::Backend(format!("swap answered {other:?}"))),
        }
    }

    async fn exists(&self, namespace: &str, key: &str) -> Outcome<bool> {
        Ok(self
            .call(cmd("EXISTS").arg(self.key(namespace, key)))
            .await?)
    }

    /// `INCRBY`: Redis's rule for the decimal ASCII i64 the key holds.
    async fn increment(&self, namespace: &str, key: &str, delta: i64) -> Outcome<i64> {
        let incremented = self
            .call(cmd("INCRBY").arg(self.key(namespace, key)).arg(delta))
            .await;
        incremented.map_err(|e| {
            let detail = e.detail().unwrap_or_default();
            match e.kind() {
                ErrorKind::Server(ServerErrorKind::ResponseError)
                    if detail.contains("not an integer") =>
                {
                    Failure::Rejected(not_integer(key))
                }
                ErrorKind::Server(ServerErrorKind::ResponseError)
                    if detail.contains("overflow") =>
                {
                    Failure::Rejected(overflows(key))
                }
                _ => e.into(),
            }
        })
    }

    /// `MGET`: the values as of one moment.
    async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<String>,
    ) -> Outcome<Vec<(String, Option<Vec<u8>>)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut command = cmd("MGET");
        for key in &keys {
            command.arg(self.key(namespace, key));
        }
        let values: Vec<Option<Vec<u8>>> = self.call(&command).await?;
        Ok(keys.into_iter().zip(values).collect())
    }

    /// `MSET`: all at once.
    async fn set_many(&self, namespace: &str, pairs: Vec<(String, Vec<u8>)>) -> Outcome<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut command = cmd("MSET");
        for (key, value) in pairs {
            command.arg(self.key(namespace, &key)).arg(value);
        }
        Ok(self.call(&command).await?)
    }

    /// `DEL`: all at once.
    async fn delete_many(&self, namespace: &str, keys: Vec<String>) -> Outcome<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut command = cmd("DEL");
        for key in &keys {
            command.arg(self.key(namespace, key));
        }
        Ok(self.call(&command).await?)
    }

    /// A `SCAN` of the namespace's keys, each batch found deleted by `DEL`,
    /// until the scan ends.
    async fn purge(&self, namespace: &str) -> Outcome<u64> {
        let pattern = Redis::pattern(&self.space(namespace));
        let (mut cursor, mut deleted) = (0u64, 0u64);
        loop {
            let (next, keys): (u64, Vec<Vec<u8>>) = self
                .call(
                    cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&pattern)
                        .arg("COUNT")
                        .arg(PURGE_SCAN),
                )
                .await?;
            if !keys.is_empty() {
                let mut command = cmd("DEL");
                for key in &keys {
                    command.arg(key);
                }
                deleted += self.call::<u64>(&command).await?;
            }
            if next == 0 {
                return Ok(deleted);
            }
            cursor = next;
        }
    }
}

#[cfg(test)]
#[path = "../../tests/support/redis_server.rs"]
mod redis_server;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Instant;

    use redis::aio::MultiplexedConnection;

    use super::redis_server::RedisServer;
    use super::*;

    /// Every key of `namespace`, page by page, and the pages.
    async fn listed(store: &Redis, namespace: &str) -> (BTreeSet<String>, usize) {
        let (mut keys, mut pages, mut cursor) = (BTreeSet::new(), 0, None);
        loop {
            let (page, next) = store.list_keys(namespace, cursor).await.unwrap();
            keys.extend(page);
            pages += 1;
            cursor = next;
            if cursor.is_none() {
                return (keys, pages);
            }
        }
    }

    /// The connections the server accepted so far.
    async fn accepted(side: &mut MultiplexedConnection) -> u64 {
        let info: String = cmd("INFO").arg("stats").query_async(side).await.unwrap();
        info.lines()
            .find_map(|line| line.strip_prefix("total_connections_received:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    /// The namespaces stay apart, after the prefix (a `:` or a glob
    /// character in one reaches no other's keys, listed or purged); a swap
    /// writes only over the value expected, increment is `INCRBY` and
    /// refuses what is no integer as the trait's does; list-keys pages
    /// through more keys than one SCAN gives; the batches are whole. A call
    /// timing out has the next one open a new connection; the server stopped
    /// fails the calls (not the opening of a store), started again they
    /// succeed.
    #[tokio::test]
    async fn a_redis_store_keeps_its_namespaces_apart_on_a_real_server() {
        let mut server = RedisServer::unix();
        let store = Redis::open(&server.url, None, "w:", DEFAULT_TIMEOUT).unwrap();
        // waits out the pause below
        let patient = redis::AsyncConnectionConfig::new().set_response_timeout(None);
        let mut side = Client::open(server.url.as_str())
            .unwrap()
            .get_multiplexed_async_connection_with_config(&patient)
            .await
            .unwrap();

        for (namespace, key, value) in [("a", "b:c", "1"), ("a:b", "c", "2"), ("a*", "k", "3")] {
            store.set(namespace, key, value.into()).await.unwrap();
        }
        let mut keys: Vec<String> = cmd("KEYS").arg("*").query_async(&mut side).await.unwrap();
        keys.sort();
        assert_eq!(keys, ["w:a*:k", "w:a:b:c", r"w:a\:b:c"]);
        assert_eq!(store.get("a:b", "c").await, Ok(Some(b"2".to_vec())));
        assert_eq!(store.get("a", "b:c").await, Ok(Some(b"1".to_vec())));
        assert_eq!(store.get("b", "c").await, Ok(None));
        for (namespace, key) in [("a", "b:c"), ("a:b", "c"), ("a*", "k")] {
            let (keys, _) = listed(&store, namespace).await;
            assert_eq!(keys, BTreeSet::from([key.to_string()]), "{namespace}");
        }
        assert_eq!(store.purge("a*").await, Ok(1));
        assert_eq!(store.purge("a*").await, Ok(0));
        let mut keys: Vec<String> = cmd("KEYS").arg("*").query_async(&mut side).await.unwrap();
        keys.sort();
        assert_eq!(keys, ["w:a:b:c", r"w:a\:b:c"], "no other namespace's");

        assert_eq!(
            store.swap("s", "k", None, b"1".into()).await,
            Ok(Swap::Done)
        );
        assert_eq!(
            store.swap("s", "k", None, b"2".into()).await,
            Ok(Swap::Stale(Some(b"1".into())))
        );
        assert_eq!(
            store.swap("s", "k", Some(b"1".into()), b"2".into()).await,
            Ok(Swap::Done)
        );
        assert_eq!(
            store.swap("s", "k", Some(b"1".into()), b"3".into()).await,
            Ok(Swap::Stale(Some(b"2".into())))
        );
        assert_eq!(store.exists("s", "k").await, Ok(true));
        store.delete("s", "k").await.unwrap();
        assert_eq!(store.exists("s", "k").await, Ok(false));
        assert_eq!(
            store.swap("s", "k", Some(b"2".into()), b"3".into()).await,
            Ok(Swap::Stale(None))
        );

        assert_eq!(store.increment("i", "n", 5).await, Ok(5));
        assert_eq!(store.increment("i", "n", -7).await, Ok(-2));
        store.set("i", "text", b"abc".into()).await.unwrap();
        store
            .set("i", "max", i64::MAX.to_string().into())
            .await
            .unwrap();
        assert_eq!(
            store.increment("i", "text", 1).await,
            Err(Failure::Rejected(not_integer("text")))
        );
        assert_eq!(
            store.increment("i", "max", 1).await,
            Err(Failure::Rejected(overflows("max")))
        );

        let pairs: Vec<_> = (0..250)
            .map(|n| (format!("key-{n:03}"), n.to_string().into_bytes()))
            .collect();
        store.set_many("big", pairs.clone()).await.unwrap();
        let (keys, pages) = listed(&store, "big").await;
        assert_eq!(keys, pairs.iter().map(|(key, _)| key.clone()).collect());
        assert!(pages > 1, "{pages} page(s)");
        assert_eq!(
            store
                .get_many("big", vec!["key-007".into(), "none".into()])
                .await,
            Ok(vec![
                ("key-007".into(), Some(b"7".into())),
                ("none".into(), None)
            ])
        );
        store
            .delete_many("big", pairs.into_iter().map(|(key, _)| key).collect())
            .await
            .unwrap();
        assert_eq!(listed(&store, "big").await, (BTreeSet::new(), 1));
        assert_eq!(store.get_many("big", Vec::new()).await, Ok(Vec::new()));
        assert_eq!(store.set_many("big", Vec::new()).await, Ok(()));
        assert_eq!(store.delete_many("big", Vec::new()).await, Ok(()));
        assert_eq!(
            store.list_keys("big", Some("x".into())).await,
            Err(Failure::Rejected("'x' is no cursor of this store".into()))
        );

        let impatient = Redis::open(&server.url, None, "w:", Duration::from_millis(300)).unwrap();
        assert_eq!(impatient.get("a:b", "c").await, Ok(Some(b"2".to_vec())));
        let before = accepted(&mut side).await;
        let () = cmd("CLIENT")
            .arg(&["PAUSE", "3000", "ALL"])
            .query_async(&mut side)
            .await
            .unwrap();
        assert_eq!(
            impatient.get("a:b", "c").await,
            Err(Failure::Backend("no answer within 300ms".into()))
        );
        // answered once the pause ends
        let () = cmd("PING").query_async(&mut side).await.unwrap();
        assert_eq!(impatient.get("a:b", "c").await, Ok(Some(b"2".to_vec())));
        assert_eq!(accepted(&mut side).await, before + 1, "a new connection");

        server.stop();
        assert!(matches!(
            store.get("a:b", "c").await,
            Err(Failure::Backend(_))
        ));
        assert!(
            Redis::open(&server.url, None, "", DEFAULT_TIMEOUT).is_ok(),
            "opened without connecting"
        );
        server.start();
        let deadline = Instant::now() + Duration::from_secs(30);
        while store.get("a:b", "c").await != Ok(None) {
            assert!(Instant::now() < deadline, "not connected again");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
