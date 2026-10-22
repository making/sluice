//! The SQLite alternative to the redb backend: one table `kv(bucket, key, value)`, WAL.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, params};

use super::{Backend, BackendResult, PAGE, Swap};

pub struct SqliteBackend {
    pub conn: Arc<Mutex<Connection>>,
}

impl SqliteBackend {
    pub fn open(path: &str) -> Self {
        let conn = Connection::open(path).expect("open sqlite");
        conn.execute_batch(
            "PRAGMA busy_timeout=5000;
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS kv (
                 bucket TEXT NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL,
                 PRIMARY KEY (bucket, key)) WITHOUT ROWID;",
        )
        .expect("schema");
        Self { conn: Arc::new(Mutex::new(conn)) }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> BackendResult<T> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || f(&mut conn.lock().unwrap()))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())
    }
}

fn read(conn: &Connection, bucket: &str, key: &str) -> rusqlite::Result<Option<Vec<u8>>> {
    conn.query_row(
        "SELECT value FROM kv WHERE bucket = ?1 AND key = ?2",
        params![bucket, key],
        |row| row.get(0),
    )
    .optional()
}

#[async_trait::async_trait]
impl Backend for SqliteBackend {
    async fn get(&self, bucket: &str, key: &str) -> BackendResult<Option<Vec<u8>>> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |conn| read(conn, &bucket, &key)).await
    }

    async fn set(&self, bucket: &str, key: &str, value: Vec<u8>) -> BackendResult<()> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |conn| {
            conn.execute(
                "INSERT INTO kv (bucket, key, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT (bucket, key) DO UPDATE SET value = excluded.value",
                params![bucket, key, value],
            )
            .map(|_| ())
        })
        .await
    }

    async fn delete(&self, bucket: &str, key: &str) -> BackendResult<()> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |conn| {
            conn.execute("DELETE FROM kv WHERE bucket = ?1 AND key = ?2", params![bucket, key])
                .map(|_| ())
        })
        .await
    }

    async fn list_keys(
        &self,
        bucket: &str,
        cursor: Option<String>,
    ) -> BackendResult<(Vec<String>, Option<String>)> {
        let bucket = bucket.to_string();
        self.blocking(move |conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT key FROM kv WHERE bucket = ?1 AND key > ?2 ORDER BY key LIMIT ?3",
            )?;
            let mut keys = stmt
                .query_map(params![bucket, cursor.unwrap_or_default(), (PAGE + 1) as i64], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
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
        let outcome = self
            .blocking(move |conn| {
                let txn = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let current = read(&txn, &bucket, &key)?;
                let next = match current {
                    None => Some(delta),
                    Some(v) => std::str::from_utf8(&v)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .and_then(|n| n.checked_add(delta)),
                };
                if let Some(next) = next {
                    txn.execute(
                        "INSERT INTO kv (bucket, key, value) VALUES (?1, ?2, ?3)
                         ON CONFLICT (bucket, key) DO UPDATE SET value = excluded.value",
                        params![bucket, key, next.to_string().into_bytes()],
                    )?;
                    txn.commit()?;
                }
                Ok(next)
            })
            .await?;
        outcome.ok_or_else(|| "value is not an integer, or overflow".to_string())
    }

    async fn swap(
        &self,
        bucket: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> BackendResult<Swap> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |conn| {
            let txn = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let current = read(&txn, &bucket, &key)?;
            if current != expected {
                return Ok(Swap::Stale(current));
            }
            txn.execute(
                "INSERT INTO kv (bucket, key, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT (bucket, key) DO UPDATE SET value = excluded.value",
                params![bucket, key, value],
            )?;
            txn.commit()?;
            Ok(Swap::Done)
        })
        .await
    }
}
