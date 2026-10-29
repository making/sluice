//! The `sqlite` store: one SQLite file on the host, a table
//! `kv(namespace, key, value)`. WAL with `synchronous=NORMAL`: a power loss
//! may drop the last commits, never corrupts the file. Several wickets on one
//! host may share the file; a statement waits up to `BUSY_TIMEOUT` for
//! another's lock. The calls block, so they run on the blocking pool.
//!
//! A namespace holds at most `max_namespace_bytes` of keys and values, so a
//! guest cannot fill the host's disk: triggers keep each namespace's bytes in
//! `usage` (whichever wicket writes), and a write taking it past the bound is
//! rolled back. A purge deletes a namespace's rows and its `usage` row.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use super::{Backend, Failure, Outcome, PAGE_KEYS, Swap, add};

/// How long a statement waits for the lock another connection holds.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The file's format, kept in `user_version`: a newer one is refused.
const FORMAT: i32 = 1;

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS kv (
        namespace TEXT NOT NULL,
        key TEXT NOT NULL,
        value BLOB NOT NULL,
        PRIMARY KEY (namespace, key)
    ) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS usage (
        namespace TEXT PRIMARY KEY,
        bytes INTEGER NOT NULL
    ) WITHOUT ROWID;
    CREATE TRIGGER IF NOT EXISTS kv_inserted AFTER INSERT ON kv BEGIN
        INSERT OR IGNORE INTO usage (namespace, bytes) VALUES (new.namespace, 0);
        UPDATE usage SET bytes = bytes + octet_length(new.key) + octet_length(new.value)
            WHERE namespace = new.namespace;
    END;
    CREATE TRIGGER IF NOT EXISTS kv_updated AFTER UPDATE ON kv BEGIN
        UPDATE usage SET bytes = bytes - octet_length(old.value) + octet_length(new.value)
            WHERE namespace = new.namespace;
    END;
    CREATE TRIGGER IF NOT EXISTS kv_deleted AFTER DELETE ON kv BEGIN
        UPDATE usage SET bytes = bytes - octet_length(old.key) - octet_length(old.value)
            WHERE namespace = old.namespace;
    END;";

const UPSERT: &str = "INSERT INTO kv (namespace, key, value) VALUES (?1, ?2, ?3) \
                      ON CONFLICT (namespace, key) DO UPDATE SET value = excluded.value";

pub struct Sqlite {
    connection: Arc<Mutex<Connection>>,
    max_namespace_bytes: u64,
}

impl From<rusqlite::Error> for Failure {
    fn from(e: rusqlite::Error) -> Self {
        Failure::Backend(e.to_string())
    }
}

impl Sqlite {
    /// Opens (creates) the file at `path`, its namespaces bounded to
    /// `max_namespace_bytes`. Blocks.
    pub fn open(path: &Path, max_namespace_bytes: u64) -> Result<Sqlite, String> {
        let failed = |e: rusqlite::Error| format!("{}: {e}", path.display());
        let mut connection = Connection::open(path).map_err(failed)?;
        connection.busy_timeout(BUSY_TIMEOUT).map_err(failed)?;
        // Switching a new file to WAL does not use the busy handler: a second
        // opener at the same moment gets SQLITE_BUSY at once, so it retries.
        let deadline = Instant::now() + BUSY_TIMEOUT;
        loop {
            let mode = connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| {
                row.get::<_, String>(0)
            });
            match mode {
                Ok(mode) if mode.eq_ignore_ascii_case("wal") => break,
                Ok(mode) => {
                    return Err(format!(
                        "{}: journal mode {mode} instead of wal",
                        path.display()
                    ));
                }
                Err(e) if busy(&e) && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(e) => return Err(failed(e)),
            }
        }
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(failed)?;
        let schema = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        let format: i32 = schema
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(failed)?;
        if format > FORMAT {
            return Err(format!(
                "{}: format {format} of a newer sluice-wicket-wasi (this one reads {FORMAT})",
                path.display()
            ));
        }
        schema.execute_batch(SCHEMA).map_err(failed)?;
        schema
            .pragma_update(None, "user_version", FORMAT)
            .map_err(failed)?;
        schema.commit().map_err(failed)?;
        Ok(Sqlite {
            connection: Arc::new(Mutex::new(connection)),
            max_namespace_bytes,
        })
    }

    /// Runs `write` in an immediate transaction (it takes the write lock at
    /// once: a read in it is current), committed unless it fails or takes
    /// `namespace` past `max_namespace_bytes`.
    async fn writing<T: Send + 'static>(
        &self,
        namespace: &str,
        write: impl FnOnce(&Transaction, &str) -> Outcome<T> + Send + 'static,
    ) -> Outcome<T> {
        let (namespace, max) = (namespace.to_string(), self.max_namespace_bytes);
        self.blocking(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let written = write(&transaction, &namespace)?;
            let bytes: i64 = transaction
                .prepare_cached("SELECT bytes FROM usage WHERE namespace = ?1")?
                .query_row(params![namespace], |row| row.get(0))
                .optional()?
                .unwrap_or(0);
            if bytes as u64 > max {
                return Err(Failure::Rejected(format!(
                    "the namespace would hold {bytes} bytes, more than the store's \
                     max-namespace-bytes {max}"
                )));
            }
            transaction.commit()?;
            Ok(written)
        })
        .await
    }

    /// Runs `call` on the connection, on the blocking pool.
    async fn blocking<T: Send + 'static>(
        &self,
        call: impl FnOnce(&mut Connection) -> Outcome<T> + Send + 'static,
    ) -> Outcome<T> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            call(&mut connection.lock().unwrap_or_else(|e| e.into_inner()))
        })
        .await
        .map_err(|e| Failure::Backend(e.to_string()))?
    }
}

fn busy(e: &rusqlite::Error) -> bool {
    e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)
}

fn upsert(connection: &Connection, namespace: &str, key: &str, value: &[u8]) -> Outcome<()> {
    connection
        .prepare_cached(UPSERT)?
        .execute(params![namespace, key, value])?;
    Ok(())
}

fn read(connection: &Connection, namespace: &str, key: &str) -> Outcome<Option<Vec<u8>>> {
    Ok(connection
        .prepare_cached("SELECT value FROM kv WHERE namespace = ?1 AND key = ?2")?
        .query_row(params![namespace, key], |row| row.get(0))
        .optional()?)
}

#[async_trait::async_trait]
impl Backend for Sqlite {
    async fn get(&self, namespace: &str, key: &str) -> Outcome<Option<Vec<u8>>> {
        let (namespace, key) = (namespace.to_string(), key.to_string());
        self.blocking(move |connection| read(connection, &namespace, &key))
            .await
    }

    async fn set(&self, namespace: &str, key: &str, value: Vec<u8>) -> Outcome<()> {
        let key = key.to_string();
        self.writing(namespace, move |transaction, namespace| {
            upsert(transaction, namespace, &key, &value)
        })
        .await
    }

    async fn delete(&self, namespace: &str, key: &str) -> Outcome<()> {
        let (namespace, key) = (namespace.to_string(), key.to_string());
        self.blocking(move |connection| {
            connection
                .prepare_cached("DELETE FROM kv WHERE namespace = ?1 AND key = ?2")?
                .execute(params![namespace, key])?;
            Ok(())
        })
        .await
    }

    /// Keys in order; the cursor is the last key of the page.
    async fn list_keys(
        &self,
        namespace: &str,
        cursor: Option<String>,
    ) -> Outcome<(Vec<String>, Option<String>)> {
        let namespace = namespace.to_string();
        self.blocking(move |connection| {
            // one past the page: whether another follows
            let limit = (PAGE_KEYS + 1) as i64;
            let mut keys = match &cursor {
                None => connection
                    .prepare_cached(
                        "SELECT key FROM kv WHERE namespace = ?1 ORDER BY key LIMIT ?2",
                    )?
                    .query_map(params![namespace, limit], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()?,
                Some(after) => connection
                    .prepare_cached(
                        "SELECT key FROM kv WHERE namespace = ?1 AND key > ?2 ORDER BY key LIMIT ?3",
                    )?
                    .query_map(params![namespace, after, limit], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()?,
            };
            let next = (keys.len() > PAGE_KEYS).then(|| {
                keys.truncate(PAGE_KEYS);
                keys.last().cloned().expect("a full page")
            });
            Ok((keys, next))
        })
        .await
    }

    async fn swap(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Outcome<Swap> {
        let key = key.to_string();
        self.writing(namespace, move |transaction, namespace| {
            let current = read(transaction, namespace, &key)?;
            if current != expected {
                return Ok(Swap::Stale(current));
            }
            upsert(transaction, namespace, &key, &value)?;
            Ok(Swap::Done)
        })
        .await
    }

    async fn exists(&self, namespace: &str, key: &str) -> Outcome<bool> {
        let (namespace, key) = (namespace.to_string(), key.to_string());
        self.blocking(move |connection| {
            Ok(connection
                .prepare_cached("SELECT 1 FROM kv WHERE namespace = ?1 AND key = ?2")?
                .exists(params![namespace, key])?)
        })
        .await
    }

    async fn increment(&self, namespace: &str, key: &str, delta: i64) -> Outcome<i64> {
        let key = key.to_string();
        self.writing(namespace, move |transaction, namespace| {
            let sum = add(&key, read(transaction, namespace, &key)?.as_deref(), delta)?;
            upsert(transaction, namespace, &key, sum.to_string().as_bytes())?;
            Ok(sum)
        })
        .await
    }

    /// In one read transaction: the values as of one moment.
    async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<String>,
    ) -> Outcome<Vec<(String, Option<Vec<u8>>)>> {
        let namespace = namespace.to_string();
        self.blocking(move |connection| {
            let transaction = connection.transaction()?;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                let value = read(&transaction, &namespace, &key)?;
                values.push((key, value));
            }
            Ok(values)
        })
        .await
    }

    /// In one transaction: all or none.
    async fn set_many(&self, namespace: &str, pairs: Vec<(String, Vec<u8>)>) -> Outcome<()> {
        self.writing(namespace, move |transaction, namespace| {
            pairs
                .iter()
                .try_for_each(|(key, value)| upsert(transaction, namespace, key, value))
        })
        .await
    }

    /// In one transaction: all or none.
    async fn delete_many(&self, namespace: &str, keys: Vec<String>) -> Outcome<()> {
        let namespace = namespace.to_string();
        self.blocking(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for key in keys {
                transaction
                    .prepare_cached("DELETE FROM kv WHERE namespace = ?1 AND key = ?2")?
                    .execute(params![namespace, key])?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    /// The namespace's rows and its usage, in one transaction.
    async fn purge(&self, namespace: &str) -> Outcome<u64> {
        let namespace = namespace.to_string();
        self.blocking(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // first: the trigger of each row deleted then finds no usage to update
            transaction
                .prepare_cached("DELETE FROM usage WHERE namespace = ?1")?
                .execute(params![namespace])?;
            let keys = transaction
                .prepare_cached("DELETE FROM kv WHERE namespace = ?1")?
                .execute(params![namespace])?;
            transaction.commit()?;
            Ok(keys as u64)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::tests::Scratch;

    /// Openers of a new file at once all open it: the one switching it to
    /// WAL second retries (without, about 1 in 20 failed with `database is
    /// locked`).
    #[test]
    fn openers_of_a_new_file_at_once_all_open_it() {
        let dir = Scratch::new("sqlite-open");
        for round in 0..20 {
            let file = dir.0.join(format!("kv-{round}.db"));
            let start = Arc::new(std::sync::Barrier::new(4));
            let openers: Vec<_> = (0..4)
                .map(|_| {
                    let (file, start) = (file.clone(), start.clone());
                    std::thread::spawn(move || {
                        start.wait();
                        Sqlite::open(&file, 1 << 20).map(|_| ())
                    })
                })
                .collect();
            for opener in openers {
                assert_eq!(opener.join().unwrap(), Ok(()), "round {round}");
            }
        }
    }
}
