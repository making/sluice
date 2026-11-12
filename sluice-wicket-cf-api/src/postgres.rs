//! The state in PostgreSQL (`--state postgres://...`): the store several cf-api instances
//! share. The rows of `crate::records` in one table, each with the revision that last wrote
//! it; the revision is one row of its own, and a change is a transaction holding that row's
//! lock: it takes what the others wrote since, runs on the state as it is then, and writes
//! what it changed as the next revision. So the changes of every instance are one after
//! another, each seeing all before it, as those of one process are under its lock; none is
//! retried for another's, only for a connection lost.
//!
//! A process answers its reads from a copy, which is never ahead of the store: a change is in
//! it once committed. It follows the others' by `LISTEN` (each commit notifies the revision),
//! and a request first asks the store for the revision (`Store::sync`), so it sees what any
//! instance answered before it began. A row removed stays, without a body, for the processes
//! yet to take it, until [`KEPT`] revisions later: a process further behind reads all again.

use std::convert::Infallible;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, Client, Config, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::records::{Book, Row};
use crate::store::State;

/// The rows' layout this build reads and writes.
const SCHEMA: i32 = 1;

/// The channel a commit notifies its revision on.
const CHANNEL: &str = "sluice_cf";

/// The revisions a row removed, and who committed each, are kept for.
const KEPT: i64 = 10_000;

/// Every how many revisions what is older than [`KEPT`] is removed.
const SWEPT_EVERY: i64 = 1_000;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on one try of a transaction, the wait for another's included: past it the
/// connection is taken for lost.
const TRY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the server lets a transaction of this process sit: one paused holding the
/// revision's lock is ended, and the others go on.
const IDLE_IN_TRANSACTION: &str = "15s";

const MIN_BACKOFF: Duration = Duration::from_millis(100);

const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How often a store not answering is said.
const WARN_EVERY: Duration = Duration::from_secs(30);

const TABLES: &str = "
SELECT pg_advisory_xact_lock(hashtext('sluice_cf'));
CREATE TABLE IF NOT EXISTS sluice_cf_meta (
    one boolean PRIMARY KEY DEFAULT true CHECK (one),
    layout integer NOT NULL,
    rev bigint NOT NULL,
    oldest bigint NOT NULL
);
INSERT INTO sluice_cf_meta (layout, rev, oldest) VALUES (1, 0, 0) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS sluice_cf_records (
    kind text NOT NULL,
    id text NOT NULL,
    ord bigint NOT NULL,
    rev bigint NOT NULL,
    body text,
    PRIMARY KEY (kind, id)
);
CREATE INDEX IF NOT EXISTS sluice_cf_records_rev ON sluice_cf_records (rev);
CREATE TABLE IF NOT EXISTS sluice_cf_commits (
    rev bigint PRIMARY KEY,
    writer text NOT NULL,
    at timestamptz NOT NULL DEFAULT now()
);
";

/// The store of the state (`--state`).
#[derive(Clone)]
pub struct PostgresSettings {
    /// `postgres://user:password@host:port/database`, with libpq's parameters.
    pub url: String,
    /// PEM CA verifying it (`--state-ca-cert`); None: the webPKI roots.
    pub ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for PostgresSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // the URL holds the password
        f.debug_struct("PostgresSettings").finish_non_exhaustive()
    }
}

/// The copy of the state a process answers its reads from, and who waits for its changes.
#[derive(Default)]
pub struct Shared {
    pub state: RwLock<State>,
    pub changes: tokio::sync::Notify,
    /// The revision the copy is of.
    pub rev: AtomicI64,
}

/// What the one changing the copy holds: the lock of `Store::update` and `Store::note`.
#[derive(Default)]
pub struct Writer {
    pub book: Book,
    /// The connection the transactions run on; None: to open. Taken for a transaction and put
    /// back at its end: one cut short (an error, its future dropped) leaves none, and the
    /// server ends what it held with the connection.
    session: Option<Session>,
}

struct Session {
    client: Client,
    head: Statement,
    locked: Statement,
    since: Statement,
    write: Statement,
    commit: Statement,
    committed: Statement,
}

/// A transaction whose commit was sent and not answered: committed or not, the next try
/// finds out.
pub struct Sent<R> {
    rev: i64,
    token: String,
    result: R,
}

pub struct Postgres {
    config: Config,
    tls: MakeRustlsConnect,
    /// The connection listening, which answers the revision too (by the statement beside
    /// it); None while it is away.
    listening: Mutex<Option<(Arc<Client>, Statement)>>,
}

fn failed(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("--state: {}", db.message()),
        None => {
            // its cause is not in what it says (a TLS handshake's: the certificate refused)
            let mut said = format!("--state: {e}");
            let mut cause = std::error::Error::source(&e);
            while let Some(e) = cause {
                said += &format!(": {e}");
                cause = e.source();
            }
            said
        }
    }
}

impl Postgres {
    pub fn new(settings: &PostgresSettings) -> Result<Postgres, String> {
        let mut config: Config = (settings.url.parse())
            // the error may quote the URL, with its password
            .map_err(|_| "--state: expected postgres://user:password@host:port/database")?;
        if config.get_application_name().is_none() {
            config.application_name("sluice-wicket-cf-api");
        }
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(CONNECT_TIMEOUT);
        }
        config.keepalives_idle(Duration::from_secs(10));
        let mut roots = rustls::RootCertStore::empty();
        match &settings.ca_pem {
            Some(pem) => {
                use rustls::pki_types::pem::PemObject as _;
                for cert in rustls::pki_types::CertificateDer::pem_slice_iter(pem) {
                    let cert = cert.map_err(|e| format!("--state-ca-cert: {e}"))?;
                    (roots.add(cert)).map_err(|e| format!("--state-ca-cert: {e}"))?;
                }
                if roots.is_empty() {
                    return Err("--state-ca-cert: no certificate in it".into());
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("--state: tls: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Postgres {
            config,
            tls: MakeRustlsConnect::new(tls),
            listening: Mutex::new(None),
        })
    }

    /// A connection for the transactions; `make`: the tables made first, where there are none.
    async fn open(&self, make: bool) -> Result<Session, String> {
        let (client, connection) = (self.config.connect(self.tls.clone()).await).map_err(failed)?;
        tokio::spawn(connection);
        if make {
            let tables = format!("BEGIN; {TABLES} COMMIT");
            client.batch_execute(&tables).await.map_err(failed)?;
        }
        let settings = format!("SET idle_in_transaction_session_timeout = '{IDLE_IN_TRANSACTION}'");
        client.batch_execute(&settings).await.map_err(failed)?;
        let prepare = async |sql: &str| client.prepare(sql).await.map_err(failed);
        let head = "SELECT rev, oldest, layout FROM sluice_cf_meta";
        Ok(Session {
            head: prepare(head).await?,
            locked: prepare(&format!("{head} FOR UPDATE")).await?,
            since: prepare(
                "SELECT kind, id, ord, body FROM sluice_cf_records WHERE rev > $1 ORDER BY ord",
            )
            .await?,
            write: prepare(
                "INSERT INTO sluice_cf_records (kind, id, ord, rev, body) \
                 SELECT kind, id, ord, $4::bigint, body \
                 FROM UNNEST($1::text[], $2::text[], $3::bigint[], $5::text[]) \
                 AS written (kind, id, ord, body) \
                 ON CONFLICT (kind, id) DO UPDATE \
                 SET ord = EXCLUDED.ord, rev = EXCLUDED.rev, body = EXCLUDED.body",
            )
            .await?,
            commit: prepare(
                "WITH moved AS (UPDATE sluice_cf_meta SET rev = $1::bigint), \
                 said AS (INSERT INTO sluice_cf_commits (rev, writer) \
                 VALUES ($1::bigint, $2::text)) \
                 SELECT pg_notify($3::text, ($1::bigint)::text)",
            )
            .await?,
            committed: prepare("SELECT 1 FROM sluice_cf_commits WHERE rev = $1 AND writer = $2")
                .await?,
            client,
        })
    }

    /// The revision, how far back the rows removed are kept, and that this build reads them.
    async fn head(session: &Session, locked: bool) -> Result<(i64, i64), String> {
        let statement = match locked {
            true => &session.locked,
            false => &session.head,
        };
        let row = (session.client.query_one(statement, &[]).await).map_err(failed)?;
        let schema: i32 = row.get(2);
        if schema > SCHEMA {
            return Err(format!(
                "--state: written by a newer sluice-wicket-cf-api (schema {schema}, this one \
                 reads {SCHEMA})"
            ));
        }
        Ok((row.get(0), row.get(1)))
    }

    /// Takes what was written since the copy's revision into it, within the transaction open
    /// on `session`: the store is at `rev`.
    async fn follow(
        session: &Session,
        book: &mut Book,
        shared: &Shared,
        (rev, floor): (i64, i64),
    ) -> Result<(), String> {
        if rev == book.rev {
            return Ok(());
        }
        // the rows removed since are gone: all there are, anew
        let all = book.rev < floor;
        let since = if all { 0 } else { book.rev };
        let rows = (session.client.query(&session.since, &[&since]).await).map_err(failed)?;
        let rows: Vec<Row> = (rows.iter())
            .map(|row| Row {
                kind: row.get(0),
                id: row.get(1),
                ord: row.get(2),
                body: row.get(3),
            })
            .collect();
        let mut state = shared.state.write().expect("no panic under the lock");
        book.take(&mut state, &rows, rev, all);
        drop(state);
        shared.rev.store(rev, Ordering::Release);
        shared.changes.notify_one();
        Ok(())
    }

    async fn execute(session: &Session, sql: &str) -> Result<(), String> {
        session.client.batch_execute(sql).await.map_err(failed)
    }

    /// Writes `rows` as the revision `rev` in the transaction open on `session`, all but its
    /// commit.
    async fn write(session: &Session, rows: &[Row], rev: i64, token: &str) -> Result<(), String> {
        let kinds: Vec<&str> = rows.iter().map(|row| row.kind.as_str()).collect();
        let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
        let ords: Vec<i64> = rows.iter().map(|row| row.ord).collect();
        let bodies: Vec<Option<&str>> = rows.iter().map(|row| row.body.as_deref()).collect();
        let written: [&(dyn ToSql + Sync); 5] = [&kinds, &ids, &ords, &rev, &bodies];
        let committed: [&(dyn ToSql + Sync); 3] = [&rev, &token, &CHANNEL];
        // sent together: one round trip
        let (written, committed) = tokio::join!(
            session.client.execute(&session.write, &written),
            session.client.query(&session.commit, &committed),
        );
        written.map_err(failed)?;
        committed.map_err(failed)?;
        if rev % SWEPT_EVERY == 0 && rev > KEPT {
            let floor = rev - KEPT;
            let sweep = format!(
                "DELETE FROM sluice_cf_records WHERE body IS NULL AND rev <= {floor}; \
                 DELETE FROM sluice_cf_commits WHERE rev <= {floor}; \
                 UPDATE sluice_cf_meta SET oldest = {floor}"
            );
            Postgres::execute(session, &sweep).await?;
        }
        Ok(())
    }

    /// Opens the store: its tables made, the copy read; an empty store takes the state `first`
    /// makes as its first revision. Whether it was empty.
    pub async fn load(
        &self,
        writer: &mut Writer,
        shared: &Shared,
        first: impl FnOnce() -> Result<State, String>,
    ) -> Result<bool, String> {
        let session = self.open(true).await?;
        // `prefer`, a URL's default, goes on without TLS where the server has none: said, as
        // the state holds the apps' credentials. A server not telling is left unsaid
        let over = "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()";
        if let Ok(Some(row)) = session.client.query_opt(over, &[]).await
            && !row.get::<_, bool>(0)
        {
            eprintln!("STATE: the connection to the store is not over TLS (sslmode=require)");
        }
        Postgres::execute(&session, "BEGIN").await?;
        let head = Postgres::head(&session, true).await?;
        let empty = head.0 == 0;
        if empty {
            let state = first()?;
            let rows = writer.book.diff(None, &state, 1);
            Postgres::write(&session, &rows, 1, &crate::store::guid()).await?;
            Postgres::execute(&session, "COMMIT").await?;
            writer.book.written(&rows, 1);
            *shared.state.write().expect("no panic under the lock") = state;
            shared.rev.store(1, Ordering::Release);
        } else {
            Postgres::follow(&session, &mut writer.book, shared, head).await?;
            Postgres::execute(&session, "ROLLBACK").await?;
        }
        writer.session = Some(session);
        Ok(empty)
    }

    /// Changes the state by `f` in a transaction, tried again until the store answers: `f`
    /// runs once more each time, on the state as it is then, so it does nothing but change it.
    pub async fn update<R>(
        &self,
        writer: &mut Writer,
        shared: &Shared,
        f: impl Fn(&mut State) -> R,
    ) -> R {
        let mut sent = None;
        let mut backoff = MIN_BACKOFF;
        let mut said: Option<tokio::time::Instant> = None;
        loop {
            let tried = self.transact(writer, shared, &f, &mut sent);
            let error = match tokio::time::timeout(TRY_TIMEOUT, tried).await {
                Ok(Ok(result)) => return result,
                Ok(Err(e)) => e,
                Err(_) => "--state: no answer".to_string(),
            };
            if said.is_none_or(|said| said.elapsed() >= WARN_EVERY) {
                eprintln!("STATE: a change waits for the store: {error}");
                said = Some(tokio::time::Instant::now());
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// One try of [`Postgres::update`].
    async fn transact<R>(
        &self,
        writer: &mut Writer,
        shared: &Shared,
        f: &impl Fn(&mut State) -> R,
        sent: &mut Option<Sent<R>>,
    ) -> Result<R, String> {
        let session = match writer.session.take() {
            Some(session) => session,
            None => self.open(false).await?,
        };
        Postgres::execute(&session, "BEGIN").await?;
        let head = Postgres::head(&session, true).await?;
        if let Some(Sent { rev, token, result }) = sent.take() {
            let sent: [&(dyn ToSql + Sync); 2] = [&rev, &token];
            let found = session.client.query_opt(&session.committed, &sent);
            if found.await.map_err(failed)?.is_some() {
                // it was committed: taken as any other's
                Postgres::follow(&session, &mut writer.book, shared, head).await?;
                Postgres::execute(&session, "ROLLBACK").await?;
                writer.session = Some(session);
                return Ok(result);
            }
        }
        Postgres::follow(&session, &mut writer.book, shared, head).await?;
        let mut next = shared
            .state
            .read()
            .expect("no panic under the lock")
            .clone();
        let result = f(&mut next);
        next.place();
        let rev = head.0 + 1;
        let rows = {
            let state = shared.state.read().expect("no panic under the lock");
            writer.book.diff(Some(&state), &next, rev)
        };
        if rows.is_empty() {
            Postgres::execute(&session, "ROLLBACK").await?;
        } else {
            let token = crate::store::guid();
            Postgres::write(&session, &rows, rev, &token).await?;
            *sent = Some(Sent { rev, token, result });
            Postgres::execute(&session, "COMMIT").await?;
            writer.book.written(&rows, rev);
            let committed = sent.take().expect("just put");
            *shared.state.write().expect("no panic under the lock") = next;
            shared.rev.store(rev, Ordering::Release);
            writer.session = Some(session);
            return Ok(committed.result);
        }
        // nothing kept changed: what it changed of the rest
        *shared.state.write().expect("no panic under the lock") = next;
        writer.session = Some(session);
        Ok(result)
    }

    /// Takes what the others wrote since the copy's revision.
    pub async fn catch_up(&self, writer: &mut Writer, shared: &Shared) -> Result<(), String> {
        let caught = async {
            let session = match writer.session.take() {
                Some(session) => session,
                None => self.open(false).await?,
            };
            // the revision and its rows as of one moment
            Postgres::execute(&session, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
            let head = Postgres::head(&session, false).await?;
            Postgres::follow(&session, &mut writer.book, shared, head).await?;
            Postgres::execute(&session, "COMMIT").await?;
            writer.session = Some(session);
            Ok(())
        };
        match tokio::time::timeout(TRY_TIMEOUT, caught).await {
            Ok(caught) => caught,
            Err(_) => Err("--state: no answer".to_string()),
        }
    }

    /// The store's revision, asked of the connection listening.
    pub async fn rev(&self) -> Result<i64, String> {
        let listening = (self.listening.lock().expect("no panic under the lock")).clone();
        let (client, rev) = listening.ok_or("--state: not connected")?;
        let row = client.query_one(&rev, &[]);
        Ok(row.await.map_err(failed)?.get(0))
    }

    /// Takes what the others committed by the revision `rev`, said to be the store's.
    async fn heard(&self, rev: i64, shared: &Shared, writer: &tokio::sync::Mutex<Writer>) {
        if rev <= shared.rev.load(Ordering::Acquire) {
            return;
        }
        let mut writer = writer.lock().await;
        if let Err(e) = self.catch_up(&mut writer, shared).await {
            eprintln!("STATE: cannot follow the store: {e}");
        }
    }

    /// Follows the others' commits until the connection listening ends: each revision
    /// notified, and at first what was committed while none listened.
    async fn listen(
        &self,
        shared: &Shared,
        writer: &tokio::sync::Mutex<Writer>,
    ) -> Result<Infallible, String> {
        let (client, mut connection) =
            (self.config.connect(self.tls.clone()).await).map_err(failed)?;
        let (revs, mut notified) = tokio::sync::mpsc::unbounded_channel();
        let driver = tokio::spawn(async move {
            let mut messages = futures_util::stream::poll_fn(|cx| connection.poll_message(cx));
            while let Some(Ok(message)) = messages.next().await {
                if let AsyncMessage::Notification(notification) = message
                    && let Ok(rev) = notification.payload().parse::<i64>()
                    && revs.send(rev).is_err()
                {
                    break;
                }
            }
        });
        let client = Arc::new(client);
        let listened = async {
            let listen = format!("LISTEN {CHANNEL}");
            client.batch_execute(&listen).await.map_err(failed)?;
            let rev = client.prepare("SELECT rev FROM sluice_cf_meta");
            let rev = rev.await.map_err(failed)?;
            *self.listening.lock().expect("no panic under the lock") = Some((client.clone(), rev));
            self.heard(i64::MAX, shared, writer).await;
            while let Some(rev) = notified.recv().await {
                self.heard(rev, shared, writer).await;
            }
            Err("--state: the connection listening ended".to_string())
        };
        let ended = listened.await;
        *self.listening.lock().expect("no panic under the lock") = None;
        driver.abort();
        ended
    }

    /// Keeps following the others' commits, connecting again with a backoff.
    pub async fn keep_listening(&self, shared: &Shared, writer: &tokio::sync::Mutex<Writer>) {
        let mut backoff = MIN_BACKOFF;
        let mut said: Option<tokio::time::Instant> = None;
        loop {
            let began = tokio::time::Instant::now();
            let Err(error) = self.listen(shared, writer).await;
            if began.elapsed() > MAX_BACKOFF {
                backoff = MIN_BACKOFF;
            }
            if said.is_none_or(|said| said.elapsed() >= WARN_EVERY) {
                eprintln!("STATE: not listening to the store: {error}");
                said = Some(tokio::time::Instant::now());
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}
