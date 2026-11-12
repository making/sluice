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
//!
//! The logins (`crate::uaa`: the keys, the sessions, the passcodes) are tables beside it, not
//! of the state: a login is no revision, wakes no one and waits for no change. Each of their
//! statements is a transaction of its own, on a connection of theirs.
//!
//! So is the lease of the instance reconciling the wickets (`crate::lease`), a row of its own
//! renewed on a connection of its own: a renewal is no revision either. It is taken under the
//! revision's lock, so a change sees the lease as it is until it commits: the leader's own
//! (`Store::lead`) is refused once another took the lease, and what an instance saw of the
//! wickets is written by it only while it is no leader, or the one ([`Postgres::transact`]).

use std::convert::Infallible;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, Client, Config, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::lease::Term;
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
CREATE TABLE IF NOT EXISTS sluice_cf_keys (
    seq integer PRIMARY KEY,
    kid text NOT NULL UNIQUE,
    key bytea NOT NULL,
    made timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sluice_cf_sessions (
    jti text PRIMARY KEY,
    subject text NOT NULL,
    expires timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS sluice_cf_sessions_expires ON sluice_cf_sessions (expires, jti);
CREATE TABLE IF NOT EXISTS sluice_cf_passcodes (
    code text PRIMARY KEY,
    identity text NOT NULL,
    expires timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS sluice_cf_lease (
    one boolean PRIMARY KEY DEFAULT true CHECK (one),
    holder text,
    epoch bigint NOT NULL,
    expires timestamptz NOT NULL
);
INSERT INTO sluice_cf_lease (holder, epoch, expires) VALUES (NULL, 0, '-infinity')
    ON CONFLICT DO NOTHING;
";

/// Bound on a statement of the lease: past it its connection is taken for lost.
const LEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// The connection of the lease, its statements one at a time (the lease's loop alone sends them).
struct Leasing {
    client: Client,
    renew: Statement,
    due: Statement,
    locked: Statement,
    take: Statement,
    release: Statement,
}

/// What a try to take the lease found.
#[derive(Clone, Debug, PartialEq)]
pub enum Taken {
    /// This instance holds it, of this epoch: a turn of its own.
    Taken(u64),
    /// Another instance holds it, of this epoch (none ever did: 0).
    Held(u64),
}

/// Bound on a statement of the logins: past it their connection is taken for lost.
const ASIDE_TIMEOUT: Duration = Duration::from_secs(5);

/// The connection of the logins, its statements sent as they come, none waiting for another.
struct Aside {
    client: Client,
    keys: Statement,
    first_key: Statement,
    put_session: Statement,
    rotate_session: Statement,
    session: Statement,
    drop_session: Statement,
    put_passcode: Statement,
    take_passcode: Statement,
}

/// A key of the tokens, as the store has it.
pub struct StoredKey {
    pub kid: String,
    pub key: Vec<u8>,
    /// Whether it signs by now: one made ahead of time is verified only until then.
    pub signs: bool,
}

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
    /// Moved at each change of the copy, this process's or another's.
    pub changes: tokio::sync::watch::Sender<u64>,
    /// The revision the copy is of.
    pub rev: AtomicI64,
    /// This instance's turn reconciling the wickets, while it has one (`crate::lease`).
    pub term: tokio::sync::watch::Sender<Option<Term>>,
    /// The epoch of the lease as this instance last saw it: the turn of whoever leads.
    pub leading: tokio::sync::watch::Sender<Option<u64>>,
}

impl Shared {
    /// The copy changed.
    pub fn changed(&self) {
        self.changes
            .send_modify(|changes| *changes = changes.wrapping_add(1));
    }

    /// Ends this instance's turn of `epoch`, if it is still the one, saying `why`.
    pub fn end_term(&self, epoch: Option<u64>, why: &str) {
        let ended = (self.term).send_if_modified(|term| match term {
            Some(Term { epoch: of }) if *of == epoch => {
                *term = None;
                true
            }
            _ => false,
        });
        if ended {
            println!("LEASE: this instance reconciles the wickets no more: {why}");
        }
    }
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

/// What a transaction finds first: the revision and how far back the rows removed are kept, and
/// the lease's holder (none before any) and epoch.
struct Head {
    at: (i64, i64),
    lease: (Option<String>, u64),
}

pub struct Postgres {
    config: Config,
    tls: MakeRustlsConnect,
    /// This instance, as the lease names its holder.
    holder: String,
    /// The connection listening, which answers the revision too (by the statement beside
    /// it); None while it is away.
    listening: Mutex<Option<(Arc<Client>, Statement)>>,
    /// The connection of the logins; None: to open. Held to open it only.
    aside: tokio::sync::Mutex<Option<Arc<Aside>>>,
    /// The connection of the lease; None: to open.
    leasing: tokio::sync::Mutex<Option<Leasing>>,
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
    /// The store of `settings`, for the instance `holder`.
    pub fn new(settings: &PostgresSettings, holder: &str) -> Result<Postgres, String> {
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
        let tls = crate::tls::client_config(settings.ca_pem.as_deref(), "--state-ca-cert")?;
        Ok(Postgres {
            config,
            tls: MakeRustlsConnect::new(tls),
            holder: holder.to_string(),
            listening: Mutex::new(None),
            aside: tokio::sync::Mutex::new(None),
            leasing: tokio::sync::Mutex::new(None),
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
        let head = "SELECT m.rev, m.oldest, m.layout, l.holder, l.epoch \
                    FROM sluice_cf_meta m LEFT JOIN sluice_cf_lease l ON true";
        Ok(Session {
            head: prepare(head).await?,
            locked: prepare(&format!("{head} FOR UPDATE OF m")).await?,
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

    /// The revision, how far back the rows removed are kept, and that this build reads them;
    /// and the lease's holder and epoch.
    async fn head(session: &Session, locked: bool) -> Result<Head, String> {
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
        let epoch: Option<i64> = row.get(4);
        Ok(Head {
            at: (row.get(0), row.get(1)),
            lease: (row.get(3), epoch.unwrap_or_default() as u64),
        })
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
        shared.changed();
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
        let empty = head.at.0 == 0;
        if empty {
            let state = first()?;
            let rows = writer.book.diff(None, &state, 1, true);
            Postgres::write(&session, &rows, 1, &crate::store::guid()).await?;
            Postgres::execute(&session, "COMMIT").await?;
            writer.book.written(&rows, 1);
            *shared.state.write().expect("no panic under the lock") = state;
            shared.rev.store(1, Ordering::Release);
        } else {
            Postgres::follow(&session, &mut writer.book, shared, head.at).await?;
            Postgres::execute(&session, "ROLLBACK").await?;
        }
        writer.session = Some(session);
        Ok(empty)
    }

    /// Changes the state by `f` in a transaction, tried again until the store answers: `f`
    /// runs once more each time, on the state as it is then, so it does nothing but change it.
    /// `leader`: a change of the instance reconciling the wickets alone, made only while the
    /// lease is its own turn's (None otherwise).
    pub async fn update<R>(
        &self,
        writer: &mut Writer,
        shared: &Shared,
        f: impl Fn(&mut State) -> R,
        leader: bool,
    ) -> Option<R> {
        let mut sent = None;
        let mut backoff = MIN_BACKOFF;
        let mut said: Option<tokio::time::Instant> = None;
        loop {
            let tried = self.transact(writer, shared, &f, &mut sent, leader);
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

    /// One try of [`Postgres::update`]. The lease is read under the revision's lock, which its
    /// takeover takes too: as it is until the commit. An instance whose turn it ended (another
    /// holds the lease) has the turn end here, what it saw of the wickets and did not write
    /// forgotten; a change of the leader alone is not made but in its turn; what an instance saw
    /// of the wickets is written unless its turn ended.
    async fn transact<R>(
        &self,
        writer: &mut Writer,
        shared: &Shared,
        f: &impl Fn(&mut State) -> R,
        sent: &mut Option<Sent<R>>,
        leader: bool,
    ) -> Result<Option<R>, String> {
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
                Postgres::follow(&session, &mut writer.book, shared, head.at).await?;
                Postgres::execute(&session, "ROLLBACK").await?;
                writer.session = Some(session);
                return Ok(Some(result));
            }
        }
        Postgres::follow(&session, &mut writer.book, shared, head.at).await?;
        let term = *shared.term.borrow();
        // whether the lease is this instance's turn: None without one
        let ours = term.map(|term| {
            let (holder, epoch) = &head.lease;
            holder.as_deref() == Some(self.holder.as_str()) && term.epoch == Some(*epoch)
        });
        if let (Some(false), Some(term)) = (ours, term) {
            let (holder, epoch) = &head.lease;
            let holder = holder.as_deref().unwrap_or("none");
            let why = format!("{holder} holds the lease, of epoch {epoch}");
            shared.end_term(term.epoch, &why);
            let mut state = shared.state.write().expect("no panic under the lock");
            writer.book.forget_observed(&mut state);
        }
        if leader && ours != Some(true) {
            Postgres::execute(&session, "ROLLBACK").await?;
            writer.session = Some(session);
            return Ok(None);
        }
        let mut next = shared
            .state
            .read()
            .expect("no panic under the lock")
            .clone();
        let result = f(&mut next);
        next.place();
        let rev = head.at.0 + 1;
        let rows = {
            let state = shared.state.read().expect("no panic under the lock");
            writer
                .book
                .diff(Some(&state), &next, rev, ours != Some(false))
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
            return Ok(Some(committed.result));
        }
        // nothing kept changed: what it changed of the rest
        *shared.state.write().expect("no panic under the lock") = next;
        writer.session = Some(session);
        Ok(Some(result))
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
            Postgres::follow(&session, &mut writer.book, shared, head.at).await?;
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

    /// What `f` asks on the connection of the lease, opened when there is none; one that fails
    /// or does not answer within [`LEASE_TIMEOUT`] is closed (its transaction with it).
    async fn leased<T>(
        &self,
        f: impl AsyncFnOnce(&Leasing) -> Result<T, tokio_postgres::Error>,
    ) -> Result<T, String> {
        let mut leasing = self.leasing.lock().await;
        let asked = async {
            if leasing.as_ref().is_none_or(|open| open.client.is_closed()) {
                *leasing = Some(self.open_leasing().await?);
            }
            let open = leasing.as_ref().expect("just opened");
            f(open).await.map_err(failed)
        };
        let asked = match tokio::time::timeout(LEASE_TIMEOUT, asked).await {
            Ok(asked) => asked,
            Err(_) => Err("--state: no answer".to_string()),
        };
        if asked.is_err() {
            *leasing = None;
        }
        asked
    }

    async fn open_leasing(&self) -> Result<Leasing, String> {
        let (client, connection) = (self.config.connect(self.tls.clone()).await).map_err(failed)?;
        tokio::spawn(connection);
        let prepare = async |sql: &str| client.prepare(sql).await.map_err(failed);
        Ok(Leasing {
            renew: prepare(
                "UPDATE sluice_cf_lease SET expires = now() + $3::bigint * interval '1 millisecond' \
                 WHERE holder = $1 AND epoch = $2::bigint AND expires > now()",
            )
            .await?,
            due: prepare("SELECT holder, epoch, expires <= now() FROM sluice_cf_lease").await?,
            locked: prepare("SELECT 1 FROM sluice_cf_meta FOR UPDATE").await?,
            // the epoch the store's clock in milliseconds at least: a store restored from a
            // backup, or a new one, goes on above what the wickets were sent
            take: prepare(
                "UPDATE sluice_cf_lease SET holder = $1, \
                 epoch = GREATEST(epoch + 1, (extract(epoch FROM now()) * 1000)::bigint), \
                 expires = now() + $2::bigint * interval '1 millisecond' \
                 WHERE expires <= now() OR holder = $1 RETURNING epoch",
            )
            .await?,
            release: prepare(
                "UPDATE sluice_cf_lease SET expires = now() WHERE holder = $1 AND epoch = $2::bigint",
            )
            .await?,
            client,
        })
    }

    /// Renews the lease of `epoch` this instance holds for `lease` more, by the store's clock:
    /// whether it held it still.
    pub async fn renew(&self, epoch: u64, lease: Duration) -> Result<bool, String> {
        let (epoch, millis) = (epoch as i64, lease.as_millis() as i64);
        let renew: [&(dyn ToSql + Sync); 3] = [&self.holder, &epoch, &millis];
        let renewed =
            self.leased(async |leasing| (leasing.client.execute(&leasing.renew, &renew)).await);
        Ok(renewed.await? == 1)
    }

    /// Takes the lease for `lease`, by the store's clock, unless another instance holds it
    /// still; under the revision's lock, so a change sees the lease as it was or as taken, never
    /// both. Taken anew, a turn of this instance's own of the epoch after it; an epoch is the
    /// store's clock in milliseconds at least.
    pub async fn take(&self, lease: Duration) -> Result<Taken, String> {
        let millis = lease.as_millis() as i64;
        let holder = self.holder.as_str();
        self.leased(async |leasing| {
            let client = &leasing.client;
            let due = client.query_one(&leasing.due, &[]).await?;
            let (held_by, epoch, due): (Option<String>, i64, bool) =
                (due.get(0), due.get(1), due.get(2));
            if !due && held_by.as_deref() != Some(holder) {
                return Ok(Taken::Held(epoch as u64));
            }
            client.batch_execute("BEGIN").await?;
            client.query_one(&leasing.locked, &[]).await?;
            let take: [&(dyn ToSql + Sync); 2] = [&holder, &millis];
            let taken = client.query_opt(&leasing.take, &take).await?;
            client.batch_execute("COMMIT").await?;
            Ok(match taken {
                Some(taken) => Taken::Taken(taken.get::<_, i64>(0) as u64),
                // another took it meanwhile
                None => {
                    let held = client.query_one(&leasing.due, &[]).await?;
                    Taken::Held(held.get::<_, i64>(1) as u64)
                }
            })
        })
        .await
    }

    /// Lets go of the lease of `epoch`, if this instance holds it: the next to try takes it.
    pub async fn release(&self, epoch: u64) -> Result<(), String> {
        let epoch = epoch as i64;
        let release: [&(dyn ToSql + Sync); 2] = [&self.holder, &epoch];
        self.leased(async |leasing| (leasing.client.execute(&leasing.release, &release)).await)
            .await
            .map(drop)
    }

    /// The connection of the logins, opened when there is none or it ended.
    async fn aside(&self) -> Result<Arc<Aside>, String> {
        let mut aside = self.aside.lock().await;
        if let Some(open) = aside.as_ref().filter(|open| !open.client.is_closed()) {
            return Ok(open.clone());
        }
        let (client, connection) = (self.config.connect(self.tls.clone()).await).map_err(failed)?;
        tokio::spawn(connection);
        let prepare = async |sql: &str| client.prepare(sql).await.map_err(failed);
        // a statement that removes rows others may remove too passes those they hold
        // (SKIP LOCKED): none waits for another, in whatever order they come to them
        let opened = Arc::new(Aside {
            keys: prepare(
                "SELECT kid, key, made <= now() FROM (\
                 SELECT seq, kid, key, made, lead(made) OVER (ORDER BY seq) AS replaced \
                 FROM sluice_cf_keys) AS keys \
                 WHERE replaced IS NULL OR replaced + $1::bigint * interval '1 second' > now() \
                 ORDER BY seq",
            )
            .await?,
            first_key: prepare(
                "WITH first AS (INSERT INTO sluice_cf_keys (seq, kid, key) VALUES (1, $1, $2) \
                 ON CONFLICT DO NOTHING RETURNING seq) \
                 INSERT INTO sluice_cf_sessions (jti, subject, expires) \
                 SELECT kept.jti, kept.subject, to_timestamp(kept.expires::double precision) \
                 FROM first, UNNEST($3::text[], $4::text[], $5::bigint[]) \
                 AS kept (jti, subject, expires) \
                 ON CONFLICT DO NOTHING",
            )
            .await?,
            put_session: prepare(
                "WITH evicted AS (DELETE FROM sluice_cf_sessions WHERE jti IN (\
                 SELECT jti FROM sluice_cf_sessions WHERE expires <= now() OR jti IN (\
                 SELECT jti FROM sluice_cf_sessions WHERE expires > now() \
                 ORDER BY expires DESC, jti OFFSET $4::bigint) \
                 FOR UPDATE SKIP LOCKED)) \
                 INSERT INTO sluice_cf_sessions (jti, subject, expires) \
                 VALUES ($1, $2, now() + $3::bigint * interval '1 second')",
            )
            .await?,
            rotate_session: prepare(
                "WITH taken AS (DELETE FROM sluice_cf_sessions \
                 WHERE jti = $1 AND expires > now() RETURNING subject) \
                 INSERT INTO sluice_cf_sessions (jti, subject, expires) \
                 SELECT $2::text, subject, now() + $3::bigint * interval '1 second' FROM taken \
                 RETURNING subject",
            )
            .await?,
            session: prepare(
                "SELECT subject FROM sluice_cf_sessions WHERE jti = $1 AND expires > now()",
            )
            .await?,
            drop_session: prepare("DELETE FROM sluice_cf_sessions WHERE jti = $1").await?,
            put_passcode: prepare(
                "WITH gone AS (DELETE FROM sluice_cf_passcodes WHERE code IN (\
                 SELECT code FROM sluice_cf_passcodes WHERE expires <= now() \
                 FOR UPDATE SKIP LOCKED)) \
                 INSERT INTO sluice_cf_passcodes (code, identity, expires) \
                 SELECT $1::text, $2::text, now() + $3::bigint * interval '1 second' \
                 WHERE (SELECT count(*) FROM sluice_cf_passcodes WHERE expires > now()) \
                 < $4::bigint \
                 ON CONFLICT DO NOTHING",
            )
            .await?,
            take_passcode: prepare(
                "DELETE FROM sluice_cf_passcodes WHERE code = $1 AND expires > now() \
                 RETURNING identity",
            )
            .await?,
            client,
        });
        *aside = Some(opened.clone());
        Ok(opened)
    }

    /// What `f` asks on the connection of the logins.
    async fn asked<T>(
        &self,
        f: impl AsyncFnOnce(&Aside) -> Result<T, tokio_postgres::Error>,
    ) -> Result<T, String> {
        let asked = async {
            let aside = self.aside().await?;
            f(&aside).await.map_err(failed)
        };
        match tokio::time::timeout(ASIDE_TIMEOUT, asked).await {
            Ok(asked) => asked,
            Err(_) => {
                *self.aside.lock().await = None;
                Err("--state: no answer".to_string())
            }
        }
    }

    /// The keys of the tokens, oldest first: one replaced is among them for `kept` seconds
    /// after the next was made, by the store's clock.
    pub async fn keys(&self, kept: i64) -> Result<Vec<StoredKey>, String> {
        let rows = self.asked(async |aside| aside.client.query(&aside.keys, &[&kept]).await);
        Ok((rows.await?.iter())
            .map(|row| StoredKey {
                kid: row.get(0),
                key: row.get(1),
                signs: row.get(2),
            })
            .collect())
    }

    /// Makes `key` the first, with the `sessions` (their `jti`, subject and expiry in seconds
    /// since the epoch) signed with it, unless there is a key: one of several instances
    /// starting makes it.
    pub async fn first_key(
        &self,
        kid: &str,
        key: &[u8],
        sessions: &[(String, String, i64)],
    ) -> Result<(), String> {
        let jtis: Vec<&str> = sessions.iter().map(|(jti, _, _)| jti.as_str()).collect();
        let subjects: Vec<&str> = sessions
            .iter()
            .map(|(_, subject, _)| subject.as_str())
            .collect();
        let expiries: Vec<i64> = sessions.iter().map(|(_, _, expires)| *expires).collect();
        let first: [&(dyn ToSql + Sync); 5] = [&kid, &key, &jtis, &subjects, &expiries];
        self.asked(async |aside| aside.client.execute(&aside.first_key, &first).await)
            .await
            .map(drop)
    }

    /// Keeps the session `jti` for `secs`, the expired ones removed and of the others the
    /// `most` expiring last kept.
    pub async fn put_session(
        &self,
        jti: &str,
        subject: &str,
        secs: i64,
        most: i64,
    ) -> Result<(), String> {
        let put: [&(dyn ToSql + Sync); 4] = [&jti, &subject, &secs, &most];
        self.asked(async |aside| aside.client.execute(&aside.put_session, &put).await)
            .await
            .map(drop)
    }

    /// Makes the session `old` the session `new`, good for `secs`: its subject, to the one
    /// of those asking at once who gets it; None: there is none, or no more.
    pub async fn rotate_session(
        &self,
        old: &str,
        new: &str,
        secs: i64,
    ) -> Result<Option<String>, String> {
        let rotated: [&(dyn ToSql + Sync); 3] = [&old, &new, &secs];
        let row = self
            .asked(async |aside| (aside.client.query_opt(&aside.rotate_session, &rotated)).await);
        Ok(row.await?.map(|row| row.get(0)))
    }

    /// The subject of the session `jti`.
    pub async fn session(&self, jti: &str) -> Result<Option<String>, String> {
        let row = self.asked(async |aside| aside.client.query_opt(&aside.session, &[&jti]).await);
        Ok(row.await?.map(|row| row.get(0)))
    }

    pub async fn drop_session(&self, jti: &str) -> Result<(), String> {
        self.asked(async |aside| aside.client.execute(&aside.drop_session, &[&jti]).await)
            .await
            .map(drop)
    }

    /// Keeps the passcode `code` for `secs`; false: it is taken, or `most` wait already.
    pub async fn put_passcode(
        &self,
        code: &str,
        identity: &str,
        secs: i64,
        most: i64,
    ) -> Result<bool, String> {
        let put: [&(dyn ToSql + Sync); 4] = [&code, &identity, &secs, &most];
        let put = self.asked(async |aside| aside.client.execute(&aside.put_passcode, &put).await);
        Ok(put.await? == 1)
    }

    /// Takes the passcode `code`: its identity, to one of those asking at once.
    pub async fn take_passcode(&self, code: &str) -> Result<Option<String>, String> {
        let row = self
            .asked(async |aside| (aside.client.query_opt(&aside.take_passcode, &[&code])).await);
        Ok(row.await?.map(|row| row.get(0)))
    }
}
