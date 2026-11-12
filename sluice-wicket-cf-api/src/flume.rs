//! The apps' own logs from the live access log: a watch of the sluice-flume
//! (`--flume`) for what the wickets feed of the apps' hosts -- the guests'
//! output (`stdout`, `stderr`) and the requests answered (`response`) -- each
//! line added to the log of the app holding its host, as Cloud Foundry has
//! it: the output as `APP/PROC/WEB` OUT / ERR of the instance writing it, a
//! request as `RTR` in the gorouter's access log format, naming the instance
//! that served it (`app_index`). A line's wicket is the one listing its flume
//! node, its instance the app's index of that wicket's share
//! (`State::instance_of`); a request's `RTR` index is the wicket's place among
//! the wickets, each a router of its share.
//!
//! ```text
//! wicket --Feed--> sluice-flume <--Watch-- cf-api --log-cache read API--> cf logs
//! ```
//!
//! The cf-api's own lines (`crate::logs`) go round the flume too, fed as the
//! node `cf-api:<id>` (the id made in `--data-dir`, [`NODE_FILE`]): every
//! instance watching takes every instance's, at the time the flume took them,
//! so their logs are one.
//!
//! A watch begins with the oldest lines the flume has at hand: an instance
//! starting holds what the others do, within the flume's ring. It resumes
//! past the last line it took when the apps' hosts change or the stream
//! drops. The lines lost on the way -- dropped by a wicket or an instance,
//! passed while the watch fell behind, or gone with a restart of the flume --
//! become a line saying so (`LGR`) in every app's log, as they may be any
//! app's; those a wicket dropped, in the logs of the apps it holds indices
//! of. These are each watch's own: they may differ between instances.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sluice_wicket_tunnel::flume::Tap;
use tonic::transport::Endpoint;

use crate::logs::{FED_LINE, FED_WALK, FEED_ROUTE, Logs, Stream};
use crate::proto::flume_client::FlumeClient;
use crate::proto::{AccessEvent, AccessFilter, Cursor, WatchRequest, watch_event, watch_gap};
use crate::store::{State, Store, rfc3339};
use crate::tls;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The kinds of the lines the apps' logs show: the wickets', the instances'.
const KINDS: [&str; 5] = ["response", "stderr", "stdout", FED_LINE, FED_WALK];

/// How the wickets are named among the flume's nodes (`wicket:<client-id>`):
/// the lines a sluice node drops are never the apps'.
const WICKET_NODE: &str = "wicket:";

/// How the cf-api's instances are named among them (`cf-api:<id>`).
const API_NODE: &str = "cf-api:";

/// The file of `--data-dir` holding the instance's id among the flume's nodes, made at its
/// first start: another directory is another node.
pub const NODE_FILE: &str = "flume-node";

const MIN_BACKOFF: Duration = Duration::from_secs(1);

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How often a watch failing again is reported.
const WARN_EVERY: Duration = Duration::from_secs(300);

/// Bound on one connect attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The flume the apps' logs come from (`--flume`).
#[derive(Clone, Debug)]
pub struct FlumeSettings {
    /// `grpc://host:port` or `grpcs://host:port`.
    pub url: String,
    /// Its bearer token.
    pub token: String,
    /// The PEM CA verifying a `grpcs://` flume; None: the webPKI roots.
    pub ca_pem: Option<Vec<u8>>,
    /// Skip the verification of a `grpcs://` flume.
    pub insecure: bool,
}

/// The logs of an instance on the flume, and the tasks keeping them: the feed of its own
/// lines as the node `cf-api:<id>`, the id that of the data directory `dir`, and the watch.
pub fn start(
    flume: FlumeSettings,
    dir: &Path,
    store: Arc<Store>,
) -> Result<(Arc<Logs>, [tokio::task::JoinHandle<()>; 2]), String> {
    let node = node(dir)?;
    let tap = Tap::new();
    let logs = Arc::new(Logs::feeding(tap.clone()));
    println!("cf logs: feeding the flume {} as {node}", flume.url);
    let to = sluice_wicket_tunnel::flume::FlumeSettings {
        url: flume.url.clone(),
        token: flume.token.clone(),
        ca_cert: None,
        insecure: flume.insecure,
    };
    let feeding = sluice_wicket_tunnel::flume::feed(to, flume.ca_pem.clone(), tap, node);
    let watching = watch(flume, store, logs.clone());
    Ok((logs, [tokio::spawn(feeding), tokio::spawn(watching)]))
}

/// This instance's node among the flume's: `cf-api:` and the id [`NODE_FILE`] of `dir` holds,
/// made when it has none.
fn node(dir: &Path) -> Result<String, String> {
    let file = dir.join(NODE_FILE);
    let failed = |e: std::io::Error| format!("{}: {e}", file.display());
    let id = match std::fs::read_to_string(&file) {
        Ok(id) if !id.trim().is_empty() => id.trim().to_string(),
        Ok(_) | Err(_) => {
            let id = uuid::Uuid::new_v4().simple().to_string();
            std::fs::create_dir_all(dir).map_err(failed)?;
            std::fs::write(&file, format!("{id}\n")).map_err(failed)?;
            id
        }
    };
    Ok(format!("{API_NODE}{id}"))
}

/// Watches the flume for good: one watch at a time, redialed with a backoff
/// whenever its stream ends.
async fn watch(flume: FlumeSettings, store: Arc<Store>, logs: Arc<Logs>) {
    let mut backoff = MIN_BACKOFF;
    let mut warned: Option<Instant> = None;
    let mut suppressed = 0u64;
    let mut announced = None;
    let mut position = Position::default();
    loop {
        let began = Instant::now();
        let Err(e) = session(&flume, &store, &logs, &mut position, &mut announced).await;
        if warned.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
            let more = match suppressed {
                0 => String::new(),
                n => format!(" ({n} more since)"),
            };
            eprintln!(
                "cf logs: the watch of the flume {} ended: {e}{more}",
                flume.url
            );
            warned = Some(Instant::now());
            suppressed = 0;
            // the watch taken up again after the warning is said too
            announced = None;
        } else {
            suppressed += 1;
        }
        // a stream that held a while had the flume up: retry soon
        if began.elapsed() > MAX_BACKOFF {
            backoff = MIN_BACKOFF;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Watches until the stream ends: opened again past the last line taken
/// whenever the apps' hosts change. A watch opened for hosts other than those
/// `announced` last is said, with the hosts: a line of a host none of them is
/// in no app's log.
async fn session(
    flume: &FlumeSettings,
    store: &Store,
    logs: &Logs,
    position: &mut Position,
    announced: &mut Option<Vec<String>>,
) -> Result<Infallible, BoxError> {
    let mut client = FlumeClient::new(endpoint(flume)?.connect().await?);
    let mut changes = store.changes();
    loop {
        let hosts = store.read(apps_by_host);
        let watched: Vec<String> = hosts.keys().cloned().collect();
        let mut route_keys = watched.clone();
        route_keys.push(FEED_ROUTE.to_string());
        let mut request = tonic::Request::new(WatchRequest {
            filter: Some(AccessFilter {
                route_keys,
                kinds: KINDS.iter().map(|kind| kind.to_string()).collect(),
                node_ids: Vec::new(),
            }),
            after: position.cursor,
            // what the other instances hold
            oldest: true,
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {}", flume.token).parse()?);
        let mut events = client.watch(request).await?.into_inner();
        if !watched.is_empty() && announced.as_ref() != Some(&watched) {
            println!(
                "cf logs: watching the flume {} for {}",
                flume.url,
                watched.join(", ")
            );
            *announced = Some(watched);
        }
        let mut taker = Taker {
            store,
            logs,
            hosts,
            position: &mut *position,
        };
        loop {
            tokio::select! {
                event = events.message() => match event? {
                    Some(event) => {
                        if let Some(body) = event.body {
                            taker.take(body);
                        }
                    }
                    None => return Err("the flume ended the watch".into()),
                },
                // the apps' hosts changed: watched again past the last line taken
                _ = changes.changed() => {
                    logs.keep(|app| store.read(|state| state.app(app).is_some()));
                    if store.read(apps_by_host) != taker.hosts {
                        break;
                    }
                }
            }
        }
    }
}

/// The app holding each host mapped to one, by the host lowercase (the
/// route key of its lines).
fn apps_by_host(state: &State) -> BTreeMap<String, String> {
    state
        .routes
        .iter()
        .filter_map(|route| {
            let app = route.app.as_ref()?;
            Some((state.url(route).to_ascii_lowercase(), app.clone()))
        })
        .collect()
}

/// Where a watch resumes, and what tells a line fed twice.
#[derive(Default)]
struct Position {
    /// The flume's run the watch sees; 0 before one.
    epoch: u64,
    /// The last line taken of that run.
    cursor: Option<Cursor>,
    /// The last line taken of each node: its `node_seq` and time.
    last: HashMap<String, (u64, i64)>,
    /// [`Position::last`] as the flume's run before left it: a node feeds a flume started
    /// again its recent lines again, until one past these.
    before: HashMap<String, (u64, i64)>,
}

impl Position {
    /// Whether the node fed `event` to the flume's run before already.
    fn again(&mut self, node: &str, event: &AccessEvent) -> bool {
        let line = (event.node_seq, event.time_unix_millis);
        if let Some(&(seq, time)) = self.before.get(node) {
            if line.0 <= seq && line.1 <= time {
                return true;
            }
            self.before.remove(node);
        }
        self.last.insert(node.to_string(), line);
        false
    }
}

/// Takes a watch's lines into the apps' logs.
struct Taker<'a> {
    store: &'a Store,
    logs: &'a Logs,
    /// The hosts watched, and the app of each.
    hosts: BTreeMap<String, String>,
    position: &'a mut Position,
}

impl Taker<'_> {
    fn take(&mut self, event: watch_event::Body) {
        match event {
            watch_event::Body::Hello(hello) => {
                if hello.reset {
                    self.lost("Lines may be missing: the flume restarted".into());
                }
                if self.position.epoch != hello.epoch {
                    // a position of another run: none until a line of this one
                    self.position.cursor = None;
                    self.position.before = std::mem::take(&mut self.position.last);
                    self.position.epoch = hello.epoch;
                }
            }
            watch_event::Body::Record(record) => {
                self.advance(record.seq);
                let Some(event) = record.event else {
                    return;
                };
                if self.position.again(&record.node_id, &event) {
                    return;
                }
                // a flume predating the time: this instance's
                let at = match record.time_unix_nanos {
                    0 => crate::store::epoch_nanos(),
                    at => u128::from(at),
                };
                if event.route_key == FEED_ROUTE {
                    let field = |name: &str| {
                        let found = event.fields.iter().find(|field| field.name == name);
                        found.map(|field| field.value.as_str())
                    };
                    self.logs.fed(&event.kind, field, Some(at));
                    return;
                }
                let Some(app) = self.hosts.get(&event.route_key) else {
                    return;
                };
                let (served, router) = self.store.read(|state| {
                    let wicket = state.wickets.nodes.get(&record.node_id);
                    let served = instance(&event).map(|index| match state.app(app) {
                        Some(app) => state.instance_of(app, wicket.map(String::as_str), index),
                        None => index,
                    });
                    let mut configured = state.wickets.configured.iter();
                    let router = wicket.and_then(|wicket| configured.position(|at| at == wicket));
                    (served, router.unwrap_or(0) as u64)
                });
                // a wicket predating the instance index names none: 0, as before
                let index = served.unwrap_or(0);
                let (source, index, stream, text) = match event.kind.as_str() {
                    "stdout" => ("APP/PROC/WEB", index, Stream::Out, field(&event, "line")),
                    "stderr" => ("APP/PROC/WEB", index, Stream::Err, field(&event, "line")),
                    // each wicket is a router of its share: its place among the wickets
                    "response" => ("RTR", router, Stream::Out, router_line(&event, app, served)),
                    _ => return,
                };
                self.logs.push(app, source, index, stream, text, Some(at));
            }
            watch_event::Body::Gap(gap) => {
                if gap.seq > 0 {
                    self.advance(gap.seq);
                }
                let dropped = gap.dropped;
                match gap.cause() {
                    watch_gap::Cause::NodeDropped if gap.node_id.starts_with(WICKET_NODE) => {
                        self.dropped(&gap.node_id, dropped)
                    }
                    watch_gap::Cause::NodeDropped if gap.node_id.starts_with(API_NODE) => self
                        .lost(format!(
                            "Up to {dropped} lines missed: the cf-api's queue to the flume was full"
                        )),
                    watch_gap::Cause::NodeDropped => {}
                    _ => self.lost(format!(
                        "Up to {dropped} lines missed: the cf-api fell behind the flume"
                    )),
                }
            }
            watch_event::Body::Node(_) => {}
        }
    }

    /// Moves the position to the line `seq` of the flume.
    fn advance(&mut self, seq: u64) {
        self.position.cursor = Some(Cursor {
            epoch: self.position.epoch,
            seq,
        });
    }

    /// Tells every app that lines may be lost: which app's they were is unknown.
    fn lost(&self, text: String) {
        let mut apps: BTreeSet<String> = self.hosts.values().cloned().collect();
        let known = |state: &State| -> Vec<String> {
            state.apps.iter().map(|app| app.guid.clone()).collect()
        };
        apps.extend(self.store.read(known));
        self.lost_to(&apps, text);
    }

    /// Tells the apps watched that the wicket of the flume node `node` holds some of that its
    /// lines were dropped; every app watched, of a node no wicket listed.
    fn dropped(&self, node: &str, dropped: u64) {
        let (apps, who) = self.store.read(|state| {
            let wicket = state.wickets.nodes.get(node);
            let who = match (state.wickets.configured.len(), wicket) {
                (1, _) => "the wicket".to_string(),
                (_, Some(wicket)) => format!("the wicket {wicket}"),
                (_, None) => format!("the flume node {node}"),
            };
            let holds = |app: &&String| {
                wicket.is_none_or(|wicket| {
                    (state.app(app)).is_some_and(|app| state.indices(app).contains_key(wicket))
                })
            };
            let apps: BTreeSet<&String> = self.hosts.values().filter(holds).collect();
            (apps, who)
        });
        let text = format!("Up to {dropped} lines missed: {who}'s queue to the flume was full");
        self.lost_to(apps, text);
    }

    fn lost_to<'a>(&self, apps: impl IntoIterator<Item = &'a String>, text: String) {
        for app in apps {
            self.logs
                .push(app, "LGR", 0, Stream::Err, text.clone(), None);
        }
    }
}

/// The value of the event's field `name`, `-` when it has none.
fn field(event: &AccessEvent, name: &str) -> String {
    event
        .fields
        .iter()
        .find(|field| field.name == name)
        .map_or_else(|| "-".to_string(), |field| field.value.clone())
}

/// The index of the app's instance the event is of (`instance`): the one
/// writing the output, the one serving the request. None for a request no
/// instance served, and from a wicket predating it.
fn instance(event: &AccessEvent) -> Option<u64> {
    event
        .fields
        .iter()
        .find(|field| field.name == "instance")
        .and_then(|field| field.value.parse().ok())
}

/// A response as the gorouter's access log has it: the host, when the
/// request began, its line, the status, the bytes (received unknown, sent),
/// the user agent, the client, the response time in seconds, the app and
/// its instance that `served` it (`-` for none, as the gorouter has it).
fn router_line(event: &AccessEvent, app: &str, served: Option<u64>) -> String {
    let took: f64 = field(event, "durationMs").parse().unwrap_or(0.0);
    let began = event
        .time_unix_millis
        .saturating_sub(took.round() as i64)
        .max(0) as u64;
    let began = format!(
        "{}.{:03}Z",
        rfc3339(began / 1000).trim_end_matches('Z'),
        began % 1000
    );
    format!(
        "{} - [{began}] \"{} {} HTTP/{}\" {} - {} \"-\" \"{}\" \"{}\" \"-\" response_time:{:.6} app_id:\"{app}\" app_index:\"{}\"",
        event.route_key,
        field(event, "method"),
        field(event, "path"),
        field(event, "http"),
        field(event, "status"),
        field(event, "bytes"),
        field(event, "userAgent"),
        field(event, "remote"),
        took / 1000.0,
        served.map_or_else(|| "-".to_string(), |index| index.to_string()),
    )
}

/// The flume's endpoint: connect timeout, keepalive (a flume gone without a
/// word ends the stream), TLS for `grpcs://`.
fn endpoint(flume: &FlumeSettings) -> Result<Endpoint, BoxError> {
    let (secure, authority) = match flume.url.strip_prefix("grpcs://") {
        Some(authority) => (true, authority),
        None => (
            false,
            flume.url.strip_prefix("grpc://").unwrap_or(&flume.url),
        ),
    };
    let mut endpoint = Endpoint::from_shared(format!(
        "{}://{authority}",
        if secure { "https" } else { "http" }
    ))?
    .connect_timeout(CONNECT_TIMEOUT)
    .http2_keep_alive_interval(Duration::from_secs(30))
    .keep_alive_timeout(Duration::from_secs(10))
    .keep_alive_while_idle(true);
    if secure {
        endpoint = tls::configure(endpoint, flume.insecure, flume.ca_pem.as_deref())?;
    }
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::access_event::Field;
    use crate::proto::{WatchGap, WatchHello, WatchRecord};

    /// A line the wicket `a` fed.
    fn event(seq: u64, kind: &str, host: &str, fields: &[(&str, &str)]) -> watch_event::Body {
        watch_event::Body::Record(WatchRecord {
            seq,
            node_id: "wicket:a".into(),
            time_unix_nanos: seq,
            event: Some(AccessEvent {
                node_seq: 1,
                // 2026-10-30T01:17:47.500Z
                time_unix_millis: 1_793_323_067_500,
                kind: kind.into(),
                route_key: host.into(),
                fields: fields
                    .iter()
                    .map(|(name, value)| Field {
                        name: name.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
            }),
        })
    }

    /// `line` as the flume node `node` fed it.
    fn of(node: &str, mut line: watch_event::Body) -> watch_event::Body {
        if let watch_event::Body::Record(record) = &mut line {
            record.node_id = node.into();
        }
        line
    }

    /// `line` as its node's line `node_seq`, logged `later` milliseconds on.
    fn numbered(node_seq: u64, later: i64, mut line: watch_event::Body) -> watch_event::Body {
        if let watch_event::Body::Record(record) = &mut line {
            let event = record.event.as_mut().unwrap();
            event.node_seq = node_seq;
            event.time_unix_millis += later;
        }
        line
    }

    fn gap(node_id: &str, cause: watch_gap::Cause) -> watch_event::Body {
        watch_event::Body::Gap(WatchGap {
            seq: 0,
            node_id: node_id.into(),
            dropped: 3,
            cause: cause as i32,
        })
    }

    /// The lines of `app`: source, instance, stream and text, oldest first.
    fn lines(logs: &Logs, app: &str) -> Vec<(&'static str, u64, Stream, String)> {
        logs.read(app, 0, None, 100, false)
            .into_iter()
            .map(|line| (line.source, line.instance, line.stream, line.text))
            .collect()
    }

    /// Two instances on one flume hold one log: a line either added is in both, at one
    /// timestamp and in one order, and in one starting after it; a reader walking on one is
    /// the other's, whose staging's end waits for it. Each feeds as its directory's node.
    #[tokio::test]
    async fn the_instances_on_a_flume_hold_one_log() {
        use crate::controller::tests::until;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("grpc://{}", listener.local_addr().unwrap());
        let settings = sluice_flume::Settings {
            token: "f1ume".into(),
            ring_size: 100,
            ring_bytes: 1 << 20,
            backfill: 1,
            forget_after: None,
            tls: None,
            otlp: None,
        };
        tokio::spawn(sluice_flume::serve(
            listener,
            settings,
            std::future::pending(),
        ));
        let instance = async |name: &str| {
            let dir = format!("sluice-cf-api-flume-{name}-{}", std::process::id());
            let dir = std::env::temp_dir().join(dir);
            let _ = std::fs::remove_dir_all(&dir);
            let (domains, wickets) = (["localhost".to_string()], ["a".to_string()]);
            let runs = BTreeMap::new();
            let store = Store::open(&dir, "o", "s", &domains, &wickets, &runs, None, None);
            let flume = FlumeSettings {
                url: url.clone(),
                token: "f1ume".into(),
                ca_pem: None,
                insecure: false,
            };
            let (logs, _) = start(flume, &dir, Arc::new(store.await.unwrap())).unwrap();
            (logs, dir)
        };
        // newest first: not a reader walking them
        let all = |logs: &Logs| logs.read("app", 0, None, 100, true);
        let (first, first_dir) = instance("first").await;
        first.add("app", "STG", "one");
        first.add_as("app", "STG", Stream::Err, "two");
        until("the lines came round", || all(&first).len() == 2).await;
        let (second, second_dir) = instance("second").await;
        second.add("app", "API", "three");
        until("each holds every line", || {
            all(&first).len() == 3 && all(&second).len() == 3
        })
        .await;
        let held = all(&first);
        assert_eq!(held, all(&second));
        let shown: Vec<_> = (held.iter())
            .map(|line| (line.source, line.stream, line.text.as_str()))
            .collect();
        assert_eq!(
            shown,
            [
                ("API", Stream::Out, "three"),
                ("STG", Stream::Err, "two"),
                ("STG", Stream::Out, "one"),
            ]
        );
        let past = held[0].timestamp + 1;
        second.following("app", past);
        until("the reader is the first's too", || {
            first.walked("app") == Some(past)
        })
        .await;
        let began = Instant::now();
        first.read_out("app", Duration::from_secs(5)).await;
        assert!(began.elapsed() < Duration::from_secs(1), "a reader past it");
        assert_ne!(node(&first_dir).unwrap(), node(&second_dir).unwrap());
        for dir in [first_dir, second_dir] {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Each line goes to the app holding its host, as cf has it: the output
    /// `APP/PROC/WEB` OUT / ERR of its instance (0 from a wicket naming
    /// none), a response `RTR` in the gorouter's format, naming the instance
    /// that served it (`-` for none); an instance is the app's index of the
    /// share of the wicket listing the line's node, a response's `RTR` that
    /// wicket's place. The lines lost reach every app watched -- a sluice
    /// node's drops none of them, a wicket's those it holds indices of -- and
    /// the position follows the lines taken, of the flume's run at hand: the
    /// lines a node feeds a flume started again once more are taken once.
    #[tokio::test]
    async fn lines_reach_the_app_of_their_host_and_losses_every_app() {
        let dir = std::env::temp_dir().join(format!("sluice-cf-api-flume-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let localhost = ["localhost".to_string()];
        let wickets = ["a".to_string(), "b".to_string()];
        let runs = BTreeMap::new();
        let store = Store::open(&dir, "o", "s", &localhost, &wickets, &runs, None, None);
        let store = store.await.unwrap();
        // its index 0 on a, 1 on b
        let scaled = store
            .update(|state| {
                let space = state.spaces[0].guid.clone();
                let app = state.create_app("scaled", &space);
                app.process.instances = 2;
                app.guid.clone()
            })
            .await;
        store
            .note(|state| {
                state.wickets.nodes = [
                    ("wicket:a".to_string(), "a".to_string()),
                    ("wicket:b".to_string(), "b".to_string()),
                ]
                .into()
            })
            .await;
        let logs = Logs::default();
        let mut position = Position {
            epoch: 1,
            cursor: Some(Cursor { epoch: 1, seq: 9 }),
            ..Default::default()
        };
        let mut taker = Taker {
            store: &store,
            logs: &logs,
            hosts: BTreeMap::from([
                ("a.localhost".to_string(), "app-a".to_string()),
                ("b.localhost".to_string(), "app-b".to_string()),
                ("hi.localhost".to_string(), "app-a".to_string()),
                ("scaled.localhost".to_string(), scaled.clone()),
            ]),
            position: &mut position,
        };
        let restart = |epoch: u64| {
            watch_event::Body::Hello(WatchHello {
                epoch,
                reset: true,
                build: None,
            })
        };
        taker.take(restart(2));
        assert_eq!(taker.position.cursor, None, "another run's");
        taker.take(event(
            1,
            "stdout",
            "a.localhost",
            &[("instance", "1"), ("line", "out")],
        ));
        taker.take(event(2, "stderr", "hi.localhost", &[("line", "err")]));
        let response = |seq: u64, host: &str, status: &str, instance: Option<&str>| {
            let mut fields = vec![
                ("method", "GET"),
                ("path", "/x?y=1"),
                ("http", "1.1"),
                ("status", status),
                ("durationMs", "2.500"),
                ("bytes", "13"),
                ("remote", "10.0.0.1"),
                ("userAgent", "curl/8"),
            ];
            fields.extend(instance.map(|instance| ("instance", instance)));
            event(seq, "response", host, &fields)
        };
        taker.take(response(3, "b.localhost", "200", Some("1")));
        taker.take(response(4, "b.localhost", "503", None));
        taker.take(event(
            5,
            "stdout",
            "c.localhost",
            &[("line", "not watched")],
        ));
        taker.take(gap("node-0", watch_gap::Cause::NodeDropped));
        taker.take(gap("wicket:x", watch_gap::Cause::NodeDropped));
        taker.take(gap("wicket:b", watch_gap::Cause::NodeDropped));
        taker.take(gap("", watch_gap::Cause::Behind));

        let lost = |text: &str| ("LGR", 0, Stream::Err, text.to_string());
        let restarted = lost("Lines may be missing: the flume restarted");
        let dropped =
            lost("Up to 3 lines missed: the flume node wicket:x's queue to the flume was full");
        let behind = lost("Up to 3 lines missed: the cf-api fell behind the flume");
        assert_eq!(
            lines(&logs, "app-a"),
            [
                restarted.clone(),
                ("APP/PROC/WEB", 1, Stream::Out, "out".to_string()),
                ("APP/PROC/WEB", 0, Stream::Err, "err".to_string()),
                dropped.clone(),
                behind.clone(),
            ]
        );
        let router = |status: &str, index: &str| {
            (
                "RTR",
                0,
                Stream::Out,
                format!(
                    "b.localhost - [2026-10-30T01:17:47.497Z] \"GET /x?y=1 HTTP/1.1\" {status} - \
                     13 \"-\" \"curl/8\" \"10.0.0.1\" \"-\" response_time:0.002500 \
                     app_id:\"app-b\" app_index:\"{index}\""
                ),
            )
        };
        assert_eq!(
            lines(&logs, "app-b"),
            [
                restarted.clone(),
                router("200", "1"),
                router("503", "-"),
                dropped.clone(),
                behind.clone(),
            ]
        );
        assert_eq!(
            taker.position.cursor,
            Some(Cursor { epoch: 2, seq: 5 }),
            "the last line taken"
        );
        // b's instance past its share, a's request of one past it, b's request: named as
        // `cf app` lists them, each request by its wicket's router
        taker.take(of(
            "wicket:b",
            event(
                6,
                "stdout",
                "scaled.localhost",
                &[("instance", "3"), ("line", "past")],
            ),
        ));
        taker.take(response(7, "scaled.localhost", "200", Some("5")));
        taker.take(of(
            "wicket:b",
            response(8, "scaled.localhost", "200", Some("0")),
        ));
        let of_scaled = lines(&logs, &scaled);
        let shown: Vec<_> = (of_scaled.iter())
            .map(|(source, index, _, text)| (*source, *index, text.rsplit(' ').next().unwrap()))
            .collect();
        assert_eq!(
            shown,
            [
                ("LGR", 0, "restarted"),
                ("LGR", 0, "full"),
                ("LGR", 0, "full"),
                ("LGR", 0, "flume"),
                ("APP/PROC/WEB", 1, "past"),
                ("RTR", 0, "app_index:\"0\""),
                ("RTR", 1, "app_index:\"1\""),
            ]
        );
        assert_eq!(
            of_scaled[2].3,
            "Up to 3 lines missed: the wicket b's queue to the flume was full"
        );
        // the flume started again: b feeds its recent lines again, then one more, and a's
        // next run its first
        let output = |seq: u64, line: &str| {
            let fields = [("instance", "3"), ("line", line)];
            event(seq, "stdout", "scaled.localhost", &fields)
        };
        taker.take(restart(3));
        taker.take(of("wicket:b", output(1, "past")));
        taker.take(of("wicket:b", numbered(2, 1, output(2, "new"))));
        taker.take(numbered(1, 1, output(3, "first")));
        let of_scaled = lines(&logs, &scaled);
        let texts: Vec<&str> = (of_scaled[7..].iter())
            .map(|(_, _, _, text)| text.as_str())
            .collect();
        let restarted = "Lines may be missing: the flume restarted";
        assert_eq!(texts, [restarted, "new", "first"]);
        assert_eq!(position.cursor, Some(Cursor { epoch: 3, seq: 3 }));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
