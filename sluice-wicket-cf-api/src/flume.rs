//! The apps' own logs from the live access log: a watch of the sluice-flume
//! (`--flume`) for what the wicket feeds of the apps' hosts -- the guests'
//! output (`stdout`, `stderr`) and the requests answered (`response`) -- each
//! line added to the log of the app holding its host, as Cloud Foundry has
//! it: the output as `APP/PROC/WEB` OUT / ERR, a request as `RTR` in the
//! gorouter's access log format.
//!
//! ```text
//! wicket --Feed--> sluice-flume <--Watch-- cf-api --log-cache read API--> cf logs
//! ```
//!
//! The watch resumes past the last line it took (the cursor, kept with the
//! state) when the apps' hosts change or the stream drops. The lines lost on
//! the way -- dropped by the wicket, passed while the watch fell behind, or
//! gone with a restart of the flume -- become a line saying so (`LGR`) in
//! every app's log, as they may be any app's. Without a host there is no
//! watch: the flume passes every line to an empty filter.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::transport::Endpoint;

use crate::logs::{Logs, Stream};
use crate::proto::flume_client::FlumeClient;
use crate::proto::{AccessEvent, AccessFilter, Cursor, WatchRequest, watch_event, watch_gap};
use crate::store::{FlumeCursor, State, Store, rfc3339};
use crate::tls;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The kinds of the wicket's lines the apps' logs show.
const KINDS: [&str; 3] = ["response", "stderr", "stdout"];

/// How the wickets are named among the flume's nodes (`wicket:<client-id>`):
/// the lines a sluice node drops are never the apps'.
const WICKET_NODE: &str = "wicket:";

const MIN_BACKOFF: Duration = Duration::from_secs(1);

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How often a watch failing again is reported.
const WARN_EVERY: Duration = Duration::from_secs(300);

/// Bound on one connect attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a cursor moved is written with the state.
const KEEP_EVERY: Duration = Duration::from_secs(5);

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

/// Watches the flume for good: one watch at a time while an app has a host,
/// redialed with a backoff whenever its stream ends.
pub async fn watch(flume: FlumeSettings, store: Arc<Store>, logs: Arc<Logs>) {
    let mut backoff = MIN_BACKOFF;
    let mut warned: Option<Instant> = None;
    let mut suppressed = 0u64;
    loop {
        let began = Instant::now();
        let Err(e) = session(&flume, &store, &logs).await;
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

/// Watches until the stream ends: opened once an app has a host, opened
/// again past the cursor whenever the hosts change.
async fn session(
    flume: &FlumeSettings,
    store: &Store,
    logs: &Logs,
) -> Result<Infallible, BoxError> {
    let mut client = None;
    loop {
        let hosts = store.read(apps_by_host);
        if hosts.is_empty() {
            store.changed().await;
            continue;
        }
        let client = match &mut client {
            Some(client) => client,
            None => client.insert(FlumeClient::new(endpoint(flume)?.connect().await?)),
        };
        let after = store.read(|state| state.flume);
        let mut request = tonic::Request::new(WatchRequest {
            filter: Some(AccessFilter {
                route_keys: hosts.keys().cloned().collect(),
                kinds: KINDS.iter().map(|kind| kind.to_string()).collect(),
                node_ids: Vec::new(),
            }),
            after: after.map(|cursor| Cursor {
                epoch: cursor.epoch,
                seq: cursor.seq,
            }),
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {}", flume.token).parse()?);
        let mut events = client.watch(request).await?.into_inner();
        let mut taker = Taker {
            store,
            logs,
            hosts,
            epoch: after.map_or(0, |cursor| cursor.epoch),
            moved: false,
        };
        let mut keep = tokio::time::interval(KEEP_EVERY);
        let ended = loop {
            tokio::select! {
                event = events.message() => match event {
                    Ok(Some(event)) => {
                        if let Some(body) = event.body {
                            taker.take(body);
                        }
                    }
                    Ok(None) => break Some("the flume ended the watch".into()),
                    Err(e) => break Some(BoxError::from(e)),
                },
                // the apps' hosts changed: watched again past the cursor
                () = store.changed() => {
                    if store.read(apps_by_host) != taker.hosts {
                        break None;
                    }
                }
                _ = keep.tick() => taker.keep(),
            }
        };
        taker.keep();
        if let Some(e) = ended {
            return Err(e);
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

/// Takes a watch's lines into the apps' logs, its position into the state.
struct Taker<'a> {
    store: &'a Store,
    logs: &'a Logs,
    /// The hosts watched, and the app of each.
    hosts: BTreeMap<String, String>,
    /// The flume's run the watch sees.
    epoch: u64,
    /// The cursor moved since it was last written.
    moved: bool,
}

impl Taker<'_> {
    fn take(&mut self, event: watch_event::Body) {
        match event {
            watch_event::Body::Hello(hello) => {
                if hello.reset {
                    self.lost("Lines may be missing: the flume restarted".into());
                }
                if self.epoch != hello.epoch {
                    // a position of another run: none until a line of this one
                    self.store.note(|state| state.flume = None);
                    self.moved = true;
                    self.epoch = hello.epoch;
                }
            }
            watch_event::Body::Record(record) => {
                self.advance(record.seq);
                let Some(event) = record.event else {
                    return;
                };
                let Some(app) = self.hosts.get(&event.route_key) else {
                    return;
                };
                let (source, stream, text) = match event.kind.as_str() {
                    "stdout" => ("APP/PROC/WEB", Stream::Out, field(&event, "line")),
                    "stderr" => ("APP/PROC/WEB", Stream::Err, field(&event, "line")),
                    "response" => ("RTR", Stream::Out, router_line(&event, app)),
                    _ => return,
                };
                self.logs.push(app, source, stream, text);
            }
            watch_event::Body::Gap(gap) => {
                if gap.seq > 0 {
                    self.advance(gap.seq);
                }
                let dropped = gap.dropped;
                match gap.cause() {
                    watch_gap::Cause::NodeDropped if !gap.node_id.starts_with(WICKET_NODE) => {}
                    watch_gap::Cause::NodeDropped => self.lost(format!(
                        "Up to {dropped} lines missed: the wicket's queue to the flume was full"
                    )),
                    _ => self.lost(format!(
                        "Up to {dropped} lines missed: the cf-api fell behind the flume"
                    )),
                }
            }
            watch_event::Body::Node(_) => {}
        }
    }

    /// Moves the cursor to the line `seq` of the flume.
    fn advance(&mut self, seq: u64) {
        let cursor = FlumeCursor {
            epoch: self.epoch,
            seq,
        };
        self.store.note(|state| state.flume = Some(cursor));
        self.moved = true;
    }

    /// Writes the cursor moved.
    fn keep(&mut self) {
        if std::mem::take(&mut self.moved) {
            self.store.update(|_| ());
        }
    }

    /// Tells every app watched that lines may be lost: which app's they
    /// were is unknown.
    fn lost(&self, text: String) {
        let apps: BTreeSet<&String> = self.hosts.values().collect();
        for app in apps {
            self.logs.push(app, "LGR", Stream::Err, text.clone());
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

/// A response as the gorouter's access log has it: the host, when the
/// request began, its line, the status, the bytes (received unknown, sent),
/// the user agent, the client, the response time in seconds and the app.
fn router_line(event: &AccessEvent, app: &str) -> String {
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
        "{} - [{began}] \"{} {} HTTP/{}\" {} - {} \"-\" \"{}\" \"{}\" \"-\" response_time:{:.6} app_id:\"{app}\" app_index:\"0\"",
        event.route_key,
        field(event, "method"),
        field(event, "path"),
        field(event, "http"),
        field(event, "status"),
        field(event, "bytes"),
        field(event, "userAgent"),
        field(event, "remote"),
        took / 1000.0,
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

    fn event(seq: u64, kind: &str, host: &str, fields: &[(&str, &str)]) -> watch_event::Body {
        watch_event::Body::Record(WatchRecord {
            seq,
            node_id: "wicket:w".into(),
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

    fn gap(node_id: &str, cause: watch_gap::Cause) -> watch_event::Body {
        watch_event::Body::Gap(WatchGap {
            seq: 0,
            node_id: node_id.into(),
            dropped: 3,
            cause: cause as i32,
        })
    }

    /// The lines of `app`: source, stream and text, oldest first.
    fn lines(logs: &Logs, app: &str) -> Vec<(&'static str, Stream, String)> {
        logs.read(app, 0, None, 100, false)
            .into_iter()
            .map(|line| (line.source, line.stream, line.text))
            .collect()
    }

    /// Each line goes to the app holding its host, as cf has it: the output
    /// `APP/PROC/WEB` OUT / ERR, a response `RTR` in the gorouter's format.
    /// The lines lost reach every app watched -- a sluice node's drops none
    /// of them -- and the cursor follows the lines taken, of the flume's run
    /// at hand.
    #[test]
    fn lines_reach_the_app_of_their_host_and_losses_every_app() {
        let dir = std::env::temp_dir().join(format!("sluice-cf-api-flume-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Store::open(&dir, "o", "s", &["localhost".to_string()]).unwrap();
        store.update(|state| {
            state.flume = Some(FlumeCursor { epoch: 1, seq: 9 });
        });
        let logs = Logs::default();
        let mut taker = Taker {
            store: &store,
            logs: &logs,
            hosts: BTreeMap::from([
                ("a.localhost".to_string(), "app-a".to_string()),
                ("b.localhost".to_string(), "app-b".to_string()),
                ("hi.localhost".to_string(), "app-a".to_string()),
            ]),
            epoch: 1,
            moved: false,
        };
        taker.take(watch_event::Body::Hello(WatchHello {
            epoch: 2,
            reset: true,
            build: None,
        }));
        assert_eq!(store.read(|state| state.flume), None, "another run's");
        taker.take(event(1, "stdout", "a.localhost", &[("line", "out")]));
        taker.take(event(2, "stderr", "hi.localhost", &[("line", "err")]));
        taker.take(event(
            3,
            "response",
            "b.localhost",
            &[
                ("method", "GET"),
                ("path", "/x?y=1"),
                ("http", "1.1"),
                ("status", "200"),
                ("durationMs", "2.500"),
                ("bytes", "13"),
                ("remote", "10.0.0.1"),
                ("userAgent", "curl/8"),
            ],
        ));
        taker.take(event(
            4,
            "stdout",
            "c.localhost",
            &[("line", "not watched")],
        ));
        taker.take(gap("node-0", watch_gap::Cause::NodeDropped));
        taker.take(gap("wicket:w", watch_gap::Cause::NodeDropped));
        taker.take(gap("", watch_gap::Cause::Behind));

        let restarted = (
            "LGR",
            Stream::Err,
            "Lines may be missing: the flume restarted".to_string(),
        );
        let dropped = (
            "LGR",
            Stream::Err,
            "Up to 3 lines missed: the wicket's queue to the flume was full".to_string(),
        );
        let behind = (
            "LGR",
            Stream::Err,
            "Up to 3 lines missed: the cf-api fell behind the flume".to_string(),
        );
        assert_eq!(
            lines(&logs, "app-a"),
            [
                restarted.clone(),
                ("APP/PROC/WEB", Stream::Out, "out".to_string()),
                ("APP/PROC/WEB", Stream::Err, "err".to_string()),
                dropped.clone(),
                behind.clone(),
            ]
        );
        assert_eq!(
            lines(&logs, "app-b"),
            [
                restarted,
                (
                    "RTR",
                    Stream::Out,
                    "b.localhost - [2026-10-30T01:17:47.497Z] \"GET /x?y=1 HTTP/1.1\" 200 - 13 \
                     \"-\" \"curl/8\" \"10.0.0.1\" \"-\" response_time:0.002500 \
                     app_id:\"app-b\" app_index:\"0\""
                        .to_string()
                ),
                dropped,
                behind,
            ]
        );
        assert_eq!(
            store.read(|state| state.flume),
            Some(FlumeCursor { epoch: 2, seq: 4 }),
            "the last line taken"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
