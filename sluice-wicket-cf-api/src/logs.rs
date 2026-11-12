//! The log lines `cf logs` reads through log-cache's read API: an in-memory
//! ring per app (the source id is the app's guid) of the cf-api's own lines --
//! staging (`STG`), lifecycle (`API`) and the wicket's outcome (`CELL`) --
//! and, with a flume, the app's own from the live access log (`flume`): its
//! output (`APP/PROC/WEB`), its requests (`RTR`) and the lines lost (`LGR`).
//!
//! With a flume the cf-api's own lines go round it too: fed as a node's
//! ([`FEED_ROUTE`]), taken into the ring as the watch passes them, at the time
//! the flume took them. Every instance watching holds the same lines at the
//! same timestamps, whichever added them, so a reader walks on from one to
//! another; so do the readers following a staging's log ([`Logs::following`]).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Value, json};
use sluice_wicket_tunnel::flume::Tap;

use crate::store::epoch_nanos;

/// Lines kept per app.
const RING: usize = 1000;

/// How long since its last read a reader walking an app's lines is taken as following them:
/// the CLI reads every 250ms while nothing is new (`cliRetryBackoff`, v8.19.0).
const FOLLOWING: Duration = Duration::from_secs(1);

/// How often [`Logs::read_out`] looks again.
const LOOK: Duration = Duration::from_millis(50);

/// How long the lines of an app the state has not are kept since the last came: a line may
/// come round before its app does, and after it went.
const UNKNOWN: Duration = Duration::from_secs(60);

/// The route key of what the instances feed the flume: no host.
pub const FEED_ROUTE: &str = "cf-api:logs";

/// The kind of a line an instance fed: `app`, `source`, `stream` (`out` / `err`), `line`, and
/// which of its run's lines of the app it is (`run`, `n`).
pub const FED_LINE: &str = "cf-log";

/// The kind of a reader's walk an instance fed: `app`, `start`.
pub const FED_WALK: &str = "cf-walk";

/// The sources of the cf-api's own lines.
const OWN: [&str; 3] = ["STG", "API", "CELL"];

/// The stream a line is of, as `cf logs` tells them apart.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stream {
    Out,
    Err,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Line {
    /// Nanoseconds since the epoch, unique and ascending within an app: when the line was
    /// added (with a flume: when the flume took it), so a reader past a line misses none
    /// after it.
    pub timestamp: u128,
    pub source: &'static str,
    /// The index of the source's instance the line is of (`APP/PROC/WEB/1`);
    /// 0 for the sources of one instance.
    pub instance: u64,
    pub stream: Stream,
    pub text: String,
}

#[derive(Default)]
struct Ring {
    lines: VecDeque<Line>,
    /// The `start_time` of the last read walking the lines, and when it came: the CLI walks
    /// from the line after the last it printed.
    walked: Option<(u128, tokio::time::Instant)>,
    /// How many lines this run fed the flume, and the last of them that came round.
    fed: u64,
    returned: u64,
    /// When a line last came.
    touched: Option<tokio::time::Instant>,
}

#[derive(Default)]
pub struct Logs {
    rings: Mutex<HashMap<String, Ring>>,
    /// What feeds this instance's lines to the flume, and this run's name among its node's;
    /// None: no flume, a line is its ring's as it is added.
    feed: Option<(Arc<Tap>, String)>,
}

impl Logs {
    /// The logs of an instance on a flume: its own lines go to `tap`, and are its rings' once
    /// the watch passes them ([`Logs::fed`]).
    pub fn feeding(tap: Arc<Tap>) -> Logs {
        Logs {
            rings: Mutex::default(),
            feed: Some((tap, uuid::Uuid::new_v4().to_string())),
        }
    }

    /// Adds a line of the cf-api's own, printed to its stdout too.
    pub fn add(&self, app: &str, source: &'static str, text: impl Into<String>) {
        self.add_as(app, source, Stream::Out, text);
    }

    /// [`Logs::add`] of a line of `stream`: a build's output, as it came.
    pub fn add_as(&self, app: &str, source: &'static str, stream: Stream, text: impl Into<String>) {
        let text = text.into();
        println!("[{app}] {source}: {text}");
        let Some((tap, run)) = &self.feed else {
            self.push(app, source, 0, stream, text, Some(epoch_nanos()));
            return;
        };
        // fed under the lock: in the order of their numbers
        let mut rings = self.rings.lock().expect("no panic under the lock");
        let ring = rings.entry(app.to_string()).or_default();
        ring.fed += 1;
        let stream = match stream {
            Stream::Out => "out",
            Stream::Err => "err",
        };
        let fields = vec![
            ("app", app.to_string()),
            ("source", source.to_string()),
            ("stream", stream.to_string()),
            ("line", text),
            ("run", run.clone()),
            ("n", ring.fed.to_string()),
        ];
        tap.publish(FED_LINE, FEED_ROUTE.to_string(), fields);
    }

    /// Takes what an instance fed, as the watch of the flume passes it: a line (taken by the
    /// flume at `at`), or a reader's walk. Its field `name` is `field(name)`.
    pub fn fed<'a>(&self, kind: &str, field: impl Fn(&str) -> Option<&'a str>, at: Option<u128>) {
        let Some(app) = field("app") else {
            return;
        };
        match kind {
            FED_LINE => {
                // a source of a build past this one's: not a line it tells
                let source = OWN.iter().find(|own| field("source") == Some(**own));
                let (Some(source), Some(text)) = (source, field("line")) else {
                    return;
                };
                let stream = match field("stream") {
                    Some("err") => Stream::Err,
                    _ => Stream::Out,
                };
                self.push(app, source, 0, stream, text.to_string(), at);
                let own = (self.feed.as_ref()).is_some_and(|(_, run)| field("run") == Some(run));
                let n = field("n").and_then(|n| n.parse::<u64>().ok());
                if let (true, Some(n)) = (own, n) {
                    let mut rings = self.rings.lock().expect("no panic under the lock");
                    if let Some(ring) = rings.get_mut(app) {
                        ring.returned = ring.returned.max(n);
                    }
                }
            }
            FED_WALK => {
                let Some(start) = field("start").and_then(|start| start.parse().ok()) else {
                    return;
                };
                let mut rings = self.rings.lock().expect("no panic under the lock");
                if let Some(ring) = rings.get_mut(app) {
                    ring.walked = Some((start, tokio::time::Instant::now()));
                }
            }
            _ => {}
        }
    }

    /// Adds a line that came `at` (nanoseconds since the epoch; None: right after the last,
    /// as a line of the watch itself): one of the app's own, which its wicket logs already.
    pub fn push(
        &self,
        app: &str,
        source: &'static str,
        instance: u64,
        stream: Stream,
        text: String,
        at: Option<u128>,
    ) {
        let mut rings = self.rings.lock().expect("no panic under the lock");
        let ring = rings.entry(app.to_string()).or_default();
        ring.touched = Some(tokio::time::Instant::now());
        let ring = &mut ring.lines;
        let next = ring.back().map(|last| last.timestamp + 1);
        let timestamp = match (at, next) {
            (Some(at), next) => at.max(next.unwrap_or(0)),
            (None, Some(next)) => next,
            (None, None) => epoch_nanos(),
        };
        ring.push_back(Line {
            timestamp,
            source,
            instance,
            stream,
            text,
        });
        while ring.len() > RING {
            ring.pop_front();
        }
    }

    /// The lines of `app` from `start` (inclusive) to `end` (exclusive), at
    /// most `limit`; the newest first with `descending`, and then the newest
    /// `limit` of them. A read in order walks them: its reader is past what is before `start`.
    pub fn read(
        &self,
        app: &str,
        start: u128,
        end: Option<u128>,
        limit: usize,
        descending: bool,
    ) -> Vec<Line> {
        let mut rings = self.rings.lock().expect("no panic under the lock");
        if !descending && let Some(ring) = rings.get_mut(app) {
            ring.walked = Some((start, tokio::time::Instant::now()));
        }
        let lines = rings
            .get(app)
            .into_iter()
            .flat_map(|ring| &ring.lines)
            .filter(|line| line.timestamp >= start && end.is_none_or(|end| line.timestamp < end));
        if descending {
            lines.rev().take(limit).cloned().collect()
        } else {
            lines.take(limit).cloned().collect()
        }
    }

    /// Tells the other instances of a read walking `app`'s lines from `start` here: the one
    /// staging waits for the reader of its log ([`Logs::read_out`]), which may read from any.
    /// Nothing without a flume: the one instance's readers are its own.
    pub fn following(&self, app: &str, start: u128) {
        if let Some((tap, _)) = &self.feed {
            let fields = vec![("app", app.to_string()), ("start", start.to_string())];
            tap.publish(FED_WALK, FEED_ROUTE.to_string(), fields);
        }
    }

    /// Waits, `within` at most, for a reader following `app`'s lines to walk past the last of
    /// them, and a look more for it to print them: the CLI walks the staging log 2s behind and
    /// stops at the staging's end, which would cut its last lines. No reader following
    /// ([`FOLLOWING`] for one to start: the CLI starts as the staging does): no wait more.
    /// With a flume, the last line is the last this instance fed, once it came round.
    pub async fn read_out(&self, app: &str, within: Duration) {
        let started = tokio::time::Instant::now();
        let deadline = started + within;
        loop {
            // whether the lines fed came round, a reader follows, and it is past the last line
            let (round, following) = {
                let rings = self.rings.lock().expect("no panic under the lock");
                rings.get(app).map_or((true, None), |ring| {
                    let last = ring.lines.back().map_or(0, |line| line.timestamp);
                    let following = ring
                        .walked
                        .and_then(|(start, at)| (at.elapsed() < FOLLOWING).then_some(start > last));
                    (ring.returned >= ring.fed, following)
                })
            };
            let late = tokio::time::Instant::now() >= deadline;
            match following {
                _ if !round && !late => tokio::time::sleep(LOOK).await,
                None if started.elapsed() >= FOLLOWING => return,
                Some(true) if round => break,
                Some(_) if late => return,
                _ => tokio::time::sleep(LOOK).await,
            }
        }
        tokio::time::sleep(LOOK).await;
    }

    pub fn forget(&self, app: &str) {
        self.rings
            .lock()
            .expect("no panic under the lock")
            .remove(app);
    }

    /// Forgets the apps that are not `known` whose last line came [`UNKNOWN`] ago: an app
    /// another instance deleted.
    pub fn keep(&self, known: impl Fn(&str) -> bool) {
        let quiet = |ring: &Ring| ring.touched.is_none_or(|at| at.elapsed() >= UNKNOWN);
        self.rings
            .lock()
            .expect("no panic under the lock")
            .retain(|app, ring| known(app) || !quiet(ring));
    }

    /// The `start_time` of the last read walking `app`'s lines, here or on another instance.
    #[cfg(test)]
    pub fn walked(&self, app: &str) -> Option<u128> {
        let rings = self.rings.lock().expect("no panic under the lock");
        rings.get(app)?.walked.map(|(start, _)| start)
    }
}

/// Log-cache's read answer: the lines as log envelopes.
pub fn envelopes(app: &str, lines: &[Line]) -> Value {
    let batch: Vec<Value> = lines
        .iter()
        .map(|line| {
            json!({
                "timestamp": line.timestamp.to_string(),
                "source_id": app,
                "instance_id": line.instance.to_string(),
                "deprecated_tags": {},
                "tags": {"source_type": line.source},
                "log": {
                    "payload": base64::engine::general_purpose::STANDARD.encode(&line.text),
                    "type": match line.stream {
                        Stream::Out => "OUT",
                        Stream::Err => "ERR",
                    },
                },
            })
        })
        .collect();
    json!({"envelopes": {"batch": batch}})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_window_in_either_order() {
        let logs = Logs::default();
        for i in 0..5 {
            logs.add("a", "STG", format!("line {i}"));
        }
        let all = logs.read("a", 0, None, 100, false);
        assert_eq!(all.len(), 5);
        assert!(
            all.windows(2)
                .all(|pair| pair[0].timestamp < pair[1].timestamp)
        );
        let from_third = logs.read("a", all[2].timestamp, Some(all[4].timestamp), 100, false);
        assert_eq!(
            from_third
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["line 2", "line 3"]
        );
        let newest = logs.read("a", 0, None, 2, true);
        assert_eq!(
            newest
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["line 4", "line 3"]
        );
        assert!(logs.read("b", 0, None, 10, false).is_empty());
        let view = envelopes("a", &newest);
        assert_eq!(view["envelopes"]["batch"][0]["tags"]["source_type"], "STG");
        assert_eq!(view["envelopes"]["batch"][0]["instance_id"], "0");
        assert_eq!(view["envelopes"]["batch"][0]["log"]["payload"], "bGluZSA0");
        // an app's line, of its instance
        logs.push("c", "APP/PROC/WEB", 2, Stream::Err, "x".into(), None);
        let view = envelopes("c", &logs.read("c", 0, None, 10, false));
        assert_eq!(view["envelopes"]["batch"][0]["instance_id"], "2");
        assert_eq!(view["envelopes"]["batch"][0]["log"]["type"], "ERR");
    }

    #[tokio::test(start_paused = true)]
    async fn a_staging_ends_once_the_reader_following_it_is_past_its_last_line() {
        use tokio::time::Instant;
        let within = Duration::from_secs(5);
        let logs = std::sync::Arc::new(Logs::default());
        logs.add("a", "STG", "Staged");
        // no reader: a wait for one to start, no more
        let start = Instant::now();
        logs.read_out("a", within).await;
        assert_eq!(start.elapsed(), FOLLOWING);
        // the CLI's walk: from the start until the line is 2s old, then past it
        let last = logs.read("a", 0, None, 100, false)[0].timestamp;
        let reader = {
            let logs = logs.clone();
            tokio::spawn(async move {
                for _ in 0..8 {
                    logs.read("a", 0, None, 100, false);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                logs.read("a", last + 1, None, 100, false);
            })
        };
        let start = Instant::now();
        logs.read_out("a", within).await;
        // at its next look, and a look more
        let waited = start.elapsed();
        assert!(waited > Duration::from_secs(2) && waited <= Duration::from_secs(2) + LOOK * 2);
        reader.await.unwrap();
        // a reader gone: no reader
        logs.add("a", "STG", "again");
        tokio::time::sleep(FOLLOWING).await;
        let start = Instant::now();
        logs.read_out("a", within).await;
        assert_eq!(start.elapsed(), FOLLOWING);

        // on a flume: a line is the ring's once it came round, at the flume's time, and a
        // staging's end waits for the last fed; another instance's reader is one too
        let logs = Logs::feeding(Tap::new());
        logs.add("a", "STG", "fed");
        assert!(logs.read("a", 0, None, 100, false).is_empty());
        let start = Instant::now();
        logs.read_out("a", within).await;
        assert!(start.elapsed() >= within, "the line fed never came round");
        let run = logs.feed.as_ref().unwrap().1.clone();
        let fed = |kind: &str, fields: &[(&'static str, &str)], at| {
            let field = |name: &str| {
                let found = fields.iter().find(|(field, _)| *field == name);
                found.map(|(_, value)| *value)
            };
            logs.fed(kind, field, at);
        };
        let line = [("app", "a"), ("source", "STG"), ("line", "fed"), ("n", "1")];
        fed(
            FED_LINE,
            &[&line[..], &[("run", "another")]].concat(),
            Some(5),
        );
        fed(
            FED_LINE,
            &[&line[..], &[("run", run.as_str())]].concat(),
            Some(7),
        );
        fed(
            FED_LINE,
            &[("app", "a"), ("source", "NEW"), ("line", "x")],
            Some(8),
        );
        let taken = logs.read("a", 0, None, 100, true);
        let times: Vec<u128> = taken.iter().map(|line| line.timestamp).collect();
        assert_eq!(times, [7, 5]);
        fed(FED_WALK, &[("app", "a"), ("start", "8")], None);
        let start = Instant::now();
        logs.read_out("a", within).await;
        assert_eq!(
            start.elapsed(),
            LOOK,
            "the other instance's reader is past it"
        );
        // an app the state has not goes once it is quiet
        logs.keep(|_| false);
        assert_eq!(logs.read("a", 0, None, 100, true).len(), 2);
        tokio::time::sleep(UNKNOWN).await;
        logs.keep(|app| app != "a");
        assert!(logs.read("a", 0, None, 100, true).is_empty());
    }
}
