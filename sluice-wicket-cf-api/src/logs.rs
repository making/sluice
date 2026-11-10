//! The log lines `cf logs` reads through log-cache's read API: an in-memory
//! ring per app (the source id is the app's guid) of the cf-api's own lines --
//! staging (`STG`), lifecycle (`API`) and the wicket's outcome (`CELL`) --
//! and, with a flume, the app's own from the live access log (`flume`): its
//! output (`APP/PROC/WEB`), its requests (`RTR`) and the lines lost (`LGR`).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Value, json};

use crate::store::epoch_nanos;

/// Lines kept per app.
const RING: usize = 1000;

/// How long since its last read a reader walking an app's lines is taken as following them:
/// the CLI reads every 250ms while nothing is new (`cliRetryBackoff`, v8.19.0).
const FOLLOWING: Duration = Duration::from_secs(1);

/// How often [`Logs::read_out`] looks again.
const LOOK: Duration = Duration::from_millis(50);

/// The stream a line is of, as `cf logs` tells them apart.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stream {
    Out,
    Err,
}

#[derive(Clone, Debug)]
pub struct Line {
    /// Nanoseconds since the epoch, unique and ascending within an app:
    /// when the line was added, so a reader past a line misses none after it.
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
}

#[derive(Default)]
pub struct Logs {
    rings: Mutex<HashMap<String, Ring>>,
}

impl Logs {
    /// Adds a line of the cf-api's own, printed to its stdout too.
    pub fn add(&self, app: &str, source: &'static str, text: impl Into<String>) {
        self.add_as(app, source, Stream::Out, text);
    }

    /// [`Logs::add`] of a line of `stream`: a build's output, as it came.
    pub fn add_as(&self, app: &str, source: &'static str, stream: Stream, text: impl Into<String>) {
        let text = text.into();
        println!("[{app}] {source}: {text}");
        self.push(app, source, 0, stream, text);
    }

    /// Adds a line of the app's own, which its wicket logs already.
    pub fn push(
        &self,
        app: &str,
        source: &'static str,
        instance: u64,
        stream: Stream,
        text: String,
    ) {
        let mut rings = self.rings.lock().expect("no panic under the lock");
        let ring = &mut rings.entry(app.to_string()).or_default().lines;
        let timestamp = ring
            .back()
            .map_or(0, |last| last.timestamp + 1)
            .max(epoch_nanos());
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

    /// Waits, `within` at most, for a reader following `app`'s lines to walk past the last of
    /// them, and a look more for it to print them: the CLI walks the staging log 2s behind and
    /// stops at the staging's end, which would cut its last lines. No reader following
    /// ([`FOLLOWING`] for one to start: the CLI starts as the staging does): no wait more.
    pub async fn read_out(&self, app: &str, within: Duration) {
        let started = tokio::time::Instant::now();
        let deadline = started + within;
        loop {
            // whether a reader follows, and whether it is past the last line
            let following = {
                let rings = self.rings.lock().expect("no panic under the lock");
                rings.get(app).and_then(|ring| {
                    let last = ring.lines.back().map_or(0, |line| line.timestamp);
                    let (start, at) = ring.walked?;
                    (at.elapsed() < FOLLOWING).then_some(start > last)
                })
            };
            match following {
                None if started.elapsed() >= FOLLOWING => return,
                Some(true) => break,
                Some(false) if tokio::time::Instant::now() >= deadline => return,
                None | Some(false) => tokio::time::sleep(LOOK).await,
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
        logs.push("c", "APP/PROC/WEB", 2, Stream::Err, "x".into());
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
    }
}
