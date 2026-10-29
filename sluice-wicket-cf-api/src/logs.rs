//! The log lines `cf logs` reads through log-cache's read API: an in-memory
//! ring per app (the source id is the app's guid) of the cf-api's own lines:
//! staging (`STG`), lifecycle (`API`) and the wicket's outcome (`CELL`).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use base64::Engine as _;
use serde_json::{Value, json};

use crate::store::epoch_nanos;

/// Lines kept per app.
const RING: usize = 1000;

#[derive(Clone, Debug)]
pub struct Line {
    /// Nanoseconds since the epoch, unique and ascending within an app.
    pub timestamp: u128,
    pub source: &'static str,
    pub text: String,
}

#[derive(Default)]
pub struct Logs {
    rings: Mutex<HashMap<String, VecDeque<Line>>>,
}

impl Logs {
    pub fn add(&self, app: &str, source: &'static str, text: impl Into<String>) {
        let text = text.into();
        println!("[{app}] {source}: {text}");
        let mut rings = self.rings.lock().expect("no panic under the lock");
        let ring = rings.entry(app.to_string()).or_default();
        let timestamp = ring
            .back()
            .map_or(0, |last| last.timestamp + 1)
            .max(epoch_nanos());
        ring.push_back(Line {
            timestamp,
            source,
            text,
        });
        while ring.len() > RING {
            ring.pop_front();
        }
    }

    /// The lines of `app` from `start` (inclusive) to `end` (exclusive), at
    /// most `limit`; the newest first with `descending`, and then the newest
    /// `limit` of them.
    pub fn read(
        &self,
        app: &str,
        start: u128,
        end: Option<u128>,
        limit: usize,
        descending: bool,
    ) -> Vec<Line> {
        let rings = self.rings.lock().expect("no panic under the lock");
        let lines =
            rings.get(app).into_iter().flatten().filter(|line| {
                line.timestamp >= start && end.is_none_or(|end| line.timestamp < end)
            });
        if descending {
            lines.rev().take(limit).cloned().collect()
        } else {
            lines.take(limit).cloned().collect()
        }
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
                "instance_id": "0",
                "deprecated_tags": {},
                "tags": {"source_type": line.source},
                "log": {
                    "payload": base64::engine::general_purpose::STANDARD.encode(&line.text),
                    "type": "OUT",
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
        assert_eq!(view["envelopes"]["batch"][0]["log"]["payload"], "bGluZSA0");
    }
}
