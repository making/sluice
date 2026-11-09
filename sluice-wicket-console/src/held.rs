//! The apps of the app API a wicket holds at its start: those put with `held-ms`, a controller's,
//! which it may have moved, changed or removed while the wicket was away (killed, its host down).
//! Held, an app is neither advertised nor served on `--listen` until its owner says it is as it
//! wants it ([`confirmed`]), puts it again or removes it, or its `held-ms` passed since the start:
//! a controller that is down keeps none of them from serving longer.
//!
//! ```text
//! PUT    /api/confirmed/<owner>   the apps of the owner the body names ({"apps": [<name>, ...]})
//!                                 are as it wants them: served
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use http::request::Parts;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::http::{Body, Response, error, json, read};
use crate::store::check_name;

/// The body of a confirmation at most: a name is 100 bytes at most.
const MAX_CONFIRMED_BYTES: usize = 1 << 20;

/// The longest `held-ms`: an hour.
pub const MAX_HELD_MS: u64 = 60 * 60 * 1000;

/// The apps a log line names at most.
const NAMED: usize = 8;

/// The apps held, by name.
pub struct Held {
    /// Their owner, and until when at the latest.
    apps: watch::Sender<BTreeMap<String, (String, Instant)>>,
}

impl Held {
    /// Holds each of `apps`, an app's owner, name and `held-ms` ([`MAX_HELD_MS`] at most), from
    /// now on: none of 0. Logged.
    pub fn new(apps: impl IntoIterator<Item = (String, String, Duration)>) -> Arc<Held> {
        let now = Instant::now();
        let longest = Duration::from_millis(MAX_HELD_MS);
        let apps: BTreeMap<String, (String, Instant)> = apps
            .into_iter()
            .filter(|(_, _, bound)| !bound.is_zero())
            .map(|(owner, name, bound)| (name, (owner, now + bound.min(longest))))
            .collect();
        if !apps.is_empty() {
            let longest = apps.values().map(|(_, until)| *until - now).max();
            println!(
                "held: {} app(s) until their owners confirm them, {}s at most: {}",
                apps.len(),
                longest.unwrap_or_default().as_secs_f64().ceil(),
                named(
                    apps.iter()
                        .map(|(name, (owner, _))| (owner.as_str(), name.as_str()))
                )
            );
        }
        Arc::new(Held {
            apps: watch::Sender::new(apps),
        })
    }

    /// Whether the app `name` is held now.
    pub fn holds(&self, name: &str) -> bool {
        let apps = self.apps.borrow();
        apps.get(name)
            .is_some_and(|(_, until)| Instant::now() < *until)
    }

    /// The app `name` is held no more: its owner put it again or removed it.
    pub fn release(&self, name: &str) {
        self.apps
            .send_if_modified(|apps| apps.remove(name).is_some());
    }

    /// The apps of `owner` held now, by name.
    pub fn of(&self, owner: &str) -> Vec<String> {
        let now = Instant::now();
        let apps = self.apps.borrow();
        (apps.iter())
            .filter(|(_, (of, until))| of == owner && now < *until)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// The apps of `owner` among `names` are held no more: those that were.
    fn confirm(&self, owner: &str, names: &[String]) -> Vec<String> {
        let mut released = Vec::new();
        self.apps.send_if_modified(|apps| {
            for name in names {
                if apps.get(name).is_some_and(|(of, _)| of == owner) {
                    apps.remove(name);
                    released.push(name.clone());
                }
            }
            !released.is_empty()
        });
        released
    }

    /// Calls `changed` at each release, and as each hold passes (logged), until none is left.
    pub async fn follow(self: Arc<Self>, changed: impl Fn()) {
        let mut seen = self.apps.subscribe();
        loop {
            let next = (seen.borrow_and_update().values())
                .map(|(_, until)| *until)
                .min();
            let Some(next) = next else {
                return;
            };
            tokio::select! {
                // the sender is this one's: never closed
                _ = seen.changed() => {}
                () = tokio::time::sleep_until(next) => {
                    let now = Instant::now();
                    let mut passed = Vec::new();
                    self.apps.send_if_modified(|apps| {
                        apps.retain(|name, (owner, until)| {
                            let held = now < *until;
                            if !held {
                                passed.push((owner.clone(), name.clone()));
                            }
                            held
                        });
                        !passed.is_empty()
                    });
                    if !passed.is_empty() {
                        println!(
                            "held: served unconfirmed, their held-ms passed: {}",
                            named(passed.iter().map(|(owner, name)| (owner.as_str(), name.as_str())))
                        );
                    }
                }
            }
            changed();
        }
    }
}

/// An app's `held-ms`, as a PUT or the console's directory has it: from 1 to [`MAX_HELD_MS`].
pub fn check_held_ms(held_ms: u64) -> Result<(), String> {
    if held_ms == 0 {
        return Err("held-ms: must be at least 1 (an app put without it is not held)".into());
    }
    if held_ms > MAX_HELD_MS {
        return Err(format!("held-ms: {MAX_HELD_MS} at most, not {held_ms}"));
    }
    Ok(())
}

/// The body of a confirmation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmedBody {
    apps: Vec<String>,
}

/// The answer to a confirmation.
#[derive(Serialize, Debug, PartialEq)]
struct ConfirmedView {
    /// The apps it released.
    released: Vec<String>,
    /// The apps of its owner held still.
    held: Vec<String>,
}

/// `PUT /api/confirmed/<owner>`: the apps of `owner` the body names are as it wants them, those
/// held served from now on. Answers those it released and those of `owner` held still.
pub async fn confirmed(held: &Held, owner: &str, parts: &Parts, body: Body) -> Response {
    let too_large = format!(
        "a confirmation is {} KiB at most",
        MAX_CONFIRMED_BYTES >> 10
    );
    match read(parts, body, MAX_CONFIRMED_BYTES, &too_large).await {
        Ok(body) => confirmation(held, owner, &body),
        Err(refused) => refused.into(),
    }
}

/// [`confirmed`] of the body read.
fn confirmation(held: &Held, owner: &str, body: &[u8]) -> Response {
    if let Err(e) = check_name("owner", owner) {
        return error(StatusCode::BAD_REQUEST, e);
    }
    let confirmed = match serde_json::from_slice::<ConfirmedBody>(body) {
        Ok(confirmed) => confirmed,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("the confirmation: {e}")),
    };
    let released = held.confirm(owner, &confirmed.apps);
    if !released.is_empty() {
        println!(
            "held: {owner} confirmed {}: served",
            named(released.iter().map(|name| (owner, name.as_str())))
        );
    }
    let held = held.of(owner);
    json(StatusCode::OK, &ConfirmedView { released, held })
}

/// The apps of `owner/name` as a log line names them: the first [`NAMED`].
fn named<'a>(apps: impl ExactSizeIterator<Item = (&'a str, &'a str)>) -> String {
    let count = apps.len();
    let mut named: Vec<String> = (apps.take(NAMED))
        .map(|(owner, name)| format!("{owner}/{name}"))
        .collect();
    if count > NAMED {
        named.push(format!("and {} more", count - NAMED));
    }
    named.join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// `owner`'s confirmation of `body`: its status and its JSON.
    async fn confirm(held: &Held, owner: &str, body: &str) -> (StatusCode, serde_json::Value) {
        use http_body_util::BodyExt as _;
        let answer = confirmation(held, owner, body.as_bytes());
        let status = answer.status();
        let body = answer.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// An app is held from the start until its owner's word, its release (a put, a removal) or
    /// its bound; another owner's word changes nothing, and each change is followed.
    #[tokio::test(start_paused = true)]
    async fn an_app_is_held_until_its_owner_confirms_it_or_its_bound() {
        let app = |owner: &str, name: &str, secs| (owner.to_string(), name.to_string(), secs);
        let held = Held::new([
            app("cf", "a", Duration::from_secs(15)),
            app("cf", "b", Duration::from_secs(15)),
            app("cf", "c", Duration::from_secs(5)),
            app("ops", "d", Duration::from_secs(15)),
            app("cf", "unheld", Duration::ZERO),
        ]);
        assert!(["a", "b", "c", "d"].iter().all(|name| held.holds(name)));
        assert!(!held.holds("unheld"));
        let changes = Arc::new(AtomicUsize::new(0));
        let followed = {
            let (held, changes) = (held.clone(), changes.clone());
            tokio::spawn(held.follow(move || {
                changes.fetch_add(1, Ordering::Relaxed);
            }))
        };
        tokio::task::yield_now().await;

        // another owner's word, a name held of none: nothing released
        let (status, answer) = confirm(&held, "ops", r#"{"apps": ["a", "x"]}"#).await;
        assert_eq!(
            (status, answer),
            (
                StatusCode::OK,
                serde_json::json!({"released": [], "held": ["d"]})
            )
        );
        assert!(held.holds("a"));
        let (status, _) = confirm(&held, "c f", r#"{"apps": []}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = confirm(&held, "cf", r#"{"names": []}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // its owner's: served, the change followed
        let (status, answer) = confirm(&held, "cf", r#"{"apps": ["a", "d"]}"#).await;
        assert_eq!(
            (status, answer),
            (
                StatusCode::OK,
                serde_json::json!({"released": ["a"], "held": ["b", "c"]})
            )
        );
        assert!(!held.holds("a") && held.holds("d"));
        tokio::task::yield_now().await;
        assert_eq!(changes.load(Ordering::Relaxed), 1);
        // put again: released
        held.release("b");
        tokio::task::yield_now().await;
        assert_eq!(changes.load(Ordering::Relaxed), 2);
        // the bounds pass: c's, then d's, the last
        tokio::time::sleep(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert!(!held.holds("c") && held.holds("d"));
        assert_eq!(changes.load(Ordering::Relaxed), 3);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!held.holds("d"));
        followed.await.unwrap();
        assert_eq!(changes.load(Ordering::Relaxed), 4);
        assert!(held.of("ops").is_empty());

        // none held: nothing followed
        let none = Held::new([app("cf", "a", Duration::ZERO)]);
        none.follow(|| panic!("no change")).await;
    }
}
