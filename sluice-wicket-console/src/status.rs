//! What the page shows of a wicket's reloads besides its routes, the long poll
//! following them, and what a listing says of an app's instances.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::watch;

use crate::store::rfc3339;

/// How long `GET /api/routes?after=<the current revision>` waits for a change.
pub const LONG_POLL: Duration = Duration::from_secs(25);

/// An instance alive, as an app's listing names it.
#[derive(Serialize, Debug, PartialEq)]
pub struct Alive {
    /// As the live access log names it.
    pub index: usize,
    /// Its start, in seconds since the epoch.
    pub since: u64,
}

/// `held`, the instances alive and their starts, lowest index first.
pub fn alive(held: BTreeMap<usize, SystemTime>) -> Vec<Alive> {
    held.into_iter()
        .map(|(index, start)| Alive {
            index,
            since: epoch_secs(start),
        })
        .collect()
}

fn epoch_secs(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// An instance kept that is down, as an app's listing names it.
#[derive(Serialize, Debug, PartialEq)]
pub struct Crashed {
    /// As the live access log names it.
    pub index: usize,
    #[serde(flatten)]
    pub shortage: Shortage,
}

/// `down`, the instances kept that are down and why, lowest index first.
pub fn crashed(down: BTreeMap<usize, Shortage>) -> Vec<Crashed> {
    down.into_iter()
        .map(|(index, shortage)| Crashed { index, shortage })
        .collect()
}

/// Why what is kept of an app is short of an instance, as a listing tells it.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Shortage {
    pub why: String,
    /// Since when it goes on, in seconds since the epoch.
    pub since: u64,
}

impl Shortage {
    /// `shortage` from now on says `why`; since its start, when it goes on.
    pub fn keep(shortage: &mut Option<Shortage>, why: impl Into<String>) {
        let since = shortage
            .as_ref()
            .map_or_else(|| epoch_secs(SystemTime::now()), |going_on| going_on.since);
        *shortage = Some(Shortage {
            why: why.into(),
            since,
        });
    }
}

/// What the console shows of the reloads besides the table.
#[derive(Clone, Default)]
pub struct Status {
    /// Changes with every change the console shows: a table published, a
    /// reload of the files failing, or the next reload succeeding.
    pub revision: u64,
    /// The last reload of the files that failed, unless one succeeded since.
    pub failure: Option<Failure>,
}

/// A failed reload of the files.
#[derive(Clone)]
pub struct Failure {
    /// What started it: `file change` or `SIGHUP`.
    pub reason: String,
    pub error: String,
    pub at: SystemTime,
}

impl Status {
    /// A reload of the files failed for `reason`.
    pub fn failed(sender: &watch::Sender<Status>, reason: &str, error: String) {
        sender.send_modify(|status| {
            status.revision += 1;
            status.failure = Some(Failure {
                reason: reason.to_string(),
                error,
                at: SystemTime::now(),
            });
        });
    }

    /// A reload read every route: a failure shown before is over.
    pub fn recovered(sender: &watch::Sender<Status>) {
        sender.send_if_modified(|status| {
            if status.failure.is_none() {
                return false;
            }
            status.revision += 1;
            status.failure = None;
            true
        });
    }

    /// The failure as the listing shows it.
    pub fn failure_view(&self) -> Option<FailureView> {
        self.failure.as_ref().map(|failure| FailureView {
            reason: failure.reason.clone(),
            error: failure.error.clone(),
            at: rfc3339(failure.at),
        })
    }
}

#[derive(Serialize)]
pub struct FailureView {
    pub reason: String,
    pub error: String,
    pub at: String,
}

/// The status once the routes moved past `after` of `query` (the current
/// revision) or [`LONG_POLL`] passed; at once without `after`.
pub async fn settled(status: &watch::Receiver<Status>, query: Option<&str>) -> Status {
    let after = form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find(|(key, _)| key == "after")
        .and_then(|(_, value)| value.parse::<u64>().ok());
    let mut status = status.clone();
    if let Some(after) = after {
        let changed = status.wait_for(|status| status.revision != after);
        let ended = matches!(tokio::time::timeout(LONG_POLL, changed).await, Ok(Err(_)));
        if ended {
            // the reloader ended (the drain): nothing changes from now on
            tokio::time::sleep(LONG_POLL).await;
        }
    }
    status.borrow().clone()
}
