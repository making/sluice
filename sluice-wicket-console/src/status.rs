//! What the page shows of a wicket's reloads besides its routes, and the long
//! poll following them.

use std::time::{Duration, SystemTime};

use serde::Serialize;
use tokio::sync::watch;

use crate::store::rfc3339;

/// How long `GET /api/routes?after=<the current revision>` waits for a change.
pub const LONG_POLL: Duration = Duration::from_secs(25);

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
