//! A wicket leaving: told to stop with an evacuation bound (`--evacuation-grace`), it serves on --
//! its listener, its tunnel streams advertising its routes, its console taking edits -- and lists
//! itself leaving, its revision moved: a controller's long poll answers at once ([`evacuate`]).
//! The controllers of its apps place them on other wickets and remove them from it; an owner that
//! moved what it could says so ([`evacuated`]), what is left of its apps then staying until the
//! drain. The wicket drains once no app it waits for is left -- the apps of its app API but those
//! no other wicket can serve (a Firecracker wicket's app with a disk), and those of the owners
//! that said so -- or the bound passed.
//!
//! ```text
//! PUT    /api/evacuated/<owner>   the owner moved what of its apps it could: those left are not
//!                                 waited for (409 unless the wicket is leaving)
//! ```

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::StatusCode;
use http::request::Parts;
use serde::Serialize;
use tokio::sync::watch;

use crate::fence::Fence;
use crate::http::{Response, error, json};
use crate::status::Status;
use crate::store::check_name;

/// How often [`evacuate`] looks at the apps besides at each change of the status.
const LOOK: Duration = Duration::from_secs(1);

/// How long [`evacuate`] waits, the last app gone, before it is over: the answer to the edit
/// that removed it goes out first (in local mode the process ends next).
const SETTLE: Duration = Duration::from_millis(250);

/// The apps a log line names at most.
const NAMED: usize = 8;

/// A wicket leaving, as its status has it.
#[derive(Clone, Debug, PartialEq)]
pub struct Leaving {
    pub since: SystemTime,
    /// When it drains at the latest.
    pub until: SystemTime,
    /// The owners that moved what of their apps they could.
    pub evacuated: BTreeSet<String>,
}

/// A wicket leaving, as the listings say it.
#[derive(Serialize, Debug, PartialEq)]
pub struct LeavingView {
    /// Since when, in seconds since the epoch.
    pub since: u64,
    /// When it drains at the latest, in seconds since the epoch.
    pub until: u64,
    /// The owners that moved what of their apps they could.
    pub evacuated: Vec<String>,
}

impl Leaving {
    pub fn view(&self) -> LeavingView {
        let secs = |at: SystemTime| at.duration_since(UNIX_EPOCH).map_or(0, |at| at.as_secs());
        LeavingView {
            since: secs(self.since),
            until: secs(self.until),
            evacuated: self.evacuated.iter().cloned().collect(),
        }
    }
}

/// An app the wicket waits for while it leaves: its owner and its name.
pub type Waited = (String, String);

/// Lists the wicket leaving (`status`'s revision moved) and waits until no app of `waited` is left
/// but those of the owners that moved what they could, or `grace` passed: the apps left then,
/// none once evacuated, logged. `waited` tells the apps of the app API a controller may place
/// elsewhere, as the routes are now: asked again at each change of the status (a table published
/// moves it) and every second.
pub async fn evacuate(
    status: &watch::Sender<Status>,
    grace: Duration,
    waited: impl Fn() -> Vec<Waited>,
) -> Vec<Waited> {
    let since = SystemTime::now();
    status.send_modify(|status| {
        status.revision += 1;
        status.leaving = Some(Leaving {
            since,
            until: since + grace,
            evacuated: BTreeSet::new(),
        });
    });
    let deadline = tokio::time::Instant::now() + grace;
    let mut changes = status.subscribe();
    let left = |status: &Status| {
        let evacuated = status.leaving.as_ref().map(|leaving| &leaving.evacuated);
        let mut left = waited();
        left.retain(|(owner, _)| evacuated.is_none_or(|evacuated| !evacuated.contains(owner)));
        left.sort();
        left.dedup();
        left
    };
    let first = left(&changes.borrow_and_update());
    if first.is_empty() {
        println!("leaving: no app of the app API to wait for");
        return first;
    }
    println!(
        "leaving: serving on while the controllers move {} app(s) elsewhere, {}s at most \
         (--evacuation-grace): {}",
        first.len(),
        grace.as_secs(),
        named(&first)
    );
    let mut now = first;
    let mut gone = false;
    while !now.is_empty() || !gone {
        // the last one gone: once more after the answer to its removal
        let look = match now.is_empty() {
            true => SETTLE,
            false => LOOK,
        };
        gone = now.is_empty();
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => {
                if now.is_empty() {
                    break;
                }
                eprintln!(
                    "evacuation grace {}s expired with {} app(s) left: {}",
                    grace.as_secs(),
                    now.len(),
                    named(&now)
                );
                return now;
            }
            _ = changes.changed(), if !gone => {}
            () = tokio::time::sleep(look) => {}
        }
        now = left(&changes.borrow_and_update());
    }
    println!("evacuated: no app left to wait for");
    now
}

/// `PUT /api/evacuated/<owner>`: the owner moved what of its apps it could while the wicket
/// leaves, the rest of them staying until the drain, unless `fence` refuses its epoch. Refused
/// (409) unless the wicket is leaving: a wicket started again waits for no one.
pub async fn evacuated(
    status: &watch::Sender<Status>,
    fence: &Fence,
    owner: &str,
    parts: &Parts,
) -> Response {
    if let Err(e) = check_name("owner", owner) {
        return error(StatusCode::BAD_REQUEST, e);
    }
    let _admitted = match fence.admit(Some(owner), parts).await {
        Ok(admitted) => admitted,
        Err(refused) => return refused.into(),
    };
    let mut view = None;
    let added = status.send_if_modified(|status| {
        let Some(leaving) = &mut status.leaving else {
            return false;
        };
        let added = leaving.evacuated.insert(owner.to_string());
        view = Some(leaving.view());
        if added {
            status.revision += 1;
        }
        added
    });
    if added {
        println!(
            "leaving: {owner} moved what of its apps it could, the rest stays until the drain"
        );
    }
    match view {
        Some(view) => json(StatusCode::OK, &view),
        None => error(
            StatusCode::CONFLICT,
            "the wicket is not leaving: nothing to evacuate",
        ),
    }
}

/// `apps` as a log line names them: `owner/name`, the first [`NAMED`].
fn named(apps: &[Waited]) -> String {
    let mut named: Vec<String> = (apps.iter().take(NAMED))
        .map(|(owner, name)| format!("{owner}/{name}"))
        .collect();
    if apps.len() > NAMED {
        named.push(format!("and {} more", apps.len() - NAMED));
    }
    named.join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Leaving moves the revision and lists the bound; the wicket waits until the apps are gone
    /// but those of an owner that moved what it could, at a change of the status or a look; past
    /// the bound, the apps left are given. An owner's word is refused unless leaving.
    #[tokio::test(start_paused = true)]
    async fn the_wicket_waits_for_its_apps_until_gone_said_so_or_the_bound() {
        let dir = std::env::temp_dir().join(format!("sluice-leaving-{}", std::process::id()));
        let fence = Fence::open(&dir).unwrap();
        let parts = http::Request::new(()).into_parts().0;
        let evacuated = async |status: &watch::Sender<Status>, owner: &str| {
            evacuated(status, &fence, owner, &parts).await
        };
        let status = watch::Sender::new(Status::default());
        let refused = evacuated(&status, "cf").await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        let app = |owner: &str, name: &str| (owner.to_string(), name.to_string());
        let apps = Arc::new(Mutex::new(vec![app("cf", "a"), app("ops", "b")]));
        let listed = apps.clone();
        let leaving = {
            let status = status.clone();
            tokio::spawn(async move {
                let waited = || listed.lock().unwrap().clone();
                evacuate(&status, Duration::from_secs(60), waited).await
            })
        };
        tokio::task::yield_now().await;
        let leaving_view = |status: &watch::Sender<Status>| {
            let status = status.borrow();
            (status.revision, status.leaving.as_ref().map(Leaving::view))
        };
        let (revision, view) = leaving_view(&status);
        let view = view.expect("leaving");
        assert_eq!((revision, view.until - view.since), (1, 60));
        assert!(view.evacuated.is_empty());
        // cf moved what it could: its app stays, not waited for
        assert_eq!(evacuated(&status, "cf").await.status(), StatusCode::OK);
        assert_eq!(leaving_view(&status).0, 2);
        assert_eq!(evacuated(&status, "cf").await.status(), StatusCode::OK);
        assert_eq!(leaving_view(&status).0, 2, "said once");
        assert_eq!(
            evacuated(&status, "c f").await.status(),
            StatusCode::BAD_REQUEST
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!leaving.is_finished(), "ops's app is still there");
        // ops's app gone, as a table published says
        apps.lock().unwrap().pop();
        status.send_modify(|status| status.revision += 1);
        assert_eq!(leaving.await.unwrap(), []);

        // gone without a change of the status: at the next look
        let status = watch::Sender::new(Status::default());
        let apps = Arc::new(Mutex::new(vec![app("cf", "a")]));
        let listed = apps.clone();
        let leaving = {
            let status = status.clone();
            tokio::spawn(async move {
                let waited = || listed.lock().unwrap().clone();
                evacuate(&status, Duration::from_secs(60), waited).await
            })
        };
        tokio::task::yield_now().await;
        apps.lock().unwrap().clear();
        let started = tokio::time::Instant::now();
        assert_eq!(leaving.await.unwrap(), []);
        assert!(started.elapsed() <= LOOK + SETTLE);

        // never gone: the bound gives the apps left
        let status = watch::Sender::new(Status::default());
        let started = tokio::time::Instant::now();
        let left = evacuate(&status, Duration::from_secs(10), || vec![app("cf", "a")]).await;
        assert_eq!(left, [app("cf", "a")]);
        assert_eq!(started.elapsed(), Duration::from_secs(10));
    }
}
