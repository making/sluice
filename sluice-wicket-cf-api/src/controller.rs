//! The wicket serves what the state wants: an app started with a droplet and
//! a route is an app of the wicket, named by its guid (its routes' hosts,
//! the snapshot's component and sandbox -- its env, which the wicket lists
//! by the names only, told by the snapshot's tag -- and the process's
//! instances as the pool); stopped, or without a route, it is none. The
//! desired state is the cf-api's: at startup and whenever the wicket's
//! revision moves, its apps of the owner are listed and the difference put /
//! deleted. The wicket serves on while the cf-api is down.
//!
//! The data of a deleted keyvalue instance is deleted once no app maps its
//! namespace, the wicket's draining routes included: tried at the deletion
//! and again after each listing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::logs::Logs;
use crate::services::Offer;
use crate::store::{App, Outcome, Purge, State, Store};
use crate::wicket::{AppBody, ComponentRef, Failure, ListedApp, Sandbox, Wicket};

pub struct Controller {
    pub store: Arc<Store>,
    pub wicket: Arc<Wicket>,
    pub logs: Arc<Logs>,
    /// One edit at a time: a trial against the wicket and its commit are
    /// not interleaved with another's.
    pub edits: tokio::sync::Mutex<()>,
    /// What the wicket offers, as it last listed it.
    pub offer: Mutex<Option<Offer>>,
}

impl Controller {
    /// The wicket's app of `app` as `state` wants it; None when the app is
    /// stopped, was never started with a droplet, or has no route.
    pub fn desired(state: &State, app: &App, owner: &str) -> Option<AppBody> {
        let running = app.running.as_ref().filter(|_| app.started)?;
        let mut hosts: Vec<String> = state
            .routes_of(&app.guid)
            .map(|route| state.url(route))
            .collect();
        if hosts.is_empty() {
            return None;
        }
        hosts.sort();
        Some(AppBody {
            owner: owner.to_string(),
            hosts,
            component: ComponentRef {
                sha256: running.sha256.clone(),
                name: running.file.clone(),
            },
            sandbox: Sandbox {
                wasi: running.wasi.clone(),
                budget_ms: running.budget_ms,
                memory_mib: Some(running.memory_mib),
                pool: app.process.instances,
                instances: running.instances,
                keyvalue: running.keyvalue.clone(),
                env: running.env.clone(),
            },
            tag: running.tag.clone(),
        })
    }

    /// What the wicket offers: as last listed, else listed now.
    pub async fn offer(&self) -> Result<Offer, Failure> {
        if let Some(offer) = self.offer.lock().expect("no panic under the lock").clone() {
            return Ok(offer);
        }
        let listing = self.wicket.list(None).await?;
        Ok(self.note_offer(&listing.capabilities, &listing.keyvalue_stores))
    }

    fn note_offer(&self, capabilities: &[String], stores: &[crate::wicket::ListedStore]) -> Offer {
        let offer = Offer::new(capabilities, stores);
        *self.offer.lock().expect("no panic under the lock") = Some(offer.clone());
        offer
    }

    /// Deletes the data of the deleted instances no started app's snapshot
    /// maps any more, as far as the wicket lets it: one a route still maps
    /// (409) waits, as does an unreachable wicket; one it cannot delete (a
    /// store gone: 404) is dropped. Not under the edit lock: the caller
    /// holds it.
    pub async fn purge_pending(&self) {
        let due: Vec<Purge> = self.store.read(|state| {
            state
                .purges
                .iter()
                .filter(|purge| {
                    let place = format!("{}/{}", purge.store, purge.namespace);
                    !state.apps.iter().any(|app| {
                        app.started
                            && app.running.as_ref().is_some_and(|running| {
                                running.keyvalue.values().any(|kept| *kept == place)
                            })
                    })
                })
                .cloned()
                .collect()
        });
        for purge in due {
            let named = format!(
                "the data of service instance {} ({}/{})",
                purge.instance, purge.store, purge.namespace
            );
            match self.wicket.purge(&purge.store, &purge.namespace).await {
                Ok(keys) => println!("SERVICES: deleted {named}: {keys} key(s)"),
                Err(Failure::Refused {
                    status: status @ (400 | 404),
                    error,
                    ..
                }) => {
                    println!("SERVICES: dropped {named}: the wicket refused it ({status}): {error}")
                }
                Err(_) => continue,
            }
            self.store
                .update(|state| state.purges.retain(|kept| *kept != purge));
        }
    }

    /// The desired app of `guid` in the state now.
    pub fn desired_of(&self, state: &State, guid: &str) -> Option<AppBody> {
        state
            .app(guid)
            .and_then(|app| Controller::desired(state, app, &self.wicket.owner))
    }

    /// Puts the app `guid` as `desired` (or removes it) and records the
    /// outcome: what the process stats and the log show.
    pub async fn apply(&self, guid: &str, desired: Option<&AppBody>) -> Result<(), Failure> {
        let Some(app) = desired else {
            return match self.wicket.delete(guid).await {
                Ok(edited) => {
                    let was = self.store.note(|state| state.wicket.remove(guid));
                    if let Some(edited) = edited.filter(|_| was.is_some()) {
                        self.logs.add(
                            guid,
                            "CELL",
                            format!("Removed from the wicket: {}", edited.message),
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    self.logs
                        .add(guid, "CELL", format!("Not removed from the wicket: {e}"));
                    Err(e)
                }
            };
        };
        let mut put = self.wicket.put(guid, app).await;
        if let Err(Failure::Refused { missing, .. }) = &put
            && !missing.is_empty()
        {
            // the droplet is not uploaded (or no more): upload it and put again
            for sha256 in missing {
                let path = self.store.droplet_path(sha256);
                let component = match std::fs::read(&path) {
                    Ok(component) => component,
                    Err(e) => {
                        put = Err(Failure::Refused {
                            status: 409,
                            error: format!(
                                "the droplet {sha256} is gone from {}: {e}",
                                path.display()
                            ),
                            host: None,
                            missing: Vec::new(),
                        });
                        break;
                    }
                };
                if let Err(e) = self.wicket.upload(sha256, component).await {
                    put = Err(e);
                    break;
                }
            }
            if put.as_ref().is_err_and(
                |e| matches!(e, Failure::Refused { missing, .. } if !missing.is_empty()),
            ) {
                put = self.wicket.put(guid, app).await;
            }
        }
        let (outcome, line) = match &put {
            Ok(edited) => (
                Outcome::Running,
                format!(
                    "Deployed {} (sha256 {}) to the wicket as {}: {}",
                    app.component.name,
                    &app.component.sha256[..12],
                    app.hosts.join(", "),
                    edited.message
                ),
            ),
            Err(Failure::Refused { error, .. }) => (
                Outcome::Refused(error.clone()),
                format!("The wicket refused the app: {error}"),
            ),
            Err(Failure::Unreachable(e)) => (
                Outcome::Unreachable,
                format!("The wicket is unreachable, the app is put when it is back: {e}"),
            ),
        };
        self.record(guid, outcome, &line);
        put.map(|_| ())
    }

    /// Records the outcome, logging it when it changed.
    fn record(&self, guid: &str, outcome: Outcome, line: &str) {
        let changed = self.store.note(|state| {
            if state.app(guid).is_none() {
                return false;
            }
            state.wicket.insert(guid.to_string(), outcome.clone()) != Some(outcome)
        });
        if changed {
            self.logs.add(guid, "CELL", line);
        }
    }

    /// The desired app of `guid` now, applied.
    pub async fn sync(&self, guid: &str) -> Result<(), Failure> {
        let desired = self.store.read(|state| self.desired_of(state, guid));
        self.apply(guid, desired.as_ref()).await
    }

    /// Puts / deletes the difference between the apps the wicket listed and
    /// the state. Not under the edit lock: the caller holds it.
    async fn fix(&self, listed: &[ListedApp]) {
        let owner = &self.wicket.owner;
        let wanted: Vec<(String, Option<AppBody>)> = self.store.read(|state| {
            state
                .apps
                .iter()
                .map(|app| (app.guid.clone(), Controller::desired(state, app, owner)))
                .collect()
        });
        for (guid, desired) in &wanted {
            let listed = listed
                .iter()
                .find(|app| app.name == *guid && app.app.owner == *owner);
            match (desired, listed) {
                (Some(desired), Some(listed)) if same(&listed.app, desired) => {
                    self.record(guid, Outcome::Running, "Found in the wicket as wanted");
                }
                (Some(_), _) | (None, Some(_)) => {
                    let _ = self.apply(guid, desired.as_ref()).await;
                }
                (None, None) => {}
            }
        }
        for app in listed {
            if app.app.owner == *owner && !wanted.iter().any(|(guid, _)| *guid == app.name) {
                match self.wicket.delete(&app.name).await {
                    Ok(_) => println!("[{}] CELL: removed from the wicket: no such app", app.name),
                    Err(e) => println!("[{}] CELL: not removed from the wicket: {e}", app.name),
                }
            }
        }
    }

    /// Lists the wicket's apps and fixes the difference at startup and
    /// whenever the wicket's revision moves, then tries the pending purges
    /// (after a hold too: a drained route's end moves no revision); while
    /// the wicket is unreachable, every started app is `STARTING`, and the
    /// wicket is tried again with a growing pause.
    pub async fn watch(self: Arc<Self>) {
        let mut after: Option<u64> = None;
        let mut pause = Duration::from_secs(1);
        loop {
            let listing = match self.wicket.list(after).await {
                Ok(listing) => listing,
                Err(e) => {
                    self.unreachable(&e);
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_secs(30));
                    continue;
                }
            };
            pause = Duration::from_secs(1);
            self.note_offer(&listing.capabilities, &listing.keyvalue_stores);
            let moved = after != Some(listing.revision);
            after = Some(listing.revision);
            let pending = self.store.read(|state| !state.purges.is_empty());
            if !moved && !pending {
                continue;
            }
            let _edit = self.edits.lock().await;
            if moved {
                self.fix(&listing.apps).await;
            }
            self.purge_pending().await;
        }
    }

    /// Every app the wicket should serve is `STARTING` until it answers.
    fn unreachable(&self, e: &Failure) {
        let guids: Vec<String> = self.store.read(|state| {
            state
                .apps
                .iter()
                .filter(|app| Controller::desired(state, app, &self.wicket.owner).is_some())
                .map(|app| app.guid.clone())
                .collect()
        });
        for guid in guids {
            self.record(
                &guid,
                Outcome::Unreachable,
                &format!("The wicket is unreachable, the app is put when it is back: {e}"),
            );
        }
    }
}

/// Whether the wicket serves `desired` as listed: a `budget_ms` left to
/// the wicket's default matches whatever it chose; the capabilities in any
/// order; the env, which the wicket lists by its names, by the tag.
fn same(listed: &AppBody, desired: &AppBody) -> bool {
    let sorted = |list: &[String]| {
        let mut list = list.to_vec();
        list.sort();
        list
    };
    sorted(&listed.hosts) == desired.hosts
        && listed.component == desired.component
        && sorted(&listed.sandbox.wasi) == sorted(&desired.sandbox.wasi)
        && listed.sandbox.pool == desired.sandbox.pool
        && listed.sandbox.instances == desired.sandbox.instances
        && listed.sandbox.memory_mib == desired.sandbox.memory_mib
        && desired
            .sandbox
            .budget_ms
            .is_none_or(|budget| listed.sandbox.budget_ms == Some(budget))
        && listed.sandbox.keyvalue == desired.sandbox.keyvalue
        && listed.tag == desired.tag
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Running;

    #[test]
    fn an_app_is_desired_started_with_a_droplet_and_a_route() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let guid = state.create_app("hello").guid.clone();
        let domain = state.domains[0].guid.clone();
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(Controller::desired(&state, &app, "cf"), None);
        let running = Running {
            droplet: "d".into(),
            sha256: "ab".repeat(32),
            file: "hello.wasm".into(),
            memory_mib: 64,
            budget_ms: Some(5000),
            instances: None,
            wasi: vec!["keyvalue".into(), "cli".into()],
            keyvalue: [(String::new(), "local/cf/i".to_string())].into(),
            env: [("GREETING".to_string(), "hello".to_string())].into(),
            tag: Some("t1".into()),
            since: 0,
        };
        state.app_mut(&guid).unwrap().running = Some(running);
        state.app_mut(&guid).unwrap().started = true;
        state.app_mut(&guid).unwrap().process.instances = 2;
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(Controller::desired(&state, &app, "cf"), None, "no route");
        let route = state.create_route("hello", &domain).guid.clone();
        state.map(&route, &guid);
        let desired = Controller::desired(&state, state.app(&guid).unwrap(), "cf").unwrap();
        assert_eq!(desired.hosts, ["hello.localhost"]);
        assert_eq!(desired.sandbox.pool, 2);
        assert_eq!(desired.sandbox.memory_mib, Some(64));
        assert_eq!(desired.sandbox.keyvalue[""], "local/cf/i");
        // the env put, listed by its names: told by the tag
        let mut body = serde_json::to_value(&desired).unwrap();
        assert_eq!(
            (&body["sandbox"]["env"]["GREETING"], &body["tag"]),
            (&"hello".into(), &"t1".into())
        );
        body["sandbox"]["env"] = serde_json::json!(["GREETING"]);
        let mut listed: AppBody = serde_json::from_value(body).unwrap();
        assert!(same(&listed, &desired));
        listed.tag = Some("t0".into());
        assert!(!same(&listed, &desired), "another snapshot");
        listed.tag = desired.tag.clone();
        listed.sandbox.budget_ms = Some(5000);
        // the wicket lists the capabilities in its order
        listed.sandbox.wasi = vec!["cli".into(), "keyvalue".into()];
        assert!(same(&listed, &desired));
        listed.sandbox.pool = 3;
        assert!(!same(&listed, &desired));
        let mut unset = desired.clone();
        unset.sandbox.budget_ms = None;
        listed.sandbox.pool = 2;
        listed.sandbox.budget_ms = Some(10_000);
        assert!(same(&listed, &unset));
        state.app_mut(&guid).unwrap().started = false;
        assert_eq!(
            Controller::desired(&state, state.app(&guid).unwrap(), "cf"),
            None
        );
    }
}
