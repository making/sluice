//! Deployments (`/v3/deployments`): an app's new snapshot -- the droplet asked, the sandbox taken
//! then -- put a wicket's share at a time while the others serve the one before
//! ([`crate::store::Rolling`]), the app answering all along as each wicket swaps its share in
//! place. A step reaches the next wickets in the order configured, as many as `max_in_flight`
//! instances take, one at least; the next step comes once every share reached serves the new
//! snapshot: its wicket took it (a Firecracker wicket once its VMs started, a wasm wicket once its
//! route loaded) and, with a readiness check, the VMs it keeps are ready. `ACTIVE` meanwhile,
//! `FINALIZED` / `DEPLOYED` at the end, which `cf push --strategy rolling` waits for.
//!
//! A share refused stops it there, `ACTIVE` as CC keeps one whose new instances crash: the
//! wicket's reason is its error and the instances' `details`, the wickets not reached serving the
//! snapshot before, until it is canceled (the CLI cancels it once its start timeout passed) or
//! superseded; the share taken later (its wicket's revision moved, the cf-api restarted), it goes
//! on. Canceled, the app is back on its snapshot and droplet before, every share put at once.
//!
//! A canary pauses once its step's instances serve the new snapshot -- one (the first wicket's
//! share), or each of its steps' percentages of the instances -- until `cf continue-deployment`,
//! then rolls the rest.
//!
//! The next deployment supersedes one under way, the shares it had not reached serving on what
//! they did; so does a start, restart or stop of the app, putting every share at once. A
//! deployment of an app serving nothing (stopped, at 0, without a route) starts it at once, as CC
//! does.
//!
//! The state holds the progress: a restart of the cf-api takes it up ([`Controller::resume`]).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::controller::Controller;
use crate::store::{
    App, Deployment, DeploymentState, Outcome, Rolling, Running, Scaled, State, now,
};
use crate::wicket::{AppBody, Sandbox, Wicket};

/// Between two looks at whether the shares a deployment reached serve it.
const PAUSE: Duration = Duration::from_millis(500);

/// The keys of a deployment's `options`.
const OPTIONS: [&str; 6] = [
    "canary",
    "max_in_flight",
    "web_instances",
    "memory_in_mb",
    "disk_in_mb",
    "log_rate_limit_in_bytes_per_second",
];

/// What `POST /v3/deployments` asks.
#[derive(Clone, Debug, PartialEq)]
pub struct Asked {
    /// `rolling` or `canary`.
    pub strategy: String,
    /// The droplet deployed; none: the current one.
    pub droplet: Option<String>,
    pub max_in_flight: u64,
    /// A canary's steps, each the percentage of the instances on the new droplet.
    pub canary_steps: Vec<u64>,
    /// The web process as the deployment sets it.
    pub scaled: Scaled,
}

impl Asked {
    /// What `body` asks, checked as CC checks it; a revision is refused: there are none.
    pub fn parse(body: &Value) -> Result<Asked, String> {
        if !body["revision"].is_null() {
            return Err("revisions are not supported: deploy a droplet".into());
        }
        let strategy = match &body["strategy"] {
            Value::Null => "rolling".to_string(),
            Value::String(strategy) if ["rolling", "canary"].contains(&strategy.as_str()) => {
                strategy.clone()
            }
            Value::String(other) => {
                return Err(format!("'{other}' is not a supported deployment strategy"));
            }
            other => return Err(format!("'{other}' is not a supported deployment strategy")),
        };
        let droplet = body["droplet"]["guid"].as_str().map(String::from);
        let options = match &body["options"] {
            Value::Null => serde_json::Map::new(),
            Value::Object(options) => options.clone(),
            _ => return Err("options must be an object".into()),
        };
        let unsupported: Vec<&str> = (options.keys())
            .map(String::as_str)
            .filter(|key| !OPTIONS.contains(key))
            .collect();
        if !unsupported.is_empty() {
            return Err(format!(
                "options has unsupported key(s): {}",
                unsupported.join(", ")
            ));
        }
        // an integer of `options` from `least`
        let number = |key: &str, least: i64, what: &str| -> Result<Option<i64>, String> {
            match options.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(value) => (value.as_i64())
                    .filter(|number| *number >= least)
                    .map(Some)
                    .ok_or_else(|| format!("{key} must be an integer {what}")),
            }
        };
        let unsigned = |number: Option<i64>| number.map(|number| number as u64);
        let canary_steps = match options.get("canary") {
            None | Some(Value::Null) => Vec::new(),
            Some(_) if strategy != "canary" => {
                return Err("options.canary are only valid for Canary deployments".into());
            }
            Some(Value::Object(canary)) => steps(canary.get("steps"))?,
            Some(_) => return Err("options.canary must be an object".into()),
        };
        Ok(Asked {
            strategy,
            droplet,
            max_in_flight: unsigned(number("max_in_flight", 1, "greater than 0")?).unwrap_or(1),
            canary_steps,
            scaled: Scaled {
                web_instances: unsigned(number("web_instances", 0, "from 0")?),
                memory_in_mb: unsigned(number("memory_in_mb", 1, "greater than 0")?),
                disk_in_mb: unsigned(number("disk_in_mb", 1, "greater than 0")?),
                log_rate_limit_in_bytes_per_second: number(
                    "log_rate_limit_in_bytes_per_second",
                    -1,
                    "from -1",
                )?,
            },
        })
    }
}

/// A canary's `steps`, checked: each an `instance_weight` from 1 to 100, ascending.
fn steps(steps: Option<&Value>) -> Result<Vec<u64>, String> {
    let Some(steps) = steps.filter(|steps| !steps.is_null()) else {
        return Ok(Vec::new());
    };
    let Some(steps) = (steps.as_array()).filter(|steps| steps.iter().all(Value::is_object)) else {
        return Err("options.canary.steps must be an array of objects".into());
    };
    let weights = (steps.iter())
        .map(|step| {
            let keys = step.as_object().into_iter().flat_map(|step| step.keys());
            let unsupported: Vec<&str> = (keys.map(String::as_str))
                .filter(|key| *key != "instance_weight")
                .collect();
            if !unsupported.is_empty() {
                return Err(format!(
                    "options.canary.steps has unsupported key(s): {}",
                    unsupported.join(", ")
                ));
            }
            (step["instance_weight"].as_u64())
                .filter(|weight| (1..=100).contains(weight))
                .ok_or_else(|| {
                    "options.canary.steps.instance_weight must be an Integer between 1-100 \
                     (inclusive)"
                        .to_string()
                })
        })
        .collect::<Result<Vec<u64>, String>>()?;
    if !weights.is_sorted() {
        return Err(
            "options.canary.steps.instance_weight must be sorted in ascending order".into(),
        );
    }
    Ok(weights)
}

impl Scaled {
    /// Sets the web process of `app` as asked, its instances as many as an app has at most.
    pub fn apply(&self, app: &mut App) -> Result<(), String> {
        let process = &mut app.process;
        if let Some(instances) = self.web_instances {
            crate::manifest::check_instances(instances)?;
            process.instances = instances;
        }
        if let Some(memory) = self.memory_in_mb {
            process.memory_mb = memory;
        }
        if let Some(disk) = self.disk_in_mb {
            process.disk_mb = disk;
        }
        if let Some(rate) = self.log_rate_limit_in_bytes_per_second {
            process.log_rate_limit = rate;
        }
        if !self.is_empty() {
            process.updated_at = now();
        }
        Ok(())
    }
}

/// What the next step of a deployment does ([`State::step`]).
#[derive(Debug, PartialEq)]
pub enum Step {
    /// Nothing: it takes no steps (paused, canceled, superseded, its app gone).
    None,
    /// It finished, or paused: the line saying so.
    Done(String),
    /// It reached the shares of these wickets, to put: the line saying so.
    Reach(Vec<String>, String),
}

impl State {
    /// The snapshot each wicket holding a share of `app` serves, by name: none when it serves
    /// nothing.
    pub fn serving(&self, app: &App) -> BTreeMap<String, Running> {
        let shares = Controller::desired(self, app, crate::OWNER).into_keys();
        shares
            .filter_map(|wicket| {
                let running = app.running_on(&wicket)?.clone();
                Some((wicket, running))
            })
            .collect()
    }

    /// Deploys `running`, the new snapshot of the app `guid` of the droplet asked
    /// (`previous_droplet` the one before), superseding the deployment under way: over the
    /// wickets `serving` the one before, a step at a time; none serving, at once. The
    /// deployment, and the line saying what it superseded.
    pub fn deploy(
        &mut self,
        guid: &str,
        running: Running,
        serving: BTreeMap<String, Running>,
        previous_droplet: String,
        asked: &Asked,
    ) -> (Deployment, Option<String>) {
        let superseded = self.supersede(guid);
        let at = now();
        let canary = asked.strategy == "canary";
        let state = match (serving.is_empty(), canary) {
            (true, _) => DeploymentState::Deployed,
            (false, true) => DeploymentState::Prepaused,
            (false, false) => DeploymentState::Deploying,
        };
        let deployment = Deployment {
            guid: crate::store::guid(),
            app: guid.to_string(),
            droplet: running.droplet.clone(),
            previous_droplet,
            strategy: asked.strategy.clone(),
            created_at: at.clone(),
            updated_at: at.clone(),
            state,
            superseded: false,
            max_in_flight: asked.max_in_flight,
            canary_steps: asked.canary_steps.clone(),
            canary_step: canary.then_some(1),
            scaled: asked.scaled.clone(),
            error: None,
            last_healthy_at: None,
            status_changed_at: Some(at.clone()),
        };
        if let Some(app) = self.app_mut(guid) {
            let previous = app.running.replace(running);
            app.started = true;
            app.updated_at = at;
            // what each wicket serves until reached: the snapshot before, or what a deployment
            // superseded had not replaced
            app.rolling = (previous.filter(|_| !serving.is_empty())).map(|previous| Rolling {
                deployment: deployment.guid.clone(),
                pending: (serving.into_iter())
                    .map(|(wicket, serves)| {
                        let other = (serves != previous).then_some(serves);
                        (wicket, other)
                    })
                    .collect(),
                previous,
            });
        }
        self.deployments.push(deployment.clone());
        // past 1000, the oldest finalized go
        while self.deployments.len() > 1000 {
            match (self.deployments.iter()).position(|kept| !kept.cancelable()) {
                Some(oldest) => self.deployments.remove(oldest),
                None => break,
            };
        }
        (deployment, superseded)
    }

    /// Finalizes the deployment under way of the app `guid` as superseded, if any (as CC does:
    /// deployed if it was deploying, else canceled), the app's shares then all of its snapshot.
    /// The line saying so.
    pub fn supersede(&mut self, guid: &str) -> Option<String> {
        if let Some(app) = self.app_mut(guid) {
            app.rolling = None;
        }
        let deployment = (self.deployments.iter_mut())
            .find(|deployment| deployment.app == guid && deployment.cancelable())?;
        deployment.superseded = true;
        deployment.error = None;
        deployment.set(match deployment.state {
            DeploymentState::Deploying => DeploymentState::Deployed,
            _ => DeploymentState::Canceled,
        });
        Some(format!(
            "Superseded the {} deployment {}",
            deployment.strategy, deployment.guid
        ))
    }

    /// The next step of the deployment `deployment` of the app `guid`, every share it reached
    /// serving it: the next wickets in the order configured holding a share it has not reached
    /// (one put on no more aside), as many as `max_in_flight` instances take, one at least, a
    /// canary's step no more than its target needs. None left, it is deployed; a canary's step
    /// at its target, paused.
    pub fn step(&mut self, guid: &str, deployment: &str) -> Step {
        let found =
            (self.deployments.iter()).position(|kept| kept.guid == deployment && kept.stepping());
        let (Some(at), Some(app)) = (found, self.app(guid)) else {
            return Step::None;
        };
        let name = app.name.clone();
        let sizes = self.share_sizes(app);
        let not_reached: Vec<(String, u64)> = match &app.rolling {
            Some(rolling) if rolling.deployment == deployment => (self.wickets.configured.iter())
                .filter(|wicket| rolling.pending.contains_key(*wicket))
                .filter(|wicket| !self.wickets.odd.contains(*wicket))
                .filter_map(|wicket| Some((wicket.clone(), *sizes.get(wicket)?)))
                .collect(),
            _ => Vec::new(),
        };
        let instances: u64 = sizes.values().sum();
        let reached = instances - not_reached.iter().map(|(_, size)| size).sum::<u64>();
        let kept = &self.deployments[at];
        let limit = match kept.state {
            DeploymentState::Prepaused => {
                let step = kept.canary_step.unwrap_or(1);
                let target = canary_target(&kept.canary_steps, step, instances);
                if reached >= target || not_reached.is_empty() {
                    let total = kept.canary_steps.len().max(1);
                    let kept = &mut self.deployments[at];
                    kept.last_healthy_at = Some(now());
                    kept.error = None;
                    kept.set(DeploymentState::Paused);
                    return Step::Done(format!(
                        "Paused the canary deployment {deployment} at step {step} of {total}: \
                         cf continue-deployment {name} goes on, cf cancel-deployment {name} goes \
                         back"
                    ));
                }
                kept.max_in_flight.min(target - reached)
            }
            _ if not_reached.is_empty() => {
                let kept = &mut self.deployments[at];
                kept.last_healthy_at = Some(now());
                kept.error = None;
                kept.set(DeploymentState::Deployed);
                let line = format!(
                    "Finished the {} deployment {deployment}: the app serves droplet {}",
                    kept.strategy, kept.droplet
                );
                if let Some(app) = self.app_mut(guid) {
                    app.rolling = None;
                }
                return Step::Done(line);
            }
            _ => kept.max_in_flight,
        };
        let taken = take(&not_reached, limit);
        let named = self.wickets_named(&taken);
        if let Some(rolling) = self.app_mut(guid).and_then(|app| app.rolling.as_mut()) {
            // what it has not reached: but those taken, those holding no share or put on no more
            let left = |wicket: &String| {
                !taken.contains(wicket) && not_reached.iter().any(|(kept, _)| kept == wicket)
            };
            rolling.pending.retain(|wicket, _| left(wicket));
        }
        let kept = &mut self.deployments[at];
        kept.last_healthy_at = Some(now());
        kept.error = None;
        kept.updated_at = now();
        let line = format!(
            "The {} deployment {deployment} puts droplet {} on {named}",
            kept.strategy, kept.droplet
        );
        Step::Reach(taken, line)
    }

    /// The instances of each share of `app`, by wicket.
    fn share_sizes(&self, app: &App) -> BTreeMap<String, u64> {
        let shares = Controller::desired(self, app, crate::OWNER);
        (self.indices(app).into_iter())
            .filter(|(wicket, _)| shares.contains_key(wicket))
            .map(|(wicket, indices)| (wicket, indices.len() as u64))
            .collect()
    }

    /// `wickets` as a log line names them: `the wicket a`, `the wickets a and b`; of one wicket
    /// configured, `the wicket`.
    fn wickets_named(&self, wickets: &[String]) -> String {
        match (self.wickets.configured.len(), wickets) {
            (1, _) | (_, []) => "the wicket".into(),
            (_, [one]) => format!("the wicket {one}"),
            (_, [first @ .., last]) => format!("the wickets {} and {last}", first.join(", ")),
        }
    }

    /// The shares the deployment `deployment` of the app `guid` reached, by wicket, with what
    /// each wicket last answered (a wicket put on no more aside): None once it takes no steps.
    pub fn reached(
        &self,
        guid: &str,
        deployment: &str,
        owner: &str,
    ) -> Option<Vec<(String, AppBody, Option<Outcome>)>> {
        self.deployment(deployment).filter(|kept| kept.stepping())?;
        let app = self.app(guid)?;
        let rolling = app.rolling.as_ref();
        let pending = rolling.filter(|rolling| rolling.deployment == deployment);
        let not_reached = |wicket: &String| {
            pending.is_some_and(|rolling| rolling.pending.contains_key(wicket))
                || self.wickets.odd.contains(wicket)
        };
        let shares = Controller::desired(self, app, owner).into_iter();
        let reached = shares.filter(|(wicket, _)| !not_reached(wicket));
        Some(
            reached
                .map(|(wicket, share)| {
                    let outcome = self.outcome(guid, &wicket).cloned();
                    (wicket, share, outcome)
                })
                .collect(),
        )
    }

    /// Notes `error` as why the deployment `deployment` does not go on, none: whether it changed.
    pub fn note_error(&mut self, deployment: &str, error: Option<String>) -> bool {
        let kept = (self.deployments.iter_mut()).find(|kept| kept.guid == deployment);
        match kept {
            Some(kept) if kept.error != error => {
                kept.error = error;
                kept.updated_at = now();
                true
            }
            _ => false,
        }
    }

    /// Cancels the deployment `deployment`: its app back on the snapshot before it and the
    /// droplet before, canceling until every share is put so ([`Controller::cancel_held`]).
    /// Whether it was not canceling yet; refused once it is finalized.
    pub fn cancel_deployment(&mut self, deployment: &str) -> Result<bool, String> {
        let at = (self.deployments.iter())
            .position(|kept| kept.guid == deployment)
            .ok_or("the deployment is gone")?;
        let kept = &self.deployments[at];
        if !kept.cancelable() {
            let (value, reason) = kept.status();
            return Err(format!(
                "Cannot cancel a deployment with status: {value} and reason: {reason}"
            ));
        }
        if kept.state == DeploymentState::Canceling {
            return Ok(false);
        }
        let (guid, previous_droplet) = (kept.app.clone(), kept.previous_droplet.clone());
        let droplet_kept = self.droplet(&previous_droplet).is_some();
        if let Some(app) = self.app_mut(&guid) {
            let ours = |rolling: &Rolling| rolling.deployment == deployment;
            if let Some(rolling) = app.rolling.take_if(|rolling| ours(rolling)) {
                app.running = Some(rolling.previous);
            }
            if droplet_kept {
                app.current_droplet = Some(previous_droplet);
            }
            app.updated_at = now();
        }
        let kept = &mut self.deployments[at];
        kept.error = None;
        kept.set(DeploymentState::Canceling);
        Ok(true)
    }

    /// Finalizes the canceling deployment `deployment`: canceled. The line saying so.
    pub fn canceled(&mut self, deployment: &str) -> Option<String> {
        let kept = (self.deployments.iter_mut())
            .find(|kept| kept.guid == deployment && kept.state == DeploymentState::Canceling)?;
        kept.set(DeploymentState::Canceled);
        Some(format!(
            "Canceled the {} deployment {}: the app serves droplet {} again",
            kept.strategy, kept.guid, kept.previous_droplet
        ))
    }

    /// Continues the paused deployment `deployment`: its next canary step, else the rest. Refused
    /// unless paused.
    pub fn continue_deployment(&mut self, deployment: &str) -> Result<(), String> {
        let kept = (self.deployments.iter_mut())
            .find(|kept| kept.guid == deployment)
            .ok_or("the deployment is gone")?;
        if !kept.continuable() {
            let (value, reason) = kept.status();
            return Err(format!(
                "Cannot continue a deployment with status: {value} and reason: {reason}"
            ));
        }
        let step = kept.canary_step.unwrap_or(1);
        match (step as usize) < kept.canary_steps.len() {
            true => {
                kept.canary_step = Some(step + 1);
                kept.set(DeploymentState::Prepaused);
            }
            false => kept.set(DeploymentState::Deploying),
        }
        Ok(())
    }
}

/// The wickets of `pending`, each with its share's instances, a step reaches: in order, as many as
/// `limit` instances take, one at least.
fn take(pending: &[(String, u64)], limit: u64) -> Vec<String> {
    let mut taken = Vec::new();
    let mut instances = 0;
    for (wicket, size) in pending {
        if !taken.is_empty() && instances + size > limit {
            break;
        }
        instances += size;
        taken.push(wicket.clone());
    }
    taken
}

/// The instances on the new snapshot at the end of a canary's step `step` (from 1) of an app of
/// `instances`, as CC plans it: the step's percentage of them, rounded, one at least; one without
/// steps.
fn canary_target(steps: &[u64], step: u64, instances: u64) -> u64 {
    let weight = step.checked_sub(1).and_then(|at| steps.get(at as usize));
    weight.map_or(1, |weight| ((instances * weight + 50) / 100).max(1))
}

impl Controller {
    /// Takes the deployment `deployment` of the app `guid` a step at a time ([`State::step`]), each
    /// once every share it reached serves it ([`Controller::serving`]), until it is finalized or
    /// paused, or takes no steps any more.
    pub async fn deploy(self: Arc<Self>, guid: String, deployment: String) {
        while self.serving(&guid, &deployment).await {
            let _app = self.hold(&guid).await;
            match self
                .store
                .update(|state| state.step(&guid, &deployment))
                .await
            {
                Step::None => return,
                Step::Done(line) => {
                    self.logs.add(&guid, "API", line);
                    return;
                }
                Step::Reach(wickets, line) => {
                    self.logs.add(&guid, "API", line);
                    self.sync_on(&guid, Some(&wickets)).await;
                }
            }
        }
    }

    /// Waits until every share the deployment `deployment` of the app `guid` reached serves it:
    /// its wicket took it and, with a readiness check, the VMs it keeps are ready. A wicket's
    /// refusal of it, or its absence, is the deployment's error meanwhile. False once it takes no
    /// steps.
    async fn serving(&self, guid: &str, deployment: &str) -> bool {
        let owner = self.owner();
        loop {
            let reached = self
                .store
                .read(|state| state.reached(guid, deployment, owner));
            let Some(reached) = reached else {
                return false;
            };
            // why it waits, and whether that is an error
            let mut waits = None;
            for (name, share, outcome) in &reached {
                let Some(wicket) = self.wickets.iter().find(|wicket| wicket.name == *name) else {
                    continue;
                };
                waits = match outcome {
                    Some(Outcome::Running) => self.unready(wicket, guid, share).await,
                    Some(Outcome::Refused(error)) => {
                        Some((format!("{} refused it: {error}", wicket.sentence()), true))
                    }
                    Some(Outcome::Unreachable) => {
                        Some((format!("{} is unreachable", wicket.sentence()), true))
                    }
                    // not put yet, as at a start of the cf-api
                    None => Some((String::new(), false)),
                };
                if waits.is_some() {
                    break;
                }
            }
            let error = (waits.as_ref())
                .filter(|(_, error)| *error)
                .map(|(why, _)| why.clone());
            let changed = self.store.read(|state| {
                (state.deployment(deployment)).is_some_and(|kept| kept.error != error)
            });
            if changed {
                self.store
                    .update(|state| state.note_error(deployment, error.clone()))
                    .await;
            }
            if waits.is_none() {
                return true;
            }
            tokio::time::sleep(PAUSE).await;
        }
    }

    /// Why the share `share` of the app `guid` on `wicket` is not ready, with a readiness check:
    /// the VMs it keeps not all ready as the wicket lists them now.
    pub(crate) async fn unready(
        &self,
        wicket: &Wicket,
        guid: &str,
        share: &AppBody,
    ) -> Option<(String, bool)> {
        let Sandbox::Vm(vm) = &share.sandbox else {
            return None;
        };
        vm.readiness.as_ref()?;
        let kept = share.sandbox.kept();
        let listing = match wicket.list(None).await {
            Ok(listing) => listing,
            Err(e) => return Some((format!("{}: {e}", wicket.sentence()), false)),
        };
        let ours = |listed: &&crate::wicket::ListedApp| {
            listed.name == guid && listed.app.owner == share.owner && listed.app.tag == share.tag
        };
        let ready = (listing.apps.iter().find(ours)).map_or(0, |listed| {
            let alive = listed.alive.iter();
            alive.filter(|alive| alive.ready != Some(false)).count() as u64
        });
        let why = format!("{}: {ready} of its {kept} VMs ready", wicket.sentence());
        (ready < kept).then_some((why, false))
    }

    /// Puts every share of the app `guid` as its snapshot before the canceling deployment
    /// `deployment` has them, then finalizes it: canceled, logged. The app held by the caller.
    pub async fn cancel_held(&self, guid: &str, deployment: &str) {
        let canceling = self.store.read(|state| {
            let kept = state.deployment(deployment);
            kept.is_some_and(|kept| kept.state == DeploymentState::Canceling)
        });
        if !canceling {
            return;
        }
        self.sync(guid).await;
        if let Some(line) = self.store.update(|state| state.canceled(deployment)).await {
            self.logs.add(guid, "API", line);
        }
    }

    /// [`Controller::cancel_held`] of a cancel a stop of the cf-api cut short.
    async fn finish_cancel(self: Arc<Self>, guid: String, deployment: String) {
        let _app = self.hold(&guid).await;
        self.cancel_held(&guid, &deployment).await;
    }

    /// Takes up what a stop of the cf-api left under way: the deployments taking steps, those
    /// canceling, and the indices moving to another wicket ([`Controller::resume_moves`]).
    pub fn resume(self: &Arc<Self>) {
        self.resume_moves();
        let under_way: Vec<(String, String, bool)> = self.store.read(|state| {
            let deployments = state.deployments.iter();
            deployments
                .filter(|kept| kept.stepping() || kept.state == DeploymentState::Canceling)
                .map(|kept| (kept.app.clone(), kept.guid.clone(), kept.stepping()))
                .collect()
        });
        for (app, deployment, stepping) in under_way {
            match stepping {
                true => tokio::spawn(self.clone().deploy(app, deployment)),
                false => tokio::spawn(self.clone().finish_cancel(app, deployment)),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A snapshot of the droplet `droplet`, its tag its name.
    fn snapshot(droplet: &str) -> Running {
        Running {
            droplet: droplet.into(),
            sha256: "ab".repeat(32),
            file: "app.wasm".into(),
            memory_mib: 64,
            tag: Some(droplet.into()),
            ..Running::default()
        }
    }

    /// The wickets a, b and c, and an app of 3 instances, one on each, serving `d1`.
    fn serving() -> (State, String) {
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.follow_wickets(&["a".into(), "b".into(), "c".into()]);
        let space = state.spaces[0].guid.clone();
        let domain = state.domains[0].guid.clone();
        let app = state.create_app("app", &space);
        app.process.instances = 3;
        app.started = true;
        app.running = Some(snapshot("d1"));
        app.placement = vec!["a".into(), "b".into(), "c".into()];
        let guid = app.guid.clone();
        let route = state.create_route("app", &domain, &space).guid.clone();
        state.map(&route, &guid);
        (state, guid)
    }

    /// Deploys `droplet` to the app `guid` as `asked`.
    fn deploy(state: &mut State, guid: &str, droplet: &str, asked: &Asked) -> Deployment {
        let serving = state.serving(state.app(guid).unwrap());
        let previous = "d0".to_string();
        state
            .deploy(guid, snapshot(droplet), serving, previous, asked)
            .0
    }

    /// The droplet each wicket's share serves, by wicket.
    fn serves(state: &State, guid: &str) -> Vec<(String, String)> {
        let shares = Controller::desired(state, state.app(guid).unwrap(), "cf");
        let tagged = shares.into_iter();
        tagged
            .map(|(wicket, share)| (wicket, share.tag.unwrap()))
            .collect()
    }

    fn droplets(serving: &[(&str, &str)]) -> Vec<(String, String)> {
        let pairs = serving.iter();
        pairs
            .map(|(wicket, droplet)| (wicket.to_string(), droplet.to_string()))
            .collect()
    }

    /// The wickets the next step reaches.
    fn reach(state: &mut State, guid: &str, deployment: &Deployment) -> Vec<String> {
        match state.step(guid, &deployment.guid) {
            Step::Reach(wickets, _) => wickets,
            other => panic!("{other:?}"),
        }
    }

    /// What the next step says it did, finishing or pausing.
    fn done(state: &mut State, guid: &str, deployment: &Deployment) -> String {
        match state.step(guid, &deployment.guid) {
            Step::Done(line) => line,
            other => panic!("{other:?}"),
        }
    }

    fn status(state: &State, deployment: &Deployment) -> (&'static str, &'static str) {
        state.deployment(&deployment.guid).unwrap().status()
    }

    /// A deployment reaches a wicket's share at a time, in the order configured, the others
    /// serving the snapshot before: as many as `max_in_flight` instances take, a canary pausing at
    /// its steps' targets. Superseded, what it had not reached serves on; canceled, every share
    /// serves the snapshot before; of an app serving nothing, at once.
    #[test]
    fn a_deployment_goes_a_wicket_at_a_time() {
        let rolling = |max_in_flight| Asked {
            strategy: "rolling".into(),
            droplet: None,
            max_in_flight,
            canary_steps: Vec::new(),
            scaled: Scaled::default(),
        };
        let canary = |steps: &[u64]| Asked {
            strategy: "canary".into(),
            canary_steps: steps.to_vec(),
            ..rolling(1)
        };
        let reached = |state: &State, guid: &str, deployment: &Deployment| {
            let reached = state.reached(guid, &deployment.guid, "cf").unwrap();
            let named = reached.into_iter();
            named
                .map(|(wicket, _, outcome)| (wicket, outcome))
                .collect::<Vec<_>>()
        };

        // one at a time
        let (mut state, guid) = serving();
        let deployment = deploy(&mut state, &guid, "d2", &rolling(1));
        assert_eq!(status(&state, &deployment), ("ACTIVE", "DEPLOYING"));
        let before = droplets(&[("a", "d1"), ("b", "d1"), ("c", "d1")]);
        assert_eq!(serves(&state, &guid), before);
        assert_eq!(reached(&state, &guid, &deployment), []);
        assert_eq!(reach(&mut state, &guid, &deployment), ["a"]);
        let a = droplets(&[("a", "d2"), ("b", "d1"), ("c", "d1")]);
        assert_eq!(serves(&state, &guid), a);
        state
            .outcomes
            .insert(guid.clone(), [("a".to_string(), Outcome::Running)].into());
        assert_eq!(
            reached(&state, &guid, &deployment),
            [("a".to_string(), Some(Outcome::Running))]
        );
        assert_eq!(reach(&mut state, &guid, &deployment), ["b"]);
        assert_eq!(reach(&mut state, &guid, &deployment), ["c"]);
        let finished = done(&mut state, &guid, &deployment);
        assert!(finished.starts_with("Finished the rolling"), "{finished}");
        assert_eq!(status(&state, &deployment), ("FINALIZED", "DEPLOYED"));
        assert_eq!(state.app(&guid).unwrap().rolling, None);
        let after = droplets(&[("a", "d2"), ("b", "d2"), ("c", "d2")]);
        assert_eq!(serves(&state, &guid), after);
        assert_eq!(state.step(&guid, &deployment.guid), Step::None);
        assert_eq!(state.reached(&guid, &deployment.guid, "cf"), None);

        // two instances at once; superseded deploying: deployed, what it reached serving on
        let (mut state, guid) = serving();
        let first = deploy(&mut state, &guid, "d2", &rolling(2));
        assert_eq!(reach(&mut state, &guid, &first), ["a", "b"]);
        let second = deploy(&mut state, &guid, "d3", &rolling(2));
        assert_eq!(status(&state, &first), ("FINALIZED", "SUPERSEDED"));
        let superseded = state.deployment(&first.guid).unwrap();
        assert_eq!(superseded.state, DeploymentState::Deployed);
        let on = droplets(&[("a", "d2"), ("b", "d2"), ("c", "d1")]);
        assert_eq!(serves(&state, &guid), on);
        assert_eq!(reach(&mut state, &guid, &second), ["a", "b"]);
        assert_eq!(reach(&mut state, &guid, &second), ["c"]);

        // a canary of one instance, paused; continued, the rest
        let (mut state, guid) = serving();
        let deployment = deploy(&mut state, &guid, "d2", &canary(&[]));
        assert_eq!(reach(&mut state, &guid, &deployment), ["a"]);
        let paused = done(&mut state, &guid, &deployment);
        assert!(
            paused.contains("at step 1 of 1: cf continue-deployment app"),
            "{paused}"
        );
        assert_eq!(status(&state, &deployment), ("ACTIVE", "PAUSED"));
        let kept = state.deployment(&deployment.guid).unwrap();
        let view = crate::views::deployment("h", &state, kept);
        assert_eq!(
            view["status"]["canary"],
            json!({"steps": {"current": 1, "total": 1}})
        );
        assert!(view["links"]["continue"].is_object() && view["links"]["cancel"].is_object());
        assert_eq!(state.step(&guid, &deployment.guid), Step::None);
        state.continue_deployment(&deployment.guid).unwrap();
        assert_eq!(reach(&mut state, &guid, &deployment), ["b"]);
        assert_eq!(reach(&mut state, &guid, &deployment), ["c"]);
        done(&mut state, &guid, &deployment);
        assert_eq!(
            state.continue_deployment(&deployment.guid),
            Err("Cannot continue a deployment with status: FINALIZED and reason: DEPLOYED".into())
        );

        // steps of 34% and 67% of 3 instances: 1 (a), then 2 (b), paused after each. Superseded
        // paused: canceled, the next one's before the snapshot it deployed, c serving on the one
        // before it
        let (mut state, guid) = serving();
        let deployment = deploy(&mut state, &guid, "d2", &canary(&[34, 67]));
        assert_eq!(reach(&mut state, &guid, &deployment), ["a"]);
        assert!(done(&mut state, &guid, &deployment).contains("at step 1 of 2"));
        state.continue_deployment(&deployment.guid).unwrap();
        let kept = state.deployment(&deployment.guid).unwrap();
        let view = crate::views::deployment("h", &state, kept);
        assert_eq!(
            (
                &view["status"]["canary"]["steps"]["current"],
                &view["options"]["canary"]["steps"][1]
            ),
            (&json!(2), &json!({"instance_weight": 67}))
        );
        assert_eq!(reach(&mut state, &guid, &deployment), ["b"]);
        assert!(done(&mut state, &guid, &deployment).contains("at step 2 of 2"));
        let next = deploy(&mut state, &guid, "d3", &rolling(1));
        assert_eq!(status(&state, &deployment), ("FINALIZED", "SUPERSEDED"));
        let superseded = state.deployment(&deployment.guid).unwrap();
        assert_eq!(superseded.state, DeploymentState::Canceled);
        assert_eq!(reach(&mut state, &guid, &next), ["a"]);
        let three = droplets(&[("a", "d3"), ("b", "d2"), ("c", "d1")]);
        assert_eq!(serves(&state, &guid), three);

        // canceled: every share on the snapshot before it, canceling until they are put
        assert_eq!(state.cancel_deployment(&next.guid), Ok(true));
        assert_eq!(status(&state, &next), ("ACTIVE", "CANCELING"));
        let back = droplets(&[("a", "d2"), ("b", "d2"), ("c", "d2")]);
        assert_eq!(serves(&state, &guid), back);
        assert_eq!(state.cancel_deployment(&next.guid), Ok(false));
        assert!(state.canceled(&next.guid).is_some());
        assert_eq!(status(&state, &next), ("FINALIZED", "CANCELED"));
        assert_eq!(
            state.cancel_deployment(&next.guid),
            Err("Cannot cancel a deployment with status: FINALIZED and reason: CANCELED".into())
        );

        // an app serving nothing: started at once, deployed
        state.app_mut(&guid).unwrap().started = false;
        let at_once = deploy(&mut state, &guid, "d4", &rolling(1));
        assert_eq!(status(&state, &at_once), ("FINALIZED", "DEPLOYED"));
        let all = droplets(&[("a", "d4"), ("b", "d4"), ("c", "d4")]);
        assert_eq!(serves(&state, &guid), all);
    }

    /// What a deployment asks, checked as CC checks it.
    #[test]
    fn a_deployment_asks_what_cc_takes() {
        let asked = |body: Value| Asked::parse(&body);
        assert_eq!(
            asked(json!({"relationships": {}})),
            Ok(Asked {
                strategy: "rolling".into(),
                droplet: None,
                max_in_flight: 1,
                canary_steps: Vec::new(),
                scaled: Scaled::default(),
            })
        );
        let options = json!({"max_in_flight": 2, "web_instances": 3, "memory_in_mb": 256,
            "canary": {"steps": [{"instance_weight": 20}, {"instance_weight": 60}]}});
        assert_eq!(
            asked(json!({"strategy": "canary", "droplet": {"guid": "d"}, "options": options})),
            Ok(Asked {
                strategy: "canary".into(),
                droplet: Some("d".into()),
                max_in_flight: 2,
                canary_steps: vec![20, 60],
                scaled: Scaled {
                    web_instances: Some(3),
                    memory_in_mb: Some(256),
                    ..Scaled::default()
                },
            })
        );
        let steps =
            |steps: Value| json!({"strategy": "canary", "options": {"canary": {"steps": steps}}});
        let options = |options: Value| json!({"options": options});
        for (body, refused) in [
            (
                json!({"strategy": "blue-green"}),
                "'blue-green' is not a supported",
            ),
            (
                json!({"revision": {"guid": "r"}}),
                "revisions are not supported",
            ),
            (
                options(json!({"max_in_flight": 0})),
                "max_in_flight must be an integer greater than 0",
            ),
            (
                options(json!({"max_in_flight": "2"})),
                "max_in_flight must be an integer greater than 0",
            ),
            (
                options(json!({"web_instances": -1})),
                "web_instances must be an integer from 0",
            ),
            (
                options(json!({"surge": 1})),
                "options has unsupported key(s): surge",
            ),
            (
                options(json!({"canary": {}})),
                "options.canary are only valid for Canary deployments",
            ),
            (
                steps(json!([{"instance_weight": 0}])),
                "an Integer between 1-100",
            ),
            (
                steps(json!([{"instance_weight": 60}, {"instance_weight": 20}])),
                "ascending",
            ),
            (steps(json!([{"weight": 20}])), "unsupported key(s): weight"),
        ] {
            let why = asked(body.clone()).unwrap_err();
            assert!(why.contains(refused), "{body}: {why}");
        }
    }
}
