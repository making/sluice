//! The brokers' operations on their instances and bindings
//! ([`crate::store::Operation`]): a provision, an update, a deprovision, a
//! bind, an unbind. Each under way is driven by a task of its own, work of the
//! turn of the instance reconciling the wickets (`Controller::follow`,
//! [`Operations::drive`]): it sends the request until the broker takes it --
//! again in the next turn, a restart of the cf-api or another instance's: the
//! requests are idempotent -- then, the broker answering 202, polls the last
//! operation, as often as its `Retry-After` asks or every [`POLL`], until it
//! succeeds, fails, or outlives its plan's `maximum_polling_duration`
//! ([`MAX_POLLING`] without one). Its end settles its job and is in the app's
//! log as a binding made at once is. Each of its changes is the turn's alone
//! (`Store::lead`): an instance whose turn ended ends none.
//!
//! A provision or a bind whose outcome is unknown (no answer in time, a
//! 5xx, a body not understood) is undone as the specification asks (orphan
//! mitigation): the broker may hold what the cf-api has not. That it is to be
//! is kept as the operation fails, before the undo is sent, so an instance
//! whose turn ended sends none, and the next turn sends one the last did not
//! see answered.

use std::sync::Arc;
use std::time::Duration;

use http::Method;
use serde_json::{Map, Value, json};

use crate::logs::Logs;
use crate::osb::{self, Answer, Endpoint, Refusal};
use crate::store::{
    JobError, Operation, OperationKind, OperationState, State, Store, epoch_secs, now,
};

/// How long the first poll waits after the broker took an operation; sooner in the tests.
const FIRST_POLL: Duration = Duration::from_millis(if cfg!(test) { 50 } else { 1000 });
/// How often a broker giving no `Retry-After` is polled.
const POLL: Duration = Duration::from_millis(if cfg!(test) { 100 } else { 5000 });
/// The longest a broker's `Retry-After` makes a poll wait.
const LONGEST_POLL: Duration = Duration::from_secs(3600);
/// How long an operation of a plan with no `maximum_polling_duration` is
/// polled: CC's `broker_client_max_async_poll_duration_minutes`.
const MAX_POLLING: u64 = 7 * 24 * 3600;

/// What an operation is of.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Target {
    Instance(String),
    Binding(String),
}

impl Target {
    pub fn guid(&self) -> &str {
        match self {
            Target::Instance(guid) | Target::Binding(guid) => guid,
        }
    }
}

pub struct Operations {
    store: Arc<Store>,
    osb: osb::Client,
    logs: Arc<Logs>,
}

/// What the next step of a task is.
enum Next {
    /// Poll after the delay.
    Poll(Duration),
    /// Send again at once (the operation changed under it).
    Again,
    Done,
}

/// What an operation needs of the state, read at each step: what it is of
/// may change or go meanwhile.
struct Work {
    target: Target,
    operation: Operation,
    endpoint: Endpoint,
    /// The instance's guid: its `instance_id`.
    instance: String,
    instance_name: String,
    service_id: String,
    plan_id: String,
    /// An update's plan's id.
    new_plan_id: Option<String>,
    /// The instance's maintenance info.
    maintenance_info: Option<Value>,
    /// The binding's app: its guid and name.
    app: Option<(String, String)>,
    bindings_retrievable: bool,
    /// The plan's `maximum_polling_duration`.
    max_polling: u64,
    context: Value,
}

impl Work {
    fn of(state: &State, target: &Target) -> Option<Result<Work, (Operation, String)>> {
        let (instance, operation, app) = match target {
            Target::Instance(guid) => {
                let instance = state.instance(guid)?;
                (instance, instance.operation.clone()?, None)
            }
            Target::Binding(guid) => {
                let binding = state.binding(guid)?;
                let instance = state.instance(&binding.instance)?;
                let app = state.app(&binding.app).map_or_else(
                    || (binding.app.clone(), String::new()),
                    |app| (app.guid.clone(), app.name.clone()),
                );
                (instance, binding.operation.clone()?, Some(app))
            }
        };
        if !operation.owed() {
            return None;
        }
        let Some(brokered) = state.brokered_of(instance) else {
            let gone = format!("the plan of service instance {} is gone", instance.name);
            return Some(Err((operation, gone)));
        };
        let new_plan_id = operation
            .plan
            .as_ref()
            .and_then(|plan| state.brokered(plan))
            .map(|plan| plan.plan.id().to_string());
        let (org, space) = (state.org_of(&instance.space), state.space(&instance.space));
        let mut context = json!({
            "platform": "cloudfoundry",
            "organization_guid": org.map(|org| &org.guid),
            "organization_name": org.map(|org| &org.name),
            "organization_annotations": {},
            "space_guid": space.map(|space| &space.guid),
            "space_name": space.map(|space| &space.name),
            "space_annotations": {},
            "instance_name": instance.name,
            "instance_annotations": {},
        });
        if let Some((_, name)) = &app {
            context["app_name"] = json!(name);
        }
        Some(Ok(Work {
            target: target.clone(),
            operation,
            endpoint: Endpoint::from(brokered.broker),
            instance: instance.guid.clone(),
            instance_name: instance.name.clone(),
            service_id: brokered.offering.id().to_string(),
            plan_id: brokered.plan.id().to_string(),
            new_plan_id,
            maintenance_info: instance.maintenance_info.clone(),
            app,
            bindings_retrievable: brokered.offering.feature("bindings_retrievable"),
            max_polling: brokered
                .plan
                .maximum_polling_duration()
                .unwrap_or(MAX_POLLING),
            context,
        }))
    }

    fn path(&self) -> String {
        match &self.target {
            Target::Instance(_) => format!("/v2/service_instances/{}", self.instance),
            Target::Binding(binding) => format!(
                "/v2/service_instances/{}/service_bindings/{binding}",
                self.instance
            ),
        }
    }

    /// The plan the request names: an update's new one.
    fn plan_id(&self) -> &str {
        self.new_plan_id.as_deref().unwrap_or(&self.plan_id)
    }

    /// What it is, as the log says it: `creating service instance db`.
    fn what(&self) -> String {
        doing(&self.target, self.operation.kind, &self.instance_name)
    }
}

/// How an operation ended.
enum End {
    Succeeded(Map<String, Value>),
    Failed(String),
    /// Failed, its outcome at the broker unknown: a provision or a bind is to be undone there.
    Orphaned(String),
}

impl Operations {
    pub fn new(store: Arc<Store>, osb: osb::Client, logs: Arc<Logs>) -> Operations {
        Operations { store, osb, logs }
    }

    /// What `state` has an operation to drive of: under way, or to be undone at its broker.
    pub fn due(state: &State) -> Vec<Target> {
        let owed = |operation: &Option<Operation>| operation.as_ref().is_some_and(Operation::owed);
        let instances = (state.instances.iter())
            .filter(|instance| owed(&instance.operation))
            .map(|instance| Target::Instance(instance.guid.clone()));
        let bindings = (state.bindings.iter())
            .filter(|binding| owed(&binding.operation))
            .map(|binding| Target::Binding(binding.guid.clone()));
        instances.chain(bindings).collect()
    }

    /// Drives the operation of `target` to its end, and one that replaced it meanwhile.
    pub async fn drive(self: Arc<Self>, target: Target) {
        loop {
            match self.step(&target).await {
                Next::Poll(delay) => tokio::time::sleep(delay).await,
                Next::Again => {}
                Next::Done => break,
            }
        }
    }

    async fn step(&self, target: &Target) -> Next {
        let work = match self.store.read(|state| Work::of(state, target)) {
            None => return Next::Done,
            Some(Ok(work)) => work,
            Some(Err((operation, why))) if operation.under_way() => {
                self.end(target, &operation, End::Failed(why)).await;
                return Next::Done;
            }
            // its plan gone, its broker too: nothing to undo it at
            Some(Err((operation, _))) => {
                self.undone(target, &operation).await;
                return Next::Done;
            }
        };
        if !work.operation.under_way() {
            self.undo(work).await
        } else if work.operation.sent {
            self.poll(work).await
        } else {
            self.send(work).await
        }
    }

    /// Sends the request; an answer at once ends it, a 202 has it polled.
    async fn send(&self, work: Work) -> Next {
        let operation = &work.operation;
        let path = work.path();
        let user = operation.user.as_deref();
        let ids = [
            ("service_id", work.service_id.as_str()),
            ("plan_id", work.plan_id()),
        ];
        let answer = match (&work.target, operation.kind) {
            (_, OperationKind::Delete) => {
                self.osb
                    .request(&work.endpoint, Method::DELETE, &path, &ids, None, user)
                    .await
            }
            (Target::Instance(_), OperationKind::Create) => {
                let mut body = json!({
                    "service_id": work.service_id,
                    "plan_id": work.plan_id,
                    "organization_guid": work.context["organization_guid"],
                    "space_guid": work.context["space_guid"],
                    "context": work.context,
                });
                if let Some(parameters) = &operation.parameters {
                    body["parameters"] = parameters.clone();
                }
                if let Some(info) = &work.maintenance_info {
                    body["maintenance_info"] = info.clone();
                }
                self.osb
                    .request(&work.endpoint, Method::PUT, &path, &[], Some(&body), user)
                    .await
            }
            (Target::Instance(_), OperationKind::Update) => {
                let mut previous = json!({
                    "service_id": work.service_id,
                    "plan_id": work.plan_id,
                    "organization_id": work.context["organization_guid"],
                    "space_id": work.context["space_guid"],
                });
                if let Some(info) = &work.maintenance_info {
                    previous["maintenance_info"] = info.clone();
                }
                let mut body = json!({
                    "service_id": work.service_id,
                    "plan_id": work.plan_id(),
                    "context": work.context,
                    "previous_values": previous,
                });
                if let Some(parameters) = &operation.parameters {
                    body["parameters"] = parameters.clone();
                }
                if let Some(info) = operation
                    .maintenance_info
                    .as_ref()
                    .or(work.maintenance_info.as_ref())
                {
                    body["maintenance_info"] = info.clone();
                }
                self.osb
                    .request(&work.endpoint, Method::PATCH, &path, &[], Some(&body), user)
                    .await
            }
            (Target::Binding(_), _) => {
                let (app, _) = work.app.clone().unwrap_or_default();
                let mut body = json!({
                    "service_id": work.service_id,
                    "plan_id": work.plan_id,
                    "app_guid": app,
                    "bind_resource": {"app_guid": app, "space_guid": work.context["space_guid"]},
                    "context": work.context,
                });
                if let Some(parameters) = &operation.parameters {
                    body["parameters"] = parameters.clone();
                }
                self.osb
                    .request(&work.endpoint, Method::PUT, &path, &[], Some(&body), user)
                    .await
            }
        };
        let creates = operation.kind == OperationKind::Create;
        match answer {
            Ok(Answer::Done(body)) => match self.check_done(&work, body) {
                Ok(body) => {
                    self.end(&work.target, operation, End::Succeeded(body))
                        .await;
                    Next::Done
                }
                Err(e) => {
                    self.end(&work.target, operation, End::Orphaned(e)).await;
                    Next::Done
                }
            },
            Ok(Answer::Gone) if operation.kind == OperationKind::Delete => {
                self.end(&work.target, operation, End::Succeeded(Map::new()))
                    .await;
                Next::Done
            }
            Ok(Answer::Gone) => {
                let e = "The service broker returned an invalid response: 410 Gone".to_string();
                self.end(&work.target, operation, End::Failed(e)).await;
                Next::Done
            }
            Ok(Answer::Accepted { .. })
                if creates
                    && matches!(work.target, Target::Binding(_))
                    && !work.bindings_retrievable =>
            {
                // its credentials could never be fetched
                let e = "The service broker responded asynchronously to a binding request, \
                         but its offering does not support fetching bindings (bindings_retrievable)";
                self.end(&work.target, operation, End::Orphaned(e.into()))
                    .await;
                Next::Done
            }
            Ok(Answer::Accepted {
                operation: broker_operation,
                body,
            }) => {
                let deadline = epoch_secs() + work.max_polling;
                let taken = self
                    .store
                    .lead(|state| {
                        let dashboard = body.get("dashboard_url").and_then(Value::as_str);
                        if let (Target::Instance(guid), Some(url)) = (&work.target, dashboard)
                            && let Some(instance) = state.instance_mut(guid)
                        {
                            instance.dashboard_url = Some(url.to_string());
                        }
                        let current = operation_mut(state, &work.target)?;
                        if current.created_at != operation.created_at {
                            return None;
                        }
                        current.sent = true;
                        current.parameters = None;
                        current.broker_operation = broker_operation.clone();
                        current.deadline = Some(deadline);
                        current.updated_at = now();
                        let job = current.job.clone();
                        if let Some(job) = job.as_deref().and_then(|job| state.job_mut(job))
                            && job.operation.starts_with("service_")
                        {
                            job.polling = true;
                        }
                        Some(())
                    })
                    .await;
                match taken {
                    Some(Some(())) => Next::Poll(FIRST_POLL),
                    Some(None) => Next::Again,
                    // the turn over
                    None => Next::Done,
                }
            }
            Err(refusal) => {
                let end = match creates && refusal.unknown {
                    true => End::Orphaned(refusal.description),
                    false => End::Failed(refusal.description),
                };
                self.end(&work.target, operation, end).await;
                Next::Done
            }
        }
    }

    /// The body of a provision, an update or a bind done, as it is to be:
    /// a binding's `credentials` an object.
    fn check_done(
        &self,
        work: &Work,
        body: Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        let binds = matches!(work.target, Target::Binding(_))
            && work.operation.kind == OperationKind::Create;
        match body.get("credentials") {
            Some(Value::Object(_)) | None => Ok(body),
            Some(_) if binds => Err(
                "The service broker returned an invalid response: credentials is a JSON object"
                    .into(),
            ),
            Some(_) => Ok(body),
        }
    }

    /// Polls the last operation: ends it, or says when to poll again.
    async fn poll(&self, work: Work) -> Next {
        let operation = &work.operation;
        if operation
            .deadline
            .is_some_and(|deadline| epoch_secs() > deadline)
        {
            let e = format!(
                "The service broker did not finish {} within {}s",
                work.what(),
                work.max_polling
            );
            self.end(&work.target, operation, End::Failed(e)).await;
            return Next::Done;
        }
        let mut query = vec![
            ("service_id", work.service_id.as_str()),
            ("plan_id", work.plan_id()),
        ];
        if let Some(broker_operation) = &operation.broker_operation {
            query.push(("operation", broker_operation));
        }
        let path = format!("{}/last_operation", work.path());
        let polled = self.osb.poll(&work.endpoint, &path, &query).await;
        let polled = match polled {
            Ok(polled) => polled,
            Err(Refusal {
                status: Some(400),
                description,
                ..
            }) => {
                self.end(&work.target, operation, End::Failed(description))
                    .await;
                return Next::Done;
            }
            Err(refusal) => {
                // the broker away for a while: polled until the deadline
                println!(
                    "SERVICES: polling {} failed, polled again: {}",
                    work.what(),
                    refusal.description
                );
                return Next::Poll(POLL);
            }
        };
        let again = polled.retry_after.map_or(POLL, |after| {
            after.clamp(Duration::from_secs(1), LONGEST_POLL)
        });
        match (polled.state, operation.kind) {
            // gone: a deletion's success, else no answer yet
            (None, OperationKind::Delete)
            | (Some(OperationState::Succeeded), OperationKind::Delete) => {
                self.end(&work.target, operation, End::Succeeded(Map::new()))
                    .await;
                Next::Done
            }
            (None, _) => Next::Poll(again),
            (Some(OperationState::InProgress), _) => {
                if let Some(description) = polled.description {
                    self.store
                        .lead(|state| {
                            if let Some(current) = operation_mut(state, &work.target)
                                && current.created_at == operation.created_at
                                && current.description != description
                            {
                                current.description = description.clone();
                                current.updated_at = now();
                            }
                        })
                        .await;
                }
                Next::Poll(again)
            }
            (Some(OperationState::Failed), _) => {
                let e = polled
                    .description
                    .unwrap_or_else(|| format!("the service broker failed {}", work.what()));
                self.end(&work.target, operation, End::Failed(e)).await;
                Next::Done
            }
            (Some(OperationState::Succeeded), _) => {
                let mut body = Map::new();
                if let Some(description) = polled.description {
                    body.insert("description".into(), Value::String(description));
                }
                if let (Target::Binding(_), OperationKind::Create) = (&work.target, operation.kind)
                {
                    // the credentials of an asynchronous binding are fetched
                    match self.osb.fetch(&work.endpoint, &work.path()).await {
                        Ok(fetched) => match self.check_done(&work, fetched) {
                            Ok(fetched) => body.extend(fetched),
                            Err(e) => {
                                self.end(&work.target, operation, End::Orphaned(e)).await;
                                return Next::Done;
                            }
                        },
                        Err(refusal) => {
                            let end = End::Orphaned(refusal.description);
                            self.end(&work.target, operation, end).await;
                            return Next::Done;
                        }
                    }
                }
                self.end(&work.target, operation, End::Succeeded(body))
                    .await;
                Next::Done
            }
        }
    }

    /// Undoes at the broker the provision or the bind the operation of `work` failed at, its
    /// outcome unknown: a deprovision or an unbind the broker may refuse, sent once it is kept
    /// that it is to be ([`Operations::end`]), and owed no more once the broker answered.
    async fn undo(&self, work: Work) -> Next {
        let ids = [
            ("service_id", work.service_id.as_str()),
            ("plan_id", work.plan_id.as_str()),
        ];
        let undone = self
            .osb
            .request(
                &work.endpoint,
                Method::DELETE,
                &work.path(),
                &ids,
                None,
                None,
            )
            .await;
        let what = work.what();
        match undone {
            Ok(_) => println!("SERVICES: undid {what} at the broker (orphan mitigation)"),
            Err(e) => println!(
                "SERVICES: could not undo {what} at the broker (orphan mitigation): {}",
                e.description
            ),
        }
        self.undone(&work.target, &work.operation).await;
        Next::Done
    }

    /// The `operation` of `target` is to be undone no more.
    async fn undone(&self, target: &Target, operation: &Operation) {
        self.store
            .lead(|state| {
                if let Some(current) = operation_mut(state, target)
                    && current.created_at == operation.created_at
                {
                    current.undo = false;
                }
            })
            .await;
    }

    /// Records how the operation ended, unless another replaced it or this instance's turn is
    /// over: on the instance or binding (a deletion's success removes it), its job and the
    /// logs; and that a provision or a bind orphaned is to be undone ([`Operations::undo`]).
    async fn end(&self, target: &Target, operation: &Operation, end: End) {
        let failed = match &end {
            End::Failed(e) | End::Orphaned(e) => Some(e.clone()),
            End::Succeeded(_) => None,
        };
        let ended = self
            .store
            .lead(|state| {
                let current = operation_mut(state, target)?;
                if current.created_at != operation.created_at || !current.under_way() {
                    return None;
                }
                let at = now();
                current.state = match &end {
                    End::Succeeded(_) => OperationState::Succeeded,
                    End::Failed(_) | End::Orphaned(_) => OperationState::Failed,
                };
                current.undo =
                    matches!(end, End::Orphaned(_)) && current.kind == OperationKind::Create;
                current.description = match &end {
                    End::Failed(e) | End::Orphaned(e) => e.clone(),
                    End::Succeeded(body) => body
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                };
                current.sent = true;
                current.parameters = None;
                current.updated_at = at.clone();
                let job = current.job.clone();
                let kind = current.kind;
                let removed = matches!(end, End::Succeeded(_)) && kind == OperationKind::Delete;
                let mut app = None;
                let mut name = String::new();
                match target {
                    Target::Instance(guid) => {
                        let instance = state.instance_mut(guid).expect("its operation found");
                        name.clone_from(&instance.name);
                        if let (
                            End::Succeeded(body),
                            OperationKind::Create | OperationKind::Update,
                        ) = (&end, kind)
                        {
                            if let Some(url) = body.get("dashboard_url").and_then(Value::as_str) {
                                instance.dashboard_url = Some(url.to_string());
                            }
                            let operation = instance.operation.as_ref().expect("found");
                            if let Some(plan) = operation.plan.clone() {
                                instance.plan = plan;
                            }
                            if let Some(info) = operation.maintenance_info.clone() {
                                instance.maintenance_info = Some(info);
                            }
                            instance.updated_at = at;
                        }
                        if removed {
                            state.instances.retain(|instance| instance.guid != *guid);
                        }
                    }
                    Target::Binding(guid) => {
                        let binding = state.binding_mut(guid).expect("its operation found");
                        app = Some(binding.app.clone());
                        let instance = binding.instance.clone();
                        if let (End::Succeeded(body), OperationKind::Create) = (&end, kind) {
                            binding.credentials = body
                                .get("credentials")
                                .and_then(Value::as_object)
                                .cloned()
                                .unwrap_or_default();
                            binding.updated_at = at;
                        }
                        if removed {
                            state.bindings.retain(|binding| binding.guid != *guid);
                        }
                        name = state
                            .instance(&instance)
                            .map_or_else(String::new, |instance| instance.name.clone());
                    }
                }
                if let Some(job) = &job {
                    let error = failed.as_ref().map(|e| JobError {
                        code: 10009,
                        title: "CF-UnableToPerform".into(),
                        detail: format!("{} failed: {e}", doing(target, kind, &name)),
                    });
                    state.settle(job, target.guid(), error);
                }
                Some((kind, app, name))
            })
            .await;
        let Some((kind, app, name)) = ended.flatten() else {
            return;
        };
        let line = match (&failed, target, kind) {
            (None, Target::Instance(_), _) => {
                println!("SERVICES: {} done", doing(target, kind, &name));
                return;
            }
            (Some(e), Target::Instance(_), _) => {
                println!("SERVICES: {} failed: {e}", doing(target, kind, &name));
                return;
            }
            (None, Target::Binding(_), OperationKind::Delete) => format!(
                "Unbound service instance {name}: applies at the next start, restart or restage"
            ),
            (None, Target::Binding(_), _) => format!(
                "Bound service instance {name}: applies at the next start, restart or restage"
            ),
            (Some(e), Target::Binding(_), _) => {
                format!("{} failed: {e}", capitalized(&doing(target, kind, &name)))
            }
        };
        if let Some(app) = app {
            self.logs.add(&app, "API", line);
        }
    }
}

/// What the operation does, as the log says it.
fn doing(target: &Target, kind: OperationKind, name: &str) -> String {
    let doing = match (target, kind) {
        (Target::Instance(_), OperationKind::Create) => "creating",
        (Target::Instance(_), OperationKind::Update) => "updating",
        (Target::Instance(_), OperationKind::Delete) => "deleting",
        (Target::Binding(_), OperationKind::Delete) => "unbinding",
        (Target::Binding(_), _) => "binding",
    };
    format!("{doing} service instance {name}")
}

fn capitalized(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().chain(chars).collect()
    })
}

fn operation_mut<'a>(state: &'a mut State, target: &Target) -> Option<&'a mut Operation> {
    match target {
        Target::Instance(guid) => state.instance_mut(guid)?.operation.as_mut(),
        Target::Binding(guid) => state.binding_mut(guid)?.operation.as_mut(),
    }
}
