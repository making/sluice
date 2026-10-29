//! The services: the wicket's keyvalue stores and capabilities, offered as
//! the service offerings `keyvalue` (a plan per store) and `wasi` (a plan
//! per capability but keyvalue) of the broker `sluice-wicket`, which is no
//! process: the cf-api serves the offerings as the wicket lists them.
//!
//! A keyvalue instance is the namespace `cf/<instance guid>` of its plan's
//! store, opened by the apps bound to it at an identifier (the binding's
//! parameter `identifier`, default `""`); renaming the instance or moving
//! the app's routes leaves its data where it is. A wasi instance grants its
//! capability, a network one `cli` too. A binding applies at the app's next
//! start, restart or restage, as the rest of the sandbox; its credentials
//! are in `VCAP_SERVICES`. Instance and binding operations complete at
//! once. An instance bound is not deleted; a keyvalue one deleted has its
//! data deleted once no app maps it (see [`Controller::purge_pending`]).
//!
//! [`Controller::purge_pending`]: crate::controller::Controller::purge_pending

use std::collections::{BTreeMap, BTreeSet};

use http::{Method, StatusCode};
use serde_json::{Map, Value, json};

use crate::api::{Api, ApiError, Reply, Req, json as respond, no_content};
use crate::store::{Binding, Instance, Purge, State, guid, now};
use crate::views;
use crate::wicket::ListedStore;

/// The broker the offerings are listed under.
pub const BROKER: &str = "sluice-wicket";
pub const KEYVALUE: &str = "keyvalue";
pub const WASI: &str = "wasi";
const OFFERINGS: [&str; 2] = [KEYVALUE, WASI];
/// The capabilities of wasi:sockets: linked only with `cli`, they grant it
/// too.
const NETWORK: [&str; 4] = ["inherit-network", "allow-ip-name-lookup", "tcp", "udp"];
/// Longest instance name, binding name and keyvalue identifier.
const MAX_NAME: usize = 255;
/// When the broker, the offerings and the plans were made: they have no
/// history.
const EPOCH: &str = "2026-01-01T00:00:00Z";

/// What the wicket offers: a keyvalue plan per store, a wasi plan per
/// capability but `keyvalue` (which a keyvalue binding grants).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Offer {
    pub plans: Vec<Plan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub offering: String,
    pub name: String,
    pub description: String,
}

impl Offer {
    pub fn new(capabilities: &[String], stores: &[ListedStore]) -> Offer {
        let stores = stores.iter().map(|store| Plan {
            offering: KEYVALUE.into(),
            name: store.name.clone(),
            description: format!(
                "a namespace of the wicket's {} store '{}'{}",
                store.kind,
                store.name,
                if store.default { " (its default)" } else { "" }
            ),
        });
        let capabilities = capabilities
            .iter()
            .filter(|name| *name != KEYVALUE)
            .map(|name| Plan {
                offering: WASI.into(),
                name: name.clone(),
                description: describe(name),
            });
        Offer {
            plans: stores.chain(capabilities).collect(),
        }
    }
}

/// What a capability's plan grants.
fn describe(capability: &str) -> String {
    let what = match capability {
        "cli" => "the whole WASI 0.3 surface linked (cli, sockets, filesystem), no network",
        "inherit-network" => "sockets may reach every address, tcp and udp",
        "allow-ip-name-lookup" => "wasi:sockets/ip-name-lookup",
        "tcp" => "TCP sockets (every address stays denied without inherit-network)",
        "udp" => "UDP sockets (every address stays denied without inherit-network)",
        other => return format!("the wicket's capability '{other}'"),
    };
    if NETWORK.contains(&capability) {
        format!("{what}; grants cli too")
    } else {
        what.to_string()
    }
}

/// A guid of what has no history (the broker, an offering, a plan): the
/// same on every cf-api.
fn stable_guid(what: &str) -> String {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(format!("{BROKER}/{what}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes)
        .into_uuid()
        .to_string()
}

pub fn broker_guid() -> String {
    stable_guid("broker")
}

pub fn offering_guid(offering: &str) -> String {
    stable_guid(&format!("offering/{offering}"))
}

pub fn plan_guid(offering: &str, plan: &str) -> String {
    stable_guid(&format!("plan/{offering}/{plan}"))
}

/// The keyvalue namespace of the instance `guid`.
pub fn namespace(guid: &str) -> String {
    format!("{}/{guid}", crate::OWNER)
}

/// What the app's bindings grant its sandbox: the capabilities, and the
/// keyvalue map (identifier -> `store/namespace`).
pub fn grants(state: &State, app: &str) -> (Vec<String>, BTreeMap<String, String>) {
    let mut wasi = BTreeSet::new();
    let mut keyvalue = BTreeMap::new();
    for binding in state.bindings_of(app) {
        let Some(instance) = state.instance(&binding.instance) else {
            continue;
        };
        if instance.offering == KEYVALUE {
            wasi.insert(KEYVALUE.to_string());
            keyvalue.insert(
                binding.identifier.clone(),
                format!("{}/{}", instance.plan, namespace(&instance.guid)),
            );
        } else {
            wasi.extend(granted(&instance.plan));
        }
    }
    (wasi.into_iter().collect(), keyvalue)
}

/// The capabilities a wasi plan grants.
fn granted(capability: &str) -> Vec<String> {
    let mut granted = vec![capability.to_string()];
    if NETWORK.contains(&capability) {
        granted.push("cli".into());
    }
    granted
}

/// Why an app is not bound to an instance.
#[derive(Debug, PartialEq)]
pub enum Unbound {
    /// It is already.
    Already,
    Refused(String),
}

/// Binds the app to the instance as `parameters` (an object, or null) ask:
/// a keyvalue binding takes `identifier`, one an app binds once.
pub fn bind(
    state: &mut State,
    app: &str,
    instance: &str,
    name: Option<String>,
    parameters: &Value,
) -> Result<Binding, Unbound> {
    let Some(found) = state.instance(instance).cloned() else {
        return Err(Unbound::Refused(format!(
            "The service instance could not be found: {instance}"
        )));
    };
    if state
        .bindings_of(app)
        .any(|binding| binding.instance == instance)
    {
        return Err(Unbound::Already);
    }
    let identifier = identifier(&found, parameters).map_err(Unbound::Refused)?;
    if let Some(name) = &name {
        check_name("the binding name", name).map_err(Unbound::Refused)?;
        if state
            .bindings_of(app)
            .any(|binding| binding.name.as_ref() == Some(name))
        {
            return Err(Unbound::Refused(format!(
                "The binding name is invalid. App binding names must be unique. The app already has a binding with name '{name}'."
            )));
        }
    }
    if found.offering == KEYVALUE
        && let Some(taken) = state.bindings_of(app).find(|binding| {
            binding.identifier == identifier
                && state
                    .instance(&binding.instance)
                    .is_some_and(|other| other.offering == KEYVALUE)
        })
    {
        let app_name = state.app(app).map_or(app, |app| app.name.as_str());
        let other = state
            .instance(&taken.instance)
            .map_or("", |other| other.name.as_str());
        return Err(Unbound::Refused(format!(
            "{app_name} binds {other} at the keyvalue identifier \"{identifier}\" already: \
             bind {} at another (-c '{{\"identifier\":\"<name>\"}}')",
            found.name
        )));
    }
    let at = now();
    let binding = Binding {
        guid: guid(),
        name,
        app: app.to_string(),
        instance: instance.to_string(),
        identifier,
        created_at: at.clone(),
        updated_at: at,
    };
    state.bindings.push(binding.clone());
    Ok(binding)
}

/// The identifier a binding to `instance` opens the namespace by, from its
/// parameters: a keyvalue binding takes `identifier` (default ""), a wasi
/// one nothing.
fn identifier(instance: &Instance, parameters: &Value) -> Result<String, String> {
    let empty = Map::new();
    let given = match parameters {
        Value::Null => &empty,
        Value::Object(given) => given,
        _ => return Err("the parameters are a JSON object".into()),
    };
    if instance.offering != KEYVALUE {
        return match given.keys().next() {
            None => Ok(String::new()),
            Some(key) => Err(format!(
                "a {} binding takes no parameters (found '{key}')",
                instance.offering
            )),
        };
    }
    if let Some(key) = given.keys().find(|key| *key != "identifier") {
        return Err(format!(
            "a keyvalue binding takes the parameter 'identifier' only (found '{key}')"
        ));
    }
    match given.get("identifier") {
        None => Ok(String::new()),
        Some(Value::String(identifier)) if identifier.len() <= MAX_NAME => Ok(identifier.clone()),
        Some(Value::String(_)) => Err(format!("identifier: {MAX_NAME} bytes at most")),
        Some(_) => Err("identifier: a string".into()),
    }
}

fn check_name(what: &str, name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        Err(format!("{what} is empty"))
    } else if name.len() > MAX_NAME {
        Err(format!("{what} is {MAX_NAME} bytes at most"))
    } else {
        Ok(())
    }
}

/// What a binding gives the app: where its namespace lives, or the
/// capabilities granted.
fn credentials(instance: &Instance, binding: &Binding) -> Value {
    if instance.offering == KEYVALUE {
        json!({"store": instance.plan, "namespace": namespace(&instance.guid), "identifier": binding.identifier})
    } else {
        json!({"capabilities": granted(&instance.plan)})
    }
}

/// `VCAP_SERVICES` of the app: its bindings by offering, as CF has them.
pub fn vcap_services(state: &State, app: &str) -> Value {
    let mut services: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for binding in state.bindings_of(app) {
        let Some(instance) = state.instance(&binding.instance) else {
            continue;
        };
        let mut tags = vec![instance.offering.clone()];
        tags.extend(instance.tags.iter().cloned());
        services
            .entry(instance.offering.as_str())
            .or_default()
            .push(json!({
                "binding_guid": binding.guid,
                "binding_name": binding.name,
                "credentials": credentials(instance, binding),
                "instance_guid": instance.guid,
                "instance_name": instance.name,
                "label": instance.offering,
                "name": binding.name.as_ref().unwrap_or(&instance.name),
                "plan": instance.plan,
                "provider": null,
                "syslog_drain_url": null,
                "tags": tags,
                "volume_mounts": [],
            }));
    }
    json!(services)
}

/// The plans: the wicket's (available) and those of instances the wicket
/// offers no more (unavailable), so that every instance names a plan.
fn catalog(offer: &Offer, state: &State) -> Vec<(Plan, bool)> {
    let mut plans: Vec<(Plan, bool)> = offer
        .plans
        .iter()
        .map(|plan| (plan.clone(), true))
        .collect();
    for instance in &state.instances {
        if !plans
            .iter()
            .any(|(plan, _)| plan.offering == instance.offering && plan.name == instance.plan)
        {
            plans.push((
                Plan {
                    offering: instance.offering.clone(),
                    name: instance.plan.clone(),
                    description: "offered by the wicket no more".into(),
                },
                false,
            ));
        }
    }
    plans
}

fn metadata() -> Value {
    json!({"labels": {}, "annotations": {}})
}

fn broker_view(base: &str) -> Value {
    let guid = broker_guid();
    json!({
        "guid": guid, "created_at": EPOCH, "updated_at": EPOCH,
        "name": BROKER, "url": "",
        "relationships": {},
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_brokers/{guid}")},
            "service_offerings": {"href": format!("{base}/v3/service_offerings?service_broker_guids={guid}")},
        },
    })
}

fn offering_view(base: &str, offering: &str) -> Value {
    let guid = offering_guid(offering);
    let description = if offering == KEYVALUE {
        "wasi:keyvalue: a namespace of a keyvalue store of the wicket, opened at an identifier \
         (the binding's parameter 'identifier', default \"\")"
    } else {
        "a capability of the wicket's sandbox, granted to the apps bound"
    };
    json!({
        "guid": guid, "created_at": EPOCH, "updated_at": EPOCH,
        "name": offering, "description": description, "available": true,
        "tags": [offering], "requires": [], "shareable": false, "documentation_url": null,
        "broker_catalog": {
            "id": guid,
            "metadata": {"shareable": false},
            "features": {
                "plan_updateable": false, "bindable": true, "instances_retrievable": false,
                "bindings_retrievable": false, "allow_context_updates": false,
            },
        },
        "relationships": {"service_broker": {"data": {"guid": broker_guid()}}},
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_offerings/{guid}")},
            "service_plans": {"href": format!("{base}/v3/service_plans?service_offering_guids={guid}")},
            "service_broker": {"href": format!("{base}/v3/service_brokers/{}", broker_guid())},
        },
    })
}

fn plan_view(base: &str, plan: &Plan, available: bool) -> Value {
    let guid = plan_guid(&plan.offering, &plan.name);
    let offering = offering_guid(&plan.offering);
    let binding = if plan.offering == KEYVALUE {
        json!({
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {"identifier": {
                "type": "string",
                "description": "the identifier the app opens the namespace by (default \"\")",
            }},
            "additionalProperties": false,
        })
    } else {
        json!({})
    };
    json!({
        "guid": guid, "created_at": EPOCH, "updated_at": EPOCH,
        "name": plan.name, "description": plan.description,
        "visibility_type": "public", "available": available, "free": true, "costs": [],
        "maintenance_info": {},
        "broker_catalog": {
            "id": guid, "metadata": {}, "maximum_polling_duration": null,
            "features": {"bindable": true, "plan_updateable": false},
        },
        "schemas": {
            "service_instance": {"create": {"parameters": {}}, "update": {"parameters": {}}},
            "service_binding": {"create": {"parameters": binding}},
        },
        "relationships": {"service_offering": {"data": {"guid": offering}}},
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_plans/{guid}")},
            "service_offering": {"href": format!("{base}/v3/service_offerings/{offering}")},
            "visibility": {"href": format!("{base}/v3/service_plans/{guid}/visibility")},
        },
    })
}

fn instance_view(base: &str, state: &State, instance: &Instance) -> Value {
    let guid = &instance.guid;
    let plan = plan_guid(&instance.offering, &instance.plan);
    let operation = if instance.created_at == instance.updated_at {
        "create"
    } else {
        "update"
    };
    json!({
        "guid": guid, "created_at": instance.created_at, "updated_at": instance.updated_at,
        "name": instance.name, "type": "managed",
        "tags": instance.tags, "dashboard_url": null, "upgrade_available": false,
        "maintenance_info": {},
        "last_operation": {
            "type": operation, "state": "succeeded", "description": "",
            "created_at": instance.updated_at, "updated_at": instance.updated_at,
        },
        "relationships": {
            "space": {"data": {"guid": state.space.guid}},
            "service_plan": {"data": {"guid": plan}},
        },
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_instances/{guid}")},
            "space": {"href": format!("{base}/v3/spaces/{}", state.space.guid)},
            "service_plan": {"href": format!("{base}/v3/service_plans/{plan}")},
            "parameters": {"href": format!("{base}/v3/service_instances/{guid}/parameters")},
            "shared_spaces": {"href": format!("{base}/v3/service_instances/{guid}/relationships/shared_spaces")},
            "service_credential_bindings": {"href": format!("{base}/v3/service_credential_bindings?service_instance_guids={guid}")},
            "service_route_bindings": {"href": format!("{base}/v3/service_route_bindings?service_instance_guids={guid}")},
        },
    })
}

fn binding_view(base: &str, binding: &Binding) -> Value {
    let guid = &binding.guid;
    json!({
        "guid": guid, "created_at": binding.created_at, "updated_at": binding.updated_at,
        "name": binding.name, "type": "app",
        "last_operation": {
            "type": "create", "state": "succeeded", "description": "",
            "created_at": binding.created_at, "updated_at": binding.updated_at,
        },
        "relationships": {
            "app": {"data": {"guid": binding.app}},
            "service_instance": {"data": {"guid": binding.instance}},
        },
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_credential_bindings/{guid}")},
            "details": {"href": format!("{base}/v3/service_credential_bindings/{guid}/details")},
            "parameters": {"href": format!("{base}/v3/service_credential_bindings/{guid}/parameters")},
            "service_instance": {"href": format!("{base}/v3/service_instances/{}", binding.instance)},
            "app": {"href": format!("{base}/v3/apps/{}", binding.app)},
        },
    })
}

/// The values of the list filter `key` (`names=a,b`).
fn values(req: &Req, key: &str) -> Option<Vec<String>> {
    req.q(key)
        .map(|values| values.split(',').map(String::from).collect())
}

/// Whether the request asks for `resource` among the included: by its
/// `fields[<resource>]` or by `include`.
fn wants(req: &Req, resource: &str) -> bool {
    req.q(&format!("fields[{resource}]")).is_some()
        || req
            .q("include")
            .is_some_and(|include| include.split(',').any(|named| named == resource))
}

/// A page of `items` filtered as the request asks (`skip` aside), with the
/// resources `included` makes of the items filtered.
fn page(
    req: &Req,
    items: Vec<Value>,
    skip: &[&str],
    included: impl FnOnce(&[Value]) -> Map<String, Value>,
) -> Reply {
    let query: Vec<(String, String)> = req
        .query
        .iter()
        .filter(|(key, _)| !skip.contains(&key.as_str()))
        .cloned()
        .collect();
    let items = views::select(items, &query);
    let included = included(&items);
    Ok(respond(
        StatusCode::OK,
        &views::page(
            &req.base,
            &req.path,
            &req.query,
            items,
            (!included.is_empty()).then_some(Value::Object(included)),
        ),
    ))
}

/// The guids `pointer` names in `items`.
fn guids<'a>(items: &'a [Value], pointer: &str) -> BTreeSet<&'a str> {
    items
        .iter()
        .filter_map(|item| item.pointer(pointer).and_then(Value::as_str))
        .collect()
}

fn tags(value: &Value) -> Result<Vec<String>, ApiError> {
    let refused = || ApiError::unprocessable("Tags must be a list of strings");
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(tags) => tags
            .iter()
            .map(|tag| tag.as_str().map(String::from).ok_or_else(refused))
            .collect(),
        _ => Err(refused()),
    }
}

/// An instance takes no parameters: a keyvalue binding takes its identifier.
fn no_parameters(value: &Value) -> Result<(), ApiError> {
    match value {
        Value::Null => Ok(()),
        Value::Object(given) if given.is_empty() => Ok(()),
        _ => Err(ApiError::unprocessable(
            "a service instance takes no parameters: a keyvalue binding takes its identifier \
             (cf bind-service APP INSTANCE -c '{\"identifier\":\"<name>\"}')",
        )),
    }
}

fn name_taken(name: &str) -> ApiError {
    ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        code: 60002,
        title: "CF-ServiceInstanceNameTaken",
        detail: format!("The service instance name is taken: {name}."),
    }
}

impl Api {
    /// `/v3/service_*`.
    pub(crate) async fn services(&self, req: &Req, segments: &[&str]) -> Reply {
        let base = req.base.as_str();
        let offer = || async { self.controller.offer().await.unwrap_or_default() };
        match (&req.method, segments) {
            (&Method::GET, ["service_brokers"]) => page(req, vec![broker_view(base)], &[], |_| Map::new()),
            (&Method::GET, ["service_brokers", guid]) if *guid == broker_guid() => {
                Ok(respond(StatusCode::OK, &broker_view(base)))
            }
            (&Method::GET, ["service_brokers", _]) => Err(ApiError::not_found("Service broker")),
            (_, ["service_brokers", ..]) => Err(ApiError::unprocessable(
                "the broker is the wicket: its offerings follow the wicket's keyvalue stores and capabilities",
            )),
            (&Method::GET, ["service_offerings"]) => {
                let offer = offer().await;
                let brokers = values(req, "service_broker_names");
                let offered: BTreeSet<String> = self
                    .store
                    .read(|state| catalog(&offer, state))
                    .into_iter()
                    .map(|(plan, _)| plan.offering)
                    .collect();
                let items = OFFERINGS
                    .iter()
                    .filter(|offering| offered.contains(**offering))
                    .filter(|_| brokers.as_ref().is_none_or(|names| names.iter().any(|name| name == BROKER)))
                    .map(|offering| offering_view(base, offering))
                    .collect();
                page(req, items, &["space_guids", "organization_guids"], |_| {
                    let mut included = Map::new();
                    if wants(req, "service_broker") {
                        included.insert("service_brokers".into(), json!([broker_view(base)]));
                    }
                    included
                })
            }
            (&Method::GET, ["service_offerings", guid]) => OFFERINGS
                .iter()
                .find(|offering| offering_guid(offering) == *guid)
                .map(|offering| respond(StatusCode::OK, &offering_view(base, offering)))
                .ok_or_else(|| ApiError::not_found("Service offering")),
            (_, ["service_offerings", ..]) => Err(ApiError::unprocessable(
                "the offerings follow the wicket's keyvalue stores and capabilities",
            )),
            (&Method::GET, ["service_plans"]) => {
                let offer = offer().await;
                let offerings = values(req, "service_offering_names");
                let brokers = values(req, "service_broker_names");
                let items = self
                    .store
                    .read(|state| catalog(&offer, state))
                    .iter()
                    .filter(|(plan, _)| {
                        offerings.as_ref().is_none_or(|names| names.contains(&plan.offering))
                            && brokers.as_ref().is_none_or(|names| names.iter().any(|name| name == BROKER))
                    })
                    .map(|(plan, available)| plan_view(base, plan, *available))
                    .collect();
                page(req, items, &["space_guids", "organization_guids"], |items| {
                    let mut included = Map::new();
                    if wants(req, "service_offering") {
                        let named = guids(items, "/relationships/service_offering/data/guid");
                        let offerings: Vec<Value> = OFFERINGS
                            .iter()
                            .filter(|offering| named.contains(offering_guid(offering).as_str()))
                            .map(|offering| offering_view(base, offering))
                            .collect();
                        included.insert("service_offerings".into(), json!(offerings));
                    }
                    if wants(req, "service_offering.service_broker") {
                        included.insert("service_brokers".into(), json!([broker_view(base)]));
                    }
                    if wants(req, "space.organization") {
                        // no plan is of a space-scoped broker
                        included.insert("spaces".into(), json!([]));
                        included.insert("organizations".into(), json!([]));
                    }
                    included
                })
            }
            (&Method::GET, ["service_plans", guid]) => {
                let offer = offer().await;
                self.store
                    .read(|state| catalog(&offer, state))
                    .iter()
                    .find(|(plan, _)| plan_guid(&plan.offering, &plan.name) == *guid)
                    .map(|(plan, available)| respond(StatusCode::OK, &plan_view(base, plan, *available)))
                    .ok_or_else(|| ApiError::not_found("Service plan"))
            }
            (&Method::GET, ["service_plans", _, "visibility"]) => {
                Ok(respond(StatusCode::OK, &json!({"type": "public"})))
            }
            (_, ["service_plans", ..]) => Err(ApiError::unprocessable(
                "the plans follow the wicket's keyvalue stores and capabilities: each is public",
            )),
            (&Method::GET, ["service_instances"]) => {
                let offer = offer().await;
                self.store.read(|state| self.instances(req, state, &offer))
            }
            (&Method::POST, ["service_instances"]) => self.create_instance(req).await,
            (&Method::GET, ["service_instances", guid]) => self
                .store
                .read(|state| state.instance(guid).map(|instance| instance_view(base, state, instance)))
                .map(|view| respond(StatusCode::OK, &view))
                .ok_or_else(|| ApiError::not_found("Service instance")),
            (&Method::PATCH, ["service_instances", guid]) => self.update_instance(req, guid).await,
            (&Method::DELETE, ["service_instances", guid]) => self.delete_instance(req, guid).await,
            (&Method::GET, ["service_instances", guid, "parameters"]) => {
                self.store
                    .read(|state| state.instance(guid).map(|_| ()))
                    .ok_or_else(|| ApiError::not_found("Service instance"))?;
                Ok(respond(StatusCode::OK, &json!({})))
            }
            (&Method::GET, ["service_instances", guid, "relationships", "shared_spaces"]) => {
                self.store
                    .read(|state| state.instance(guid).map(|_| ()))
                    .ok_or_else(|| ApiError::not_found("Service instance"))?;
                Ok(respond(
                    StatusCode::OK,
                    &json!({"data": [], "links": {"self": {"href": format!("{base}/v3/service_instances/{guid}/relationships/shared_spaces")}}}),
                ))
            }
            (_, ["service_instances", _, "relationships", "shared_spaces", ..]) => Err(
                ApiError::unprocessable("sharing is not supported: the cf-api has one space"),
            ),
            (&Method::GET, ["service_credential_bindings"]) => {
                self.store.read(|state| bindings(req, state))
            }
            (&Method::POST, ["service_credential_bindings"]) => self.create_binding(req).await,
            (&Method::GET, ["service_credential_bindings", guid]) => self
                .store
                .read(|state| state.binding(guid).map(|binding| binding_view(base, binding)))
                .map(|view| respond(StatusCode::OK, &view))
                .ok_or_else(|| ApiError::not_found("Service credential binding")),
            (&Method::GET, ["service_credential_bindings", guid, what @ ("details" | "parameters")]) => self
                .store
                .read(|state| {
                    let binding = state.binding(guid)?;
                    let instance = state.instance(&binding.instance)?;
                    Some(if *what == "details" {
                        json!({"credentials": credentials(instance, binding), "syslog_drain_url": null, "volume_mounts": []})
                    } else if instance.offering == KEYVALUE {
                        json!({"identifier": binding.identifier})
                    } else {
                        json!({})
                    })
                })
                .map(|view| respond(StatusCode::OK, &view))
                .ok_or_else(|| ApiError::not_found("Service credential binding")),
            (&Method::DELETE, ["service_credential_bindings", guid]) => self.delete_binding(req, guid).await,
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        }
    }

    /// The instances, with the plans, offerings, broker, space and
    /// organization they name included as asked.
    fn instances(&self, req: &Req, state: &State, offer: &Offer) -> Reply {
        let base = req.base.as_str();
        let plans = values(req, "service_plan_names");
        let items = state
            .instances
            .iter()
            .filter(|instance| {
                plans
                    .as_ref()
                    .is_none_or(|names| names.contains(&instance.plan))
            })
            .map(|instance| instance_view(base, state, instance))
            .collect();
        page(req, items, &[], |items| {
            let named = guids(items, "/guid");
            let selected: Vec<&Instance> = state
                .instances
                .iter()
                .filter(|instance| named.contains(instance.guid.as_str()))
                .collect();
            let catalog = catalog(offer, state);
            let mut included = Map::new();
            if wants(req, "service_plan") {
                let plans: Vec<Value> = catalog
                    .iter()
                    .filter(|(plan, _)| {
                        selected.iter().any(|instance| {
                            instance.offering == plan.offering && instance.plan == plan.name
                        })
                    })
                    .map(|(plan, available)| plan_view(base, plan, *available))
                    .collect();
                included.insert("service_plans".into(), json!(plans));
            }
            if wants(req, "service_plan.service_offering") {
                let offerings: Vec<Value> = OFFERINGS
                    .iter()
                    .filter(|offering| {
                        selected
                            .iter()
                            .any(|instance| instance.offering == **offering)
                    })
                    .map(|offering| offering_view(base, offering))
                    .collect();
                included.insert("service_offerings".into(), json!(offerings));
            }
            if wants(req, "service_plan.service_offering.service_broker") {
                included.insert("service_brokers".into(), json!([broker_view(base)]));
            }
            if wants(req, "space") {
                included.insert("spaces".into(), json!([views::space(base, state)]));
            }
            if wants(req, "space.organization") {
                included.insert("organizations".into(), json!([views::org(base, state)]));
            }
            included
        })
    }

    async fn create_instance(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        if body["type"].as_str() != Some("managed") {
            return Err(ApiError::unprocessable(
                "user-provided service instances are not supported: the services are the wicket's \
                 keyvalue stores and capabilities (cf marketplace)",
            ));
        }
        let name = body["name"].as_str().unwrap_or_default().to_string();
        check_name("the name", &name).map_err(ApiError::unprocessable)?;
        let wanted = body["relationships"]["service_plan"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.service_plan names the plan"))?;
        no_parameters(&body["parameters"])?;
        let tags = tags(&body["tags"])?;
        let offer = self.controller.offer().await.map_err(|e| {
            ApiError::unprocessable(format!(
                "the plans are the wicket's, and it is unreachable: {e}"
            ))
        })?;
        let plan = offer
            .plans
            .iter()
            .find(|plan| plan_guid(&plan.offering, &plan.name) == wanted)
            .ok_or_else(|| {
                ApiError::unprocessable(
                    "Invalid service plan. Ensure that the service plan exists, is available, and you have access to it.",
                )
            })?;
        let _edit = self.controller.edits.lock().await;
        let instance = self.store.update(|state| {
            if state.instance_named(&name).is_some() {
                return Err(name_taken(&name));
            }
            let at = now();
            let instance = Instance {
                guid: guid(),
                name: name.clone(),
                offering: plan.offering.clone(),
                plan: plan.name.clone(),
                tags,
                created_at: at.clone(),
                updated_at: at,
            };
            state.instances.push(instance.clone());
            Ok(instance)
        })?;
        println!(
            "SERVICES: created service instance {} ({} {})",
            instance.name, instance.offering, instance.plan
        );
        self.job(
            req,
            "service_instance.create",
            Vec::new(),
            Some((
                "service_instances".into(),
                format!("/v3/service_instances/{}", instance.guid),
            )),
        )
    }

    /// A rename or new tags apply at once; the plan stays.
    async fn update_instance(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        no_parameters(&body["parameters"])?;
        if !body["maintenance_info"].is_null() {
            return Err(ApiError::unprocessable("no upgrade is available"));
        }
        let name = body["name"].as_str().map(String::from);
        if let Some(name) = &name {
            check_name("the name", name).map_err(ApiError::unprocessable)?;
        }
        let tags = (!body["tags"].is_null())
            .then(|| tags(&body["tags"]))
            .transpose()?;
        let plan = body["relationships"]["service_plan"]["data"]["guid"].as_str();
        let _edit = self.controller.edits.lock().await;
        let view = self.store.update(|state| {
            let current = state
                .instance(guid)
                .cloned()
                .ok_or_else(|| ApiError::not_found("Service instance"))?;
            if plan.is_some_and(|plan| plan != plan_guid(&current.offering, &current.plan)) {
                return Err(ApiError::unprocessable(
                    "a plan change is not supported: a keyvalue instance's data stays in its store, \
                     a wasi instance is its capability; create another instance",
                ));
            }
            if let Some(name) = &name
                && state
                    .instance_named(name)
                    .is_some_and(|other| other.guid != guid)
            {
                return Err(name_taken(name));
            }
            let instance = state
                .instances
                .iter_mut()
                .find(|instance| instance.guid == guid)
                .expect("found above");
            if let Some(name) = &name {
                instance.name = name.clone();
            }
            if let Some(tags) = &tags {
                instance.tags = tags.clone();
            }
            instance.updated_at = now();
            let instance = instance.clone();
            Ok(instance_view(&req.base, state, &instance))
        })?;
        Ok(respond(StatusCode::OK, &view))
    }

    /// Refused while bound (`purge=true` drops the bindings); a keyvalue
    /// instance's data is deleted once no app maps it.
    async fn delete_instance(&self, req: &Req, guid: &str) -> Reply {
        let purge = req.q("purge") == Some("true");
        let _edit = self.controller.edits.lock().await;
        let (instance, dropped) = self.store.update(|state| {
            let instance = state
                .instance(guid)
                .cloned()
                .ok_or_else(|| ApiError::not_found("Service instance"))?;
            let bound: Vec<Binding> = state
                .bindings
                .iter()
                .filter(|binding| binding.instance == guid)
                .cloned()
                .collect();
            if !bound.is_empty() && !purge {
                let apps: Vec<&str> = bound
                    .iter()
                    .map(|binding| state.app(&binding.app).map_or(binding.app.as_str(), |app| app.name.as_str()))
                    .collect();
                return Err(ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    code: 10006,
                    title: "CF-AssociationNotEmpty",
                    detail: format!(
                        "Please delete the service_bindings, service_keys, and routes associations for your service_instances. \
                         ({} is bound to {}: cf unbind-service)",
                        instance.name,
                        apps.join(", ")
                    ),
                });
            }
            state.bindings.retain(|binding| binding.instance != guid);
            state.instances.retain(|kept| kept.guid != guid);
            if instance.offering == KEYVALUE {
                state.purges.push(Purge {
                    store: instance.plan.clone(),
                    namespace: namespace(guid),
                    instance: instance.name.clone(),
                });
            }
            Ok((instance, bound))
        })?;
        for binding in &dropped {
            self.logs.add(
                &binding.app,
                "API",
                format!(
                    "Unbound service instance {} (purged): applies at the next start, restart or restage",
                    instance.name
                ),
            );
        }
        println!("SERVICES: deleted service instance {}", instance.name);
        self.controller.purge_pending().await;
        if purge {
            return Ok(no_content());
        }
        self.job(req, "service_instance.delete", Vec::new(), None)
    }

    async fn create_binding(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        if body["type"].as_str() != Some("app") {
            return Err(ApiError::unprocessable(
                "service keys are not supported: an app binds an instance (cf bind-service)",
            ));
        }
        if body["strategy"]
            .as_str()
            .is_some_and(|strategy| strategy != "single")
        {
            return Err(ApiError::unprocessable(
                "a binding strategy but single is not supported: an app binds an instance once",
            ));
        }
        let instance = body["relationships"]["service_instance"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| {
                ApiError::unprocessable("relationships.service_instance names the instance")
            })?;
        let app = body["relationships"]["app"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.app names the app"))?;
        let name = body["name"].as_str().map(String::from);
        let _edit = self.controller.edits.lock().await;
        let (binding, instance_name) = self.store.update(|state| {
            if state.app(app).is_none() {
                return Err(ApiError::unprocessable(format!(
                    "The app could not be found: {app}"
                )));
            }
            let instance_name = state
                .instance(instance)
                .map(|instance| instance.name.clone())
                .ok_or_else(|| {
                    ApiError::unprocessable(format!(
                        "The service instance could not be found: {instance}"
                    ))
                })?;
            match bind(state, app, instance, name.clone(), &body["parameters"]) {
                Ok(binding) => Ok((binding, instance_name)),
                Err(Unbound::Already) => Err(ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    code: 90003,
                    title: "CF-ServiceBindingAppServiceTaken",
                    detail: "The app is already bound to the service instance.".into(),
                }),
                Err(Unbound::Refused(e)) => Err(ApiError::unprocessable(e)),
            }
        })?;
        self.logs.add(
            app,
            "API",
            format!(
                "Bound service instance {instance_name}: applies at the next start, restart or restage"
            ),
        );
        self.job(
            req,
            "service_bindings.create",
            Vec::new(),
            Some((
                "service_credential_binding".into(),
                format!("/v3/service_credential_bindings/{}", binding.guid),
            )),
        )
    }

    async fn delete_binding(&self, req: &Req, guid: &str) -> Reply {
        let _edit = self.controller.edits.lock().await;
        let (binding, instance_name) = self.store.update(|state| {
            let binding = state
                .binding(guid)
                .cloned()
                .ok_or_else(|| ApiError::not_found("Service credential binding"))?;
            let name = state
                .instance(&binding.instance)
                .map_or_else(String::new, |instance| instance.name.clone());
            state.bindings.retain(|kept| kept.guid != guid);
            Ok::<_, ApiError>((binding, name))
        })?;
        self.logs.add(
            &binding.app,
            "API",
            format!(
                "Unbound service instance {instance_name}: applies at the next start, restart or restage"
            ),
        );
        self.job(req, "service_bindings.delete", Vec::new(), None)
    }
}

/// The bindings, with the apps and instances they name included as asked.
fn bindings(req: &Req, state: &State) -> Reply {
    let base = req.base.as_str();
    let items = state
        .bindings
        .iter()
        .map(|binding| binding_view(base, binding))
        .collect();
    page(req, items, &[], |items| {
        let mut included = Map::new();
        if wants(req, "app") {
            let named = guids(items, "/relationships/app/data/guid");
            let apps: Vec<Value> = state
                .apps
                .iter()
                .filter(|app| named.contains(app.guid.as_str()))
                .map(|app| views::app(base, state, app))
                .collect();
            included.insert("apps".into(), json!(apps));
        }
        if wants(req, "service_instance") {
            let named = guids(items, "/relationships/service_instance/data/guid");
            let instances: Vec<Value> = state
                .instances
                .iter()
                .filter(|instance| named.contains(instance.guid.as_str()))
                .map(|instance| instance_view(base, state, instance))
                .collect();
            included.insert("service_instances".into(), json!(instances));
        }
        included
    })
}
