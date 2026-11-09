//! The services: the wicket's keyvalue stores and capabilities, offered as
//! the service offerings `keyvalue` (a plan per store) and `wasi` (a plan
//! per capability but keyvalue) of the broker `sluice-wicket`, which is no
//! process: the cf-api serves the offerings as the wicket lists them.
//!
//! A keyvalue instance is the namespace `cf/<instance guid>` of its plan's
//! store, opened by the apps bound to it at an identifier (the binding's
//! parameter `identifier`, default `""`); renaming the instance or moving
//! the app's routes leaves its data where it is. A store of each wicket's
//! host (`sqlite`) holds an instance's data on one wicket, its home, where
//! the apps mapping it run (`crate::placement`): an app is not bound to two
//! instances of two homes. A store the wickets share is one by its identity. A wasi instance grants its
//! capability, a network one `cli` too. A binding applies at the app's next
//! start, restart or restage, as the rest of the sandbox; its credentials
//! are in `VCAP_SERVICES`. Instance and binding operations complete at
//! once. An instance bound is not deleted; a keyvalue one deleted has its
//! data deleted once no app maps it (see [`Controller::purge_pending`]).
//!
//! A user-provided instance is the user's credentials (`cf
//! create-user-provided-service -p`), given the apps bound in
//! `VCAP_SERVICES` and nothing more: it grants no capability, on either
//! wicket. As on CF, its operations answer at once (no job), and new
//! credentials apply at the next start, as a binding.
//!
//! A registered broker's instance (`crate::brokers`) is the broker's: its
//! create, update and delete, and its bindings' create and delete, are the
//! broker's operations (`crate::operations`), asynchronous as the broker
//! makes them: the job is PROCESSING until the broker answers, POLLING while
//! it is polled, and the instance or binding shows the last operation. One
//! operation at a time: another asked meanwhile is refused (409). A binding's
//! credentials are in `VCAP_SERVICES` once it is made, from the app's next
//! start; a manifest's `services:` waits for its bindings to be made, an
//! app's delete for its bindings to be unbound.
//!
//! [`Controller::purge_pending`]: crate::controller::Controller::purge_pending

use std::collections::{BTreeMap, BTreeSet};

use http::{Method, StatusCode};
use serde_json::{Map, Value, json};

use crate::api::{Api, ApiError, Reply, Req, json as respond, no_content, space_to_develop};
use crate::brokers;
use crate::roles::{Access, Need};
use crate::store::{
    Binding, Instance, Operation, OperationKind, OperationState, Purge, State, guid, now,
};
use crate::views;
use crate::wicket::ListedStore;

/// The broker the offerings are listed under.
pub const BROKER: &str = "sluice-wicket";
pub const KEYVALUE: &str = "keyvalue";
pub const WASI: &str = "wasi";
/// The offering of the user's instances: no broker's, no plan.
pub const USER_PROVIDED: &str = "user-provided";
const OFFERINGS: [&str; 2] = [KEYVALUE, WASI];
/// The capabilities of wasi:sockets: linked only with `cli`, they grant it
/// too.
const NETWORK: [&str; 4] = ["inherit-network", "allow-ip-name-lookup", "tcp", "udp"];
/// Longest instance name, binding name and keyvalue identifier.
const MAX_NAME: usize = 255;
/// The most a user-provided instance's credentials hold, as JSON: as the
/// user's variables, half of an env the wicket takes; a start checks the
/// whole env ([`crate::env::check_guest`]).
const MAX_CREDENTIALS: usize = crate::env::MAX_BYTES;
/// When the broker, the offerings and the plans were made: they have no
/// history.
const EPOCH: &str = "2026-01-01T00:00:00Z";
/// What the CLI reads as a broker's instance or binding giving away none of
/// its parameters.
const NO_INSTANCE_PARAMETERS: &str =
    "This service does not support fetching service instance parameters.";
const NO_BINDING_PARAMETERS: &str =
    "This service does not support fetching service binding parameters.";

/// What a wicket offers: a keyvalue plan per store, a wasi plan per
/// capability but `keyvalue` (which a keyvalue binding grants); of several
/// wickets, what every one does ([`Offer::common`]): a store of each one's
/// host by its name, a shared one by its name and identity.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Offer {
    pub plans: Vec<Plan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub offering: String,
    pub name: String,
    pub description: String,
    /// A keyvalue store's identity when the wickets of other hosts reach it too.
    pub shared: Option<String>,
}

impl Offer {
    pub fn new(capabilities: &[String], stores: &[ListedStore]) -> Offer {
        let stores = stores.iter().map(|store| Plan {
            offering: KEYVALUE.into(),
            name: store.name.clone(),
            description: format!(
                "a namespace of the {} store '{}'{}, {}",
                store.kind,
                store.name,
                if store.default { " (the default)" } else { "" },
                match store.shared {
                    Some(_) => "shared by the wickets",
                    None => "each wicket's host's own: the apps bound run on one wicket",
                }
            ),
            shared: store.shared.clone(),
        });
        let capabilities = capabilities
            .iter()
            .filter(|name| *name != KEYVALUE)
            .map(|name| Plan {
                offering: WASI.into(),
                name: name.clone(),
                description: describe(name),
                shared: None,
            });
        Offer {
            plans: stores.chain(capabilities).collect(),
        }
    }

    /// What every one of `offers` offers, as the first describes it: an app may run on any of
    /// them. None of no offer.
    pub fn common(offers: &[&Offer]) -> Option<Offer> {
        let (first, others) = offers.split_first()?;
        let offered = |offer: &Offer, plan: &Plan| {
            (offer.plans.iter()).any(|kept| {
                kept.offering == plan.offering
                    && kept.name == plan.name
                    && kept.shared == plan.shared
            })
        };
        let plans = first.plans.iter();
        Some(Offer {
            plans: plans
                .filter(|plan| others.iter().all(|offer| offered(offer, plan)))
                .cloned()
                .collect(),
        })
    }

    /// Each keyvalue store `offers` list, by name: whether it is one store of them all (each
    /// listing it shared, of one identity), else each wicket's host's own.
    pub fn stores(offers: &[&Offer]) -> BTreeMap<String, bool> {
        let mut identities: BTreeMap<&str, BTreeSet<Option<&str>>> = BTreeMap::new();
        let plans = offers.iter().flat_map(|offer| offer.plans.iter());
        for plan in plans.filter(|plan| plan.offering == KEYVALUE) {
            let identity = plan.shared.as_deref();
            identities.entry(&plan.name).or_default().insert(identity);
        }
        identities
            .into_iter()
            .map(|(name, identities)| {
                let one = identities.len() == 1 && !identities.contains(&None);
                (name.to_string(), one)
            })
            .collect()
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

/// The guid of the instance's plan: the wicket's, or a broker's.
fn plan_of(instance: &Instance) -> String {
    match instance.brokered() {
        true => instance.plan.clone(),
        false => plan_guid(&instance.offering, &instance.plan),
    }
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
        match instance.offering.as_str() {
            KEYVALUE => {
                wasi.insert(KEYVALUE.to_string());
                keyvalue.insert(
                    binding.identifier.clone(),
                    format!("{}/{}", instance.plan, namespace(&instance.guid)),
                );
            }
            WASI => wasi.extend(granted(&instance.plan)),
            _ => {}
        }
    }
    (wasi.into_iter().collect(), keyvalue)
}

/// The home of `instance`'s data, a namespace of a store of each wicket's host
/// (`crate::placement`), when an instance `app` binds already has its data on another wicket:
/// that one's name, the home and its wicket.
fn elsewhere<'a>(
    state: &'a State,
    app: &str,
    instance: &Instance,
) -> Option<(&'a str, &'a str, &'a str)> {
    let home = |instance: &Instance| {
        let local = instance.offering == KEYVALUE
            && state.wickets.stores.get(&instance.plan) == Some(&false);
        let place = format!("{}/{}", instance.plan, namespace(&instance.guid));
        local.then(|| state.homes.get(&place)).flatten()
    };
    let its = home(instance)?;
    let bound = state.bindings_of(app);
    let others = bound.filter_map(|binding| state.instance(&binding.instance));
    others
        .filter_map(|other| Some((other, home(other)?)))
        .find(|(_, home)| *home != its)
        .map(|(other, home)| (other.name.as_str(), its.as_str(), home.as_str()))
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
    /// An operation of the broker on the instance is under way.
    Busy(String),
    Refused(String),
}

/// Binds the app to the instance as `parameters` (an object, or null) ask:
/// a keyvalue binding takes `identifier`, one an app binds once; a broker's
/// binding takes what its broker does, made by the broker
/// (`crate::operations`): its operation is under way, its job none yet.
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
    let operation = match found.brokered() {
        true => Some(brokered_binding(state, &found, parameters)?),
        false => None,
    };
    let identifier = match found.brokered() {
        true => String::new(),
        false => identifier(&found, parameters).map_err(Unbound::Refused)?,
    };
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
    if let Some((other, home, its)) = elsewhere(state, app, &found) {
        let app_name = state.app(app).map_or(app, |app| app.name.as_str());
        return Err(Unbound::Refused(format!(
            "{app_name} binds {other}, whose data is on the wicket {its}: the data of {} is on \
             the wicket {home}, and an app runs on one wicket of the stores of each wicket's \
             host ({})",
            found.name, found.plan
        )));
    }
    let at = now();
    let binding = Binding {
        guid: guid(),
        name,
        app: app.to_string(),
        instance: instance.to_string(),
        identifier,
        credentials: Map::new(),
        operation,
        created_at: at.clone(),
        updated_at: at,
    };
    state.bindings.push(binding.clone());
    Ok(binding)
}

/// The operation of a new binding to a broker's instance, the instance
/// ready and its plan bindable; the parameters to send.
fn brokered_binding(
    state: &State,
    instance: &Instance,
    parameters: &Value,
) -> Result<Operation, Unbound> {
    if instance.busy() {
        return Err(Unbound::Busy(in_progress(&instance.name)));
    }
    if failed_create(instance) {
        return Err(Unbound::Refused(format!(
            "The service instance {} could not be created: delete it (cf delete-service)",
            instance.name
        )));
    }
    let brokered = state.brokered_of(instance).ok_or_else(|| {
        Unbound::Refused(format!(
            "the plan of service instance {} is gone",
            instance.name
        ))
    })?;
    if !brokered.plan.feature(brokered.offering, "bindable") {
        return Err(Unbound::Refused(
            "Service instance is not bindable: its plan does not allow bindings.".into(),
        ));
    }
    let mut operation = Operation::new(OperationKind::Create, None, None);
    operation.parameters = object_parameters(parameters).map_err(Unbound::Refused)?;
    Ok(operation)
}

/// A broker's parameters: an object, or none.
fn object_parameters(parameters: &Value) -> Result<Option<Value>, String> {
    match parameters {
        Value::Null => Ok(None),
        Value::Object(_) => Ok(Some(parameters.clone())),
        _ => Err("the parameters are a JSON object".into()),
    }
}

/// Whether a broker's instance was never made.
fn failed_create(instance: &Instance) -> bool {
    instance.operation.as_ref().is_some_and(|operation| {
        operation.kind == OperationKind::Create && operation.state == OperationState::Failed
    })
}

fn in_progress(instance: &str) -> String {
    format!("An operation for service instance {instance} is in progress.")
}

/// 409, as CC refuses an operation while another is under way.
fn busy(detail: String) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: 60016,
        title: "CF-AsyncServiceInstanceOperationInProgress",
        detail,
    }
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
    if instance.user_provided() && !given.is_empty() {
        return Err(
            "Binding parameters are not supported for user-provided service instances".into(),
        );
    }
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

/// What a binding gives the app: where its namespace lives, the
/// capabilities granted, the user's credentials, or the broker's.
fn credentials(instance: &Instance, binding: &Binding) -> Value {
    if instance.brokered() {
        return Value::Object(binding.credentials.clone());
    }
    match instance.offering.as_str() {
        KEYVALUE => {
            json!({"store": instance.plan, "namespace": namespace(&instance.guid), "identifier": binding.identifier})
        }
        USER_PROVIDED => Value::Object(instance.credentials.clone()),
        _ => json!({"capabilities": granted(&instance.plan)}),
    }
}

/// `VCAP_SERVICES` of the app: its bindings made, by offering, as CF has
/// them.
pub fn vcap_services(state: &State, app: &str) -> Value {
    let mut services: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for binding in state.bindings_of(app).filter(|binding| binding.made()) {
        let Some(instance) = state.instance(&binding.instance) else {
            continue;
        };
        // a user-provided instance's tags are its own, and it has no plan; a broker's
        // offering's tags come first
        let (label, plan, mut tags) = match state.brokered_of(instance) {
            Some(brokered) => (
                brokered.offering.name().to_string(),
                Some(brokered.plan.name().to_string()),
                brokered.offering.catalog["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|tag| tag.as_str().map(String::from))
                    .collect(),
            ),
            None if instance.user_provided() => (instance.offering.clone(), None, Vec::new()),
            None => (
                instance.offering.clone(),
                Some(instance.plan.clone()),
                vec![instance.offering.clone()],
            ),
        };
        tags.extend(instance.tags.iter().cloned());
        let mut entry = json!({
            "binding_guid": binding.guid,
            "binding_name": binding.name,
            "credentials": credentials(instance, binding),
            "instance_guid": instance.guid,
            "instance_name": instance.name,
            "label": label,
            "name": binding.name.as_ref().unwrap_or(&instance.name),
            "syslog_drain_url": null,
            "tags": tags,
            "volume_mounts": [],
        });
        if let Some(plan) = plan {
            entry["plan"] = json!(plan);
            entry["provider"] = Value::Null;
        }
        services.entry(label).or_default().push(entry);
    }
    json!(services)
}

/// The wicket's plans (available) and those of instances the wicket offers
/// no more (unavailable), so that every instance names a plan.
fn wicket_plans(offer: &Offer, state: &State) -> Vec<(Plan, bool)> {
    let mut plans: Vec<(Plan, bool)> = offer
        .plans
        .iter()
        .map(|plan| (plan.clone(), true))
        .collect();
    for instance in state
        .instances
        .iter()
        .filter(|instance| !instance.user_provided() && !instance.brokered())
    {
        if !plans
            .iter()
            .any(|(plan, _)| plan.offering == instance.offering && plan.name == instance.plan)
        {
            plans.push((
                Plan {
                    offering: instance.offering.clone(),
                    name: instance.plan.clone(),
                    description: "offered by the wicket no more".into(),
                    shared: None,
                },
                false,
            ));
        }
    }
    plans
}

/// Every broker, offering and plan as CC shows them: the wicket's and the
/// registered brokers'.
struct Catalog {
    brokers: Vec<Value>,
    offerings: Vec<Value>,
    plans: Vec<Value>,
}

fn catalog(base: &str, offer: &Offer, state: &State) -> Catalog {
    let wicket = wicket_plans(offer, state);
    let mut brokers = vec![broker_view(base)];
    let mut offerings: Vec<Value> = OFFERINGS
        .iter()
        .filter(|offering| wicket.iter().any(|(plan, _)| plan.offering == **offering))
        .map(|offering| offering_view(base, offering))
        .collect();
    let mut plans: Vec<Value> = wicket
        .iter()
        .map(|(plan, available)| plan_view(base, plan, *available))
        .collect();
    for broker in &state.brokers {
        brokers.push(brokers::view(base, broker));
        for offering in &broker.offerings {
            offerings.push(brokers::offering_view(base, broker, offering));
            for plan in &offering.plans {
                plans.push(brokers::plan_view(base, broker, offering, plan));
            }
        }
    }
    Catalog {
        brokers,
        offerings,
        plans,
    }
}

/// The guid of the resource `item` names as `relation`.
fn related<'a>(item: &'a Value, relation: &str) -> Option<&'a str> {
    item["relationships"][relation]["data"]["guid"].as_str()
}

fn by_guid<'a>(items: &'a [Value], guid: &str) -> Option<&'a Value> {
    items.iter().find(|item| item["guid"] == guid)
}

impl Catalog {
    fn offering_of(&self, plan: &Value) -> Option<&Value> {
        by_guid(&self.offerings, related(plan, "service_offering")?)
    }

    fn broker_of(&self, offering: &Value) -> Option<&Value> {
        by_guid(&self.brokers, related(offering, "service_broker")?)
    }

    /// Those of `items` that `of` relates them to, once each, in order.
    fn of<'a>(items: &'a [Value], named: impl Iterator<Item = Option<&'a Value>>) -> Vec<Value> {
        let named: BTreeSet<&str> = named
            .flatten()
            .filter_map(|item| item["guid"].as_str())
            .collect();
        items
            .iter()
            .filter(|item| {
                item["guid"]
                    .as_str()
                    .is_some_and(|guid| named.contains(guid))
            })
            .cloned()
            .collect()
    }
}

/// Whether the holder of `access` sees the plan in the spaces and organizations the request
/// filters by (`space_guids`, `organization_guids`), of those it sees: a public plan
/// anywhere, an organization's in that organization, a space-scoped broker's in its space.
/// Unfiltered, an admin sees every plan; filtered, none that is no one's (`admin`).
fn seen(req: &Req, state: &State, access: &Access, plan: &Value) -> bool {
    let wanted = |key: &str, guid: &str| {
        values(req, key).is_none_or(|guids| guids.iter().any(|g| g == guid))
    };
    let filtered = req.q("space_guids").is_some() || req.q("organization_guids").is_some();
    if access.admin && !filtered {
        return true;
    }
    let spaces: Vec<&crate::store::Space> = state
        .spaces
        .iter()
        .filter(|space| {
            wanted("space_guids", &space.guid)
                && wanted("organization_guids", &space.org)
                && access.can(state, &space.guid, Need::Read)
        })
        .collect();
    let orgs: Vec<&str> = state
        .orgs
        .iter()
        .map(|org| org.guid.as_str())
        .filter(|org| {
            wanted("organization_guids", org)
                && access.sees_org(state, org)
                && (req.q("space_guids").is_none() || spaces.iter().any(|space| space.org == *org))
        })
        .collect();
    let guid = plan["guid"].as_str().unwrap_or_default();
    match state.brokered(guid) {
        // the wicket's: public
        None => !filtered || !orgs.is_empty(),
        Some(brokered) => match brokered.plan.visibility.as_str() {
            _ if brokered.plan.gone => false,
            "public" => !filtered || !orgs.is_empty(),
            "organization" => orgs
                .iter()
                .any(|org| brokered.plan.orgs.iter().any(|o| o == org)),
            "space" => spaces
                .iter()
                .any(|space| brokered.broker.space.as_deref() == Some(space.guid.as_str())),
            _ => false,
        },
    }
}

/// Whether the name of `item` is among those the request filters by `key`.
fn named(req: &Req, key: &str, item: Option<&Value>) -> bool {
    values(req, key).is_none_or(|names| {
        item.and_then(|item| item["name"].as_str())
            .is_some_and(|name| names.iter().any(|wanted| wanted == name))
    })
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

/// An operation as CC shows the last one.
fn operation_view(operation: &Operation) -> Value {
    json!({
        "type": operation.kind.as_str(), "state": operation.state.as_str(),
        "description": operation.description,
        "created_at": operation.created_at, "updated_at": operation.updated_at,
    })
}

fn instance_view(base: &str, state: &State, instance: &Instance) -> Value {
    let guid = &instance.guid;
    let plan = plan_of(instance);
    let last_operation = match &instance.operation {
        Some(operation) => operation_view(operation),
        None => {
            let kind = if instance.created_at == instance.updated_at {
                "create"
            } else {
                "update"
            };
            json!({
                "type": kind, "state": "succeeded", "description": "",
                "created_at": instance.updated_at, "updated_at": instance.updated_at,
            })
        }
    };
    if instance.user_provided() {
        return json!({
            "guid": guid, "created_at": instance.created_at, "updated_at": instance.updated_at,
            "name": instance.name, "type": USER_PROVIDED, "tags": instance.tags,
            "syslog_drain_url": null, "route_service_url": null,
            "last_operation": last_operation,
            "relationships": {"space": {"data": {"guid": instance.space}}},
            "metadata": metadata(),
            "links": {
                "self": {"href": format!("{base}/v3/service_instances/{guid}")},
                "space": {"href": format!("{base}/v3/spaces/{}", instance.space)},
                "credentials": {"href": format!("{base}/v3/service_instances/{guid}/credentials")},
                "service_credential_bindings": {"href": format!("{base}/v3/service_credential_bindings?service_instance_guids={guid}")},
                "service_route_bindings": {"href": format!("{base}/v3/service_route_bindings?service_instance_guids={guid}")},
            },
        });
    }
    // an upgrade: the plan's maintenance info newer than the instance's
    let offered = state
        .brokered_of(instance)
        .and_then(|brokered| brokered.plan.maintenance_info())
        .and_then(|info| info.get("version"));
    let upgrade_available = offered.is_some_and(|version| {
        instance
            .maintenance_info
            .as_ref()
            .and_then(|info| info.get("version"))
            != Some(version)
    });
    json!({
        "guid": guid, "created_at": instance.created_at, "updated_at": instance.updated_at,
        "name": instance.name, "type": "managed",
        "tags": instance.tags, "dashboard_url": instance.dashboard_url,
        "upgrade_available": upgrade_available,
        "maintenance_info": instance.maintenance_info.clone().unwrap_or_else(|| json!({})),
        "last_operation": last_operation,
        "relationships": {
            "space": {"data": {"guid": instance.space}},
            "service_plan": {"data": {"guid": plan}},
        },
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_instances/{guid}")},
            "space": {"href": format!("{base}/v3/spaces/{}", instance.space)},
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
    let last_operation = match &binding.operation {
        Some(operation) => operation_view(operation),
        None => json!({
            "type": "create", "state": "succeeded", "description": "",
            "created_at": binding.created_at, "updated_at": binding.updated_at,
        }),
    };
    json!({
        "guid": guid, "created_at": binding.created_at, "updated_at": binding.updated_at,
        "name": binding.name, "type": "app",
        "last_operation": last_operation,
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

/// A wicket's instance takes no parameters: a keyvalue binding takes its
/// identifier.
fn no_parameters(value: &Value) -> Result<(), ApiError> {
    match value {
        Value::Null => Ok(()),
        Value::Object(given) if given.is_empty() => Ok(()),
        _ => Err(ApiError::unprocessable(
            "a service instance of the wicket takes no parameters: a keyvalue binding takes its \
             identifier (cf bind-service APP INSTANCE -c '{\"identifier\":\"<name>\"}')",
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

fn invalid_plan() -> ApiError {
    ApiError::unprocessable(
        "Invalid service plan. Ensure that the service plan exists, is available, and you have access to it.",
    )
}

/// Refuses what a user-provided instance cannot have here: a syslog drain
/// (no drain gets the apps' logs) and a route service (none is), its
/// parameters (it has credentials), a plan.
fn user_provided_only(body: &Value) -> Result<(), ApiError> {
    let set = |key: &str| body[key].as_str().is_some_and(|url| !url.is_empty());
    if set("syslog_drain_url") {
        return Err(ApiError::unprocessable(
            "a syslog drain is not supported: cf logs reads the apps' logs",
        ));
    }
    if set("route_service_url") {
        return Err(ApiError::unprocessable("route services are not supported"));
    }
    if !body["parameters"].is_null() {
        return Err(ApiError::unprocessable(
            "a user-provided service instance takes credentials, not parameters (-p)",
        ));
    }
    if !body["relationships"]["service_plan"].is_null() {
        return Err(ApiError::unprocessable(
            "a user-provided service instance has no plan",
        ));
    }
    Ok(())
}

/// A user-provided instance's credentials as given: a JSON object of
/// [`MAX_CREDENTIALS`] at most, or none (null, absent). An error holds no
/// value.
fn user_credentials(value: &Value) -> Result<Option<Map<String, Value>>, ApiError> {
    match value {
        Value::Null => Ok(None),
        Value::Object(credentials) => {
            let bytes = value.to_string().len();
            if bytes > MAX_CREDENTIALS {
                return Err(ApiError::unprocessable(format!(
                    "the credentials hold {bytes} bytes of JSON, more than {} KiB",
                    MAX_CREDENTIALS >> 10
                )));
            }
            Ok(Some(credentials.clone()))
        }
        _ => Err(ApiError::unprocessable("the credentials are a JSON object")),
    }
}

/// What an update of a broker's instance asks of the broker.
struct BrokeredUpdate {
    plan: Option<String>,
    maintenance_info: Option<Value>,
    parameters: Option<Value>,
}

impl Api {
    /// The instance `guid`, if the request's holder may `need` of its space.
    fn instance_as(&self, req: &Req, guid: &str, need: Need) -> Result<Instance, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let instance = state
                .instance(guid)
                .ok_or_else(|| ApiError::not_found("Service instance"))?;
            access.check(state, &instance.space, need, "Service instance")?;
            Ok(instance.clone())
        })
    }

    /// The binding `guid`, if the request's holder may `need` of its app's space.
    fn binding_as(&self, req: &Req, guid: &str, need: Need) -> Result<Binding, ApiError> {
        let access = req.access()?;
        self.store.read(|state| {
            let what = "Service credential binding";
            let binding = state
                .binding(guid)
                .ok_or_else(|| ApiError::not_found(what))?;
            let space = state.app(&binding.app).map_or("", |app| app.space.as_str());
            access.check(state, space, need, what)?;
            Ok(binding.clone())
        })
    }

    /// `/v3/service_*`.
    pub(crate) async fn services(&self, req: &Req, segments: &[&str]) -> Reply {
        let base = req.base.as_str();
        let access = req.access()?;
        let offer = || async { self.controller.offer().await.unwrap_or_default() };
        match (&req.method, segments) {
            (&Method::GET, ["service_brokers"]) => {
                let brokers = self
                    .store
                    .read(|state| catalog(base, &Offer::default(), state).brokers);
                page(req, brokers, &[], |_| Map::new())
            }
            (&Method::GET, ["service_brokers", guid]) if *guid == broker_guid() => {
                Ok(respond(StatusCode::OK, &broker_view(base)))
            }
            (_, ["service_brokers", guid, ..]) if *guid == broker_guid() => {
                Err(ApiError::unprocessable(
                    "the broker sluice-wicket is the wicket: its offerings follow the wicket's keyvalue stores and capabilities",
                ))
            }
            (_, ["service_brokers", ..]) => match self.registered_brokers(req, segments).await {
                Some(reply) => reply,
                None if req.method == Method::GET => Err(ApiError::not_found("Service broker")),
                None => Err(ApiError::unknown(&req.method, &req.path)),
            },
            (&Method::GET, ["service_offerings"]) => {
                let offer = offer().await;
                self.store.read(|state| {
                    let catalog = catalog(base, &offer, state);
                    let items: Vec<Value> = catalog
                        .offerings
                        .iter()
                        .filter(|offering| {
                            named(req, "service_broker_names", catalog.broker_of(offering))
                        })
                        .filter(|offering| {
                            let plans = catalog.plans.iter().filter(|plan| {
                                related(plan, "service_offering") == offering["guid"].as_str()
                            });
                            plans.into_iter().any(|plan| seen(req, state, access, plan))
                        })
                        .cloned()
                        .collect();
                    page(
                        req,
                        items,
                        &["space_guids", "organization_guids"],
                        |items| {
                            let mut included = Map::new();
                            if wants(req, "service_broker") {
                                let brokers = Catalog::of(
                                    &catalog.brokers,
                                    items.iter().map(|offering| catalog.broker_of(offering)),
                                );
                                included.insert("service_brokers".into(), json!(brokers));
                            }
                            included
                        },
                    )
                })
            }
            (&Method::GET, ["service_offerings", guid]) => {
                let offer = offer().await;
                self.store
                    .read(|state| by_guid(&catalog(base, &offer, state).offerings, guid).cloned())
                    .map(|offering| respond(StatusCode::OK, &offering))
                    .ok_or_else(|| ApiError::not_found("Service offering"))
            }
            (&Method::DELETE, ["service_offerings", guid]) => match self.delete_offering(req, guid)
            {
                Some(deleted) => deleted.map(|()| no_content()),
                None => Err(ApiError::unprocessable(
                    "the offerings of sluice-wicket follow the wicket's keyvalue stores and capabilities",
                )),
            },
            (_, ["service_offerings", ..]) => Err(ApiError::unprocessable(
                "an offering follows its broker's catalog: cf update-service-broker fetches it again",
            )),
            (&Method::GET, ["service_plans"]) => {
                let offer = offer().await;
                self.store
                    .read(|state| self.plans(req, state, access, &offer))
            }
            (&Method::GET, ["service_plans", guid]) => {
                let offer = offer().await;
                self.store
                    .read(|state| by_guid(&catalog(base, &offer, state).plans, guid).cloned())
                    .map(|plan| respond(StatusCode::OK, &plan))
                    .ok_or_else(|| ApiError::not_found("Service plan"))
            }
            (_, ["service_plans", guid, "visibility", rest @ ..]) => {
                match self.plan_visibility(req, guid, rest) {
                    Some(reply) => reply,
                    None if req.method == Method::GET && rest.is_empty() => {
                        Ok(respond(StatusCode::OK, &json!({"type": "public"})))
                    }
                    None => Err(ApiError::unprocessable(
                        "the plans of sluice-wicket follow the wicket's keyvalue stores and capabilities: each is public",
                    )),
                }
            }
            (_, ["service_plans", ..]) => Err(ApiError::unprocessable(
                "a plan follows its broker's catalog: cf update-service-broker fetches it again",
            )),
            (&Method::GET, ["service_instances"]) => {
                let offer = offer().await;
                self.store
                    .read(|state| self.instances(req, state, access, &offer))
            }
            (&Method::POST, ["service_instances"]) => self.create_instance(req).await,
            (&Method::GET, ["service_instances", guid]) => {
                let instance = self.instance_as(req, guid, Need::Read)?;
                Ok(respond(
                    StatusCode::OK,
                    &self
                        .store
                        .read(|state| instance_view(base, state, &instance)),
                ))
            }
            (&Method::PATCH, ["service_instances", guid]) => self.update_instance(req, guid).await,
            (&Method::DELETE, ["service_instances", guid]) => self.delete_instance(req, guid).await,
            (&Method::GET, ["service_instances", guid, "parameters"]) => {
                self.instance_as(req, guid, Need::Develop)?;
                self.instance_parameters(guid).await
            }
            (&Method::GET, ["service_instances", guid, "credentials"]) => {
                let instance = self.instance_as(req, guid, Need::Develop)?;
                match instance.user_provided() {
                    true => Ok(respond(
                        StatusCode::OK,
                        &Value::Object(instance.credentials),
                    )),
                    false => Err(ApiError::not_found("Service instance")),
                }
            }
            (&Method::GET, ["service_instances", guid, "relationships", "shared_spaces"]) => {
                self.instance_as(req, guid, Need::Read)?;
                Ok(respond(
                    StatusCode::OK,
                    &json!({"data": [], "links": {"self": {"href": format!("{base}/v3/service_instances/{guid}/relationships/shared_spaces")}}}),
                ))
            }
            (_, ["service_instances", _, "relationships", "shared_spaces", ..]) => Err(
                ApiError::unprocessable("sharing is not supported: an instance is its space's"),
            ),
            (&Method::GET, ["service_credential_bindings"]) => {
                self.store.read(|state| bindings(req, state, access))
            }
            (&Method::POST, ["service_credential_bindings"]) => self.create_binding(req).await,
            (&Method::GET, ["service_credential_bindings", guid]) => {
                let binding = self.binding_as(req, guid, Need::Read)?;
                Ok(respond(StatusCode::OK, &binding_view(base, &binding)))
            }
            (&Method::GET, ["service_credential_bindings", guid, "details"]) => {
                self.binding_as(req, guid, Need::Develop)?;
                self.store
                .read(|state| {
                    let binding = state.binding(guid)?;
                    let instance = state.instance(&binding.instance)?;
                    Some(json!({"credentials": credentials(instance, binding), "syslog_drain_url": null, "volume_mounts": []}))
                })
                .map(|view| respond(StatusCode::OK, &view))
                .ok_or_else(|| ApiError::not_found("Service credential binding"))
            }
            (&Method::GET, ["service_credential_bindings", guid, "parameters"]) => {
                self.binding_as(req, guid, Need::Develop)?;
                self.binding_parameters(guid).await
            }
            (&Method::DELETE, ["service_credential_bindings", guid]) => {
                self.delete_binding(req, guid).await
            }
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        }
    }

    /// The plans, with their offerings, brokers, spaces and organizations
    /// included as asked; filtered by what is seen where the request says.
    fn plans(&self, req: &Req, state: &State, access: &Access, offer: &Offer) -> Reply {
        let catalog = catalog(&req.base, offer, state);
        let items: Vec<Value> = catalog
            .plans
            .iter()
            .filter(|plan| {
                let offering = catalog.offering_of(plan);
                let broker = offering.and_then(|offering| catalog.broker_of(offering));
                named(req, "service_offering_names", offering)
                    && named(req, "service_broker_names", broker)
                    && values(req, "service_broker_guids").is_none_or(|guids| {
                        broker
                            .and_then(|broker| broker["guid"].as_str())
                            .is_some_and(|guid| guids.iter().any(|wanted| wanted == guid))
                    })
                    && seen(req, state, access, plan)
            })
            .cloned()
            .collect();
        let skip = ["space_guids", "organization_guids", "service_broker_guids"];
        page(req, items, &skip, |items| {
            let mut included = Map::new();
            let offerings = Catalog::of(
                &catalog.offerings,
                items.iter().map(|plan| catalog.offering_of(plan)),
            );
            if wants(req, "service_offering.service_broker") {
                let brokers = Catalog::of(
                    &catalog.brokers,
                    offerings.iter().map(|offering| catalog.broker_of(offering)),
                );
                included.insert("service_brokers".into(), json!(brokers));
            }
            if wants(req, "service_offering") || wants(req, "service_offering.service_broker") {
                included.insert("service_offerings".into(), json!(offerings));
            }
            // a space-scoped broker's plans are of its space
            let scoped = items.iter().filter_map(|plan| related(plan, "space"));
            if let Some(Value::Object(spaces)) =
                views::spaces_included(&req.base, state, &req.query, scoped)
            {
                included.extend(spaces);
            }
            included
        })
    }

    /// The instances, with the plans, offerings, broker, space and
    /// organization they name included as asked.
    fn instances(&self, req: &Req, state: &State, access: &Access, offer: &Offer) -> Reply {
        let base = req.base.as_str();
        let catalog = catalog(base, offer, state);
        let items = state
            .instances
            .iter()
            .filter(|instance| access.can(state, &instance.space, Need::Read))
            .filter(|instance| {
                named(
                    req,
                    "service_plan_names",
                    by_guid(&catalog.plans, &plan_of(instance)),
                )
            })
            .map(|instance| instance_view(base, state, instance))
            .collect();
        page(req, items, &[], |items| {
            let plans = Catalog::of(
                &catalog.plans,
                items.iter().map(|instance| {
                    related(instance, "service_plan").and_then(|guid| by_guid(&catalog.plans, guid))
                }),
            );
            let offerings = Catalog::of(
                &catalog.offerings,
                plans.iter().map(|plan| catalog.offering_of(plan)),
            );
            let mut included = Map::new();
            if wants(req, "service_plan") {
                included.insert("service_plans".into(), json!(plans));
            }
            if wants(req, "service_plan.service_offering") {
                included.insert("service_offerings".into(), json!(offerings));
            }
            if wants(req, "service_plan.service_offering.service_broker") {
                let brokers = Catalog::of(
                    &catalog.brokers,
                    offerings.iter().map(|offering| catalog.broker_of(offering)),
                );
                included.insert("service_brokers".into(), json!(brokers));
            }
            let spaces = items
                .iter()
                .filter_map(|instance| related(instance, "space"));
            if let Some(Value::Object(spaces)) =
                views::spaces_included(base, state, &req.query, spaces)
            {
                included.extend(spaces);
            }
            included
        })
    }

    async fn create_instance(&self, req: &Req) -> Reply {
        let body: Value = req.json()?;
        let space = body["relationships"]["space"]["data"]["guid"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let access = req.access()?;
        self.store
            .read(|state| space_to_develop(access, state, &space))?;
        match body["type"].as_str() {
            Some("managed") => {}
            Some(USER_PROVIDED) => return self.create_user_provided(req, &body, &space),
            _ => {
                return Err(ApiError::unprocessable(
                    "type is managed (cf create-service) or user-provided (cf create-user-provided-service)",
                ));
            }
        }
        let name = body["name"].as_str().unwrap_or_default().to_string();
        check_name("the name", &name).map_err(ApiError::unprocessable)?;
        let wanted = body["relationships"]["service_plan"]["data"]["guid"]
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("relationships.service_plan names the plan"))?;
        let tags = tags(&body["tags"])?;
        if self.store.read(|state| state.brokered(wanted).is_some()) {
            return self.create_brokered(req, &body, &space, name, wanted, tags);
        }
        no_parameters(&body["parameters"])?;
        let offer = self.controller.offer().await.map_err(|e| {
            ApiError::unprocessable(format!(
                "the plans are the wicket's, and it is unreachable: {e}"
            ))
        })?;
        let plan = offer
            .plans
            .iter()
            .find(|plan| plan_guid(&plan.offering, &plan.name) == wanted)
            .ok_or_else(invalid_plan)?;
        let instance = self.store.update(|state| {
            space_to_develop(access, state, &space)?;
            if state.instance_named(&space, &name).is_some() {
                return Err(name_taken(&name));
            }
            let at = now();
            let instance = Instance {
                guid: guid(),
                name: name.clone(),
                space: space.clone(),
                offering: plan.offering.clone(),
                plan: plan.name.clone(),
                tags,
                credentials: Map::new(),
                broker: None,
                dashboard_url: None,
                maintenance_info: None,
                operation: None,
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

    /// A broker's instance: made by its broker, the job settled when it is
    /// (`crate::operations`); its plan one the space sees.
    fn create_brokered(
        &self,
        req: &Req,
        body: &Value,
        space: &str,
        name: String,
        plan: &str,
        tags: Vec<String>,
    ) -> Reply {
        let parameters = object_parameters(&body["parameters"]).map_err(ApiError::unprocessable)?;
        let user = self.claims(req)?.user_id;
        let access = req.access()?;
        let job = self.store.update(|state| {
            space_to_develop(access, state, space)?;
            if state.instance_named(space, &name).is_some() {
                return Err(name_taken(&name));
            }
            let brokered = state.brokered(plan).ok_or_else(invalid_plan)?;
            let org = state.space(space).map_or("", |space| space.org.as_str());
            if !brokered.plan.visible(brokered.broker, org, space) || brokered.offering.gone {
                return Err(invalid_plan());
            }
            let (offering, broker) = (brokered.offering.guid.clone(), brokered.broker.guid.clone());
            let maintenance_info = brokered.plan.maintenance_info().cloned();
            let guid = guid();
            let job = state.add_waiting_job(
                "service_instance.create",
                vec![guid.clone()],
                Some((
                    "service_instances".into(),
                    format!("/v3/service_instances/{guid}"),
                )),
            );
            let mut operation =
                Operation::new(OperationKind::Create, Some(job.clone()), Some(user));
            operation.parameters = parameters;
            let at = now();
            state.instances.push(Instance {
                guid,
                name: name.clone(),
                space: space.to_string(),
                offering,
                plan: plan.to_string(),
                tags,
                credentials: Map::new(),
                broker: Some(broker),
                dashboard_url: None,
                maintenance_info,
                operation: Some(operation),
                created_at: at.clone(),
                updated_at: at,
            });
            Ok(job)
        })?;
        println!("SERVICES: creating service instance {name} through its broker");
        self.operations.kick();
        Ok(self.located(req, &job))
    }

    /// A user-provided instance: its credentials, no plan; answered at once,
    /// as CC does (201, no job).
    fn create_user_provided(&self, req: &Req, body: &Value, space: &str) -> Reply {
        let name = body["name"].as_str().unwrap_or_default().to_string();
        check_name("the name", &name).map_err(ApiError::unprocessable)?;
        user_provided_only(body)?;
        let credentials = user_credentials(&body["credentials"])?.unwrap_or_default();
        let tags = tags(&body["tags"])?;
        let access = req.access()?;
        let view = self.store.update(|state| {
            space_to_develop(access, state, space)?;
            if state.instance_named(space, &name).is_some() {
                return Err(name_taken(&name));
            }
            let at = now();
            let instance = Instance {
                guid: guid(),
                name: name.clone(),
                space: space.to_string(),
                offering: USER_PROVIDED.into(),
                plan: String::new(),
                tags,
                credentials,
                broker: None,
                dashboard_url: None,
                maintenance_info: None,
                operation: None,
                created_at: at.clone(),
                updated_at: at,
            };
            state.instances.push(instance.clone());
            Ok(instance_view(&req.base, state, &instance))
        })?;
        println!("SERVICES: created user-provided service instance {name}");
        Ok(respond(StatusCode::CREATED, &view))
    }

    /// A rename or new tags apply at once, and so do a user-provided
    /// instance's new credentials (replacing the old, as on CC), which its
    /// apps get at their next start. A broker's instance's new plan,
    /// parameters or maintenance info (and a new name, its offering taking
    /// context updates) are its broker's to make: answered 202, the plan
    /// taken when the broker made it.
    async fn update_instance(&self, req: &Req, guid: &str) -> Reply {
        let body: Value = req.json()?;
        self.instance_as(req, guid, Need::Develop)?;
        let (user_provided, brokered) = self
            .store
            .read(|state| {
                state
                    .instance(guid)
                    .map(|instance| (instance.user_provided(), instance.brokered()))
            })
            .ok_or_else(|| ApiError::not_found("Service instance"))?;
        let credentials = if user_provided {
            user_provided_only(&body)?;
            user_credentials(&body["credentials"])?
        } else if !body["credentials"].is_null() {
            return Err(ApiError::unprocessable(
                "credentials are a user-provided service instance's",
            ));
        } else {
            None
        };
        if !brokered {
            no_parameters(&body["parameters"])?;
            if !body["maintenance_info"].is_null() {
                return Err(ApiError::unprocessable("no upgrade is available"));
            }
        }
        let parameters = object_parameters(&body["parameters"]).map_err(ApiError::unprocessable)?;
        let name = body["name"].as_str().map(String::from);
        if let Some(name) = &name {
            check_name("the name", name).map_err(ApiError::unprocessable)?;
        }
        let tags = (!body["tags"].is_null())
            .then(|| tags(&body["tags"]))
            .transpose()?;
        let plan = body["relationships"]["service_plan"]["data"]["guid"].as_str();
        let user = self.claims(req)?.user_id;
        let (view, name, bound, job) = self.store.update(|state| {
            let current = state
                .instance(guid)
                .cloned()
                .ok_or_else(|| ApiError::not_found("Service instance"))?;
            let update = match current.brokered() {
                true => Some(brokered_update(state, &current, plan, &body, parameters.clone())?),
                false => None,
            };
            if !current.brokered()
                && plan.is_some_and(|plan| plan != plan_guid(&current.offering, &current.plan))
            {
                return Err(ApiError::unprocessable(
                    "a plan change is not supported: a keyvalue instance's data stays in its store, \
                     a wasi instance is its capability; create another instance",
                ));
            }
            if let Some(name) = &name
                && state
                    .instance_named(&current.space, name)
                    .is_some_and(|other| other.guid != guid)
            {
                return Err(name_taken(name));
            }
            let renamed = name.as_ref().is_some_and(|name| *name != current.name);
            let context_update = renamed
                && state
                    .brokered_of(&current)
                    .is_some_and(|brokered| brokered.offering.feature("allow_context_updates"));
            let broker_asked = update.as_ref().is_some_and(|update| {
                update.plan.is_some() || update.maintenance_info.is_some() || update.parameters.is_some()
            }) || context_update;
            let job = broker_asked.then(|| {
                state.add_waiting_job(
                    "service_instance.update",
                    vec![guid.to_string()],
                    Some(("service_instances".into(), format!("/v3/service_instances/{guid}"))),
                )
            });
            let instance = state.instance_mut(guid).expect("found above");
            if let Some(name) = &name {
                instance.name = name.clone();
            }
            if let Some(tags) = &tags {
                instance.tags = tags.clone();
            }
            let changed = credentials
                .as_ref()
                .is_some_and(|credentials| *credentials != instance.credentials);
            if let Some(credentials) = credentials {
                instance.credentials = credentials;
            }
            instance.updated_at = now();
            if let (Some(job), Some(update)) = (&job, update) {
                let mut operation =
                    Operation::new(OperationKind::Update, Some(job.clone()), Some(user));
                operation.plan = update.plan;
                operation.maintenance_info = update.maintenance_info;
                operation.parameters = update.parameters;
                instance.operation = Some(operation);
            }
            let instance = instance.clone();
            let bound: Vec<String> = match changed {
                true => state
                    .bindings
                    .iter()
                    .filter(|binding| binding.instance == guid)
                    .map(|binding| binding.app.clone())
                    .collect(),
                false => Vec::new(),
            };
            Ok((instance_view(&req.base, state, &instance), instance.name, bound, job))
        })?;
        for app in &bound {
            self.logs.add(
                app,
                "API",
                format!(
                    "Updated the credentials of service instance {name}: applies at the next start, restart or restage"
                ),
            );
        }
        if let Some(job) = job {
            println!("SERVICES: updating service instance {name} through its broker");
            self.operations.kick();
            return Ok(self.located(req, &job));
        }
        Ok(respond(StatusCode::OK, &view))
    }

    /// Refused while bound (`purge=true` drops the bindings, and a broker's
    /// instance without asking its broker); a keyvalue instance's data is
    /// deleted once no app maps it; a broker's instance is its broker's to
    /// delete (202).
    async fn delete_instance(&self, req: &Req, guid: &str) -> Reply {
        self.instance_as(req, guid, Need::Develop)?;
        let purge = req.q("purge") == Some("true");
        let user = self.claims(req)?.user_id;
        let (instance, dropped, job) = self.store.update(|state| {
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
            if instance.brokered() && !purge {
                if instance.busy() {
                    return Err(busy(in_progress(&instance.name)));
                }
                let job = state.add_waiting_job("service_instance.delete", vec![guid.to_string()], None);
                let deleted = state.instance_mut(guid).expect("found above");
                deleted.operation =
                    Some(Operation::new(OperationKind::Delete, Some(job.clone()), Some(user)));
                return Ok((instance, Vec::new(), Some(job)));
            }
            state.bindings.retain(|binding| binding.instance != guid);
            state.instances.retain(|kept| kept.guid != guid);
            if instance.offering == KEYVALUE {
                state.purges.push(Purge {
                    store: instance.plan.clone(),
                    namespace: namespace(guid),
                    instance: instance.name.clone(),
                    purged: Vec::new(),
                });
            }
            Ok((instance, bound, None))
        })?;
        if let Some(job) = job {
            println!(
                "SERVICES: deleting service instance {} through its broker",
                instance.name
            );
            self.operations.kick();
            return Ok(self.located(req, &job));
        }
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
        if purge || instance.user_provided() {
            return Ok(no_content());
        }
        self.job(req, "service_instance.delete", Vec::new(), None)
    }

    /// A broker's instance's parameters, as its broker gives them if its
    /// offering lets it (`instances_retrievable`); the wicket's have none.
    async fn instance_parameters(&self, guid: &str) -> Reply {
        let found = self
            .store
            .read(|state| {
                let instance = state.instance(guid)?;
                let brokered = state.brokered_of(instance).map(|brokered| {
                    (
                        crate::osb::Endpoint::from(brokered.broker),
                        brokered.offering.feature("instances_retrievable"),
                    )
                });
                Some((instance.user_provided(), brokered))
            })
            .ok_or_else(|| ApiError::not_found("Service instance"))?;
        match found {
            (true, _) => Err(ApiError::unprocessable(
                "a user-provided service instance has no parameters: it has credentials",
            )),
            (false, None) => Ok(respond(StatusCode::OK, &json!({}))),
            (false, Some((_, false))) => Err(ApiError::bad_request(NO_INSTANCE_PARAMETERS)),
            (false, Some((endpoint, true))) => {
                let fetched = self
                    .osb
                    .fetch(&endpoint, &format!("/v2/service_instances/{guid}"))
                    .await
                    .map_err(|refusal| broker_error(refusal.description))?;
                Ok(respond(
                    StatusCode::OK,
                    &fetched
                        .get("parameters")
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                ))
            }
        }
    }

    /// A binding's parameters: a keyvalue binding's identifier, a broker's
    /// as its broker gives them if its offering lets it
    /// (`bindings_retrievable`).
    async fn binding_parameters(&self, guid: &str) -> Reply {
        let found = self
            .store
            .read(|state| {
                let binding = state.binding(guid)?;
                let instance = state.instance(&binding.instance)?;
                let brokered = state.brokered_of(instance).map(|brokered| {
                    (
                        crate::osb::Endpoint::from(brokered.broker),
                        brokered.offering.feature("bindings_retrievable"),
                    )
                });
                let local = match instance.offering == KEYVALUE {
                    true => json!({"identifier": binding.identifier}),
                    false => json!({}),
                };
                Some((
                    format!(
                        "/v2/service_instances/{}/service_bindings/{guid}",
                        instance.guid
                    ),
                    brokered,
                    local,
                ))
            })
            .ok_or_else(|| ApiError::not_found("Service credential binding"))?;
        match found {
            (_, None, local) => Ok(respond(StatusCode::OK, &local)),
            (_, Some((_, false)), _) => Err(ApiError::bad_request(NO_BINDING_PARAMETERS)),
            (path, Some((endpoint, true)), _) => {
                let fetched = self
                    .osb
                    .fetch(&endpoint, &path)
                    .await
                    .map_err(|refusal| broker_error(refusal.description))?;
                Ok(respond(
                    StatusCode::OK,
                    &fetched
                        .get("parameters")
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                ))
            }
        }
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
        let user = self.claims(req)?.user_id;
        let access = req.access()?;
        let (binding, instance_name, user_provided, job) = self.store.update(|state| {
            let space = state
                .app(app)
                .filter(|app| access.can(state, &app.space, Need::Read))
                .map(|app| app.space.clone())
                .ok_or_else(|| {
                    ApiError::unprocessable(format!("The app could not be found: {app}"))
                })?;
            access.check(state, &space, Need::Develop, "App")?;
            let (instance_name, user_provided) = state
                .instance(instance)
                .filter(|instance| instance.space == space)
                .map(|instance| (instance.name.clone(), instance.user_provided()))
                .ok_or_else(|| {
                    ApiError::unprocessable(format!(
                        "The service instance could not be found: {instance}"
                    ))
                })?;
            let binding = match bind(state, app, instance, name.clone(), &body["parameters"]) {
                Ok(binding) => binding,
                Err(Unbound::Already) => {
                    return Err(ApiError {
                        status: StatusCode::UNPROCESSABLE_ENTITY,
                        code: 90003,
                        title: "CF-ServiceBindingAppServiceTaken",
                        detail: "The app is already bound to the service instance.".into(),
                    });
                }
                Err(Unbound::Busy(e)) => return Err(busy(e)),
                Err(Unbound::Refused(e)) => return Err(ApiError::unprocessable(e)),
            };
            let job = binding.operation.is_some().then(|| {
                let job = state.add_waiting_job(
                    "service_bindings.create",
                    vec![binding.guid.clone()],
                    Some((
                        "service_credential_binding".into(),
                        format!("/v3/service_credential_bindings/{}", binding.guid),
                    )),
                );
                let operation = state
                    .binding_mut(&binding.guid)
                    .and_then(|binding| binding.operation.as_mut())
                    .expect("just bound");
                operation.job = Some(job.clone());
                operation.user = Some(user);
                job
            });
            Ok((binding, instance_name, user_provided, job))
        })?;
        if let Some(job) = job {
            // logged when the broker made it
            self.operations.kick();
            return Ok(self.located(req, &job));
        }
        self.logs.add(
            app,
            "API",
            format!(
                "Bound service instance {instance_name}: applies at the next start, restart or restage"
            ),
        );
        if user_provided {
            // as CC: a user-provided instance's binding has no broker to wait for
            return Ok(respond(
                StatusCode::CREATED,
                &binding_view(&req.base, &binding),
            ));
        }
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

    /// At once, but a broker's binding: its broker's to delete (202).
    async fn delete_binding(&self, req: &Req, guid: &str) -> Reply {
        self.binding_as(req, guid, Need::Develop)?;
        let user = self.claims(req)?.user_id;
        let (binding, instance_name, user_provided, job) = self.store.update(|state| {
            let binding = state
                .binding(guid)
                .cloned()
                .ok_or_else(|| ApiError::not_found("Service credential binding"))?;
            let (name, user_provided, brokered) = state
                .instance(&binding.instance)
                .map_or_else(Default::default, |instance| {
                    (instance.name.clone(), instance.user_provided(), instance.brokered())
                });
            if brokered {
                if binding.busy() {
                    let app = state.app(&binding.app).map_or("", |app| app.name.as_str());
                    return Err(busy(format!(
                        "An operation for the service binding between app {app} and service instance {name} is in progress."
                    )));
                }
                let job = state.add_waiting_job("service_bindings.delete", vec![guid.to_string()], None);
                let unbound = state.binding_mut(guid).expect("found above");
                unbound.operation =
                    Some(Operation::new(OperationKind::Delete, Some(job.clone()), Some(user)));
                return Ok((binding, name, user_provided, Some(job)));
            }
            state.bindings.retain(|kept| kept.guid != guid);
            Ok::<_, ApiError>((binding, name, user_provided, None))
        })?;
        if let Some(job) = job {
            self.operations.kick();
            return Ok(self.located(req, &job));
        }
        self.logs.add(
            &binding.app,
            "API",
            format!(
                "Unbound service instance {instance_name}: applies at the next start, restart or restage"
            ),
        );
        if user_provided {
            return Ok(no_content());
        }
        self.job(req, "service_bindings.delete", Vec::new(), None)
    }
}

/// What an update of the broker's instance `current` asks of its broker:
/// a plan of its offering the space sees, its plan updateable; the
/// maintenance info its plan has; parameters. None is under way.
fn brokered_update(
    state: &State,
    current: &Instance,
    plan: Option<&str>,
    body: &Value,
    parameters: Option<Value>,
) -> Result<BrokeredUpdate, ApiError> {
    if current.busy() {
        return Err(busy(in_progress(&current.name)));
    }
    if failed_create(current) {
        return Err(ApiError::unprocessable(format!(
            "The service instance {} could not be created: delete it (cf delete-service)",
            current.name
        )));
    }
    let brokered = state.brokered_of(current).ok_or_else(invalid_plan)?;
    let plan = plan.filter(|plan| *plan != current.plan);
    if let Some(plan) = plan {
        let wanted = state.brokered(plan).ok_or_else(invalid_plan)?;
        let org = state
            .org_of(&current.space)
            .map_or("", |org| org.guid.as_str());
        if wanted.offering.guid != brokered.offering.guid
            || !wanted.plan.visible(wanted.broker, org, &current.space)
        {
            return Err(invalid_plan());
        }
        if !brokered.plan.feature(brokered.offering, "plan_updateable") {
            return Err(ApiError::bad_request(
                "The service does not support changing plans.",
            ));
        }
    }
    let maintenance_info = match &body["maintenance_info"] {
        Value::Null => None,
        info => {
            let offered = brokered.plan.maintenance_info();
            let version = |info: Option<&Value>| info.and_then(|info| info.get("version")).cloned();
            if version(Some(info)).is_none() || version(Some(info)) != version(offered) {
                return Err(ApiError::unprocessable(
                    "The maintenance_info.version requested is invalid. Please ensure it matches what the service plan has available.",
                ));
            }
            (version(current.maintenance_info.as_ref()) != version(offered))
                .then(|| offered.cloned().expect("its version matched"))
        }
    };
    Ok(BrokeredUpdate {
        plan: plan.map(String::from),
        maintenance_info,
        parameters,
    })
}

/// A broker's answer that is no success, to a request answered at once.
fn broker_error(detail: String) -> ApiError {
    ApiError {
        status: StatusCode::BAD_GATEWAY,
        code: 10001,
        title: "CF-ServiceBrokerBadResponse",
        detail,
    }
}

/// The bindings of the apps the holder of `access` sees, with the apps and instances they name
/// included as asked.
fn bindings(req: &Req, state: &State, access: &Access) -> Reply {
    let base = req.base.as_str();
    let items = state
        .bindings
        .iter()
        .filter(|binding| {
            state
                .app(&binding.app)
                .is_some_and(|app| access.can(state, &app.space, Need::Read))
        })
        .map(|binding| binding_view(base, binding))
        .collect();
    page(req, items, &[], |items| {
        let mut included = Map::new();
        if wants(req, "app") {
            let named: BTreeSet<&str> = items
                .iter()
                .filter_map(|item| related(item, "app"))
                .collect();
            let apps: Vec<Value> = state
                .apps
                .iter()
                .filter(|app| named.contains(app.guid.as_str()))
                .map(|app| views::app(base, app))
                .collect();
            included.insert("apps".into(), json!(apps));
        }
        if wants(req, "service_instance") {
            let named: BTreeSet<&str> = items
                .iter()
                .filter_map(|item| related(item, "service_instance"))
                .collect();
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The wickets offer a store of each one's host by its name, a shared one by its name and
    /// identity: two servers of one name are offered by none, and of each wicket's host.
    #[test]
    fn a_store_is_offered_when_every_wicket_lists_it_alike() {
        let store = |name: &str, shared: Option<&str>| ListedStore {
            name: name.into(),
            kind: "redis".into(),
            default: false,
            shared: shared.map(String::from),
        };
        let caps = ["cli".to_string(), KEYVALUE.to_string()];
        let a = Offer::new(
            &caps,
            &[
                store("local", None),
                store("cache", Some("redis://r:6379/0")),
                store("other", Some("redis://r:6379/1")),
            ],
        );
        let b = Offer::new(
            &caps[..1],
            &[
                store("local", None),
                store("cache", Some("redis://r:6379/0")),
                store("other", Some("redis://s:6379/1")),
            ],
        );
        let common = Offer::common(&[&a, &b]).unwrap();
        let plans: Vec<(&str, &str)> = (common.plans.iter())
            .map(|plan| (plan.offering.as_str(), plan.name.as_str()))
            .collect();
        assert_eq!(
            plans,
            [(KEYVALUE, "local"), (KEYVALUE, "cache"), (WASI, "cli")]
        );
        assert!(
            common.plans[0]
                .description
                .contains("each wicket's host's own")
        );
        assert!(
            common.plans[1]
                .description
                .contains("shared by the wickets")
        );
        let stores = [("cache", true), ("local", false), ("other", false)]
            .map(|(name, shared)| (name.to_string(), shared));
        assert_eq!(Offer::stores(&[&a, &b]), stores.into());
    }
}
