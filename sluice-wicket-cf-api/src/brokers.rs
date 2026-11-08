//! The service brokers registered (`cf create-service-broker`): any Open
//! Service Broker API broker (`crate::osb`), beside the wicket's own
//! offerings (the broker `sluice-wicket`, `crate::services`). Its catalog is
//! fetched when it is registered and again on `cf update-service-broker`; an
//! offering or a plan the catalog no longer has stays, deactivated, while it
//! has instances.
//!
//! A broker's URL is any the cf-api reaches, so the brokers and who sees
//! their plans are for the [`Editors`] of `--service-broker-editor` alone:
//! everyone holds `cloud_controller.admin`. A plan is seen by none (`admin`)
//! until `cf enable-service-access` makes it public or the organization's; a
//! space-scoped broker's are its space's from the start. A plan no one sees
//! offers no new instance (`crate::services`).
//!
//! [`Editors`]: crate::security_groups::Editors

use std::collections::BTreeSet;

use http::{Method, StatusCode};
use serde_json::{Value, json};

use crate::api::{Api, ApiError, Reply, Req, json as respond, no_content};
use crate::osb::{CatalogOffering, Endpoint};
use crate::store::{Broker, BrokerPlan, JobError, Offering, State, guid, now};

/// The option naming who may edit the brokers.
const EDITORS: &str = "service-broker-editor";
/// A broker's name at most.
const MAX_NAME: usize = 255;

fn metadata() -> Value {
    json!({"labels": {}, "annotations": {}})
}

/// A registered broker as CC shows it: never its credentials.
pub fn view(base: &str, broker: &Broker) -> Value {
    let guid = &broker.guid;
    let mut links = json!({
        "self": {"href": format!("{base}/v3/service_brokers/{guid}")},
        "service_offerings": {"href": format!("{base}/v3/service_offerings?service_broker_guids={guid}")},
    });
    let relationships = match &broker.space {
        Some(space) => {
            links["space"] = json!({"href": format!("{base}/v3/spaces/{space}")});
            json!({"space": {"data": {"guid": space}}})
        }
        None => json!({}),
    };
    json!({
        "guid": guid, "created_at": broker.created_at, "updated_at": broker.updated_at,
        "name": broker.name, "url": broker.url,
        "relationships": relationships,
        "metadata": metadata(),
        "links": links,
    })
}

/// A broker's offering as CC shows it: its catalog's features and metadata.
pub fn offering_view(base: &str, broker: &Broker, offering: &Offering) -> Value {
    let guid = &offering.guid;
    let catalog = &offering.catalog;
    let metadata_of = catalog
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    json!({
        "guid": guid, "created_at": offering.created_at, "updated_at": offering.updated_at,
        "name": offering.name(),
        "description": catalog.get("description").cloned().unwrap_or_default(),
        "available": !offering.gone,
        "tags": catalog.get("tags").cloned().unwrap_or_else(|| json!([])),
        "requires": catalog.get("requires").cloned().unwrap_or_else(|| json!([])),
        "shareable": metadata_of.get("shareable").and_then(Value::as_bool).unwrap_or(false),
        "documentation_url": metadata_of.get("documentationUrl").cloned().unwrap_or(Value::Null),
        "broker_catalog": {
            "id": offering.id(),
            "metadata": metadata_of,
            "features": {
                "plan_updateable": offering.feature("plan_updateable"),
                "bindable": offering.feature("bindable"),
                "instances_retrievable": offering.feature("instances_retrievable"),
                "bindings_retrievable": offering.feature("bindings_retrievable"),
                "allow_context_updates": offering.feature("allow_context_updates"),
            },
        },
        "relationships": {"service_broker": {"data": {"guid": broker.guid}}},
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_offerings/{guid}")},
            "service_plans": {"href": format!("{base}/v3/service_plans?service_offering_guids={guid}")},
            "service_broker": {"href": format!("{base}/v3/service_brokers/{}", broker.guid)},
        },
    })
}

/// A broker's plan as CC shows it.
pub fn plan_view(base: &str, broker: &Broker, offering: &Offering, plan: &BrokerPlan) -> Value {
    let guid = &plan.guid;
    let catalog = &plan.catalog;
    let metadata_of = catalog
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let schema = |path: &[&str]| {
        let mut at = catalog.get("schemas");
        for key in path {
            at = at.and_then(|value| value.get(key));
        }
        at.cloned().unwrap_or_else(|| json!({}))
    };
    let mut relationships = json!({"service_offering": {"data": {"guid": offering.guid}}});
    if let Some(space) = &broker.space {
        relationships["space"] = json!({"data": {"guid": space}});
    }
    json!({
        "guid": guid, "created_at": plan.created_at, "updated_at": plan.updated_at,
        "name": plan.name(),
        "description": catalog.get("description").cloned().unwrap_or_default(),
        "visibility_type": plan.visibility,
        "available": !plan.gone && !offering.gone,
        "free": catalog.get("free").and_then(Value::as_bool).unwrap_or(true),
        "costs": costs(&metadata_of),
        "maintenance_info": plan.maintenance_info().cloned().unwrap_or_else(|| json!({})),
        "broker_catalog": {
            "id": plan.id(),
            "metadata": metadata_of,
            "maximum_polling_duration": plan.maximum_polling_duration(),
            "features": {
                "bindable": plan.feature(offering, "bindable"),
                "plan_updateable": plan.feature(offering, "plan_updateable"),
            },
        },
        "schemas": {
            "service_instance": {
                "create": {"parameters": schema(&["service_instance", "create", "parameters"])},
                "update": {"parameters": schema(&["service_instance", "update", "parameters"])},
            },
            "service_binding": {
                "create": {"parameters": schema(&["service_binding", "create", "parameters"])},
            },
        },
        "relationships": relationships,
        "metadata": metadata(),
        "links": {
            "self": {"href": format!("{base}/v3/service_plans/{guid}")},
            "service_offering": {"href": format!("{base}/v3/service_offerings/{}", offering.guid)},
            "visibility": {"href": format!("{base}/v3/service_plans/{guid}/visibility")},
        },
    })
}

/// A plan's costs as CC lists them: of its catalog's `metadata.costs`
/// (`{"amount": {"usd": 1.0}, "unit": "MONTHLY"}`), one per currency.
fn costs(metadata: &Value) -> Value {
    let mut costs = Vec::new();
    for cost in metadata["costs"].as_array().into_iter().flatten() {
        for (currency, amount) in cost["amount"].as_object().into_iter().flatten() {
            costs.push(json!({
                "amount": amount,
                "currency": currency.to_uppercase(),
                "unit": cost["unit"],
            }));
        }
    }
    json!(costs)
}

/// The visibility of a plan as `/v3/service_plans/<guid>/visibility` shows it.
fn visibility_view(state: &State, plan: &BrokerPlan) -> Value {
    let mut view = json!({"type": plan.visibility});
    if plan.visibility == "organization" {
        let orgs: Vec<Value> = plan
            .orgs
            .iter()
            .filter_map(|guid| state.org(guid))
            .map(|org| json!({"guid": org.guid, "name": org.name}))
            .collect();
        view["organizations"] = json!(orgs);
    }
    view
}

/// The plan guids of the instances: a plan of one is not dropped.
fn in_use(state: &State) -> BTreeSet<String> {
    state
        .instances
        .iter()
        .filter(|instance| instance.brokered())
        .map(|instance| instance.plan.clone())
        .collect()
}

/// Takes the catalog fetched into the broker, its offerings and plans
/// keeping their guids and visibility by their ids; one gone with an
/// instance stays, deactivated, and is warned of.
pub fn reconcile(
    broker: &mut Broker,
    catalog: Vec<CatalogOffering>,
    in_use: &BTreeSet<String>,
) -> Vec<String> {
    let at = now();
    let fresh_visibility = match broker.space {
        Some(_) => "space",
        None => "admin",
    };
    let mut kept = Vec::new();
    let mut warnings = Vec::new();
    let mut old = std::mem::take(&mut broker.offerings);
    for fetched in catalog {
        let id = fetched.offering["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let mut previous = old
            .iter()
            .position(|offering| offering.id() == id)
            .map(|at| old.remove(at));
        let mut plans = Vec::new();
        for catalog in fetched.plans {
            let id = catalog["id"].as_str().unwrap_or_default();
            let before = previous.as_mut().and_then(|offering| {
                let at = offering.plans.iter().position(|plan| plan.id() == id)?;
                Some(offering.plans.remove(at))
            });
            plans.push(match before {
                Some(plan) => BrokerPlan {
                    updated_at: if plan.catalog == catalog && !plan.gone {
                        plan.updated_at.clone()
                    } else {
                        at.clone()
                    },
                    catalog,
                    gone: false,
                    ..plan
                },
                None => BrokerPlan {
                    guid: guid(),
                    catalog,
                    visibility: fresh_visibility.into(),
                    orgs: Vec::new(),
                    gone: false,
                    created_at: at.clone(),
                    updated_at: at.clone(),
                },
            });
        }
        let name = fetched.offering["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if let Some(previous) = &previous {
            plans.extend(deactivated(previous, &name, in_use, &mut warnings));
        }
        kept.push(match previous {
            Some(offering) => Offering {
                updated_at: if offering.catalog == fetched.offering && !offering.gone {
                    offering.updated_at.clone()
                } else {
                    at.clone()
                },
                catalog: fetched.offering,
                plans,
                gone: false,
                ..offering
            },
            None => Offering {
                guid: guid(),
                catalog: fetched.offering,
                plans,
                gone: false,
                created_at: at.clone(),
                updated_at: at.clone(),
            },
        });
    }
    // the offerings gone from the catalog: kept as far as their plans have instances
    for offering in old {
        let plans = deactivated(&offering, offering.name(), in_use, &mut warnings);
        if !plans.is_empty() {
            if !offering.gone {
                warnings.push(format!(
                    "Service offering {} is gone from the broker's catalog: it is deactivated \
                     while instances of its plans exist",
                    offering.name()
                ));
            }
            kept.push(Offering {
                plans,
                gone: true,
                updated_at: at.clone(),
                ..offering
            });
        }
    }
    broker.offerings = kept;
    broker.updated_at = at;
    warnings
}

/// The plans of `offering` left (those the catalog dropped) that have
/// instances, deactivated; a warning names those newly so.
fn deactivated(
    offering: &Offering,
    name: &str,
    in_use: &BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> Vec<BrokerPlan> {
    let plans: Vec<BrokerPlan> = offering
        .plans
        .iter()
        .filter(|plan| in_use.contains(&plan.guid))
        .map(|plan| BrokerPlan {
            gone: true,
            ..plan.clone()
        })
        .collect();
    let newly: Vec<&str> = offering
        .plans
        .iter()
        .filter(|plan| in_use.contains(&plan.guid) && !plan.gone)
        .map(BrokerPlan::name)
        .collect();
    if !newly.is_empty() {
        warnings.push(format!(
            "Service offering {name}: the plans {} are gone from the broker's catalog: they are \
             deactivated while their instances exist, which the broker is still to bind, unbind \
             and delete",
            newly.join(", ")
        ));
    }
    plans
}

/// What a broker's create or update body says: its name, URL and
/// credentials, each checked when given.
#[derive(Default)]
struct Given {
    name: Option<String>,
    url: Option<String>,
    credentials: Option<(String, String)>,
}

fn given(body: &Value) -> Result<Given, ApiError> {
    let mut given = Given::default();
    if let Some(name) = body.get("name").filter(|name| !name.is_null()) {
        let name = name
            .as_str()
            .filter(|name| !name.trim().is_empty() && name.len() <= MAX_NAME)
            .ok_or_else(|| {
                ApiError::unprocessable(format!("Name is a string of 1 to {MAX_NAME} bytes"))
            })?;
        given.name = Some(name.to_string());
    }
    if let Some(url) = body.get("url").filter(|url| !url.is_null()) {
        let url = url
            .as_str()
            .ok_or_else(|| ApiError::unprocessable("Url must be a valid url"))?;
        let parsed = reqwest::Url::parse(url)
            .map_err(|e| ApiError::unprocessable(format!("Url must be a valid url: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(ApiError::unprocessable(
                "Url must be a valid url: http:// or https://, with a host",
            ));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(ApiError::unprocessable(
                "Url must be a valid url: the credentials go in authentication, not in the url",
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(ApiError::unprocessable(
                "Url must be a valid url: no query and no fragment",
            ));
        }
        given.url = Some(url.to_string());
    }
    if let Some(authentication) = body
        .get("authentication")
        .filter(|authentication| !authentication.is_null())
    {
        if authentication["type"] != "basic" {
            return Err(ApiError::unprocessable("Authentication type must be basic"));
        }
        let credential = |key: &str| {
            authentication["credentials"][key]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(String::from)
                .ok_or_else(|| {
                    ApiError::unprocessable(format!(
                        "Authentication credentials {key} must be a string, not empty"
                    ))
                })
        };
        given.credentials = Some((credential("username")?, credential("password")?));
    }
    Ok(given)
}

fn name_taken(name: &str) -> ApiError {
    ApiError::unprocessable(format!(
        "Name must be unique: a service broker is named {name}"
    ))
}

impl Api {
    /// `/v3/service_brokers*` of a registered broker; `None`: not one.
    pub(crate) async fn registered_brokers(&self, req: &Req, segments: &[&str]) -> Option<Reply> {
        let base = req.base.as_str();
        let reply = match (&req.method, segments) {
            (&Method::POST, ["service_brokers"]) => self.create_broker(req).await,
            (&Method::GET, ["service_brokers", guid]) => {
                let view = self
                    .store
                    .read(|state| state.broker(guid).map(|broker| view(base, broker)))?;
                Ok(respond(StatusCode::OK, &view))
            }
            (&Method::PATCH, ["service_brokers", guid]) => {
                self.store.read(|state| state.broker(guid).map(|_| ()))?;
                self.update_broker(req, guid).await
            }
            (&Method::DELETE, ["service_brokers", guid]) => {
                self.store.read(|state| state.broker(guid).map(|_| ()))?;
                self.delete_broker(req, guid).await
            }
            _ => return None,
        };
        Some(reply)
    }

    async fn create_broker(&self, req: &Req) -> Reply {
        let editor = self.editor(req, &self.service_broker_editors, EDITORS)?;
        let body: Value = req.json()?;
        let given = given(&body)?;
        let name = given
            .name
            .ok_or_else(|| ApiError::unprocessable("Name is a string of 1 to 255 bytes"))?;
        let url = given
            .url
            .ok_or_else(|| ApiError::unprocessable("Url must be a valid url"))?;
        let (username, password) = given.credentials.ok_or_else(|| {
            ApiError::unprocessable("Authentication must be given: basic, with credentials")
        })?;
        let space = match body["relationships"]["space"]["data"]["guid"].as_str() {
            Some(space) => {
                if self.store.read(|state| state.space(space).is_none()) {
                    return Err(ApiError::unprocessable(
                        "Invalid space. Ensure that the space exists and you have access to it.",
                    ));
                }
                Some(space.to_string())
            }
            None => None,
        };
        if self.store.read(|state| taken(state, &name, None)) {
            return Err(name_taken(&name));
        }
        let endpoint = Endpoint {
            url: url.clone(),
            username: username.clone(),
            password: password.clone(),
        };
        let catalog = match self.osb.catalog(&endpoint).await {
            Ok(catalog) => catalog,
            Err(e) => {
                return self.job(
                    req,
                    "service_broker.catalog.synchronize",
                    vec![failed(e)],
                    None,
                );
            }
        };
        let at = now();
        let mut broker = Broker {
            guid: guid(),
            name: name.clone(),
            url,
            username,
            password,
            space,
            offerings: Vec::new(),
            created_at: at.clone(),
            updated_at: at,
        };
        reconcile(&mut broker, catalog, &BTreeSet::new());
        let path = format!("/v3/service_brokers/{}", broker.guid);
        self.store.update(|state| {
            if taken(state, &name, None) {
                return Err(name_taken(&name));
            }
            state.brokers.push(broker.clone());
            Ok(())
        })?;
        println!(
            "SERVICE BROKERS: {editor}: registered {name} at {} ({} offering(s))",
            broker.url,
            broker.offerings.len()
        );
        self.job(
            req,
            "service_broker.catalog.synchronize",
            Vec::new(),
            Some(("service_brokers".into(), path)),
        )
    }

    /// A new name applies at once (200); a new URL or new credentials fetch
    /// the catalog again, which applies only when it is fetched and good.
    async fn update_broker(&self, req: &Req, guid: &str) -> Reply {
        let editor = self.editor(req, &self.service_broker_editors, EDITORS)?;
        let body: Value = req.json()?;
        let given = given(&body)?;
        if let Some(name) = &given.name
            && self.store.read(|state| taken(state, name, Some(guid)))
        {
            return Err(name_taken(name));
        }
        if given.url.is_none() && given.credentials.is_none() {
            let view = self.store.update(|state| {
                let broker = state
                    .brokers
                    .iter_mut()
                    .find(|broker| broker.guid == guid)
                    .ok_or_else(|| ApiError::not_found("Service broker"))?;
                if let Some(name) = given.name {
                    broker.name = name;
                    broker.updated_at = now();
                }
                Ok::<_, ApiError>(view(&req.base, broker))
            })?;
            println!("SERVICE BROKERS: {editor}: renamed {}", view["name"]);
            return Ok(respond(StatusCode::OK, &view));
        }
        let mut endpoint = self
            .store
            .read(|state| state.broker(guid).map(Endpoint::from))
            .ok_or_else(|| ApiError::not_found("Service broker"))?;
        if let Some(url) = &given.url {
            endpoint.url.clone_from(url);
        }
        if let Some((username, password)) = &given.credentials {
            endpoint.username.clone_from(username);
            endpoint.password.clone_from(password);
        }
        let catalog = match self.osb.catalog(&endpoint).await {
            Ok(catalog) => catalog,
            Err(e) => return self.job(req, "service_broker.update", vec![failed(e)], None),
        };
        let (name, warnings) = self.store.update(|state| {
            let in_use = in_use(state);
            let broker = state
                .brokers
                .iter_mut()
                .find(|broker| broker.guid == guid)
                .ok_or_else(|| ApiError::not_found("Service broker"))?;
            if let Some(name) = given.name {
                broker.name = name;
            }
            broker.url = endpoint.url;
            broker.username = endpoint.username;
            broker.password = endpoint.password;
            let warnings = reconcile(broker, catalog, &in_use);
            Ok::<_, ApiError>((broker.name.clone(), warnings))
        })?;
        println!("SERVICE BROKERS: {editor}: updated {name} and its catalog");
        let job = self.store.update(|state| {
            let job = state.add_job(
                "service_broker.update",
                Vec::new(),
                Some((
                    "service_brokers".into(),
                    format!("/v3/service_brokers/{guid}"),
                )),
            );
            state.job_mut(&job).expect("just added").warnings = warnings;
            job
        });
        Ok(self.located(req, &job))
    }

    /// Refused while its plans have instances, as on CC.
    async fn delete_broker(&self, req: &Req, guid: &str) -> Reply {
        let editor = self.editor(req, &self.service_broker_editors, EDITORS)?;
        let name = self.store.update(|state| {
            let broker = state
                .broker(guid)
                .ok_or_else(|| ApiError::not_found("Service broker"))?;
            let instances: Vec<&str> = state
                .instances
                .iter()
                .filter(|instance| instance.broker.as_deref() == Some(guid))
                .map(|instance| instance.name.as_str())
                .collect();
            if !instances.is_empty() {
                return Err(ApiError::unprocessable(format!(
                    "Can not remove brokers that have associated service instances: {}",
                    instances.join(", ")
                )));
            }
            let name = broker.name.clone();
            state.brokers.retain(|broker| broker.guid != guid);
            Ok(name)
        })?;
        println!("SERVICE BROKERS: {editor}: deleted {name}");
        self.job(req, "service_broker.delete", Vec::new(), None)
    }

    /// `/v3/service_plans/<guid>/visibility[/<org>]` of a broker's plan;
    /// `None`: not one.
    pub(crate) fn plan_visibility(&self, req: &Req, plan: &str, rest: &[&str]) -> Option<Reply> {
        let found = self.store.read(|state| {
            state.brokered(plan).map(|brokered| {
                (
                    visibility_view(state, brokered.plan),
                    brokered.broker.space.is_some(),
                )
            })
        })?;
        let reply = match (&req.method, rest) {
            (&Method::GET, []) => Ok(respond(StatusCode::OK, &found.0)),
            (&Method::POST | &Method::PATCH, []) => self.set_visibility(req, plan, found.1),
            (&Method::DELETE, [org]) => self.unset_visibility(req, plan, org),
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        };
        Some(reply)
    }

    /// `public`, `admin`, or `organization`, of the organizations named: appended to those
    /// before (POST) or replacing them (PATCH).
    fn set_visibility(&self, req: &Req, plan: &str, space_scoped: bool) -> Reply {
        let editor = self.editor(req, &self.service_broker_editors, EDITORS)?;
        if space_scoped {
            return Err(ApiError::unprocessable(
                "You cannot change access for space-scoped service plans.",
            ));
        }
        let body: Value = req.json()?;
        let wanted = body["type"].as_str().unwrap_or_default().to_string();
        let named: Vec<String> = match wanted.as_str() {
            "public" | "admin" => Vec::new(),
            "organization" => {
                let named: Vec<String> = body["organizations"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|org| org["guid"].as_str().unwrap_or_default().to_string())
                    .collect();
                if named.is_empty() {
                    return Err(ApiError::unprocessable(
                        "Organizations must be given for the type 'organization'",
                    ));
                }
                named
            }
            _ => {
                return Err(ApiError::unprocessable(
                    "Type must be one of 'public', 'admin', 'organization'",
                ));
            }
        };
        let append = req.method == Method::POST;
        let view = self.store.update(|state| {
            if let Some(unknown) = named.iter().find(|guid| state.org(guid).is_none()) {
                return Err(ApiError::unprocessable(format!(
                    "Could not find Organizations with guids: [\"{unknown}\"]"
                )));
            }
            let plan = plan_mut(state, plan).ok_or_else(|| ApiError::not_found("Service plan"))?;
            match wanted.as_str() {
                "organization" => {
                    if !append || plan.visibility != "organization" {
                        plan.orgs.clear();
                    }
                    for org in &named {
                        if !plan.orgs.contains(org) {
                            plan.orgs.push(org.clone());
                        }
                    }
                }
                _ => plan.orgs.clear(),
            }
            plan.visibility.clone_from(&wanted);
            plan.updated_at = now();
            let plan = plan.clone();
            Ok::<_, ApiError>(visibility_view(state, &plan))
        })?;
        println!("SERVICE BROKERS: {editor}: plan {plan} visible to {wanted}");
        Ok(respond(StatusCode::OK, &view))
    }

    /// The organization's access removed: an organization's plan of none is no one's (`admin`).
    fn unset_visibility(&self, req: &Req, plan: &str, org: &str) -> Reply {
        let editor = self.editor(req, &self.service_broker_editors, EDITORS)?;
        self.store.update(|state| {
            if state.org(org).is_none() {
                return Err(ApiError::not_found("Organization"));
            }
            let plan = plan_mut(state, plan).ok_or_else(|| ApiError::not_found("Service plan"))?;
            if plan.visibility == "organization" {
                plan.orgs.retain(|kept| kept != org);
                if plan.orgs.is_empty() {
                    plan.visibility = "admin".into();
                }
                plan.updated_at = now();
            }
            Ok(())
        })?;
        println!(
            "SERVICE BROKERS: {editor}: plan {plan} no longer visible to the organization {org}"
        );
        Ok(no_content())
    }

    /// `DELETE /v3/service_offerings/<guid>` of a broker's offering:
    /// refused while it has instances, or with `purge=true` dropping them
    /// and their bindings, the broker not asked. `None`: not one.
    pub(crate) fn delete_offering(&self, req: &Req, guid: &str) -> Option<Result<(), ApiError>> {
        self.store.read(|state| {
            state
                .brokers
                .iter()
                .any(|broker| {
                    broker
                        .offerings
                        .iter()
                        .any(|offering| offering.guid == guid)
                })
                .then_some(())
        })?;
        let deleted = (|| {
            let editor = self.editor(req, &self.service_broker_editors, EDITORS)?;
            let purge = req.q("purge") == Some("true");
            let (name, dropped) = self.store.update(|state| {
                let (name, plans): (String, BTreeSet<String>) = state
                    .brokers
                    .iter()
                    .flat_map(|broker| &broker.offerings)
                    .find(|offering| offering.guid == guid)
                    .map(|offering| {
                        (
                            offering.name().to_string(),
                            offering.plans.iter().map(|plan| plan.guid.clone()).collect(),
                        )
                    })
                    .ok_or_else(|| ApiError::not_found("Service offering"))?;
                let instances: BTreeSet<String> = state
                    .instances
                    .iter()
                    .filter(|instance| instance.brokered() && plans.contains(&instance.plan))
                    .map(|instance| instance.guid.clone())
                    .collect();
                if !instances.is_empty() && !purge {
                    return Err(ApiError {
                        status: StatusCode::UNPROCESSABLE_ENTITY,
                        code: 10006,
                        title: "CF-AssociationNotEmpty",
                        detail: format!(
                            "Please delete the service_instances associations for your service_offerings \
                             ({name} has {} instance(s): cf purge-service-offering)",
                            instances.len()
                        ),
                    });
                }
                let dropped: Vec<(String, String)> = state
                    .bindings
                    .iter()
                    .filter(|binding| instances.contains(&binding.instance))
                    .filter_map(|binding| {
                        let instance = state.instance(&binding.instance)?;
                        Some((binding.app.clone(), instance.name.clone()))
                    })
                    .collect();
                state
                    .bindings
                    .retain(|binding| !instances.contains(&binding.instance));
                state
                    .instances
                    .retain(|instance| !instances.contains(&instance.guid));
                for broker in &mut state.brokers {
                    broker.offerings.retain(|offering| offering.guid != guid);
                }
                Ok::<_, ApiError>((name, dropped))
            })?;
            for (app, instance) in dropped {
                self.logs.add(
                    &app,
                    "API",
                    format!(
                        "Unbound service instance {instance} (purged): applies at the next start, restart or restage"
                    ),
                );
            }
            println!("SERVICE BROKERS: {editor}: deleted service offering {name}");
            Ok(())
        })();
        Some(deleted)
    }
}

/// Whether a broker but `except` is named `name`: the wicket's among them.
fn taken(state: &State, name: &str, except: Option<&str>) -> bool {
    name == crate::services::BROKER
        || state
            .brokers
            .iter()
            .any(|broker| broker.name == name && Some(broker.guid.as_str()) != except)
}

fn plan_mut<'a>(state: &'a mut State, guid: &str) -> Option<&'a mut BrokerPlan> {
    state
        .brokers
        .iter_mut()
        .flat_map(|broker| broker.offerings.iter_mut())
        .flat_map(|offering| offering.plans.iter_mut())
        .find(|plan| plan.guid == guid)
}

/// A failure of a broker's catalog as a job's error.
fn failed(e: String) -> JobError {
    JobError {
        code: 270012,
        title: "CF-ServiceBrokerCatalogInvalid".into(),
        detail: e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::osb::check_catalog;

    fn broker() -> Broker {
        Broker {
            guid: "b".into(),
            name: "db-broker".into(),
            url: "http://broker".into(),
            username: "u".into(),
            password: "s3cr3t".into(),
            space: None,
            offerings: Vec::new(),
            created_at: now(),
            updated_at: now(),
        }
    }

    fn catalog(plans: &[&str]) -> Vec<CatalogOffering> {
        let plans: Vec<Value> = plans
            .iter()
            .map(|id| json!({"id": id, "name": format!("plan-{id}"), "description": "d"}))
            .collect();
        if plans.is_empty() {
            return Vec::new();
        }
        check_catalog(&json!({"services": [{
            "id": "s", "name": "db", "description": "a database", "bindable": true,
            "plans": plans,
        }]}))
        .unwrap()
    }

    /// A catalog fetched again keeps the guids and the visibility of what
    /// it still has; a plan or an offering it dropped goes, unless it has
    /// instances: then it stays, deactivated, warned of once. A
    /// space-scoped broker's plans are its space's; no debug output holds
    /// the password.
    #[test]
    fn a_catalog_fetched_again_keeps_what_it_has_and_what_has_instances() {
        let mut broker = broker();
        assert!(reconcile(&mut broker, catalog(&["p1", "p2", "p3"]), &BTreeSet::new()).is_empty());
        let offering = broker.offerings[0].guid.clone();
        let guids: Vec<String> = broker.offerings[0]
            .plans
            .iter()
            .map(|p| p.guid.clone())
            .collect();
        assert!(
            broker.offerings[0]
                .plans
                .iter()
                .all(|plan| plan.visibility == "admin")
        );
        broker.offerings[0].plans[0].visibility = "public".into();

        let in_use = BTreeSet::from([guids[1].clone()]);
        let warnings = reconcile(&mut broker, catalog(&["p1", "p4"]), &in_use);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("the plans plan-p2 are gone"),
            "{warnings:?}"
        );
        let offered = &broker.offerings[0];
        assert_eq!(offered.guid, offering);
        let plans: Vec<(&str, bool, &str)> = offered
            .plans
            .iter()
            .map(|plan| (plan.id(), plan.gone, plan.visibility.as_str()))
            .collect();
        assert_eq!(
            plans,
            [
                ("p1", false, "public"),
                ("p4", false, "admin"),
                ("p2", true, "admin")
            ]
        );
        assert_eq!(offered.plans[0].guid, guids[0]);
        assert_eq!(offered.plans[2].guid, guids[1]);
        // again: warned of no more; the offering gone, kept for its plan's instance
        assert!(reconcile(&mut broker, catalog(&["p1", "p4"]), &in_use).is_empty());
        let warnings = reconcile(&mut broker, catalog(&[]), &in_use);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("Service offering db is gone"),
            "{warnings:?}"
        );
        assert!(broker.offerings[0].gone);
        assert_eq!(broker.offerings[0].plans.len(), 1);
        assert!(reconcile(&mut broker, catalog(&[]), &BTreeSet::new()).is_empty());
        assert!(broker.offerings.is_empty());

        let mut scoped = Broker {
            space: Some("space".into()),
            ..broker
        };
        reconcile(&mut scoped, catalog(&["p1"]), &BTreeSet::new());
        assert_eq!(scoped.offerings[0].plans[0].visibility, "space");
        let shown = format!("{scoped:?}");
        assert!(!shown.contains("s3cr3t"), "{shown}");
    }
}
