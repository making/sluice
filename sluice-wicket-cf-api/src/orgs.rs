//! The organizations, their spaces, the roles in them and the users the roles name
//! (`/v3/organizations`, `/v3/spaces`, `/v3/roles`, `/v3/users`), as `cf create-org`, `cf
//! create-space`, `cf target`, `cf set-org-role`, `cf set-space-role`, `cf org-users` and `cf
//! space-users` use them; who may do what is [`crate::roles`].
//!
//! An admin makes and deletes the organizations; an organization's manager makes, renames and
//! deletes its spaces and gives its roles; a space's manager its own. A space deleted takes what
//! it holds with it, as on CC: its apps (gone from the wicket), routes, service instances (a
//! keyvalue one's data) and roles; but a broker's instance, which only its broker deletes: one
//! refuses the deletion until `cf delete-service`. A role names a user by the name it logs in
//! with (its `user_id` derived from it), a client by its id; the roles a group of the provider
//! gives (`--group-role`) are the provider's, not listed here.

use std::collections::BTreeSet;

use http::{Method, StatusCode};
use serde_json::{Value, json};

use crate::api::{Api, ApiError, Reply, Req, json as respond};
use crate::roles::{Need, RoleType};
use crate::store::{Purge, Role, State, User, guid, now};
use crate::views;

/// The longest name of an organization or a space, as on CC.
const MAX_NAME: usize = 255;

fn org_not_found() -> ApiError {
    ApiError::not_found("Organization")
}

fn space_not_found() -> ApiError {
    ApiError::not_found("Space")
}

/// An organization's or a space's name, trimmed; what is wrong with it.
fn name_of(body: &Value) -> Result<String, ApiError> {
    body["name"]
        .as_str()
        .map(str::trim)
        .filter(|name| !name.is_empty() && name.len() <= MAX_NAME)
        .map(String::from)
        .ok_or_else(|| ApiError::unprocessable(format!("Name must be 1 to {MAX_NAME} characters")))
}

fn org_taken(name: &str) -> ApiError {
    ApiError::unprocessable(format!("Organization '{name}' already exists."))
}

/// What the CLI reads as a space's name taken (`cf create-space` then says it exists).
fn space_taken() -> ApiError {
    ApiError::unprocessable("Name must be unique per organization")
}

/// A role as `/v3/roles` shows it.
fn role_view(base: &str, role: &Role) -> Value {
    let (org, space) = match role.kind.of_space() {
        true => (Value::Null, json!({"guid": role.at})),
        false => (json!({"guid": role.at}), Value::Null),
    };
    let mut links = json!({
        "self": {"href": format!("{base}/v3/roles/{}", role.guid)},
        "user": {"href": format!("{base}/v3/users/{}", role.user)},
    });
    match role.kind.of_space() {
        true => links["space"] = json!({"href": format!("{base}/v3/spaces/{}", role.at)}),
        false => {
            links["organization"] = json!({"href": format!("{base}/v3/organizations/{}", role.at)})
        }
    }
    json!({
        "guid": role.guid, "created_at": role.created_at, "updated_at": role.created_at,
        "type": role.kind.as_str(),
        "relationships": {
            "user": {"data": {"guid": role.user}},
            "organization": {"data": org},
            "space": {"data": space},
        },
        "links": links,
    })
}

/// A user as `/v3/users` shows it: a client has no name, its id presented.
fn user_view(base: &str, user: &User) -> Value {
    json!({
        "guid": user.guid, "created_at": user.created_at, "updated_at": user.created_at,
        "username": user.username, "origin": user.origin,
        "presentation_name": user.username.as_deref().unwrap_or(&user.guid),
        "metadata": {"labels": {}, "annotations": {}},
        "links": {"self": {"href": format!("{base}/v3/users/{}", user.guid)}},
    })
}

/// The one quota, of no limit: `cf org` shows it.
fn quota_view(base: &str) -> Value {
    json!({
        "guid": "default", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        "name": "default",
        "apps": {
            "total_memory_in_mb": null, "per_process_memory_in_mb": null, "total_instances": null,
            "per_app_tasks": null, "log_rate_limit_in_bytes_per_second": null,
        },
        "services": {"paid_services_allowed": true, "total_service_instances": null, "total_service_keys": null},
        "routes": {"total_routes": null, "total_reserved_ports": null},
        "domains": {"total_domains": null},
        "relationships": {"organizations": {"data": []}},
        "links": {"self": {"href": format!("{base}/v3/organization_quotas/default")}},
    })
}

/// The page of `items` the query asks, with `included`.
fn listing(req: &Req, items: Vec<Value>, included: Option<Value>) -> Reply {
    Ok(respond(
        StatusCode::OK,
        &views::page(
            &req.base,
            &req.path,
            &req.query,
            views::select(items, &req.query),
            included,
        ),
    ))
}

/// Whether the request asks `include` to hold `resource`.
fn includes(req: &Req, resource: &str) -> bool {
    req.q("include")
        .is_some_and(|include| include.split(',').any(|named| named == resource))
}

/// The guid a body's `relationships.<what>.data.guid` names.
fn related<'a>(body: &'a Value, what: &str) -> Option<&'a str> {
    body["relationships"][what]["data"]["guid"].as_str()
}

impl Api {
    /// `/v3/organizations*`, `/v3/organization_quotas*`, `/v3/spaces*` but a space's manifest
    /// and security groups, `/v3/roles*`, `/v3/users*`.
    pub(crate) async fn orgs(&self, req: &Req, segments: &[&str]) -> Reply {
        let base = req.base.as_str();
        let access = req.access()?;
        match (&req.method, segments) {
            (&Method::GET, ["organizations"]) => {
                let items = self.store.read(|state| {
                    state
                        .orgs
                        .iter()
                        .filter(|org| access.sees_org(state, &org.guid))
                        .map(|org| views::org(base, org))
                        .collect()
                });
                listing(req, items, None)
            }
            (&Method::POST, ["organizations"]) => {
                if !access.admin {
                    return Err(ApiError::not_authorized("an admin's"));
                }
                let name = name_of(&req.json()?)?;
                let view = self
                    .store
                    .update(|state| {
                        if state.org_named(&name).is_some() {
                            return Err(org_taken(&name));
                        }
                        Ok(views::org(base, state.create_org(&name)))
                    })
                    .await?;
                println!("ORGS: {}: created organization {name}", access.name);
                Ok(respond(StatusCode::CREATED, &view))
            }
            (&Method::GET, ["organizations", guid]) => self
                .store
                .read(|state| {
                    let org = state.org(guid).filter(|_| access.sees_org(state, guid))?;
                    Some(views::org(base, org))
                })
                .map(|org| respond(StatusCode::OK, &org))
                .ok_or_else(org_not_found),
            (&Method::PATCH, ["organizations", guid]) => {
                let body: Value = req.json()?;
                let view = self
                    .store
                    .update(|state| {
                        if state.org(guid).is_none() || !access.sees_org(state, guid) {
                            return Err(org_not_found());
                        }
                        if !access.manages_org(guid) {
                            return Err(ApiError::not_authorized("an organization manager's"));
                        }
                        if !body["name"].is_null() {
                            let name = name_of(&body)?;
                            if state
                                .org_named(&name)
                                .is_some_and(|other| other.guid != *guid)
                            {
                                return Err(org_taken(&name));
                            }
                            let org = state.orgs.iter_mut().find(|org| org.guid == *guid);
                            org.expect("found above").name = name;
                        }
                        Ok(views::org(base, state.org(guid).expect("found above")))
                    })
                    .await?;
                Ok(respond(StatusCode::OK, &view))
            }
            (&Method::DELETE, ["organizations", guid]) => self.delete_org(req, guid).await,
            (&Method::GET, ["organizations", guid, "domains"]) => {
                self.seen_org(req, guid)?;
                let items = self.store.read(|state| {
                    state
                        .domains
                        .iter()
                        .map(|domain| views::domain(base, domain))
                        .collect()
                });
                listing(req, items, None)
            }
            (&Method::GET, ["organizations", guid, "domains", "default"]) => {
                self.seen_org(req, guid)?;
                self.store
                    .read(|state| {
                        state
                            .domains
                            .first()
                            .map(|domain| views::domain(base, domain))
                    })
                    .map(|domain| respond(StatusCode::OK, &domain))
                    .ok_or_else(|| ApiError::not_found("Domain"))
            }
            (
                &Method::GET,
                [
                    "organizations",
                    guid,
                    "relationships",
                    "default_isolation_segment",
                ],
            ) => {
                self.seen_org(req, guid)?;
                Ok(respond(StatusCode::OK, &json!({"data": null, "links": {}})))
            }
            (&Method::GET, ["organization_quotas"]) => listing(req, vec![quota_view(base)], None),
            (&Method::GET, ["organization_quotas", "default"]) => {
                Ok(respond(StatusCode::OK, &quota_view(base)))
            }
            (&Method::GET, ["spaces"]) => self.store.read(|state| {
                let spaces: Vec<_> = state
                    .spaces
                    .iter()
                    .filter(|space| access.can(state, &space.guid, Need::Read))
                    .collect();
                let included = includes(req, "organization").then(|| {
                    let named: BTreeSet<&str> =
                        spaces.iter().map(|space| space.org.as_str()).collect();
                    let orgs: Vec<Value> = state
                        .orgs
                        .iter()
                        .filter(|org| named.contains(org.guid.as_str()))
                        .map(|org| views::org(base, org))
                        .collect();
                    json!({"organizations": orgs})
                });
                let items = spaces
                    .into_iter()
                    .map(|space| views::space(base, space))
                    .collect();
                listing(req, items, included)
            }),
            (&Method::POST, ["spaces"]) => {
                let body: Value = req.json()?;
                let name = name_of(&body)?;
                let org = related(&body, "organization").ok_or_else(|| {
                    ApiError::unprocessable("relationships.organization names the organization")
                })?;
                let view = self.store.update(|state| {
                    if state.org(org).is_none() || !access.sees_org(state, org) {
                        return Err(ApiError::unprocessable(
                            "Invalid organization. Ensure the organization exists and you have access to it.",
                        ));
                    }
                    if !access.manages_org(org) {
                        return Err(ApiError::not_authorized("an organization manager's"));
                    }
                    if state.space_named(org, &name).is_some() {
                        return Err(space_taken());
                    }
                    Ok(views::space(base, state.create_space(&name, org)))
                }).await?;
                println!("ORGS: {}: created space {name}", access.name);
                Ok(respond(StatusCode::CREATED, &view))
            }
            (&Method::GET, ["spaces", guid]) => self
                .store
                .read(|state| {
                    access.check(state, guid, Need::Read, "Space")?;
                    state
                        .space(guid)
                        .map(|space| views::space(base, space))
                        .ok_or_else(space_not_found)
                })
                .map(|space| respond(StatusCode::OK, &space)),
            (&Method::PATCH, ["spaces", guid]) => {
                let body: Value = req.json()?;
                let view = self
                    .store
                    .update(|state| {
                        let space = state.space(guid).cloned().ok_or_else(space_not_found)?;
                        access.check(state, guid, Need::Manage, "Space")?;
                        if !body["name"].is_null() {
                            let name = name_of(&body)?;
                            if state
                                .space_named(&space.org, &name)
                                .is_some_and(|other| other.guid != *guid)
                            {
                                return Err(space_taken());
                            }
                            let found = state.spaces.iter_mut().find(|space| space.guid == *guid);
                            found.expect("found above").name = name;
                        }
                        Ok(views::space(base, state.space(guid).expect("found above")))
                    })
                    .await?;
                Ok(respond(StatusCode::OK, &view))
            }
            (&Method::DELETE, ["spaces", guid]) => self.delete_space(req, guid).await,
            (&Method::GET, ["spaces", guid, "features", name]) => {
                self.store
                    .read(|state| access.check(state, guid, Need::Read, "Space"))?;
                Ok(respond(
                    StatusCode::OK,
                    &json!({"name": name, "description": "", "enabled": false}),
                ))
            }
            (&Method::GET, ["spaces", guid, "features"]) => {
                self.store
                    .read(|state| access.check(state, guid, Need::Read, "Space"))?;
                Ok(respond(
                    StatusCode::OK,
                    &json!({"resources": [{"name": "ssh", "description": "", "enabled": false}], "pagination": {"total_results": 1, "total_pages": 1}}),
                ))
            }
            (&Method::GET, ["spaces", guid, "relationships", "isolation_segment"]) => {
                self.store
                    .read(|state| access.check(state, guid, Need::Read, "Space"))?;
                Ok(respond(StatusCode::OK, &json!({"data": null, "links": {}})))
            }
            (&Method::GET, ["roles"]) => self.store.read(|state| {
                let roles: Vec<&Role> = state
                    .roles
                    .iter()
                    .filter(|role| role_seen(access, state, role))
                    .collect();
                let mut included = serde_json::Map::new();
                if includes(req, "user") {
                    let named: BTreeSet<&str> =
                        roles.iter().map(|role| role.user.as_str()).collect();
                    let users: Vec<Value> = state
                        .users
                        .iter()
                        .filter(|user| named.contains(user.guid.as_str()))
                        .map(|user| user_view(base, user))
                        .collect();
                    included.insert("users".into(), json!(users));
                }
                if includes(req, "space") {
                    let named: BTreeSet<&str> = roles.iter().map(|role| role.at.as_str()).collect();
                    let spaces: Vec<Value> = state
                        .spaces
                        .iter()
                        .filter(|space| named.contains(space.guid.as_str()))
                        .map(|space| views::space(base, space))
                        .collect();
                    included.insert("spaces".into(), json!(spaces));
                }
                if includes(req, "organization") {
                    let named: BTreeSet<&str> = roles.iter().map(|role| role.at.as_str()).collect();
                    let orgs: Vec<Value> = state
                        .orgs
                        .iter()
                        .filter(|org| named.contains(org.guid.as_str()))
                        .map(|org| views::org(base, org))
                        .collect();
                    included.insert("organizations".into(), json!(orgs));
                }
                let items = roles
                    .into_iter()
                    .map(|role| role_view(base, role))
                    .collect();
                listing(
                    req,
                    items,
                    (!included.is_empty()).then_some(Value::Object(included)),
                )
            }),
            (&Method::GET, ["roles", guid]) => self
                .store
                .read(|state| {
                    let role = state
                        .role(guid)
                        .filter(|role| role_seen(access, state, role))?;
                    Some(role_view(base, role))
                })
                .map(|role| respond(StatusCode::OK, &role))
                .ok_or_else(|| ApiError::not_found("Role")),
            (&Method::POST, ["roles"]) => self.create_role(req).await,
            (&Method::DELETE, ["roles", guid]) => {
                let role = self
                    .store
                    .update(|state| {
                        let role = state
                            .role(guid)
                            .filter(|role| role_seen(access, state, role))
                            .cloned()
                            .ok_or_else(|| ApiError::not_found("Role"))?;
                        may_give(access, state, &role)?;
                        state.roles.retain(|kept| kept.guid != *guid);
                        Ok::<_, ApiError>(role)
                    })
                    .await?;
                println!(
                    "ORGS: {}: removed the role {} of {} at {}",
                    access.name,
                    role.kind.as_str(),
                    role.user,
                    role.at
                );
                self.job(req, "role.delete", Vec::new(), None).await
            }
            (&Method::GET, ["users"]) => {
                let items = self.store.read(|state| {
                    users_seen(access, state)
                        .into_iter()
                        .map(|user| user_view(base, &user))
                        .collect()
                });
                listing(req, items, None)
            }
            (&Method::GET, ["users", guid]) => self
                .store
                .read(|state| {
                    users_seen(access, state)
                        .into_iter()
                        .find(|user| user.guid == *guid)
                        .map(|user| user_view(base, &user))
                })
                .map(|user| respond(StatusCode::OK, &user))
                .ok_or_else(|| ApiError::not_found("User")),
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        }
    }

    /// The organization `guid`, if the request's holder sees it.
    fn seen_org(&self, req: &Req, guid: &str) -> Result<(), ApiError> {
        let access = req.access()?;
        self.store
            .read(|state| state.org(guid).is_some() && access.sees_org(state, guid))
            .then_some(())
            .ok_or_else(org_not_found)
    }

    /// A role given: to a user by its name (and origin), or by its guid, a client's id among
    /// them; in a space, to one with a role in its organization, as CC asks.
    async fn create_role(&self, req: &Req) -> Reply {
        let access = req.access()?;
        let body: Value = req.json()?;
        let kind = body["type"]
            .as_str()
            .and_then(RoleType::parse)
            .ok_or_else(|| {
                ApiError::unprocessable("Type must be a role of an organization or a space")
            })?;
        let user = &body["relationships"]["user"]["data"];
        let at = match kind.of_space() {
            true => related(&body, "space"),
            false => related(&body, "organization"),
        }
        .ok_or_else(|| {
            ApiError::unprocessable(match kind.of_space() {
                true => "relationships.space names the space of a space role",
                false => {
                    "relationships.organization names the organization of an organization role"
                }
            })
        })?
        .to_string();
        // the user named: a name is anyone's who logs in by it, a guid a known user's or a client's
        let named = match (user["username"].as_str(), user["guid"].as_str()) {
            (Some(username), _) if !username.is_empty() => Some(User {
                guid: crate::uaa::user_id(username),
                username: Some(username.to_string()),
                origin: Some(
                    user["origin"]
                        .as_str()
                        .filter(|origin| !origin.is_empty())
                        .unwrap_or(self.uaa.origin())
                        .to_string(),
                ),
                created_at: now(),
            }),
            (_, Some(guid)) => self
                .store
                .read(|state| state.user(guid).cloned())
                .or_else(|| {
                    self.uaa.knows_client(guid).then(|| User {
                        guid: guid.to_string(),
                        username: None,
                        origin: None,
                        created_at: now(),
                    })
                }),
            _ => None,
        };
        let user = named.ok_or_else(|| {
            ApiError::unprocessable(
                "Invalid user. Ensure that the user exists and you have access to it.",
            )
        })?;
        let view = self.store.update(|state| {
            let (place, name) = match kind.of_space() {
                true => {
                    let space = state
                        .space(&at)
                        .filter(|_| access.can(state, &at, Need::Read))
                        .ok_or_else(|| ApiError::unprocessable("Invalid space. Ensure that the space exists and you have access to it."))?;
                    ("space", space.name.clone())
                }
                false => {
                    let org = state
                        .org(&at)
                        .filter(|_| access.sees_org(state, &at))
                        .ok_or_else(|| ApiError::unprocessable("Invalid organization. Ensure that the organization exists and you have access to it."))?;
                    ("organization", org.name.clone())
                }
            };
            let role = Role {
                guid: guid(),
                kind,
                user: user.guid.clone(),
                at: at.clone(),
                created_at: now(),
            };
            may_give(access, state, &role)?;
            let presented = user.username.as_deref().unwrap_or(&user.guid);
            if state
                .roles
                .iter()
                .any(|kept| kept.user == role.user && kept.kind == kind && kept.at == at)
            {
                return Err(ApiError::unprocessable(format!(
                    "User '{presented}' already has '{}' role in {place} '{name}'.",
                    kind.as_str()
                )));
            }
            if kind.of_space() {
                let org = state.space(&at).map(|space| space.org.clone()).unwrap_or_default();
                let member = state
                    .roles
                    .iter()
                    .any(|kept| kept.user == role.user && !kept.kind.of_space() && kept.at == org);
                if !member {
                    return Err(ApiError::unprocessable(
                        "Users cannot be assigned roles in a space if they do not have a role in that space's organization.",
                    ));
                }
            }
            if state.user(&user.guid).is_none() {
                state.users.push(user.clone());
            }
            state.roles.push(role.clone());
            Ok(role_view(&req.base, &role))
        }).await?;
        println!(
            "ORGS: {}: gave {} the role {} at {at}",
            access.name,
            user.username.as_deref().unwrap_or(&user.guid),
            kind.as_str()
        );
        Ok(respond(StatusCode::CREATED, &view))
    }

    /// Deletes the space and what it holds, by an organization manager.
    async fn delete_space(&self, req: &Req, guid: &str) -> Reply {
        let access = req.access()?;
        let space = self.store.read(|state| {
            let space = state.space(guid).cloned().ok_or_else(space_not_found)?;
            access.check(state, guid, Need::Read, "Space")?;
            match access.manages_org(&space.org) {
                true => Ok(space),
                false => Err(ApiError::not_authorized("an organization manager's")),
            }
        })?;
        self.empty(std::slice::from_ref(&space.guid)).await?;
        println!("ORGS: {}: deleted space {}", access.name, space.name);
        self.job(req, "space.delete", Vec::new(), None).await
    }

    /// Deletes the organization, its spaces and what they hold, by an admin.
    async fn delete_org(&self, req: &Req, guid: &str) -> Reply {
        let access = req.access()?;
        let (org, spaces) = self.store.read(|state| {
            let org = state
                .org(guid)
                .filter(|_| access.sees_org(state, guid))
                .cloned()
                .ok_or_else(org_not_found)?;
            if !access.admin {
                return Err(ApiError::not_authorized("an admin's"));
            }
            let spaces: Vec<String> = state
                .spaces
                .iter()
                .filter(|space| space.org == *guid)
                .map(|space| space.guid.clone())
                .collect();
            Ok((org, spaces))
        })?;
        self.empty(&spaces).await?;
        self.store
            .update(|state| {
                state.roles.retain(|role| role.at != *guid);
                for plan in state
                    .brokers
                    .iter_mut()
                    .flat_map(|broker| &mut broker.offerings)
                    .flat_map(|offering| &mut offering.plans)
                {
                    plan.orgs.retain(|kept| kept != guid);
                    if plan.visibility == "organization" && plan.orgs.is_empty() {
                        plan.visibility = "admin".into();
                    }
                }
                state.orgs.retain(|kept| kept.guid != *guid);
            })
            .await;
        println!("ORGS: {}: deleted organization {}", access.name, org.name);
        self.job(req, "organization.delete", Vec::new(), None).await
    }

    /// Deletes the spaces and what they hold: their apps (gone from the wicket), routes,
    /// service instances (a keyvalue one's data with them), space-scoped brokers and roles.
    /// Refused, nothing deleted, while one holds a broker's instance: its broker deletes it.
    async fn empty(&self, spaces: &[String]) -> Result<(), ApiError> {
        let ours = |space: &str| spaces.iter().any(|guid| guid == space);
        let apps: Vec<(String, String)> = self.store.read(|state| {
            let brokered: Vec<&str> = state
                .instances
                .iter()
                .filter(|instance| ours(&instance.space) && instance.brokered())
                .map(|instance| instance.name.as_str())
                .collect();
            if !brokered.is_empty() {
                return Err(ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    code: 10006,
                    title: "CF-AssociationNotEmpty",
                    detail: format!(
                        "service instances of a broker are its broker's to delete: cf delete-service {} first",
                        brokered.join(", ")
                    ),
                });
            }
            Ok(state
                .apps
                .iter()
                .filter(|app| ours(&app.space))
                .map(|app| (app.guid.clone(), app.name.clone()))
                .collect())
        })?;
        for (app, name) in apps {
            self.controller.remove(&app, &name).await;
        }
        let routes = self
            .store
            .update(|state| {
                if let Some(app) = state.apps.iter().find(|app| ours(&app.space)) {
                    return Err(ApiError::unprocessable(format!(
                        "app {} was pushed meanwhile: delete again",
                        app.name
                    )));
                }
                let routes: Vec<String> = state
                    .routes
                    .iter()
                    .filter(|route| ours(&route.space))
                    .map(|route| route.guid.clone())
                    .collect();
                state.routes.retain(|route| !ours(&route.space));
                let gone: Vec<_> = state
                    .instances
                    .iter()
                    .filter(|instance| ours(&instance.space))
                    .cloned()
                    .collect();
                for instance in &gone {
                    if instance.offering == crate::services::KEYVALUE {
                        state.purges.push(Purge {
                            store: instance.plan.clone(),
                            namespace: crate::services::namespace(&instance.guid),
                            instance: instance.name.clone(),
                            purged: Vec::new(),
                        });
                    }
                }
                let gone: BTreeSet<String> =
                    gone.into_iter().map(|instance| instance.guid).collect();
                state
                    .bindings
                    .retain(|binding| !gone.contains(&binding.instance));
                state.instances.retain(|instance| !ours(&instance.space));
                state
                    .brokers
                    .retain(|broker| !broker.space.as_deref().is_some_and(ours));
                for group in &mut state.security_groups {
                    group.running_spaces.retain(|space| !ours(space));
                    group.staging_spaces.retain(|space| !ours(space));
                }
                state.roles.retain(|role| !ours(&role.at));
                state.spaces.retain(|space| !ours(&space.guid));
                Ok::<_, ApiError>(routes)
            })
            .await?;
        for route in routes {
            self.controller.forget(&route);
        }
        self.controller.purge_pending().await;
        Ok(())
    }
}

/// Whether `access` sees the role: of an organization it sees, or a space it reads.
fn role_seen(access: &crate::roles::Access, state: &State, role: &Role) -> bool {
    match role.kind.of_space() {
        true => access.can(state, &role.at, Need::Read),
        false => access.sees_org(state, &role.at),
    }
}

/// Whether `access` may give or take `role`: an organization's by its manager, a space's by
/// the organization's or the space's.
fn may_give(access: &crate::roles::Access, state: &State, role: &Role) -> Result<(), ApiError> {
    let may = match role.kind.of_space() {
        true => access.can(state, &role.at, Need::Manage),
        false => access.manages_org(&role.at),
    };
    match may {
        true => Ok(()),
        false => Err(ApiError::not_authorized(match role.kind.of_space() {
            true => "a space manager's or organization manager's",
            false => "an organization manager's",
        })),
    }
}

/// The users `access` sees: an admin all, another those of the roles it sees, and itself.
fn users_seen(access: &crate::roles::Access, state: &State) -> Vec<User> {
    let mut seen: Vec<User> = state
        .users
        .iter()
        .filter(|user| {
            access.admin
                || user.guid == access.user
                || state
                    .roles
                    .iter()
                    .any(|role| role.user == user.guid && role_seen(access, state, role))
        })
        .cloned()
        .collect();
    if !seen.iter().any(|user| user.guid == access.user) {
        seen.push(User {
            guid: access.user.clone(),
            username: Some(access.name.clone()).filter(|name| *name != access.user),
            origin: None,
            created_at: now(),
        });
    }
    seen
}
