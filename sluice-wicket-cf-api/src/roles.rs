//! Who may do what: the roles of Cloud Controller, in the organizations and their spaces, and
//! the admins. A request's [`Access`] is what its token's holder may: the roles given it
//! (`cf set-org-role`, `cf set-space-role`, kept in the state), those its groups map to
//! (`--group-role`, the groups the claim `--oidc-groups-claim` of its login's ID token lists),
//! and admin, as `--admin` / `--admin-client` or a group mapped to `admin` say; in the open mode
//! without them, everyone.
//!
//! | | sees | may |
//! |---|---|---|
//! | admin | everything | everything |
//! | organization manager | the organization, its spaces and what they hold | the spaces, the roles of the organization and its spaces, the organization's name |
//! | organization user, auditor, billing manager | the organization | |
//! | space manager | the space and what it holds | the space's name and roles |
//! | space auditor | the space and what it holds | |
//! | space supporter | the space and what it holds | an app's lifecycle, scale, routes, packages, builds, deployments and bindings |
//! | space developer | the space and what it holds | all of it: apps, their environment, service instances |
//!
//! A resource of a space its holder does not see is not found (404), one it sees and may not
//! change refused (403), as CC answers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::ApiError;
use crate::security_groups::Editors;
use crate::store::State;
use crate::uaa::Claims;

/// A role's type, as CC names it.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RoleType {
    OrganizationUser,
    OrganizationAuditor,
    OrganizationManager,
    OrganizationBillingManager,
    SpaceAuditor,
    SpaceDeveloper,
    SpaceManager,
    SpaceSupporter,
}

const TYPES: [RoleType; 8] = [
    RoleType::OrganizationUser,
    RoleType::OrganizationAuditor,
    RoleType::OrganizationManager,
    RoleType::OrganizationBillingManager,
    RoleType::SpaceAuditor,
    RoleType::SpaceDeveloper,
    RoleType::SpaceManager,
    RoleType::SpaceSupporter,
];

impl RoleType {
    pub fn as_str(self) -> &'static str {
        match self {
            RoleType::OrganizationUser => "organization_user",
            RoleType::OrganizationAuditor => "organization_auditor",
            RoleType::OrganizationManager => "organization_manager",
            RoleType::OrganizationBillingManager => "organization_billing_manager",
            RoleType::SpaceAuditor => "space_auditor",
            RoleType::SpaceDeveloper => "space_developer",
            RoleType::SpaceManager => "space_manager",
            RoleType::SpaceSupporter => "space_supporter",
        }
    }

    pub fn parse(text: &str) -> Option<RoleType> {
        TYPES.into_iter().find(|kind| kind.as_str() == text)
    }

    /// Whether it is a space's (else an organization's).
    pub fn of_space(self) -> bool {
        self.as_str().starts_with("space_")
    }
}

/// What a request needs of a space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    /// To see it and what it holds, but the secrets: the environment, credentials.
    Read,
    /// To run its apps: start, stop, scale, routes, packages, builds, deployments, bindings.
    Operate,
    /// To develop in it: the apps themselves, their environment, the service instances.
    Develop,
    /// To manage it: its name and its roles.
    Manage,
}

/// What a group of the provider maps to (`--group-role`).
#[derive(Clone, Debug, PartialEq)]
pub enum Grant {
    Admin,
    /// A role of the organization named.
    Org(RoleType, String),
    /// A role of the space named, of the organization named.
    Space(RoleType, String, String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupRole {
    pub group: String,
    pub grant: Grant,
}

/// Parses `--group-role` values, each `<group>=<role>`, or comma-separated: `admin`,
/// `<organization role>@<org>` or `<space role>@<org>/<space>`. A group's name may hold `=`
/// (the last one ends it), not `,`.
pub fn parse_group_roles<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<GroupRole>, String> {
    let mut mapped = Vec::new();
    for entry in values
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let wrong = |why: &str| {
            format!(
                "--group-role {entry}: {why}; expected <group>=admin, \
                 <group>=organization_<role>@<org> or <group>=space_<role>@<org>/<space>"
            )
        };
        let (group, role) = entry
            .rsplit_once('=')
            .filter(|(group, _)| !group.is_empty())
            .ok_or_else(|| wrong("no group"))?;
        let grant = match role.split_once('@') {
            None if role == "admin" => Grant::Admin,
            None => return Err(wrong("no organization")),
            Some((kind, at)) => {
                let kind = RoleType::parse(kind).ok_or_else(|| wrong("not a role"))?;
                match (kind.of_space(), at.split_once('/')) {
                    (false, None) if !at.is_empty() => Grant::Org(kind, at.into()),
                    (true, Some((org, space))) if !org.is_empty() && !space.is_empty() => {
                        Grant::Space(kind, org.into(), space.into())
                    }
                    (false, _) => return Err(wrong("an organization role names an organization")),
                    (true, _) => return Err(wrong("a space role names <org>/<space>")),
                }
            }
        };
        mapped.push(GroupRole {
            group: group.to_string(),
            grant,
        });
    }
    Ok(mapped)
}

/// Who holds which roles but those given through the API.
#[derive(Clone, Debug, PartialEq)]
pub struct Policy {
    /// `--admin`, `--admin-client`.
    pub admins: Editors,
    /// The open mode without `--admin`: whoever logs in is an admin.
    pub everyone_admin: bool,
    /// The ID token's claim listing a user's groups (`--oidc-groups-claim`).
    pub groups_claim: String,
    pub group_roles: Vec<GroupRole>,
}

impl Default for Policy {
    fn default() -> Policy {
        Policy {
            admins: Editors::default(),
            everyone_admin: true,
            groups_claim: "groups".into(),
            group_roles: Vec::new(),
        }
    }
}

impl Policy {
    /// Whether anyone can be an admin: someone must create the organizations and give roles.
    pub fn has_admins(&self) -> bool {
        self.everyone_admin
            || !self.admins.rules.is_empty()
            || !self.admins.clients.is_empty()
            || self
                .group_roles
                .iter()
                .any(|mapped| mapped.grant == Grant::Admin)
    }

    /// The groups `identity` (an ID token's claims) lists: the claim's strings.
    fn groups<'a>(&self, identity: Option<&'a Value>) -> Vec<&'a str> {
        match identity.map(|identity| &identity[&self.groups_claim]) {
            Some(Value::Array(groups)) => groups.iter().filter_map(Value::as_str).collect(),
            Some(Value::String(group)) => vec![group.as_str()],
            _ => Vec::new(),
        }
    }
}

/// What the holder of a token may.
#[derive(Clone, Debug, PartialEq)]
pub struct Access {
    pub admin: bool,
    /// Its user's guid (a client's id), as the roles name it.
    pub user: String,
    /// Its name: the user's, or the client's id.
    pub name: String,
    /// Its roles, given and of its groups, each with its organization's or space's guid.
    roles: Vec<(RoleType, String)>,
}

impl Access {
    /// The access of `token`, whose login's claims are `identity`.
    pub fn of(state: &State, token: &Claims, identity: Option<&Value>, policy: &Policy) -> Access {
        let user = token.user_id.clone();
        let mut roles: Vec<(RoleType, String)> = state
            .roles
            .iter()
            .filter(|role| role.user == user)
            .map(|role| (role.kind, role.at.clone()))
            .collect();
        let mut admin =
            policy.everyone_admin || policy.admins.check(token, identity, "admin").is_ok();
        let groups = policy.groups(identity);
        for mapped in policy
            .group_roles
            .iter()
            .filter(|mapped| groups.contains(&mapped.group.as_str()))
        {
            match &mapped.grant {
                Grant::Admin => admin = true,
                Grant::Org(kind, org) => {
                    if let Some(org) = state.org_named(org) {
                        roles.push((*kind, org.guid.clone()));
                    }
                }
                Grant::Space(kind, org, space) => {
                    if let Some(space) = state
                        .org_named(org)
                        .and_then(|org| state.space_named(&org.guid, space))
                    {
                        roles.push((*kind, space.guid.clone()));
                    }
                }
            }
        }
        Access {
            admin,
            user,
            name: token.name().to_string(),
            roles,
        }
    }

    /// Whoever: an admin (the tests' and the controller's own).
    pub fn admin(name: &str) -> Access {
        Access {
            admin: true,
            user: name.to_string(),
            name: name.to_string(),
            roles: Vec::new(),
        }
    }

    fn has(&self, at: &str, kinds: &[RoleType]) -> bool {
        self.roles
            .iter()
            .any(|(kind, guid)| guid == at && kinds.contains(kind))
    }

    /// Whether it sees the organization: a role in it or in one of its spaces.
    pub fn sees_org(&self, state: &State, org: &str) -> bool {
        self.admin
            || self.roles.iter().any(|(kind, at)| match kind.of_space() {
                false => at == org,
                true => state.space(at).is_some_and(|space| space.org == org),
            })
    }

    /// Whether it may manage the organization: its spaces, its roles, its name.
    pub fn manages_org(&self, org: &str) -> bool {
        self.admin || self.has(org, &[RoleType::OrganizationManager])
    }

    /// Whether it may `need` of the space `space`.
    pub fn can(&self, state: &State, space: &str, need: Need) -> bool {
        if self.admin {
            return true;
        }
        let org = state.space(space).map_or("", |space| space.org.as_str());
        let any = [
            RoleType::SpaceAuditor,
            RoleType::SpaceDeveloper,
            RoleType::SpaceManager,
            RoleType::SpaceSupporter,
        ];
        match need {
            Need::Read => self.manages_org(org) || self.has(space, &any),
            Need::Operate => self.has(space, &[RoleType::SpaceDeveloper, RoleType::SpaceSupporter]),
            Need::Develop => self.has(space, &[RoleType::SpaceDeveloper]),
            Need::Manage => self.manages_org(org) || self.has(space, &[RoleType::SpaceManager]),
        }
    }

    /// [`Access::can`], else why not: `what` not found where it does not see the space, not
    /// authorized where it does.
    pub fn check(
        &self,
        state: &State,
        space: &str,
        need: Need,
        what: &str,
    ) -> Result<(), ApiError> {
        if self.can(state, space, need) {
            Ok(())
        } else if self.can(state, space, Need::Read) {
            Err(ApiError::not_authorized(match need {
                Need::Read => "reading it",
                Need::Operate => "a space developer's or supporter's",
                Need::Develop => "a space developer's",
                Need::Manage => "a space manager's or organization manager's",
            }))
        } else {
            Err(ApiError::not_found(what))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Role, now};

    /// A user's token, or with the origin `client` a client's, of the id `user_id`.
    fn token(user_id: &str, origin: &str) -> Claims {
        let client = if origin == "client" { user_id } else { "cf" };
        serde_json::from_value(serde_json::json!({
            "jti": "j", "sub": user_id, "scope": [], "client_id": client, "cid": client, "azp": client,
            "grant_type": "password", "user_id": user_id, "origin": origin,
            "user_name": user_id, "email": "", "iat": 0, "exp": 0, "iss": "", "zid": "uaa",
            "aud": [],
        }))
        .unwrap()
    }

    #[test]
    fn the_roles_given_and_of_the_groups_say_what_it_may() {
        let mut state = State::new("org", "dev", &["localhost".into()]);
        let org = state.orgs[0].guid.clone();
        let dev = state.spaces[0].guid.clone();
        let prod = state.create_space("prod", &org).guid.clone();
        let other = state.create_org("other").guid.clone();
        let policy = Policy {
            admins: Editors {
                rules: crate::oidc::parse_rules(["email=root@example.com"], "--admin").unwrap(),
                clients: vec!["ci".into()],
            },
            everyone_admin: false,
            groups_claim: "teams".into(),
            group_roles: parse_group_roles([
                "devs=space_developer@org/dev,leads=organization_manager@org",
                "cn=ops=admin",
                "ghosts=space_auditor@none/dev",
            ])
            .unwrap(),
        };
        let access = |state: &State, user: &str, origin: &str, identity: Value| {
            Access::of(state, &token(user, origin), Some(&identity), &policy)
        };
        let nobody = access(
            &state,
            "u1",
            "oidc",
            serde_json::json!({"teams": ["ghosts"]}),
        );
        assert!(!nobody.admin && !nobody.sees_org(&state, &org));
        assert!(!nobody.can(&state, &dev, Need::Read));

        // a group's space role: the space and its organization, not the other spaces
        let dev_member = access(&state, "u2", "oidc", serde_json::json!({"teams": "devs"}));
        assert!(dev_member.sees_org(&state, &org) && !dev_member.sees_org(&state, &other));
        assert!(dev_member.can(&state, &dev, Need::Develop));
        assert!(!dev_member.can(&state, &dev, Need::Manage));
        assert!(!dev_member.can(&state, &prod, Need::Read));
        let refused = dev_member
            .check(&state, &dev, Need::Manage, "Space")
            .unwrap_err();
        assert_eq!(refused.status, 403);
        let hidden = dev_member
            .check(&state, &prod, Need::Read, "App")
            .unwrap_err();
        assert_eq!(
            (hidden.status.as_u16(), hidden.detail.as_str()),
            (404, "App not found")
        );

        // an organization manager sees its spaces' content and manages them, deploys nothing
        let lead = access(
            &state,
            "u3",
            "oidc",
            serde_json::json!({"teams": ["leads"]}),
        );
        assert!(lead.can(&state, &prod, Need::Manage) && lead.can(&state, &prod, Need::Read));
        assert!(!lead.can(&state, &prod, Need::Operate));
        assert!(lead.manages_org(&org) && !lead.manages_org(&other));

        // given roles: a supporter operates, an auditor reads
        for (kind, at) in [
            (RoleType::SpaceSupporter, &prod),
            (RoleType::SpaceAuditor, &dev),
        ] {
            state.roles.push(Role {
                guid: crate::store::guid(),
                kind,
                user: "u4".into(),
                at: at.clone(),
                created_at: now(),
            });
        }
        let given = access(&state, "u4", "oidc", serde_json::json!({}));
        assert!(
            given.can(&state, &prod, Need::Operate) && !given.can(&state, &prod, Need::Develop)
        );
        assert!(given.can(&state, &dev, Need::Read) && !given.can(&state, &dev, Need::Operate));

        // the admins: a claim rule, a group (whose name may hold '='), a client
        let admins = [
            access(
                &state,
                "u5",
                "oidc",
                serde_json::json!({"email": "root@example.com"}),
            ),
            access(
                &state,
                "u6",
                "oidc",
                serde_json::json!({"teams": ["cn=ops"]}),
            ),
            Access::of(&state, &token("ci", "client"), None, &policy),
        ];
        assert!(admins.iter().all(|access| access.admin), "{admins:?}");
        assert!(!Access::of(&state, &token("cd", "client"), None, &policy).admin);
        // the open mode without --admin: everyone
        let open = Policy::default();
        assert!(Access::of(&state, &token("u7", "uaa"), None, &open).admin);
    }

    #[test]
    fn a_group_role_names_what_it_grants() {
        for (wrong, why) in [
            ("devs", "no group"),
            ("=admin", "no group"),
            ("devs=space_developer", "no organization"),
            ("devs=developer@org/dev", "not a role"),
            (
                "devs=space_developer@org",
                "a space role names <org>/<space>",
            ),
            (
                "devs=organization_user@org/dev",
                "an organization role names an organization",
            ),
            (
                "devs=organization_user@",
                "an organization role names an organization",
            ),
        ] {
            let refused = parse_group_roles([wrong]).unwrap_err();
            assert!(refused.contains(why), "{wrong}: {refused}");
        }
    }
}
