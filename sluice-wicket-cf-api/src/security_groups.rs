//! The security groups: what an app of a Firecracker wicket may connect to, as the wicket's
//! `egress` of its route. A rule of `tcp` to a `destination` and its `ports` is a rule of the
//! wicket per destination's block and per port:
//!
//! ```text
//! {"protocol": "tcp", "destination": "10.0.0.1-10.0.0.3", "ports": "80,443"}
//!   10.0.0.1:80, 10.0.0.2/31:80, 10.0.0.1:443, 10.0.0.2/31:443
//! ```
//!
//! The wicket relays TCP alone: another protocol is refused. A guest has no name server, so a
//! `destination` may also be a name, which CF's are not (`0.0.0.0/0:443` resolves nothing). The
//! rules reach a guest on its kernel's command line: [`MAX_EGRESS`] bytes bound them, a port
//! range's included.
//!
//! The groups bound to the one space, or to every space, apply to its apps at their next start,
//! as the environment does (`Running::egress`); staging's have no effect: no VM stages. Anyone
//! logged in reads them. A rule reaches what the wicket's host reaches, so editing them is for
//! the [`Editors`] alone, not for whoever holds `cloud_controller.admin`, which everyone does.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use http::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{Api, ApiError, Reply, Req, json as respond, no_content};
use crate::oidc::{ClaimRule, allowed};
use crate::store::{SecurityGroup, SecurityRule, State, guid, now};
use crate::uaa::Claims;
use crate::views;

/// The bytes of a route's rules a Firecracker wicket takes (its `vm::MAX_EGRESS`): a group's,
/// with `,` between them, refused beyond it. The groups of an app together are the wicket's
/// to refuse, at its start.
const MAX_EGRESS: usize = 1024;

/// A group's name at most, as on CC.
const MAX_NAME: usize = 250;

/// Who may edit what reaches the network of the cf-api's or the wicket's host: the security
/// groups (`--security-group-editor`, `--security-group-editor-client`), the service brokers
/// (`--service-broker-editor`, `--service-broker-editor-client`); no one by default.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Editors {
    pub rules: Vec<ClaimRule>,
    /// Clients have no ID token for a rule to read.
    pub clients: Vec<String>,
}

impl Editors {
    /// Whether the holder of `token`, whose [`crate::uaa::Uaa::identity`] is `identity`, may, as
    /// the options `--<option>` and `--<option>-client` say.
    pub(crate) fn check(
        &self,
        token: &Claims,
        identity: Option<&Value>,
        option: &str,
    ) -> Result<(), String> {
        if token.origin == "client" {
            return match self.clients.contains(&token.client_id) {
                true => Ok(()),
                false => Err(format!(
                    "the client {} is not of --{option}-client",
                    token.client_id
                )),
            };
        }
        if self.rules.is_empty() {
            return Err(format!("--{option} names no one"));
        }
        let identity = identity.ok_or("this token was replaced by a refresh: log in again")?;
        allowed(&self.rules, identity)
    }
}

/// The destinations of the groups applying to the apps of the space `space`, as the wicket
/// names them, sorted, once each.
pub fn egress(state: &State, space: &str) -> Result<Vec<String>, String> {
    let mut all = BTreeSet::new();
    for group in state
        .security_groups
        .iter()
        .filter(|group| applies(group, space, true))
    {
        all.extend(rules_of(&group.rules).map_err(|e| format!("{}: {e}", group.name))?);
    }
    Ok(all.into_iter().collect())
}

/// Whether `group` applies to the apps of `space`, `running` or staging.
fn applies(group: &SecurityGroup, space: &str, running: bool) -> bool {
    let (global, spaces) = match running {
        true => (group.running, &group.running_spaces),
        false => (group.staging, &group.staging_spaces),
    };
    global || spaces.iter().any(|guid| guid == space)
}

/// The wicket's rules of `rules`, [`MAX_EGRESS`] bytes at most; what is wrong with them.
fn rules_of(rules: &[SecurityRule]) -> Result<Vec<String>, String> {
    let mut all = BTreeSet::new();
    let mut bytes = 0;
    for (n, rule) in rules.iter().enumerate() {
        let wrong = |e: String| format!("Rules[{n}]: {e}");
        if rule.protocol != "tcp" {
            return Err(wrong(format!(
                "protocol {}: the wicket relays tcp alone",
                rule.protocol
            )));
        }
        let ports = rule
            .ports
            .as_deref()
            .ok_or_else(|| wrong("ports are required for tcp".into()))?;
        let ports = ports_of(ports).map_err(wrong)?;
        let hosts = hosts_of(&rule.destination).map_err(wrong)?;
        for host in &hosts {
            for (first, last) in &ports {
                for port in *first..=*last {
                    let text = host.rule(port);
                    let len = text.len();
                    if all.insert(text) {
                        bytes += len + 1;
                    }
                    if bytes > MAX_EGRESS + 1 {
                        return Err(wrong(format!(
                            "a guest takes {MAX_EGRESS} bytes of rules, one per port and per \
                             block of the destination: {} with ports {} goes past them",
                            rule.destination,
                            rule.ports.as_deref().unwrap_or_default()
                        )));
                    }
                }
            }
        }
    }
    Ok(all.into_iter().collect())
}

/// `ports`: `443`, `80,443`, `8000-8100`; each with its last port.
fn ports_of(ports: &str) -> Result<Vec<(u16, u16)>, String> {
    let port = |text: &str| {
        text.trim()
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| format!("ports {ports}: {text}: expected 1 to 65535"))
    };
    ports
        .split(',')
        .map(|part| match part.split_once('-') {
            None => port(part).map(|port| (port, port)),
            Some((first, last)) => match (port(first)?, port(last)?) {
                (first, last) if first <= last => Ok((first, last)),
                _ => Err(format!(
                    "ports {ports}: {part}: a range starts at its lowest"
                )),
            },
        })
        .collect()
}

/// What a wicket's rule names but its port.
#[derive(Clone, Debug, PartialEq)]
enum Host {
    Name(String),
    /// An address and the bits of its prefix.
    Net(IpAddr, u32),
}

impl Host {
    /// The wicket's rule (its `Rule`'s text) of this host and `port`.
    fn rule(&self, port: u16) -> String {
        match self {
            Host::Name(name) => format!("{name}:{port}"),
            Host::Net(IpAddr::V4(ip), 32) => format!("{ip}:{port}"),
            Host::Net(IpAddr::V4(ip), prefix) => format!("{ip}/{prefix}:{port}"),
            Host::Net(IpAddr::V6(ip), 128) => format!("[{ip}]:{port}"),
            Host::Net(IpAddr::V6(ip), prefix) => format!("[{ip}/{prefix}]:{port}"),
        }
    }
}

/// `destination`: comma-separated, as a CC configured so takes them, each an address, a CIDR, a
/// range of addresses or a name.
fn hosts_of(destination: &str) -> Result<Vec<Host>, String> {
    let mut hosts = Vec::new();
    for part in destination.split(',').map(str::trim) {
        if let Ok(ip) = part.parse::<IpAddr>() {
            hosts.push(Host::Net(ip, width(ip)));
        } else if let Some((first, last)) = part
            .split_once('-')
            .and_then(|(first, last)| Some((first.parse().ok()?, last.parse().ok()?)))
        {
            hosts.extend(cover(first, last).map_err(|e| format!("destination {part}: {e}"))?);
        } else if let Some((ip, prefix)) = part.split_once('/') {
            let bad = || format!("destination {part}: not a CIDR");
            let ip: IpAddr = ip.parse().map_err(|_| bad())?;
            let prefix = prefix
                .parse()
                .ok()
                .filter(|prefix| *prefix <= width(ip))
                .ok_or_else(bad)?;
            // as a firewall takes it: the address's bits past the prefix mean nothing
            let bits = number(ip) & !low_number(width(ip) - prefix);
            hosts.push(Host::Net(from_number(ip, bits), prefix));
        } else {
            hosts.push(Host::Name(name_of(part)?));
        }
    }
    Ok(hosts)
}

/// `text` as the wicket takes a name: lowercased, labels of letters, digits, `-` and `_`, the
/// last one starting with a letter (no address in disguise).
fn name_of(text: &str) -> Result<String, String> {
    let name = text.to_ascii_lowercase();
    let label = |label: &str| {
        (1..=63).contains(&label.len())
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
            && !label.starts_with('-')
            && !label.ends_with('-')
    };
    let last = name.rsplit('.').next().unwrap_or_default();
    if name.len() > 253
        || !name.split('.').all(label)
        || !last.starts_with(|first: char| first.is_ascii_lowercase())
    {
        return Err(format!(
            "destination {text}: not an address, a CIDR, a range of addresses or a host name"
        ));
    }
    Ok(name)
}

/// The CIDRs covering `first` to `last`, the fewest.
fn cover(first: IpAddr, last: IpAddr) -> Result<Vec<Host>, String> {
    if first.is_ipv4() != last.is_ipv4() {
        return Err("addresses of one family".into());
    }
    let width = width(first);
    let (mut at, end) = (number(first), number(last));
    if at > end {
        return Err("a range starts at its lowest".into());
    }
    let mut blocks = Vec::new();
    loop {
        // the largest block starting here (`at`'s trailing zeros) ending by `end`
        let aligned = at.trailing_zeros().min(width);
        let remaining = end - at;
        let fits = match remaining.checked_add(1) {
            Some(count) => count.ilog2(),
            None => 128,
        };
        let bits = aligned.min(fits);
        blocks.push(Host::Net(from_number(first, at), width - bits));
        let block_end = at + low_number(bits);
        if block_end == end {
            return Ok(blocks);
        }
        at = block_end + 1;
    }
}

/// The bits of an address of `ip`'s family.
fn width(ip: IpAddr) -> u32 {
    match ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

/// `ip` as a number.
fn number(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(ip) => u128::from(u32::from(ip)),
        IpAddr::V6(ip) => u128::from(ip),
    }
}

/// The address of `like`'s family numbered `number`.
fn from_number(like: IpAddr, number: u128) -> IpAddr {
    match like {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::from(number as u32)),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::from(number)),
    }
}

/// The number with the low `bits` set.
fn low_number(bits: u32) -> u128 {
    match bits {
        128 => u128::MAX,
        bits => (1 << bits) - 1,
    }
}

/// A body's `globally_enabled`.
#[derive(Deserialize, Default)]
struct Enabled {
    running: Option<bool>,
    staging: Option<bool>,
}

/// A body's `relationships`.
#[derive(Deserialize, Default)]
struct Relationships {
    #[serde(default)]
    running_spaces: Spaces,
    #[serde(default)]
    staging_spaces: Spaces,
}

/// A relationship's `{"data": [{"guid": ...}]}`.
#[derive(Deserialize, Default)]
struct Spaces {
    #[serde(default)]
    data: Vec<SpaceRef>,
}

#[derive(Deserialize)]
struct SpaceRef {
    guid: String,
}

impl Spaces {
    /// The guids, which must be spaces'.
    fn guids(&self, state: &State) -> Result<Vec<String>, ApiError> {
        let mut guids = Vec::new();
        for space in &self.data {
            if state.space(&space.guid).is_none() {
                return Err(ApiError::unprocessable(format!(
                    "Spaces with guids [\"{}\"] do not exist, or you do not have access to them.",
                    space.guid
                )));
            }
            if !guids.contains(&space.guid) {
                guids.push(space.guid.clone());
            }
        }
        Ok(guids)
    }
}

#[derive(Deserialize)]
struct Create {
    name: String,
    #[serde(default)]
    rules: Vec<SecurityRule>,
    #[serde(default)]
    globally_enabled: Enabled,
    #[serde(default)]
    relationships: Relationships,
}

#[derive(Deserialize)]
struct Patch {
    name: Option<String>,
    rules: Option<Vec<SecurityRule>>,
    #[serde(default)]
    globally_enabled: Enabled,
}

/// Whether `name` can name a group of `state`, `of` being the one it names.
fn check_name(state: &State, name: &str, of: Option<&str>) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_NAME {
        return Err(ApiError::unprocessable(format!(
            "Name must be 1 to {MAX_NAME} characters"
        )));
    }
    let taken = state
        .security_groups
        .iter()
        .any(|group| group.name == name && Some(group.guid.as_str()) != of);
    match taken {
        // the CLI knows this one: it says so, and goes on
        true => Err(ApiError::unprocessable(format!(
            "Security group with name '{name}' already exists."
        ))),
        false => Ok(()),
    }
}

impl Api {
    /// `/v3/security_groups*`.
    pub(crate) fn security_groups(&self, req: &Req, segments: &[&str]) -> Reply {
        let base = req.base.as_str();
        let found = |group: Option<SecurityGroup>| {
            let group = group.ok_or_else(|| ApiError::not_found("Security group"))?;
            Ok(respond(
                StatusCode::OK,
                &views::security_group(base, &group),
            ))
        };
        let editor = match req.method {
            Method::GET => None,
            _ => Some(self.security_group_editor(req)?),
        };
        let reply = match (&req.method, segments) {
            (&Method::GET, ["security_groups"]) => {
                let groups = self.store.read(|state| state.security_groups.clone());
                self.security_group_page(req, groups)
            }
            (&Method::GET, ["security_groups", guid]) => found(self.security_group(guid)),
            (&Method::POST, ["security_groups"]) => {
                let body: Create = req.json()?;
                rules_of(&body.rules)?;
                let group = self.store.update(|state| {
                    check_name(state, &body.name, None)?;
                    let at = now();
                    let group = SecurityGroup {
                        guid: guid(),
                        name: body.name.clone(),
                        rules: body.rules.clone(),
                        running: body.globally_enabled.running.unwrap_or(false),
                        staging: body.globally_enabled.staging.unwrap_or(false),
                        running_spaces: body.relationships.running_spaces.guids(state)?,
                        staging_spaces: body.relationships.staging_spaces.guids(state)?,
                        created_at: at.clone(),
                        updated_at: at,
                    };
                    state.security_groups.push(group.clone());
                    Ok::<_, ApiError>(group)
                })?;
                Ok(respond(
                    StatusCode::CREATED,
                    &views::security_group(base, &group),
                ))
            }
            (&Method::PATCH, ["security_groups", guid]) => {
                let body: Patch = req.json()?;
                if let Some(rules) = &body.rules {
                    rules_of(rules)?;
                }
                found(self.edit_security_group(guid, |state, group| {
                    if let Some(name) = &body.name {
                        check_name(state, name, Some(&group.guid))?;
                        group.name.clone_from(name);
                    }
                    if let Some(rules) = &body.rules {
                        group.rules.clone_from(rules);
                    }
                    group.running = body.globally_enabled.running.unwrap_or(group.running);
                    group.staging = body.globally_enabled.staging.unwrap_or(group.staging);
                    Ok(())
                })?)
            }
            (&Method::DELETE, ["security_groups", guid]) => {
                let found = self.store.update(|state| {
                    let before = state.security_groups.len();
                    state.security_groups.retain(|group| group.guid != *guid);
                    state.security_groups.len() != before
                });
                if !found {
                    return Err(ApiError::not_found("Security group"));
                }
                self.job(req, "security_group.delete", Vec::new(), None)
            }
            (
                &Method::POST,
                [
                    "security_groups",
                    guid,
                    "relationships",
                    kind @ ("running_spaces" | "staging_spaces"),
                ],
            ) => {
                let body: Spaces = req.json()?;
                let group = self.edit_security_group(guid, |state, group| {
                    let spaces = spaces_of(group, kind);
                    for space in body.guids(state)? {
                        if !spaces.contains(&space) {
                            spaces.push(space);
                        }
                    }
                    Ok(())
                })?;
                let mut group = group.ok_or_else(|| ApiError::not_found("Security group"))?;
                let path = format!("/v3/security_groups/{guid}/relationships/{kind}");
                let data: Vec<Value> = spaces_of(&mut group, kind)
                    .iter()
                    .map(|guid| json!({"guid": guid}))
                    .collect();
                Ok(respond(
                    StatusCode::OK,
                    &json!({"data": data, "links": {"self": {"href": format!("{base}{path}")}}}),
                ))
            }
            (
                &Method::DELETE,
                [
                    "security_groups",
                    guid,
                    "relationships",
                    kind @ ("running_spaces" | "staging_spaces"),
                    space,
                ],
            ) => {
                let group = self.edit_security_group(guid, |_, group| {
                    spaces_of(group, kind).retain(|kept| kept != space);
                    Ok(())
                })?;
                group.ok_or_else(|| ApiError::not_found("Security group"))?;
                Ok(no_content())
            }
            _ => Err(ApiError::unknown(&req.method, &req.path)),
        };
        if let (Some(editor), true) = (editor, reply.is_ok()) {
            // a rule reaches the wicket's host: on the record, who
            println!("SECURITY GROUPS: {editor}: {} {}", req.method, req.path);
        }
        reply
    }

    /// `/v3/spaces/<guid>/{running,staging}_security_groups`.
    pub(crate) fn security_groups_of_space(&self, req: &Req, guid: &str, running: bool) -> Reply {
        let groups = self.store.read(|state| {
            state.space(guid).is_some().then(|| {
                let of = |group: &&SecurityGroup| applies(group, guid, running);
                state
                    .security_groups
                    .iter()
                    .filter(of)
                    .cloned()
                    .collect::<Vec<_>>()
            })
        });
        self.security_group_page(req, groups.ok_or_else(|| ApiError::not_found("Space"))?)
    }

    /// A page of `groups`, as the query filters them.
    fn security_group_page(&self, req: &Req, groups: Vec<SecurityGroup>) -> Reply {
        let bound = |key: &str, spaces: &[String]| {
            req.q(key).is_none_or(|wanted| {
                wanted
                    .split(',')
                    .any(|guid| spaces.iter().any(|space| space == guid))
            })
        };
        let items = groups
            .iter()
            .filter(|group| bound("running_space_guids", &group.running_spaces))
            .filter(|group| bound("staging_space_guids", &group.staging_spaces))
            .map(|group| views::security_group(&req.base, group))
            .collect();
        Ok(respond(
            StatusCode::OK,
            &views::page(
                &req.base,
                &req.path,
                &req.query,
                views::select(items, &req.query),
                None,
            ),
        ))
    }

    fn security_group(&self, guid: &str) -> Option<SecurityGroup> {
        self.store.read(|state| {
            state
                .security_groups
                .iter()
                .find(|group| group.guid == guid)
                .cloned()
        })
    }

    /// Changes the group `guid` as `change` says: it as it is now, None when there is none.
    fn edit_security_group(
        &self,
        guid: &str,
        change: impl Fn(&State, &mut SecurityGroup) -> Result<(), ApiError>,
    ) -> Result<Option<SecurityGroup>, ApiError> {
        self.store.update(|state| {
            let Some(at) = state.security_groups.iter().position(|g| g.guid == guid) else {
                return Ok(None);
            };
            let mut group = state.security_groups[at].clone();
            change(state, &mut group)?;
            group.updated_at = now();
            state.security_groups[at] = group.clone();
            Ok(Some(group))
        })
    }

    /// The name of who edits, if they may.
    fn security_group_editor(&self, req: &Req) -> Result<String, ApiError> {
        self.editor(req, &self.security_group_editors, "security-group-editor")
    }

    /// The name of who edits, if `editors` (of `--<option>`) let them.
    pub(crate) fn editor(
        &self,
        req: &Req,
        editors: &Editors,
        option: &str,
    ) -> Result<String, ApiError> {
        let token = self.claims(req)?;
        let identity = self.uaa.identity(&token);
        editors
            .check(&token, identity.as_ref(), option)
            .map_err(|why| ApiError::not_authorized(&why))?;
        Ok(token.name().to_string())
    }
}

/// The spaces of `group` for the relationship `kind`.
fn spaces_of<'a>(group: &'a mut SecurityGroup, kind: &str) -> &'a mut Vec<String> {
    match kind {
        "running_spaces" => &mut group.running_spaces,
        _ => &mut group.staging_spaces,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(protocol: &str, destination: &str, ports: Option<&str>) -> SecurityRule {
        SecurityRule {
            protocol: protocol.into(),
            destination: destination.into(),
            ports: ports.map(str::to_string),
            description: None,
            log: None,
        }
    }

    fn rules(destination: &str, ports: &str) -> Result<Vec<String>, String> {
        rules_of(&[rule("tcp", destination, Some(ports))])
    }

    #[test]
    fn a_rule_is_the_wickets_per_block_and_per_port() {
        for (destination, ports, expected) in [
            ("10.0.0.5", "5432", vec!["10.0.0.5:5432"]),
            ("10.0.0.0/24", "443", vec!["10.0.0.0/24:443"]),
            ("0.0.0.0/0", "443", vec!["0.0.0.0/0:443"]),
            // the bits past the prefix mean nothing
            ("10.0.0.5/24", "443", vec!["10.0.0.0/24:443"]),
            ("10.0.0.5/32", "443", vec!["10.0.0.5:443"]),
            // a range: its CIDRs, the fewest
            (
                "10.0.0.1-10.0.0.9",
                "443",
                vec![
                    "10.0.0.1:443",
                    "10.0.0.2/31:443",
                    "10.0.0.4/30:443",
                    "10.0.0.8/31:443",
                ],
            ),
            ("10.0.0.4-10.0.0.7", "443", vec!["10.0.0.4/30:443"]),
            ("10.0.0.4-10.0.0.4", "443", vec!["10.0.0.4:443"]),
            ("0.0.0.0-255.255.255.255", "443", vec!["0.0.0.0/0:443"]),
            (
                "::-ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                "443",
                vec!["[::/0]:443"],
            ),
            ("2001:db8::5", "443", vec!["[2001:db8::5]:443"]),
            ("2001:db8::5/32", "443", vec!["[2001:db8::/32]:443"]),
            (
                "2001:db8::1-2001:db8::2",
                "443",
                vec!["[2001:db8::1]:443", "[2001:db8::2]:443"],
            ),
            // the names of this API, lowercase as the wicket has them
            (
                "Blog-API.example.com",
                "443",
                vec!["blog-api.example.com:443"],
            ),
            ("localhost", "6379", vec!["localhost:6379"]),
            // the ports
            ("db", "80, 443", vec!["db:443", "db:80"]),
            ("db", "8000-8002", vec!["db:8000", "db:8001", "db:8002"]),
            ("db", "80,80,79-80", vec!["db:79", "db:80"]),
            // several destinations
            ("10.0.0.5, db", "80", vec!["10.0.0.5:80", "db:80"]),
        ] {
            assert_eq!(
                rules(destination, ports).unwrap(),
                expected,
                "{destination}"
            );
        }
    }

    #[test]
    fn what_the_wicket_cannot_relay_or_hold_is_refused() {
        for (protocol, destination, ports, expected) in [
            (
                "udp",
                "10.0.0.5",
                Some("53"),
                "protocol udp: the wicket relays tcp alone",
            ),
            ("icmp", "10.0.0.5", None, "protocol icmp"),
            ("all", "0.0.0.0/0", None, "protocol all"),
            ("tcp", "10.0.0.5", None, "ports are required"),
            ("tcp", "10.0.0.5", Some("0"), "expected 1 to 65535"),
            ("tcp", "10.0.0.5", Some("65536"), "expected 1 to 65535"),
            ("tcp", "10.0.0.5", Some("http"), "expected 1 to 65535"),
            (
                "tcp",
                "10.0.0.5",
                Some("90-80"),
                "a range starts at its lowest",
            ),
            ("tcp", "10.0.0.5/33", Some("80"), "not a CIDR"),
            (
                "tcp",
                "10.0.0.9-10.0.0.1",
                Some("80"),
                "a range starts at its lowest",
            ),
            ("tcp", "10.0.0.1-::1", Some("80"), "addresses of one family"),
            ("tcp", "10.1", Some("80"), "not an address"),
            ("tcp", "a b", Some("80"), "not an address"),
            ("tcp", "db:80", Some("80"), "not an address"),
            ("tcp", "", Some("80"), "not an address"),
            // past the guest's kernel command line, at once for all its 65535 ports
            (
                "tcp",
                "0.0.0.0/0",
                Some("1-65535"),
                "a guest takes 1024 bytes",
            ),
            (
                "tcp",
                "0.0.0.1-255.255.255.254",
                Some("8000-8100"),
                "a guest takes 1024 bytes",
            ),
        ] {
            let refused = rules_of(&[
                rule("tcp", "10.0.0.1", Some("1")),
                rule(protocol, destination, ports),
            ])
            .unwrap_err();
            assert!(refused.starts_with("Rules[1]: "), "{refused}");
            assert!(refused.contains(expected), "{refused}");
        }
        // 1024 bytes, the `,` between them counted: 64 rules of 15 take 1023
        let fits = rules("0.0.0.0/0", "10000-10063").unwrap();
        assert_eq!(fits.join(",").len(), 1023);
        assert!(rules("0.0.0.0/0", "10000-10064").is_err());
    }

    #[test]
    fn the_groups_of_the_space_apply_running_alone() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let group = |name: &str, destination: &str| SecurityGroup {
            guid: name.into(),
            name: name.into(),
            rules: vec![rule("tcp", destination, Some("80"))],
            running: false,
            staging: false,
            running_spaces: Vec::new(),
            staging_spaces: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
        };
        let space = state.spaces[0].guid.clone();
        let mut bound = group("bound", "db");
        bound.running_spaces.push(space.clone());
        let mut global = group("global", "10.0.0.5");
        global.running = true;
        let mut staging = group("staging", "staged");
        staging.staging = true;
        staging.staging_spaces.push(space.clone());
        let mut again = group("again", "db");
        again.running = true;
        state.security_groups = vec![bound, global, staging, group("unbound", "none"), again];
        assert_eq!(egress(&state, &space).unwrap(), ["10.0.0.5:80", "db:80"]);
    }
}
