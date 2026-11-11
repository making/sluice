//! Command line and environment: each option `--name <value>` falls back to
//! `SLUICE_WICKET_CF_API_<NAME>` (upper case, `-` as `_`), then to its default.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::flume::FlumeSettings;
use crate::oidc::{OidcSettings, parse_rules};
use crate::security_groups::Editors;
use crate::{Settings, TlsSettings, WicketSettings};

pub const USAGE: &str = "\
usage: sluice-wicket-cf-api --wicket <url> [options]
       sluice-wicket-cf-api --wicket <name>=<url> --wicket <name>=<url> ... [options]

  --listen <host:port>        bind address (default 127.0.0.1:9091); with --any-credentials a
                              loopback address only
  --wicket <name>=<url>       a wicket's console, http://host:port, and its name: an app's
                              instances are spread over the wickets, a state of one wicket
                              kept on the first; repeatable (or comma-separated). --wicket
                              <url> names the one wicket 'wicket'
  --wicket-token <token>      their --console-token, one for all
  --wicket-token-file <path>  read the token from a file instead
  --wicket-away-grace <secs>  of several wickets, one not answering that long is gone: placed
                              on no more, the apps' indices on it start on the others, until it
                              has answered as long again (default 30, 0 = never)
  --domain <name>             a shared domain; repeatable (or comma-separated): the first
                              takes the default and random routes (default localhost)
  --data-dir <path>           the state, the packages and the droplets (default cf-api)
  --org <name>                the organization made at the first start (default sluice)
  --space <name>              and its space (default wicket)
  --cnb-builder <image>       the Cloud Native Buildpacks builder staging a zip of source on a
                              Firecracker wicket (the buildpack cnb, detecting last); none:
                              no source is staged
  --cnb-builder-username <name>
                              its registry's credentials, its run image's too where that is
                              of the same registry
  --cnb-builder-password <password>
  --cnb-builder-password-file <path>
                              read the password from a file instead
  --cnb-run-image <image>     the run image a droplet runs on, in place of the builder's: of
                              the builder's OS, architecture and distribution (Paketo's tiny)
  --cnb-egress <host:port>    a destination a build may connect to, as a route's egress of
                              the wicket; repeatable (or comma-separated). None: a build
                              has no network
  --cnb-memory-mib <MiB>      a build VM's memory: the app's if larger (default 2048)
  --cnb-disk-mib <MiB>        its scratch disk: the app's if larger (default 8192)
  --cnb-vcpus <n>             its vCPUs (default 2)
  --cnb-timeout <secs>        how long a build may take (default 900)
  --flume <url>               the sluice-flume the wicket feeds: the apps' output and
                              requests reach cf logs from it; grpc://host:port or
                              grpcs://host:port
  --flume-token <token>       its bearer token (required with --flume)
  --flume-token-file <path>   read the token from a file instead
  --flume-ca-cert <path>      PEM CA verifying a grpcs:// flume (default the webPKI roots)
  --flume-insecure            skip the verification of a grpcs:// flume
  --any-credentials           no provider: whoever reaches the API logs in and deploys, any
                              relay to its loopback included (a wicket's route, a tunnel, a
                              proxy). This or --oidc-issuer is required
  --oidc-issuer <url>         the OpenID Provider users log in through (cf login --sso);
                              its discovery is at <url>/.well-known/openid-configuration
  --oidc-client-id <id>       the client registered with it, whose redirect URI is
                              <api url>/uaa/callback (required with --oidc-issuer)
  --oidc-client-secret <s>    its secret; none = a public client (PKCE only)
  --oidc-client-secret-file <path>
                              read the secret from a file instead
  --oidc-claim <claim>=<pattern>
                              who may log in: the ID token's claim must hold (a list:
                              contain) the pattern, * matching any run of characters;
                              repeatable (or comma-separated): one claim's patterns are
                              alternatives, distinct claims must all hold (at least one
                              rule is required with --oidc-issuer, e.g. email=*)
  --oidc-scopes <list>        the scopes asked for, comma-separated (default
                              openid,email,profile; Dex needs groups for the groups claim)
  --oidc-ca-cert <path>       PEM CA verifying the provider (default the webPKI roots)
  --oidc-groups-claim <claim> the ID token's claim listing a user's groups, which
                              --group-role maps (default groups)
  --admin <claim>=<pattern>   who is an admin: as --oidc-claim; without --oidc-issuer the
                              claims are user_name and email, both the name anyone logs in
                              with, and without --admin or --admin-client everyone is
  --admin-client <id>         a client of --client-credentials who is an admin; repeatable
  --group-role <group>=<role> the role a group of the provider gives: admin,
                              organization_<role>@<org> or space_<role>@<org>/<space>;
                              repeatable (or comma-separated). With --oidc-issuer, which
                              needs an admin: --admin, --admin-client or a group's
  --client-credentials <id>:<secret>
                              a client of cf auth --client-credentials (CI); repeatable
  --client-credentials-file <path>
                              read them from a file instead, one <id>:<secret> per line
  --security-group-editor <claim>=<pattern>
                              who may edit the security groups, nobody without it: as
                              --oidc-claim; without --oidc-issuer the claims are user_name
                              and email, both the name anyone logs in with
  --security-group-editor-client <id>
                              a client of --client-credentials who may too; repeatable
  --service-broker-editor <claim>=<pattern>
                              who may register, update and delete service brokers, and set
                              who sees their plans (cf enable-service-access), nobody without
                              it: a broker's URL is any the cf-api reaches; as
                              --security-group-editor
  --service-broker-editor-client <id>
                              a client of --client-credentials who may too; repeatable
  --tls-cert <path>           serve TLS: the certificate chain, PEM
  --tls-key <path>            and its private key, PEM
  --stop-grace <secs>         how long a stop (SIGTERM, SIGINT, stdin's end) waits for the
                              requests and the wickets' puts under way, the listener closed
                              (default 8: within a Firecracker wicket's 10s to a VM's
                              process)
  --exit-on-stdin-close       exit when stdin reaches its end (a parent's child process)
  --version                   print the build (version, commit, build time) and exit

Each option also reads SLUICE_WICKET_CF_API_<NAME> from the environment, e.g.
SLUICE_WICKET_CF_API_WICKET_TOKEN; another variable of that prefix is refused.";

/// What to run: print something and exit, or serve.
#[derive(Debug)]
pub enum Command {
    Help,
    Version,
    Serve(Box<Config>),
}

#[derive(Debug)]
pub struct Config {
    pub listen: String,
    pub exit_on_stdin_close: bool,
    pub settings: Settings,
}

const VALUED: &[&str] = &[
    "listen",
    "wicket",
    "wicket-token",
    "wicket-token-file",
    "wicket-away-grace",
    "domain",
    "data-dir",
    "org",
    "space",
    "cnb-builder",
    "cnb-builder-username",
    "cnb-builder-password",
    "cnb-builder-password-file",
    "cnb-run-image",
    "cnb-egress",
    "cnb-memory-mib",
    "cnb-disk-mib",
    "cnb-vcpus",
    "cnb-timeout",
    "flume",
    "flume-token",
    "flume-token-file",
    "flume-ca-cert",
    "oidc-issuer",
    "oidc-client-id",
    "oidc-client-secret",
    "oidc-client-secret-file",
    "oidc-claim",
    "oidc-scopes",
    "oidc-ca-cert",
    "oidc-groups-claim",
    "admin",
    "admin-client",
    "group-role",
    "client-credentials",
    "client-credentials-file",
    "security-group-editor",
    "security-group-editor-client",
    "service-broker-editor",
    "service-broker-editor-client",
    "tls-cert",
    "tls-key",
    "stop-grace",
];

/// The switches read from the environment too.
const SWITCHES: &[&str] = &["any-credentials", "exit-on-stdin-close", "flume-insecure"];

/// Refuses the variables named as an option's (`SLUICE_WICKET_CF_API_<NAME>`) that name
/// none, a misspelled one among them: it would go unread.
pub fn check_env(names: impl IntoIterator<Item = String>) -> Result<(), String> {
    let option = |suffix: &str| {
        VALUED
            .iter()
            .chain(SWITCHES)
            .any(|known| known.to_uppercase().replace('-', "_") == suffix)
    };
    let mut unknown: Vec<String> = names
        .into_iter()
        .filter(|name| {
            name.strip_prefix("SLUICE_WICKET_CF_API_")
                .is_some_and(|suffix| !option(suffix))
        })
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    Err(format!(
        "unknown environment variable: {} (no option reads it)",
        unknown.join(", ")
    ))
}

/// Parses the arguments (without the program name) over the environment.
pub fn parse(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Command, String> {
    let mut given: HashMap<&str, Vec<String>> = HashMap::new();
    let mut exit_on_stdin_close = false;
    let mut flume_insecure = false;
    let mut any_credentials = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--exit-on-stdin-close" => exit_on_stdin_close = true,
            "--flume-insecure" => flume_insecure = true,
            "--any-credentials" => any_credentials = true,
            _ => {
                let (name, inline) = match arg.strip_prefix("--").map(|rest| rest.split_once('=')) {
                    Some(Some((name, value))) => (name, Some(value.to_string())),
                    Some(None) => (&arg[2..], None),
                    None => return Err(format!("unexpected argument: {arg}")),
                };
                let Some(name) = VALUED.iter().find(|valued| **valued == name) else {
                    return Err(format!("unknown option: --{name}"));
                };
                let value = match inline {
                    Some(value) => value,
                    None => args
                        .next()
                        .ok_or_else(|| format!("--{name} needs a value"))?,
                };
                given.entry(name).or_default().push(value);
            }
        }
    }
    let values = |name: &str| -> Vec<String> {
        given.get(name).cloned().unwrap_or_else(|| {
            env(&format!(
                "SLUICE_WICKET_CF_API_{}",
                name.to_uppercase().replace('-', "_")
            ))
            .filter(|value| !value.is_empty())
            .into_iter()
            .collect()
        })
    };
    let value = |name: &str| values(name).pop();
    let wickets = wickets(&values("wicket"))?;
    // a secret, else the file holding it
    let secret = |name: &str| -> Result<Option<String>, String> {
        match (value(name), value(&format!("{name}-file"))) {
            (Some(secret), _) => Ok(Some(secret)),
            (None, Some(path)) => Ok(Some(
                std::fs::read_to_string(&path)
                    .map_err(|e| format!("--{name}-file {path}: {e}"))?
                    .trim()
                    .to_string(),
            )),
            (None, None) => Ok(None),
        }
    };
    let pem = |name: &str| -> Result<Option<Vec<u8>>, String> {
        match value(name) {
            Some(path) => Ok(Some(
                std::fs::read(&path).map_err(|e| format!("--{name} {path}: {e}"))?,
            )),
            None => Ok(None),
        }
    };
    let wicket_token = secret("wicket-token")?;
    let wicket_away_grace = match value("wicket-away-grace") {
        Some(seconds) => seconds.trim().parse::<u64>().map_err(|_| {
            format!("--wicket-away-grace: a number of seconds (0 = never), not '{seconds}'")
        })?,
        None => DEFAULT_WICKET_AWAY_GRACE,
    };
    let stop_grace = match value("stop-grace") {
        Some(seconds) => seconds
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("--stop-grace: a number of seconds, not '{seconds}'"))?,
        None => DEFAULT_STOP_GRACE,
    };
    let cnb_egress: Vec<String> = values("cnb-egress")
        .iter()
        .flat_map(|value| value.split(','))
        .map(|rule| rule.trim().to_string())
        .filter(|rule| !rule.is_empty())
        .collect();
    let cnb_credentials = match (
        value("cnb-builder-username"),
        secret("cnb-builder-password")?,
    ) {
        (Some(username), Some(password)) => Some((username, password)),
        (None, None) => None,
        _ => {
            return Err(
                "--cnb-builder-username and --cnb-builder-password (or --cnb-builder-password-file) go together".into(),
            );
        }
    };
    let number = |name: &str, default: u64, range: std::ops::RangeInclusive<u64>| {
        let Some(given) = value(name) else {
            return Ok(None);
        };
        match given.trim().parse::<u64>() {
            Ok(number) if range.contains(&number) => Ok(Some(number)),
            _ => Err(format!(
                "--{name}: a number from {} to {} (default {default}), not '{given}'",
                range.start(),
                range.end()
            )),
        }
    };
    use crate::staging::Cnb;
    let sized = [
        number("cnb-memory-mib", Cnb::MEMORY_MIB, 128..=1 << 20)?,
        number("cnb-disk-mib", Cnb::DISK_MIB, 64..=1 << 20)?,
        number("cnb-vcpus", u64::from(Cnb::VCPUS), 1..=32)?,
        number("cnb-timeout", Cnb::TIMEOUT.as_secs(), 1..=60 * 60)?,
    ];
    let run_image = value("cnb-run-image");
    let cnb = match value("cnb-builder") {
        Some(builder) => {
            for (name, image) in [
                ("builder", Some(&builder)),
                ("run-image", run_image.as_ref()),
            ] {
                if let Some(image) = image {
                    sluice_wicket_registry::Reference::parse(image)
                        .map_err(|e| format!("--cnb-{name}: {e}"))?;
                }
            }
            let [memory_mib, disk_mib, vcpus, timeout] = sized;
            Some(Cnb {
                builder,
                run_image,
                credentials: cnb_credentials,
                egress: cnb_egress,
                memory_mib: memory_mib.unwrap_or(Cnb::MEMORY_MIB),
                disk_mib: disk_mib.unwrap_or(Cnb::DISK_MIB),
                vcpus: vcpus.map_or(Cnb::VCPUS, |vcpus| vcpus as u32),
                timeout: timeout.map_or(Cnb::TIMEOUT, std::time::Duration::from_secs),
            })
        }
        None if cnb_credentials.is_some()
            || run_image.is_some()
            || !cnb_egress.is_empty()
            || sized.iter().any(Option::is_some) =>
        {
            return Err(
                "--cnb-builder-username, --cnb-run-image, --cnb-egress and the build's size need \
                 --cnb-builder: \
                 they are a build's"
                    .into(),
            );
        }
        None => None,
    };
    let flume = match value("flume") {
        Some(url) => {
            if !url.starts_with("grpc://") && !url.starts_with("grpcs://") {
                return Err(format!(
                    "--flume: expected grpc://host:port or grpcs://host:port, got '{url}'"
                ));
            }
            let token = secret("flume-token")?
                .filter(|token| !token.is_empty())
                .ok_or("--flume-token (or --flume-token-file) is required with --flume")?;
            Some(FlumeSettings {
                url,
                token,
                ca_pem: pem("flume-ca-cert")?,
                insecure: flume_insecure
                    || env("SLUICE_WICKET_CF_API_FLUME_INSECURE")
                        .is_some_and(|value| value == "true"),
            })
        }
        None => None,
    };
    let mut domains: Vec<String> = values("domain")
        .iter()
        .flat_map(|value| value.split(','))
        .map(|domain| domain.trim().trim_matches('.').to_ascii_lowercase())
        .filter(|domain| !domain.is_empty())
        .collect();
    if domains.is_empty() {
        domains.push("localhost".into());
    }
    for domain in &domains {
        if domain.split('.').any(|label| {
            label.is_empty()
                || !label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        }) {
            return Err(format!("--domain: not a domain name: {domain}"));
        }
    }
    domains.dedup();
    let oidc = match value("oidc-issuer") {
        Some(issuer) => {
            if !issuer.starts_with("http://") && !issuer.starts_with("https://") {
                return Err(format!("--oidc-issuer: not an http(s) url: {issuer}"));
            }
            let client_id =
                value("oidc-client-id").ok_or("--oidc-client-id is required with --oidc-issuer")?;
            let claims = parse_rules(
                values("oidc-claim").iter().map(String::as_str),
                "--oidc-claim",
            )?;
            if claims.is_empty() {
                return Err(
                    "--oidc-claim is required with --oidc-issuer: who may log in (email=* for anyone the provider knows)".into(),
                );
            }
            let scopes: Vec<String> = value("oidc-scopes")
                .unwrap_or_else(|| "openid,email,profile".into())
                .split([',', ' '])
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .map(str::to_string)
                .collect();
            if !scopes.iter().any(|scope| scope == "openid") {
                return Err("--oidc-scopes: openid must be among them".into());
            }
            Some(OidcSettings {
                issuer,
                client_id,
                client_secret: secret("oidc-client-secret")?.filter(|secret| !secret.is_empty()),
                scopes,
                claims,
                ca_pem: pem("oidc-ca-cert")?,
            })
        }
        None => None,
    };
    let mut clients: Vec<(String, String)> = Vec::new();
    let mut credentials = values("client-credentials");
    if let Some(path) = value("client-credentials-file") {
        credentials.extend(
            std::fs::read_to_string(&path)
                .map_err(|e| format!("--client-credentials-file {path}: {e}"))?
                .lines()
                .map(str::to_string),
        );
    }
    for pair in credentials
        .iter()
        .map(|pair| pair.trim())
        .filter(|pair| !pair.is_empty())
    {
        let (id, secret) = pair
            .split_once(':')
            .filter(|(id, secret)| !id.is_empty() && !secret.is_empty())
            .ok_or_else(|| format!("--client-credentials: expected <id>:<secret>, got '{pair}'"))?;
        clients.push((id.to_string(), secret.to_string()));
    }
    if !clients.is_empty() && oidc.is_none() {
        return Err(
            "--client-credentials needs --oidc-issuer: without it any client passes".into(),
        );
    }
    let editors = |option: &str| {
        let editors = Editors {
            rules: parse_rules(
                values(option).iter().map(String::as_str),
                &format!("--{option}"),
            )?,
            clients: values(&format!("{option}-client")),
        };
        match editors
            .clients
            .iter()
            .find(|client| !clients.iter().any(|(id, _)| id == *client))
        {
            Some(unknown) => Err(format!(
                "--{option}-client {unknown}: not a client of --client-credentials"
            )),
            None => Ok(editors),
        }
    };
    let security_group_editors = editors("security-group-editor")?;
    let service_broker_editors = editors("service-broker-editor")?;
    let tls = match (value("tls-cert").is_some(), value("tls-key").is_some()) {
        (true, true) => Some(TlsSettings {
            cert_pem: pem("tls-cert")?.expect("given"),
            key_pem: pem("tls-key")?.expect("given"),
        }),
        (false, false) => None,
        _ => return Err("--tls-cert and --tls-key go together".into()),
    };
    let any_credentials = any_credentials
        || env("SLUICE_WICKET_CF_API_ANY_CREDENTIALS").is_some_and(|value| value == "true");
    match (&oidc, any_credentials) {
        (None, false) => {
            return Err(
                "who logs in is not said: --oidc-issuer for an OpenID Provider, or --any-credentials for whoever reaches the API, any relay to its loopback included (a wicket's route, a tunnel, a proxy)".into(),
            );
        }
        (Some(_), true) => {
            return Err(
                "--any-credentials goes without --oidc-issuer: with it the provider says who logs in".into(),
            );
        }
        _ => {}
    }
    let admins = editors("admin")?;
    let group_roles =
        crate::roles::parse_group_roles(values("group-role").iter().map(String::as_str))?;
    if !group_roles.is_empty() && oidc.is_none() {
        return Err("--group-role needs --oidc-issuer: the groups are the provider's".into());
    }
    let policy = crate::roles::Policy {
        everyone_admin: oidc.is_none() && admins.rules.is_empty() && admins.clients.is_empty(),
        admins,
        groups_claim: value("oidc-groups-claim").unwrap_or_else(|| "groups".into()),
        group_roles,
    };
    if !policy.has_admins() {
        return Err(
            "who is an admin is not said: --admin <claim>=<pattern>, --admin-client <id> or --group-role <group>=admin; an admin makes the organizations and gives the roles".into(),
        );
    }
    Ok(Command::Serve(Box::new(Config {
        listen: value("listen").unwrap_or_else(|| "127.0.0.1:9091".to_string()),
        exit_on_stdin_close: exit_on_stdin_close
            || env("SLUICE_WICKET_CF_API_EXIT_ON_STDIN_CLOSE").is_some_and(|value| value == "true"),
        settings: Settings {
            wickets,
            wicket_token,
            wicket_away_grace: std::time::Duration::from_secs(wicket_away_grace),
            domains,
            data_dir: PathBuf::from(value("data-dir").unwrap_or_else(|| "cf-api".to_string())),
            org: value("org").unwrap_or_else(|| "sluice".to_string()),
            space: value("space").unwrap_or_else(|| "wicket".to_string()),
            cnb,
            flume,
            oidc,
            clients,
            policy,
            security_group_editors,
            service_broker_editors,
            tls,
            stop_grace: std::time::Duration::from_secs(stop_grace),
        },
    })))
}

/// The name of the one wicket `--wicket <url>` gives.
pub const ONE_WICKET: &str = "wicket";

/// Default of `--wicket-away-grace`, in seconds: a wicket restarting, its apps' VMs booting,
/// is back well within it.
const DEFAULT_WICKET_AWAY_GRACE: u64 = 30;

/// Default of `--stop-grace`, in seconds: below the 10s a Firecracker wicket gives a VM's
/// process after SIGTERM, the state written in what is left.
const DEFAULT_STOP_GRACE: u64 = 8;

/// The wickets `--wicket` names, each `<name>=<url>` (or comma-separated), or one `<url>`, named
/// [`ONE_WICKET`]: distinct names of distinct consoles.
fn wickets(given: &[String]) -> Result<Vec<WicketSettings>, String> {
    let entries: Vec<&str> = (given.iter())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect();
    if entries.is_empty() {
        return Err("--wicket is required".into());
    }
    let url = |text: &str| text.starts_with("http://") || text.starts_with("https://");
    let mut wickets: Vec<WicketSettings> = Vec::new();
    for entry in &entries {
        let (name, at) = match (url(entry), entries.len()) {
            (true, 1) => (ONE_WICKET, *entry),
            (true, _) => {
                return Err(format!(
                    "--wicket: each of several wickets is <name>=<url>, not {entry}"
                ));
            }
            (false, _) => entry.split_once('=').ok_or_else(|| {
                format!("--wicket: expected <name>=<url> or an http(s) url, got '{entry}'")
            })?,
        };
        let named = |b: u8| b.is_ascii_alphanumeric() || b"-_.".contains(&b);
        if name.is_empty() || !name.bytes().all(named) {
            return Err(format!(
                "--wicket: a name is of letters, digits, '-', '_' and '.', not '{name}'"
            ));
        }
        if !url(at) {
            return Err(format!("--wicket {name}: not an http(s) url: {at}"));
        }
        let at = at.trim_end_matches('/');
        if wickets.iter().any(|kept| kept.name == name) {
            return Err(format!("--wicket: {name} is named twice"));
        }
        if let Some(kept) = wickets.iter().find(|kept| kept.url == at) {
            return Err(format!(
                "--wicket: {} and {name} are one console, {at}",
                kept.name
            ));
        }
        wickets.push(WicketSettings {
            name: name.to_string(),
            url: at.to_string(),
        });
    }
    Ok(wickets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oidc::ClaimRule;

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(String::from).collect()
    }

    fn serve(command: Result<Command, String>) -> Config {
        match command {
            Ok(Command::Serve(config)) => *config,
            other => panic!("not a serve command: {other:?}"),
        }
    }

    #[test]
    fn options_override_the_environment_which_overrides_the_defaults() {
        let env = |name: &str| match name {
            "SLUICE_WICKET_CF_API_WICKET" => Some("http://wicket:9090".to_string()),
            "SLUICE_WICKET_CF_API_WICKET_TOKEN" => Some("from-env".to_string()),
            "SLUICE_WICKET_CF_API_DOMAIN" => Some("a.example, b.example".to_string()),
            "SLUICE_WICKET_CF_API_FLUME_TOKEN" => Some("flume-env".to_string()),
            "SLUICE_WICKET_CF_API_FLUME_INSECURE" => Some("true".to_string()),
            "SLUICE_WICKET_CF_API_OIDC_CLIENT_SECRET" => Some("hush".to_string()),
            "SLUICE_WICKET_CF_API_OIDC_CLAIM" => {
                Some("email=*@example.com, email=root@example.org,groups=devs".to_string())
            }
            "SLUICE_WICKET_CF_API_CLIENT_CREDENTIALS" => Some("robot:r0bot".to_string()),
            "SLUICE_WICKET_CF_API_SECURITY_GROUP_EDITOR" => Some("groups=ops".to_string()),
            "SLUICE_WICKET_CF_API_SECURITY_GROUP_EDITOR_CLIENT" => Some("robot".to_string()),
            "SLUICE_WICKET_CF_API_SERVICE_BROKER_EDITOR" => Some("email=*@example.com".to_string()),
            "SLUICE_WICKET_CF_API_GROUP_ROLE" => Some("ops=admin".to_string()),
            "SLUICE_WICKET_CF_API_ADMIN_CLIENT" => Some("robot".to_string()),
            _ => None,
        };
        let config = serve(parse(
            args(
                "--listen 0.0.0.0:9191 --domain=c.example --flume grpcs://flume:8002 \
                 --oidc-issuer https://idp.example.com/dex --oidc-client-id cf \
                 --oidc-scopes openid,email,groups --oidc-groups-claim teams \
                 --group-role devs=space_developer@sluice/wicket",
            ),
            env,
        ));
        assert_eq!(config.listen, "0.0.0.0:9191");
        let wicket = |name: &str, url: &str| WicketSettings {
            name: name.into(),
            url: url.into(),
        };
        assert_eq!(
            config.settings.wickets,
            [wicket("wicket", "http://wicket:9090")]
        );
        assert_eq!(config.settings.wicket_token.as_deref(), Some("from-env"));
        assert_eq!(
            config.settings.wicket_away_grace,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(config.settings.stop_grace.as_secs(), 8);
        assert_eq!(config.settings.domains, ["c.example"]);
        assert_eq!(config.settings.data_dir, PathBuf::from("cf-api"));
        assert_eq!(config.settings.org, "sluice");
        assert!(!config.exit_on_stdin_close);
        let flume = config.settings.flume.expect("a flume");
        assert_eq!(
            (flume.url.as_str(), flume.token.as_str(), flume.insecure),
            ("grpcs://flume:8002", "flume-env", true)
        );
        assert!(flume.ca_pem.is_none());
        let oidc = config.settings.oidc.expect("a provider");
        assert_eq!(
            (
                oidc.issuer.as_str(),
                oidc.client_id.as_str(),
                oidc.client_secret.as_deref()
            ),
            ("https://idp.example.com/dex", "cf", Some("hush"))
        );
        assert_eq!(oidc.scopes, ["openid", "email", "groups"]);
        assert_eq!(
            oidc.claims,
            [
                ClaimRule {
                    claim: "email".into(),
                    patterns: vec!["*@example.com".into(), "root@example.org".into()],
                },
                ClaimRule {
                    claim: "groups".into(),
                    patterns: vec!["devs".into()],
                },
            ]
        );
        assert_eq!(
            config.settings.clients,
            [("robot".to_string(), "r0bot".to_string())]
        );
        assert_eq!(
            config.settings.security_group_editors,
            Editors {
                rules: vec![ClaimRule {
                    claim: "groups".into(),
                    patterns: vec!["ops".into()],
                }],
                clients: vec!["robot".into()],
            }
        );
        assert_eq!(
            config.settings.service_broker_editors,
            Editors {
                rules: vec![ClaimRule {
                    claim: "email".into(),
                    patterns: vec!["*@example.com".into()],
                }],
                clients: Vec::new(),
            }
        );
        let policy = &config.settings.policy;
        assert!(!policy.everyone_admin && policy.admins.rules.is_empty());
        assert_eq!(policy.admins.clients, ["robot"]);
        assert_eq!(policy.groups_claim, "teams");
        assert_eq!(
            policy.group_roles,
            crate::roles::parse_group_roles(["devs=space_developer@sluice/wicket"]).unwrap()
        );
        assert!(config.settings.tls.is_none());
        let config = serve(parse(
            args(
                "--wicket http://w:1 --any-credentials --domain x.io --domain y.io,Z.io. \
                 --wicket-away-grace 0 --stop-grace 30 --cnb-builder r.io/builder:1 --cnb-builder-username u \
                 --cnb-builder-password p --cnb-run-image r.io/run-tiny:1 \
                 --cnb-egress a.io:443,b.io:443 --cnb-egress c.io:80 \
                 --cnb-memory-mib 4096 --cnb-timeout 1800",
            ),
            |_| None,
        ));
        assert_eq!(config.settings.domains, ["x.io", "y.io", "z.io"]);
        assert!(config.settings.wicket_away_grace.is_zero());
        assert_eq!(config.settings.stop_grace.as_secs(), 30);
        // the builder of source, its builds' egress; its credentials its registry's alone
        let cnb = config.settings.cnb.unwrap();
        assert_eq!(cnb.builder, "r.io/builder:1");
        assert_eq!(cnb.run_image.as_deref(), Some("r.io/run-tiny:1"));
        assert_eq!(cnb.egress, ["a.io:443", "b.io:443", "c.io:80"]);
        assert_eq!(cnb.credentials_of("r.io/run:1"), Some(("u", "p")));
        assert_eq!(cnb.credentials_of("other.io/run:1"), None);
        // its VMs' size and time: those given, else the defaults
        assert_eq!(
            (
                cnb.memory_mib,
                cnb.disk_mib,
                cnb.vcpus,
                cnb.timeout.as_secs()
            ),
            (4096, 8192, 2, 1800)
        );
        // several wickets, each named, in the order given: of the options, else the environment
        let config = serve(parse(
            args("--wicket a=http://a:9090/ --wicket b-1=https://b:9090,c.2=http://c:9090"),
            |name| match name {
                "SLUICE_WICKET_CF_API_WICKET" => Some("d=http://d:9090".to_string()),
                "SLUICE_WICKET_CF_API_ANY_CREDENTIALS" => Some("true".to_string()),
                _ => None,
            },
        ));
        assert_eq!(
            config.settings.wickets,
            [
                wicket("a", "http://a:9090"),
                wicket("b-1", "https://b:9090"),
                wicket("c.2", "http://c:9090")
            ]
        );
        let config = serve(parse(args("--any-credentials"), |name| {
            (name == "SLUICE_WICKET_CF_API_WICKET").then(|| "a=http://a:1, b=http://b:1".into())
        }));
        assert_eq!(
            config.settings.wickets,
            [wicket("a", "http://a:1"), wicket("b", "http://b:1")]
        );
        let config = serve(parse(
            args("--wicket http://w:1 --exit-on-stdin-close"),
            |name| (name == "SLUICE_WICKET_CF_API_ANY_CREDENTIALS").then(|| "true".to_string()),
        ));
        assert_eq!(config.settings.domains, ["localhost"]);
        assert!(config.exit_on_stdin_close);
        assert!(config.settings.wicket_token.is_none());
        assert!(config.settings.flume.is_none());
        assert!(config.settings.oidc.is_none());
        assert!(config.settings.clients.is_empty());
        assert_eq!(config.settings.security_group_editors, Editors::default());
        assert_eq!(config.settings.policy, crate::roles::Policy::default());
        // a variable of the options' prefix naming none is refused, not left unread
        assert_eq!(
            check_env(
                [
                    "PATH",
                    "SLUICE_WICKET_CF_API_FLUME",
                    "SLUICE_WICKET_CF_API_FLUME_TOKEN_FILE",
                    "SLUICE_WICKET_CF_API_ANY_CREDENTIALS"
                ]
                .map(String::from)
            ),
            Ok(())
        );
        assert_eq!(
            check_env(
                ["SLUICE_WICKET_CF_API_FLUNE", "SLUICE_WICKET_CF_API_FLUME"].map(String::from)
            ),
            Err(
                "unknown environment variable: SLUICE_WICKET_CF_API_FLUNE (no option reads it)"
                    .into()
            )
        );
    }

    #[test]
    fn rejects_what_it_cannot_serve_with() {
        let none = |_: &str| None;
        let error = |line: &str| parse(args(line), none).unwrap_err();
        assert!(error("").contains("--wicket"));
        assert!(error("--wicket w:1").contains("http(s)"));
        assert!(error("--wicket a=w:1").contains("--wicket a: not an http(s) url"));
        // several, each named, distinct, of distinct consoles
        for (line, refusal) in [
            (
                "--wicket http://a:1 --wicket b=http://b:1",
                "each of several",
            ),
            ("--wicket a=http://a:1,a=http://b:1", "a is named twice"),
            (
                "--wicket a=http://a:1 --wicket b=http://a:1/",
                "a and b are one console",
            ),
            ("--wicket a/b=http://a:1", "a name is of letters"),
            ("--wicket =http://a:1", "a name is of letters"),
        ] {
            assert!(error(line).contains(refusal), "{line}: {}", error(line));
        }
        assert!(error("--wicket http://w:1 --domain a_b").contains("--domain"));
        for line in [
            "--cnb-egress a.io:443",
            "--cnb-builder-username u",
            "--cnb-vcpus 4",
            "--cnb-run-image r.io/run:1",
        ] {
            let line = format!("--wicket http://w:1 --any-credentials {line}");
            assert!(error(&line).contains("--cnb-builder"), "{line}");
        }
        assert!(
            error("--wicket http://w:1 --wicket-away-grace 30s")
                .contains("--wicket-away-grace: a number of seconds")
        );
        assert!(
            error("--wicket http://w:1 --stop-grace 8s")
                .contains("--stop-grace: a number of seconds, not '8s'")
        );
        assert!(error("--wicket http://w:1 --colour").contains("--colour"));
        assert!(error("--wicket").contains("needs a value"));
        assert!(
            error("--wicket http://w:1 --flume grpc://f:1")
                .contains("--flume-token (or --flume-token-file) is required")
        );
        assert!(
            error("--wicket http://w:1 --flume f:1 --flume-token t")
                .contains("expected grpc://host:port or grpcs://host:port, got 'f:1'")
        );
        let oidc = "--wicket http://w:1 --oidc-issuer https://idp --oidc-client-id cf";
        assert!(error(oidc).contains("--oidc-claim is required"));
        // a provider's users are no admins unless said
        assert!(
            error(&format!("{oidc} --oidc-claim email=*")).contains("who is an admin is not said")
        );
        assert!(
            error("--wicket http://w:1 --any-credentials --group-role ops=admin")
                .contains("--group-role needs --oidc-issuer")
        );
        assert!(
            error("--wicket http://w:1 --oidc-issuer https://idp --oidc-claim email=*")
                .contains("--oidc-client-id is required")
        );
        assert!(error(&format!("{oidc} --oidc-claim email")).contains("<claim>=<pattern>"));
        assert!(
            error(&format!("{oidc} --oidc-claim email=* --oidc-scopes email")).contains("openid")
        );
        assert!(
            error("--wicket http://w:1 --client-credentials robot:s")
                .contains("--client-credentials needs --oidc-issuer")
        );
        assert!(
            error(&format!(
                "{oidc} --oidc-claim email=* --client-credentials robot"
            ))
            .contains("<id>:<secret>")
        );
        assert!(error("--wicket http://w:1 --security-group-editor email").contains("<claim>="));
        for client in [
            "--security-group-editor-client",
            "--service-broker-editor-client",
        ] {
            assert!(
                error(&format!("--wicket http://w:1 {client} robot")).contains(&format!(
                    "{client} robot: not a client of --client-credentials"
                ))
            );
        }
        assert!(error("--wicket http://w:1 --tls-key k.pem").contains("go together"));
        // who logs in is said, wherever it runs: nothing tells what relays to its loopback
        for named in ["--oidc-issuer", "--any-credentials"] {
            assert!(error("--wicket http://w:1").contains(named));
        }
        assert!(
            error(&format!("{oidc} --oidc-claim email=* --any-credentials"))
                .contains("--any-credentials goes without --oidc-issuer")
        );
        assert!(matches!(
            parse(args("--wicket http://w:1 --help"), none),
            Ok(Command::Help)
        ));
    }
}
