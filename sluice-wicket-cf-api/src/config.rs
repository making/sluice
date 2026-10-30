//! Command line and environment: each option `--name <value>` falls back to
//! `SLUICE_WICKET_CF_API_<NAME>` (upper case, `-` as `_`), then to its default.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::flume::FlumeSettings;
use crate::oidc::{ClaimRule, OidcSettings};
use crate::{Settings, TlsSettings};

pub const USAGE: &str = "\
usage: sluice-wicket-cf-api --wicket <url> [options]

  --listen <host:port>        bind address (default 127.0.0.1:9091); without --oidc-issuer a
                              loopback address only: the API takes any credentials then
  --wicket <url>              the wicket's console, http://host:port
  --wicket-token <token>      its --console-token
  --wicket-token-file <path>  read the token from a file instead
  --domain <name>             a shared domain; repeatable (or comma-separated): the first
                              takes the default and random routes (default localhost)
  --data-dir <path>           the state, the packages and the droplets (default cf-api)
  --org <name>                the one organization (default sluice)
  --space <name>              the one space (default wicket)
  --flume <url>               the sluice-flume the wicket feeds: the apps' output and
                              requests reach cf logs from it; grpc://host:port or
                              grpcs://host:port
  --flume-token <token>       its bearer token (required with --flume)
  --flume-token-file <path>   read the token from a file instead
  --flume-ca-cert <path>      PEM CA verifying a grpcs:// flume (default the webPKI roots)
  --flume-insecure            skip the verification of a grpcs:// flume
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
  --client-credentials <id>:<secret>
                              a client of cf auth --client-credentials (CI); repeatable
  --client-credentials-file <path>
                              read them from a file instead, one <id>:<secret> per line
  --tls-cert <path>           serve TLS: the certificate chain, PEM
  --tls-key <path>            and its private key, PEM
  --exit-on-stdin-close       exit when stdin reaches its end (a parent's child process)
  --version                   print the build (version, commit, build time) and exit

Each option also reads SLUICE_WICKET_CF_API_<NAME> from the environment, e.g.
SLUICE_WICKET_CF_API_WICKET_TOKEN.";

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
    "domain",
    "data-dir",
    "org",
    "space",
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
    "client-credentials",
    "client-credentials-file",
    "tls-cert",
    "tls-key",
];

/// Parses the arguments (without the program name) over the environment.
pub fn parse(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Command, String> {
    let mut given: HashMap<&str, Vec<String>> = HashMap::new();
    let mut exit_on_stdin_close = false;
    let mut flume_insecure = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--exit-on-stdin-close" => exit_on_stdin_close = true,
            "--flume-insecure" => flume_insecure = true,
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
    let wicket = value("wicket").ok_or("--wicket is required")?;
    if !wicket.starts_with("http://") && !wicket.starts_with("https://") {
        return Err(format!("--wicket: not an http(s) url: {wicket}"));
    }
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
            let mut claims: Vec<ClaimRule> = Vec::new();
            for rule in values("oidc-claim")
                .iter()
                .flat_map(|value| value.split(','))
                .map(str::trim)
                .filter(|rule| !rule.is_empty())
            {
                let (claim, pattern) = rule
                    .split_once('=')
                    .filter(|(claim, _)| !claim.is_empty())
                    .ok_or_else(|| {
                        format!("--oidc-claim: expected <claim>=<pattern>, got '{rule}'")
                    })?;
                match claims.iter_mut().find(|known| known.claim == claim) {
                    Some(known) => known.patterns.push(pattern.to_string()),
                    None => claims.push(ClaimRule {
                        claim: claim.to_string(),
                        patterns: vec![pattern.to_string()],
                    }),
                }
            }
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
    let tls = match (value("tls-cert").is_some(), value("tls-key").is_some()) {
        (true, true) => Some(TlsSettings {
            cert_pem: pem("tls-cert")?.expect("given"),
            key_pem: pem("tls-key")?.expect("given"),
        }),
        (false, false) => None,
        _ => return Err("--tls-cert and --tls-key go together".into()),
    };
    Ok(Command::Serve(Box::new(Config {
        listen: value("listen").unwrap_or_else(|| "127.0.0.1:9091".to_string()),
        exit_on_stdin_close: exit_on_stdin_close
            || env("SLUICE_WICKET_CF_API_EXIT_ON_STDIN_CLOSE").is_some_and(|value| value == "true"),
        settings: Settings {
            wicket,
            wicket_token,
            domains,
            data_dir: PathBuf::from(value("data-dir").unwrap_or_else(|| "cf-api".to_string())),
            org: value("org").unwrap_or_else(|| "sluice".to_string()),
            space: value("space").unwrap_or_else(|| "wicket".to_string()),
            flume,
            oidc,
            clients,
            tls,
        },
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            _ => None,
        };
        let config = serve(parse(
            args(
                "--listen 0.0.0.0:9191 --domain=c.example --flume grpcs://flume:8002 \
                 --oidc-issuer https://idp.example.com/dex --oidc-client-id cf \
                 --oidc-scopes openid,email,groups",
            ),
            env,
        ));
        assert_eq!(config.listen, "0.0.0.0:9191");
        assert_eq!(config.settings.wicket, "http://wicket:9090");
        assert_eq!(config.settings.wicket_token.as_deref(), Some("from-env"));
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
        assert!(config.settings.tls.is_none());
        let config = serve(parse(
            args("--wicket http://w:1 --domain x.io --domain y.io,Z.io."),
            |_| None,
        ));
        assert_eq!(config.settings.domains, ["x.io", "y.io", "z.io"]);
        let config = serve(parse(
            args("--wicket http://w:1 --exit-on-stdin-close"),
            |_| None,
        ));
        assert_eq!(config.settings.domains, ["localhost"]);
        assert!(config.exit_on_stdin_close);
        assert!(config.settings.wicket_token.is_none());
        assert!(config.settings.flume.is_none());
        assert!(config.settings.oidc.is_none());
        assert!(config.settings.clients.is_empty());
    }

    #[test]
    fn rejects_what_it_cannot_serve_with() {
        let none = |_: &str| None;
        let error = |line: &str| parse(args(line), none).unwrap_err();
        assert!(error("").contains("--wicket"));
        assert!(error("--wicket w:1").contains("http(s)"));
        assert!(error("--wicket http://w:1 --domain a_b").contains("--domain"));
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
        assert!(error("--wicket http://w:1 --tls-key k.pem").contains("go together"));
        assert!(matches!(
            parse(args("--wicket http://w:1 --help"), none),
            Ok(Command::Help)
        ));
    }
}
