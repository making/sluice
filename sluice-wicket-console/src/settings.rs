//! The console's options (`--console`, `--console-token`, `--console-dir`, the
//! `--oidc-*` sign-in), as both wickets take them: the command line (or the
//! environment) over the config file.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use sluice_wicket_oidc::{OidcSettings, Provider, Signer};
use tokio::net::TcpListener;

use crate::gate::{self, Gate, SignIn};

/// The console's directory without `--console-dir`: next to the config file,
/// else in the working directory.
const DEFAULT_DIR: &str = "deployed";

/// The console (`--console`).
pub struct ConsoleSettings {
    /// Where it listens, `host:port`.
    pub address: String,
    /// The credentials it asks for; None: none, unless browsers sign in
    /// through the provider, on a loopback address only.
    pub token: Option<String>,
    /// Where it keeps the routes it deployed.
    pub dir: PathBuf,
    /// The OpenID Provider its browsers sign in through.
    pub oidc: Option<OidcSettings>,
}

impl ConsoleSettings {
    /// How the console is reached, for the log.
    pub fn credentials(&self) -> String {
        match (&self.token, &self.oidc) {
            (Some(_), Some(oidc)) => format!("--console-token, or a sign-in at {}", oidc.issuer),
            (None, Some(oidc)) => format!("a sign-in at {}", oidc.issuer),
            (Some(_), None) => "--console-token required".to_string(),
            (None, None) => "loopback, no token".to_string(),
        }
    }

    /// Binds the console's address: a loopback one only without credentials.
    pub async fn bind(&self) -> Result<TcpListener, String> {
        gate::bind(&self.address, self.token.is_some() || self.oidc.is_some()).await
    }

    /// The gate of the console's credentials, `realm` the wicket's name; a
    /// session key drawn.
    pub fn gate(&self, realm: &'static str) -> Result<Gate, String> {
        Ok(Gate {
            token: self.token.clone(),
            sign_in: match &self.oidc {
                Some(oidc) => Some(SignIn {
                    provider: Provider::new(oidc.clone())?,
                    signer: Signer::random(),
                }),
                None => None,
            },
            realm,
        })
    }
}

/// The console's options as one source gives them: the command line (or the
/// environment), or the config file (its paths relative to it).
#[derive(Default)]
pub struct ConsoleOptions {
    pub console: Option<String>,
    pub console_token: Option<String>,
    pub console_dir: Option<PathBuf>,
    pub oidc_issuer: Option<String>,
    pub oidc_client_id: Option<String>,
    pub oidc_client_secret: Option<String>,
    /// `claim=pattern` rules, each of them comma-separated rules too.
    pub oidc_claim: Option<Vec<String>>,
    pub oidc_scopes: Option<String>,
    pub oidc_ca_cert: Option<PathBuf>,
}

/// A config file's rule, or a list of them (`oidc-claim`).
#[derive(Deserialize, PartialEq, Debug, Default, Clone)]
#[serde(from = "OneOrMany")]
pub struct Rules(pub Vec<String>);

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl From<OneOrMany> for Rules {
    fn from(given: OneOrMany) -> Rules {
        Rules(match given {
            OneOrMany::One(rule) => vec![rule],
            OneOrMany::Many(rules) => rules,
        })
    }
}

impl ConsoleOptions {
    /// The console of `flags` over `file` (the config file's options, `dir`
    /// its directory; `key` names a key in an error), None without
    /// `--console`. A sign-in without a console is refused.
    pub fn resolve(
        flags: ConsoleOptions,
        file: ConsoleOptions,
        dir: &Path,
        key: impl Fn(&str) -> String,
    ) -> Result<Option<ConsoleSettings>, String> {
        let given = |flag: Option<String>, file: Option<String>| {
            flag.or(file).filter(|value| !value.is_empty())
        };
        let in_file = |path: Option<PathBuf>| path.map(|path| dir.join(path));
        let oidc = match given(flags.oidc_issuer, file.oidc_issuer) {
            Some(issuer) => {
                if !issuer.starts_with("http://") && !issuer.starts_with("https://") {
                    return Err(format!(
                        "{}: not an http(s) url: {issuer}",
                        key("oidc-issuer")
                    ));
                }
                let client_id =
                    given(flags.oidc_client_id, file.oidc_client_id).ok_or_else(|| {
                        format!("{} is required with oidc-issuer", key("oidc-client-id"))
                    })?;
                let claims = match flags.oidc_claim {
                    Some(rules) => sluice_wicket_oidc::parse_rules(
                        rules.iter().map(String::as_str),
                        "--oidc-claim",
                    )?,
                    None => sluice_wicket_oidc::parse_rules(
                        file.oidc_claim.iter().flatten().map(String::as_str),
                        &key("oidc-claim"),
                    )?,
                };
                if claims.is_empty() {
                    return Err(format!(
                        "{} is required with oidc-issuer: who may sign in (email=* for anyone the provider knows)",
                        key("oidc-claim")
                    ));
                }
                let scopes: Vec<String> = flags
                    .oidc_scopes
                    .or(file.oidc_scopes)
                    .unwrap_or_else(|| "openid,email,profile".into())
                    .split([',', ' '])
                    .map(str::trim)
                    .filter(|scope| !scope.is_empty())
                    .map(str::to_string)
                    .collect();
                if !scopes.iter().any(|scope| scope == "openid") {
                    return Err(format!("{}: openid must be among them", key("oidc-scopes")));
                }
                let ca_pem =
                    match flags.oidc_ca_cert.or_else(|| in_file(file.oidc_ca_cert)) {
                        Some(path) => Some(std::fs::read(&path).map_err(|e| {
                            format!("{} {}: {e}", key("oidc-ca-cert"), path.display())
                        })?),
                        None => None,
                    };
                Some(OidcSettings {
                    issuer,
                    client_id,
                    client_secret: given(flags.oidc_client_secret, file.oidc_client_secret),
                    scopes,
                    claims,
                    ca_pem,
                })
            }
            None => None,
        };
        let Some(address) = given(flags.console, file.console) else {
            if oidc.is_some() {
                return Err(format!("{} needs --console", key("oidc-issuer")));
            }
            return Ok(None);
        };
        Ok(Some(ConsoleSettings {
            address,
            token: given(flags.console_token, file.console_token),
            dir: flags
                .console_dir
                .or_else(|| in_file(file.console_dir))
                .unwrap_or_else(|| dir.join(DEFAULT_DIR)),
            oidc,
        }))
    }
}
