//! Command line and environment: each option `--name <value>` falls back to
//! `SLUICE_WICKET_CF_API_<NAME>` (upper case, `-` as `_`), then to its default.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::Settings;

pub const USAGE: &str = "\
usage: sluice-wicket-cf-api --wicket <url> [options]

  --listen <host:port>        bind address (default 127.0.0.1:9091); a loopback address only:
                              the API takes any credentials yet
  --wicket <url>              the wicket's console, http://host:port
  --wicket-token <token>      its --console-token
  --wicket-token-file <path>  read the token from a file instead
  --domain <name>             a shared domain; repeatable (or comma-separated): the first
                              takes the default and random routes (default localhost)
  --data-dir <path>           the state, the packages and the droplets (default cf-api)
  --org <name>                the one organization (default sluice)
  --space <name>              the one space (default wicket)
  --exit-on-stdin-close       exit when stdin reaches its end (a parent's child process)
  --version                   print the build (version, commit, build time) and exit

Each option also reads SLUICE_WICKET_CF_API_<NAME> from the environment, e.g.
SLUICE_WICKET_CF_API_WICKET_TOKEN.";

/// What to run: print something and exit, or serve.
#[derive(Debug)]
pub enum Command {
    Help,
    Version,
    Serve(Config),
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
];

/// Parses the arguments (without the program name) over the environment.
pub fn parse(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Command, String> {
    let mut given: HashMap<&str, Vec<String>> = HashMap::new();
    let mut exit_on_stdin_close = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--exit-on-stdin-close" => exit_on_stdin_close = true,
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
    let wicket_token = match (value("wicket-token"), value("wicket-token-file")) {
        (Some(token), _) => Some(token),
        (None, Some(path)) => Some(
            std::fs::read_to_string(&path)
                .map_err(|e| format!("--wicket-token-file {path}: {e}"))?
                .trim()
                .to_string(),
        ),
        (None, None) => None,
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
    Ok(Command::Serve(Config {
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
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(String::from).collect()
    }

    fn serve(command: Result<Command, String>) -> Config {
        match command {
            Ok(Command::Serve(config)) => config,
            other => panic!("not a serve command: {other:?}"),
        }
    }

    #[test]
    fn options_override_the_environment_which_overrides_the_defaults() {
        let env = |name: &str| match name {
            "SLUICE_WICKET_CF_API_WICKET" => Some("http://wicket:9090".to_string()),
            "SLUICE_WICKET_CF_API_WICKET_TOKEN" => Some("from-env".to_string()),
            "SLUICE_WICKET_CF_API_DOMAIN" => Some("a.example, b.example".to_string()),
            _ => None,
        };
        let config = serve(parse(
            args("--listen 127.0.0.1:9191 --domain=c.example"),
            env,
        ));
        assert_eq!(config.listen, "127.0.0.1:9191");
        assert_eq!(config.settings.wicket, "http://wicket:9090");
        assert_eq!(config.settings.wicket_token.as_deref(), Some("from-env"));
        assert_eq!(config.settings.domains, ["c.example"]);
        assert_eq!(config.settings.data_dir, PathBuf::from("cf-api"));
        assert_eq!(config.settings.org, "sluice");
        assert!(!config.exit_on_stdin_close);
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
    }

    #[test]
    fn rejects_what_it_cannot_serve_with() {
        let none = |_: &str| None;
        assert!(parse(args(""), none).unwrap_err().contains("--wicket"));
        assert!(
            parse(args("--wicket w:1"), none)
                .unwrap_err()
                .contains("http(s)")
        );
        assert!(
            parse(args("--wicket http://w:1 --domain a_b"), none)
                .unwrap_err()
                .contains("--domain")
        );
        assert!(
            parse(args("--wicket http://w:1 --colour"), none)
                .unwrap_err()
                .contains("--colour")
        );
        assert!(
            parse(args("--wicket"), none)
                .unwrap_err()
                .contains("needs a value")
        );
        assert!(matches!(
            parse(args("--wicket http://w:1 --help"), none),
            Ok(Command::Help)
        ));
    }
}
