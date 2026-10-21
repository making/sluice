//! Command line and environment: each option `--name <value>` falls back to
//! `SLUICE_FLUME_<NAME>` (upper case, `-` as `_`), then to its default.

use std::collections::HashMap;

use crate::Settings;

pub const USAGE: &str = "\
usage: sluice-flume --token <token> [options]

  --host <address>        bind address (default 0.0.0.0)
  --port <port>           listen port (default 8002)
  --token <token>         bearer token of every Feed and Watch call
  --token-file <path>     read the token from a file instead
  --tls-cert <path>       PEM certificate chain: serve TLS (with --tls-key)
  --tls-key <path>        PEM private key of --tls-cert
  --ring-size <lines>     newest lines kept for backfill and resume (default 10000)
  --backfill <lines>      newest lines a new watch begins with (default 200)
  --exit-on-stdin-close   exit when stdin reaches its end (a parent's child process)
  --version               print the version and exit

Each option also reads SLUICE_FLUME_<NAME> from the environment, e.g. SLUICE_FLUME_TOKEN.";

/// What to run: print something and exit, or serve.
#[derive(Debug)]
pub enum Command {
    Help,
    Version,
    Serve(Config),
}

#[derive(Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub exit_on_stdin_close: bool,
    pub settings: Settings,
}

const VALUED: &[&str] = &[
    "host",
    "port",
    "token",
    "token-file",
    "tls-cert",
    "tls-key",
    "ring-size",
    "backfill",
];

/// Parses the arguments (without the program name) over the environment.
pub fn parse(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Command, String> {
    let mut given: HashMap<&str, String> = HashMap::new();
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
                given.insert(name, value);
            }
        }
    }
    let value = |name: &str| {
        given.get(name).cloned().or_else(|| {
            env(&format!(
                "SLUICE_FLUME_{}",
                name.to_uppercase().replace('-', "_")
            ))
            .filter(|value| !value.is_empty())
        })
    };
    let number = |name: &str, default: usize| -> Result<usize, String> {
        value(name).map_or(Ok(default), |raw| {
            raw.parse()
                .map_err(|_| format!("--{name}: not a number: {raw}"))
        })
    };
    let token = match (value("token"), value("token-file")) {
        (Some(token), _) => token,
        (None, Some(path)) => std::fs::read_to_string(&path)
            .map_err(|e| format!("--token-file {path}: {e}"))?
            .trim()
            .to_string(),
        (None, None) => return Err("--token (or --token-file) is required".to_string()),
    };
    if token.is_empty() || token.parse::<tonic::metadata::MetadataValue<_>>().is_err() {
        return Err("the token must be non-empty printable ASCII".to_string());
    }
    let tls = match (value("tls-cert"), value("tls-key")) {
        (Some(cert), Some(key)) => Some((
            std::fs::read(&cert).map_err(|e| format!("--tls-cert {cert}: {e}"))?,
            std::fs::read(&key).map_err(|e| format!("--tls-key {key}: {e}"))?,
        )),
        (None, None) => None,
        _ => return Err("--tls-cert and --tls-key go together".to_string()),
    };
    let port = value("port").map_or(Ok(8002), |raw| {
        raw.parse::<u16>()
            .map_err(|_| format!("--port: not a port: {raw}"))
    })?;
    Ok(Command::Serve(Config {
        host: value("host").unwrap_or_else(|| "0.0.0.0".to_string()),
        port,
        exit_on_stdin_close: exit_on_stdin_close
            || env("SLUICE_FLUME_EXIT_ON_STDIN_CLOSE").is_some_and(|value| value == "true"),
        settings: Settings {
            token,
            ring_size: number("ring-size", 10_000)?.max(1),
            backfill: number("backfill", 200)?,
            tls,
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
            "SLUICE_FLUME_TOKEN" => Some("from-env".to_string()),
            "SLUICE_FLUME_PORT" => Some("9100".to_string()),
            "SLUICE_FLUME_RING_SIZE" => Some("50".to_string()),
            _ => None,
        };
        let config = serve(parse(args("--port 9200 --backfill=7"), env));
        assert_eq!(config.port, 9200);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.settings.token, "from-env");
        assert_eq!(config.settings.ring_size, 50);
        assert_eq!(config.settings.backfill, 7);
        assert!(config.settings.tls.is_none());
        assert!(!config.exit_on_stdin_close);
        assert!(
            serve(parse(args("--token t --exit-on-stdin-close"), |_| None)).exit_on_stdin_close
        );
    }

    #[test]
    fn rejects_what_it_cannot_serve_with() {
        let none = |_: &str| None;
        assert!(parse(args(""), none).unwrap_err().contains("--token"));
        assert!(
            parse(args("--token t --tls-cert c.pem"), none)
                .unwrap_err()
                .contains("together")
        );
        assert!(
            parse(args("--token t --port 70000"), none)
                .unwrap_err()
                .contains("--port")
        );
        assert!(
            parse(args("--token t --colour"), none)
                .unwrap_err()
                .contains("--colour")
        );
        assert!(
            parse(args("--token"), none)
                .unwrap_err()
                .contains("needs a value")
        );
        assert!(matches!(
            parse(args("--token t --help"), none),
            Ok(Command::Help)
        ));
    }
}
