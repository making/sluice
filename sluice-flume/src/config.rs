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
  --ring-bytes <bytes>    most bytes those lines may hold (default 67108864)
  --backfill <lines>      newest lines a new watch begins with (default 200)
  --forget-after <secs>   how long a node disconnected stays listed (default
                          3600, 0 = until the flume ends)
  --otlp-endpoint <url>   export every line here, OTLP/HTTP (opt-in; a path of
                          / or none gets /v1/logs appended)
  --otlp-service-name <name>    service.name of the exported resources (default sluice)
  --otlp-resource-attributes <k=v,...>  resource attributes to add (a key
                          also set here wins), as OTEL_RESOURCE_ATTRIBUTES
  --otlp-compression <gzip|none>  body encoding of a request (default gzip)
  --otlp-headers <k=v,...>      request headers, values percent-decoded, as
                          OTEL_EXPORTER_OTLP_HEADERS
  --otlp-headers-file <path>    more of them from a file (k=v a line or a comma list)
  --otlp-ca <path>        PEM CA verifying the endpoint (default the webPKI roots)
  --otlp-client-cert <path>     PEM client certificate for mTLS (with --otlp-client-key)
  --otlp-client-key <path>      PEM private key of --otlp-client-cert
  --otlp-batch-size <records>   records a request takes at most (default 512)
  --otlp-batch-delay <ms>       how long a batch waits after its first line (default 1000)
  --otlp-kinds <kinds>    export only these kinds (default all)
  --otlp-route-keys <keys>      export only these route keys (default all)
  --otlp-node-ids <ids>   export only these nodes (default all)
  --exit-on-stdin-close   exit when stdin reaches its end (a parent's child process)
  --version               print the build (version, commit, build time) and exit

Each option also reads SLUICE_FLUME_<NAME> from the environment, e.g. SLUICE_FLUME_TOKEN;
another variable of that prefix is refused.";

/// What to run: print something and exit, or serve.
#[derive(Debug)]
pub enum Command {
    Help,
    Version,
    Serve(Box<Config>),
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
    "ring-bytes",
    "backfill",
    "forget-after",
    "otlp-endpoint",
    "otlp-service-name",
    "otlp-resource-attributes",
    "otlp-compression",
    "otlp-headers",
    "otlp-headers-file",
    "otlp-ca",
    "otlp-client-cert",
    "otlp-client-key",
    "otlp-batch-size",
    "otlp-batch-delay",
    "otlp-kinds",
    "otlp-route-keys",
    "otlp-node-ids",
];

/// The switches read from the environment too.
const SWITCHES: &[&str] = &["exit-on-stdin-close"];

/// Refuses the variables named as an option's (`SLUICE_FLUME_<NAME>`) that name
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
            name.strip_prefix("SLUICE_FLUME_")
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
    let file = |name: &str| -> Result<Option<Vec<u8>>, String> {
        value(name)
            .map(|path| std::fs::read(&path).map_err(|e| format!("--{name} {path}: {e}")))
            .transpose()
    };
    let otlp = match value("otlp-endpoint") {
        Some(endpoint) => Some(crate::export::OtlpSettings::new(crate::export::RawOtlp {
            endpoint,
            service_name: value("otlp-service-name"),
            resource_attributes: value("otlp-resource-attributes"),
            compression: value("otlp-compression"),
            headers: value("otlp-headers"),
            headers_file: file("otlp-headers-file")?
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
            ca_pem: file("otlp-ca")?,
            client_cert_pem: file("otlp-client-cert")?,
            client_key_pem: file("otlp-client-key")?,
            batch_size: value("otlp-batch-size"),
            batch_delay: value("otlp-batch-delay"),
            kinds: value("otlp-kinds"),
            route_keys: value("otlp-route-keys"),
            node_ids: value("otlp-node-ids"),
        })?),
        // the others configure nothing without an endpoint: refused, not unread
        None => {
            let configured = VALUED
                .iter()
                .filter(|name| name.starts_with("otlp-") && **name != "otlp-endpoint")
                .find(|name| value(name).is_some());
            if let Some(name) = configured {
                return Err(format!("--{name} needs --otlp-endpoint"));
            }
            None
        }
    };
    Ok(Command::Serve(Box::new(Config {
        host: value("host").unwrap_or_else(|| "0.0.0.0".to_string()),
        port,
        exit_on_stdin_close: exit_on_stdin_close
            || env("SLUICE_FLUME_EXIT_ON_STDIN_CLOSE").is_some_and(|value| value == "true"),
        settings: Settings {
            token,
            ring_size: number("ring-size", 10_000)?.max(1),
            ring_bytes: number("ring-bytes", 64 << 20)?.max(1),
            backfill: number("backfill", 200)?,
            forget_after: Some(number("forget-after", 3600)? as u64)
                .filter(|secs| *secs > 0)
                .map(std::time::Duration::from_secs),
            tls,
            otlp,
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
            "SLUICE_FLUME_TOKEN" => Some("from-env".to_string()),
            "SLUICE_FLUME_PORT" => Some("9100".to_string()),
            "SLUICE_FLUME_RING_SIZE" => Some("50".to_string()),
            "SLUICE_FLUME_RING_BYTES" => Some("1024".to_string()),
            "SLUICE_FLUME_OTLP_ENDPOINT" => Some("https://collector.example/v1/logs".to_string()),
            "SLUICE_FLUME_OTLP_BATCH_SIZE" => Some("64".to_string()),
            _ => None,
        };
        let config = serve(parse(
            args("--port 9200 --backfill=7 --otlp-endpoint http://127.0.0.1:4318"),
            env,
        ));
        assert_eq!(config.port, 9200);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.settings.token, "from-env");
        assert_eq!(config.settings.ring_size, 50);
        assert_eq!(config.settings.ring_bytes, 1024);
        assert_eq!(config.settings.backfill, 7);
        let hour = std::time::Duration::from_secs(3600);
        assert_eq!(config.settings.forget_after, Some(hour));
        assert!(config.settings.tls.is_none());
        // the flag overrides the endpoint, which added the signal's path; the
        // batch size stays the environment's
        let otlp = config.settings.otlp.as_ref().expect("the endpoint given");
        assert_eq!(otlp.endpoint(), "http://127.0.0.1:4318/v1/logs");
        assert_eq!(otlp.batch_size, 64);
        assert_eq!(
            serve(parse(args("--token t"), env))
                .settings
                .otlp
                .as_ref()
                .map(|otlp| otlp.endpoint().to_string()),
            Some("https://collector.example/v1/logs".to_string())
        );
        assert!(!config.exit_on_stdin_close);
        assert!(
            serve(parse(args("--token t --exit-on-stdin-close"), |_| None)).exit_on_stdin_close
        );
        // a variable of the options' prefix naming none is refused, not left unread
        assert_eq!(
            check_env(
                [
                    "PATH",
                    "SLUICE_FLUME_TOKEN",
                    "SLUICE_FLUME_OTLP_ENDPOINT",
                    "SLUICE_FLUME_EXIT_ON_STDIN_CLOSE"
                ]
                .map(String::from)
            ),
            Ok(())
        );
        assert_eq!(
            check_env(["SLUICE_FLUME_TOKNE", "SLUICE_FLUME_PORT"].map(String::from)),
            Err("unknown environment variable: SLUICE_FLUME_TOKNE (no option reads it)".into())
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
        let rejected = |line: &str| parse(args(line), none).unwrap_err();
        assert!(
            rejected("--token t --otlp-endpoint ftp://host/logs").contains("not http or https")
        );
        assert!(
            rejected("--token t --otlp-endpoint http://h --otlp-compression lz4").contains("gzip")
        );
        assert!(
            rejected("--token t --otlp-endpoint http://h --otlp-headers AuthorizationBearer")
                .contains("no '='")
        );
        assert!(
            rejected("--token t --otlp-endpoint http://h --otlp-client-cert Cargo.toml")
                .contains("together")
        );
        assert!(
            rejected("--token t --otlp-service-name svc")
                .contains("--otlp-service-name needs --otlp-endpoint")
        );
    }
}
