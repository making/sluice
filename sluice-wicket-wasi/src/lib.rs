//! sluice-wicket-wasi: a wasm platform on the sluice tunnel.
//!
//! Connects to a sluice server with the same `sluice.v1.Tunnel` gRPC API as
//! the Java client, but instead of relaying to TCP upstreams it terminates
//! HTTP on the tunnel and handles every request with a wasi:http 0.3
//! component (`wasi:http/handler@0.3.0`). One process, N routes.
//!
//! ```text
//! sluice server(s) --tunnel stream per node--> wicket --hyper (h1 / h2c)--> wasi:http component
//! ```
//!
//! The supervisor (`supervisor`) ports the Java client's fan-out semantics
//! (`TunnelClient` / `NodeConnection`): the `--server` endpoint is only asked
//! for the node list (`ListNodes`); the membership keeps one tunnel stream
//! (`session`) per node at its public url, and a dropped stream reconnects
//! with jittered exponential backoff (ceiling 1s..30s, each wait drawn from
//! its upper half). Exactly one stream per node is kept: the server closes
//! the older registration when a client id re-registers.
//!
//! On SIGTERM / SIGINT the wicket drains (`drain`, the Java client's
//! `TunnelClient#drain`): every node stream withdraws the routes and ends
//! once the server acknowledged it and no connection is left on it, within
//! `--drain-grace`.
//!
//! The routes come from `--wasm` flags and / or a `--config` YAML file
//! (`config`) and are reloaded without a restart (`routes::Reloader`): on a
//! change of their files, or on SIGHUP. Every node stream advertises the
//! reloaded routes again.
//!
//! `--listen` serves the same routes over plain TCP (`listener`), routed by
//! the request's host; without `--server` that is all the wicket does (local
//! mode). A `wasi=keyvalue` route's guest keeps state in the host's stores
//! (`keyvalue`).
//!
//! `--console` serves a page and an API (`console`) deploying uploaded
//! components as routes, through the same reload; they are kept in the
//! console's directory (`deployed`) across restarts.
//!
//! Scope (deliberately minimal):
//!
//! - `grpc://` / `grpcs://` control plane (`--ca-cert` pins the CA,
//!   `--insecure` skips verification); no mTLS yet
//! - one pre-linked (`InstancePre`) instance per request, under a per-route
//!   epoch CPU budget, memory cap and `wasmtime serve`-style WASI
//!   capabilities, and a process-wide instance limit (`--max-instances`)
//!   with optional per-route caps (`instances=N`);
//!   response bodies stream via a bounded relay
//!
//! Usage:
//!
//! ```text
//! sluice-wicket-wasi --server grpc://127.0.0.1:8001 --token SECRET \
//!     --wasm demo.local=./hello.wasm \
//!     --wasm relay.local='./relay.wasm?wasi=cli,inherit-network'
//! sluice-wicket-wasi --config wicket-wasi.yaml
//! sluice-wicket-wasi --listen 127.0.0.1:8080 --wasm 'counter.local=./counter.wasm?wasi=keyvalue'
//! sluice-wicket-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090
//! ```

pub mod config;
mod console;
mod deployed;
mod drain;
mod keyvalue;
mod listener;
mod routes;
mod session;
mod signals;
mod supervisor;
mod tls;
#[cfg(test)]
mod transport_window_test;
mod tunnel;
mod wasm_host;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub mod proto {
    tonic::include_proto!("sluice.v1");
}

use console::Console;
use deployed::Deployed;
use drain::Drain;
use proto::PeerBuild;
use routes::Reloader;
use session::{Shared, transport_window};
use signals::Signals;
use wasm_host::Instances;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How a server that sent no build (it predates the field) is described.
const UNKNOWN_BUILD: &str = "unknown (older build)";

/// Longest value kept from each string of a server's build, in chars.
const MAX_FIELD_CHARS: usize = 64;

/// This binary's build, embedded by `build.rs`; an empty commit means git
/// was unavailable when it was built.
pub fn build() -> PeerBuild {
    PeerBuild {
        implementation: "sluice-wicket-wasi".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("SLUICE_BUILD_COMMIT").into(),
        dirty: env!("SLUICE_BUILD_DIRTY") == "true",
        build_time: env!("SLUICE_BUILD_TIME").into(),
    }
}

/// [`build`] in one line, see [`describe`].
pub fn describe_build() -> String {
    describe(&build())
}

/// One line naming a build, e.g. `sluice-wicket-wasi 0.0.1-SNAPSHOT
/// (<commit>, dirty, built 2026-10-12T01:04:33Z)`; an empty field reads as
/// unknown.
fn describe(build: &PeerBuild) -> String {
    let or = |value: &str, unknown: &str| {
        if value.is_empty() {
            unknown.to_string()
        } else {
            value.to_string()
        }
    };
    let mut details = vec![or(&build.commit, "commit unknown")];
    if build.dirty {
        details.push("dirty".into());
    }
    if !build.build_time.is_empty() {
        details.push(format!("built {}", build.build_time));
    }
    format!(
        "{} {} ({})",
        or(&build.implementation, "unknown implementation"),
        or(&build.version, "version unknown"),
        details.join(", ")
    )
}

/// [`describe`] of a build another peer sent: control characters become `?`
/// and each string keeps at most [`MAX_FIELD_CHARS`] chars, so it cannot
/// forge or flood log lines.
fn describe_peer(build: &PeerBuild) -> String {
    let bound = |value: &str| -> String {
        let mut bounded: String = value
            .chars()
            .take(MAX_FIELD_CHARS)
            .map(|c| if c.is_control() { '?' } else { c })
            .collect();
        if value.chars().count() > MAX_FIELD_CHARS {
            bounded.push_str("...");
        }
        bounded
    };
    describe(&PeerBuild {
        implementation: bound(&build.implementation),
        version: bound(&build.version),
        commit: bound(&build.commit),
        dirty: build.dirty,
        build_time: bound(&build.build_time),
    })
}

/// Connects to the server and serves the routes of `settings` until the drain
/// after SIGTERM / SIGINT ended, or at once on one with `--drain-grace 0`.
pub async fn run(settings: config::Settings) -> Result<(), BoxError> {
    let config::Settings {
        server,
        listen,
        token,
        client_id,
        insecure,
        ca_cert,
        max_connections,
        max_instances,
        stall_timeout,
        drain_grace,
        watch_interval,
        mut console,
        mut routes,
    } = settings;
    println!("wicket build: {}", describe_build());
    let instances = Instances::new(max_instances);
    if let Some(console) = &mut console {
        let deployed = Deployed::open(&console.dir)?;
        // named as it resolved from here on: the log says where the files are
        console.dir = deployed.dir().to_path_buf();
        routes.deploy(deployed);
    }

    // Fetches / prewarms every route's component (fail fast) and builds its
    // handler on `instances`.
    let reloader = Reloader::start(routes, instances.clone()).await?;
    let routes = reloader.routes().clone();
    let listener = match &listen {
        Some(address) => Some(listener::bind(address).await?),
        None => None,
    };
    let console_listener = match &console {
        Some(console) => Some(console::bind(&console.address, console.token.is_some()).await?),
        None => None,
    };
    println!(
        "instance limit {max_instances} ({} h2 streams per connection)",
        instances.streams_per_connection()
    );
    println!(
        "routes: {} (generation {}); watching their files every {}ms (0 = off), SIGHUP reloads",
        routes.current().advertised.len(),
        routes.current().generation,
        watch_interval.as_millis()
    );
    let (hangups, hangups_received) = mpsc::channel(1);
    signals::forward_hangups(hangups)?;
    let mut listening = None;
    if let Some(listener) = listener {
        let address = listener.local_addr()?;
        println!("listening on {address} (http/1.1, h2c), {max_connections} connections at most");
        listening = Some(address);
        tokio::spawn(listener::run(
            listener,
            routes.clone(),
            instances.clone(),
            max_connections,
        ));
    }
    let (requests, requests_received) = mpsc::channel(8);
    if let (Some(console), Some(listener)) = (console, console_listener) {
        println!(
            "console on http://{} ({}); its routes kept in {}",
            listener.local_addr()?,
            match console.token {
                Some(_) => "--console-token required",
                None => "loopback, no token",
            },
            console.dir.display()
        );
        tokio::spawn(console::run(
            listener,
            Arc::new(Console {
                routes: routes.clone(),
                status: reloader.status(),
                requests,
                token: console.token,
                server: server.clone(),
                listen: listening,
            }),
        ));
    } else {
        // no console: the reloader sees the channel closed, no edit comes
        drop(requests);
    }
    let Some(server) = server else {
        // local mode alone: nothing to drain, a signal ends the process
        reloader
            .run(
                watch_interval,
                hangups_received,
                requests_received,
                CancellationToken::new(),
            )
            .await;
        return Ok(());
    };

    let ca_pem = match ca_cert {
        Some(path) => {
            Some(std::fs::read(&path).map_err(|e| format!("--ca-cert {}: {e}", path.display()))?)
        }
        None => None,
    };
    let shared = Arc::new(Shared {
        token,
        id: client_id,
        insecure,
        ca_pem,
        routes,
        limit: tunnel::Limit::new(max_connections),
        stall_timeout,
        transport_window: transport_window(max_connections),
        drain: Drain::default(),
        tasks: TaskTracker::new(),
        up: Default::default(),
    });
    println!("connection limit {max_connections}");
    println!(
        "gRPC flow control window {} bytes",
        transport_window(max_connections)
    );
    println!(
        "tunnel stall timeout {}s (0 = off)",
        stall_timeout.as_secs()
    );
    println!("drain grace {}s (0 = off)", drain_grace.as_secs());

    // the drain stops the reloads (the console's edits too): the routes are withdrawn
    tokio::spawn(reloader.run(
        watch_interval,
        hangups_received,
        requests_received,
        shared.drain.begin.clone(),
    ));

    let (tx, rx) = mpsc::channel(32);
    let supervisor = supervisor::supervise(rx, tx, shared.clone(), server);
    if drain_grace.is_zero() {
        // no handler: a signal ends the process at once
        return supervisor.await;
    }
    let mut signals = Signals::install()?;
    tokio::select! {
        result = supervisor => return result,
        // dropping the supervisor: no membership change opens a stream from now on
        name = signals.recv() => println!("{name}: draining"),
    }
    let interrupt = async {
        let name = signals.recv().await;
        eprintln!("{name} during the drain: ending it at once");
    };
    drain::drain(&shared, drain_grace, interrupt).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_names_every_known_part_and_reads_empty_as_unknown() {
        let build = PeerBuild {
            implementation: "sluice-server".into(),
            version: "0.0.1-SNAPSHOT".into(),
            commit: "7312472".into(),
            dirty: true,
            build_time: "2026-10-12T01:04:33Z".into(),
        };
        assert_eq!(
            describe(&build),
            "sluice-server 0.0.1-SNAPSHOT (7312472, dirty, built 2026-10-12T01:04:33Z)"
        );
        assert_eq!(
            describe(&PeerBuild::default()),
            "unknown implementation version unknown (commit unknown)"
        );
    }

    #[test]
    fn describe_peer_bounds_what_the_server_sent() {
        let build = PeerBuild {
            implementation: "sluice-server\nforged".into(),
            version: "9".repeat(MAX_FIELD_CHARS + 1),
            ..Default::default()
        };
        assert_eq!(
            describe_peer(&build),
            format!(
                "sluice-server?forged {}... (commit unknown)",
                "9".repeat(MAX_FIELD_CHARS)
            )
        );
    }
}
