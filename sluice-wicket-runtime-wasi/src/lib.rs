//! sluice-wicket-runtime-wasi: a wasm platform on the sluice tunnel.
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
//! The tunnel client is `sluice-wicket-tunnel`'s, as the other Rust wickets': the
//! `--server` endpoint is asked for the node list, one tunnel stream per node
//! advertises the routes and serves their connections, and SIGTERM / SIGINT
//! drains the streams within `--drain-grace`. Each connection is served by
//! the component of its target (`wasm_host::serve`).
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
//! `--flume` feeds the guests' output and the answered requests to a
//! sluice-flume (`sluice-wicket-tunnel`'s `flume`), the live access log the
//! sluice console and the cf-api watch.
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
//! sluice-wicket-runtime-wasi --server grpc://127.0.0.1:8001 --token SECRET \
//!     --wasm demo.local=./hello.wasm \
//!     --wasm relay.local='./relay.wasm?wasi=cli,inherit-network'
//! sluice-wicket-runtime-wasi --config wicket-runtime-wasi.yaml
//! sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 --wasm 'counter.local=./counter.wasm?wasi=keyvalue'
//! sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090
//! ```

pub mod config;
mod console;
mod deployed;
mod files;
mod keyvalue;
mod listener;
mod routes;
mod wasm_host;

use sluice_wicket_console::env;

use std::sync::Arc;

use sluice_wicket_tunnel::flume::{self, Tap};
use sluice_wicket_tunnel::session::Shared;
use sluice_wicket_tunnel::signals::{self, Signals};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The tunnel's messages and the flume's (`sluice-wicket-tunnel`'s).
pub use sluice_wicket_tunnel::proto;

use console::Console;
use deployed::Deployed;
use proto::PeerBuild;
use routes::Reloader;
use wasm_host::Instances;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// This binary's build, embedded by `build.rs`; an empty commit means git
/// was unavailable when it was built.
pub fn build() -> PeerBuild {
    PeerBuild {
        implementation: "sluice-wicket-runtime-wasi".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("SLUICE_BUILD_COMMIT").into(),
        dirty: env!("SLUICE_BUILD_DIRTY") == "true",
        build_time: env!("SLUICE_BUILD_TIME").into(),
    }
}

/// [`build`] in one line, see [`sluice_wicket_tunnel::describe`].
pub fn describe_build() -> String {
    sluice_wicket_tunnel::describe(&build())
}

/// Serves the routes of `settings` (through the server, on `--listen`, or
/// both) until SIGTERM / SIGINT: until the drain after it ended, or at once
/// where nothing drains (without `--server`, or with `--drain-grace 0`).
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
        flume,
        mut routes,
    } = settings;
    println!("wicket build: {}", describe_build());
    // before any component compiles: the pool is sized once, by the instance limit
    println!("{}", wasm_host::start_engine(max_instances));
    let tap = match flume {
        Some(_) => Tap::new(),
        None => Tap::off(),
    };
    let instances = Instances::fed(max_instances, tap.clone());
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
        Some(console) => Some(console.bind().await?),
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
            console.credentials(),
            console.dir.display()
        );
        tokio::spawn(sluice_wicket_console::serve::run(
            listener,
            console.gate("sluice-wicket-runtime-wasi")?,
            Arc::new(Console {
                routes: routes.clone(),
                status: reloader.status(),
                requests,
                stores: reloader.stores().clone(),
                server: server.clone(),
                listen: listening,
                dir: console.dir.clone(),
            }),
        ));
    } else {
        // no console: the reloader sees the channel closed, no edit comes
        drop(requests);
    }
    if let Some(flume) = flume {
        flume::start(flume, tap, &client_id)?;
    }
    // installed whether or not a drain follows: as the init (pid 1) of a
    // container, a process without a handler ignores SIGTERM
    let mut signals = Signals::install()?;
    let Some(server) = server else {
        // local mode alone: nothing to drain, a signal ends the process at once
        let reloads = reloader.run(
            watch_interval,
            hangups_received,
            requests_received,
            CancellationToken::new(),
        );
        tokio::select! {
            () = reloads => {}
            name = signals.recv() => println!("{name}: ending at once"),
        }
        return Ok(());
    };

    let ca_pem = match ca_cert {
        Some(path) => {
            Some(std::fs::read(&path).map_err(|e| format!("--ca-cert {}: {e}", path.display()))?)
        }
        None => None,
    };
    let shared = Shared::new(
        sluice_wicket_tunnel::session::Settings {
            token,
            id: client_id,
            insecure,
            ca_pem,
            max_connections,
            stall_timeout,
            build: build(),
            workers: None,
        },
        routes,
    );

    // the drain stops the reloads (the console's edits too): the routes are withdrawn
    tokio::spawn(reloader.run(
        watch_interval,
        hangups_received,
        requests_received,
        shared.drain.begin.clone(),
    ));

    sluice_wicket_tunnel::serve(shared, server, drain_grace, &mut signals).await
}
