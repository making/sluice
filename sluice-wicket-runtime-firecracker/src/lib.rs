//! sluice-wicket-runtime-firecracker: a tunnel client serving each route with
//! Firecracker microVMs, as sluice-wicket-runtime-wasi serves each with a wasi:http
//! component.
//!
//! ```text
//! sluice server(s) --tunnel--> wicket --vsock (raw bytes per connection)--> guest in a microVM
//! ```
//!
//! The tunnel client is `sluice-wicket-tunnel`'s, as sluice-wicket-runtime-wasi's: the
//! `--server` endpoint is asked for the node list, one tunnel stream per node
//! advertises the routes and serves their connections, and SIGTERM / SIGINT
//! drains the streams within `--drain-grace`.
//!
//! The wicket does not terminate HTTP: each tunnel connection is relayed
//! verbatim to a vsock stream of a guest (framed, see `wire`, to carry
//! half-closes), as the Java client relays to a TCP upstream; the guest's
//! `/init` relays it on to the process's TCP port (`routes`). A route's VMs
//! are either one shared VM, or a VM per connection cloned from a snapshot
//! (`route`, `vm`), within the VMs' budget (`capacity`).
//!
//! The processes' output in the VMs reaches the wicket's stdout / stderr, and
//! with `--flume` the live access log (`output`, through `sluice-wicket-tunnel`'s
//! feed).
//!
//! The guests have no network device: their connections to the destinations of
//! their route's `egress` reach the wicket over vsock, which connects to them
//! (`egress`).
//!
//! In jail mode (`--jail-ids`) the process started as root is the keeper
//! (`keeper`): it runs the wicket as `--jail-user` and spawns the VMs at its
//! request, each in a jail of its own (`jail`).

pub mod capacity;
pub mod config;
pub mod egress;
pub mod jail;
pub mod keeper;
pub mod listener;
pub mod output;
pub mod route;
pub mod routes;
pub mod state;
pub mod vm;
pub mod wire;

use std::sync::Arc;

use sluice_wicket_tunnel::flume::{self, Tap};
use sluice_wicket_tunnel::proto::PeerBuild;
use sluice_wicket_tunnel::session::{self, Shared};
use sluice_wicket_tunnel::signals::{self, Signals};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use capacity::Capacity;
use output::{Printer, Sink};
use routes::Reloader;
use vm::Launcher;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// This binary's build, embedded by `build.rs`; an empty commit means git
/// was unavailable when it was built.
pub fn build() -> PeerBuild {
    PeerBuild {
        implementation: "sluice-wicket-runtime-firecracker".into(),
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

/// Boots the routes' VMs as `launcher` starts them and serves them (through the
/// server, on `--listen`, or both) until SIGTERM / SIGINT: until the drain
/// after it ended, or at once where nothing drains (without `--server`, or
/// with `--drain-grace 0`).
pub async fn run(settings: config::Settings, launcher: Launcher) -> Result<(), BoxError> {
    let config::Settings {
        server,
        listen,
        token,
        client_id,
        insecure,
        ca_cert,
        max_connections,
        stall_timeout,
        drain_grace,
        max_vms,
        max_memory,
        watch_interval,
        state_dir: _,
        flume,
        jail: _,
        routes,
        specs,
    } = settings;
    println!("wicket build: {}", describe_build());
    // installed whether or not a drain follows: as the init (pid 1) of a
    // container, a process without a handler ignores SIGTERM
    let mut signals = Signals::install()?;
    let ca_pem = match ca_cert {
        Some(path) => {
            Some(std::fs::read(&path).map_err(|e| format!("--ca-cert {}: {e}", path.display()))?)
        }
        None => None,
    };
    let (open_files, vm_open_files) = vm::set_open_files(max_connections)?;
    println!("open file limit {open_files}, each VM's {vm_open_files}");
    let capacity = Capacity::new(max_vms, max_memory);
    println!(
        "VM limit {max_vms}, guest memory limit {}",
        max_memory.map_or("none".to_string(), |mib| format!("{mib} MiB"))
    );
    let tap = match flume {
        Some(_) => Tap::new(),
        None => Tap::off(),
    };
    let sink = Arc::new(Sink {
        printer: Printer::stdio(),
        tap: tap.clone(),
    });
    let places = Arc::new(launcher.start()?);
    let reloader = Reloader::start(routes, specs, capacity, places, sink).await?;
    let routes = reloader.routes().clone();
    if let Some(flume) = flume {
        flume::start(flume, tap, &client_id)?;
    }
    println!(
        "routes: {}; watching their files every {}ms (0 = off), SIGHUP reloads",
        routes.current().advertised_hosts(),
        watch_interval.as_millis()
    );
    let (hangups, hangups_received) = mpsc::channel(1);
    signals::forward_hangups(hangups)?;
    if let Some(address) = listen {
        let listener = tokio::net::TcpListener::bind(&address).await?;
        println!("listening on {}", listener.local_addr()?);
        tokio::spawn(listener::run(listener, routes.clone()));
    }
    let Some(server) = server else {
        // local mode alone: nothing to drain, a signal ends the process at once
        let reloads = reloader.run(watch_interval, hangups_received, CancellationToken::new());
        tokio::select! {
            () = reloads => {}
            name = signals.recv() => println!("{name}: ending at once"),
        }
        return Ok(());
    };
    let shared = Shared::new(
        session::Settings {
            token,
            id: client_id,
            insecure,
            ca_pem,
            max_connections,
            stall_timeout,
            build: build(),
        },
        Arc::clone(&routes),
    );
    // the drain stops the reloads: the routes are withdrawn
    tokio::spawn(reloader.run(watch_interval, hangups_received, shared.drain.begin.clone()));
    sluice_wicket_tunnel::serve(shared, server, drain_grace, &mut signals).await
}
