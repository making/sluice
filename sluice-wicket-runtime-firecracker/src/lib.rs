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
//! are either VMs booted, or clones of a snapshot -- one per connection,
//! reused by the next connections, or a pool serving many each -- (`route`, `vm`),
//! within the VMs' budget (`capacity`).
//!
//! `--listen` serves the routes over plain TCP as well, each connection routed
//! by its first request's host (`listener`).
//!
//! The processes' output in the VMs reaches the wicket's stdout / stderr, and
//! with `--flume` the live access log (`output`, through `sluice-wicket-tunnel`'s
//! feed).
//!
//! The guests have no network device: their connections to the destinations of
//! their route's `egress` reach the wicket over vsock, which connects to them
//! (`egress`).
//!
//! `--console` serves a page and an API (`console`) deploying uploaded rootfs
//! images as routes without a restart, kept in the console's directory
//! (`deployed`) across restarts. An app's booted VM may have a disk there that
//! outlives it (`disk`).
//!
//! In jail mode (`--jail-ids`) the process started as root is the keeper
//! (`keeper`): it runs the wicket as `--jail-user` and spawns the VMs at its
//! request, each in a jail of its own (`jail`).

pub mod capacity;
pub mod config;
pub mod console;
pub mod deployed;
pub mod disk;
pub mod egress;
pub mod exchange;
pub mod head;
pub mod hpack;
pub mod image;
pub mod jail;
pub mod keeper;
pub mod listener;
pub mod output;
pub mod rootfs;
pub mod route;
pub mod routes;
pub mod seccomp;
pub mod state;
pub mod vm;
pub mod wire;

use std::sync::Arc;

use sluice_wicket_tunnel::flume::{self, Tap};
use sluice_wicket_tunnel::proto::PeerBuild;
use sluice_wicket_tunnel::session::{self, Shared};
use sluice_wicket_tunnel::signals::{self, Signals};
use sluice_wicket_tunnel::workers::Workers;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use capacity::Capacity;
use console::Console;
use deployed::Deployed;
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
/// with `--drain-grace 0`). However that ends, every VM on a disk is stopped by then.
pub async fn run(
    settings: config::Settings,
    launcher: Launcher,
    workers: Arc<Workers>,
) -> Result<(), BoxError> {
    let stops = TaskTracker::new();
    let ran = serving(settings, launcher, workers, stops.clone()).await;
    stops.close();
    stops.wait().await;
    ran
}

/// [`run`], but for the stops under way in `stops`, which what it leaves behind goes to.
async fn serving(
    settings: config::Settings,
    launcher: Launcher,
    workers: Arc<Workers>,
    stops: TaskTracker,
) -> Result<(), BoxError> {
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
        max_instances,
        max_memory_mib,
        workers: _,
        watch_interval,
        state_dir: _,
        guest,
        flume,
        jail,
        mut console,
        mut routes,
        specs: _,
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
    // a VM's connections: the tunnel's and the listener's
    let serving = usize::from(server.is_some()) + usize::from(listen.is_some());
    let (open_files, vm_open_files) = vm::set_open_files(max_connections * serving)?;
    println!("open file limit {open_files}, each VM's {vm_open_files}");
    let capacity = Capacity::new(max_instances, max_memory_mib);
    println!(
        "VM limit {max_instances}, guest memory limit {}",
        max_memory_mib.map_or("none".to_string(), |mib| format!("{mib} MiB"))
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
    // opened by the wicket, its user's (jail mode: not the keeper's)
    let mut intake = None;
    if let Some(console) = &mut console {
        let deployed = Deployed::open(&console.dir)?;
        // named as it resolved from here on: the log says where the files are
        console.dir = deployed.dir().to_path_buf();
        intake = Some(deployed.images().intake());
        routes.deploy(deployed);
    }
    let reloader = Reloader::start(
        routes,
        capacity.clone(),
        places,
        sink,
        workers.clone(),
        stops,
    )
    .await?;
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
    let mut listening = None;
    if let Some(address) = listen {
        let listener = sluice_wicket_tunnel::listener::bind(&address).await?;
        listening = Some(listener.local_addr()?);
        println!(
            "listening on {}, {max_connections} connections at most",
            listener.local_addr()?
        );
        tokio::spawn(listener::run(
            listener,
            routes.clone(),
            workers.clone(),
            max_connections,
        ));
    }
    let (requests, requests_received) = mpsc::channel(8);
    match console.zip(intake) {
        Some((console, intake)) => {
            let listener = console.bind().await?;
            println!(
                "console on http://{} ({}); its routes kept in {}",
                listener.local_addr()?,
                console.credentials(),
                console.dir.display()
            );
            tokio::spawn(sluice_wicket_console::serve::run(
                listener,
                console.gate("sluice-wicket-runtime-firecracker")?,
                Arc::new(Console {
                    routes: routes.clone(),
                    status: reloader.status(),
                    requests,
                    capacity,
                    images: guest.map(|guest| images(guest, &console.dir)).transpose()?,
                    dir: console.dir,
                    intake,
                    jailed: jail.is_some(),
                    server: server.clone(),
                    listen: listening,
                }),
            ));
        }
        // no console: the reloader sees the channel closed, no edit comes
        None => drop(requests),
    }
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
        routes.current().stop_disks().await;
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
            workers: Some(workers),
        },
        Arc::clone(&routes),
    );
    // the drain stops the reloads: the routes are withdrawn
    let reloads = tokio::spawn(reloader.run(
        watch_interval,
        hangups_received,
        requests_received,
        shared.drain.begin.clone(),
    ));
    let served = sluice_wicket_tunnel::serve(shared, server, drain_grace, &mut signals).await;
    // ended with what it held, the starts under way among it: done whatever ended the serving
    reloads.abort();
    let _ = reloads.await;
    routes.current().stop_disks().await;
    served
}

/// What the console makes rootfs images with (`--guest`), in the directory `dir`.
fn images(guest: std::path::PathBuf, dir: &std::path::Path) -> Result<image::Images, BoxError> {
    std::fs::File::open(&guest).map_err(|e| format!("--guest {}: {e}", guest.display()))?;
    Ok(image::Images {
        registry: sluice_wicket_registry::Registry::new(concat!(
            "sluice-wicket-runtime-firecracker/",
            env!("CARGO_PKG_VERSION")
        ))?,
        guest,
        tools: rootfs::Tools::find()?,
        building: tokio::sync::Semaphore::new(1),
        limit: console::MAX_ROOTFS_BYTES,
        work: dir.join(deployed::BUILDS),
    })
}
