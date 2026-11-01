use std::path::PathBuf;
use std::sync::Arc;

use sluice_wicket_firecracker::route::{Route, RouteSpec, Routes};
use sluice_wicket_firecracker::{BoxError, listener, tunnel};

const USAGE: &str = "usage: sluice-wicket-firecracker --kernel <vmlinux> \
--vm host=<rootfs.ext4>[?mode=shared|per-connection&vcpus=N&mem=MiB&port=N&warm=N]... \
[--server grpc://host:port --token T --client-id ID] [--listen host:port] \
[--firecracker <path>] [--state-dir <dir>]";

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let (mut server, mut token, mut id, mut listen) = (None, String::new(), None, None);
    let mut firecracker = PathBuf::from("firecracker");
    let mut kernel = None;
    let mut state_dir = None;
    let mut vms = Vec::new();
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{arg}: missing value\n{USAGE}"))
        };
        match arg.as_str() {
            "--server" => server = Some(value()?),
            "--token" => token = value()?,
            "--client-id" => id = Some(value()?),
            "--listen" => listen = Some(value()?),
            "--firecracker" => firecracker = PathBuf::from(value()?),
            "--kernel" => kernel = Some(PathBuf::from(value()?)),
            "--state-dir" => state_dir = Some(PathBuf::from(value()?)),
            "--vm" => vms.push(value()?),
            "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            _ => return Err(format!("unknown argument {arg}\n{USAGE}").into()),
        }
    }
    let kernel = std::path::absolute(kernel.ok_or(USAGE)?)?;
    if vms.is_empty() || (server.is_none() && listen.is_none()) {
        return Err(USAGE.into());
    }
    // absolute: each firecracker runs in a directory of its own
    let state_dir = std::path::absolute(state_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("sluice-wicket-firecracker-{}", std::process::id()))
    }))?;
    let mut routes = Routes::new();
    for (index, flag) in vms.iter().enumerate() {
        let spec = RouteSpec::parse(flag, &firecracker, &kernel)?;
        let route = Route::start(spec, state_dir.join(format!("route-{index}"))).await?;
        routes.insert(route.spec.address(), Arc::new(route));
    }
    let routes = Arc::new(routes);
    if let Some(address) = listen {
        if routes.len() != 1 {
            return Err("--listen serves a single --vm route".into());
        }
        let listener = tokio::net::TcpListener::bind(&address).await?;
        println!("listening on {}", listener.local_addr()?);
        let route = routes.values().next().unwrap().clone();
        tokio::spawn(listener::run(listener, route));
    }
    if let Some(server) = server {
        let settings = tunnel::Settings {
            server,
            token,
            id: id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        };
        println!("tunnel to {} as {}", settings.server, settings.id);
        tokio::spawn(tunnel::run(settings, routes.clone()));
    }
    tokio::signal::ctrl_c().await?;
    // the routes' VMs are killed as the routes drop
    drop(routes);
    let _ = std::fs::remove_dir_all(&state_dir);
    Ok(())
}
