//! sluice-wicket-cf-api: the Cloud Foundry API facade the stock cf CLI
//! deploys wasm components through -- the subset of the Cloud Controller v3
//! API, the UAA token endpoints and log-cache's read API it uses -- keeping
//! the desired state of its apps and having one wicket serve them through
//! the wicket's app API; the wicket's keyvalue stores and capabilities are
//! its services.
//!
//! ```text
//! cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> sluice-wicket-wasi
//! ```

pub mod api;
pub mod config;
pub mod controller;
pub mod logs;
pub mod manifest;
mod multipart;
pub mod services;
pub mod staging;
pub mod store;
pub mod uaa;
mod views;
pub mod wicket;

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;

/// The owner the apps are put under in the wicket.
pub const OWNER: &str = "cf";

/// This binary's build, embedded by `build.rs`; an empty commit means git
/// was unavailable when it was built.
pub struct Build {
    pub version: &'static str,
    pub commit: &'static str,
    pub dirty: bool,
    pub build_time: &'static str,
}

pub fn build() -> Build {
    Build {
        version: env!("CARGO_PKG_VERSION"),
        commit: env!("SLUICE_BUILD_COMMIT"),
        dirty: env!("SLUICE_BUILD_DIRTY") == "true",
        build_time: env!("SLUICE_BUILD_TIME"),
    }
}

/// [`build`] in one line, e.g. `sluice-wicket-cf-api 0.0.1-SNAPSHOT (<commit>,
/// dirty, built 2026-10-30T01:04:33Z)`.
pub fn describe_build() -> String {
    let build = build();
    let mut details = vec![if build.commit.is_empty() {
        "commit unknown".to_string()
    } else {
        build.commit.to_string()
    }];
    if build.dirty {
        details.push("dirty".into());
    }
    details.push(format!("built {}", build.build_time));
    format!(
        "sluice-wicket-cf-api {} ({})",
        build.version,
        details.join(", ")
    )
}

/// What the server needs once its listener is bound.
#[derive(Clone, Debug)]
pub struct Settings {
    /// The wicket's console, `http://host:port`.
    pub wicket: String,
    /// Its `--console-token`.
    pub wicket_token: Option<String>,
    /// The shared domains; the first takes the default and random routes.
    pub domains: Vec<String>,
    /// The state, the packages and the droplets.
    pub data_dir: PathBuf,
    pub org: String,
    pub space: String,
}

/// Binds `listen`: a loopback address only, as the API takes any
/// credentials yet.
pub async fn bind(listen: &str) -> Result<TcpListener, String> {
    let failed = |e: std::io::Error| format!("--listen {listen}: {e}");
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(listen)
        .await
        .map_err(failed)?
        .collect();
    if resolved.iter().any(|at| !at.ip().is_loopback()) {
        return Err(format!(
            "--listen {listen}: the API takes any credentials, so it listens on a loopback address only"
        ));
    }
    TcpListener::bind(&resolved[..]).await.map_err(failed)
}

/// Serves the API on `listener` until `shutdown` completes, the wicket
/// reconciled all along.
pub async fn serve(
    listener: TcpListener,
    settings: Settings,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    std::fs::create_dir_all(&settings.data_dir)
        .map_err(|e| format!("--data-dir {}: {e}", settings.data_dir.display()))?;
    let store = Arc::new(store::Store::open(
        &settings.data_dir,
        &settings.org,
        &settings.space,
        &settings.domains,
    )?);
    let uaa = uaa::Uaa::open(&settings.data_dir)?;
    let logs = Arc::new(logs::Logs::default());
    let wicket = Arc::new(wicket::Wicket::new(
        &settings.wicket,
        settings.wicket_token.clone(),
        OWNER,
    ));
    let controller = Arc::new(controller::Controller {
        store: store.clone(),
        wicket,
        logs: logs.clone(),
        edits: tokio::sync::Mutex::new(()),
        offer: std::sync::Mutex::new(None),
    });
    let api = Arc::new(api::Api {
        store,
        uaa,
        logs,
        controller: controller.clone(),
        build: describe_build(),
    });
    let watch = tokio::spawn(controller.watch());
    tokio::select! {
        () = api::run(listener, api) => {}
        () = shutdown => {}
    }
    watch.abort();
    Ok(())
}
