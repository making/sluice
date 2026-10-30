//! sluice-wicket-cf-api: the Cloud Foundry API facade the stock cf CLI
//! deploys wasm components through -- the subset of the Cloud Controller v3
//! API, the UAA token endpoints and log-cache's read API it uses -- keeping
//! the desired state of its apps and having one wicket serve them through
//! the wicket's app API; a component comes in a zip or from an OCI registry
//! (`registry`), and the wicket's keyvalue stores and capabilities are its
//! services. With a flume, the apps' output and requests reach their logs
//! from the live access log the wicket feeds (`flume`).
//!
//! ```text
//! cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> sluice-wicket-wasi
//!                                                  ^                              |
//!                                                  +--Watch-- sluice-flume <--Feed+
//! ```

pub mod api;
pub mod config;
pub mod controller;
mod env;
pub mod flume;
pub mod logs;
pub mod manifest;
mod multipart;
/// The OpenID Provider client, shared with the wicket's console.
pub use sluice_oidc as oidc;
pub mod registry;
pub mod services;
pub mod staging;
pub mod store;
mod tls;
pub mod uaa;
mod views;
pub mod wicket;

pub mod proto {
    tonic::include_proto!("sluice.v1");
}

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
    /// Where the apps' output and requests come from; None: the logs hold
    /// the cf-api's own lines only.
    pub flume: Option<flume::FlumeSettings>,
    /// The OpenID Provider users log in through; None: any credentials pass.
    pub oidc: Option<oidc::OidcSettings>,
    /// The clients of `cf auth --client-credentials`, with the provider.
    pub clients: Vec<(String, String)>,
    /// The listener's own TLS; None: plain HTTP.
    pub tls: Option<TlsSettings>,
}

/// The listener's certificate chain and private key, PEM.
#[derive(Clone, Debug)]
pub struct TlsSettings {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// Binds `listen`; `loopback_only` (no provider: the API takes any
/// credentials) refuses any other address.
pub async fn bind(listen: &str, loopback_only: bool) -> Result<TcpListener, String> {
    let failed = |e: std::io::Error| format!("--listen {listen}: {e}");
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(listen)
        .await
        .map_err(failed)?
        .collect();
    if loopback_only && resolved.iter().any(|at| !at.ip().is_loopback()) {
        return Err(format!(
            "--listen {listen}: without --oidc-issuer the API takes any credentials, so it listens on a loopback address only"
        ));
    }
    TcpListener::bind(&resolved[..]).await.map_err(failed)
}

/// The TLS of the listener, from `--tls-cert` / `--tls-key`.
fn tls_acceptor(tls: &TlsSettings) -> Result<tokio_rustls::TlsAcceptor, String> {
    use rustls::pki_types::pem::PemObject as _;
    let certs = rustls::pki_types::CertificateDer::pem_slice_iter(&tls.cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("--tls-cert: {e}"))?;
    if certs.is_empty() {
        return Err("--tls-cert: no certificate in it".into());
    }
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(&tls.key_pem)
        .map_err(|e| format!("--tls-key: {e}"))?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("tls: {e}"))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| format!("--tls-cert / --tls-key: {e}"))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Serves the API on `listener` until `shutdown` completes, the wicket
/// reconciled all along.
pub async fn serve(
    listener: TcpListener,
    settings: Settings,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    // reqwest brings no crypto provider of its own (sluice-oidc's
    // rustls-no-provider): ring, as tonic's TLS and the listener's
    let _ = rustls::crypto::ring::default_provider().install_default();
    std::fs::create_dir_all(&settings.data_dir)
        .map_err(|e| format!("--data-dir {}: {e}", settings.data_dir.display()))?;
    let store = Arc::new(store::Store::open(
        &settings.data_dir,
        &settings.org,
        &settings.space,
        &settings.domains,
    )?);
    let mode = match &settings.oidc {
        Some(oidc) => uaa::Mode::Provider {
            clients: settings.clients.clone(),
            rules: oidc.claims.clone(),
        },
        None => uaa::Mode::Open,
    };
    let uaa = uaa::Uaa::open(&settings.data_dir, mode)?;
    let provider = match &settings.oidc {
        Some(oidc) => Some(oidc::Provider::new(oidc.clone())?),
        None => None,
    };
    let acceptor = match &settings.tls {
        Some(tls) => Some(tls_acceptor(tls)?),
        None => None,
    };
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
    let flume = settings
        .flume
        .clone()
        .map(|flume| tokio::spawn(flume::watch(flume, store.clone(), logs.clone())));
    let api = Arc::new(api::Api {
        store: store.clone(),
        uaa,
        provider,
        tls: acceptor.is_some(),
        logs,
        controller: controller.clone(),
        registry: registry::Registry::new()?,
        build: describe_build(),
    });
    let watch = tokio::spawn(controller.watch());
    tokio::select! {
        () = api::run(listener, acceptor, api) => {}
        () = shutdown => {}
    }
    watch.abort();
    if let Some(flume) = flume {
        flume.abort();
        // the watch's cursor as it got
        store.update(|_| ());
    }
    Ok(())
}
