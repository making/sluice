//! sluice-wicket-cf-api: the Cloud Foundry API facade the stock cf CLI
//! deploys wasm components (or, on Firecracker wickets, rootfs images) through
//! -- the subset of the Cloud Controller v3 API, the UAA token endpoints and
//! log-cache's read API it uses -- keeping the desired state of its apps and
//! having its wickets serve them through each wicket's app API, an app's
//! instances spread over them (`placement`, `controller`) and moved off a wicket
//! told to stop or gone, a deployment put a wicket's share at a time (`deployments`); a
//! component comes
//! in a zip or from an OCI registry (`registry`), a rootfs in a zip or made by
//! a wicket of an OCI image, and what every wicket offers of its keyvalue stores
//! and capabilities are its services. With a flume, the apps' output and requests
//! reach their logs from the live access log the wickets feed (`flume`), and its
//! own lines go round it, so every instance on it holds one log (`logs`). Several
//! wickets are reached through a sluice server, each advertising the hosts of the
//! apps it runs. Of several instances on one store, one at a time reconciles the
//! wickets, the holder of a lease (`lease`).
//!
//! ```text
//! cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> wickets
//!                                                  ^                            |
//!                                                  +--Watch-- sluice-flume <--Feed+
//!                                                  +--Feed------^
//! ```

pub mod api;
pub mod blobs;
pub mod brokers;
pub mod config;
pub mod controller;
pub mod deployments;
mod env;
pub mod flume;
pub mod instances;
pub mod lease;
pub mod logs;
pub mod manifest;
mod multipart;
pub mod operations;
pub mod osb;
/// The OpenID Provider client, shared with the wicket's console.
pub use sluice_wicket_oidc as oidc;
pub mod orgs;
pub mod placement;
pub mod postgres;
mod records;
pub mod registry;
pub mod roles;
pub mod s3;
pub mod security_groups;
pub mod services;
pub mod staging;
pub mod stop;
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

/// Bound on a connection to another instance: one that is gone is found so at once.
const PEER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// The owner the apps are put under in the wickets.
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
    /// The wickets, one at least: the first takes a state of one wicket.
    pub wickets: Vec<WicketSettings>,
    /// Their `--console-token`, one for all.
    pub wicket_token: Option<String>,
    /// How long one of several wickets may not answer before it is gone (`crate::controller`):
    /// zero, never.
    pub wicket_away_grace: std::time::Duration,
    /// The shared domains; the first takes the default and random routes.
    pub domains: Vec<String>,
    /// The state, the packages and the droplets.
    pub data_dir: PathBuf,
    /// The store of the state in place of the directory's (`crate::postgres`): several
    /// instances'.
    pub state: Option<postgres::PostgresSettings>,
    /// The bucket of the packages and the droplets in place of the directory (`crate::blobs`):
    /// several instances'.
    pub blobs: Option<blobs::BlobSettings>,
    pub org: String,
    pub space: String,
    /// The builder staging source (`crate::staging`); None: no source is staged.
    pub cnb: Option<staging::Cnb>,
    /// Where the apps' output and requests come from; None: the logs hold
    /// the cf-api's own lines only.
    pub flume: Option<flume::FlumeSettings>,
    /// The OpenID Provider users log in through; None: any credentials pass.
    pub oidc: Option<oidc::OidcSettings>,
    /// The clients of `cf auth --client-credentials`, with the provider.
    pub clients: Vec<(String, String)>,
    /// Who is an admin, and the roles the provider's groups give.
    pub policy: roles::Policy,
    /// Who may edit the security groups.
    pub security_group_editors: security_groups::Editors,
    /// Who may edit the service brokers.
    pub service_broker_editors: security_groups::Editors,
    /// The listener's own TLS; None: plain HTTP.
    pub tls: Option<TlsSettings>,
    /// How long a stop waits for the requests and the puts under way.
    pub stop_grace: std::time::Duration,
    /// PEM CA verifying the other instances (`--instance-ca-cert`); None: the webPKI roots.
    pub instance_ca: Option<Vec<u8>>,
    /// The timing of the lease of the instance reconciling the wickets: no option's.
    pub lease: lease::Timing,
}

/// A wicket the apps are placed on.
#[derive(Clone, Debug, PartialEq)]
pub struct WicketSettings {
    /// What the state places an app's indices on, and `cf app` shows as their host.
    pub name: String,
    /// Its console, `http://host:port`.
    pub url: String,
}

/// The listener's certificate chain and private key, PEM.
#[derive(Clone, Debug)]
pub struct TlsSettings {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// Takes the data directory, made if missing, for this process: an exclusive `flock` of its
/// `.lock`, held while the file returned is open (the kernel lets it go at the process's end, a
/// kill included), refused while another process holds it, before anything there is removed or
/// written; then whether it can be written. Before the listener binds, which a VM's `/init`
/// takes for ready.
pub fn check_data_dir(dir: &std::path::Path) -> Result<std::fs::File, String> {
    let failed = |e: std::io::Error| format!("--data-dir {}: {e}", dir.display());
    std::fs::create_dir_all(dir).map_err(failed)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(".lock"))
        .map_err(failed)?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(format!(
                "--data-dir {}: in use by another sluice-wicket-cf-api",
                dir.display()
            ));
        }
        Err(std::fs::TryLockError::Error(e)) => return Err(failed(e)),
    }
    let probe = dir.join(format!("{}.tmp", store::guid()));
    std::fs::write(&probe, b"")
        .and_then(|()| std::fs::remove_file(&probe))
        .map_err(failed)?;
    Ok(lock)
}

/// The wickets `settings` name.
fn wickets(settings: &Settings) -> Vec<Arc<wicket::Wicket>> {
    let of = settings.wickets.len();
    (settings.wickets.iter())
        .map(|wicket| {
            let token = settings.wicket_token.clone();
            Arc::new(wicket::Wicket::new(
                &wicket.name,
                of,
                &wicket.url,
                token,
                OWNER,
            ))
        })
        .collect()
}

/// What several instances on a store are to have alike (`crate::instances`), by option: a
/// secret or a long one as a digest of how this build shows it, which another build may show
/// otherwise.
fn alike(settings: &Settings) -> std::collections::BTreeMap<String, String> {
    use instances::{BLOBS, FLUME, NONE};
    fn digest(of: &impl std::fmt::Debug) -> String {
        staging::sha256(format!("{of:?}").as_bytes())[..12].to_string()
    }
    fn given<T: std::fmt::Debug>(of: &Option<T>) -> String {
        of.as_ref().map_or(NONE.to_string(), digest)
    }
    let wickets: Vec<String> = (settings.wickets.iter())
        .map(|wicket| format!("{}={}", wicket.name, wicket.url))
        .collect();
    let blobs =
        (settings.blobs.as_ref()).map(|blobs| format!("s3://{}/{}", blobs.bucket, blobs.prefix));
    let oidc = (settings.oidc.as_ref()).map(|oidc| (&oidc.issuer, &oidc.client_id, &oidc.claims));
    let instance_url = settings
        .state
        .as_ref()
        .and_then(|state| state.instance_url.as_ref());
    let alike = [
        ("admin", digest(&settings.policy)),
        (BLOBS, blobs.unwrap_or(NONE.to_string())),
        ("client-credentials", digest(&settings.clients)),
        ("cnb-builder", given(&settings.cnb)),
        (
            FLUME,
            given(&settings.flume.as_ref().map(|flume| &flume.url)),
        ),
        (
            "instance-url",
            instance_url.map_or(NONE, |_| "given").to_string(),
        ),
        ("oidc-issuer", given(&oidc)),
        (
            "security-group-editor",
            digest(&settings.security_group_editors),
        ),
        (
            "service-broker-editor",
            digest(&settings.service_broker_editors),
        ),
        ("wicket", wickets.join(",")),
        (
            "wicket-away-grace",
            format!("{}s", settings.wicket_away_grace.as_secs()),
        ),
        ("wicket-token", given(&settings.wicket_token)),
    ];
    (alike.into_iter())
        .map(|(option, value)| (option.to_string(), value))
        .collect()
}

/// Refuses two `--wicket`s naming one wicket, of those answering now.
pub async fn check_wickets(settings: &Settings) -> Result<(), String> {
    // reqwest brings no crypto provider of its own
    let _ = rustls::crypto::ring::default_provider().install_default();
    controller::distinct(&wickets(settings)).await
}

/// Binds `listen`; `loopback_only` (`--any-credentials`) refuses any other address.
pub async fn bind(listen: &str, loopback_only: bool) -> Result<TcpListener, String> {
    let failed = |e: std::io::Error| format!("--listen {listen}: {e}");
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(listen)
        .await
        .map_err(failed)?
        .collect();
    if loopback_only && resolved.iter().any(|at| !at.ip().is_loopback()) {
        return Err(format!(
            "--listen {listen}: with --any-credentials the API listens on a loopback address only"
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

/// How long a stop waits for its last write of the state past `--stop-grace`.
const LAST_WRITE: std::time::Duration = std::time::Duration::from_secs(1);

/// Serves the API on `listener` until `shutdown` completes, the wickets
/// reconciled all along; then stops ([`stop`]): the listener closed, of several instances the
/// lease let go of, the requests and the puts under way finished within `--stop-grace`, the
/// state written.
pub async fn serve(
    listener: TcpListener,
    settings: Settings,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    // reqwest brings no crypto provider of its own (sluice-wicket-oidc's
    // rustls-no-provider): ring, as tonic's TLS and the listener's
    let _ = rustls::crypto::ring::default_provider().install_default();
    let names: Vec<String> = settings.wickets.iter().map(|w| w.name.clone()).collect();
    // what the wickets answering run: an index on a wicket no --wicket names any more is
    // placed again by it at once
    let runs = controller::stacks_of(&wickets(&settings)).await;
    let store = Arc::new(
        store::Store::open(
            &settings.data_dir,
            &settings.org,
            &settings.space,
            &settings.domains,
            &names,
            &runs,
            settings.state.as_ref(),
            settings.blobs.as_ref(),
        )
        .await?,
    );
    let mode = match &settings.oidc {
        Some(oidc) => uaa::Mode::Provider {
            clients: settings.clients.clone(),
            rules: oidc.claims.clone(),
        },
        None => uaa::Mode::Open,
    };
    // beside the instances on the store, if it may be
    let lease = store.lease(settings.lease, alike(&settings));
    lease.join().await?;
    let uaa = uaa::Uaa::open(&settings.data_dir, mode, store.shared()).await?;
    let provider = match &settings.oidc {
        Some(oidc) => Some(oidc::Provider::new(oidc.clone())?),
        None => None,
    };
    let acceptor = match &settings.tls {
        Some(tls) => Some(tls_acceptor(tls)?),
        None => None,
    };
    // on a flume, the logs of every instance on it
    let (logs, flume) = match settings.flume.clone() {
        Some(flume) => {
            let (logs, tasks) = flume::start(flume, &settings.data_dir, store.clone())?;
            (logs, Some(tasks))
        }
        None => (Arc::new(logs::Logs::default()), None),
    };
    let osb = osb::Client::new()?;
    let controller = Arc::new(controller::Controller::new(
        store.clone(),
        wickets(&settings),
        logs.clone(),
        settings.wicket_away_grace,
        settings.cnb.clone(),
        osb.clone(),
        registry::Registry::new()?,
    ));
    let peers = tls::client_config(settings.instance_ca.as_deref(), "--instance-ca-cert")?;
    let peers = reqwest::Client::builder()
        .connect_timeout(PEER_CONNECT_TIMEOUT)
        // let go of before the other instance closes it: no edit lands on a closing one
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .tls_backend_preconfigured(peers)
        .build()
        .map_err(|e| format!("--instance-ca-cert: {e}"))?;
    let api = Arc::new(api::Api {
        store: store.clone(),
        uaa,
        policy: settings.policy.clone(),
        security_group_editors: settings.security_group_editors.clone(),
        service_broker_editors: settings.service_broker_editors.clone(),
        osb,
        provider,
        tls: acceptor.is_some(),
        logs,
        controller: controller.clone(),
        build: describe_build(),
        peers,
    });
    // what a stop left under way (the stagings, the brokers' operations, the apps' deletes,
    // the deployments and moves) is taken up in this instance's turn reconciling the wickets
    let stop = controller.stop.clone();
    let keeping = tokio::spawn(lease.clone().run());
    let leading = tokio::spawn(controller.lead());
    let sweep = tokio::spawn(store.clone().keep_swept());
    tokio::select! {
        () = api::run(listener, acceptor, api) => {}
        () = shutdown => {}
    }
    // the listener closed
    stop.begin();
    let (connections, requests, puts) = stop.under_way();
    println!(
        "API: stopping: no new connection; {connections} connection(s), {requests} request(s) \
         and {puts} put(s) under way, given {}s",
        settings.stop_grace.as_secs_f32()
    );
    let grace = tokio::time::Instant::now() + settings.stop_grace;
    let stopping = async {
        // of several instances, the next one's turn while the requests here are answered: the
        // edits made in this turn first, one going on sent to the next
        if store.shared().is_some() {
            let _ = tokio::time::timeout(settings.lease.edits_end, stop.edited()).await;
            lease.release().await;
        }
        stop.drained().await;
    };
    if tokio::time::timeout_at(grace, stopping).await.is_err() {
        let (connections, requests, puts) = stop.under_way();
        println!(
            "API: stopped past --stop-grace: {connections} connection(s), {requests} request(s) \
             and {puts} put(s) cut"
        );
    }
    sweep.abort();
    for task in flume.iter().flatten() {
        task.abort();
    }
    // what was noted, what was seen of the wickets: within the grace, or LAST_WRITE of a store
    // slow to answer
    let last = grace.max(tokio::time::Instant::now() + LAST_WRITE);
    if tokio::time::timeout_at(last, store.update(|_| ()))
        .await
        .is_err()
    {
        eprintln!("API: stopped with the state's store not answering: the last notes not written");
    }
    // the one process's turn, and a lease not let go of above
    let _ = tokio::time::timeout_at(last, lease.release()).await;
    keeping.abort();
    leading.abort();
    Ok(())
}

#[cfg(test)]
mod tests {
    use sluice_wicket_server::http::QUIET;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpStream;
    use tokio::time::Instant;

    use super::*;

    /// The data directory is one process's: a second take is refused while the first is held,
    /// taken once it is let go.
    #[test]
    fn a_data_directory_is_taken_by_one() {
        let dir = std::env::temp_dir().join(format!("cf-api-lock-{}", store::guid()));
        let held = check_data_dir(&dir).unwrap();
        assert_eq!(
            check_data_dir(&dir).unwrap_err(),
            format!(
                "--data-dir {}: in use by another sluice-wicket-cf-api",
                dir.display()
            )
        );
        drop(held);
        // a child another test is spawning holds a copy of the lock's file until it execs
        let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while let Err(e) = check_data_dir(&dir) {
            assert!(std::time::Instant::now() < until, "{e}");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn settings(data_dir: PathBuf, tls: Option<TlsSettings>) -> Settings {
        Settings {
            wickets: vec![WicketSettings {
                name: "wicket".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            wicket_token: None,
            wicket_away_grace: std::time::Duration::ZERO,
            domains: vec!["apps.test".into()],
            data_dir,
            state: None,
            blobs: None,
            org: "org".into(),
            space: "space".into(),
            cnb: None,
            flume: None,
            oidc: None,
            clients: Vec::new(),
            policy: roles::Policy::default(),
            security_group_editors: security_groups::Editors::default(),
            service_broker_editors: security_groups::Editors::default(),
            tls,
            stop_grace: std::time::Duration::from_secs(8),
            instance_ca: None,
            lease: lease::Timing::default(),
        }
    }

    /// A client sending nothing loses its connection after the bound, in the
    /// TLS handshake as in the wait for a request's head, and at once to another
    /// connection when all slots are taken: it holds no slot.
    #[tokio::test]
    async fn a_quiet_connection_is_closed() {
        let certified = rcgen::generate_simple_self_signed(["localhost".to_string()]).unwrap();
        let tls = TlsSettings {
            cert_pem: certified.cert.pem().into_bytes(),
            key_pem: certified.signing_key.serialize_pem().into_bytes(),
        };
        let dir = std::env::temp_dir().join(format!("cf-api-quiet-{}", store::guid()));
        for tls in [None, Some(tls)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let dir = dir.join(store::guid());
            // the bound on the paused clock, advanced whenever the runtime idles
            tokio::time::pause();
            tokio::spawn(async move {
                serve(listener, settings(dir, tls), std::future::pending())
                    .await
                    .unwrap()
            });
            let started = Instant::now();
            let mut client = TcpStream::connect(address).await.unwrap();
            assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(started.elapsed() >= QUIET, "{:?}", started.elapsed());
            assert!(started.elapsed() < QUIET * 2, "{:?}", started.elapsed());

            // on the real clock (a second: the first waits out FRESH before it gives way):
            // idling on the OS's connects would advance a paused one past the bound, which
            // would close the connection in place of the eviction
            tokio::time::resume();
            let started = Instant::now();
            let mut quiet = Vec::new();
            for _ in 0..api::MAX_CONNECTIONS {
                quiet.push(TcpStream::connect(address).await.unwrap());
            }
            let _next = TcpStream::connect(address).await.unwrap();
            assert_eq!(quiet[0].read(&mut [0; 1]).await.unwrap(), 0);
            assert!(started.elapsed() < QUIET, "{:?}", started.elapsed());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Keeps the paused clock still until dropped: a blocking task running stops its
    /// auto-advance, which otherwise leaps to the next timer whenever the runtime waits on a
    /// socket, past [`QUIET`] before the sockets' bytes arrive.
    fn hold_clock() -> std::sync::mpsc::Sender<()> {
        let (release, released) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || released.recv());
        release
    }

    /// All slots taken, a request with a token keeps its connection, one without
    /// gives way, each still sending its body.
    #[tokio::test(start_paused = true)]
    async fn only_an_authenticated_request_keeps_its_slot() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let dir = std::env::temp_dir().join(format!("cf-api-held-{}", store::guid()));
        let data_dir = dir.clone();
        tokio::spawn(async move {
            serve(listener, settings(data_dir, None), std::future::pending())
                .await
                .unwrap()
        });
        let _held = hold_clock();
        let grant = "grant_type=password&username=u";
        let mut signing_in = TcpStream::connect(address).await.unwrap();
        let request = format!(
            "POST /uaa/oauth/token HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\
             content-length: {}\r\n\r\n{grant}",
            grant.len()
        );
        signing_in.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        signing_in.read_to_string(&mut answer).await.unwrap();
        let (_, tokens) = answer.split_once("\r\n\r\n").unwrap();
        let tokens: serde_json::Value = serde_json::from_str(tokens).unwrap();
        let token = tokens["access_token"].as_str().unwrap();

        // each in its handler, reading its body: its client told to send it
        let sending = async |head: String| {
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(head.as_bytes()).await.unwrap();
            let mut continued = [0; 25];
            client.read_exact(&mut continued).await.unwrap();
            assert_eq!(&continued, b"HTTP/1.1 100 Continue\r\n\r\n");
            client.write_all(b"ab").await.unwrap();
            client
        };
        let head = "POST /v3/x HTTP/1.1\r\nhost: t\r\nexpect: 100-continue\r\n\
                    content-length: 5\r\n";
        let mut admitted = sending(format!("{head}authorization: bearer {token}\r\n\r\n")).await;
        let mut anyone = sending(format!("{head}\r\n")).await;
        // no longer fresh
        tokio::time::advance(std::time::Duration::from_secs(1)).await;

        let mut quiet = Vec::new();
        for _ in 2..api::MAX_CONNECTIONS {
            quiet.push(TcpStream::connect(address).await.unwrap());
        }
        let _next = TcpStream::connect(address).await.unwrap();
        assert_eq!(anyone.read(&mut [0; 1]).await.unwrap(), 0);

        admitted.write_all(b"cde").await.unwrap();
        let mut status = [0; 8];
        admitted.read_exact(&mut status).await.unwrap();
        assert_eq!(&status, b"HTTP/1.1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A wicket answering its first listing (the start's) at once, the next once `opened` is
    /// true, and one after a revision never: its URL.
    async fn held_wicket(opened: tokio::sync::watch::Receiver<bool>) -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let listed = Arc::new(AtomicUsize::new(0));
        let serving = move |stream, _, slot| {
            let (opened, listed) = (opened.clone(), listed.clone());
            tokio::spawn(sluice_wicket_server::http::serve(stream, slot, move |r| {
                let (mut opened, listed) = (opened.clone(), listed.clone());
                async move {
                    if hyper::Request::uri(&r).query().is_some() {
                        std::future::pending::<()>().await;
                    }
                    if listed.fetch_add(1, Ordering::Relaxed) > 0 {
                        opened.wait_for(|opened| *opened).await.unwrap();
                    }
                    let listing = br#"{"revision": 0, "generation": 0, "apps": []}"#;
                    hyper::Response::new(http_body_util::Full::new(bytes::Bytes::from_static(
                        listing,
                    )))
                }
            }));
        };
        tokio::spawn(sluice_wicket_server::accept::run(
            listener, 16, "wicket", serving,
        ));
        url
    }

    /// The answer next on `client`, its head and body.
    async fn answer(client: &mut TcpStream) -> String {
        let mut read = Vec::new();
        let end = loop {
            if let Some(at) = read.windows(4).position(|window| window == b"\r\n\r\n") {
                break at + 4;
            }
            assert_ne!(client.read_buf(&mut read).await.unwrap(), 0, "cut short");
        };
        let head = String::from_utf8_lossy(&read[..end]).to_lowercase();
        let length: usize = (head.lines())
            .find_map(|line| line.strip_prefix("content-length: "))
            .map_or(0, |length| length.parse().unwrap());
        while read.len() < end + length {
            assert_ne!(client.read_buf(&mut read).await.unwrap(), 0, "cut short");
        }
        String::from_utf8_lossy(&read).into_owned()
    }

    /// `/healthz` says ready once the wickets were listed. A stop closes the listener and an
    /// idle connection at once, answers a request under way whole, closing its connection, and
    /// ends once past `--stop-grace` with one still under way.
    #[tokio::test]
    async fn a_stop_answers_the_requests_under_way() {
        let dir = std::env::temp_dir().join(format!("cf-api-stop-{}", store::guid()));
        let (open, opened) = tokio::sync::watch::channel(false);
        let mut settings = settings(dir.clone(), None);
        settings.wickets[0].url = held_wicket(opened).await;
        settings.stop_grace = std::time::Duration::from_secs(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let served = tokio::spawn(serve(listener, settings, async {
            let _ = stopped.await;
        }));

        let mut idle = TcpStream::connect(address).await.unwrap();
        let mut healthz = async || {
            let probe = "GET /healthz HTTP/1.1\r\nhost: t\r\n\r\n";
            idle.write_all(probe.as_bytes()).await.unwrap();
            answer(&mut idle).await
        };
        let answered = healthz().await;
        assert!(answered.starts_with("HTTP/1.1 503 "), "{answered}");
        assert!(
            answered.ends_with("\r\n\r\nnot ready: not listed yet: the wicket\n"),
            "{answered}"
        );
        open.send_replace(true);
        let started = Instant::now();
        while !healthz().await.ends_with("\r\n\r\nready\n") {
            assert!(started.elapsed() < std::time::Duration::from_secs(5));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // each in its handler, reading its body
        let sending = async || {
            let mut client = TcpStream::connect(address).await.unwrap();
            let head = "POST /v3/x HTTP/1.1\r\nhost: t\r\nexpect: 100-continue\r\n\
                        content-length: 5\r\n\r\n";
            client.write_all(head.as_bytes()).await.unwrap();
            let mut continued = [0; 25];
            client.read_exact(&mut continued).await.unwrap();
            client.write_all(b"ab").await.unwrap();
            client
        };
        let mut under_way = sending().await;
        let _cut = sending().await;
        let started = Instant::now();
        stop.send(()).unwrap();

        assert_eq!(idle.read(&mut [0; 1]).await.unwrap(), 0);
        while TcpStream::connect(address).await.is_ok() {
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
            tokio::task::yield_now().await;
        }
        under_way.write_all(b"cde").await.unwrap();
        let mut answered = String::new();
        under_way.read_to_string(&mut answered).await.unwrap();
        assert!(answered.starts_with("HTTP/1.1 401 "), "{answered}");
        assert!(answered.contains("\r\nconnection: close\r\n"), "{answered}");

        served.await.unwrap().unwrap();
        let stopped = started.elapsed();
        assert!(
            (1000..2000).contains(&stopped.as_millis()),
            "stopped after {stopped:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    /// Two instances on one store, sharing a bucket and a flume as several do: an edit the wickets take at once, made through the one not
    /// reconciling them, is tried and committed by the one that is, in its turn: refused by a
    /// wicket it is a 422 and changes nothing; two of one app through both leave the wicket
    /// and the state alike; and the one reconciling the wickets killed with an edit under way,
    /// the edit is answered once, by the next; stopped with one under way past the lease's
    /// wait, the next takes over and makes it, answered through the one stopping. No write
    /// reaches the wicket but of a turn.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_edit_on_any_instance_is_made_by_the_one_reconciling_the_wickets() {
        use crate::controller::tests::{FakeWicket, until};
        use serde_json::{Value, json};

        let _ = rustls::crypto::ring::default_provider().install_default();
        let (url, _) = store::postgres_server::database().await;
        let (fake, wicket) = FakeWicket::start(std::time::Duration::from_millis(500)).await;
        let dir = std::env::temp_dir().join(format!("cf-api-edits-{}", store::guid()));
        let bucket = store::s3_server::bucket().await;
        let flume = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let logs = flume::FlumeSettings {
            url: format!("grpc://{}", flume.local_addr().unwrap()),
            token: "f1ume".into(),
            ca_pem: None,
            insecure: false,
        };
        let served = sluice_flume::Settings {
            token: logs.token.clone(),
            ring_size: 100,
            ring_bytes: 1 << 20,
            backfill: 1,
            tls: None,
            otlp: None,
        };
        tokio::spawn(sluice_flume::serve(flume, served, std::future::pending()));
        let state = postgres::PostgresSettings {
            url: format!("{url}?sslmode=disable"),
            ca_pem: None,
            instance_url: None,
        };
        // an app started on the wicket, of one instance, its route mapped
        let (guid, route) = {
            let (domains, wickets) = (["apps.test".to_string()], ["wicket".to_string()]);
            let runs = [("wicket".to_string(), wicket::Runtime::Wasm)].into();
            let seed = &dir.join("seed");
            let seed = store::Store::open(
                seed,
                "org",
                "space",
                &domains,
                &wickets,
                &runs,
                Some(&state),
                None,
            );
            let seed = seed.await.unwrap();
            seed.update(|state| {
                let space = state.spaces[0].guid.clone();
                let domain = state.domains[0].guid.clone();
                let app = state.create_app("hello", &space);
                app.process.instances = 1;
                app.started = true;
                app.running = Some(store::Running {
                    sha256: "ab".repeat(32),
                    file: "hello.wasm".into(),
                    memory_mib: 64,
                    tag: Some("t1".into()),
                    ..store::Running::default()
                });
                let guid = app.guid.clone();
                let route = state.create_route("hello", &domain, &space).guid.clone();
                state.map(&route, &guid);
                (guid, route)
            })
            .await
        };
        // each instance on a runtime of its own: killed with it
        struct Running(Option<tokio::runtime::Runtime>);
        impl Running {
            fn kill(&mut self) {
                if let Some(runtime) = self.0.take() {
                    runtime.shutdown_background();
                }
            }
        }
        impl Drop for Running {
            fn drop(&mut self) {
                self.kill();
            }
        }
        let instance = |name: &str| {
            let (dir, wicket) = (dir.join(name), wicket.clone());
            let (mut state, bound) = (state.clone(), tokio::sync::oneshot::channel());
            let (bucket, logs) = (bucket.clone(), logs.clone());
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let served = runtime.spawn(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                let mut settings = settings(dir, None);
                settings.wickets[0].url = wicket;
                state.instance_url = Some(url.clone());
                settings.state = Some(state);
                (settings.blobs, settings.flume) = (Some(bucket), Some(logs));
                settings.lease = store::tests::TIMING;
                bound.0.send(url).unwrap();
                let stopped = async {
                    if stopped.await.is_err() {
                        std::future::pending::<()>().await;
                    }
                };
                serve(listener, settings, stopped).await.unwrap()
            });
            async move { (bound.1.await.unwrap(), Running(Some(runtime)), stop, served) }
        };
        let http = reqwest::Client::new();
        let ready = async |api: &str| {
            let started = Instant::now();
            loop {
                let health = http.get(format!("{api}/healthz")).send().await.unwrap();
                if health.status() == 200 {
                    return;
                }
                let why = health.text().await.unwrap();
                assert!(started.elapsed() < QUIET, "{api} is not ready: {why}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        };
        let (leader, mut leading, _stop, _served) = instance("leader").await;
        ready(&leader).await;
        // the other is ready once the lease names where the edits go
        let (other, _following, stop, stopped) = instance("other").await;
        ready(&other).await;
        let token = (http
            .post(format!("{other}/uaa/oauth/token"))
            .basic_auth("cf", Some("")))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("grant_type=password&username=u&password=p");
        let token: Value = token.send().await.unwrap().json().await.unwrap();
        let token = format!("bearer {}", token["access_token"].as_str().unwrap());
        let call = async |method: http::Method, api: &str, path: &str, body: Option<Value>| {
            let request = (http.request(method, format!("{api}{path}")))
                .header("authorization", &token)
                .header("host", "api.apps.test");
            let request = match body {
                Some(body) => request.json(&body),
                None => request,
            };
            let response = request.send().await.unwrap();
            let status = response.status().as_u16();
            let body = response.bytes().await.unwrap();
            (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
        };
        let instances = async |api: &str| {
            let path = format!("/v3/apps/{guid}/processes/web");
            call(http::Method::GET, api, &path, None).await.1["instances"].clone()
        };
        let served = || {
            let apps = fake.apps.lock().unwrap();
            apps.get(&guid)
                .map(|app| app["sandbox"]["instances"].clone())
        };
        until("the app on the wicket", || served() == Some(json!(1))).await;
        let turn = fake.writes.lock().unwrap()[0].0;
        assert!(turn.is_some());

        // refused by the wicket: a 422 through the other, nothing changed
        let scale = format!("/v3/apps/{guid}/processes/web/actions/scale");
        *fake.refusing.lock().unwrap() = Some("t1".into());
        let (status, refused) = call(
            http::Method::POST,
            &other,
            &scale,
            Some(json!({"instances": 2})),
        )
        .await;
        assert_eq!(status, 422, "{refused}");
        let detail = refused["errors"][0]["detail"].as_str().unwrap();
        assert!(detail.contains("the tag is refused"), "{detail}");
        assert_eq!(
            (instances(&other).await, instances(&leader).await, served()),
            (json!(1), json!(1), Some(json!(1)))
        );
        *fake.refusing.lock().unwrap() = None;

        // two edits of the app at once, one through each: the wicket and the state alike, the
        // links those of the API its client reached
        let (through_other, through_leader) = tokio::join!(
            call(
                http::Method::POST,
                &other,
                &scale,
                Some(json!({"instances": 2}))
            ),
            call(
                http::Method::POST,
                &leader,
                &scale,
                Some(json!({"instances": 3}))
            ),
        );
        assert_eq!((through_other.0, through_leader.0), (202, 202));
        let link = through_other.1["links"]["self"]["href"].as_str().unwrap();
        assert!(
            link.starts_with("http://api.apps.test/v3/processes/"),
            "{link}"
        );
        let scaled = instances(&other).await;
        assert!(scaled == json!(2) || scaled == json!(3), "{scaled}");
        assert_eq!(served(), Some(scaled));
        // every write so far the leader's, of its turn
        {
            let writes = fake.writes.lock().unwrap();
            assert!(
                writes.iter().all(|write| *write == (turn, true)),
                "{writes:?}"
            );
        }

        // the leader killed with an edit under way, its trial at the wicket: answered once
        // by the other, in its own turn
        let destinations = format!("/v3/routes/{route}/destinations");
        let mapped = call(http::Method::GET, &other, &destinations, None).await.1;
        let destination = mapped["destinations"][0]["guid"].as_str().unwrap();
        let unmap = format!("{destinations}/{destination}");
        let before = fake.writes.lock().unwrap().len();
        let unmapped = async { call(http::Method::DELETE, &other, &unmap, None).await.0 };
        let killed = async {
            until("the trial at the wicket", || {
                fake.writes.lock().unwrap().len() > before
            })
            .await;
            leading.kill();
        };
        let (unmapped, ()) = tokio::join!(unmapped, killed);
        assert_eq!(unmapped, 204);
        let mapped = call(http::Method::GET, &other, &destinations, None).await.1;
        assert_eq!(mapped["destinations"], json!([]));
        until("the app off the wicket", || served().is_none()).await;
        assert!(fake.fenced_none(), "{:?}", fake.writes.lock().unwrap());

        // the other, reconciling them now, stopped with an edit under way through it, longer
        // than the lease waits for: the next takes over, makes it and answers it, through the
        // one stopping, which ends once it answered
        let (next, _taking, _stop, _served) = instance("next").await;
        ready(&next).await;
        let map = json!({"destinations": [{"app": {"guid": guid}}]});
        let (status, mapped) = call(http::Method::POST, &other, &destinations, Some(map)).await;
        assert_eq!(status, 200, "{mapped}");
        until("the app on the wicket again", || served().is_some()).await;
        let destination = mapped["destinations"][0]["guid"].as_str().unwrap();
        let unmap = format!("{destinations}/{destination}");
        let (turn, before) = {
            let writes = fake.writes.lock().unwrap();
            (writes.last().unwrap().0, writes.len())
        };
        let unmapped = async { call(http::Method::DELETE, &other, &unmap, None).await.0 };
        let stopping = async {
            until("the trial at the wicket", || {
                fake.writes.lock().unwrap().len() > before
            })
            .await;
            stop.send(()).unwrap();
            Instant::now()
        };
        let (unmapped, began) = tokio::join!(unmapped, stopping);
        assert_eq!(unmapped, 204);
        stopped.await.unwrap();
        let within = std::time::Duration::from_secs(4);
        assert!(
            began.elapsed() < within,
            "stopped after {:?}",
            began.elapsed()
        );
        let mapped = call(http::Method::GET, &next, &destinations, None).await.1;
        assert_eq!(mapped["destinations"], json!([]));
        until("the app off the wicket again", || served().is_none()).await;
        assert!(fake.fenced_none(), "{:?}", fake.writes.lock().unwrap());
        let writes = fake.writes.lock().unwrap();
        assert!(
            writes.iter().all(|(epoch, _)| epoch.is_some()),
            "{writes:?}"
        );
        assert!(writes.iter().any(|(epoch, _)| *epoch > turn), "{writes:?}");
    }
}
