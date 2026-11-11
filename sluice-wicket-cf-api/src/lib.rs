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
//! reach their logs from the live access log the wickets feed (`flume`). Several
//! wickets are reached through a sluice server, each advertising the hosts of the
//! apps it runs.
//!
//! ```text
//! cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> wickets
//!                                                  ^                            |
//!                                                  +--Watch-- sluice-flume <--Feed+
//! ```

pub mod api;
pub mod brokers;
pub mod config;
pub mod controller;
pub mod deployments;
mod env;
pub mod flume;
pub mod logs;
pub mod manifest;
mod multipart;
pub mod operations;
pub mod osb;
/// The OpenID Provider client, shared with the wicket's console.
pub use sluice_wicket_oidc as oidc;
pub mod orgs;
pub mod placement;
pub mod registry;
pub mod roles;
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

/// Serves the API on `listener` until `shutdown` completes, the wickets
/// reconciled all along; then stops ([`stop`]): the listener closed, the requests and the puts
/// under way finished within `--stop-grace`, the state written.
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
    let store = Arc::new(store::Store::open(
        &settings.data_dir,
        &settings.org,
        &settings.space,
        &settings.domains,
        &names,
        &runs,
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
    let controller = Arc::new(controller::Controller::new(
        store.clone(),
        wickets(&settings),
        logs.clone(),
        settings.wicket_away_grace,
        settings.cnb.clone(),
    ));
    let flume = settings
        .flume
        .clone()
        .map(|flume| tokio::spawn(flume::watch(flume, store.clone(), logs.clone())));
    let osb = osb::Client::new()?;
    let operations = Arc::new(operations::Operations::new(
        store.clone(),
        osb.clone(),
        logs.clone(),
    ));
    // what a stop left under way
    operations.kick();
    let api = Arc::new(api::Api {
        store: store.clone(),
        uaa,
        policy: settings.policy.clone(),
        security_group_editors: settings.security_group_editors.clone(),
        service_broker_editors: settings.service_broker_editors.clone(),
        osb,
        operations,
        provider,
        tls: acceptor.is_some(),
        logs,
        controller: controller.clone(),
        registry: registry::Registry::new()?,
        build: describe_build(),
    });
    // the deployments a stop left under way
    controller.resume();
    let stop = controller.stop.clone();
    let watch = tokio::spawn(controller.watch());
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
    if tokio::time::timeout(settings.stop_grace, stop.drained())
        .await
        .is_err()
    {
        let (connections, requests, puts) = stop.under_way();
        println!(
            "API: stopped past --stop-grace: {connections} connection(s), {requests} request(s) \
             and {puts} put(s) cut"
        );
    }
    watch.abort();
    if let Some(flume) = flume {
        flume.abort();
    }
    // what was noted, the flume watch's cursor as it got
    store.update(|_| ());
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
        drop(check_data_dir(&dir).unwrap());
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
}
