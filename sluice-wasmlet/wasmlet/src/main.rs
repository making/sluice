//! sluice-wasmlet: a wasm platform on the sluice tunnel.
//!
//! Connectes to a sluice server with the same `sluice.v1.Tunnel` gRPC API as
//! the Java client, but instead of relaying to TCP upstreams it terminates
//! HTTP on the tunnel and handles every request with a wasi:http 0.3
//! component (`wasi:http/handler@0.3.0`). One process, N routes.
//!
//! The supervisor (`supervise`) ports the Java client's fan-out semantics
//! (`TunnelClient` / `NodeConnection`): the `--server` endpoint is only the
//! bootstrap; the node list learned via `ListNodes` / `MembershipUpdate` keeps
//! one tunnel stream per node, a dropped stream reconnects with exponential
//! backoff (1s..30s), and the `AdvertiseAck` renames the session after its
//! node. Exactly one stream per node is kept: the server closes the older
//! registration when a client id re-registers.
//!
//! Scope (deliberately minimal):
//!
//! - `grpc://` / `grpcs://` control plane (`--ca-cert` pins the CA,
//!   `--insecure` skips verification); no mTLS yet
//! - one pre-linked (`InstancePre`) instance per request, under a per-route
//!   epoch CPU budget, memory cap and `wasmtime serve`-style WASI
//!   capabilities; response bodies stream via a bounded relay
//!
//! Usage:
//!
//! ```text
//! sluice-wasmlet --server grpc://127.0.0.1:8001 --token SECRET \
//!     --wasm demo.local=./hello.wasm \
//!     --wasm relay.local='./relay.wasm?wasi=cli,inherit-network'
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/sluice.v1.rs"));
}

mod tls;
mod wasm_host;

use pb::frame::Body;
use pb::{Advertise, Error, Frame, Upstream};
use wasm_host::{Loaded, Locator, Sandbox};

struct Route {
    host: String,
    component: Locator,
    sandbox: Sandbox,
}

/// A resolved route target: the shared `Loaded` plus the sandbox its target
/// names.
struct ComponentEntry {
    component: Arc<Loaded>,
    sandbox: Sandbox,
}

struct Config {
    server: String,
    token: String,
    id: String,
    insecure: bool,
    ca_cert: Option<String>,
    routes: Vec<Route>,
}

/// Reconnect backoff: exponential seconds, as `NodeConnection`.
const BACKOFF_INITIAL_SECS: u64 = 1;

const BACKOFF_MAX_SECS: u64 = 30;

/// Bound on one connect attempt, so a black-holed node cannot park the
/// reconnect loop.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Downstream control for one virtual connection, consumed by its serve task.
enum Ctrl {
    /// Bytes to write into the connection (hyper's read side).
    Payload(Vec<u8>),
    /// The server half-closed: no more tunnel -> hyper data.
    WriteClose,
    /// The connection failed on the tunnel side: tear it down.
    Abort,
}

/// conn id -> sender into that connection's serve task; one per stream
/// attempt, since conn ids are the server's.
type Registry = Arc<Mutex<HashMap<i64, mpsc::Sender<Ctrl>>>>;

/// Frames received before the serve task starts are absorbed by this buffer.
const SERVE_BUFFER: usize = 32;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// State shared by every node session.
struct Shared {
    token: String,
    id: String,
    insecure: bool,
    ca_pem: Option<Vec<u8>>,
    /// Upstreams advertised on every stream.
    advertised: Vec<Upstream>,
    /// Target (`wasm:<locator>[?<sandbox>]`) -> component entry; the dial
    /// address key. The target carries the sandbox, so routes sharing a
    /// component with different sandboxes stay distinct.
    components: HashMap<String, ComponentEntry>,
}

/// One tunnel stream toward one node. The supervisor's `Arc` identity survives
/// renames, so a session keeps its tasks across a rekey.
struct Session {
    /// Endpoint this session dials, as configured / advertised (`grpc(s)://host:port`).
    url: String,
    /// Cancelled by the supervisor to stop the reconnect loop.
    token: CancellationToken,
}

/// Supervisor events, one channel shared by every session.
enum Event {
    /// The stream is up and the advertise was acked by `node_id`.
    Connected {
        session: Arc<Session>,
        node_id: String,
    },
    /// A membership snapshot (pushed, or fetched via `ListNodes`).
    Membership { nodes: Vec<pb::Node> },
}

/// A session task's fixed state across its reconnect attempts.
struct NodeTask {
    shared: Arc<Shared>,
    session: Arc<Session>,
    /// Map key this session started under; the supervisor renames it on ack.
    key: String,
    events: mpsc::Sender<Event>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run(parse_args()).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn parse_args() -> Config {
    let mut config = Config {
        server: String::new(),
        token: String::new(),
        id: "sluice-wasmlet".into(),
        insecure: false,
        ca_cert: None,
        routes: Vec::new(),
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| -> String {
            match args.next() {
                Some(v) => v,
                None => panic!("missing value for {name}"),
            }
        };
        match arg.as_str() {
            "--server" => config.server = value("server"),
            "--token" => config.token = value("token"),
            "--id" => config.id = value("id"),
            "--insecure" => config.insecure = true,
            "--ca-cert" => config.ca_cert = Some(value("ca-cert")),
            "--wasm" => {
                let spec = value("wasm");
                let (host, target) = spec.split_once('=').unwrap_or_else(|| {
                    panic!("--wasm must be host=component-locator, got '{spec}'")
                });
                let (target, sandbox) =
                    parse_route_target(target).unwrap_or_else(|e| panic!("--wasm {spec}: {e}"));
                let component =
                    Locator::parse(&target).unwrap_or_else(|e| panic!("--wasm {spec}: {e}"));
                config.routes.push(Route {
                    host: host.to_string(),
                    component,
                    sandbox,
                });
            }
            other => panic!("unknown argument '{other}'"),
        }
    }
    if config.server.is_empty() || config.routes.is_empty() {
        panic!("--server and at least one --wasm host=locator are required");
    }
    config
}

/// Splits `locator[?budget-ms=N&memory-mib=N&wasi=cap,...]` into the locator
/// and the per-route sandbox. The `?` separator means literal `?` in file
/// paths needs URL-escaping (`file://...%3F...`).
fn parse_route_target(spec: &str) -> Result<(String, Sandbox), String> {
    match spec.split_once('?') {
        Some((target, query)) => Ok((target.to_string(), Sandbox::parse(query)?)),
        None => Ok((spec.to_string(), Sandbox::default())),
    }
}

/// The advertised target of a route: the component locator plus its
/// canonical sandbox query (omitted when all defaults).
fn route_target(locator: &str, sandbox: &Sandbox) -> String {
    match sandbox.query() {
        query if query.is_empty() => locator.to_string(),
        query => format!("{locator}?{query}"),
    }
}

/// Splits the `--server` value into `(secure, authority)`; a bare host:port
/// and `grpc://` are plaintext, `grpcs://` is TLS.
fn resolve_server(server: &str) -> (bool, &str) {
    match server.strip_prefix("grpcs://") {
        Some(authority) => (true, authority),
        None => (false, server.strip_prefix("grpc://").unwrap_or(server)),
    }
}

/// Attaches the client identity to a request: the `authorization` bearer token
/// (when set) and `x-sluice-id`, the metadata of `NodeConnection#runSession`.
fn identify<T>(request: &mut tonic::Request<T>, token: &str, id: &str) -> Result<(), BoxError> {
    if !token.is_empty() {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }
    request.metadata_mut().insert("x-sluice-id", id.parse()?);
    Ok(())
}

async fn run(config: Config) -> Result<(), BoxError> {
    let Config {
        server,
        token,
        id,
        insecure,
        ca_cert,
        routes,
    } = config;

    // Fetch / prewarm every component (fail fast). The target is the component
    // locator (`wasm:<url>`); the server passes it through verbatim as the dial
    // address and the client resolves it back.
    let mut components: HashMap<String, ComponentEntry> = HashMap::new();
    let mut advertised = Vec::with_capacity(routes.len());
    for route in &routes {
        let target = route_target(&route.component.target().await?, &route.sandbox);
        let component = match wasm_host::load(&route.component, route.sandbox.caps).await {
            Ok(component) => component,
            Err(e) => return Err(format!("wasm route '{}': {e}", route.host).into()),
        };
        components.insert(
            target.clone(),
            ComponentEntry {
                component,
                sandbox: route.sandbox,
            },
        );
        advertised.push(Upstream {
            host: route.host.clone(),
            target_url: target,
            preserve_host: true,
            ..Default::default()
        });
    }
    let ca_pem = match ca_cert {
        Some(path) => Some(std::fs::read(&path).map_err(|e| format!("--ca-cert {path}: {e}"))?),
        None => None,
    };
    let shared = Arc::new(Shared {
        token,
        id,
        insecure,
        ca_pem,
        advertised,
        components,
    });

    let (tx, rx) = mpsc::channel::<Event>(32);
    supervise(rx, tx, shared, server).await
}

/// The fan-out supervisor: one session per known node, keyed by node id (the
/// bootstrap session keeps its endpoint as key until its first ack names the
/// node). All membership transitions run on this single task.
async fn supervise(
    mut events: mpsc::Receiver<Event>,
    spawn_events: mpsc::Sender<Event>,
    shared: Arc<Shared>,
    bootstrap: String,
) -> Result<(), BoxError> {
    let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
    let session = spawn_session(
        &shared,
        bootstrap.clone(),
        bootstrap.clone(),
        spawn_events.clone(),
    );
    sessions.insert(bootstrap, session);
    while let Some(event) = events.recv().await {
        match event {
            Event::Connected { session, node_id } => rekey(&mut sessions, session, node_id),
            Event::Membership { nodes } => reconcile(&mut sessions, nodes, &shared, &spawn_events),
        }
    }
    Ok(())
}

/// Renames `session` after the node id learned from its ack
/// (`TunnelClient#rekey`). A different session already mapped to the node is a
/// duplicate stream: the server keeps only the newest registration
/// (`SessionRegistry#register` closes the old one), so it is cancelled here.
fn rekey(sessions: &mut HashMap<String, Arc<Session>>, session: Arc<Session>, node_id: String) {
    if node_id.is_empty() {
        return;
    }
    let current = sessions
        .iter()
        .find(|(_, known)| Arc::ptr_eq(known, &session))
        .map(|(key, _)| key.clone());
    if current.as_deref() == Some(node_id.as_str()) {
        return; // already known under this name
    }
    if let Some(known) = sessions.get(&node_id) {
        if !Arc::ptr_eq(known, &session) {
            println!("duplicate stream for node {node_id}; closing the older one");
            known.token.cancel();
        }
    }
    if let Some(key) = current {
        sessions.remove(&key);
    }
    sessions.insert(node_id, session);
}

/// Adopts the membership (`TunnelClient#reconcile`): opens a stream per
/// unseen node -- adopting the session already dialing that endpoint, a second
/// stream to one node would make the server drop the first registration --
/// and drops the map entries of nodes that left; their tasks keep retrying
/// and re-enter via the ack rename.
fn reconcile(
    sessions: &mut HashMap<String, Arc<Session>>,
    nodes: Vec<pb::Node>,
    shared: &Arc<Shared>,
    events: &mpsc::Sender<Event>,
) {
    if nodes.is_empty() {
        return;
    }
    for node in &nodes {
        if node.node_id.is_empty() || sessions.contains_key(&node.node_id) {
            continue;
        }
        if node.public_url.is_empty() {
            continue; // no reachable address; the bootstrap stream covers it once acked
        }
        let same_endpoint = sessions
            .iter()
            .find(|(_, session)| session.url == node.public_url)
            .map(|(key, _)| key.clone());
        match same_endpoint {
            Some(key) => {
                let session = sessions.remove(&key).expect("key found in the same map");
                println!(
                    "node {} is the endpoint of the {key} session; adopting it",
                    node.node_id
                );
                rekey(sessions, session, node.node_id.clone());
            }
            None => {
                println!(
                    "opening tunnel stream to node {} at {}",
                    node.node_id, node.public_url
                );
                let session = spawn_session(
                    shared,
                    node.node_id.clone(),
                    node.public_url.clone(),
                    events.clone(),
                );
                sessions.insert(node.node_id.clone(), session);
            }
        }
    }
    let live: HashSet<String> = nodes.into_iter().map(|node| node.node_id).collect();
    sessions.retain(|key, _| live.contains(key));
}

/// Spawns the reconnect loop of one session; returns its handle.
fn spawn_session(
    shared: &Arc<Shared>,
    key: String,
    url: String,
    events: mpsc::Sender<Event>,
) -> Arc<Session> {
    let session = Arc::new(Session {
        url,
        token: CancellationToken::new(),
    });
    let task = NodeTask {
        shared: shared.clone(),
        session: session.clone(),
        key,
        events,
    };
    tokio::spawn(task.run());
    session
}

impl NodeTask {
    /// Connect / serve / reconnect loop with exponential backoff
    /// (`NodeConnection#runLoop`); exits once the supervisor cancels the token.
    async fn run(self) {
        let mut backoff = BACKOFF_INITIAL_SECS;
        loop {
            match self.attempt().await {
                Ok(true) => backoff = BACKOFF_INITIAL_SECS,
                Ok(false) => {}
                Err(e) => eprintln!("[{}] tunnel stream ended: {e}", self.key),
            }
            let token = self.session.token.clone();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(backoff)) => {}
                _ = token.cancelled() => return,
            }
            backoff = (backoff * 2).min(BACKOFF_MAX_SECS);
        }
    }

    /// One connect..stream-end attempt; `Ok(true)` when the tunnel came up
    /// (the advertise was acked) before the stream ended.
    async fn attempt(&self) -> Result<bool, BoxError> {
        println!("[{}] connecting to {} ...", self.key, self.session.url);
        let (secure, authority) = resolve_server(&self.session.url);
        let mut endpoint = tonic::transport::Endpoint::from_shared(format!(
            "{}://{authority}",
            if secure { "https" } else { "http" }
        ))?
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
        if secure {
            endpoint = tls::configure(
                endpoint,
                self.shared.insecure,
                self.shared.ca_pem.as_deref(),
            )?;
        }
        let channel = endpoint.connect().await?;

        // The bidi stream: outbound frames flow through `tx`, inbound via `inbound`.
        let (tx, rx) = mpsc::channel::<Frame>(64);
        let mut request = tonic::Request::new(ReceiverStream::new(rx));
        identify(&mut request, &self.shared.token, &self.shared.id)?;
        let inbound = <pb::tunnel_client::TunnelClient<_>>::new(channel.clone())
            .connect(request)
            .await?
            .into_inner();

        tx.send(Frame {
            body: Some(Body::Advertise(Advertise {
                upstreams: self.shared.advertised.clone(),
            })),
        })
        .await?;

        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let established = self.stream(inbound, &tx, &registry, &channel).await;

        // Tear down this attempt's virtual connections: their tunnel side is
        // gone, and the next attempt carries fresh conn ids.
        let pending: Vec<_> = registry.lock().unwrap().drain().collect();
        for (_, sender) in pending {
            let _ = sender.send(Ctrl::Abort).await;
        }

        established
    }

    /// Serves one established stream to the end; `Ok(true)` once the advertise
    /// was acked.
    async fn stream(
        &self,
        mut inbound: tonic::Streaming<Frame>,
        tx: &mpsc::Sender<Frame>,
        registry: &Registry,
        channel: &tonic::transport::Channel,
    ) -> Result<bool, BoxError> {
        let mut established = false;
        loop {
            let frame = tokio::select! {
                frame = inbound.message() => match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) => return Ok(established),
                    Err(e) => return Err(e.into()),
                },
                _ = self.session.token.cancelled() => return Ok(established),
            };
            match frame.body {
                Some(Body::AdvertiseAck(ack)) => {
                    println!(
                        "[{}] advertise acked by {} (rejected ports: {:?})",
                        self.key, ack.node_id, ack.rejected_ports
                    );
                    if !established {
                        established = true;
                        if !ack.node_id.is_empty() {
                            // the supervisor renames this session after the node
                            let _ = self
                                .events
                                .send(Event::Connected {
                                    session: self.session.clone(),
                                    node_id: ack.node_id.clone(),
                                })
                                .await;
                        }
                        // self-healing: re-fetch the node list on every
                        // established stream (`TunnelClient#onConnected`)
                        let channel = channel.clone();
                        let shared = self.shared.clone();
                        let events = self.events.clone();
                        tokio::spawn(async move {
                            match list_nodes(&channel, &shared).await {
                                Ok(nodes) => {
                                    let _ = events.send(Event::Membership { nodes }).await;
                                }
                                Err(e) => eprintln!("ListNodes failed: {e}"),
                            }
                        });
                    }
                }
                Some(Body::Connect(connect)) => {
                    let Some(entry) = self.shared.components.get(&connect.address) else {
                        let _ = tx
                            .send(Frame {
                                body: Some(Body::Error(Error {
                                    conn_id: connect.conn_id,
                                    message: format!("no wasm route for '{}'", connect.address),
                                })),
                            })
                            .await;
                        continue;
                    };
                    let (ctrl_tx, ctrl_rx) = mpsc::channel(SERVE_BUFFER);
                    registry.lock().unwrap().insert(connect.conn_id, ctrl_tx);
                    let component = entry.component.clone();
                    let sandbox = entry.sandbox;
                    tokio::spawn(wasm_host::serve(
                        tx.clone(),
                        registry.clone(),
                        connect.conn_id,
                        component,
                        sandbox,
                        ctrl_rx,
                    ));
                }
                Some(Body::Data(data)) => {
                    let sender = registry.lock().unwrap().get(&data.conn_id).cloned();
                    if let Some(sender) = sender {
                        let _ = sender.send(Ctrl::Payload(data.payload)).await;
                    }
                }
                Some(Body::Close(close)) => {
                    let sender = registry.lock().unwrap().get(&close.conn_id).cloned();
                    if let Some(sender) = sender {
                        let _ = sender.send(Ctrl::WriteClose).await;
                    }
                }
                Some(Body::Error(error)) => {
                    println!(
                        "[{}] conn {} failed: {}",
                        self.key, error.conn_id, error.message
                    );
                    let sender = registry.lock().unwrap().get(&error.conn_id).cloned();
                    if let Some(sender) = sender {
                        let _ = sender.send(Ctrl::Abort).await;
                    }
                }
                Some(Body::Drain(drain)) => {
                    // keep the stream: the server drains in-flight connections
                    // before it closes the stream itself; the failover is the
                    // membership's job (`NodeConnection` case DRAIN)
                    println!("[{}] server draining: {}", self.key, drain.reason);
                }
                Some(Body::MembershipUpdate(update)) => {
                    println!(
                        "[{}] membership v{}: {:?}",
                        self.key,
                        update.membership_version,
                        update
                            .nodes
                            .iter()
                            .map(|node| node.node_id.as_str())
                            .collect::<Vec<_>>()
                    );
                    let _ = self
                        .events
                        .send(Event::Membership {
                            nodes: update.nodes,
                        })
                        .await;
                }
                // Advertise from the server is unexpected; keep-alive is a no-op.
                _ => {}
            }
        }
    }
}

/// Unary `ListNodes` over an established stream's channel
/// (`NodeConnection#listNodes`).
async fn list_nodes(
    channel: &tonic::transport::Channel,
    shared: &Shared,
) -> Result<Vec<pb::Node>, BoxError> {
    let mut request = tonic::Request::new(pb::ListNodesRequest {});
    identify(&mut request, &shared.token, &shared.id)?;
    request.set_timeout(Duration::from_secs(10));
    let response = <pb::tunnel_client::TunnelClient<_>>::new(channel.clone())
        .list_nodes(request)
        .await?;
    Ok(response.into_inner().nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_target_without_options_keeps_defaults() {
        let (target, sandbox) =
            parse_route_target("target/wasm32-wasip3/release/hello.wasm").unwrap();
        assert_eq!(target, "target/wasm32-wasip3/release/hello.wasm");
        assert_eq!(sandbox, Sandbox::default());
        assert_eq!(route_target("wasm:file:///hello.wasm", &sandbox), "wasm:file:///hello.wasm");
    }

    #[test]
    fn route_target_options_override_limits() {
        let (target, sandbox) = parse_route_target("./a.wasm?budget-ms=250&memory-mib=64").unwrap();
        assert_eq!(target, "./a.wasm");
        assert_eq!(sandbox.limits.budget, Duration::from_millis(250));
        assert_eq!(sandbox.limits.memory, 64 << 20);
        assert_eq!(sandbox.caps, wasm_host::Caps::default());
    }

    #[test]
    fn route_target_grants_wasi_capabilities() {
        let (_, sandbox) = parse_route_target("./a.wasm?wasi=inherit-network,cli").unwrap();
        assert_eq!(
            sandbox.caps,
            wasm_host::Caps {
                cli: true,
                inherit_network: true,
                ..Default::default()
            }
        );
    }

    #[test]
    fn route_target_rejects_unknown_capabilities_and_options() {
        let error = parse_route_target("./a.wasm?wasi=cli,fs").unwrap_err();
        assert!(error.contains("unknown wasi capability 'fs'"), "{error}");
        let error = parse_route_target("./a.wasm?fuel=1").unwrap_err();
        assert!(error.contains("unknown option 'fuel'"), "{error}");
    }

    /// `http` is accepted for flag parity with `-S http` but changes nothing:
    /// outgoing http is always linked, as under `wasmtime serve`, so it does
    /// not split the advertised target either.
    #[test]
    fn route_target_accepts_http_as_always_granted() {
        let (_, sandbox) = parse_route_target("./a.wasm?wasi=http").unwrap();
        assert_eq!(sandbox, Sandbox::default());
        let (_, sandbox) = parse_route_target("./a.wasm?wasi=http,cli").unwrap();
        assert_eq!(route_target("wasm:file:///a.wasm", &sandbox), "wasm:file:///a.wasm?wasi=cli");
    }

    #[test]
    fn network_capabilities_require_cli() {
        let error = parse_route_target("./a.wasm?wasi=inherit-network").unwrap_err();
        assert!(error.contains("need 'cli'"), "{error}");
    }

    /// The advertised target carries the canonical sandbox query, so two
    /// routes on one component with different sandboxes get distinct dial
    /// addresses, and equal sandboxes share one regardless of spelling.
    #[test]
    fn route_target_canonicalizes_the_sandbox_query() {
        let (_, a) = parse_route_target("a.wasm?wasi=tcp,cli,inherit-network&memory-mib=64").unwrap();
        let (_, b) = parse_route_target("a.wasm?memory-mib=64&wasi=cli,inherit-network,tcp").unwrap();
        let (_, c) = parse_route_target("a.wasm?budget-ms=10000&wasi=cli").unwrap();
        assert_eq!(
            route_target("wasm:file:///a.wasm", &a),
            "wasm:file:///a.wasm?memory-mib=64&wasi=cli,inherit-network,tcp"
        );
        assert_eq!(route_target("wasm:file:///a.wasm", &a), route_target("wasm:file:///a.wasm", &b));
        assert_eq!(route_target("wasm:file:///a.wasm", &c), "wasm:file:///a.wasm?wasi=cli");
    }

    fn session(url: &str) -> Arc<Session> {
        Arc::new(Session {
            url: url.into(),
            token: CancellationToken::new(),
        })
    }

    #[test]
    fn resolves_plain_grpc_https_and_bare_authority() {
        assert_eq!(
            resolve_server("grpc://127.0.0.1:8001"),
            (false, "127.0.0.1:8001")
        );
        assert_eq!(
            resolve_server("grpcs://tunnel.example.com:8443"),
            (true, "tunnel.example.com:8443")
        );
        assert_eq!(resolve_server("127.0.0.1:8001"), (false, "127.0.0.1:8001"));
    }

    #[test]
    fn rekey_renames_the_session_and_cancels_the_duplicate() {
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let bootstrap = session("grpc://127.0.0.1:8101");
        let other = session("grpcs://10.0.0.1:8201");
        sessions.insert("grpc://127.0.0.1:8101".into(), bootstrap.clone());
        sessions.insert("node-2".into(), other.clone());

        // the bootstrap ack names its node: the endpoint key becomes the node id
        rekey(&mut sessions, bootstrap.clone(), "node-1".into());
        assert_eq!(sessions.len(), 2);
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &bootstrap));
        assert!(sessions.contains_key("node-2"));
        assert!(!sessions.contains_key("grpc://127.0.0.1:8101"));

        // a second session acking as node-1 is a duplicate stream: it takes
        // over and the previous one is cancelled
        let duplicate = session("node-1-alt.example.com:8101");
        rekey(&mut sessions, duplicate.clone(), "node-1".into());
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &duplicate));
        assert!(bootstrap.token.is_cancelled());
        assert!(!duplicate.token.is_cancelled());
    }

    #[test]
    fn rekey_registers_the_node_even_without_a_map_entry() {
        // a prune may have dropped the entry before the ack arrived; the ack
        // re-inserts the session (TunnelClient#rekey puts unconditionally)
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let session = session("grpc://127.0.0.1:8101");
        rekey(&mut sessions, session.clone(), "node-1".into());
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &session));
        assert!(!session.token.is_cancelled());
    }

    #[test]
    fn rekey_ignores_empty_and_known_node_ids() {
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let session = session("grpc://127.0.0.1:8101");
        sessions.insert("node-1".into(), session.clone());
        rekey(&mut sessions, session.clone(), String::new());
        rekey(&mut sessions, session.clone(), "node-1".into());
        assert!(Arc::ptr_eq(sessions.get("node-1").unwrap(), &session));
        assert!(!session.token.is_cancelled());
    }
}
