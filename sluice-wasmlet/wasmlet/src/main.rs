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
//!   capabilities, and a process-wide instance limit (`--max-instances`);
//!   response bodies stream via a bounded relay
//!
//! Usage:
//!
//! ```text
//! sluice-wasmlet --server grpc://127.0.0.1:8001 --token SECRET \
//!     --wasm demo.local=./hello.wasm \
//!     --wasm relay.local='./relay.wasm?wasi=cli,inherit-network'
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/sluice.v1.rs"));
}

#[cfg(test)]
mod build_info;
mod tls;
mod tunnel;
mod wasm_host;

use pb::frame::Body;
use pb::{Advertise, Frame, PeerBuild, Upstream};
use tunnel::{Connections, Outbound};
use wasm_host::{Handler, Instances, Locator, Sandbox};

struct Route {
    host: String,
    component: Locator,
    sandbox: Sandbox,
}

struct Config {
    server: String,
    token: String,
    id: String,
    insecure: bool,
    ca_cert: Option<String>,
    routes: Vec<Route>,
    max_connections: usize,
    max_instances: usize,
    stall_timeout: Duration,
}

/// Reconnect backoff: exponential seconds, as `NodeConnection`.
const BACKOFF_INITIAL_SECS: u64 = 1;

const BACKOFF_MAX_SECS: u64 = 30;

/// Bound on one connect attempt, so a black-holed node cannot park the
/// reconnect loop.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default of `--max-connections`: 256 connections x one 1MiB receive window
/// (plus at most two partly used 16KiB segments and 80 segments' overhead
/// each) = about 265MiB of the server's payload buffered at most.
const DEFAULT_MAX_CONNECTIONS: usize = 256;

/// Default of `--tunnel-stall-timeout`, the Java side's
/// `sluice.client.tunnel-stall-timeout`.
const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Default of `--max-instances`: 64 guest instances alive at once, i.e. at
/// most 64 x the largest `memory-mib` of guest linear memory (16GiB at the
/// default 256MiB).
const DEFAULT_MAX_INSTANCES: usize = 64;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// State shared by every node session.
struct Shared {
    token: String,
    id: String,
    insecure: bool,
    ca_pem: Option<Vec<u8>>,
    /// Upstreams advertised on every stream.
    advertised: Vec<Upstream>,
    /// Target (`wasm:<locator>[?<sandbox>]`) -> its handler; the dial address
    /// key. The target carries the sandbox, so routes sharing a component
    /// with different sandboxes stay distinct.
    components: Arc<HashMap<String, Arc<Handler>>>,
    /// Connections open at once, over every node session.
    limit: Arc<tunnel::Limit>,
    /// How long connection frames may wait with none taken before a stream
    /// is given up; zero = never.
    stall_timeout: Duration,
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
        // same default policy as the Java client: random id unless --client-id is given
        id: uuid::Uuid::new_v4().to_string(),
        insecure: false,
        ca_cert: None,
        routes: Vec::new(),
        max_connections: DEFAULT_MAX_CONNECTIONS,
        max_instances: DEFAULT_MAX_INSTANCES,
        stall_timeout: DEFAULT_STALL_TIMEOUT,
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
            "--version" => {
                println!("{}", describe(&peer_build()));
                std::process::exit(0);
            }
            "--server" => config.server = value("server"),
            "--token" => config.token = value("token"),
            "--client-id" => config.id = value("client-id"),
            "--insecure" => config.insecure = true,
            "--ca-cert" => config.ca_cert = Some(value("ca-cert")),
            "--max-connections" => {
                config.max_connections =
                    parse_limit("max-connections", &value("max-connections"), usize::MAX)
                        .unwrap_or_else(|e| panic!("{e}"));
            }
            "--max-instances" => {
                config.max_instances = parse_limit(
                    "max-instances",
                    &value("max-instances"),
                    wasm_host::Instances::MAX,
                )
                .unwrap_or_else(|e| panic!("{e}"));
            }
            "--tunnel-stall-timeout" => {
                config.stall_timeout =
                    parse_seconds("tunnel-stall-timeout", &value("tunnel-stall-timeout"))
                        .unwrap_or_else(|e| panic!("{e}"));
            }
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

/// The value of a `--max-*` limit: a number from 1 to `max`.
fn parse_limit(name: &str, value: &str, max: usize) -> Result<usize, String> {
    match value.parse() {
        Ok(limit) if limit > 0 && limit <= max => Ok(limit),
        _ if max == usize::MAX => Err(format!("--{name} must be a positive number, got '{value}'")),
        _ => Err(format!(
            "--{name} must be a number from 1 to {max}, got '{value}'"
        )),
    }
}

/// The value of a duration flag in whole seconds; `0` turns it off.
fn parse_seconds(name: &str, value: &str) -> Result<Duration, String> {
    value
        .parse()
        .map(Duration::from_secs)
        .map_err(|_| format!("--{name} must be a number of seconds (0 = off), got '{value}'"))
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
        max_connections,
        max_instances,
        stall_timeout,
    } = config;
    println!("wasmlet build: {}", describe(&peer_build()));
    let instances = Instances::new(max_instances);

    // Fetch / prewarm every component (fail fast). The target is the component
    // locator (`wasm:<url>`); the server passes it through verbatim as the dial
    // address and the client resolves it back.
    let mut components: HashMap<String, Arc<Handler>> = HashMap::new();
    let mut advertised = Vec::with_capacity(routes.len());
    for route in &routes {
        let target = route_target(&route.component.target().await?, &route.sandbox);
        let component = match wasm_host::load(&route.component, route.sandbox.caps).await {
            Ok(component) => component,
            Err(e) => return Err(format!("wasm route '{}': {e}", route.host).into()),
        };
        let handler = Handler::new(component, route.sandbox, &instances)
            .map_err(|e| format!("wasm route '{}': {e}", route.host))?;
        components.insert(target.clone(), handler);
        advertised.push(Upstream {
            host: route.host.clone(),
            target_url: target,
            rewrite_host: false,
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
        components: Arc::new(components),
        limit: tunnel::Limit::new(max_connections),
        stall_timeout,
    });
    println!("connection limit {max_connections}");
    println!(
        "tunnel stall timeout {}s (0 = off)",
        stall_timeout.as_secs()
    );
    println!(
        "instance limit {max_instances} ({} h2 streams per connection)",
        instances.streams_per_connection()
    );

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

        // The bidi stream: outbound frames are scheduled by `outbound`,
        // inbound arrive via `inbound`.
        let (outbound, frames) = Outbound::new();
        outbound.control(advertise(self.shared.advertised.clone()));
        let mut request = tonic::Request::new(frames);
        identify(&mut request, &self.shared.token, &self.shared.id)?;
        let inbound = <pb::tunnel_client::TunnelClient<_>>::new(channel.clone())
            .connect(request)
            .await?
            .into_inner();

        let watchdog = outbound.clone();
        let connections = Connections::new(
            self.key.clone(),
            outbound,
            self.shared.components.clone(),
            self.shared.limit.clone(),
        );
        let established = self
            .stream(inbound, &connections, &channel, &watchdog)
            .await;

        // Tear down this attempt's virtual connections: their tunnel side is
        // gone, and the next attempt carries fresh conn ids.
        connections.teardown();

        established
    }

    /// Serves one established stream to the end; `Ok(true)` once the advertise
    /// was acked. The stream is given up once `outbound` stalled for the
    /// stall timeout (a server that stopped reading, keeping the connection
    /// alive): the caller's teardown then drops the queued frames and frees
    /// their slots, and dropping the call resets the stream.
    async fn stream(
        &self,
        mut inbound: tonic::Streaming<Frame>,
        connections: &Connections,
        channel: &tonic::transport::Channel,
        outbound: &Outbound,
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
                stalled = outbound.stalled(self.shared.stall_timeout) => {
                    eprintln!(
                        "[{}] tunnel stream held frames queued with none taken for {}ms: giving it up",
                        self.key,
                        stalled.as_millis()
                    );
                    return Ok(established);
                }
            };
            match frame.body {
                Some(Body::AdvertiseAck(ack)) => {
                    println!(
                        "[{}] advertise acked by {} (rejected ports: {:?})",
                        self.key, ack.node_id, ack.rejected_ports
                    );
                    if !established {
                        established = true;
                        println!(
                            "[{}] server build: {}",
                            self.key,
                            ack.build
                                .as_ref()
                                .map_or_else(|| UNKNOWN_BUILD.to_string(), describe_peer)
                        );
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
                // connection frames; Advertise from the server is unexpected and
                // keep-alive a no-op, both ignored there
                Some(body) => connections.deliver(body).await,
                None => {}
            }
        }
    }
}

/// The Advertise opening every stream: the routes, the receive window that
/// makes the server open windowed connections, and this binary's build.
fn advertise(upstreams: Vec<Upstream>) -> Frame {
    Frame {
        body: Some(Body::Advertise(Advertise {
            upstreams,
            connection_window: tunnel::WINDOW,
            build: Some(peer_build()),
        })),
    }
}

/// How a server that sent no build (it predates the field) is described.
const UNKNOWN_BUILD: &str = "unknown (older build)";

/// Longest value kept from each string of a server's build, in chars.
const MAX_FIELD_CHARS: usize = 64;

/// This binary's build, embedded by `build.rs`; an empty commit means git
/// was unavailable when it was built.
fn peer_build() -> PeerBuild {
    PeerBuild {
        implementation: "sluice-wasmlet".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("SLUICE_WASMLET_COMMIT").into(),
        dirty: env!("SLUICE_WASMLET_DIRTY") == "true",
        build_time: env!("SLUICE_WASMLET_BUILD_TIME").into(),
    }
}

/// One line naming a build, e.g. `sluice-wasmlet 0.0.1-SNAPSHOT
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
    fn limits_take_a_number_from_one_to_their_max() {
        assert_eq!(parse_limit("max-instances", "8", 64), Ok(8));
        assert_eq!(parse_limit("max-instances", "64", 64), Ok(64));
        for bad in ["0", "65", "-1", "x", ""] {
            assert_eq!(
                parse_limit("max-instances", bad, 64),
                Err(format!(
                    "--max-instances must be a number from 1 to 64, got '{bad}'"
                )),
            );
        }
        assert_eq!(
            parse_limit("max-connections", "0", usize::MAX),
            Err("--max-connections must be a positive number, got '0'".to_string()),
        );
    }

    /// Frames of the server's stream, gRPC-framed (5-byte prefix + message).
    fn grpc_frame(frame: &Frame) -> bytes::Bytes {
        use prost::Message as _;
        let message = frame.encode_to_vec();
        let mut out = Vec::with_capacity(5 + message.len());
        out.push(0);
        out.extend_from_slice(&(message.len() as u32).to_be_bytes());
        out.extend_from_slice(&message);
        out.into()
    }

    /// A tunnel server that stops reading: it reads the advertise, acks it,
    /// asks for one connection to `target` carrying an http/1.1 request for
    /// `/stream` (10MiB), and from then on never reads the request stream
    /// nor grants window back, while it keeps the connection and its
    /// response stream open.
    async fn stalling_server(target: String) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("grpc://{}", listener.local_addr().unwrap());
        let served = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(socket).await.unwrap();
            let mut held = Vec::new();
            while let Some(accepted) = connection.accept().await {
                let (request, mut respond) = accepted.unwrap();
                if request.uri().path() != "/sluice.v1.Tunnel/Connect" {
                    continue; // ListNodes: reset by the drop
                }
                let mut body = request.into_body();
                // the advertise: read it, and grant only what it took
                let advertise = body.data().await.unwrap().unwrap();
                body.flow_control()
                    .release_capacity(advertise.len())
                    .unwrap();
                let response = http::Response::builder()
                    .header("content-type", "application/grpc")
                    .body(())
                    .unwrap();
                let mut send = respond.send_response(response, false).unwrap();
                let frames = [
                    Body::AdvertiseAck(pb::AdvertiseAck {
                        node_id: "stalling".into(),
                        ..Default::default()
                    }),
                    Body::Connect(pb::Connect {
                        conn_id: 1,
                        address: target.clone(),
                        // credit past the response: only the stream holds it back
                        connection_window: 64 << 20,
                    }),
                    Body::Data(pb::Data {
                        conn_id: 1,
                        payload: b"GET /stream HTTP/1.1\r\nhost: stall.local\r\n\r\n".to_vec(),
                    }),
                ];
                for body in frames {
                    send.send_data(grpc_frame(&Frame { body: Some(body) }), false)
                        .unwrap();
                }
                held.push((body, send));
            }
            drop(held);
        });
        // drive the connection on: accept() above polls it
        (url, served)
    }

    /// Against a server that stops reading, the node stream is given up at
    /// the stall timeout: the attempt ends and the connection's slot is
    /// freed, though the server keeps the connection alive.
    #[tokio::test]
    async fn a_server_that_stops_reading_has_the_stream_given_up_at_the_stall_timeout() {
        let timeout = Duration::from_secs(2);
        let locator =
            Locator::parse(wasm_host::tests::guest_wasm("hello").to_str().unwrap()).unwrap();
        let target = locator.target().await.unwrap();
        let loaded = wasm_host::load(&locator, wasm_host::Caps::default())
            .await
            .unwrap();
        let handler = Handler::new(loaded, Sandbox::default(), &Instances::new(4)).unwrap();
        let limit = tunnel::Limit::new(4);
        let shared = Arc::new(Shared {
            token: String::new(),
            id: "stall-test".into(),
            insecure: false,
            ca_pem: None,
            advertised: Vec::new(),
            components: Arc::new(HashMap::from([(target.clone(), handler)])),
            limit: limit.clone(),
            stall_timeout: timeout,
        });
        let (url, _server) = stalling_server(target).await;
        let (events, _received) = mpsc::channel(32);
        let task = NodeTask {
            shared,
            session: Arc::new(Session {
                url,
                token: CancellationToken::new(),
            }),
            key: "stalling".into(),
            events,
        };
        let start = tokio::time::Instant::now();
        let attempt = tokio::spawn(async move { task.attempt().await.map_err(|e| e.to_string()) });
        let mut held = false;
        while !attempt.is_finished() && start.elapsed() < timeout * 10 {
            held |= limit.open() == 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(attempt.is_finished(), "the attempt ended");
        let established = attempt.await.unwrap();

        assert!(
            held,
            "precondition: the connection held its slot while stalled"
        );
        assert_eq!(established, Ok(true), "the stream had come up");
        assert!(
            start.elapsed() >= timeout,
            "not before the timeout: {:?}",
            start.elapsed()
        );
        assert_eq!(limit.open(), 0, "the slot is freed");
    }

    #[test]
    fn durations_take_whole_seconds_and_zero_turns_them_off() {
        assert_eq!(
            parse_seconds("tunnel-stall-timeout", "90"),
            Ok(Duration::from_secs(90))
        );
        assert_eq!(
            parse_seconds("tunnel-stall-timeout", "0"),
            Ok(Duration::ZERO)
        );
        for bad in ["-1", "1.5", "60s", ""] {
            assert_eq!(
                parse_seconds("tunnel-stall-timeout", bad),
                Err(format!(
                    "--tunnel-stall-timeout must be a number of seconds (0 = off), got '{bad}'"
                )),
            );
        }
    }

    #[test]
    fn route_target_without_options_keeps_defaults() {
        let (target, sandbox) =
            parse_route_target("target/wasm32-wasip3/release/hello.wasm").unwrap();
        assert_eq!(target, "target/wasm32-wasip3/release/hello.wasm");
        assert_eq!(sandbox, Sandbox::default());
        assert_eq!(
            route_target("wasm:file:///hello.wasm", &sandbox),
            "wasm:file:///hello.wasm"
        );
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
        assert_eq!(
            route_target("wasm:file:///a.wasm", &sandbox),
            "wasm:file:///a.wasm?wasi=cli"
        );
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
        let (_, a) =
            parse_route_target("a.wasm?wasi=tcp,cli,inherit-network&memory-mib=64").unwrap();
        let (_, b) =
            parse_route_target("a.wasm?memory-mib=64&wasi=cli,inherit-network,tcp").unwrap();
        let (_, c) = parse_route_target("a.wasm?budget-ms=10000&wasi=cli").unwrap();
        assert_eq!(
            route_target("wasm:file:///a.wasm", &a),
            "wasm:file:///a.wasm?memory-mib=64&wasi=cli,inherit-network,tcp"
        );
        assert_eq!(
            route_target("wasm:file:///a.wasm", &a),
            route_target("wasm:file:///a.wasm", &b)
        );
        assert_eq!(
            route_target("wasm:file:///a.wasm", &c),
            "wasm:file:///a.wasm?wasi=cli"
        );
    }

    /// Without the window the server opens every connection in legacy mode,
    /// and no other test would notice.
    #[test]
    fn the_advertise_announces_the_connection_window() {
        let Some(Body::Advertise(advertise)) = advertise(Vec::new()).body else {
            panic!("an Advertise");
        };
        assert_eq!(advertise.connection_window, 1024 * 1024);
        assert_eq!(advertise.connection_window, tunnel::WINDOW);
    }

    /// Without the build the server shows this wasmlet as an older build.
    #[test]
    fn the_advertise_carries_the_embedded_build() {
        let Some(Body::Advertise(advertise)) = advertise(Vec::new()).body else {
            panic!("an Advertise");
        };
        let build = advertise.build.expect("a build");
        assert_eq!(build.implementation, "sluice-wasmlet");
        assert_eq!(build.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(build.commit, env!("SLUICE_WASMLET_COMMIT"));
        assert_eq!(build.build_time, env!("SLUICE_WASMLET_BUILD_TIME"));
    }

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

    /// A build without git (missing program, or outside a work tree) still
    /// gets an identification: an empty commit, reported as unknown.
    #[test]
    fn git_state_without_git_is_unknown() {
        let dir = std::env::temp_dir();
        let missing = build_info::git_state("sluice-no-such-git", &dir);
        assert_eq!(
            missing,
            build_info::GitState {
                commit: String::new(),
                dirty: false
            }
        );
        assert!(build_info::git_watch_paths("sluice-no-such-git", &dir).is_empty());
    }

    #[test]
    fn git_state_reads_the_commit_of_the_work_tree() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let Some(head) = build_info::git("git", dir, &["rev-parse", "HEAD"]) else {
            return; // no git on this machine: covered by git_state_without_git_is_unknown
        };
        assert_eq!(build_info::git_state("git", dir).commit, head);
        assert!(
            build_info::git_watch_paths("git", dir)
                .iter()
                .any(|path| path.ends_with("HEAD")),
            "HEAD is watched"
        );
    }

    #[test]
    fn rfc3339_formats_utc_instants() {
        assert_eq!(build_info::rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(build_info::rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(build_info::rfc3339(1_760_000_000), "2025-10-09T08:53:20Z");
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
