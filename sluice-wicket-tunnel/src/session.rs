//! One tunnel stream toward one node (`NodeConnection` of the Java client):
//! connect, advertise the routes, serve the connections, reconnect with
//! backoff.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::drain::Drain;
use crate::proto::frame::Body;
use crate::proto::{Advertise, Frame, PeerBuild, Upstream};
use crate::routes::{Routes, StreamAcks, Table};
use crate::tunnel::{self, Connections, Outbound};
use crate::workers::Workers;
use crate::{BoxError, UNKNOWN_BUILD, describe_peer, proto, tls};

/// Reconnect backoff: the ceiling doubles from 1s to 30s and each wait is
/// drawn from its upper half (equal jitter), as `ReconnectBackoff` of the Java
/// client, so clients that failed together do not retry together.
const BACKOFF_INITIAL_SECS: u64 = 1;

const BACKOFF_MAX_SECS: u64 = 30;

pub struct Backoff {
    ceiling_secs: u64,
}

impl Backoff {
    pub fn new() -> Self {
        Self {
            ceiling_secs: BACKOFF_INITIAL_SECS,
        }
    }

    /// The wait before the next attempt, `unit` in `[0, 1)` picking the point
    /// in the upper half of the ceiling; the ceiling of the one after doubles.
    pub fn next(&mut self, unit: f64) -> Duration {
        let ceiling = Duration::from_secs(self.ceiling_secs);
        self.ceiling_secs = (self.ceiling_secs * 2).min(BACKOFF_MAX_SECS);
        ceiling / 2 + (ceiling / 2).mul_f64(unit)
    }

    /// Back to the initial ceiling, after the tunnel came up.
    pub fn reset(&mut self) {
        self.ceiling_secs = BACKOFF_INITIAL_SECS;
    }
}

/// A uniform random number in `[0, 1)` from the 53 high bits of a random
/// (v4) uuid.
pub fn jitter_unit() -> f64 {
    (uuid::Uuid::new_v4().as_u128() >> 75) as f64 / (1u64 << 53) as f64
}

/// Bound on one connect attempt, so a black-holed node cannot park the
/// reconnect loop.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The HTTP/2 window announced for the gRPC connection carrying the tunnel
/// (stream and connection alike, the tunnel being one stream): twice what the
/// connections may have in flight, `max_connections` x [`tunnel::WINDOW`],
/// capped at the largest HTTP/2 window.
///
/// The connections' windows bound what the server may send; the transport
/// window only has to stay out of their way. h2 grants a window back once
/// half of it was read, so a transfer can settle with half of it in flight:
/// twice the budget keeps the budget's worth in flight even then. It is a
/// ceiling on what the server may send, not a buffer: the receive loop takes
/// frames off the stream as they arrive and hands them to the connections,
/// whose own windows (the budget) bound what they hold.
pub fn transport_window(max_connections: usize) -> u32 {
    let budget = (max_connections as u64).saturating_mul(u64::from(tunnel::WINDOW));
    budget
        .saturating_mul(2)
        .clamp(u64::from(tunnel::WINDOW), u64::from(i32::MAX as u32)) as u32
}

/// Fixes the HTTP/2 stream and connection windows of `endpoint` at `window`
/// and turns the adaptive window off. Left at hyper's defaults (2MiB per
/// stream, 5MiB per connection) the tunnel stream, shared by every virtual
/// connection, would cap the whole tunnel at a fraction of one round trip's
/// worth of the connections' windows.
pub fn with_transport_window(
    endpoint: tonic::transport::Endpoint,
    window: u32,
) -> tonic::transport::Endpoint {
    endpoint
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .http2_adaptive_window(false)
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// What the node sessions of a wicket are set up with.
pub struct Settings {
    /// The bearer token of every request; empty = none.
    pub token: String,
    /// The client id (`x-sluice-id`).
    pub id: String,
    /// `grpcs://` without verifying the server's certificate.
    pub insecure: bool,
    /// The CA a `grpcs://` server's certificate is verified against; None =
    /// the webPKI roots.
    pub ca_pem: Option<Vec<u8>>,
    /// Connections open at once, over every node session.
    pub max_connections: usize,
    /// How long connection frames may wait with none taken before a stream
    /// is given up; zero = never.
    pub stall_timeout: Duration,
    /// This wicket's build, sent in every Advertise.
    pub build: PeerBuild,
    /// The workers a node session is placed on ([`Workers::place`]), its connections
    /// with it; None = the runtime of the moment.
    pub workers: Option<Arc<Workers>>,
}

/// State shared by every node session.
pub struct Shared<T> {
    pub token: String,
    pub id: String,
    pub insecure: bool,
    pub ca_pem: Option<Vec<u8>>,
    /// The routes every stream advertises and its connections are served
    /// by; replaced by a reload.
    pub routes: Arc<Routes<T>>,
    /// Connections open at once, over every node session.
    pub limit: Arc<tunnel::Limit>,
    /// How long connection frames may wait with none taken before a stream
    /// is given up; zero = never.
    pub stall_timeout: Duration,
    /// HTTP/2 window (stream and connection) the gRPC connection announces,
    /// see [`transport_window`].
    pub transport_window: u32,
    /// The shutdown's drain, seen by every session.
    pub drain: Drain,
    /// The session tasks, the detached ones included: the drain waits for
    /// them.
    pub tasks: TaskTracker,
    /// Tunnel streams up (their advertise acked) and not ended yet; the
    /// supervisor asks the bootstrap for the nodes while there is none.
    pub up: AtomicUsize,
    /// The sessions of the membership (spawned, not ended yet), whose streams
    /// tell the readiness ([`Shared::readiness`]).
    sessions: std::sync::Mutex<Vec<Arc<Session>>>,
    /// This wicket's build, sent in every Advertise.
    pub build: PeerBuild,
    /// See [`Settings::workers`].
    pub workers: Option<Arc<Workers>>,
}

impl<T> Shared<T> {
    /// The state of the sessions serving `routes`, before any started.
    pub fn new(settings: Settings, routes: Arc<Routes<T>>) -> Arc<Shared<T>> {
        let Settings {
            token,
            id,
            insecure,
            ca_pem,
            max_connections,
            stall_timeout,
            build,
            workers,
        } = settings;
        Arc::new(Shared {
            token,
            id,
            insecure,
            ca_pem,
            routes,
            limit: tunnel::Limit::new(max_connections),
            stall_timeout,
            transport_window: transport_window(max_connections),
            drain: Drain::default(),
            tasks: TaskTracker::new(),
            up: AtomicUsize::new(0),
            sessions: std::sync::Mutex::default(),
            build,
            workers,
        })
    }
}

impl<T: Table> Shared<T> {
    /// Whether the wicket serves through the server, as a readiness probe asks: not from the drain
    /// on; else once every node of the membership has a stream up that took the current routes'
    /// generation, and every route has an instance ready and is held for no controller
    /// ([`Routes::readiness`]). Err says why not.
    pub fn readiness(&self) -> Result<(), String> {
        if self.drain.begin.is_cancelled() {
            return Err("draining".into());
        }
        let generation = self.routes.current().generation();
        let behind: Vec<String> = {
            let sessions = self.sessions.lock().unwrap();
            let members: Vec<&Arc<Session>> = sessions
                .iter()
                .filter(|session| !session.token.is_cancelled())
                .collect();
            if members.is_empty() {
                return Err("no server node known yet".into());
            }
            members
                .into_iter()
                .filter(|session| {
                    session
                        .took
                        .lock()
                        .unwrap()
                        .is_none_or(|took| took < generation)
                })
                .map(|session| session.label())
                .collect()
        };
        if !behind.is_empty() {
            return Err(format!(
                "no stream up that took the routes of generation {generation} to {}",
                behind.join(", ")
            ));
        }
        self.routes.readiness()
    }
}

/// One tunnel stream toward the node at one url.
pub struct Session {
    /// Endpoint this session dials: the node's public url, or the bootstrap
    /// for a node without one (`grpc(s)://host:port`).
    pub url: String,
    /// The node the membership names at `url`; empty when unknown.
    pub node_id: String,
    /// Cancelled by the supervisor to stop the reconnect loop.
    pub token: CancellationToken,
    /// The newest generation the server took on the session's stream up; None while none is up.
    took: std::sync::Mutex<Option<u64>>,
}

impl Session {
    fn new(url: String, node_id: String) -> Session {
        Session {
            url,
            node_id,
            token: CancellationToken::new(),
            took: std::sync::Mutex::default(),
        }
    }

    /// The node id, the url while it is unknown.
    fn label(&self) -> String {
        if self.node_id.is_empty() {
            self.url.clone()
        } else {
            self.node_id.clone()
        }
    }
}

/// A session task's fixed state across its reconnect attempts.
struct NodeTask<T> {
    shared: Arc<Shared<T>>,
    session: Arc<Session>,
    /// Log label: the node id, the url while it is unknown.
    label: String,
    /// The membership snapshots the session learns (pushed, or fetched via
    /// `ListNodes`), to the supervisor.
    events: mpsc::Sender<Vec<proto::Node>>,
}

/// Counts a stream in [`Shared::up`] while it lives.
struct Up<'a>(&'a AtomicUsize);

impl<'a> Up<'a> {
    fn new(up: &'a AtomicUsize) -> Self {
        up.fetch_add(1, Ordering::AcqRel);
        Self(up)
    }
}

impl Drop for Up<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Splits the `--server` (or `--flume`) value into `(secure, authority)`; a
/// bare host:port and `grpc://` are plaintext, `grpcs://` is TLS.
pub fn resolve_server(server: &str) -> (bool, &str) {
    match server.strip_prefix("grpcs://") {
        Some(authority) => (true, authority),
        None => (false, server.strip_prefix("grpc://").unwrap_or(server)),
    }
}

/// The endpoint of the control plane at `url`, set up as every tunnel
/// stream's: connect timeout, keepalive, the fixed transport window and the
/// TLS of the wicket.
fn endpoint<T>(shared: &Shared<T>, url: &str) -> Result<tonic::transport::Endpoint, BoxError> {
    let (secure, authority) = resolve_server(url);
    let mut endpoint = tonic::transport::Endpoint::from_shared(format!(
        "{}://{authority}",
        if secure { "https" } else { "http" }
    ))?
    .connect_timeout(CONNECT_TIMEOUT)
    .http2_keep_alive_interval(Duration::from_secs(30))
    .keep_alive_timeout(Duration::from_secs(10))
    .keep_alive_while_idle(true);
    endpoint = with_transport_window(endpoint, shared.transport_window);
    if secure {
        endpoint = tls::configure(endpoint, shared.insecure, shared.ca_pem.as_deref())?;
    }
    Ok(endpoint)
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

/// Spawns the reconnect loop of the session to `node_id` (empty when
/// unknown) at `url`; returns its handle.
pub fn spawn_session<T: Table>(
    shared: &Arc<Shared<T>>,
    node_id: String,
    url: String,
    events: mpsc::Sender<Vec<proto::Node>>,
) -> Arc<Session> {
    let session = Arc::new(Session::new(url, node_id));
    let label = session.label();
    shared.sessions.lock().unwrap().push(session.clone());
    let task = NodeTask {
        shared: shared.clone(),
        session: session.clone(),
        label,
        events,
    };
    match &shared.workers {
        Some(workers) => shared.tasks.spawn_on(task.run(), workers.place()),
        None => shared.tasks.spawn(task.run()),
    };
    session
}

impl<T: Table> NodeTask<T> {
    /// Connect / serve / reconnect loop with jittered exponential backoff
    /// (`NodeConnection#runLoop`); exits once the supervisor cancels the token
    /// or the drain began, after the stream's end.
    async fn run(self) {
        self.reconnecting().await;
        let mut sessions = self.shared.sessions.lock().unwrap();
        sessions.retain(|session| !Arc::ptr_eq(session, &self.session));
    }

    /// [`NodeTask::run`]'s loop.
    async fn reconnecting(&self) {
        let mut backoff = Backoff::new();
        loop {
            match self.attempt().await {
                Ok(true) => backoff.reset(),
                Ok(false) => {}
                Err(e) => eprintln!("[{}] tunnel stream ended: {e}", self.label),
            }
            let token = self.session.token.clone();
            let draining = self.shared.drain.begin.clone();
            tokio::select! {
                _ = tokio::time::sleep(backoff.next(jitter_unit())) => {}
                _ = token.cancelled() => return,
                _ = draining.cancelled() => return,
            }
        }
    }

    /// One connect..stream-end attempt; `Ok(true)` when the tunnel came up
    /// (the advertise was acked) before the stream ended.
    async fn attempt(&self) -> Result<bool, BoxError> {
        println!("[{}] connecting to {} ...", self.label, self.session.url);
        let endpoint = endpoint(&self.shared, &self.session.url)?;
        let draining = self.shared.drain.begin.clone();
        let channel = tokio::select! {
            channel = endpoint.connect() => channel?,
            // nothing is served here yet: no stream to drain
            _ = draining.cancelled() => return Ok(false),
        };

        // The bidi stream: outbound frames are scheduled by `outbound`,
        // inbound arrive via `inbound`.
        let (outbound, frames) = Outbound::new();
        // registered before the routes are read: a reload from now on is
        // seen by `table` and waits for this stream's acknowledgement
        let acks = self.shared.routes.stream();
        let mut table = self.shared.routes.subscribe();
        let routes = table.borrow_and_update().clone();
        let offered = routes.offered();
        outbound.control(advertise(offered.clone(), false, &self.shared.build));
        let advertised = Advertised {
            unacked: VecDeque::from([(routes.generation(), offered.clone())]),
            upstreams: offered,
            acks,
            session: self.session.clone(),
        };
        let mut request = tonic::Request::new(frames);
        identify(&mut request, &self.shared.token, &self.shared.id)?;
        let mut client = <proto::tunnel_client::TunnelClient<_>>::new(channel.clone());
        let inbound = tokio::select! {
            response = client.connect(request) => response?.into_inner(),
            // no frame of the server was taken: dropping the call cancels it
            _ = draining.cancelled() => return Ok(false),
        };

        let watchdog = outbound.clone();
        let connections = Connections::new(
            self.label.clone(),
            outbound,
            self.shared.routes.clone(),
            self.shared.limit.clone(),
        );
        let established = self
            .stream(
                inbound,
                &connections,
                &channel,
                &watchdog,
                table,
                advertised,
            )
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
    ///
    /// Once the drain began, the routes are withdrawn (`Advertise.withdraw`)
    /// and the connections keep being served. The server's
    /// `AdvertiseAck.withdrawn` tells that no CONNECT follows: once no
    /// connection holds a slot any more, the request stream is ended, which
    /// the server answers by ending its side. An ack to the withdrawing
    /// advertise without `withdrawn` comes from a server predating the drain,
    /// which keeps routing here: the stream ends at once. The drain's abort
    /// ends it at once too.
    ///
    /// Until the drain, a reload that changed the routes (`table`) is
    /// advertised again, and so is a change of which are ready
    /// ([`Table::ready`]); each acknowledgement tells the routes which
    /// generation the server took on this stream.
    async fn stream(
        &self,
        mut inbound: tonic::Streaming<Frame>,
        connections: &Connections<T>,
        channel: &tonic::transport::Channel,
        outbound: &Outbound,
        mut table: watch::Receiver<Arc<T>>,
        mut advertised: Advertised,
    ) -> Result<bool, BoxError> {
        let mut established = false;
        // counts the stream in `Shared::up` from its acknowledgement on
        let mut _up = None;
        // the advertise opening the stream
        let mut advertises_sent = 1;
        let mut acks_received = 0;
        // the ordinal of the withdrawing advertise among the stream's; 0 = not sent
        let mut withdrawal = 0;
        let mut withdrawn = false;
        let mut ending = false;
        let mut held = connections.held();
        let drain = &self.shared.drain;
        enum Step {
            Frame(Frame),
            Withdraw,
            Drained,
            Reloaded,
        }
        loop {
            let step = tokio::select! {
                frame = inbound.message() => match frame {
                    Ok(Some(frame)) => Step::Frame(frame),
                    Ok(None) => return Ok(established),
                    Err(e) => return Err(e.into()),
                },
                _ = self.session.token.cancelled() => return Ok(established),
                _ = drain.abort.cancelled() => return Ok(established),
                _ = drain.begin.cancelled(), if withdrawal == 0 => Step::Withdraw,
                // the reloader outlives every stream: the table never closes
                Ok(()) = table.changed(), if withdrawal == 0 => Step::Reloaded,
                _ = held.wait_for(|held| *held == 0), if withdrawn && !ending => Step::Drained,
                stalled = outbound.stalled(self.shared.stall_timeout) => {
                    eprintln!(
                        "[{}] tunnel stream held frames queued with none taken for {}ms: giving it up",
                        self.label,
                        stalled.as_millis()
                    );
                    return Ok(established);
                }
            };
            let frame = match step {
                Step::Frame(frame) => frame,
                Step::Reloaded => {
                    let routes = table.borrow_and_update().clone();
                    if advertised.update(&*routes) {
                        advertises_sent += 1;
                        outbound.control(advertise(
                            advertised.upstreams.clone(),
                            false,
                            &self.shared.build,
                        ));
                        let withheld = routes.advertised().len() - advertised.upstreams.len();
                        println!(
                            "[{}] advertising the routes ({} upstream(s), generation {}{})",
                            self.label,
                            advertised.upstreams.len(),
                            routes.generation(),
                            match withheld {
                                0 => String::new(),
                                n => format!("; {n} left out with no instance ready"),
                            }
                        );
                    }
                    continue;
                }
                Step::Withdraw => {
                    advertises_sent += 1;
                    withdrawal = advertises_sent;
                    outbound.control(advertise(
                        advertised.upstreams.clone(),
                        true,
                        &self.shared.build,
                    ));
                    println!(
                        "[{}] withdrawing the routes; {} connection(s) open",
                        self.label,
                        *held.borrow()
                    );
                    continue;
                }
                Step::Drained => {
                    ending = true;
                    println!("[{}] drained; ending the tunnel stream", self.label);
                    // no frame is left to send: the request stream ends
                    outbound.shutdown();
                    continue;
                }
            };
            match frame.body {
                Some(Body::AdvertiseAck(ack)) => {
                    acks_received += 1;
                    if ack.withdrawn {
                        if !withdrawn {
                            withdrawn = true;
                            println!(
                                "[{}] server acknowledged the withdrawal; {} connection(s) left",
                                self.label,
                                *held.borrow()
                            );
                        }
                        continue;
                    }
                    if withdrawal > 0 && acks_received >= withdrawal {
                        eprintln!(
                            "[{}] the server predates the client drain (Advertise.withdraw) and \
                             keeps routing to this client; closing the tunnel stream at once",
                            self.label
                        );
                        return Ok(established);
                    }
                    advertised.acked();
                    println!(
                        "[{}] advertise acked by {} (rejected ports: {:?})",
                        self.label, ack.node_id, ack.rejected_ports
                    );
                    if !established {
                        established = true;
                        _up = Some(Up::new(&self.shared.up));
                        println!(
                            "[{}] server build: {}",
                            self.label,
                            ack.build
                                .as_ref()
                                .map_or_else(|| UNKNOWN_BUILD.to_string(), describe_peer)
                        );
                        let expected = &self.session.node_id;
                        if !expected.is_empty()
                            && !ack.node_id.is_empty()
                            && ack.node_id != *expected
                        {
                            // a public url balanced across nodes: two streams of this
                            // wicket may meet on one node and replace each other
                            // there, leaving the other node without
                            eprintln!(
                                "[{}] the stream to node {expected} at {} reached node {}; \
                                 every node needs a public url of its own",
                                self.label, self.session.url, ack.node_id
                            );
                        }
                        // self-healing: re-fetch the node list on every
                        // established stream (`TunnelClient#onConnected`)
                        let channel = channel.clone();
                        let shared = self.shared.clone();
                        let events = self.events.clone();
                        tokio::spawn(async move {
                            match list_nodes(&channel, &shared).await {
                                Ok(nodes) => {
                                    let _ = events.send(nodes).await;
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
                    println!("[{}] server draining: {}", self.label, drain.reason);
                }
                Some(Body::MembershipUpdate(update)) => {
                    println!(
                        "[{}] membership v{}: {:?}",
                        self.label,
                        update.membership_version,
                        update
                            .nodes
                            .iter()
                            .map(|node| node.node_id.as_str())
                            .collect::<Vec<_>>()
                    );
                    let _ = self.events.send(update.nodes).await;
                }
                // connection frames; Advertise from the server is unexpected and
                // keep-alive a no-op, both ignored there
                Some(body) => connections.dispatch(body),
                None => {}
            }
        }
    }
}

/// The routes a stream advertised, and the generations its advertises carried
/// that the server has not acknowledged yet, with their routes.
struct Advertised {
    upstreams: Vec<Upstream>,
    /// Oldest first: acknowledgements come in the order of the advertises.
    unacked: VecDeque<(u64, Vec<Upstream>)>,
    acks: StreamAcks,
    /// The stream's session, told what the server took ([`Session::took`]) until the stream ends.
    session: Arc<Session>,
}

impl Advertised {
    /// Takes the reloaded `routes`, or their readiness changed; true when the
    /// upstreams offered ([`Table::offered`]) changed and are to be
    /// advertised. Unchanged, the server already has them: the generation
    /// counts as acknowledged with the advertise still pending, or at once.
    fn update(&mut self, routes: &impl Table) -> bool {
        let offered = routes.offered();
        if offered != self.upstreams {
            self.unacked
                .push_back((routes.generation(), offered.clone()));
            self.upstreams = offered;
            return true;
        }
        match self.unacked.back_mut() {
            Some((pending, _)) => *pending = routes.generation(),
            None => self.took(routes.generation(), &self.upstreams),
        }
        false
    }

    /// The server acknowledged the oldest advertise.
    fn acked(&mut self) {
        if let Some((generation, upstreams)) = self.unacked.pop_front() {
            self.took(generation, &upstreams);
        }
    }

    /// The server took the routes of `generation` on the stream, `upstreams` as advertised.
    fn took(&self, generation: u64, upstreams: &[Upstream]) {
        self.acks.acked(generation, upstreams);
        let mut took = self.session.took.lock().unwrap();
        *took = Some(took.map_or(generation, |took| took.max(generation)));
    }
}

impl Drop for Advertised {
    /// The stream ended: none of the session's is up.
    fn drop(&mut self) {
        *self.session.took.lock().unwrap() = None;
    }
}

/// The Advertise opening every stream: the routes, the receive window the
/// server requires (it fails the stream of a peer announcing none), and the
/// wicket's `build`. With `withdraw`, the drain's: the routes are repeated,
/// so a server predating the field registers them unchanged and acknowledges
/// it without `withdrawn`.
fn advertise(upstreams: Vec<Upstream>, withdraw: bool, build: &PeerBuild) -> Frame {
    Frame {
        body: Some(Body::Advertise(Advertise {
            upstreams,
            connection_window: tunnel::WINDOW,
            build: Some(build.clone()),
            withdraw,
        })),
    }
}

/// The node list `bootstrap` answers (`TunnelClient#discover`): one node
/// without id and public url, which stands for the bootstrap itself, when the
/// server has no `ListNodes`; empty when it could not be asked.
pub async fn discover<T>(shared: &Shared<T>, bootstrap: &str) -> Vec<proto::Node> {
    let asked = async {
        let channel = endpoint(shared, bootstrap)?.connect().await?;
        list_nodes(&channel, shared).await
    };
    match asked.await {
        Ok(nodes) => nodes,
        Err(e)
            if e.downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::Unimplemented) =>
        {
            vec![proto::Node::default()]
        }
        Err(e) => {
            eprintln!("asking {bootstrap} for the server nodes failed: {e}");
            Vec::new()
        }
    }
}

/// Unary `ListNodes` on `channel` (`NodeConnection#listNodes`), authenticated
/// as the tunnel streams are.
async fn list_nodes<T>(
    channel: &tonic::transport::Channel,
    shared: &Shared<T>,
) -> Result<Vec<proto::Node>, BoxError> {
    let mut request = tonic::Request::new(proto::ListNodesRequest {});
    identify(&mut request, &shared.token, &shared.id)?;
    request.set_timeout(Duration::from_secs(10));
    let response = <proto::tunnel_client::TunnelClient<_>>::new(channel.clone())
        .list_nodes(request)
        .await?;
    Ok(response.into_inner().nodes)
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::routes::testing::Fixed;

    #[test]
    fn backoff_waits_in_the_upper_half_of_a_doubling_capped_ceiling() {
        let ceilings = [1, 2, 4, 8, 16, 30, 30, 30];
        for unit in [0.0, 0.25, 0.5, 0.999_999] {
            let mut backoff = Backoff::new();
            for ceiling in ceilings {
                let wait = backoff.next(unit);
                let ceiling = Duration::from_secs(ceiling);
                assert!(
                    wait >= ceiling / 2 && wait <= ceiling,
                    "{wait:?} of {ceiling:?}"
                );
            }
        }
        // the extremes are the half and (nearly) the whole ceiling
        assert_eq!(Backoff::new().next(0.0), Duration::from_millis(500));
        let mut backoff = Backoff::new();
        backoff.next(0.0);
        assert_eq!(backoff.next(0.0), Duration::from_secs(1));
    }

    #[test]
    fn backoff_resets_to_the_initial_ceiling() {
        let mut backoff = Backoff::new();
        for _ in 0..6 {
            backoff.next(0.0);
        }
        backoff.reset();
        assert_eq!(backoff.next(0.0), Duration::from_millis(500));
        assert_eq!(backoff.next(0.0), Duration::from_secs(1));
    }

    #[test]
    fn the_jitter_source_varies_within_the_unit_interval() {
        let units: std::collections::HashSet<u64> = (0..100)
            .map(|_| {
                let unit = jitter_unit();
                assert!((0.0..1.0).contains(&unit), "{unit}");
                unit.to_bits()
            })
            .collect();
        assert!(units.len() > 90, "{} distinct of 100", units.len());
    }

    #[test]
    fn clients_with_different_draws_do_not_wait_alike() {
        let waits: std::collections::HashSet<Duration> = (0..20)
            .map(|_| Backoff::new().next(jitter_unit()))
            .collect();
        assert!(waits.len() > 10, "{} distinct of 20", waits.len());
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
                    Body::AdvertiseAck(proto::AdvertiseAck {
                        node_id: "stalling".into(),
                        ..Default::default()
                    }),
                    Body::Connect(proto::Connect {
                        conn_id: 1,
                        address: target.clone(),
                        // credit past the response: only the stream holds it back
                        connection_window: 64 << 20,
                        ..Default::default()
                    }),
                    Body::Data(proto::Data {
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
        let (shared, limit, target) = hello_shared(timeout);
        let (url, _server) = stalling_server(target).await;
        let (events, _received) = mpsc::channel(32);
        let task = NodeTask {
            shared,
            session: Arc::new(Session::new(url, String::new())),
            label: "stalling".into(),
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

    /// A table published with other upstreams is advertised on the live
    /// stream; one with the same upstreams is not. A route with no instance
    /// ready is left out, from the first advertise on, until it is ready.
    #[tokio::test]
    async fn a_published_table_is_advertised_when_its_upstreams_changed() {
        let hosts = |advertise: &Advertise| -> Vec<String> {
            advertise
                .upstreams
                .iter()
                .map(|up| up.host.clone())
                .collect()
        };
        let first = Fixed::hosts(1, &["a.local", "c.local"]);
        first.unready.lock().unwrap().push("c.local".into());
        let routes = Routes::new(first);
        let (shared, _) = shared_on(&routes, Duration::ZERO);
        let (mut stream, _events) = served_session(&shared).await;
        assert_eq!(hosts(&stream.advertise().await), ["a.local"]);
        stream.ack(false);

        routes.publish(Fixed::hosts(2, &["a.local"]));
        routes.publish(Fixed::hosts(3, &["a.local", "b.local"]));
        let readvertised = stream.advertise().await;
        assert_eq!(
            hosts(&readvertised),
            ["a.local", "b.local"],
            "the changed table only"
        );
        assert!(!readvertised.withdraw);
        stream.ack(false);

        let unready = routes.current().unready.clone();
        unready.lock().unwrap().push("b.local".into());
        routes.readiness_changed();
        assert_eq!(hosts(&stream.advertise().await), ["a.local"]);
        stream.ack(false);
        unready.lock().unwrap().clear();
        routes.readiness_changed();
        assert_eq!(hosts(&stream.advertise().await), ["a.local", "b.local"]);
        stream.ack(false);
        // held, as with none ready
        let held = routes.current().held.clone();
        held.lock().unwrap().push("a.local".into());
        routes.readiness_changed();
        assert_eq!(hosts(&stream.advertise().await), ["b.local"]);
        stream.ack(false);
        held.lock().unwrap().clear();
        routes.readiness_changed();
        assert_eq!(hosts(&stream.advertise().await), ["a.local", "b.local"]);
    }

    /// Each acknowledgement tells which generation the server took: the one
    /// of its advertise, or a later one reloaded to the same routes while it
    /// was pending; a reload to the same routes with none pending counts at
    /// once. The routes of an advertise acknowledged are taken.
    #[test]
    fn an_advertise_acknowledges_the_generations_carrying_its_routes() {
        use futures_util::FutureExt as _;
        let routes = Routes::new(Fixed::hosts(1, &[]));
        let acknowledged = |generation| routes.acknowledged(generation).now_or_never().is_some();
        let upstream = |host: &str| Upstream {
            host: host.into(),
            ..Default::default()
        };
        let mut advertised = Advertised {
            upstreams: vec![upstream("a")],
            unacked: VecDeque::from([(1, vec![upstream("a")])]),
            acks: routes.stream(),
            session: Arc::new(Session::new("grpc://node".into(), String::new())),
        };
        assert!(!routes.taken(&upstream("a")), "nothing acknowledged");
        let table = |generation, hosts: &[&str]| Fixed {
            generation,
            advertised: hosts.iter().map(|host| upstream(host)).collect(),
            ..Fixed::hosts(generation, &[])
        };
        assert!(!advertised.update(&table(2, &["a"])), "the same routes");
        assert!(!acknowledged(1));
        advertised.acked();
        assert!(acknowledged(2), "the pending advertise carried 2");
        assert!(routes.taken(&upstream("a")));
        assert!(!advertised.update(&table(3, &["a"])));
        assert!(acknowledged(3), "none pending: at once");
        assert!(advertised.update(&table(4, &["a", "b"])), "new routes");
        assert!(!acknowledged(4));
        assert!(!routes.taken(&upstream("b")), "pending");
        advertised.acked();
        assert!(acknowledged(4));
        assert!(routes.taken(&upstream("b")));
        drop(advertised);
        assert!(acknowledged(99), "no live stream holds anything back");
        assert!(routes.taken(&upstream("c")), "no live stream to take it");
    }

    /// Without the window the server fails the stream (FAILED_PRECONDITION);
    /// without the build it shows the wicket as an older build.
    #[test]
    fn the_advertise_announces_the_connection_window_and_the_build() {
        let build = PeerBuild {
            implementation: "sluice-wicket-test".into(),
            version: "1".into(),
            ..Default::default()
        };
        let Some(Body::Advertise(advertise)) = advertise(Vec::new(), false, &build).body else {
            panic!("an Advertise");
        };
        assert_eq!(advertise.connection_window, 1024 * 1024);
        assert_eq!(advertise.connection_window, tunnel::WINDOW);
        assert_eq!(advertise.build, Some(build));
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
}

/// Test doubles of the server's side of a tunnel stream, and the state of
/// wickets on it.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use std::collections::HashMap;

    use tokio::sync::Semaphore;

    use super::*;

    /// A message of the server's, gRPC-framed (5-byte prefix + message).
    pub fn grpc_frame(message: &impl prost::Message) -> bytes::Bytes {
        let message = message.encode_to_vec();
        let mut out = Vec::with_capacity(5 + message.len());
        out.push(0);
        out.extend_from_slice(&(message.len() as u32).to_be_bytes());
        out.extend_from_slice(&message);
        out.into()
    }

    /// The peer's next gRPC message on `body`, `buf` holding what arrived of
    /// it; `Ok(None)` once the peer ended its stream, an error once it reset
    /// it.
    pub async fn recv_message<M: prost::Message + Default>(
        body: &mut h2::RecvStream,
        buf: &mut Vec<u8>,
    ) -> Result<Option<M>, h2::Error> {
        loop {
            if buf.len() >= 5 {
                let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
                if buf.len() >= 5 + len {
                    let message = M::decode(&buf[5..5 + len]).unwrap();
                    buf.drain(..5 + len);
                    return Ok(Some(message));
                }
            }
            let Some(chunk) = body.data().await else {
                return Ok(None);
            };
            let chunk = chunk?;
            let _ = body.flow_control().release_capacity(chunk.len());
            buf.extend_from_slice(&chunk);
        }
    }

    /// The state of a wicket serving the test routes' `hello.local`, its
    /// connection limit and the route's target.
    #[cfg(test)]
    pub(crate) fn hello_shared(
        stall_timeout: Duration,
    ) -> (
        Arc<Shared<crate::routes::testing::Fixed>>,
        Arc<tunnel::Limit>,
        String,
    ) {
        let routes = Routes::new(crate::routes::testing::Fixed::hosts(1, &["hello.local"]));
        let (shared, limit) = shared_on(&routes, stall_timeout);
        let target = routes.current().advertised[0].target_url.clone();
        (shared, limit, target)
    }

    /// The state of a wicket serving `routes`, and its connection limit (4).
    pub fn shared_on<T>(
        routes: &Arc<Routes<T>>,
        stall_timeout: Duration,
    ) -> (Arc<Shared<T>>, Arc<tunnel::Limit>) {
        let shared = Shared::new(
            Settings {
                token: String::new(),
                id: "test".into(),
                insecure: false,
                ca_pem: None,
                max_connections: 4,
                stall_timeout,
                build: PeerBuild::default(),
                workers: None,
            },
            routes.clone(),
        );
        let limit = shared.limit.clone();
        (shared, limit)
    }

    /// Hang guard of the tests' waits.
    pub const HANG: Duration = Duration::from_secs(10);

    /// One tunnel stream as the server sees it, over raw h2: the wicket's
    /// frames in, the server's out.
    pub struct ServerStream {
        body: h2::RecvStream,
        send: h2::SendStream<bytes::Bytes>,
        buf: Vec<u8>,
    }

    impl ServerStream {
        /// The wicket's next frame; `Ok(None)` once it ended its request
        /// stream, an error once it reset it.
        pub async fn recv(&mut self) -> Result<Option<Frame>, h2::Error> {
            recv_message(&mut self.body, &mut self.buf).await
        }

        /// The next frame `pick` takes, skipping the others (window updates).
        pub async fn next<R>(&mut self, mut pick: impl FnMut(Body) -> Option<R>) -> R {
            loop {
                let frame = tokio::time::timeout(HANG, self.recv())
                    .await
                    .expect("hang guard")
                    .expect("the stream is open")
                    .expect("the stream is open");
                if let Some(picked) = frame.body.and_then(&mut pick) {
                    return picked;
                }
            }
        }

        /// The wicket's next Advertise.
        pub async fn advertise(&mut self) -> Advertise {
            self.next(|body| match body {
                Body::Advertise(advertise) => Some(advertise),
                _ => None,
            })
            .await
        }

        /// Reads to the end of the wicket's request stream, ended or reset;
        /// whether it was ended.
        pub async fn ended(&mut self) -> bool {
            loop {
                match tokio::time::timeout(HANG, self.recv())
                    .await
                    .expect("hang guard")
                {
                    Ok(Some(_)) => {}
                    Ok(None) => return true,
                    Err(_) => return false,
                }
            }
        }

        pub fn send(&mut self, body: Body) {
            self.send
                .send_data(grpc_frame(&Frame { body: Some(body) }), false)
                .unwrap();
        }

        pub fn ack(&mut self, withdrawn: bool) {
            self.send(Body::AdvertiseAck(proto::AdvertiseAck {
                node_id: "node".into(),
                withdrawn,
                ..Default::default()
            }));
        }

        pub fn connect(&mut self, conn_id: i64, target: &str) {
            self.send(Body::Connect(proto::Connect {
                conn_id,
                address: target.into(),
                connection_window: tunnel::WINDOW,
                ..Default::default()
            }));
        }

        /// An http/1.1 request for `path` on `conn_id`; the status line of
        /// its response, read to its end (a chunked body holding
        /// `path=<path>\n`, as the hello guests answer).
        pub async fn exchange(&mut self, conn_id: i64, path: &str) -> String {
            self.send(Body::Data(proto::Data {
                conn_id,
                payload: format!("GET {path} HTTP/1.1\r\nhost: hello.local\r\n\r\n").into_bytes(),
            }));
            let body = format!("path={path}\n");
            let mut response = String::new();
            while !(response.contains(&body) && response.ends_with("0\r\n\r\n")) {
                let payload = self
                    .next(|body| match body {
                        Body::Data(data) if data.conn_id == conn_id => Some(data.payload),
                        _ => None,
                    })
                    .await;
                response.push_str(std::str::from_utf8(&payload).unwrap());
            }
            response.lines().next().unwrap_or_default().to_string()
        }

        /// Sends `bytes` on `conn_id`.
        pub fn data(&mut self, conn_id: i64, bytes: &str) {
            self.send(Body::Data(proto::Data {
                conn_id,
                payload: bytes.as_bytes().to_vec(),
            }));
        }

        /// The chunked response on `conn_id`, read to its end.
        pub async fn response(&mut self, conn_id: i64) -> String {
            let mut response = String::new();
            while !response.ends_with("\r\n0\r\n\r\n") {
                let payload = self
                    .next(|body| match body {
                        Body::Data(data) if data.conn_id == conn_id => Some(data.payload),
                        _ => None,
                    })
                    .await;
                response.push_str(std::str::from_utf8(&payload).unwrap());
            }
            response
        }

        /// Ends the server's side with an OK status.
        pub fn end(&mut self) {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            self.send.send_trailers(trailers).unwrap();
        }
    }

    /// A tunnel server handing each Connect stream to the test; ListNodes is
    /// answered with `nodes`, or UNIMPLEMENTED without (a server predating
    /// it: the supervisor streams to the bootstrap).
    pub async fn tunnel_server(
        nodes: Option<Vec<proto::Node>>,
    ) -> (String, mpsc::Receiver<ServerStream>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("grpc://{}", listener.local_addr().unwrap());
        let (streams, received) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let streams = streams.clone();
                let nodes = nodes.clone();
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    // accept() drives the connection on
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        let response = http::Response::builder()
                            .header("content-type", "application/grpc")
                            .body(())
                            .unwrap();
                        if request.uri().path() != "/sluice.v1.Tunnel/Connect" {
                            let Some(nodes) = nodes.clone() else {
                                let unimplemented = http::Response::builder()
                                    .header("content-type", "application/grpc")
                                    .header("grpc-status", "12")
                                    .body(())
                                    .unwrap();
                                let _ = respond.send_response(unimplemented, true);
                                continue;
                            };
                            let mut send = respond.send_response(response, false).unwrap();
                            let listed = proto::ListNodesResponse {
                                nodes,
                                membership_version: 1,
                            };
                            send.send_data(grpc_frame(&listed), false).unwrap();
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().unwrap());
                            send.send_trailers(trailers).unwrap();
                            continue;
                        }
                        let send = respond.send_response(response, false).unwrap();
                        let stream = ServerStream {
                            body: request.into_body(),
                            send,
                            buf: Vec::new(),
                        };
                        if streams.send(stream).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (url, received)
    }

    /// The dial address the server derives from an advertised target URL
    /// (`Router.addressOf` of sluice-server): a `wasm:` or `firecracker:`
    /// target verbatim, `host[:port]` of any other.
    pub fn dial_address(target_url: &str) -> String {
        if target_url.starts_with("wasm:") || target_url.starts_with("firecracker:") {
            return target_url.to_string();
        }
        let rest = target_url
            .split_once("://")
            .map_or(target_url, |(_, rest)| rest);
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host)
            .to_string()
    }

    /// The server's side of one tunnel stream, as its data plane runs it:
    /// every Advertise is acknowledged (and handed to the test), and each
    /// virtual connection opened ([`ServerTunnel::connect`]) is a byte stream
    /// as the server's public client sees it, both ways under the protocol's
    /// flow control: the wicket's payload is granted back as the test reads
    /// it, and the test's goes out against the wicket's credit. The test's
    /// shutdown of its writes is the CLOSE; the wicket's CLOSE ends the
    /// test's reads; its ERROR drops the stream. The wicket ending its side
    /// of the tunnel stream (drained) ends the server's.
    pub struct ServerTunnel {
        state: Arc<TunnelState>,
        advertises: mpsc::UnboundedReceiver<Advertise>,
        /// The wicket's last Advertise taken.
        latest: Advertise,
    }

    type PumpedConnection = (mpsc::UnboundedSender<Body>, Arc<Semaphore>);

    struct TunnelState {
        send: std::sync::Mutex<h2::SendStream<bytes::Bytes>>,
        /// conn id -> the connection's frames from the wicket, and the
        /// wicket's credit for the test's payload on it.
        connections: std::sync::Mutex<HashMap<i64, PumpedConnection>>,
        /// The receive window the wicket announced (its latest Advertise).
        window: std::sync::atomic::AtomicU32,
    }

    impl TunnelState {
        fn send(&self, body: Body) {
            // a stream the wicket ended takes nothing more: the test sees it
            // through the connections ending
            let _ = self
                .send
                .lock()
                .unwrap()
                .send_data(grpc_frame(&Frame { body: Some(body) }), false);
        }
    }

    impl ServerTunnel {
        /// Takes over `stream` once its first Advertise arrived (returned,
        /// acknowledged).
        pub async fn new(mut stream: ServerStream) -> (ServerTunnel, Advertise) {
            let advertise = stream.advertise().await;
            stream.ack(false);
            let ServerStream {
                mut body,
                send,
                mut buf,
            } = stream;
            let state = Arc::new(TunnelState {
                send: std::sync::Mutex::new(send),
                connections: Default::default(),
                window: advertise.connection_window.into(),
            });
            let (advertised, advertises) = mpsc::unbounded_channel();
            let demux = state.clone();
            tokio::spawn(async move {
                use prost::Message as _;
                loop {
                    while buf.len() >= 5 {
                        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
                        if buf.len() < 5 + len {
                            break;
                        }
                        let frame = Frame::decode(&buf[5..5 + len]).unwrap();
                        buf.drain(..5 + len);
                        demux.dispatch(frame, &advertised);
                    }
                    let chunk = match body.data().await {
                        Some(Ok(chunk)) => chunk,
                        // the wicket ended its side (drained): so does the server
                        None => {
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().unwrap());
                            let _ = demux.send.lock().unwrap().send_trailers(trailers);
                            break;
                        }
                        Some(Err(_)) => break,
                    };
                    let _ = body.flow_control().release_capacity(chunk.len());
                    buf.extend_from_slice(&chunk);
                }
                // the stream ended: so do its connections
                demux.connections.lock().unwrap().clear();
            });
            let latest = advertise.clone();
            let tunnel = ServerTunnel {
                state,
                advertises,
                latest,
            };
            (tunnel, advertise)
        }

        /// The wicket's next Advertise after the first, acknowledged.
        pub async fn advertise(&mut self) -> Advertise {
            let advertise = tokio::time::timeout(HANG, self.advertises.recv())
                .await
                .expect("hang guard")
                .expect("the stream is open");
            self.latest = advertise.clone();
            advertise
        }

        /// The wicket's Advertise once it is as `what` takes it: the last one come, else the
        /// next until one is. What an edit is waited for by: the routes a stream advertises
        /// move with their readiness too (a route left out while its instances restart), and
        /// those advertises are passed over.
        pub async fn advertise_where(&mut self, what: impl Fn(&Advertise) -> bool) -> Advertise {
            while let Ok(advertise) = self.advertises.try_recv() {
                self.latest = advertise;
            }
            while !what(&self.latest) {
                self.advertise().await;
            }
            self.latest.clone()
        }

        /// Opens connection `conn_id` to the dial address `address`, as the
        /// server does for a client of the route.
        pub fn connect(&self, conn_id: i64, address: &str) -> tokio::io::DuplexStream {
            self.connect_from(conn_id, address, "")
        }

        /// Opens connection `conn_id` as [`ServerTunnel::connect`] does, for a
        /// client of `client_address` (the CONNECT's).
        pub fn connect_from(
            &self,
            conn_id: i64,
            address: &str,
            client_address: &str,
        ) -> tokio::io::DuplexStream {
            let (client, pumped) = tokio::io::duplex(64 * 1024);
            let credit = Arc::new(Semaphore::new(
                self.state.window.load(std::sync::atomic::Ordering::Acquire) as usize,
            ));
            let (frames, received) = mpsc::unbounded_channel();
            self.state
                .connections
                .lock()
                .unwrap()
                .insert(conn_id, (frames, credit.clone()));
            self.state.send(Body::Connect(proto::Connect {
                conn_id,
                address: address.into(),
                connection_window: tunnel::WINDOW,
                client_address: client_address.into(),
                ..Default::default()
            }));
            tokio::spawn(pump(conn_id, pumped, credit, received, self.state.clone()));
            client
        }
    }

    impl TunnelState {
        /// One frame of the wicket's, to its connection.
        fn dispatch(&self, frame: Frame, advertised: &mpsc::UnboundedSender<Advertise>) {
            let connection = |conn_id| self.connections.lock().unwrap().get(&conn_id).cloned();
            match frame.body {
                Some(Body::Advertise(advertise)) => {
                    self.window.store(
                        advertise.connection_window,
                        std::sync::atomic::Ordering::Release,
                    );
                    self.send(Body::AdvertiseAck(proto::AdvertiseAck {
                        node_id: "node".into(),
                        withdrawn: advertise.withdraw,
                        ..Default::default()
                    }));
                    let _ = advertised.send(advertise);
                }
                Some(Body::WindowUpdate(update)) if update.increment > 0 => {
                    if let Some((_, credit)) = connection(update.conn_id) {
                        credit.add_permits(update.increment as usize);
                    }
                }
                Some(body @ (Body::Data(_) | Body::Close(_) | Body::Error(_))) => {
                    let conn_id = match &body {
                        Body::Data(data) => data.conn_id,
                        Body::Close(close) => close.conn_id,
                        Body::Error(error) => error.conn_id,
                        _ => unreachable!(),
                    };
                    if let Some((frames, _)) = connection(conn_id) {
                        let _ = frames.send(body);
                    }
                }
                _ => {}
            }
        }
    }

    /// Relays one connection between the test's byte stream (`pumped`'s
    /// other end) and the tunnel stream, until both directions ended.
    async fn pump(
        conn_id: i64,
        pumped: tokio::io::DuplexStream,
        credit: Arc<Semaphore>,
        mut frames: mpsc::UnboundedReceiver<Body>,
        state: Arc<TunnelState>,
    ) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let (mut reader, mut writer) = tokio::io::split(pumped);
        let up = async {
            let mut buffer = vec![0u8; 16 * 1024];
            loop {
                let n = reader.read(&mut buffer).await?;
                if n == 0 {
                    state.send(Body::Close(proto::Close { conn_id }));
                    return Ok(());
                }
                credit
                    .acquire_many(n as u32)
                    .await
                    .map_err(std::io::Error::other)?
                    .forget();
                state.send(Body::Data(proto::Data {
                    conn_id,
                    payload: buffer[..n].to_vec(),
                }));
            }
        };
        let down = async {
            while let Some(body) = frames.recv().await {
                match body {
                    Body::Data(data) => {
                        writer.write_all(&data.payload).await?;
                        state.send(Body::WindowUpdate(proto::WindowUpdate {
                            conn_id,
                            increment: data.payload.len() as u32,
                            ..Default::default()
                        }));
                    }
                    Body::Close(_) => return writer.shutdown().await,
                    _ => break,
                }
            }
            Err(std::io::Error::other("the connection failed"))
        };
        let failed = tokio::try_join!(up, down).is_err();
        state.connections.lock().unwrap().remove(&conn_id);
        if failed {
            // the test's end sees the stream dropped; the wicket, an ERROR
            state.send(Body::Error(proto::Error {
                conn_id,
                message: "connection closed".into(),
            }));
        }
    }

    /// A session of `shared` on a [`tunnel_server`], its stream as the server
    /// sees it, and the session's event receiver (kept: the stream reports
    /// to it).
    pub async fn served_session<T: Table>(
        shared: &Arc<Shared<T>>,
    ) -> (ServerStream, mpsc::Receiver<Vec<proto::Node>>) {
        let (url, mut streams) = tunnel_server(None).await;
        let (events, received) = mpsc::channel(32);
        spawn_session(shared, String::new(), url, events);
        let stream = tokio::time::timeout(HANG, streams.recv())
            .await
            .expect("hang guard")
            .expect("a stream");
        (stream, received)
    }
}
