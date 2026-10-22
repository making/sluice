//! One tunnel stream toward one node (`NodeConnection` of the Java client):
//! connect, advertise the routes, serve the connections, reconnect with
//! backoff.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::drain::Drain;
use crate::proto::frame::Body;
use crate::proto::{Advertise, Frame, Upstream};
use crate::routes::{self, Routes};
use crate::supervisor::Event;
use crate::tunnel::{self, Connections, Outbound};
use crate::{BoxError, UNKNOWN_BUILD, describe_peer, proto, tls};

/// Reconnect backoff: the ceiling doubles from 1s to 30s and each wait is
/// drawn from its upper half (equal jitter), as `ReconnectBackoff` of the Java
/// client, so clients that failed together do not retry together.
const BACKOFF_INITIAL_SECS: u64 = 1;

const BACKOFF_MAX_SECS: u64 = 30;

struct Backoff {
    ceiling_secs: u64,
}

impl Backoff {
    fn new() -> Self {
        Self {
            ceiling_secs: BACKOFF_INITIAL_SECS,
        }
    }

    /// The wait before the next attempt, `unit` in `[0, 1)` picking the point
    /// in the upper half of the ceiling; the ceiling of the one after doubles.
    fn next(&mut self, unit: f64) -> Duration {
        let ceiling = Duration::from_secs(self.ceiling_secs);
        self.ceiling_secs = (self.ceiling_secs * 2).min(BACKOFF_MAX_SECS);
        ceiling / 2 + (ceiling / 2).mul_f64(unit)
    }

    /// Back to the initial ceiling, after the tunnel came up.
    fn reset(&mut self) {
        self.ceiling_secs = BACKOFF_INITIAL_SECS;
    }
}

/// A uniform random number in `[0, 1)` from the 53 high bits of a random
/// (v4) uuid.
fn jitter_unit() -> f64 {
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

/// State shared by every node session.
pub struct Shared {
    pub token: String,
    pub id: String,
    pub insecure: bool,
    pub ca_pem: Option<Vec<u8>>,
    /// The routes every stream advertises and its connections are served
    /// by; replaced by a reload.
    pub routes: Arc<Routes>,
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
}

/// One tunnel stream toward one node. The supervisor's `Arc` identity survives
/// renames, so a session keeps its tasks across a rekey.
pub struct Session {
    /// Endpoint this session dials, as configured / advertised (`grpc(s)://host:port`).
    pub url: String,
    /// Cancelled by the supervisor to stop the reconnect loop.
    pub token: CancellationToken,
}

/// A session task's fixed state across its reconnect attempts.
struct NodeTask {
    shared: Arc<Shared>,
    session: Arc<Session>,
    /// Map key this session started under; the supervisor renames it on ack.
    key: String,
    events: mpsc::Sender<Event>,
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

/// Spawns the reconnect loop of one session; returns its handle.
pub fn spawn_session(
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
    shared.tasks.spawn(task.run());
    session
}

impl NodeTask {
    /// Connect / serve / reconnect loop with jittered exponential backoff
    /// (`NodeConnection#runLoop`); exits once the supervisor cancels the token
    /// or the drain began, after the stream's end.
    async fn run(self) {
        let mut backoff = Backoff::new();
        loop {
            match self.attempt().await {
                Ok(true) => backoff.reset(),
                Ok(false) => {}
                Err(e) => eprintln!("[{}] tunnel stream ended: {e}", self.key),
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
        endpoint = with_transport_window(endpoint, self.shared.transport_window);
        if secure {
            endpoint = tls::configure(
                endpoint,
                self.shared.insecure,
                self.shared.ca_pem.as_deref(),
            )?;
        }
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
        outbound.control(advertise(routes.advertised.clone(), false));
        let advertised = Advertised {
            upstreams: routes.advertised.clone(),
            unacked: VecDeque::from([routes.generation]),
            acks,
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
            self.key.clone(),
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
    /// advertised again; each acknowledgement tells the routes which
    /// generation the server took on this stream.
    async fn stream(
        &self,
        mut inbound: tonic::Streaming<Frame>,
        connections: &Connections,
        channel: &tonic::transport::Channel,
        outbound: &Outbound,
        mut table: watch::Receiver<Arc<routes::Table>>,
        mut advertised: Advertised,
    ) -> Result<bool, BoxError> {
        let mut established = false;
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
                        self.key,
                        stalled.as_millis()
                    );
                    return Ok(established);
                }
            };
            let frame = match step {
                Step::Frame(frame) => frame,
                Step::Reloaded => {
                    let routes = table.borrow_and_update().clone();
                    if advertised.update(&routes) {
                        advertises_sent += 1;
                        outbound.control(advertise(advertised.upstreams.clone(), false));
                        println!(
                            "[{}] advertising the reloaded routes ({} upstream(s), generation {})",
                            self.key,
                            advertised.upstreams.len(),
                            routes.generation
                        );
                    }
                    continue;
                }
                Step::Withdraw => {
                    advertises_sent += 1;
                    withdrawal = advertises_sent;
                    outbound.control(advertise(advertised.upstreams.clone(), true));
                    println!(
                        "[{}] withdrawing the routes; {} connection(s) open",
                        self.key,
                        *held.borrow()
                    );
                    continue;
                }
                Step::Drained => {
                    ending = true;
                    println!("[{}] drained; ending the tunnel stream", self.key);
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
                                self.key,
                                *held.borrow()
                            );
                        }
                        continue;
                    }
                    if withdrawal > 0 && acks_received >= withdrawal {
                        eprintln!(
                            "[{}] the server predates the client drain (Advertise.withdraw) and \
                             keeps routing to this client; closing the tunnel stream at once",
                            self.key
                        );
                        return Ok(established);
                    }
                    advertised.acked();
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
                Some(body) => connections.dispatch(body),
                None => {}
            }
        }
    }
}

/// The routes a stream advertised, and the generations its advertises carried
/// that the server has not acknowledged yet.
struct Advertised {
    upstreams: Vec<Upstream>,
    /// Oldest first: acknowledgements come in the order of the advertises.
    unacked: VecDeque<u64>,
    acks: routes::StreamAcks,
}

impl Advertised {
    /// Takes the reloaded `routes`; true when their upstreams changed and
    /// are to be advertised. Unchanged, the server already has them: the
    /// generation counts as acknowledged with the advertise still pending,
    /// or at once.
    fn update(&mut self, routes: &routes::Table) -> bool {
        if routes.advertised != self.upstreams {
            self.upstreams = routes.advertised.clone();
            self.unacked.push_back(routes.generation);
            return true;
        }
        match self.unacked.back_mut() {
            Some(pending) => *pending = routes.generation,
            None => self.acks.acked(routes.generation),
        }
        false
    }

    /// The server acknowledged the oldest advertise.
    fn acked(&mut self) {
        if let Some(generation) = self.unacked.pop_front() {
            self.acks.acked(generation);
        }
    }
}

/// The Advertise opening every stream: the routes, the receive window the
/// server requires (it fails the stream of a peer announcing none), and this
/// binary's build. With `withdraw`, the drain's: the routes are repeated, so
/// a server predating the field registers them unchanged and acknowledges it
/// without `withdrawn`.
fn advertise(upstreams: Vec<Upstream>, withdraw: bool) -> Frame {
    Frame {
        body: Some(Body::Advertise(Advertise {
            upstreams,
            connection_window: tunnel::WINDOW,
            build: Some(crate::build()),
            withdraw,
        })),
    }
}

/// Unary `ListNodes` over an established stream's channel
/// (`NodeConnection#listNodes`).
async fn list_nodes(
    channel: &tonic::transport::Channel,
    shared: &Shared,
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
    use std::collections::HashMap;

    use super::testing::*;
    use super::*;
    use crate::config;
    use crate::routes::Reloader;
    use crate::wasm_host::Instances;

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
        let (shared, limit, target) = hello_shared(timeout).await;
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

    /// A reload is advertised on the live stream. A component changed on
    /// disk serves the next request of a connection already open, after the
    /// request in flight finished on the old one; a removed target keeps
    /// being served -- a CONNECT the server routed before it took the new
    /// routes included -- until the stream acknowledged them, then its
    /// connections close and a CONNECT to it fails.
    #[tokio::test]
    async fn a_reload_is_advertised_and_a_removed_target_serves_until_acknowledged() {
        let dir = routes::tests::Scratch::new("advertise");
        dir.guest("a.wasm", "hello");
        dir.guest("b.wasm", "hello");
        let path = dir.write(
            "wicket.yaml",
            "routes:\n  - {host: a.local, component: a.wasm}\n  - {host: b.local, component: b.wasm}\n",
        );
        let reloader =
            Reloader::start(config::RouteSource::file(&path).unwrap(), Instances::new(8))
                .await
                .unwrap();
        let (shared, limit) = shared_on(reloader.routes(), Duration::ZERO);
        let (a, b) = {
            let table = shared.routes.current();
            (table.target("a.local"), table.target("b.local"))
        };
        let (hangups, hangups_received) = mpsc::channel(1);
        let stop = CancellationToken::new();
        tokio::spawn(reloader.run(Duration::ZERO, hangups_received, stop.clone()));
        let (mut stream, _events) = served_session(&shared).await;
        assert_eq!(stream.advertise().await.upstreams.len(), 2);
        stream.ack(false);
        stream.connect(1, &a);
        stream.connect(2, &b);
        assert_eq!(stream.exchange(2, "/b").await, "HTTP/1.1 200 OK");
        // in flight across the reload: the body is half sent
        stream.data(
            1,
            "POST / HTTP/1.1\r\nhost: a.local\r\ncontent-length: 4\r\n\r\nab",
        );

        dir.guest("a.wasm", "fetch");
        dir.write(
            "wicket.yaml",
            "routes:\n  - {host: a.local, component: a.wasm}\n",
        );
        hangups.send(()).await.unwrap();
        let readvertised = stream.advertise().await;
        let hosts: Vec<_> = readvertised.upstreams.iter().map(|up| &up.host).collect();
        assert_eq!(hosts, ["a.local"]);
        assert!(!readvertised.withdraw);
        assert_eq!(
            readvertised.upstreams[0].target_url, a,
            "a keeps its target"
        );

        stream.data(1, "cd");
        let in_flight = stream.response(1).await;
        assert!(in_flight.contains("body_bytes=4"), "the old a: {in_flight}");
        stream.data(1, "GET / HTTP/1.1\r\nhost: a.local\r\n\r\n");
        let next = stream.response(1).await;
        assert!(next.starts_with("HTTP/1.1 400"), "the new a: {next}");
        // routed by the server before it took the new routes
        stream.connect(3, &b);
        assert_eq!(stream.exchange(3, "/late").await, "HTTP/1.1 200 OK");
        assert_eq!(stream.exchange(2, "/again").await, "HTTP/1.1 200 OK");

        stream.ack(false);
        let mut closed = Vec::new();
        while closed.len() < 2 {
            closed.push(
                stream
                    .next(|body| match body {
                        Body::Close(close) => Some(close.conn_id),
                        _ => None,
                    })
                    .await,
            );
        }
        closed.sort();
        assert_eq!(closed, [2, 3], "b's connections close, a's stays");
        stream.connect(4, &b);
        let error = stream
            .next(|body| match body {
                Body::Error(error) if error.conn_id == 4 => Some(error.message),
                _ => None,
            })
            .await;
        assert!(error.contains("no wasm route"), "{error}");
        assert_eq!(limit.open(), 1, "a's connection only");
        stop.cancel();
    }

    /// Each acknowledgement tells which generation the server took: the one
    /// of its advertise, or a later one reloaded to the same routes while it
    /// was pending; a reload to the same routes with none pending counts at
    /// once.
    #[test]
    fn an_advertise_acknowledges_the_generations_carrying_its_routes() {
        use futures_util::FutureExt as _;
        let routes = Routes::fixed(HashMap::new());
        let acknowledged = |generation| routes.acknowledged(generation).now_or_never().is_some();
        let upstream = |host: &str| Upstream {
            host: host.into(),
            ..Default::default()
        };
        let mut advertised = Advertised {
            upstreams: vec![upstream("a")],
            unacked: VecDeque::from([1]),
            acks: routes.stream(),
        };
        let table = |generation, hosts: &[&str]| {
            routes::Table::advertising(
                generation,
                hosts.iter().map(|host| upstream(host)).collect(),
            )
        };
        assert!(!advertised.update(&table(2, &["a"])), "the same routes");
        assert!(!acknowledged(1));
        advertised.acked();
        assert!(acknowledged(2), "the pending advertise carried 2");
        assert!(!advertised.update(&table(3, &["a"])));
        assert!(acknowledged(3), "none pending: at once");
        assert!(advertised.update(&table(4, &["a", "b"])), "new routes");
        assert!(!acknowledged(4));
        advertised.acked();
        assert!(acknowledged(4));
        drop(advertised);
        assert!(acknowledged(99), "no live stream holds anything back");
    }

    /// Without the window the server fails the stream (FAILED_PRECONDITION),
    /// and no other test here would notice.
    #[test]
    fn the_advertise_announces_the_connection_window() {
        let Some(Body::Advertise(advertise)) = advertise(Vec::new(), false).body else {
            panic!("an Advertise");
        };
        assert_eq!(advertise.connection_window, 1024 * 1024);
        assert_eq!(advertise.connection_window, tunnel::WINDOW);
    }

    /// Without the build the server shows this wicket as an older build.
    #[test]
    fn the_advertise_carries_the_embedded_build() {
        let Some(Body::Advertise(advertise)) = advertise(Vec::new(), false).body else {
            panic!("an Advertise");
        };
        let build = advertise.build.expect("a build");
        assert_eq!(build.implementation, "sluice-wicket-wasi");
        assert_eq!(build.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(build.commit, env!("SLUICE_BUILD_COMMIT"));
        assert_eq!(build.build_time, env!("SLUICE_BUILD_TIME"));
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

/// Test doubles of the server's side of a tunnel stream, and wickets serving
/// the hello guest.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::config;
    use crate::routes::Reloader;
    use crate::wasm_host::{self, Instances};

    /// Frames of the server's stream, gRPC-framed (5-byte prefix + message).
    pub fn grpc_frame(frame: &Frame) -> bytes::Bytes {
        use prost::Message as _;
        let message = frame.encode_to_vec();
        let mut out = Vec::with_capacity(5 + message.len());
        out.push(0);
        out.extend_from_slice(&(message.len() as u32).to_be_bytes());
        out.extend_from_slice(&message);
        out.into()
    }

    /// The state of a wicket serving the hello guest for `hello.local`, its
    /// connection limit and the route's target.
    pub async fn hello_shared(
        stall_timeout: Duration,
    ) -> (Arc<Shared>, Arc<tunnel::Limit>, String) {
        let hello = wasm_host::tests::guest_wasm("hello");
        let source = config::RouteSource::flags(vec![routes::Spec {
            host: "hello.local".into(),
            component: wasm_host::Locator::parse(hello.to_str().unwrap()).unwrap(),
            sandbox: wasm_host::Sandbox::default(),
        }]);
        let reloader = Reloader::start(source, Instances::new(4)).await.unwrap();
        let (shared, limit) = shared_on(reloader.routes(), stall_timeout);
        let target = shared.routes.current().advertised[0].target_url.clone();
        (shared, limit, target)
    }

    /// The state of a wicket serving `routes`, and its connection limit.
    pub fn shared_on(
        routes: &Arc<Routes>,
        stall_timeout: Duration,
    ) -> (Arc<Shared>, Arc<tunnel::Limit>) {
        let limit = tunnel::Limit::new(4);
        let shared = Arc::new(Shared {
            token: String::new(),
            id: "test".into(),
            insecure: false,
            ca_pem: None,
            routes: routes.clone(),
            limit: limit.clone(),
            stall_timeout,
            transport_window: transport_window(4),
            drain: Drain::default(),
            tasks: TaskTracker::new(),
        });
        (shared, limit)
    }

    /// Hang guard of the drain tests' waits.
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
            use prost::Message as _;
            loop {
                if self.buf.len() >= 5 {
                    let len = u32::from_be_bytes(self.buf[1..5].try_into().unwrap()) as usize;
                    if self.buf.len() >= 5 + len {
                        let frame = Frame::decode(&self.buf[5..5 + len]).unwrap();
                        self.buf.drain(..5 + len);
                        return Ok(Some(frame));
                    }
                }
                let Some(chunk) = self.body.data().await else {
                    return Ok(None);
                };
                let chunk = chunk?;
                let _ = self.body.flow_control().release_capacity(chunk.len());
                self.buf.extend_from_slice(&chunk);
            }
        }

        /// The next frame `pick` takes, skipping the others (window updates).
        pub async fn next<T>(&mut self, mut pick: impl FnMut(Body) -> Option<T>) -> T {
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
        /// its response, read to its end.
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
    /// reset.
    pub async fn tunnel_server() -> (String, mpsc::Receiver<ServerStream>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("grpc://{}", listener.local_addr().unwrap());
        let (streams, received) = mpsc::channel(4);
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(socket).await.unwrap();
            // accept() drives the connection on
            while let Some(Ok((request, mut respond))) = connection.accept().await {
                if request.uri().path() != "/sluice.v1.Tunnel/Connect" {
                    continue;
                }
                let response = http::Response::builder()
                    .header("content-type", "application/grpc")
                    .body(())
                    .unwrap();
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
        (url, received)
    }

    /// A session of `shared` on a [`tunnel_server`], its stream as the server
    /// sees it, and the session's event receiver (kept: the stream reports
    /// to it).
    pub async fn served_session(shared: &Arc<Shared>) -> (ServerStream, mpsc::Receiver<Event>) {
        let (url, mut streams) = tunnel_server().await;
        let (events, received) = mpsc::channel(32);
        spawn_session(shared, url.clone(), url, events);
        let stream = tokio::time::timeout(HANG, streams.recv())
            .await
            .expect("hang guard")
            .expect("a stream");
        (stream, received)
    }
}
