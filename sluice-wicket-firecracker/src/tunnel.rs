//! The tunnel stream to one sluice server, and the relay of each of its connections
//! to a guest's vsock stream.
//!
//! Spike scope: one server (no ListNodes fan-out, no drain), reconnects with a fixed
//! backoff. Flow control as the protocol requires: the wicket announces a
//! per-connection receive window and grants back what it wrote to the guest; it sends
//! DATA only against the server's credit. Reclaims are ignored (allowed by the proto).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::BoxError;
use crate::proto::frame::Body;
use crate::proto::tunnel_client::TunnelClient;
use crate::proto::{Advertise, Close, Data, Error, Frame, PeerBuild, Upstream, WindowUpdate};
use crate::route::{Route, Routes};
use crate::wire;

/// The receive window announced for every connection.
const WINDOW: u32 = 1024 * 1024;

/// Largest DATA payload sent.
const MAX_FRAME: usize = 16 * 1024;

pub struct Settings {
    pub server: String,
    pub token: String,
    pub id: String,
}

/// Keeps a tunnel stream to the server up, forever.
pub async fn run(settings: Settings, routes: Arc<Routes>) {
    loop {
        match session(&settings, &routes).await {
            Ok(()) => eprintln!("tunnel stream ended; reconnecting"),
            Err(e) => eprintln!("tunnel stream failed: {e}; reconnecting"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// What the receive loop hands a connection's relay.
enum Inbound {
    Payload(Vec<u8>),
    /// The server half-closed: no more payload.
    Close,
}

struct Connection {
    inbound: mpsc::UnboundedSender<Inbound>,
    credit: Arc<Credit>,
    /// Payload received and not yet granted back.
    unacked: Arc<std::sync::atomic::AtomicU64>,
}

type Registry = Arc<Mutex<HashMap<i64, Connection>>>;

async fn session(settings: &Settings, routes: &Arc<Routes>) -> Result<(), BoxError> {
    let authority = settings
        .server
        .strip_prefix("grpc://")
        .unwrap_or(&settings.server);
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{authority}"))?
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_while_idle(true)
        .connect()
        .await?;
    let (outbound, frames) = mpsc::unbounded_channel();
    let _ = outbound.send(advertise(routes));
    let mut request = tonic::Request::new(UnboundedReceiverStream::new(frames));
    if !settings.token.is_empty() {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", settings.token).parse()?,
        );
    }
    request
        .metadata_mut()
        .insert("x-sluice-id", settings.id.parse()?);
    let mut inbound = TunnelClient::new(channel)
        .connect(request)
        .await?
        .into_inner();
    let registry: Registry = Arc::default();
    let result = loop {
        let frame = match inbound.message().await {
            Ok(Some(frame)) => frame,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e.into()),
        };
        match frame.body {
            Some(Body::AdvertiseAck(ack)) => println!(
                "advertise acked by {} (rejected ports: {:?})",
                ack.node_id, ack.rejected_ports
            ),
            Some(Body::Connect(connect)) => {
                let Some(route) = routes.get(&connect.address).cloned() else {
                    eprintln!(
                        "conn {}: no route for '{}'",
                        connect.conn_id, connect.address
                    );
                    let _ = outbound.send(error(connect.conn_id, "no firecracker route"));
                    continue;
                };
                let (tx, rx) = mpsc::unbounded_channel();
                let credit = Arc::new(Credit::new(connect.connection_window));
                let unacked = Arc::new(std::sync::atomic::AtomicU64::new(0));
                registry.lock().unwrap().insert(
                    connect.conn_id,
                    Connection {
                        inbound: tx,
                        credit: credit.clone(),
                        unacked: unacked.clone(),
                    },
                );
                tokio::spawn(relay(
                    connect.conn_id,
                    route,
                    rx,
                    credit,
                    unacked,
                    outbound.clone(),
                    registry.clone(),
                ));
            }
            Some(Body::Data(data)) => {
                let registry = registry.lock().unwrap();
                if let Some(connection) = registry.get(&data.conn_id) {
                    let len = data.payload.len() as u64;
                    let held = connection
                        .unacked
                        .fetch_add(len, std::sync::atomic::Ordering::AcqRel);
                    if held + len > u64::from(WINDOW) {
                        let _ = outbound.send(error(data.conn_id, "flow control window exceeded"));
                        connection.credit.fail();
                        continue;
                    }
                    let _ = connection.inbound.send(Inbound::Payload(data.payload));
                }
            }
            Some(Body::Close(close)) => {
                if let Some(connection) = registry.lock().unwrap().get(&close.conn_id) {
                    let _ = connection.inbound.send(Inbound::Close);
                }
            }
            Some(Body::Error(e)) => {
                if let Some(connection) = registry.lock().unwrap().remove(&e.conn_id) {
                    connection.credit.fail();
                }
            }
            Some(Body::WindowUpdate(update)) => {
                if let Some(connection) = registry.lock().unwrap().get(&update.conn_id) {
                    connection.credit.grant(update.increment);
                }
            }
            // keep-alive, membership and drain: out of the spike's scope
            _ => {}
        }
    };
    for (_, connection) in registry.lock().unwrap().drain() {
        connection.credit.fail();
    }
    result
}

/// One connection: a lease on a guest stream, bytes both ways, half-closes passed on.
async fn relay(
    conn_id: i64,
    route: Arc<Route>,
    mut inbound: mpsc::UnboundedReceiver<Inbound>,
    credit: Arc<Credit>,
    unacked: Arc<std::sync::atomic::AtomicU64>,
    outbound: mpsc::UnboundedSender<Frame>,
    registry: Registry,
) {
    let (mut stream, vm) = match route.open().await {
        Ok(lease) => (lease.stream, lease.vm),
        Err(e) => {
            eprintln!("conn {conn_id} ({}): {e}", route.spec.host);
            let _ = outbound.send(error(conn_id, &format!("guest unreachable: {e}")));
            registry.lock().unwrap().remove(&conn_id);
            return;
        }
    };
    // borrowed halves: an owned write half shuts the socket down when dropped, which
    // Firecracker takes as the end of the whole connection (its answer is lost)
    let (from_guest, mut to_guest) = stream.split();
    let mut from_guest = wire::Reader::new(from_guest);
    let upstream = {
        let outbound = outbound.clone();
        let to_guest = &mut to_guest;
        async move {
            while let Some(message) = inbound.recv().await {
                match message {
                    Inbound::Payload(payload) => {
                        wire::write_data(to_guest, &payload).await?;
                        let len = payload.len() as u32;
                        unacked.fetch_sub(u64::from(len), std::sync::atomic::Ordering::AcqRel);
                        let _ = outbound.send(Frame {
                            body: Some(Body::WindowUpdate(WindowUpdate {
                                conn_id,
                                increment: len,
                                ..Default::default()
                            })),
                        });
                    }
                    Inbound::Close => break,
                }
            }
            // half-close toward the guest, in band; the other direction goes on
            wire::write_close(to_guest).await
        }
    };
    let downstream = {
        let outbound = outbound.clone();
        let credit = credit.clone();
        let from_guest = &mut from_guest;
        async move {
            let mut buffer = vec![0u8; MAX_FRAME];
            loop {
                let Some(allowed) = credit.take(MAX_FRAME, conn_id, &outbound).await else {
                    return Err(std::io::Error::other("connection failed"));
                };
                let n = match from_guest.read(&mut buffer[..allowed]).await? {
                    wire::Read::Data(n) => n,
                    wire::Read::Close => {
                        let _ = outbound.send(Frame {
                            body: Some(Body::Close(Close { conn_id })),
                        });
                        return Ok(());
                    }
                    wire::Read::Eof => {
                        return Err(std::io::Error::other(
                            "guest ended the connection without CLOSE",
                        ));
                    }
                };
                credit.give_back((allowed - n) as u32);
                let _ = outbound.send(Frame {
                    body: Some(Body::Data(Data {
                        conn_id,
                        payload: buffer[..n].to_vec(),
                    })),
                });
            }
        }
    };
    let failed = async { credit.failed().await };
    let result = tokio::select! {
        result = async { tokio::try_join!(upstream, downstream) } => result.map(|_| ()),
        () = failed => Err(std::io::Error::other("connection failed")),
    };
    if let Err(e) = result
        && !credit.is_failed()
    {
        let _ = outbound.send(error(conn_id, &e.to_string()));
    }
    registry.lock().unwrap().remove(&conn_id);
    // a per-connection VM ends with its connection
    drop(vm);
}

/// The server's credit for DATA on one connection.
struct Credit {
    state: Mutex<(u64, bool)>,
    changed: Notify,
}

impl Credit {
    fn new(window: u32) -> Credit {
        Credit {
            state: Mutex::new((u64::from(window), false)),
            changed: Notify::new(),
        }
    }

    fn grant(&self, increment: u32) {
        self.state.lock().unwrap().0 += u64::from(increment);
        self.changed.notify_waiters();
    }

    fn give_back(&self, unused: u32) {
        if unused > 0 {
            self.grant(unused);
        }
    }

    fn fail(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_waiters();
    }

    fn is_failed(&self) -> bool {
        self.state.lock().unwrap().1
    }

    async fn failed(&self) {
        loop {
            let notified = self.changed.notified();
            if self.is_failed() {
                return;
            }
            notified.await;
        }
    }

    /// Takes up to `most` bytes of credit, waiting for some (and telling the server
    /// once it is blocked); None once the connection failed.
    async fn take(
        &self,
        most: usize,
        conn_id: i64,
        outbound: &mpsc::UnboundedSender<Frame>,
    ) -> Option<usize> {
        let mut told = false;
        loop {
            let notified = self.changed.notified();
            {
                let mut state = self.state.lock().unwrap();
                if state.1 {
                    return None;
                }
                if state.0 > 0 {
                    let taken = state.0.min(most as u64);
                    state.0 -= taken;
                    return Some(taken as usize);
                }
            }
            if !told {
                told = true;
                let _ = outbound.send(Frame {
                    body: Some(Body::WindowUpdate(WindowUpdate {
                        conn_id,
                        blocked: true,
                        ..Default::default()
                    })),
                });
            }
            notified.await;
        }
    }
}

fn advertise(routes: &Routes) -> Frame {
    Frame {
        body: Some(Body::Advertise(Advertise {
            upstreams: routes
                .values()
                .map(|route| Upstream {
                    host: route.spec.host.clone(),
                    target_url: route.spec.target(),
                    ..Default::default()
                })
                .collect(),
            connection_window: WINDOW,
            build: Some(PeerBuild {
                implementation: "sluice-wicket-firecracker".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            }),
            withdraw: false,
        })),
    }
}

fn error(conn_id: i64, message: &str) -> Frame {
    Frame {
        body: Some(Body::Error(Error {
            conn_id,
            message: message.into(),
        })),
    }
}
