//! The accept loop: at most so many connections open at once, each with TCP
//! keepalive, so that a peer gone silently gives its slot back. All taken, one
//! that only waits on its client gives way ([`Claim::yields`]), so no number of
//! connections left open keeps out the next.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::Instant;

/// How long the accept loop waits after an accept failed.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// How long a connection just accepted is left to show what it is at before it may give way: asked
/// at once, one of a burst past the limit would go before its request was read, and at the rate of
/// a flood before any client's could arrive.
pub const FRESH: Duration = Duration::from_secs(1);

/// [`keep_alive`]'s idle time, probe interval and probes left unanswered.
const KEEP_ALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEP_ALIVE_PROBE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_PROBES: u32 = 3;

/// A listener's connections open.
pub struct Open {
    max: usize,
    connections: Mutex<Connections>,
    /// A connection ended or now yields. Only the accept loop waits on it.
    changed: Notify,
}

#[derive(Default)]
struct Connections {
    by_id: HashMap<u64, Connection>,
    /// Numbers the connections and orders their turns to yield.
    clock: u64,
}

struct Connection {
    source: IpAddr,
    yields: bool,
    /// When it last turned to waiting on its client, on the clock.
    since: u64,
    /// Until when it does not give way though it yields ([`FRESH`]).
    fresh_until: Instant,
    /// Told to give way, and not gone yet.
    told: bool,
    asked: Arc<Notify>,
}

impl Connections {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Tells a connection that yields to give way, unless one is on its way
    /// out: the one waiting longest of the source holding the most, so that a
    /// source opening many pushes out only its own. With all that yield still
    /// fresh, when the first of them no longer is.
    fn ask_one(&mut self) -> Option<Instant> {
        if self.by_id.values().any(|connection| connection.told) {
            return None;
        }
        let mut held: HashMap<IpAddr, usize> = HashMap::new();
        for connection in self.by_id.values() {
            *held.entry(connection.source).or_default() += 1;
        }
        let now = Instant::now();
        let next = self
            .by_id
            .values_mut()
            .filter(|connection| connection.yields && connection.fresh_until <= now)
            .max_by_key(|connection| (held[&connection.source], Reverse(connection.since)));
        if let Some(connection) = next {
            connection.told = true;
            connection.asked.notify_one();
            return None;
        }
        self.by_id
            .values()
            .filter(|connection| connection.yields)
            .map(|connection| connection.fresh_until)
            .min()
    }
}

impl Open {
    pub fn new(max: usize) -> Arc<Open> {
        Arc::new(Open {
            max,
            connections: Mutex::default(),
            changed: Notify::new(),
        })
    }

    /// A slot for a connection from `peer`, once one is free: a connection that
    /// yields is told to give way, once no longer [`FRESH`], and with none this
    /// waits for one to end or yield.
    pub async fn place(self: &Arc<Open>, peer: IpAddr) -> Slot {
        loop {
            let fresh_until = {
                let mut connections = self.connections.lock().expect("never poisoned");
                if connections.by_id.len() < self.max {
                    let id = connections.tick();
                    let asked = Arc::new(Notify::new());
                    connections.by_id.insert(
                        id,
                        Connection {
                            source: source(peer),
                            yields: false,
                            since: id,
                            fresh_until: Instant::now() + FRESH,
                            told: false,
                            asked: asked.clone(),
                        },
                    );
                    let claim = Claim {
                        open: self.clone(),
                        id,
                    };
                    return Slot { claim, asked };
                }
                connections.ask_one()
            };
            let changed = self.changed.notified();
            match fresh_until {
                Some(at) => {
                    let _ = tokio::time::timeout_at(at, changed).await;
                }
                None => changed.await,
            }
        }
    }
}

/// Whom a connection counts against: its address, an IPv6 one by its /64, which
/// one host commonly has all of.
fn source(peer: IpAddr) -> IpAddr {
    match peer.to_canonical() {
        IpAddr::V6(address) => IpAddr::V6(Ipv6Addr::from_bits(address.to_bits() & (!0 << 64))),
        address => address,
    }
}

/// One of the listener's connections open: dropped, another may be accepted.
pub struct Slot {
    claim: Claim,
    asked: Arc<Notify>,
}

impl Slot {
    /// What says whether this connection may give way.
    pub fn claim(&self) -> &Claim {
        &self.claim
    }

    /// Ready once another connection wants this slot: drop it, and the
    /// connection with it. Only a connection that yields is asked.
    pub async fn asked(&self) {
        self.asked.notified().await
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let open = &self.claim.open;
        let mut connections = open.connections.lock().expect("never poisoned");
        connections.by_id.remove(&self.claim.id);
        open.changed.notify_one();
    }
}

/// Whether a connection may lose its [`Slot`] to another. It holds no slot: once
/// the slot is dropped it says nothing.
#[derive(Clone)]
pub struct Claim {
    open: Arc<Open>,
    id: u64,
}

impl Claim {
    /// The connection only waits on its client, or serves one not known to be
    /// anyone: it gives way when all slots are taken, the longest waiting first,
    /// once no longer [`FRESH`]. A connection is given its slot not yielding, and
    /// keeps it so unless this is said.
    pub fn yields(&self) {
        let mut connections = self.open.connections.lock().expect("never poisoned");
        let now = connections.tick();
        if let Some(connection) = connections.by_id.get_mut(&self.id) {
            connection.yields = true;
            connection.since = now;
            self.open.changed.notify_one();
        }
    }

    /// The connection is at work for someone admitted: it keeps its slot until
    /// [`Claim::yields`] is said again, with no time left of being [`FRESH`].
    pub fn holds(&self) {
        let mut connections = self.open.connections.lock().expect("never poisoned");
        if let Some(connection) = connections.by_id.get_mut(&self.id) {
            connection.yields = false;
            connection.fresh_until = Instant::now();
        }
    }
}

/// TCP keepalive, as the server's `sluice.limits.tcp-keep-alive` by default: once
/// `stream` carried nothing for 60s the kernel probes its peer every 10s, and
/// after 3 probes unanswered, or one answered with a reset, fails its reads and
/// writes. Nothing else tells of a peer gone without its FIN or reset reaching
/// us (a host removed, a NAT entry dropped), whose connection would hold its
/// slot for good.
pub fn keep_alive(stream: &TcpStream) -> io::Result<()> {
    socket2::SockRef::from(stream).set_tcp_keepalive(
        &socket2::TcpKeepalive::new()
            .with_time(KEEP_ALIVE_IDLE)
            .with_interval(KEEP_ALIVE_PROBE_INTERVAL)
            .with_retries(KEEP_ALIVE_PROBES),
    )
}

/// Accepts connections for good, at most `max_connections` open at once: all
/// taken, one that yields gives way ([`Claim::yields`]); with none yielding the
/// next waits, those beyond it in the backlog. Each goes to `serve` with its
/// [`Slot`], to be held until the connection ended, [`keep_alive`] set. A failed
/// accept (out of file descriptors, say) is retried, not fatal. `label` names
/// the listener in what is logged.
pub async fn run<F>(listener: TcpListener, max_connections: usize, label: &str, mut serve: F)
where
    F: FnMut(TcpStream, SocketAddr, Slot),
{
    let open = Open::new(max_connections);
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let slot = open.place(peer.ip()).await;
                let _ = stream.set_nodelay(true);
                if let Err(e) = keep_alive(&stream) {
                    eprintln!("{label}: no keepalive on {peer}: {e}");
                }
                serve(stream, peer, slot);
            }
            Err(e) => {
                eprintln!("{label}: accept failed: {e}");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::{JoinHandle, yield_now};

    use super::*;

    /// An accepted socket goes to `serve` probing its peer once it idled: nothing
    /// else tells of a peer gone without a FIN or reset.
    #[tokio::test]
    async fn an_accepted_socket_probes_a_vanished_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, mut accepting) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(run(listener, 1, "test", move |stream, _, slot| {
            accepted.send((stream, slot)).unwrap();
        }));
        let _client = TcpStream::connect(address).await.unwrap();
        let (stream, _slot) = accepting.recv().await.unwrap();
        let socket = socket2::SockRef::from(&stream);
        assert!(socket.keepalive().unwrap());
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(60)
        );
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(10)
        );
        assert_eq!(socket.tcp_keepalive_retries().unwrap(), 3);
    }

    /// Whether `slot` was told to give way by now.
    async fn asked(slot: &Slot) -> bool {
        tokio::time::timeout(Duration::ZERO, slot.asked())
            .await
            .is_ok()
    }

    /// A slot taken by a task of its own, having tried: it waits if none is free.
    async fn wanted(open: &Arc<Open>, peer: &str) -> JoinHandle<Slot> {
        let open = open.clone();
        let peer = peer.parse().unwrap();
        let wanted = tokio::spawn(async move { open.place(peer).await });
        yield_now().await;
        wanted
    }

    /// All slots taken, the one wanted comes from whoever yields: of the source
    /// holding the most, the one waiting longest. One that holds is left alone,
    /// and one just accepted, for a moment; with no other the next waits, for one
    /// to end or yield.
    #[tokio::test(start_paused = true)]
    async fn the_slot_wanted_comes_from_the_largest_sources_longest_waiting() {
        let open = Open::new(5);
        let quiet = wanted(&open, "10.0.0.1").await.await.unwrap();
        let held = wanted(&open, "2001:db8:1:1::1").await.await.unwrap();
        let longest = wanted(&open, "2001:db8:1:1::2").await.await.unwrap();
        let later = wanted(&open, "2001:db8:1:1::3").await.await.unwrap();
        let mapped = wanted(&open, "::ffff:10.0.0.1").await.await.unwrap();
        for slot in [&quiet, &longest, &later, &mapped] {
            slot.claim().yields();
        }

        // the /64 holds 3, 10.0.0.1 holds 2, its mapped address counted
        let next = wanted(&open, "10.0.0.2").await;
        assert!(!asked(&longest).await, "just accepted");
        tokio::time::sleep(FRESH).await;
        yield_now().await;
        assert!(asked(&longest).await);
        for slot in [&quiet, &held, &later, &mapped] {
            assert!(!asked(slot).await);
        }
        assert!(!next.is_finished());
        drop(longest);
        let second = next.await.unwrap();

        // 2 each: the longest waiting of them
        let next = wanted(&open, "10.0.0.3").await;
        assert!(asked(&quiet).await);
        drop(quiet);
        let third = next.await.unwrap();

        // none yields, those just placed included; a request shown ends a slot's first moment
        later.claim().holds();
        mapped.claim().holds();
        let next = wanted(&open, "10.0.0.4").await;
        for slot in [&held, &later, &mapped, &second, &third] {
            assert!(!asked(slot).await);
        }
        second.claim().holds();
        second.claim().yields();
        yield_now().await;
        assert!(asked(&second).await);
        assert!(!next.is_finished());
        drop(second);
        next.await.unwrap();
    }
}
