//! The routes a wicket serves, as the tunnel sees them: the [`Table`] every
//! node stream advertises and binds its connections by, published by the
//! wicket ([`Routes::publish`]) and followed by every stream, which
//! advertises a changed table again; and the streams' acknowledgements,
//! which tell when every server took a generation
//! ([`Routes::acknowledged`]), and a route ([`Routes::taken`]). A removed
//! target is served until then: the server may have routed a connection to it
//! before it took the new routes.
//!
//! A stream advertises the routes ready alone ([`Table::ready`]): one with no
//! instance to serve it leaves the advertisement, so the server sends its
//! host to another client serving it, or falls through as for a host no client
//! advertises, until it is ready again ([`Routes::readiness_changed`]). It is
//! still bound: the server may have routed a connection to it before. So is a
//! route held ([`Table::held`]): an app a wicket serves once its controller
//! confirmed it since the wicket's start. Routes note the hosts that had an
//! instance ready since the wicket's start: what its readiness waits for
//! ([`Routes::readiness`]).
//!
//! A route's weight (`Upstream::weight`: its instances, by which the server
//! spreads its host over the clients serving it) is no change of the routes
//! ([`same_routes`]): a wicket publishes a change of it in the generation it
//! was, which every stream advertises all the same.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::proto::Upstream;
use crate::tunnel::TunnelIo;

/// The routes at one point: what is advertised and what serves it.
pub trait Table: Send + Sync + Sized + 'static {
    /// What serves a connection.
    type Binding: Binding;

    /// Counts the reloads that changed the routes. A stream acknowledges the
    /// generation whose routes the server took.
    fn generation(&self) -> u64;

    /// The upstreams of the routes: every stream advertises those [`Table::ready`].
    fn advertised(&self) -> &[Upstream];

    /// Whether `route` has an instance ready to serve its next connection: as it is now, not as
    /// the table was published. The wicket tells a change ([`Routes::readiness_changed`]).
    fn ready(&self, _route: &Upstream) -> bool {
        true
    }

    /// Whether `route` is held: an app of a controller's, which the wicket serves once the
    /// controller confirmed it since its start, or a bound passed. The wicket tells a change
    /// ([`Routes::readiness_changed`]).
    fn held(&self, _route: &Upstream) -> bool {
        false
    }

    /// What a stream advertises: [`Table::advertised`], those [`Table::ready`] and not
    /// [`Table::held`].
    fn offered(&self) -> Vec<Upstream> {
        let advertised = self.advertised().iter();
        advertised
            .filter(|up| !self.held(up) && self.ready(up))
            .cloned()
            .collect()
    }

    /// Whether `route`, advertised alone, takes any host on a wicket's listener:
    /// the local loop of a route given by a flag. Not on a wicket whose routes are
    /// edited: a host left without a route is not another's.
    fn takes_any_host(&self, route: &Upstream) -> bool;

    /// The route taking any host on a wicket's listener, if one does.
    fn sole(&self) -> Option<&Upstream> {
        match self.advertised() {
            [sole] if self.takes_any_host(sole) => Some(sole),
            _ => None,
        }
    }

    /// The advertised route of `host` (no port, see
    /// [`crate::listener::host_of`]) on a wicket's listener, compared ignoring
    /// case; or the [`Table::sole`] route whatever the host. None for an unknown
    /// host.
    fn host_route(&self, host: &str) -> Option<&Upstream> {
        self.sole().or_else(|| {
            self.advertised()
                .iter()
                .find(|up| up.host.eq_ignore_ascii_case(host))
        })
    }

    /// What serves a connection to `address` (the dial address the server
    /// derived from an advertised target): its binding, or the ERROR message
    /// refusing the connection. `routes` holds this table as the current
    /// one, for a binding that follows the tables published after it.
    fn bind(routes: &Arc<Routes<Self>>, address: &str) -> Result<Self::Binding, String>;
}

/// What serves one connection.
pub trait Binding: Send + 'static {
    /// Serves the connection to its end, on a task of its own: reads and
    /// writes `io`, then finishes it (see [`TunnelIo::ending`]).
    fn serve(self, io: TunnelIo) -> impl Future<Output = ()> + Send + 'static;
}

/// The current [`Table`], shared by every node stream and connection, and the
/// acknowledgements of the streams.
pub struct Routes<T> {
    table: watch::Sender<Arc<T>>,
    acks: Arc<Acks>,
    /// The hosts of the routes that had an instance ready ([`Table::ready`]) since the wicket's
    /// start, as a publish, a change told ([`Routes::readiness_changed`]) or a readiness asked
    /// saw them.
    were_ready: Mutex<HashSet<String>>,
}

/// Live stream -> what the server acknowledged on it.
#[derive(Default)]
struct Acks {
    streams: watch::Sender<HashMap<u64, Took>>,
    next_stream: AtomicU64,
}

/// What the server took on a stream: nothing before its first acknowledgement.
#[derive(Default)]
struct Took {
    /// The newest generation.
    generation: u64,
    /// The routes of the newest advertise.
    upstreams: Vec<Upstream>,
}

impl<T> Routes<T> {
    pub fn current(&self) -> Arc<T> {
        self.table.borrow().clone()
    }

    /// Follows the table: a stream re-advertises on a change.
    pub fn subscribe(&self) -> watch::Receiver<Arc<T>> {
        self.table.subscribe()
    }

    /// Registers a live stream, unregistered when the guard drops.
    pub fn stream(&self) -> StreamAcks {
        let id = self.acks.next_stream.fetch_add(1, Ordering::Relaxed);
        self.acks.streams.send_modify(|streams| {
            streams.insert(id, Took::default());
        });
        StreamAcks {
            acks: self.acks.clone(),
            id,
        }
    }

    /// Resolves once every live stream acknowledged `generation` or a later
    /// one; at once without a live stream.
    pub async fn acknowledged(&self, generation: u64) {
        let mut streams = self.acks.streams.subscribe();
        let _ = streams
            .wait_for(|streams| streams.values().all(|took| took.generation >= generation))
            .await;
    }

    /// Whether the server of every live stream took `route` as it is, in its stream's newest
    /// advertise acknowledged: it sends a connection to it. So without a live stream.
    pub fn taken(&self, route: &Upstream) -> bool {
        let streams = self.acks.streams.borrow();
        streams.values().all(|took| took.upstreams.contains(route))
    }
}

impl<T: Table> Routes<T> {
    pub fn new(table: T) -> Arc<Routes<T>> {
        let routes = Routes {
            table: watch::Sender::new(Arc::new(table)),
            acks: Arc::default(),
            were_ready: Mutex::default(),
        };
        routes.unready();
        Arc::new(routes)
    }

    /// Makes `table` the current one: every stream advertises it, if its
    /// upstreams changed, and binds its connections by it from now on.
    pub fn publish(&self, table: T) {
        self.table.send_replace(Arc::new(table));
        self.unready();
    }

    /// The readiness of the current table's routes ([`Table::ready`]) may have changed: every
    /// stream advertises those ready again, if they are not what it advertised. The same
    /// generation: the server takes no other routes than the table's.
    pub fn readiness_changed(&self) {
        self.table.send_modify(|_| {});
        self.unready();
    }

    /// What serves a connection to `address`, see [`Table::bind`].
    pub fn bind(self: &Arc<Self>, address: &str) -> Result<T::Binding, String> {
        T::bind(self, address)
    }

    /// Whether the current table's routes let the wicket serve: none held ([`Table::held`]),
    /// and, while `waiting` (for a bound from the wicket's start), each with an instance ready
    /// since then ([`Table::ready`]). Ok names the hosts of the routes with no instance ready
    /// now, if any; Err those of the routes that are not as told, then them.
    pub fn readiness(&self, waiting: bool) -> Result<Option<String>, String> {
        let Unready { held, none } = self.unready();
        let (yet, now): (Vec<_>, Vec<_>) = none.into_iter().partition(|(_, was)| waiting && !was);
        let hosts = |hosts: Vec<String>| (!hosts.is_empty()).then(|| hosts.join(", "));
        let routes =
            |routes: Vec<(String, bool)>| hosts(routes.into_iter().map(|(host, _)| host).collect());
        let why = [
            hosts(held).map(|held| format!("held until their owners confirm them: {held}")),
            routes(yet).map(|yet| format!("no instance ready yet for {yet}")),
        ];
        let note = routes(now).map(|now| format!("no instance ready for {now}"));
        let why: Vec<String> = why.into_iter().flatten().collect();
        match why.is_empty() {
            true => Ok(note),
            false => Err(why.into_iter().chain(note).collect::<Vec<_>>().join("; ")),
        }
    }

    /// The hosts of the current table's routes with no instance ready since the wicket's start,
    /// held routes aside.
    pub fn never_ready(&self) -> Vec<String> {
        let none = self.unready().none.into_iter();
        none.filter(|(_, was)| !was).map(|(host, _)| host).collect()
    }

    /// The current table's routes not ready, in its order; notes those ready.
    fn unready(&self) -> Unready {
        let table = self.current();
        let mut were_ready = self.were_ready.lock().unwrap();
        let mut unready = Unready::default();
        for route in table.advertised() {
            let host = route.host.clone();
            if table.held(route) {
                unready.held.push(host);
            } else if table.ready(route) {
                were_ready.insert(host);
            } else {
                let was = were_ready.contains(&host);
                unready.none.push((host, was));
            }
        }
        unready
    }
}

/// The hosts of a table's routes not ready ([`Routes::readiness`]).
#[derive(Default)]
struct Unready {
    /// Held ([`Table::held`]).
    held: Vec<String>,
    /// With no instance ready, each with whether it had one since the wicket's start.
    none: Vec<(String, bool)>,
}

/// Whether `a` and `b` advertise the same routes, in the same order, whatever their weights.
pub fn same_routes(a: &[Upstream], b: &[Upstream]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(a, b)| {
            let weighed = Upstream {
                weight: b.weight,
                ..a.clone()
            };
            weighed == *b
        })
}

/// A live stream's acknowledgements; dropping it unregisters the stream.
pub struct StreamAcks {
    acks: Arc<Acks>,
    id: u64,
}

impl StreamAcks {
    /// The server took the routes of `generation` on this stream, `upstreams` those it was
    /// last advertised.
    pub fn acked(&self, generation: u64, upstreams: &[Upstream]) {
        self.acks.streams.send_modify(|streams| {
            if let Some(took) = streams.get_mut(&self.id) {
                took.generation = took.generation.max(generation);
                upstreams.clone_into(&mut took.upstreams);
            }
        });
    }
}

impl Drop for StreamAcks {
    fn drop(&mut self) {
        self.acks.streams.send_modify(|streams| {
            streams.remove(&self.id);
        });
    }
}

/// Routes for the tests: fixed upstreams, every target among them served by
/// a stand-in for the wicket's handler.
#[cfg(test)]
pub(crate) mod testing {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// The routes of one generation, serving each advertised target with
    /// [`Hello`].
    pub(crate) struct Fixed {
        pub(crate) generation: u64,
        pub(crate) advertised: Vec<Upstream>,
        /// See [`Table::takes_any_host`].
        pub(crate) takes_any_host: bool,
        /// The hosts with no instance ready, as they are now ([`Table::ready`]).
        pub(crate) unready: Arc<std::sync::Mutex<Vec<String>>>,
        /// The hosts held, as they are now ([`Table::held`]).
        pub(crate) held: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Fixed {
        /// `hosts`, each advertised with the target `test:<host>`.
        pub(crate) fn hosts(generation: u64, hosts: &[&str]) -> Fixed {
            Fixed {
                generation,
                advertised: hosts
                    .iter()
                    .map(|host| Upstream {
                        host: host.to_string(),
                        target_url: format!("test:{host}"),
                        ..Default::default()
                    })
                    .collect(),
                takes_any_host: false,
                unready: Arc::default(),
                held: Arc::default(),
            }
        }
    }

    impl Table for Fixed {
        type Binding = Hello;

        fn generation(&self) -> u64 {
            self.generation
        }

        fn advertised(&self) -> &[Upstream] {
            &self.advertised
        }

        fn ready(&self, route: &Upstream) -> bool {
            !self.unready.lock().unwrap().contains(&route.host)
        }

        fn held(&self, route: &Upstream) -> bool {
            self.held.lock().unwrap().contains(&route.host)
        }

        fn takes_any_host(&self, _route: &Upstream) -> bool {
            self.takes_any_host
        }

        fn bind(routes: &Arc<Routes<Fixed>>, address: &str) -> Result<Hello, String> {
            let table = routes.current();
            if table.advertised.iter().any(|up| up.target_url == address) {
                Ok(Hello)
            } else {
                Err(format!("no test route for '{address}'"))
            }
        }
    }

    /// Answers every http/1.1 GET on the connection with a chunked body,
    /// `path=<path>\n`, or 10MiB for `/stream`; ends the connection at its
    /// end (the server's CLOSE) or failure.
    pub(crate) struct Hello;

    /// What `/stream` answers.
    const STREAM_BYTES: usize = 10 << 20;

    impl Binding for Hello {
        async fn serve(self, mut io: TunnelIo) {
            let ending = io.ending();
            let failed = io.failed();
            let served = async {
                let mut received = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let n = io.read(&mut buffer).await?;
                    if n == 0 {
                        return io.shutdown().await;
                    }
                    received.extend_from_slice(&buffer[..n]);
                    while let Some(end) = received.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head: Vec<u8> = received.drain(..end + 4).collect();
                        let head = String::from_utf8_lossy(&head);
                        let path = head.split(' ').nth(1).unwrap_or("/").to_string();
                        io.write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n")
                            .await?;
                        if path == "/stream" {
                            let chunk = vec![b'x'; 64 * 1024];
                            for _ in 0..STREAM_BYTES / chunk.len() {
                                io.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                                    .await?;
                                io.write_all(&chunk).await?;
                                io.write_all(b"\r\n").await?;
                            }
                        } else {
                            let body = format!("path={path}\n");
                            io.write_all(format!("{:x}\r\n{body}\r\n", body.len()).as_bytes())
                                .await?;
                        }
                        io.write_all(b"0\r\n\r\n").await?;
                    }
                }
            };
            tokio::select! {
                _ = served => {}
                () = failed => {}
            }
            ending.finish();
        }
    }
}
