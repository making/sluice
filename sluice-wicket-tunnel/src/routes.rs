//! The routes a wicket serves, as the tunnel sees them: the [`Table`] every
//! node stream advertises and binds its connections by, published by the
//! wicket ([`Routes::publish`]) and followed by every stream, which
//! advertises a changed table again; and the streams' acknowledgements,
//! which tell when every server took a generation
//! ([`Routes::acknowledged`]). A removed target is served until then: the
//! server may have routed a connection to it before it took the new routes.
//!
//! A stream advertises the routes ready alone ([`Table::ready`]): one with no
//! instance to serve it leaves the advertisement, so the server sends its
//! host to another client serving it, or falls through as for a host no client
//! advertises, until it is ready again ([`Routes::readiness_changed`]). It is
//! still bound: the server may have routed a connection to it before.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

    /// What a stream advertises: [`Table::advertised`], those [`Table::ready`].
    fn offered(&self) -> Vec<Upstream> {
        let advertised = self.advertised().iter();
        advertised.filter(|up| self.ready(up)).cloned().collect()
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
}

/// Live stream -> the newest generation the server acknowledged on it (0
/// before its first acknowledgement).
#[derive(Default)]
struct Acks {
    streams: watch::Sender<HashMap<u64, u64>>,
    next_stream: AtomicU64,
}

impl<T> Routes<T> {
    pub fn new(table: T) -> Arc<Routes<T>> {
        Arc::new(Routes {
            table: watch::Sender::new(Arc::new(table)),
            acks: Arc::default(),
        })
    }

    pub fn current(&self) -> Arc<T> {
        self.table.borrow().clone()
    }

    /// Follows the table: a stream re-advertises on a change.
    pub fn subscribe(&self) -> watch::Receiver<Arc<T>> {
        self.table.subscribe()
    }

    /// Makes `table` the current one: every stream advertises it, if its
    /// upstreams changed, and binds its connections by it from now on.
    pub fn publish(&self, table: T) {
        self.table.send_replace(Arc::new(table));
    }

    /// The readiness of the current table's routes ([`Table::ready`]) may have changed: every
    /// stream advertises those ready again, if they are not what it advertised. The same
    /// generation: the server takes no other routes than the table's.
    pub fn readiness_changed(&self) {
        self.table.send_modify(|_| {});
    }

    /// Registers a live stream, unregistered when the guard drops.
    pub fn stream(&self) -> StreamAcks {
        let id = self.acks.next_stream.fetch_add(1, Ordering::Relaxed);
        self.acks.streams.send_modify(|streams| {
            streams.insert(id, 0);
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
            .wait_for(|streams| streams.values().all(|acked| *acked >= generation))
            .await;
    }
}

impl<T: Table> Routes<T> {
    /// What serves a connection to `address`, see [`Table::bind`].
    pub fn bind(self: &Arc<Self>, address: &str) -> Result<T::Binding, String> {
        T::bind(self, address)
    }
}

/// A live stream's acknowledgements; dropping it unregisters the stream.
pub struct StreamAcks {
    acks: Arc<Acks>,
    id: u64,
}

impl StreamAcks {
    /// The server took the routes of `generation` on this stream.
    pub fn acked(&self, generation: u64) {
        self.acks.streams.send_modify(|streams| {
            if let Some(acked) = streams.get_mut(&self.id) {
                *acked = (*acked).max(generation);
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
