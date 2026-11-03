//! Local mode's listener (`--listen`): a wicket's routes served over plain TCP,
//! with or without a sluice server. What the wickets' local modes have alike:
//! the accept loop ([`run`]), the stream replaying the bytes a wicket read to
//! pick the route ([`Replay`]), and the rule picking it
//! ([`crate::routes::Table::host_route`]). How a connection is served -- HTTP
//! terminated, or relayed verbatim -- is the wicket's.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How long the accept loop waits after an accept failed.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// [`keep_alive`]'s idle time, probe interval and probes left unanswered.
const KEEP_ALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEP_ALIVE_PROBE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_PROBES: u32 = 3;

/// Binds `address` (`host:port`).
pub async fn bind(address: &str) -> Result<TcpListener, String> {
    TcpListener::bind(address)
        .await
        .map_err(|e| format!("--listen {address}: {e}"))
}

/// One of the listener's connections open: dropped, another may be accepted.
pub struct Slot {
    _permit: OwnedSemaphorePermit,
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

/// Accepts connections for good, at most `max_connections` open at once (a
/// connection beyond waits in the backlog), and hands each to `serve` with its
/// [`Slot`], to be held until the connection ended, [`keep_alive`] set. A failed
/// accept (out of file descriptors, say) is retried, not fatal.
pub async fn run<F>(listener: TcpListener, max_connections: usize, mut serve: F)
where
    F: FnMut(TcpStream, SocketAddr, Slot),
{
    let open = Arc::new(Semaphore::new(max_connections));
    loop {
        let permit = open.clone().acquire_owned().await.expect("never closed");
        match listener.accept().await {
            Ok((stream, peer)) => {
                let _ = stream.set_nodelay(true);
                if let Err(e) = keep_alive(&stream) {
                    eprintln!("--listen: no keepalive on {peer}: {e}");
                }
                serve(stream, peer, Slot { _permit: permit });
            }
            Err(e) => {
                eprintln!("--listen: accept failed: {e}");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

/// The host of `authority` (a `Host` header or an h2 `:authority`): its port
/// left out, an IPv6 address kept in its brackets.
pub fn host_of(authority: &str) -> &str {
    let authority = authority.trim();
    if authority.starts_with('[') {
        return match authority.find(']') {
            Some(end) => &authority[..=end],
            None => authority,
        };
    }
    match authority.split_once(':') {
        Some((host, _)) => host,
        None => authority,
    }
}

/// A stream whose first bytes were read to decide how to serve it, and are
/// read again first: [`Replay::fill`] reads them, then reading the stream
/// replays them before the rest. Writes go to the stream.
pub struct Replay<S> {
    read: Vec<u8>,
    /// How much of `read` was read again.
    replayed: usize,
    stream: S,
}

impl<S: AsyncRead + Unpin> Replay<S> {
    pub fn new(stream: S) -> Replay<S> {
        Replay {
            read: Vec::new(),
            replayed: 0,
            stream,
        }
    }

    /// The bytes read so far.
    pub fn read_so_far(&self) -> &[u8] {
        &self.read
    }

    /// Reads at most `max` more bytes behind those read so far; 0 at the
    /// stream's end. Only before the stream is read.
    pub async fn fill(&mut self, max: usize) -> io::Result<usize> {
        debug_assert_eq!(self.replayed, 0, "filled after a read");
        let start = self.read.len();
        self.read.resize(start + max, 0);
        let read = self.stream.read(&mut self.read[start..]).await;
        self.read.truncate(start + *read.as_ref().unwrap_or(&0));
        read
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Replay<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if this.replayed < this.read.len() {
            let rest = &this.read[this.replayed..];
            let n = rest.len().min(buf.remaining());
            buf.put_slice(&rest[..n]);
            this.replayed += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Replay<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt as _;

    use super::*;
    use crate::routes::Table as _;
    use crate::routes::testing::Fixed;

    /// A host is routed ignoring its case and port; a route alone takes any host
    /// only as its wicket says.
    #[test]
    fn a_host_takes_its_route_or_the_sole_one() {
        let two = Fixed::hosts(1, &["a.local", "[::1]"]);
        let routed = |table: &Fixed, authority: &str| {
            table
                .host_route(host_of(authority))
                .map(|up| up.host.clone())
        };
        assert_eq!(routed(&two, "A.Local:8080").as_deref(), Some("a.local"));
        assert_eq!(routed(&two, "[::1]:8080").as_deref(), Some("[::1]"));
        assert_eq!(routed(&two, "b.local"), None);
        assert_eq!(routed(&two, ""), None);
        let alone = Fixed::hosts(1, &["a.local"]);
        assert_eq!(routed(&alone, "A.local").as_deref(), Some("a.local"));
        assert_eq!(routed(&alone, "b.local"), None);
        let sole = Fixed {
            takes_any_host: true,
            ..alone
        };
        assert_eq!(routed(&sole, "b.local").as_deref(), Some("a.local"));
        assert_eq!(routed(&Fixed::hosts(1, &[]), "a.local"), None);
        let several = Fixed {
            takes_any_host: true,
            ..Fixed::hosts(1, &["a.local", "b.local"])
        };
        assert_eq!(routed(&several, "c.local"), None);
    }

    /// An accepted socket goes to `serve` probing its peer once it idled: nothing
    /// else tells of a peer gone without a FIN or reset.
    #[tokio::test]
    async fn an_accepted_socket_probes_a_vanished_peer() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, mut accepting) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(run(listener, 1, move |stream, _, slot| {
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

    /// The bytes filled are read again first, then the rest of the stream.
    #[tokio::test]
    async fn the_bytes_filled_are_replayed_first() {
        let (client, server) = tokio::io::duplex(64);
        let mut replay = Replay::new(server);
        let mut client = client;
        client.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        assert_eq!(replay.fill(4).await.unwrap(), 4);
        assert_eq!(replay.read_so_far(), b"GET ");
        client.write_all(b"\r\n").await.unwrap();
        drop(client);
        let mut all = Vec::new();
        replay.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"GET / HTTP/1.1\r\n\r\n");
    }
}
