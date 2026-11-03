//! The accept loop: at most so many connections open at once, each with TCP
//! keepalive, so that a peer gone silently gives its slot back.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How long the accept loop waits after an accept failed.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// [`keep_alive`]'s idle time, probe interval and probes left unanswered.
const KEEP_ALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEP_ALIVE_PROBE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_PROBES: u32 = 3;

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
/// accept (out of file descriptors, say) is retried, not fatal. `label` names
/// the listener in what is logged.
pub async fn run<F>(listener: TcpListener, max_connections: usize, label: &str, mut serve: F)
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
                    eprintln!("{label}: no keepalive on {peer}: {e}");
                }
                serve(stream, peer, Slot { _permit: permit });
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
}
