//! `--listen`: the routes served over plain TCP as well, relayed as the tunnel's
//! connections are (no sluice server needed: a local loop for trying a rootfs).
//!
//! A connection is routed once, by its first request (`head`): the route of its
//! host -- the `Host` header, or the h2c `:authority` -- or the sole route
//! whatever the host (`sluice_wicket_tunnel::routes::Table::sole`), which reads
//! nothing first: its guest may speak first. What was read is relayed
//! verbatim, as the rest: a later request of the connection, an h2 stream or a
//! keep-alive request, stays on that route whatever its host, and no
//! `X-Forwarded-*` header is added. An unknown host is answered 404, a request
//! finding no room for its VM 502 ([`bad_gateway`]); a connection that is not
//! HTTP, or whose head runs past `head::MAX_HEAD` or takes longer than
//! [`HEAD_TIMEOUT`], is closed.
//!
//! A connection holds a slot, and a VM on a per-connection or reuse route, and no server's limits
//! stand before it, so its client gets [`QUIET`] with no request in progress (`exchange`), and to
//! take a write of an answer ([`Unread`], whatever the protocol): the guest then ends the
//! connection as for a client closing it ([`relay`]). What a request waits on is its guest, and
//! what cannot be read as HTTP is the route's `idle-ms`'s.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

use sluice_wicket_server::accept;
use sluice_wicket_server::http::{QUIET, Unread};
use sluice_wicket_tunnel::listener::{Replay, host_of};
use sluice_wicket_tunnel::routes::Routes;
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::exchange::Exchanges;
use crate::head::{self, Head};
use crate::route::Lease;
use crate::routes::Table;
use crate::wire;

/// How long a connection may take to send its head.
pub const HEAD_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a refused connection is read after its answer, for the answer not to
/// be lost to a reset.
const LINGER: Duration = Duration::from_secs(2);

/// Serves each connection, at most `max_connections` at once (see
/// `sluice_wicket_server::accept::run`), on a worker, by the table of the
/// moment.
pub async fn run(
    listener: TcpListener,
    routes: Arc<Routes<Table>>,
    workers: Arc<Workers>,
    max_connections: usize,
) {
    accept::run(
        listener,
        max_connections,
        "--listen",
        |client, peer, slot| {
            let Ok(client) = client.into_std() else {
                return;
            };
            let routes = routes.clone();
            workers.place().spawn(async move {
                let _slot = slot;
                // the worker's from here on
                if let Ok(client) = TcpStream::from_std(client) {
                    serve(Replay::new(Unread::new(client)), peer, &routes).await;
                }
            });
        },
    )
    .await
}

async fn serve(mut client: Replay<Unread<TcpStream>>, peer: SocketAddr, routes: &Routes<Table>) {
    let route = match routes.current().sole() {
        Some(route) => route,
        None => {
            let head = match tokio::time::timeout(HEAD_TIMEOUT, head::read(&mut client)).await {
                Ok(Ok(head)) => head,
                Ok(Err(e)) => return eprintln!("listener conn {peer}: {e}"),
                Err(_) => {
                    let timeout = HEAD_TIMEOUT.as_secs();
                    return eprintln!("listener conn {peer}: no request head within {timeout}s");
                }
            };
            match routes.current().by_host(host_of(head.host())) {
                Some(route) => route,
                None => {
                    let message = format!("no firecracker route for host '{}'", head.host());
                    return answer(&mut client, &head, "404 Not Found", &message).await;
                }
            }
        }
    };
    match route.open(&peer.ip().to_string()).await {
        Ok(Lease { stream, hold }) => {
            let relayed = relay(&mut client, stream, route.idle()).await;
            // a reused clone kept before the client sees the end
            let ended = hold.end(relayed.is_ok());
            drop(client);
            if let Err(e) = relayed {
                eprintln!("listener ({}): {e}", route.label());
            }
            ended.await;
        }
        Err(e) => {
            eprintln!("listener ({}): {e}", route.label());
            if let Some(why) = route.no_room(&e) {
                bad_gateway(&mut client, &why).await;
            }
        }
    }
}

/// [`wire::relay`] for a client left [`QUIET`] with no request in progress: the guest is told as
/// of a client closing and closes in turn, so a reused clone is kept, and the client sees the end
/// (an h2 client a GOAWAY first). A guest keeping the connection [`LINGER`] more fails it.
async fn relay<C, G>(client: &mut C, guest: G, idle: Option<Duration>) -> io::Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
    G: AsyncRead + AsyncWrite + Unpin,
{
    let progress = Progress::new();
    let watched = Watched {
        client,
        progress: &progress,
        goaway: None,
    };
    tokio::select! {
        relayed = wire::relay(watched, guest, idle) => relayed,
        e = progress.left_waiting() => Err(e),
    }
}

/// A relayed connection's exchanges, and what becomes of its client once they wait on it too long.
struct Progress {
    state: Mutex<State>,
    /// Whether the connection waits on its client alone.
    waiting: watch::Sender<bool>,
}

struct State {
    exchanges: Exchanges,
    /// Reading the client is over: the relay reads its end.
    closing: bool,
    reader: Option<Waker>,
}

impl Progress {
    fn new() -> Progress {
        Progress {
            state: Mutex::new(State {
                exchanges: Exchanges::default(),
                closing: false,
                reader: None,
            }),
            waiting: watch::Sender::new(true),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("no panic holding it")
    }

    fn fed(&self, feed: impl FnOnce(&mut Exchanges)) {
        let mut state = self.state();
        feed(&mut state.exchanges);
        let waiting = state.exchanges.waits_on_client();
        self.waiting.send_if_modified(|now| {
            let changed = *now != waiting;
            *now = waiting;
            changed
        });
    }

    /// Ends the client's side once the connection waited on it for [`QUIET`] straight; resolves
    /// with the error failing a relay that did not end [`LINGER`] after.
    async fn left_waiting(&self) -> io::Error {
        let mut waiting = self.waiting.subscribe();
        loop {
            // failing only once the sender, ours, is gone
            let _ = waiting.wait_for(|waiting| *waiting).await;
            if tokio::time::timeout(QUIET, waiting.changed())
                .await
                .is_err()
            {
                break;
            }
        }
        {
            let mut state = self.state();
            state.closing = true;
            if let Some(reader) = state.reader.take() {
                reader.wake();
            }
        }
        tokio::time::sleep(LINGER).await;
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "its guest kept a connection its client left waiting for {}s",
                QUIET.as_secs()
            ),
        )
    }
}

/// The client's stream, what goes by it fed to [`Progress`]. Past the wait on the client, it reads
/// as ended.
struct Watched<'a, C> {
    client: &'a mut C,
    progress: &'a Progress,
    /// What is left to write before shutting down, once that started.
    goaway: Option<Vec<u8>>,
}

impl<C: AsyncRead + Unpin> AsyncRead for Watched<'_, C> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        {
            let mut state = self.progress.state();
            if state.closing {
                return Poll::Ready(Ok(()));
            }
            state.reader = Some(cx.waker().clone());
        }
        let before = buf.filled().len();
        ready!(Pin::new(&mut *self.client).poll_read(cx, buf))?;
        let read = &buf.filled()[before..];
        self.progress.fed(|exchanges| exchanges.client_sent(read));
        Poll::Ready(Ok(()))
    }
}

impl<C: AsyncWrite + Unpin> AsyncWrite for Watched<'_, C> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = ready!(Pin::new(&mut *self.client).poll_write(cx, buf))?;
        let taken = &buf[..written];
        self.progress.fed(|exchanges| exchanges.guest_sent(taken));
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.client).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let goaway = this.goaway.get_or_insert_with(|| {
            let state = this.progress.state();
            match state.closing {
                true => state.exchanges.goaway().unwrap_or_default(),
                false => Vec::new(),
            }
        });
        while !goaway.is_empty() {
            let written = ready!(Pin::new(&mut *this.client).poll_write(cx, goaway))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            goaway.drain(..written);
        }
        Pin::new(&mut *this.client).poll_shutdown(cx)
    }
}

/// Answers the request of a connection its route found no room for 502, saying `why`: no guest
/// answers it, and only the wicket knows. False when it is no HTTP request, or none came
/// within [`HEAD_TIMEOUT`]; a tunnel's connection reads its head here, as a sole route's does.
pub async fn bad_gateway<S>(client: &mut Replay<S>, why: &str) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(HEAD_TIMEOUT, head::read(client)).await {
        Ok(Ok(head)) => {
            answer(client, &head, "502 Bad Gateway", why).await;
            true
        }
        _ => false,
    }
}

/// Answers a connection `status` with `message`, and closes it: http/1.1, or the h2 stream of
/// its first request, the connection ended after it (GOAWAY).
async fn answer<S>(client: &mut Replay<S>, head: &Head, status: &str, message: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let answer = match head {
        Head::Http1 { .. } => format!(
            "HTTP/1.1 {status}\r\ncache-control: no-store\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{message}",
            message.len()
        )
        .into_bytes(),
        Head::H2 { stream, .. } => h2_answer(*stream, status, message),
    };
    if client.write_all(&answer).await.is_err() || client.shutdown().await.is_err() {
        return;
    }
    let _ = tokio::time::timeout(LINGER, async {
        let mut discarded = [0u8; 4096];
        while client.read(&mut discarded).await.is_ok_and(|read| read > 0) {}
    })
    .await;
}

/// The frames answering `stream` `status` (its code first) with `message`: the server's SETTINGS,
/// the client's acknowledged, the stream's response, a GOAWAY after it.
fn h2_answer(stream: u32, status: &str, message: &str) -> Vec<u8> {
    let frame = |kind: u8, flags: u8, stream: u32, payload: &[u8]| {
        let len = (payload.len() as u32).to_be_bytes();
        [&len[1..], &[kind, flags], &stream.to_be_bytes(), payload].concat()
    };
    // HPACK, unindexed: :status (static index 8), its code, cache-control (24): no-store
    let code = &status.as_bytes()[..3];
    let block = [&[0x08, 0x03][..], code, &[0x0f, 0x09, 0x08], b"no-store"].concat();
    let goaway = [stream.to_be_bytes(), 0u32.to_be_bytes()].concat();
    [
        frame(0x4, 0, 0, &[]),
        frame(0x4, 0x1, 0, &[]),
        // END_HEADERS
        frame(0x1, 0x4, stream, &block),
        // END_STREAM
        frame(0x0, 0x1, stream, message.as_bytes()),
        frame(0x7, 0, 0, &goaway),
    ]
    .concat()
}

#[cfg(test)]
mod tests {
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
    use tokio::task::JoinHandle;
    use tokio::time::{Instant, sleep};

    use super::*;
    use crate::head::PREFACE;
    use crate::wire::{Read, Reader, write_close, write_data};

    const GET: &[u8] = b"GET / HTTP/1.1\r\nhost: a\r\n\r\n";
    const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi";
    /// Time asserted around a moment waited for.
    const NEAR: Duration = Duration::from_millis(100);

    /// A relay's other ends: its client's, a guest's speaking the framing.
    struct Ends {
        client: DuplexStream,
        guest: Reader<ReadHalf<DuplexStream>>,
        to_client: WriteHalf<DuplexStream>,
        relayed: JoinHandle<io::Result<()>>,
    }

    /// A relay, with `room` bytes between it and its client.
    fn relayed(room: usize) -> Ends {
        let (client, plain) = tokio::io::duplex(room);
        let (framed, guest) = tokio::io::duplex(64 * 1024);
        let (guest, to_client) = tokio::io::split(guest);
        let relayed =
            tokio::spawn(async move { relay(&mut Unread::new(plain), framed, None).await });
        Ends {
            client,
            guest: Reader::new(guest),
            to_client,
            relayed,
        }
    }

    fn near(what: &str, started: Instant, expected: Duration) {
        let waited = started.elapsed();
        assert!(
            waited + NEAR > expected && waited < expected + NEAR,
            "{what}: {waited:?}"
        );
    }

    impl Ends {
        /// The next thing the guest reads.
        async fn guest_reads(&mut self) -> Result<Vec<u8>, Read> {
            let mut buffer = vec![0; 64 * 1024];
            match self.guest.read(&mut buffer).await.unwrap() {
                Read::Data(n) => Ok(buffer[..n].to_vec()),
                other => Err(other),
            }
        }

        /// `sent` by the client, read by the guest.
        async fn request(&mut self, sent: &[u8]) {
            self.client.write_all(sent).await.unwrap();
            assert_eq!(self.guest_reads().await.as_deref(), Ok(sent));
        }

        /// `answer` by the guest, read by the client.
        async fn answer(&mut self, answer: &[u8]) {
            write_data(&mut self.to_client, answer).await.unwrap();
            let mut read = vec![0; answer.len()];
            self.client.read_exact(&mut read).await.unwrap();
            assert_eq!(read, answer);
        }

        /// The guest reads its client's end after `quiet` from now, and closes: the relay ends well,
        /// what the client read last returned.
        async fn ends_after(mut self, quiet: Duration) -> Vec<u8> {
            let started = Instant::now();
            assert_eq!(self.guest_reads().await, Err(Read::Close));
            near("the guest told", started, quiet);
            write_close(&mut self.to_client).await.unwrap();
            let mut rest = Vec::new();
            self.client.read_to_end(&mut rest).await.unwrap();
            self.relayed.await.unwrap().unwrap();
            rest
        }
    }

    /// No request in progress for a minute, whether none came or the last was answered, its client
    /// goes, an h2 one told by a GOAWAY. A request waiting on its guest stays, and a next head that
    /// did not end starts no wait over.
    #[tokio::test(start_paused = true)]
    async fn a_client_a_minute_with_no_request_in_progress_goes() {
        assert_eq!(relayed(1024).ends_after(QUIET).await, b"");

        let mut ends = relayed(1024);
        ends.request(GET).await;
        sleep(QUIET * 3).await;
        ends.answer(OK).await;
        sleep(QUIET / 2).await;
        ends.request(b"GET / HT").await;
        assert_eq!(ends.ends_after(QUIET / 2).await, b"");

        let frame = |kind: u8, flags: u8, stream: u8, payload: &[u8]| {
            let head = [0, 0, payload.len() as u8, kind, flags, 0, 0, 0, stream];
            [&head[..], payload].concat()
        };
        let mut ends = relayed(1024);
        let settings = frame(0x4, 0, 0, &[]);
        ends.request(&[PREFACE, &settings, &frame(0x1, 0x5, 3, b"x")].concat())
            .await;
        ends.answer(&[settings, frame(0x1, 0x5, 3, b"y")].concat())
            .await;
        assert_eq!(
            ends.ends_after(QUIET).await,
            frame(0x7, 0, 0, &[0, 0, 0, 3, 0, 0, 0, 0])
        );
    }

    /// A guest speaking first makes a connection verbatim: nothing tells of it but the route's
    /// `idle-ms`. A guest not closing behind its client fails the relay, as does a client not
    /// reading, whatever the protocol.
    #[tokio::test(start_paused = true)]
    async fn a_relay_stays_verbatim_and_fails_on_who_does_not_go() {
        let mut ends = relayed(1024);
        ends.answer(b"220 hello\r\n").await;
        sleep(QUIET * 3).await;
        assert!(!ends.relayed.is_finished());

        let started = Instant::now();
        let mut ends = relayed(1024);
        assert_eq!(ends.guest_reads().await, Err(Read::Close));
        let kept = ends.relayed.await.unwrap().unwrap_err();
        assert_eq!(kept.kind(), io::ErrorKind::TimedOut, "{kept}");
        near("the guest kept it", started, QUIET + LINGER);

        let started = Instant::now();
        let mut ends = relayed(16);
        write_data(&mut ends.to_client, b"220 hello, at length\r\n")
            .await
            .unwrap();
        let unread = ends.relayed.await.unwrap().unwrap_err();
        assert_eq!(unread.kind(), io::ErrorKind::TimedOut, "{unread}");
        near("not read", started, QUIET);
    }
}
