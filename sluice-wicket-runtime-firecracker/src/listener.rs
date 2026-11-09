//! `--listen`: the routes served over plain TCP as well, relayed as the tunnel's
//! connections are (no sluice server needed: a local loop for trying a rootfs).
//!
//! A connection is routed once, by its first request (`head`): the route of its
//! host -- the `Host` header, or the h2c `:authority` -- or the sole route
//! whatever the host (`sluice_wicket_tunnel::routes::Table::sole`), which reads
//! nothing first: its guest may speak first. What was read is relayed
//! verbatim, as the rest: a later request of the connection, an h2 stream or a
//! keep-alive request, stays on that route whatever its host, and no
//! `X-Forwarded-*` header is added. An unknown host is answered 404, an app held
//! until its owner confirms it 503, a request finding no room for its VM 502
//! ([`bad_gateway`]); a connection that is not
//! HTTP, or whose head runs past `head::MAX_HEAD` or takes longer than
//! [`HEAD_TIMEOUT`], is closed.
//!
//! A connection holds a slot, and a VM on a per-connection or reuse route, and no server's limits
//! stand before it, so its client gets [`QUIET`] with no request in progress (`exchange`), and to
//! take a write of an answer ([`Unread`], whatever the protocol): the guest then ends the
//! connection as for a client closing it ([`relay`]). What a request waits on is its guest, and
//! what cannot be read as HTTP is the route's `idle-ms`'s.
//!
//! All the slots taken, the next connection is not left to wait: one with no request sent whole gives
//! way, ending the same way, unless it cannot be told (`exchange::Standing`).

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

use sluice_wicket_server::accept::{self, Claim, Slot};
use sluice_wicket_server::http::{QUIET, Unread};
use sluice_wicket_tunnel::listener::{Replay, host_of};
use sluice_wicket_tunnel::routes::Routes;
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::exchange::{Exchanges, Standing};
use crate::head::{self, Head};
use crate::route::{Lease, Route};
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
                // the worker's from here on
                if let Ok(client) = TcpStream::from_std(client) {
                    serve(Replay::new(Unread::new(client)), peer, &routes, slot).await;
                }
            });
        },
    )
    .await
}

/// Serves a connection until it ended, or another wanted `slot`: while its client alone is waited
/// for, not while a VM is opened for it, which is bounded by the route's wait and boot.
async fn serve(
    mut client: Replay<Unread<TcpStream>>,
    peer: SocketAddr,
    routes: &Routes<Table>,
    slot: Slot,
) {
    let route = match routes.current().sole() {
        Some(route) => route,
        None => {
            slot.claim().yields();
            let routed = tokio::select! {
                routed = by_head(&mut client, peer, routes) => routed,
                () = slot.asked() => None,
            };
            let Some(route) = routed else {
                return;
            };
            slot.claim().holds();
            route
        }
    };
    match route.open(&peer.ip().to_string()).await {
        Ok(Lease { stream, hold }) => {
            let relayed = relay(&mut client, stream, route.idle(), &slot).await;
            // a reused clone kept before the client sees the end
            let ended = hold.end(relayed.is_ok());
            drop(client);
            // the connection is over, however long its clone takes to
            drop(slot);
            if let Err(e) = relayed {
                eprintln!("listener ({}): {e}", route.label());
            }
            ended.await;
        }
        Err(e) => {
            eprintln!("listener ({}): {e}", route.label());
            if let Some(why) = route.no_room(&e) {
                slot.claim().yields();
                tokio::select! {
                    _ = bad_gateway(&mut client, &why) => {}
                    () = slot.asked() => {}
                }
            }
        }
    }
}

/// The route of a connection's head, within [`HEAD_TIMEOUT`]; None with the connection answered
/// or not worth one.
async fn by_head<S>(
    client: &mut Replay<S>,
    peer: SocketAddr,
    routes: &Routes<Table>,
) -> Option<Arc<Route>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let head = match tokio::time::timeout(HEAD_TIMEOUT, head::read(client)).await {
        Ok(Ok(head)) => head,
        Ok(Err(e)) => {
            eprintln!("listener conn {peer}: {e}");
            return None;
        }
        Err(_) => {
            let timeout = HEAD_TIMEOUT.as_secs();
            eprintln!("listener conn {peer}: no request head within {timeout}s");
            return None;
        }
    };
    let table = routes.current();
    if let Some((name, owner)) = table.held_at(host_of(head.host())) {
        let held = format!("the app '{name}' of {owner} is held until its owner confirms it");
        answer(client, &head, "503 Service Unavailable", &held).await;
        return None;
    }
    let route = table.by_host(host_of(head.host()));
    if route.is_none() {
        let message = format!("no firecracker route for host '{}'", head.host());
        answer(client, &head, "404 Not Found", &message).await;
    }
    route
}

/// [`wire::relay`] for a client left [`QUIET`] with no request in progress, or giving way to one
/// wanting `slot`: the guest is told as of a client closing and closes in turn, so a reused clone
/// is kept, and the client sees the end (an h2 client a GOAWAY first). A guest keeping the
/// connection [`LINGER`] more fails it.
async fn relay<C, G>(
    client: &mut C,
    guest: G,
    idle: Option<Duration>,
    slot: &Slot,
) -> io::Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
    G: AsyncRead + AsyncWrite + Unpin,
{
    let progress = Progress::new(slot.claim().clone());
    let watched = Watched {
        client,
        progress: &progress,
        goaway: None,
    };
    tokio::select! {
        relayed = wire::relay(watched, guest, idle) => relayed,
        e = progress.ending(slot) => Err(e),
    }
}

/// A relayed connection's exchanges, and what becomes of its client once they wait on it too long
/// or another connection wants its slot.
struct Progress {
    state: Mutex<State>,
    /// Whether the connection waits on its client alone.
    waiting: watch::Sender<bool>,
    claim: Claim,
}

struct State {
    exchanges: Exchanges,
    /// What the claim was last told.
    standing: Standing,
    /// Reading the client is over: the relay reads its end.
    closing: bool,
    reader: Option<Waker>,
}

impl Progress {
    /// With `claim` holding, as for a head read to route the connection, read again first: it
    /// goes by what the first read of the client tells.
    fn new(claim: Claim) -> Progress {
        Progress {
            state: Mutex::new(State {
                exchanges: Exchanges::default(),
                standing: Standing::Busy,
                closing: false,
                reader: None,
            }),
            waiting: watch::Sender::new(true),
            claim,
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("no panic holding it")
    }

    fn fed(&self, feed: impl FnOnce(&mut Exchanges)) {
        let mut state = self.state();
        feed(&mut state.exchanges);
        let standing = state.exchanges.standing();
        self.waiting.send_if_modified(|now| {
            let waiting = standing == Standing::Quiet;
            let changed = *now != waiting;
            *now = waiting;
            changed
        });
        // only on a change: a client dripping stays the one waiting longest
        if standing != state.standing {
            state.standing = standing;
            match standing {
                Standing::Busy => self.claim.holds(),
                Standing::Quiet | Standing::Sending => self.claim.yields(),
            }
        }
    }

    /// Ends the client's side once the connection waited on it for [`QUIET`] straight, or another
    /// wanted `slot`; resolves with the error failing a relay that did not end [`LINGER`] after.
    async fn ending(&self, slot: &Slot) -> io::Error {
        let why = tokio::select! {
            () = self.left_waiting() => "its client left waiting",
            () = slot.asked() => "another wanted its slot",
        };
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
            format!("its guest kept a connection {why}"),
        )
    }

    /// Resolves once the connection waited on its client for [`QUIET`] straight.
    async fn left_waiting(&self) {
        let mut waiting = self.waiting.subscribe();
        loop {
            // failing only once the sender, ours, is gone
            let _ = waiting.wait_for(|waiting| *waiting).await;
            if tokio::time::timeout(QUIET, waiting.changed())
                .await
                .is_err()
            {
                return;
            }
        }
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
        let read = Pin::new(&mut *self.client).poll_read(cx, buf);
        if read.is_pending() {
            // nothing read to route the connection: its claim goes by what it stands at
            self.progress.fed(|_| {});
        }
        ready!(read)?;
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
    use std::net::{IpAddr, Ipv4Addr};

    use sluice_wicket_server::accept::{FRESH, Open};
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
    use tokio::task::JoinHandle;
    use tokio::time::{Instant, sleep, timeout};

    use super::*;
    use crate::head::PREFACE;
    use crate::wire::{Read, Reader, write_close, write_data};

    const GET: &[u8] = b"GET / HTTP/1.1\r\nhost: a\r\n\r\n";
    const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi";
    const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    /// Time asserted around a moment waited for.
    const NEAR: Duration = Duration::from_millis(100);

    /// A relay's other ends: its client's, a guest's speaking the framing.
    struct Ends {
        client: DuplexStream,
        guest: Reader<ReadHalf<DuplexStream>>,
        to_client: WriteHalf<DuplexStream>,
        relayed: JoinHandle<io::Result<()>>,
        /// The connections open, this relay's among them.
        open: Arc<Open>,
    }

    /// A relay, with `room` bytes between it and its client, the only connection open.
    fn relayed(room: usize) -> Ends {
        relayed_of(&Open::new(1), room)
    }

    /// As [`relayed`], among the connections of `open`.
    fn relayed_of(open: &Arc<Open>, room: usize) -> Ends {
        let (client, plain) = tokio::io::duplex(room);
        let (framed, guest) = tokio::io::duplex(64 * 1024);
        let (guest, to_client) = tokio::io::split(guest);
        let slots = open.clone();
        let relayed = tokio::spawn(async move {
            let slot = slots.place(LOCALHOST).await;
            relay(&mut Unread::new(plain), framed, None, &slot).await
        });
        Ends {
            client,
            guest: Reader::new(guest),
            to_client,
            relayed,
            open: open.clone(),
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

        /// One more connection wanting a slot, those there no longer just accepted.
        async fn wanted(&self) -> JoinHandle<Slot> {
            sleep(FRESH).await;
            let open = self.open.clone();
            tokio::spawn(async move { open.place(LOCALHOST).await })
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

    /// All the slots taken, a connection gives way with no request in progress or one whose body
    /// its client did not finish, its guest told as of its client closing and an h2 client a GOAWAY;
    /// a guest keeping it fails it. A request sent whole keeps its slot until its answer ended,
    /// and what cannot be told keeps it for good.
    #[tokio::test(start_paused = true)]
    async fn a_connection_gives_way_unless_a_request_was_sent_whole() {
        const POST: &[u8] = b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 5\r\n\r\nab";
        for sent in [&b""[..], b"GET / HT", POST] {
            let mut ends = relayed(1024);
            if !sent.is_empty() {
                ends.request(sent).await;
            }
            let wanted = ends.wanted().await;
            assert_eq!(ends.ends_after(Duration::ZERO).await, b"");
            timeout(NEAR, wanted).await.expect("kept").unwrap();
        }

        let frame = |kind: u8, payload: &[u8]| {
            let head = [0, 0, payload.len() as u8, kind, 0, 0, 0, 0, 0];
            [&head[..], payload].concat()
        };
        let mut ends = relayed(1024);
        let settings = frame(0x4, &[]);
        ends.request(&[PREFACE, &settings].concat()).await;
        ends.answer(&settings).await;
        let _wanted = ends.wanted().await;
        assert_eq!(ends.ends_after(Duration::ZERO).await, frame(0x7, &[0; 8]));

        let mut ends = relayed(1024);
        ends.request(POST).await;
        let wanted = ends.wanted().await;
        let started = Instant::now();
        assert_eq!(ends.guest_reads().await, Err(Read::Close));
        let kept = ends.relayed.await.unwrap().unwrap_err();
        assert_eq!(kept.kind(), io::ErrorKind::TimedOut, "{kept}");
        near("the guest kept it", started, LINGER);
        timeout(NEAR, wanted).await.expect("kept").unwrap();

        // dripped bytes make no one the last to have waited
        let open = Open::new(2);
        let mut dripping = relayed_of(&open, 1024);
        dripping.request(POST).await;
        sleep(NEAR).await;
        let mut other = relayed_of(&open, 1024);
        other.request(POST).await;
        sleep(NEAR).await;
        dripping.request(b"c").await;
        let _wanted = dripping.wanted().await;
        assert_eq!(dripping.guest_reads().await, Err(Read::Close));
        sleep(NEAR).await;
        assert!(!other.relayed.is_finished());

        let mut ends = relayed(1024);
        ends.request(GET).await;
        let wanted = ends.wanted().await;
        sleep(QUIET * 3).await;
        assert!(!wanted.is_finished());
        ends.answer(OK).await;
        assert_eq!(ends.ends_after(Duration::ZERO).await, b"");
        timeout(NEAR, wanted).await.expect("kept").unwrap();

        // what cannot be told
        let mut ends = relayed(1024);
        ends.answer(b"220 hello\r\n").await;
        let wanted = ends.wanted().await;
        sleep(QUIET * 3).await;
        assert!(!wanted.is_finished() && !ends.relayed.is_finished());
    }
}
