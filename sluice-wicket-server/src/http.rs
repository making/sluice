//! An HTTP/1.1 connection no client holds by going quiet, whatever it stopped
//! doing: sending the next request's head ([`serve`]), a request's body
//! ([`Body`]), or reading the answer. A client still sending or reading,
//! however slowly, and one awaiting its answer (a long poll) keep theirs.
//!
//! All the listener's slots taken, a connection gives way ([`Slot::asked`])
//! unless its request was admitted ([`Body::admit`]): a request of no one known
//! holds nothing against someone else's, however it keeps moving.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use hyper::body::{Bytes, Frame, Incoming, SizeHint};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

use crate::accept::{Claim, Slot};

/// How long a client may leave a connection waiting for it. An idle connection
/// closed so is one a client opens again, as with any server's keep-alive.
pub const QUIET: Duration = Duration::from_secs(60);

/// How long the wait on a client has lasted: from the first poll finding it
/// waited for, until [`Wait::over`].
#[derive(Default)]
struct Wait(Option<Pin<Box<Sleep>>>);

impl Wait {
    fn over(&mut self) {
        self.0 = None;
    }

    /// Ready with the error ending the wait, once it lasted [`QUIET`].
    fn lasted(&mut self, cx: &mut Context<'_>, what: &str) -> Poll<io::Error> {
        let sleep = self
            .0
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(QUIET)));
        ready!(sleep.as_mut().poll(cx));
        Poll::Ready(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("the client {what} for {}s", QUIET.as_secs()),
        ))
    }
}

/// A request's body, failing once its client sent none of it for [`QUIET`]
/// while it was read. A handler reading it answers as for any broken body, and
/// the connection closes behind the answer, the body unfinished.
pub struct Body {
    incoming: Incoming,
    wait: Wait,
    claim: Claim,
}

impl Body {
    /// The request is of someone the handler knows: the connection keeps its
    /// slot until the answer is written, however long that takes. Any other
    /// gives way when all are taken: its answer is lost, its handler goes on as
    /// for a client leaving.
    pub fn admit(&self) {
        self.claim.holds();
    }
}

impl hyper::body::Body for Body {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = &mut *self;
        match Pin::new(&mut this.incoming).poll_frame(cx) {
            Poll::Ready(frame) => {
                this.wait.over();
                Poll::Ready(frame.map(|frame| frame.map_err(io::Error::other)))
            }
            Poll::Pending => this
                .wait
                .lasted(cx, "sent nothing of a body")
                .map(|quiet| Some(Err(quiet))),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.incoming.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.incoming.size_hint()
    }
}

/// A connection's answer being written, to the last byte: an answer's body ends
/// with its last frame still to be written, a whole answer's if it has but one.
struct Writing {
    claim: Claim,
    body_ended: AtomicBool,
}

impl Writing {
    /// The stream took all it was given: an ended answer's connection yields.
    fn flushed(&self) {
        if self.body_ended.swap(false, Ordering::Relaxed) {
            self.claim.yields();
        }
    }
}

/// An answer's body, telling when it ended.
struct Answer<B> {
    body: Pin<Box<B>>,
    writing: Arc<Writing>,
}

impl<B> Drop for Answer<B> {
    fn drop(&mut self) {
        self.writing.body_ended.store(true, Ordering::Relaxed);
    }
}

impl<B: hyper::body::Body> hyper::body::Body for Answer<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        self.body.as_mut().poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// A connection's stream, failing a write its client took none of for
/// [`QUIET`]. TCP keepalive does not tell: the kernel of a client not reading
/// answers its probes.
pub struct Unread<I> {
    io: I,
    wait: Wait,
    writing: Option<Arc<Writing>>,
}

impl<I> Unread<I> {
    pub fn new(io: I) -> Unread<I> {
        Unread {
            io,
            wait: Wait::default(),
            writing: None,
        }
    }

    fn writing<T>(
        &mut self,
        cx: &mut Context<'_>,
        written: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if written.is_ready() {
            self.wait.over();
            return written;
        }
        self.wait
            .lasted(cx, "read nothing of an answer")
            .map(|quiet| Err(quiet))
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for Unread<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for Unread<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.io).poll_write(cx, buf);
        self.writing(cx, written)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.io).poll_write_vectored(cx, bufs);
        self.writing(cx, written)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let flushed = Pin::new(&mut self.io).poll_flush(cx);
        if flushed.is_ready()
            && let Some(writing) = &self.writing
        {
            writing.flushed();
        }
        self.writing(cx, flushed)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let shut = Pin::new(&mut self.io).poll_shutdown(cx);
        self.writing(cx, shut)
    }
}

/// Serves HTTP/1.1 on `io` until it ended: its client left, stopped sending a
/// request's head for [`QUIET`], the wait for the next one between requests
/// included, or another connection wanted `slot`. A handshake before it needs
/// its own bound of [`QUIET`], and to give way too.
pub async fn serve<I, H, F, B>(io: I, slot: Slot, handler: H)
where
    I: AsyncRead + AsyncWrite + Unpin,
    H: Fn(hyper::Request<Body>) -> F,
    F: Future<Output = hyper::Response<B>>,
    B: hyper::body::Body + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let claim = slot.claim();
    claim.yields();
    let writing = Arc::new(Writing {
        claim: claim.clone(),
        body_ended: AtomicBool::new(false),
    });
    let io = Unread {
        writing: Some(writing.clone()),
        ..Unread::new(io)
    };
    let service = hyper::service::service_fn(|request: hyper::Request<Incoming>| {
        // no longer the one that waited longest
        claim.yields();
        let answer = handler(request.map(|incoming| Body {
            incoming,
            wait: Wait::default(),
            claim: claim.clone(),
        }));
        let writing = writing.clone();
        async move {
            Ok::<_, Infallible>(answer.await.map(|body| Answer {
                body: Box::pin(body),
                writing,
            }))
        }
    });
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(QUIET)
        .serve_connection(TokioIo::new(io), service);
    tokio::select! {
        // a client leaving, or left behind, is no news
        _ = connection => {}
        () = slot.asked() => {}
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use http_body_util::{BodyExt as _, Full};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
    use tokio::task::{JoinHandle, yield_now};
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::accept::Open;

    /// How far from [`QUIET`] a bound is taken to expire.
    const NEAR: Duration = Duration::from_secs(1);
    const LARGE: usize = 1 << 20;

    async fn answer(request: hyper::Request<Body>) -> hyper::Response<Full<Bytes>> {
        if request.uri().path().starts_with("/admitted") {
            request.body().admit();
        }
        let (status, body) = match request.uri().path() {
            "/poll" | "/admitted/poll" => {
                sleep(QUIET * 3).await;
                (200, Bytes::from_static(b"changed"))
            }
            "/large" | "/admitted/large" => (200, Bytes::from(vec![b'x'; LARGE])),
            _ => match request.into_body().collect().await {
                Ok(body) => (200, Bytes::from(body.to_bytes().len().to_string())),
                Err(e) => (400, Bytes::from(e.to_string())),
            },
        };
        hyper::Response::builder()
            .status(status)
            .body(Full::new(body))
            .unwrap()
    }

    fn connection() -> (DuplexStream, JoinHandle<()>) {
        let (client, server) = tokio::io::duplex(4096);
        let served = tokio::spawn(async move {
            let slot = Open::new(1).place(Ipv4Addr::LOCALHOST.into()).await;
            serve(server, slot, answer).await
        });
        (client, served)
    }

    /// The answer next on `client`, each read `pause` after the one before: its
    /// head, its body.
    async fn answered(client: &mut DuplexStream, pause: Duration) -> (String, Vec<u8>) {
        let mut read = Vec::new();
        let mut more = async |read: &mut Vec<u8>| {
            sleep(pause).await;
            assert_ne!(client.read_buf(read).await.unwrap(), 0, "cut short");
        };
        let end = loop {
            if let Some(at) = read.windows(4).position(|window| window == b"\r\n\r\n") {
                break at + 4;
            }
            more(&mut read).await;
        };
        let head = String::from_utf8_lossy(&read[..end]).to_string();
        let length: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .expect("a length")
            .parse()
            .unwrap();
        while read.len() < end + length {
            more(&mut read).await;
        }
        (head, read.split_off(end))
    }

    fn get(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: t\r\n\r\n")
    }

    /// A client that stopped sending or reading, at any point of a request,
    /// loses the connection after the bound and not before.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_client_loses_its_connection() {
        let next_request = get("/");
        let upload = "POST / HTTP/1.1\r\nhost: t\r\ncontent-length: 5\r\n\r\nab";
        let unread = get("/large");
        for sent in ["", "GET / HTTP/1.1\r\nhost", &next_request, upload, &unread] {
            let (mut client, mut served) = connection();
            client.write_all(sent.as_bytes()).await.unwrap();
            if sent == next_request {
                let (head, _) = answered(&mut client, Duration::ZERO).await;
                assert!(head.contains("200"), "{head}");
            }
            assert!(
                timeout(QUIET - NEAR, &mut served).await.is_err(),
                "ended early after {sent:?}"
            );
            timeout(NEAR * 2, &mut served)
                .await
                .unwrap_or_else(|_| panic!("still open after {sent:?}"))
                .unwrap();
            if sent == upload {
                let (head, body) = answered(&mut client, Duration::ZERO).await;
                assert!(head.contains("400"), "{head}");
                let body = String::from_utf8_lossy(&body);
                assert!(body.contains("sent nothing"), "{body}");
            }
        }
    }

    /// A client awaiting its answer, sending or reading slowly keeps its
    /// connection past the bound, and may go on with it.
    #[tokio::test(start_paused = true)]
    async fn a_client_at_work_keeps_its_connection() {
        let pause = QUIET - NEAR;
        let (mut client, served) = connection();

        client.write_all(get("/poll").as_bytes()).await.unwrap();
        assert_eq!(answered(&mut client, Duration::ZERO).await.1, b"changed");

        client
            .write_all(b"POST / HTTP/1.1\r\nhost: t\r\ncontent-length: 3\r\n\r\n")
            .await
            .unwrap();
        for byte in [b"a", b"b", b"c"] {
            sleep(pause).await;
            client.write_all(byte).await.unwrap();
        }
        assert_eq!(answered(&mut client, Duration::ZERO).await.1, b"3");

        client.write_all(get("/large").as_bytes()).await.unwrap();
        assert_eq!(answered(&mut client, pause).await.1.len(), LARGE);

        client.write_all(get("/").as_bytes()).await.unwrap();
        assert_eq!(answered(&mut client, Duration::ZERO).await.1, b"0");
        assert!(!served.is_finished());
    }

    /// Whatever its client is at, a connection gives way to one wanting its slot
    /// at once, unless its request was admitted; that one goes once its answer
    /// was read.
    #[tokio::test(start_paused = true)]
    async fn a_connection_wanted_gives_way_unless_admitted() {
        let drip = "POST / HTTP/1.1\r\nhost: t\r\ncontent-length: 5\r\n\r\nab";
        let partial = "GET / HTTP/1.1\r\nhost";
        let served = get("/");
        let unadmitted = get("/poll");
        let unread = get("/large");
        let admitted = get("/admitted/poll");
        let reading = get("/admitted/large");
        for (sent, gives_way) in [
            ("", true),
            (partial, true),
            (served.as_str(), true),
            (drip, true),
            (unadmitted.as_str(), true),
            (unread.as_str(), true),
            (admitted.as_str(), false),
            (reading.as_str(), false),
        ] {
            let open = Open::new(1);
            let (mut client, io) = tokio::io::duplex(4096);
            let slot = open.place(Ipv4Addr::LOCALHOST.into()).await;
            tokio::spawn(serve(io, slot, answer));
            client.write_all(sent.as_bytes()).await.unwrap();
            if sent == served {
                answered(&mut client, Duration::ZERO).await;
            }
            // the request reached its handler
            yield_now().await;

            let wanted = open.place(Ipv4Addr::LOCALHOST.into());
            if gives_way {
                timeout(NEAR, wanted)
                    .await
                    .unwrap_or_else(|_| panic!("kept after {sent:?}"));
                continue;
            }
            let mut wanted = Box::pin(wanted);
            assert!(
                timeout(QUIET - NEAR, &mut wanted).await.is_err(),
                "gave way after {sent:?}"
            );
            let (head, _) = answered(&mut client, Duration::ZERO).await;
            assert!(head.contains("200"), "{head}");
            timeout(NEAR, wanted)
                .await
                .unwrap_or_else(|_| panic!("kept after the answer to {sent:?}"));
        }
    }
}
