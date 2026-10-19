//! The virtual connections of one tunnel stream: dispatch of the server's
//! frames, per-connection credit flow control, the scheduling of outbound
//! frames ([`Outbound`]), and the byte stream adapter hyper serves each
//! connection over.
//!
//! The flow control follows the protocol of the Java `VirtualConnection`
//! (`sluice-proto`). Every connection is windowed; a CONNECT without a
//! window (a server predating the protocol) is answered with ERROR
//! [`SERVER_WINDOW_REQUIRED`] and neither registered nor served:
//!
//! - the server may have at most [`WINDOW`] payload bytes in flight beyond what
//!   this side granted back. Payload past it fails only that connection
//!   (ERROR [`WINDOW_EXCEEDED`]); the receive loop never waits on a windowed
//!   connection. The payload waits in the connection's [`Inbox`], coalesced
//!   into segments of at most [`SEGMENT`], so its memory stays near the window
//!   whatever the size of the frames;
//! - bytes handed to hyper are granted back (WindowUpdate) once they reach
//!   half the window;
//! - hyper's writes send only against the server's credit; a write without
//!   credit waits on its own task until a grant, the server's ERROR or the
//!   stream's end, and tells the server once per wait (WindowUpdate
//!   `blocked`), so that the server may grow its window;
//! - the server's request to give back the credit past `reclaim_above` is
//!   answered by giving it up and announcing it (WindowUpdate `released`);
//! - a connection ending before the server's CLOSE / ERROR arrived sends ERROR
//!   ("connection closed"), since the payload the server still sends would be
//!   dropped and never granted.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::stream::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::pb::frame::Body;
use crate::pb::{Close, Connect, Data, Error, Frame, WindowUpdate};
use crate::wasm_host::{self, Handler};

/// Receive window of every connection, announced in `Advertise`;
/// the Java side's `VirtualConnection.DEFAULT_WINDOW`.
pub const WINDOW: u32 = 1024 * 1024;

/// ERROR message for payload beyond the granted window; the Java side's
/// `VirtualConnection.WINDOW_EXCEEDED`.
pub const WINDOW_EXCEEDED: &str = "flow control window exceeded";

/// ERROR message of a connection ending while the server may still send.
pub const CONNECTION_CLOSED: &str = "connection closed";

/// Failure of every connection still registered when the stream ends.
const STREAM_CLOSED: &str = "tunnel stream closed";

/// ERROR message of a CONNECT beyond the connection limit; the Java side's
/// `ConnectionLimit.LIMIT_REACHED`.
pub const LIMIT_REACHED: &str = "connection limit reached";

/// ERROR message answering a CONNECT that announces no receive window (a
/// server predating per-connection flow control); the Java side's
/// `NodeConnection.SERVER_WINDOW_REQUIRED`.
pub const SERVER_WINDOW_REQUIRED: &str = "the server announced no per-connection receive window \
     (Connect.connection_window); this client requires sluice-server 046cd21 (2026-10-12) or later";

/// Read error of a connection the server failed.
const ABORTED: &str = "connection aborted by the tunnel peer";

/// Upper bound for a single DATA frame emitted by the hyper write path.
const CHUNK: usize = 16 * 1024;

/// The most payload one receive segment holds;
/// the Java side's `VirtualConnection.SEGMENT_SIZE`.
const SEGMENT: usize = 16 * 1024;

/// The most segments the inbox of a connection with `window` holds:
/// segments smaller than [`SEGMENT`] at least double one after another, so at
/// most log2([`SEGMENT`]) of them; every full-size one but the head and the
/// tail holds only unread payload, which the window bounds.
#[cfg(test)]
const fn max_segments(window: usize) -> usize {
    window / SEGMENT + SEGMENT.trailing_zeros() as usize + 2
}

/// DATA frames one connection may have queued on the outbound stream; its
/// writer waits beyond it. 16 x [`CHUNK`] = 256KiB.
pub(crate) const DATA_QUEUE_CAPACITY: usize = 16;

/// Write error after the connection's CLOSE or ERROR was queued.
const WRITE_CLOSED: &str = "connection closed for writing";

// ---------------------------------------------------------------------------
// Outbound

/// The frames queued for the request stream, scheduled like the Java
/// `SessionSender`:
///
/// - control frames (Advertise, WindowUpdate, ERRORs no DATA precedes) wait
///   on an unbounded queue that is always sent first, so queueing one never
///   waits;
/// - DATA, CLOSE and the connection-end ERROR wait on their connection's
///   queue, in order. The stream takes one frame per turn from the head of a
///   ring of the connections with pending frames and re-appends the
///   connection while frames remain, so besides control frames one
///   connection's backlog delays another's frame by one frame per turn, not
///   by the backlog;
/// - a connection's queue holds at most [`DATA_QUEUE_CAPACITY`] DATA frames.
///   Only that connection's writer waits at the cap, and only a DATA frame
///   taken from that queue, a failure of the connection or the end of the
///   stream wakes it. CLOSE / ERROR bypass the cap;
/// - a queue is dropped as soon as it is empty, so an idle connection costs
///   nothing; a later frame starts a new one;
/// - once the stream ends (the teardown, or gRPC dropping the request
///   stream), queued frames are dropped, later ones too, and writers waiting
///   at the cap fail;
/// - the stall clock ([`Outbound::stalled`]) runs while connection frames
///   wait and the stream takes none of them: the request stream is polled
///   only while gRPC's flow control has room, so a server that stops
///   reading stops the polls. Control frames are no progress; with no
///   connection frame queued nothing is stalled.
#[derive(Clone)]
pub struct Outbound {
    queues: Arc<Mutex<Queues>>,
}

#[derive(Default)]
struct Queues {
    control: VecDeque<Frame>,
    /// Live (non-empty) connection queues by conn id.
    connections: HashMap<i64, ConnectionQueue>,
    /// The ids of the live queues in serve order; each exactly once.
    ring: VecDeque<i64>,
    /// The request stream waiting for a frame.
    stream: Option<Waker>,
    closed: bool,
    /// Since when connection frames wait with none taken: set when a frame
    /// joins an empty ring, moved on by each connection frame taken while
    /// others wait, cleared once none waits.
    waiting_since: Option<Instant>,
}

#[derive(Default)]
struct ConnectionQueue {
    frames: VecDeque<Frame>,
    /// DATA frames within `frames`.
    data: usize,
    /// The writer waiting at the cap.
    writer: Option<Waker>,
    /// The ended connection's slot, freed with the queue.
    permit: Option<Permit>,
}

/// The tunnel stream was closed: no frame can be sent any more.
#[derive(Debug)]
struct Closed;

impl Outbound {
    /// The queues and the request stream draining them.
    pub fn new() -> (Outbound, Frames) {
        let queues = Arc::new(Mutex::new(Queues::default()));
        (
            Outbound {
                queues: queues.clone(),
            },
            Frames(Outbound { queues }),
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Queues> {
        self.queues.lock().unwrap()
    }

    /// Queues a control frame; never waits. Dropped once the stream is gone.
    pub fn control(&self, frame: Frame) {
        let waker = {
            let mut queues = self.lock();
            if queues.closed {
                return;
            }
            queues.control.push_back(frame);
            queues.stream.take()
        };
        wake(waker);
    }

    /// Ready once `conn_id`'s queue has room for a DATA frame; otherwise
    /// parks the writer until a DATA frame of the queue leaves, the
    /// connection fails ([`Outbound::wake_writer`]) or the stream ends.
    fn poll_data_slot(&self, conn_id: i64, cx: &mut Context<'_>) -> Poll<Result<(), Closed>> {
        let mut queues = self.lock();
        if queues.closed {
            return Poll::Ready(Err(Closed));
        }
        match queues.connections.get_mut(&conn_id) {
            Some(queue) if queue.data >= DATA_QUEUE_CAPACITY => {
                queue.writer = Some(cx.waker().clone());
                Poll::Pending
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    /// Queues a DATA frame for `conn_id`. The cap is the caller's, through
    /// [`Outbound::poll_data_slot`]: a connection has a single writer.
    fn data(&self, conn_id: i64, frame: Frame) {
        self.push(conn_id, frame, true);
    }

    /// Queues CLOSE / ERROR for `conn_id` after its DATA; never waits.
    fn marker(&self, conn_id: i64, frame: Frame) {
        self.push(conn_id, frame, false);
    }

    fn push(&self, conn_id: i64, frame: Frame, data: bool) {
        let waker = {
            let mut queues = self.lock();
            if queues.closed {
                return;
            }
            if queues.ring.is_empty() {
                queues.waiting_since = Some(Instant::now());
            }
            let queue = queues.connections.entry(conn_id).or_default();
            let joins = queue.frames.is_empty();
            queue.frames.push_back(frame);
            if data {
                queue.data += 1;
            }
            if joins {
                queues.ring.push_back(conn_id);
            }
            queues.stream.take()
        };
        wake(waker);
    }

    /// Keeps an ended connection's slot until its queued frames left, so the
    /// connection limit also bounds what ended connections still queue.
    /// Freed at once when nothing is queued or the stream ended.
    fn hold(&self, conn_id: i64, permit: Permit) {
        let mut queues = self.lock();
        if let Some(queue) = queues.connections.get_mut(&conn_id) {
            queue.permit = Some(permit);
        }
    }

    /// Wakes `conn_id`'s writer if it waits at the cap, so it sees the
    /// connection's failure.
    fn wake_writer(&self, conn_id: i64) {
        let waker = self
            .lock()
            .connections
            .get_mut(&conn_id)
            .and_then(|queue| queue.writer.take());
        wake(waker);
    }

    /// Ends the stream: drops every queued frame and wakes the stream and
    /// every writer waiting at a cap. Idempotent.
    pub fn shutdown(&self) {
        let (stream, writers) = {
            let mut queues = self.lock();
            queues.closed = true;
            queues.control.clear();
            queues.ring.clear();
            queues.waiting_since = None;
            let writers: Vec<Waker> = queues
                .connections
                .drain()
                .filter_map(|(_, queue)| queue.writer)
                .collect();
            (queues.stream.take(), writers)
        };
        wake(stream);
        writers.into_iter().for_each(Waker::wake);
    }

    /// The next frame: control first, then one frame of the connection at
    /// the head of the ring. Pending while nothing is queued; None once the
    /// stream ended.
    fn poll_frame(&self, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        let (frame, writer) = {
            let mut queues = self.lock();
            if queues.closed {
                return Poll::Ready(None);
            }
            if let Some(frame) = queues.control.pop_front() {
                return Poll::Ready(Some(frame));
            }
            let Some(conn_id) = queues.ring.pop_front() else {
                queues.stream = Some(cx.waker().clone());
                return Poll::Pending;
            };
            let queue = queues
                .connections
                .get_mut(&conn_id)
                .expect("a ringed queue is live");
            let frame = queue.frames.pop_front().expect("a live queue holds frames");
            let mut writer = None;
            if matches!(frame.body, Some(Body::Data(_))) {
                queue.data -= 1;
                writer = queue.writer.take();
            }
            if queue.frames.is_empty() {
                queues.connections.remove(&conn_id);
            } else {
                queues.ring.push_back(conn_id);
            }
            // a frame taken is progress: the others wait from now on
            queues.waiting_since = (!queues.ring.is_empty()).then(Instant::now);
            (frame, writer)
        };
        wake(writer);
        Poll::Ready(Some(frame))
    }

    /// Resolves once connection frames have waited `timeout` with none
    /// taken by the request stream, with how long they waited; never with a
    /// zero `timeout`. Every wake re-reads the clock, so a stall that ended
    /// in between does not count.
    pub async fn stalled(&self, timeout: Duration) -> Duration {
        if timeout.is_zero() {
            return std::future::pending().await;
        }
        loop {
            let since = self.lock().waiting_since;
            match since {
                Some(since) => {
                    let waited = since.elapsed();
                    if waited >= timeout {
                        return waited;
                    }
                    tokio::time::sleep_until(since + timeout).await;
                }
                // nothing waits: look again in a while, a stall needs the
                // whole timeout anyway
                None => tokio::time::sleep(timeout.min(Duration::from_secs(1))).await,
            }
        }
    }

    /// The live connection queues.
    #[cfg(test)]
    fn queues(&self) -> usize {
        self.lock().connections.len()
    }

    /// The DATA frames queued for `conn_id`.
    #[cfg(test)]
    fn queued_data(&self, conn_id: i64) -> usize {
        self.lock()
            .connections
            .get(&conn_id)
            .map_or(0, |queue| queue.data)
    }
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// The request stream of one stream attempt. Dropping it (gRPC is done with
/// the request) ends the stream as [`Outbound::shutdown`] does.
pub struct Frames(Outbound);

impl Stream for Frames {
    type Item = Frame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        self.0.poll_frame(cx)
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

fn error_frame(conn_id: i64, message: &str) -> Frame {
    Frame {
        body: Some(Body::Error(Error {
            conn_id,
            message: message.to_string(),
        })),
    }
}

fn close_frame(conn_id: i64) -> Frame {
    Frame {
        body: Some(Body::Close(Close { conn_id })),
    }
}

// ---------------------------------------------------------------------------
// Connection limit

/// A cap on the connections open at once over every stream of the process.
/// A connection buffers at most [`WINDOW`] of the server's payload
/// in at most `max_segments` segments of its [`Inbox`], and
/// [`DATA_QUEUE_CAPACITY`] outbound DATA frames, so the cap bounds the total.
pub struct Limit {
    max: usize,
    open: AtomicUsize,
}

impl Limit {
    pub fn new(max: usize) -> Arc<Limit> {
        Arc::new(Limit {
            max,
            open: AtomicUsize::new(0),
        })
    }

    /// A slot for one more connection, or None when `max` are open.
    pub fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        self.open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (open < self.max).then_some(open + 1)
            })
            .ok()
            .map(|_| Permit(self.clone()))
    }

    /// The permits currently held.
    #[cfg(test)]
    pub fn open(&self) -> usize {
        self.open.load(Ordering::Acquire)
    }
}

/// The slot of one connection, freed when dropped: held by its byte stream,
/// which owns its buffered payload, then by its end, then by its outbound
/// queue until the last queued frame left ([`Outbound::hold`]).
pub struct Permit(Arc<Limit>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// Per-connection flow state

/// State shared by the dispatch and the connection's reader / writer.
struct Flow {
    /// Receive window announced to the server.
    window: u32,
    state: Mutex<FlowState>,
    /// Cancelled once the connection failed: the server's ERROR or a local
    /// reset. The serve task watches it, since hyper may not be reading.
    failed: CancellationToken,
}

#[derive(Default)]
struct FlowState {
    /// Payload the server still accepts.
    credit: u64,
    /// Payload received minus payload granted back.
    outstanding: u64,
    /// The server sent CLOSE.
    remote_closed: bool,
    /// The server sent ERROR.
    remote_failed: bool,
    /// Why writes fail: the server's ERROR, a violation, the stream's end.
    failure: Option<String>,
    /// Torn down locally (violation, stream end): the reader fails at once,
    /// dropping what is buffered, and no further frame is sent.
    reset: bool,
    close_sent: bool,
    error_sent: bool,
    /// The writer waiting for credit.
    writer: Option<Waker>,
    /// The server was told the writer waits for credit; cleared by a grant.
    blocked_sent: bool,
    /// The server's payload not yet read.
    inbox: Inbox,
    /// How the inbound stream ends once the inbox is read: the server's first
    /// CLOSE or ERROR. Payload after it is dropped.
    remote_end: Option<RemoteEnd>,
    /// The reader waiting for payload or the end.
    reader: Option<Waker>,
    /// Test hook: the times the reader waited.
    #[cfg(test)]
    reader_waits: usize,
}

#[derive(Clone, Copy)]
enum RemoteEnd {
    Closed,
    Failed,
}

/// The payload of a connection in arrival order, in segments of at
/// most [`SEGMENT`]: small frames are coalesced and large ones split, so
/// neither the per-segment overhead nor the read part of the head segment
/// grows with the way the server frames its payload. At most
/// `window + 2 x SEGMENT` of capacity in `max_segments` segments: every
/// segment but the tail is full, and the unread payload is within the window.
#[derive(Default)]
struct Inbox {
    segments: VecDeque<Vec<u8>>,
    /// Read offset in the head segment.
    head: usize,
}

impl Inbox {
    /// Fills the tail segment's room, then appends segments of at most
    /// [`SEGMENT`], each at least twice the capacity of the one before it (up
    /// to the segment size) or the rest of the payload. The first segment of
    /// an empty inbox is the payload itself when it fits one: a reader that
    /// keeps up costs no copy here.
    fn push(&mut self, payload: Vec<u8>) {
        if self.segments.is_empty() && payload.capacity() <= SEGMENT {
            self.segments.push_back(payload);
            return;
        }
        let mut data = &payload[..];
        while !data.is_empty() {
            let full = self
                .segments
                .back()
                .is_none_or(|tail| tail.len() == tail.capacity());
            if full {
                let capacity = match self.segments.back() {
                    None => data.len().min(SEGMENT),
                    Some(tail) => SEGMENT.min(data.len().max(2 * tail.capacity())),
                };
                self.segments.push_back(Vec::with_capacity(capacity));
            }
            let tail = self.segments.back_mut().expect("a tail segment");
            let n = data.len().min(tail.capacity() - tail.len());
            tail.extend_from_slice(&data[..n]);
            data = &data[n..];
        }
    }

    /// Copies up to `buf.remaining()` bytes, across segments; the bytes copied.
    fn read_into(&mut self, buf: &mut ReadBuf<'_>) -> usize {
        let mut n = 0;
        while buf.remaining() > 0 {
            let Some(segment) = self.segments.front() else {
                break;
            };
            let count = buf.remaining().min(segment.len() - self.head);
            buf.put_slice(&segment[self.head..self.head + count]);
            n += count;
            self.head += count;
            if self.head == segment.len() {
                self.segments.pop_front();
                self.head = 0;
            }
        }
        n
    }

    /// Whether the unread bytes start with `prefix`, across segments.
    fn starts_with(&self, prefix: &[u8]) -> bool {
        let mut unread = self
            .segments
            .iter()
            .enumerate()
            .flat_map(|(i, segment)| &segment[if i == 0 { self.head } else { 0 }..]);
        prefix.iter().all(|byte| unread.next() == Some(byte))
    }

    fn clear(&mut self) {
        self.segments = VecDeque::new();
        self.head = 0;
    }

    fn len(&self) -> usize {
        self.segments.iter().map(Vec::len).sum::<usize>() - self.head
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.segments.iter().map(Vec::capacity).sum()
    }
}

/// What the dispatch does with received payload.
enum Receive {
    Accept,
    /// The connection already failed: drop silently.
    Drop,
    Exceeded,
}

impl Flow {
    fn new(window: u32, credit: u64) -> Self {
        Flow {
            window,
            state: Mutex::new(FlowState {
                credit,
                ..Default::default()
            }),
            failed: CancellationToken::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FlowState> {
        self.state.lock().unwrap()
    }

    /// Accounts received payload against the window and queues it in the
    /// inbox. Payload after the server's CLOSE is dropped; an
    /// empty payload ends the reader after the inbox, as on the Java side. A
    /// violation resets the connection, drops the inbox and wakes the reader
    /// and the writer.
    fn receive(&self, payload: Vec<u8>) -> Receive {
        let (received, reader, writer) = {
            let mut state = self.lock();
            if state.reset || state.remote_failed {
                return Receive::Drop;
            }
            state.outstanding += payload.len() as u64;
            if state.outstanding > u64::from(self.window) {
                state.reset = true;
                state.error_sent = true;
                state.failure = Some(WINDOW_EXCEEDED.to_string());
                state.inbox.clear();
                (Receive::Exceeded, state.reader.take(), state.writer.take())
            } else if payload.is_empty() {
                state.remote_end.get_or_insert(RemoteEnd::Closed);
                (Receive::Accept, state.reader.take(), None)
            } else if state.remote_end.is_some() {
                return Receive::Drop;
            } else {
                state.inbox.push(payload);
                (Receive::Accept, state.reader.take(), None)
            }
        };
        if matches!(received, Receive::Exceeded) {
            self.failed.cancel();
        }
        for waker in [reader, writer].into_iter().flatten() {
            waker.wake();
        }
        received
    }

    /// Adds the server's grant (saturating) and wakes the writer.
    fn grant(&self, increment: u32) {
        let waker = {
            let mut state = self.lock();
            state.credit = state.credit.saturating_add(u64::from(increment));
            state.blocked_sent = false;
            state.writer.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// The server asks for the credit past `keep`: gives it up; what was given
    /// up, to announce as released.
    fn reclaim(&self, keep: u32) -> u32 {
        let mut state = self.lock();
        let released = state
            .credit
            .saturating_sub(u64::from(keep))
            .min(u64::from(u32::MAX));
        state.credit -= released;
        released as u32
    }

    /// The server's CLOSE: the reader ends after the inbox. True the first
    /// time.
    fn remote_close(&self) -> bool {
        let (first, reader) = {
            let mut state = self.lock();
            let first = !std::mem::replace(&mut state.remote_closed, true);
            state.remote_end.get_or_insert(RemoteEnd::Closed);
            (first, state.reader.take())
        };
        if let Some(waker) = reader {
            waker.wake();
        }
        first
    }

    /// The server's ERROR: writes fail from now on, the reader after the
    /// inbox. True the first time.
    fn remote_fail(&self, message: &str) -> bool {
        let (first, reader, writer) = {
            let mut state = self.lock();
            let first = !std::mem::replace(&mut state.remote_failed, true);
            state.failure.get_or_insert_with(|| message.to_string());
            state.remote_end.get_or_insert(RemoteEnd::Failed);
            (first, state.reader.take(), state.writer.take())
        };
        self.failed.cancel();
        for waker in [reader, writer].into_iter().flatten() {
            waker.wake();
        }
        first
    }

    /// Local teardown: reader and writer fail, the inbox is dropped, nothing
    /// more is sent.
    fn reset(&self, message: &str) {
        let (reader, writer) = {
            let mut state = self.lock();
            state.reset = true;
            state.failure.get_or_insert_with(|| message.to_string());
            state.inbox.clear();
            (state.reader.take(), state.writer.take())
        };
        self.failed.cancel();
        for waker in [reader, writer].into_iter().flatten() {
            waker.wake();
        }
    }

    fn reset_failure(&self) -> Option<String> {
        let state = self.lock();
        if state.reset {
            state.failure.clone()
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Registry and dispatch

/// conn id -> the flow state of a registered connection; one per stream
/// attempt, since conn ids are the server's.
type Registry = Arc<Mutex<HashMap<i64, Arc<Flow>>>>;

/// The virtual connections of one stream attempt.
pub struct Connections {
    /// Session label for the log.
    key: String,
    outbound: Outbound,
    /// Dial address (the advertised target) -> its handler.
    routes: Arc<HashMap<String, Arc<Handler>>>,
    registry: Registry,
    limit: Arc<Limit>,
    /// Test hook: the CONNECTs rejected for announcing no window.
    #[cfg(test)]
    windowless_rejections: AtomicUsize,
}

impl Connections {
    pub fn new(
        key: String,
        outbound: Outbound,
        routes: Arc<HashMap<String, Arc<Handler>>>,
        limit: Arc<Limit>,
    ) -> Self {
        Connections {
            key,
            outbound,
            routes,
            registry: Registry::default(),
            limit,
            #[cfg(test)]
            windowless_rejections: AtomicUsize::new(0),
        }
    }

    /// Routes one connection frame of the server to its connection. A plain
    /// function: it cannot await, so a slow connection never stalls the
    /// receive loop here; keep it non-`async`.
    pub fn dispatch(&self, body: Body) {
        match body {
            Body::Connect(connect) => self.connect(connect),
            Body::Data(data) => self.data(data),
            Body::Close(close) => {
                if let Some(flow) = self.lookup(close.conn_id) {
                    flow.remote_close();
                }
            }
            Body::Error(error) => {
                println!(
                    "[{}] conn {} failed: {}",
                    self.key, error.conn_id, error.message
                );
                if let Some(flow) = self.lookup(error.conn_id)
                    && flow.remote_fail(&error.message)
                {
                    self.outbound.wake_writer(error.conn_id);
                }
            }
            Body::WindowUpdate(update) => {
                // ignored for unknown connections, as on the Java side
                if let Some(flow) = self.lookup(update.conn_id) {
                    if update.increment != 0 {
                        flow.grant(update.increment);
                    }
                    if update.reclaim_above != 0 {
                        let released = flow.reclaim(update.reclaim_above);
                        if released > 0 {
                            self.outbound.control(Frame {
                                body: Some(Body::WindowUpdate(WindowUpdate {
                                    conn_id: update.conn_id,
                                    released,
                                    ..Default::default()
                                })),
                            });
                        }
                    }
                }
            }
            // session frames are the receive loop's; KeepAlive is a no-op
            _ => {}
        }
    }

    fn lookup(&self, conn_id: i64) -> Option<Arc<Flow>> {
        self.registry.lock().unwrap().get(&conn_id).cloned()
    }

    fn connect(&self, connect: Connect) {
        if connect.connection_window == 0 {
            // neither registered nor served, like a CONNECT beyond the limit
            eprintln!(
                "[{}] {SERVER_WINDOW_REQUIRED}; rejecting conn {}",
                self.key, connect.conn_id
            );
            #[cfg(test)]
            self.windowless_rejections.fetch_add(1, Ordering::AcqRel);
            self.outbound
                .control(error_frame(connect.conn_id, SERVER_WINDOW_REQUIRED));
            return;
        }
        let Some(handler) = self.routes.get(&connect.address) else {
            self.outbound.control(error_frame(
                connect.conn_id,
                &format!("no wasm route for '{}'", connect.address),
            ));
            return;
        };
        // answered like a missing route: neither registered nor served
        let Some(permit) = self.limit.try_acquire() else {
            self.outbound
                .control(error_frame(connect.conn_id, LIMIT_REACHED));
            return;
        };
        let mut io = self.register(connect.conn_id, connect.connection_window);
        io.permit = Some(permit);
        tokio::spawn(wasm_host::serve(io, handler.clone()));
    }

    fn data(&self, data: Data) {
        let Some(flow) = self.lookup(data.conn_id) else {
            return;
        };
        match flow.receive(data.payload) {
            Receive::Accept | Receive::Drop => {}
            Receive::Exceeded => {
                eprintln!("[{}] conn {}: {WINDOW_EXCEEDED}", self.key, data.conn_id);
                self.outbound
                    .control(error_frame(data.conn_id, WINDOW_EXCEEDED));
                self.outbound.wake_writer(data.conn_id);
                self.registry.lock().unwrap().remove(&data.conn_id);
            }
        }
    }

    /// Registers a connection with the server's window as its initial credit
    /// and returns the byte stream hyper serves it over.
    fn register(&self, conn_id: i64, server_window: u32) -> TunnelIo {
        let flow = Arc::new(Flow::new(WINDOW, u64::from(server_window)));
        self.registry.lock().unwrap().insert(conn_id, flow.clone());
        TunnelIo {
            conn_id,
            outbound: self.outbound.clone(),
            flow,
            registry: self.registry.clone(),
            consumed: 0,
            permit: None,
        }
    }

    /// Test hook: the CONNECTs rejected for announcing no window.
    #[cfg(test)]
    pub(crate) fn windowless_rejections(&self) -> usize {
        self.windowless_rejections.load(Ordering::Acquire)
    }

    /// The stream ended: fails every connection without waiting and ends the
    /// outbound stream. Readers and writers wake with an error; nothing more
    /// is sent.
    pub fn teardown(&self) {
        let drained: Vec<_> = self.registry.lock().unwrap().drain().collect();
        for (_, flow) in drained {
            flow.reset(STREAM_CLOSED);
        }
        // after the resets: the writers it wakes see the connection's failure
        self.outbound.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Tunnel <-> hyper byte stream adapter

/// AsyncRead/AsyncWrite over one virtual connection: reads take the inbox,
/// writes emit DATA frames against the server's credit.
pub struct TunnelIo {
    conn_id: i64,
    outbound: Outbound,
    flow: Arc<Flow>,
    registry: Registry,
    /// Bytes handed to hyper and not yet granted back.
    consumed: u64,
    /// The connection's slot of the connection limit, until `ending` takes
    /// it.
    permit: Option<Permit>,
}

impl TunnelIo {
    pub fn conn_id(&self) -> i64 {
        self.conn_id
    }

    /// The bytes received and not yet read.
    pub fn buffered(&self) -> usize {
        self.flow.lock().inbox.len()
    }

    /// Whether the unread bytes start with `prefix`, across frames.
    pub fn peek_prefix(&self, prefix: &[u8]) -> bool {
        self.flow.lock().inbox.starts_with(prefix)
    }

    /// Resolves once the connection failed: the server's ERROR, payload past
    /// the window, or the stream's end. Unlike the read error these cause,
    /// it does not depend on hyper reading.
    pub fn failed(&self) -> WaitForCancellationFutureOwned {
        self.flow.failed.clone().cancelled_owned()
    }

    /// Waits for unread bytes, which stay in the inbox; false at the end
    /// (the server's CLOSE or ERROR, or a reset).
    pub async fn fill(&mut self) -> bool {
        std::future::poll_fn(|cx| self.poll_fill(cx)).await
    }

    /// `fill`: true once the inbox holds bytes, false at the end.
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        let mut state = self.flow.lock();
        if state.reset {
            return Poll::Ready(false);
        }
        if !state.inbox.segments.is_empty() {
            return Poll::Ready(true);
        }
        if state.remote_end.is_some() {
            return Poll::Ready(false);
        }
        state.reader = Some(cx.waker().clone());
        Poll::Pending
    }

    /// What the serve task needs once hyper is done with the connection;
    /// takes over the connection's slot.
    pub fn ending(&mut self) -> Ending {
        Ending {
            conn_id: self.conn_id,
            flow: self.flow.clone(),
            outbound: self.outbound.clone(),
            registry: self.registry.clone(),
            permit: self.permit.take(),
        }
    }

    /// The read: the inbox, then the end.
    fn poll_inbox(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(failure) = self.flow.reset_failure() {
            return Poll::Ready(Err(std::io::Error::other(failure)));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let n = {
            let mut state = self.flow.lock();
            // checked again under the lock: a reset in between took no waker
            if state.reset {
                let failure = state.failure.clone().unwrap_or_default();
                return Poll::Ready(Err(std::io::Error::other(failure)));
            }
            let n = state.inbox.read_into(buf);
            if n == 0 {
                return match state.remote_end {
                    // an unfilled ReadBuf on return signals EOF to hyper
                    Some(RemoteEnd::Closed) => Poll::Ready(Ok(())),
                    Some(RemoteEnd::Failed) => Poll::Ready(Err(std::io::Error::other(ABORTED))),
                    None => {
                        state.reader = Some(cx.waker().clone());
                        #[cfg(test)]
                        {
                            state.reader_waits += 1;
                        }
                        Poll::Pending
                    }
                };
            }
            n
        };
        self.consumed(n);
        Poll::Ready(Ok(()))
    }

    /// Test hook: the inbox's segments.
    #[cfg(test)]
    fn inbox_segments(&self) -> usize {
        self.flow.lock().inbox.segments.len()
    }

    /// Test hook: the payload in the inbox, not yet read.
    #[cfg(test)]
    fn inbox_bytes(&self) -> usize {
        self.flow.lock().inbox.len()
    }

    /// Test hook: the capacity of the inbox's segments, read, unread or empty.
    #[cfg(test)]
    fn inbox_capacity(&self) -> usize {
        self.flow.lock().inbox.capacity()
    }

    /// Grants the consumed bytes back once they reach half the window. The
    /// grant goes on the control queue: it never waits.
    fn consumed(&mut self, n: usize) {
        self.consumed += n as u64;
        if self.consumed < u64::from(self.flow.window / 2) {
            return;
        }
        let increment = self.consumed;
        self.consumed = 0;
        {
            let mut state = self.flow.lock();
            if state.reset || state.remote_failed {
                return; // the server sends nothing more
            }
            // lower the count before the grant leaves: the server's next
            // payload must never be measured against a stale count
            state.outstanding = state.outstanding.saturating_sub(increment);
        }
        self.outbound.control(Frame {
            body: Some(Body::WindowUpdate(WindowUpdate {
                conn_id: self.conn_id,
                increment: increment as u32,
                ..Default::default()
            })),
        });
    }

    /// Queues CLOSE after the connection's DATA, once; never waits.
    fn send_close(&mut self) {
        {
            let mut state = self.flow.lock();
            if state.close_sent || state.error_sent || state.reset {
                return;
            }
            state.close_sent = true;
        }
        self.outbound
            .marker(self.conn_id, close_frame(self.conn_id));
    }
}

impl AsyncRead for TunnelIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.poll_inbox(cx, buf)
    }
}

impl AsyncWrite for TunnelIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        {
            let mut state = this.flow.lock();
            if let Some(failure) = &state.failure {
                return Poll::Ready(Err(std::io::Error::other(failure.clone())));
            }
            if state.close_sent || state.error_sent {
                return Poll::Ready(Err(std::io::Error::other(WRITE_CLOSED)));
            }
            if state.credit == 0 {
                // a grant, the server's ERROR or the teardown wakes it
                state.writer = Some(cx.waker().clone());
                let tell = !std::mem::replace(&mut state.blocked_sent, true);
                drop(state);
                if tell {
                    this.outbound.control(Frame {
                        body: Some(Body::WindowUpdate(WindowUpdate {
                            conn_id: this.conn_id,
                            blocked: true,
                            ..Default::default()
                        })),
                    });
                }
                return Poll::Pending;
            }
        }
        match this.outbound.poll_data_slot(this.conn_id, cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(Closed)) => {
                return Poll::Ready(Err(std::io::Error::other(STREAM_CLOSED)));
            }
            Poll::Pending => return Poll::Pending,
        }
        let n = {
            let mut state = this.flow.lock();
            if let Some(failure) = &state.failure {
                return Poll::Ready(Err(std::io::Error::other(failure.clone())));
            }
            // only this writer takes credit: it is still >= what was seen
            let n = buf.len().min(CHUNK).min(state.credit as usize);
            state.credit -= n as u64;
            n
        };
        this.outbound.data(
            this.conn_id,
            Frame {
                body: Some(Body::Data(Data {
                    conn_id: this.conn_id,
                    payload: buf[..n].to_vec(),
                })),
            },
        );
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // Half-close the tunnel direction when hyper is done with the connection.
        self.send_close();
        Poll::Ready(Ok(()))
    }
}

/// The end of one connection, run by its serve task after hyper returned.
pub struct Ending {
    conn_id: i64,
    flow: Arc<Flow>,
    outbound: Outbound,
    registry: Registry,
    permit: Option<Permit>,
}

impl Ending {
    /// Unregisters the connection and queues its terminal frame after its
    /// DATA, without waiting: ERROR ("connection closed") for a connection
    /// the server may still send on, else CLOSE unless one went
    /// out (hyper skips its shutdown on an error). The connection's slot is
    /// freed once its queued frames left.
    pub fn finish(mut self) {
        self.registry.lock().unwrap().remove(&self.conn_id);
        let frame = {
            let mut state = self.flow.lock();
            // hyper is done: what it did not read is never read
            state.inbox.clear();
            if state.reset || state.error_sent {
                None
            } else if !state.remote_closed && !state.remote_failed {
                state.error_sent = true;
                Some(error_frame(self.conn_id, CONNECTION_CLOSED))
            } else if !state.close_sent {
                state.close_sent = true;
                Some(close_frame(self.conn_id))
            } else {
                None
            }
        };
        if let Some(frame) = frame {
            self.outbound.marker(self.conn_id, frame);
        }
        if let Some(permit) = self.permit.take() {
            self.outbound.hold(self.conn_id, permit);
        }
    }
}

// ---------------------------------------------------------------------------
// Test support

/// A stand-in for the server side of the stream: feeds frames through the
/// dispatch as the receive loop does and reads what the wasmlet sends.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub(crate) struct Peer {
        pub(crate) connections: Connections,
        frames: Pin<Box<dyn Stream<Item = Frame> + Send + Sync>>,
        /// Grant every DATA frame back as it is received, as a server whose
        /// public peer reads promptly does.
        pub(crate) auto_grant: bool,
    }

    impl Peer {
        pub(crate) fn new(routes: HashMap<String, Arc<Handler>>) -> Peer {
            Peer::limited(routes, Limit::new(usize::MAX))
        }

        /// A peer whose connections share `limit`.
        pub(crate) fn limited(routes: HashMap<String, Arc<Handler>>, limit: Arc<Limit>) -> Peer {
            let (outbound, frames) = Outbound::new();
            Peer {
                connections: Connections::new("test".into(), outbound, Arc::new(routes), limit),
                frames: Box::pin(frames),
                auto_grant: true,
            }
        }

        /// One frame from the server, dispatched as the receive loop does.
        pub(crate) fn send(&self, body: Body) {
            self.connections.dispatch(body);
        }

        pub(crate) fn connect(&self, conn_id: i64, address: &str, window: u32) {
            self.send(Body::Connect(Connect {
                conn_id,
                address: address.into(),
                connection_window: window,
                ..Default::default()
            }));
        }

        pub(crate) fn data(&self, conn_id: i64, payload: &[u8]) {
            self.send(Body::Data(Data {
                conn_id,
                payload: payload.to_vec(),
            }));
        }

        pub(crate) fn error(&self, conn_id: i64, message: &str) {
            self.send(Body::Error(Error {
                conn_id,
                message: message.into(),
            }));
        }

        /// The DATA frames queued for `conn_id`.
        pub(crate) fn queued_data(&self, conn_id: i64) -> usize {
            self.connections.outbound.queued_data(conn_id)
        }

        /// The connection is registered (its end has not run).
        pub(crate) fn registered(&self, conn_id: i64) -> bool {
            self.connections.lookup(conn_id).is_some()
        }

        /// The next frame the wasmlet sent; None once the stream ended.
        pub(crate) async fn recv(&mut self) -> Option<Frame> {
            use futures_util::StreamExt as _;
            let frame = self.frames.next().await?;
            if let (true, Some(Body::Data(data))) = (self.auto_grant, &frame.body) {
                self.connections.dispatch(Body::WindowUpdate(WindowUpdate {
                    conn_id: data.conn_id,
                    increment: data.payload.len() as u32,
                    ..Default::default()
                }));
            }
            Some(frame)
        }
    }

    impl Drop for Peer {
        /// The stream ends with the peer, as on a real stream.
        fn drop(&mut self) {
            self.connections.teardown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::Peer;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;
    use std::time::Duration;

    /// Counts its wakes.
    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Arc<CountingWaker>, Waker) {
        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        (counter.clone(), Waker::from(counter))
    }

    fn peer() -> Peer {
        Peer::new(HashMap::new())
    }

    fn data_frame(conn_id: i64, len: usize) -> Frame {
        Frame {
            body: Some(data_body(conn_id, len)),
        }
    }

    fn data_body(conn_id: i64, len: usize) -> Body {
        Body::Data(Data {
            conn_id,
            payload: vec![0x5a; len],
        })
    }

    /// Feeds `total` bytes to `conn_id` in 16KiB frames.
    fn feed(peer: &Peer, conn_id: i64, total: usize) {
        let mut left = total;
        while left > 0 {
            let n = left.min(CHUNK);
            peer.send(data_body(conn_id, n));
            left -= n;
        }
    }

    /// Reads exactly `n` bytes from `io` (or fails), polling it like hyper.
    async fn read_exact(io: &mut TunnelIo, n: usize) -> std::io::Result<Vec<u8>> {
        use tokio::io::AsyncReadExt as _;
        let mut out = vec![0; n];
        io.read_exact(&mut out).await?;
        Ok(out)
    }

    /// The next frame, or None when nothing arrives within 200ms.
    async fn next_frame(peer: &mut Peer) -> Option<Frame> {
        tokio::time::timeout(Duration::from_millis(200), peer.recv())
            .await
            .ok()
            .flatten()
    }

    async fn drain_frames(peer: &mut Peer) -> Vec<Body> {
        let mut frames = Vec::new();
        while let Some(frame) = next_frame(peer).await {
            frames.push(frame.body.expect("frame body"));
        }
        frames
    }

    fn poll_write(io: &mut TunnelIo, waker: &Waker, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(io).poll_write(&mut Context::from_waker(waker), buf)
    }

    #[tokio::test]
    async fn a_connection_whose_reader_never_polls_does_not_stall_another() {
        let peer = peer();
        let _stalled = peer.connections.register(1, WINDOW);
        let mut served = peer.connections.register(2, WINDOW);

        // conn 1 takes its whole window while its reader never polls
        feed(&peer, 1, WINDOW as usize);
        feed(&peer, 2, 5);

        let received = tokio::time::timeout(Duration::from_secs(1), read_exact(&mut served, 5))
            .await
            .expect("conn 2's payload is delivered")
            .unwrap();
        assert_eq!(received, vec![0x5a; 5]);
    }

    #[tokio::test]
    async fn payload_past_the_window_fails_only_that_connection() {
        let mut peer = peer();
        let mut violated = peer.connections.register(1, WINDOW);
        let mut other = peer.connections.register(2, WINDOW);
        feed(&peer, 1, WINDOW as usize);
        assert!(
            drain_frames(&mut peer).await.is_empty(),
            "a full window is no violation"
        );

        feed(&peer, 1, 1);

        let frames = drain_frames(&mut peer).await;
        assert!(
            matches!(&frames[..], [Body::Error(error)]
                if error.conn_id == 1 && error.message == WINDOW_EXCEEDED),
            "one ERROR for conn 1: {frames:?}"
        );
        let error = read_exact(&mut violated, 1).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            WINDOW_EXCEEDED,
            "the reader fails at once, dropping the buffered window"
        );
        let (_, waker) = counting_waker();
        let Poll::Ready(Err(error)) = poll_write(&mut violated, &waker, b"x") else {
            panic!("the writer fails too");
        };
        assert_eq!(error.to_string(), WINDOW_EXCEEDED);

        // later payload is dropped, not counted again; conn 2 is unaffected
        feed(&peer, 1, 64 * 1024);
        feed(&peer, 2, 3);
        assert_eq!(read_exact(&mut other, 3).await.unwrap(), vec![0x5a; 3]);
        assert!(drain_frames(&mut peer).await.is_empty(), "a single ERROR");
    }

    #[tokio::test]
    async fn a_write_without_credit_is_pending_until_a_grant_wakes_it() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, 10);
        let (wakes, waker) = counting_waker();

        let Poll::Ready(Ok(n)) = poll_write(&mut io, &waker, &[1; 16]) else {
            panic!("the initial credit is sendable");
        };
        assert_eq!(n, 10, "a write takes at most the credit");
        assert!(poll_write(&mut io, &waker, &[2; 6]).is_pending());
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0);

        peer.send(Body::WindowUpdate(WindowUpdate {
            conn_id: 1,
            increment: 4,
            ..Default::default()
        }));
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the grant wakes the writer"
        );
        let Poll::Ready(Ok(n)) = poll_write(&mut io, &waker, &[2; 6]) else {
            panic!("the grant is sendable");
        };
        assert_eq!(n, 4);

        let frames = drain_frames(&mut peer).await;
        let sizes: Vec<usize> = frames
            .iter()
            .filter_map(|body| match body {
                Body::Data(data) => Some(data.payload.len()),
                _ => None,
            })
            .collect();
        assert_eq!(sizes, vec![10, 4], "never more than the credit on the wire");
        assert_eq!(blocked(&frames), 1, "the server was told of the wait");
    }

    /// The WindowUpdates telling the server the writer of conn 1 waits.
    fn blocked(frames: &[Body]) -> usize {
        frames
            .iter()
            .filter(|body| {
                matches!(body, Body::WindowUpdate(update)
                    if update.conn_id == 1 && update.blocked && update.increment == 0)
            })
            .count()
    }

    #[tokio::test]
    async fn a_writer_tells_the_server_once_per_wait() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, 4);
        let (_, waker) = counting_waker();
        assert!(matches!(
            poll_write(&mut io, &waker, &[1; 4]),
            Poll::Ready(Ok(4))
        ));
        assert!(poll_write(&mut io, &waker, &[1; 4]).is_pending());
        assert!(poll_write(&mut io, &waker, &[1; 4]).is_pending());

        peer.send(Body::WindowUpdate(WindowUpdate {
            conn_id: 1,
            increment: 4,
            ..Default::default()
        }));
        assert!(matches!(
            poll_write(&mut io, &waker, &[1; 4]),
            Poll::Ready(Ok(4))
        ));
        assert!(poll_write(&mut io, &waker, &[1; 4]).is_pending());
        // drained at the end: the test peer grants the DATA it takes
        assert_eq!(
            blocked(&drain_frames(&mut peer).await),
            2,
            "once per wait, and the next wait tells again"
        );
    }

    #[tokio::test]
    async fn a_reclaim_gives_up_the_credit_past_what_the_server_keeps() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, 1000);
        let (_, waker) = counting_waker();
        peer.send(Body::WindowUpdate(WindowUpdate {
            conn_id: 1,
            reclaim_above: 300,
            ..Default::default()
        }));
        let released: Vec<u32> = drain_frames(&mut peer)
            .await
            .iter()
            .filter_map(|body| match body {
                Body::WindowUpdate(update) if update.released > 0 => Some(update.released),
                _ => None,
            })
            .collect();
        assert_eq!(released, vec![700], "what it gave up, announced");
        let Poll::Ready(Ok(n)) = poll_write(&mut io, &waker, &[1; 1000]) else {
            panic!("the kept credit is sendable");
        };
        assert_eq!(n, 300, "never the released credit");
        assert!(poll_write(&mut io, &waker, &[1; 1]).is_pending());

        // nothing past what the server keeps: nothing released
        peer.send(Body::WindowUpdate(WindowUpdate {
            conn_id: 1,
            reclaim_above: 300,
            ..Default::default()
        }));
        assert!(
            drain_frames(&mut peer)
                .await
                .iter()
                .all(|body| !matches!(body, Body::WindowUpdate(update) if update.released > 0))
        );
    }

    #[tokio::test]
    async fn a_write_waiting_for_credit_fails_on_the_servers_error() {
        let peer = peer();
        let mut io = peer.connections.register(1, 1);
        let (wakes, waker) = counting_waker();
        assert!(matches!(
            poll_write(&mut io, &waker, b"ab"),
            Poll::Ready(Ok(1))
        ));
        assert!(poll_write(&mut io, &waker, b"b").is_pending());

        peer.error(1, "public peer reset");

        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the ERROR wakes the writer"
        );
        let Poll::Ready(Err(error)) = poll_write(&mut io, &waker, b"b") else {
            panic!("the waiting write fails");
        };
        assert_eq!(error.to_string(), "public peer reset");
    }

    #[tokio::test]
    async fn a_write_waiting_for_credit_fails_when_the_stream_ends() {
        let peer = peer();
        let mut io = peer.connections.register(1, 1);
        let (wakes, waker) = counting_waker();
        assert!(matches!(
            poll_write(&mut io, &waker, b"ab"),
            Poll::Ready(Ok(1))
        ));
        assert!(poll_write(&mut io, &waker, b"b").is_pending());

        peer.connections.teardown();

        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the teardown wakes the writer"
        );
        let Poll::Ready(Err(error)) = poll_write(&mut io, &waker, b"b") else {
            panic!("the waiting write fails");
        };
        assert_eq!(error.to_string(), STREAM_CLOSED);
        let error = read_exact(&mut io, 1).await.unwrap_err();
        assert_eq!(error.to_string(), STREAM_CLOSED, "and so does the reader");
    }

    fn grants(frames: &[Body]) -> Vec<u32> {
        frames
            .iter()
            .filter_map(|body| match body {
                Body::WindowUpdate(update) => {
                    assert_eq!(update.conn_id, 1);
                    Some(update.increment)
                }
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn reading_grants_once_per_half_window_consumed() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let half = WINDOW as usize / 2;
        feed(&peer, 1, WINDOW as usize);

        read_exact(&mut io, half - 1).await.unwrap();
        assert!(
            grants(&drain_frames(&mut peer).await).is_empty(),
            "below half"
        );
        read_exact(&mut io, 1).await.unwrap();
        assert_eq!(grants(&drain_frames(&mut peer).await), vec![WINDOW / 2]);
        read_exact(&mut io, half).await.unwrap();
        assert_eq!(grants(&drain_frames(&mut peer).await), vec![WINDOW / 2]);

        // what was granted may be sent again: the window is open, no violation
        feed(&peer, 1, WINDOW as usize);
        read_exact(&mut io, half).await.unwrap();
        read_exact(&mut io, half).await.unwrap();
        let frames = drain_frames(&mut peer).await;
        assert_eq!(grants(&frames), vec![WINDOW / 2, WINDOW / 2], "{frames:?}");
    }

    #[tokio::test]
    async fn control_frames_overtake_queued_data() {
        let (outbound, frames) = Outbound::new();
        let mut frames = Box::pin(frames);
        for _ in 0..DATA_QUEUE_CAPACITY {
            outbound.data(1, data_frame(1, 1));
        }
        outbound.control(error_frame(2, "x"));
        use futures_util::StreamExt as _;
        let first = frames.next().await.unwrap();
        assert!(
            matches!(first.body, Some(Body::Error(_))),
            "control first: {first:?}"
        );
    }

    const STALL: Duration = Duration::from_secs(60);

    /// Takes one frame from the request stream, as gRPC does when its flow
    /// control has room.
    async fn take(frames: &mut Pin<Box<Frames>>) -> Frame {
        use futures_util::StreamExt as _;
        frames.next().await.expect("a frame")
    }

    #[tokio::test(start_paused = true)]
    async fn connection_frames_not_taken_stall_past_the_timeout() {
        let (outbound, _frames) = Outbound::new();
        let start = Instant::now();
        outbound.data(1, data_frame(1, 1));
        outbound.marker(1, close_frame(1));
        // frames queued later, none taken, do not move the clock on
        let producer = outbound.clone();
        let more = tokio::spawn(async move {
            for conn_id in 2.. {
                tokio::time::sleep(STALL / 10).await;
                producer.data(conn_id, data_frame(conn_id, 1));
            }
        });

        let waited = tokio::time::timeout(STALL * 2, outbound.stalled(STALL)).await;
        more.abort();
        let waited = waited.expect("stalled while frames kept being queued");

        assert_eq!(
            start.elapsed(),
            STALL,
            "resolved at the timeout, not before"
        );
        assert_eq!(waited, STALL);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_taking_frames_slower_than_they_queue_never_stalls() {
        let (outbound, frames) = Outbound::new();
        let mut frames = Box::pin(frames);
        for _ in 0..DATA_QUEUE_CAPACITY {
            outbound.data(1, data_frame(1, 1));
        }
        let watchdog = outbound.clone();
        let stalled = tokio::spawn(async move { watchdog.stalled(STALL).await });
        // one frame per half timeout, over eight timeouts; the queue never
        // empties, so every step is a stall shorter than the timeout
        for _ in 0..DATA_QUEUE_CAPACITY {
            tokio::time::sleep(STALL / 2).await;
            assert!(!stalled.is_finished(), "cut while frames were taken");
            take(&mut frames).await;
            outbound.data(1, data_frame(1, 1));
        }
        assert_eq!(
            outbound.queued_data(1),
            DATA_QUEUE_CAPACITY,
            "precondition: frames kept waiting"
        );
        stalled.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_queued_never_stalls_and_control_frames_are_no_progress() {
        let (outbound, frames) = Outbound::new();
        let mut frames = Box::pin(frames);
        // idle: no connection frame waits, however long nothing is taken
        outbound.control(error_frame(2, "x"));
        let idle = tokio::time::timeout(STALL * 3, outbound.stalled(STALL)).await;
        assert!(idle.is_err(), "an idle stream is never stalled");

        // control frames leaving do not move the stall clock on
        let start = Instant::now();
        outbound.data(1, data_frame(1, 1));
        let watchdog = outbound.clone();
        let stalled = tokio::spawn(async move { watchdog.stalled(STALL).await });
        while !stalled.is_finished() && start.elapsed() < STALL * 2 {
            outbound.control(error_frame(2, "x"));
            assert!(matches!(take(&mut frames).await.body, Some(Body::Error(_))));
            tokio::time::sleep(STALL / 10).await;
        }
        let waited = stalled.await.expect("the watchdog resolved");
        assert!(waited >= STALL, "waited {waited:?}");
        assert!(
            start.elapsed() < STALL * 2,
            "resolved while control frames kept leaving"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_timeout_never_stalls() {
        let (outbound, _frames) = Outbound::new();
        outbound.data(1, data_frame(1, 1));
        let stalled = tokio::time::timeout(STALL * 10, outbound.stalled(Duration::ZERO)).await;
        assert!(stalled.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn the_end_of_the_stream_ends_the_stall() {
        let (outbound, _frames) = Outbound::new();
        outbound.data(1, data_frame(1, 1));
        tokio::time::sleep(STALL / 2).await;
        outbound.shutdown();
        let stalled = tokio::time::timeout(STALL * 3, outbound.stalled(STALL)).await;
        assert!(stalled.is_err(), "nothing waits on an ended stream");
    }

    #[tokio::test]
    async fn a_connect_without_a_route_is_answered_on_the_control_queue() {
        let (outbound, frames) = Outbound::new();
        let mut frames = Box::pin(frames);
        // another connection's queue is full: an answer queued behind it
        // would wait
        for _ in 0..DATA_QUEUE_CAPACITY {
            outbound.data(9, data_frame(9, 1));
        }
        let connections = Connections::new(
            "test".into(),
            outbound,
            Arc::default(),
            Limit::new(usize::MAX),
        );
        connections.dispatch(Body::Connect(Connect {
            conn_id: 7,
            address: "wasm:missing".into(),
            connection_window: WINDOW,
            ..Default::default()
        }));
        use futures_util::StreamExt as _;
        let first = frames.next().await.unwrap();
        assert!(
            matches!(&first.body, Some(Body::Error(error))
                if error.conn_id == 7 && error.message.contains("no wasm route")),
            "{first:?}"
        );
    }

    #[test]
    fn a_limit_admits_up_to_its_max_and_a_dropped_permit_frees_its_slot() {
        let limit = Limit::new(2);
        let first = limit.try_acquire().expect("first slot");
        let _second = limit.try_acquire().expect("second slot");
        assert!(limit.try_acquire().is_none());
        assert_eq!(limit.open(), 2);
        drop(first);
        assert_eq!(limit.open(), 1);
        assert!(limit.try_acquire().is_some());
    }

    async fn finish(mut io: TunnelIo) {
        io.ending().finish();
    }

    #[tokio::test]
    async fn a_windowed_connection_ending_before_the_servers_close_sends_error() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        std::future::poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx))
            .await
            .unwrap();
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(
            matches!(&frames[..], [Body::Close(_), Body::Error(error)]
                if error.message == CONNECTION_CLOSED),
            "CLOSE from hyper's shutdown, then ERROR: {frames:?}"
        );
        assert!(
            peer.connections.lookup(1).is_none(),
            "unregistered: later payload is dropped"
        );
    }

    #[tokio::test]
    async fn a_windowed_connection_ending_after_the_servers_close_sends_close() {
        let mut peer = peer();
        let io = peer.connections.register(1, WINDOW);
        peer.send(Body::Close(Close { conn_id: 1 }));
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(matches!(&frames[..], [Body::Close(_)]), "{frames:?}");
    }

    #[tokio::test]
    async fn a_windowed_connection_ending_after_the_servers_error_sends_no_error() {
        let mut peer = peer();
        let io = peer.connections.register(1, WINDOW);
        peer.error(1, "gone");
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(matches!(&frames[..], [Body::Close(_)]), "{frames:?}");
    }

    #[tokio::test]
    async fn a_connection_shut_down_twice_sends_close_once() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        for _ in 0..2 {
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx))
                .await
                .unwrap();
        }
        peer.send(Body::Close(Close { conn_id: 1 }));
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(matches!(&frames[..], [Body::Close(_)]), "{frames:?}");
    }

    // -- outbound scheduling

    fn label(body: &Body) -> String {
        match body {
            Body::Data(data) => format!("{} DATA", data.conn_id),
            Body::Close(close) => format!("{} CLOSE", close.conn_id),
            Body::Error(error) => format!("{} ERROR", error.conn_id),
            other => format!("{other:?}"),
        }
    }

    fn labels(frames: &[Body]) -> Vec<String> {
        frames.iter().map(label).collect()
    }

    /// Writes into `io` until a write is pending (the writer parked); the
    /// bytes accepted.
    fn fill_until_pending(io: &mut TunnelIo, waker: &Waker) -> usize {
        let mut written = 0;
        for _ in 0..1000 {
            match poll_write(io, waker, &[0x33; CHUNK]) {
                Poll::Ready(Ok(n)) => written += n,
                Poll::Pending => return written,
                Poll::Ready(Err(e)) => panic!("the write failed: {e}"),
            }
        }
        panic!("the writer never parked: {written} bytes accepted");
    }

    /// A task writing `payload` into `io` as fast as it takes it, then
    /// closing; the bytes accepted so far.
    fn spawn_writer(
        mut io: TunnelIo,
        payload: Vec<u8>,
    ) -> (Arc<AtomicUsize>, tokio::task::JoinHandle<TunnelIo>) {
        let written = Arc::new(AtomicUsize::new(0));
        let counter = written.clone();
        let task = tokio::spawn(async move {
            let mut offset = 0;
            while offset < payload.len() {
                offset +=
                    std::future::poll_fn(|cx| Pin::new(&mut io).poll_write(cx, &payload[offset..]))
                        .await
                        .unwrap();
                counter.store(offset, Ordering::SeqCst);
            }
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx))
                .await
                .unwrap();
            io
        });
        (written, task)
    }

    fn pattern(len: usize, seed: usize) -> Vec<u8> {
        (0..len).map(|i| ((i + seed) % 251) as u8).collect()
    }

    /// Connection 1 has 1MiB of credit and a writer that queues all it may;
    /// connection 2's DATA and CLOSE leave one turn each after it, not
    /// behind its backlog (one shared FIFO sent them after all 64 frames).
    #[tokio::test]
    async fn another_connections_data_and_close_overtake_a_full_backlog() {
        let mut peer = peer();
        peer.auto_grant = false;
        let io1 = peer.connections.register(1, WINDOW);
        let mut io2 = peer.connections.register(2, WINDOW);
        let backlog = pattern(WINDOW as usize, 0);
        let (written, writer) = spawn_writer(io1, backlog.clone());
        while peer.queued_data(1) < DATA_QUEUE_CAPACITY {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert_eq!(
            written.load(Ordering::SeqCst),
            DATA_QUEUE_CAPACITY * CHUNK,
            "conn 1 queued its cap and has credit left"
        );
        assert!(!writer.is_finished(), "conn 1's writer waits at its cap");

        let (_, waker) = counting_waker();
        assert!(
            matches!(poll_write(&mut io2, &waker, b"x"), Poll::Ready(Ok(1))),
            "conn 2's writer does not wait on conn 1's queue"
        );
        let closed = Pin::new(&mut io2).poll_shutdown(&mut Context::from_waker(&waker));
        assert!(matches!(closed, Poll::Ready(Ok(()))), "{closed:?}");

        let mut order = Vec::new();
        let mut received = Vec::new();
        while !order.contains(&"1 CLOSE".to_string()) {
            let frame = tokio::time::timeout(Duration::from_secs(1), peer.recv())
                .await
                .expect("conn 1's backlog keeps flowing")
                .expect("the stream is open");
            let body = frame.body.unwrap();
            if let Body::Data(data) = &body
                && data.conn_id == 1
            {
                received.extend_from_slice(&data.payload);
            }
            order.push(label(&body));
        }
        assert_eq!(
            order[..4],
            ["1 DATA", "2 DATA", "1 DATA", "2 CLOSE"],
            "one turn each"
        );
        assert_eq!(received, backlog, "conn 1's bytes complete and in order");
        writer.await.unwrap();
    }

    /// A writer waiting at its cap is woken by DATA leaving its own queue
    /// only: other connections' frames, queued or sent, never wake it.
    #[tokio::test]
    async fn only_its_own_frames_leaving_wake_a_writer_waiting_at_its_cap() {
        let mut peer = peer();
        let outbound = peer.connections.outbound.clone();
        let mut io = peer.connections.register(1, WINDOW);
        let (wakes, waker) = counting_waker();
        assert_eq!(
            fill_until_pending(&mut io, &waker),
            DATA_QUEUE_CAPACITY * CHUNK
        );
        for _ in 0..100 {
            outbound.data(2, data_frame(2, 1));
        }
        outbound.marker(2, close_frame(2));
        outbound.control(error_frame(3, "x"));
        outbound.wake_writer(2);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0, "queueing elsewhere");

        let mut own = 0;
        let mut others = 0;
        for _ in 0..1000 {
            let body = next_frame(&mut peer).await.unwrap().body.unwrap();
            match &body {
                Body::Data(data) if data.conn_id == 1 => own += 1,
                _ => others += 1,
            }
            if own == wakes.0.load(Ordering::SeqCst) {
                // woken: refill as hyper would, parking again at the cap
                fill_until_pending(&mut io, &waker);
            }
            if matches!(body, Body::Close(_)) {
                break;
            }
        }
        assert_eq!(others, 102, "conn 2's frames and the control frame left");
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            own,
            "one wake per own frame, none for the {others} others"
        );
    }

    /// CLOSE and the connection-end ERROR are queued at once behind a full
    /// queue, after the connection's DATA.
    #[tokio::test]
    async fn close_and_error_bypass_the_cap_after_the_connections_data() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let (_, waker) = counting_waker();
        fill_until_pending(&mut io, &waker);
        assert_eq!(peer.queued_data(1), DATA_QUEUE_CAPACITY);

        let closed = Pin::new(&mut io).poll_shutdown(&mut Context::from_waker(&waker));
        assert!(matches!(closed, Poll::Ready(Ok(()))), "{closed:?}");
        io.ending().finish();

        let mut expected = vec!["1 DATA".to_string(); DATA_QUEUE_CAPACITY];
        expected.extend(["1 CLOSE".to_string(), "1 ERROR".to_string()]);
        assert_eq!(labels(&drain_frames(&mut peer).await), expected);
    }

    #[tokio::test]
    async fn a_write_after_the_close_fails() {
        let mut peer = peer();
        let (_, waker) = counting_waker();
        for (conn_id, window) in [(1, 0), (2, WINDOW)] {
            let mut io = peer.connections.register(conn_id, window);
            let closed = Pin::new(&mut io).poll_shutdown(&mut Context::from_waker(&waker));
            assert!(matches!(closed, Poll::Ready(Ok(()))), "{closed:?}");
            let Poll::Ready(Err(error)) = poll_write(&mut io, &waker, b"x") else {
                panic!("conn {conn_id}: the write fails");
            };
            assert_eq!(error.to_string(), WRITE_CLOSED);
        }
        assert_eq!(
            labels(&drain_frames(&mut peer).await),
            ["1 CLOSE", "2 CLOSE"],
            "no DATA after a CLOSE"
        );
    }

    /// Two connections writing at once: their frames interleave, and each
    /// connection's bytes and CLOSE keep their order.
    #[tokio::test]
    async fn interleaved_connections_keep_their_own_order() {
        let mut peer = peer();
        let payloads = [pattern(40 * CHUNK, 0), pattern(40 * CHUNK, 7)];
        let writers: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                let io = peer.connections.register(i as i64 + 1, WINDOW);
                spawn_writer(io, payload.clone()).1
            })
            .collect();
        let mut received = [Vec::new(), Vec::new()];
        let mut order: Vec<String> = Vec::new();
        while order
            .iter()
            .filter(|label| label.ends_with("CLOSE"))
            .count()
            < 2
        {
            let body = tokio::time::timeout(Duration::from_secs(1), peer.recv())
                .await
                .unwrap()
                .unwrap()
                .body
                .unwrap();
            match &body {
                Body::Data(data) => {
                    assert!(
                        !order.contains(&format!("{} CLOSE", data.conn_id)),
                        "DATA after its CLOSE"
                    );
                    received[data.conn_id as usize - 1].extend_from_slice(&data.payload);
                }
                Body::Close(_) => {}
                other => panic!("unexpected {other:?}"),
            }
            order.push(label(&body));
        }
        assert_eq!(received, payloads);
        let switches = order.windows(2).filter(|pair| pair[0] != pair[1]).count();
        assert!(
            switches >= 40,
            "the connections interleave: {switches} switches"
        );
        for writer in writers {
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_connection_queue_lives_only_while_it_holds_frames() {
        let mut peer = peer();
        let outbound = peer.connections.outbound.clone();
        let mut open = peer.connections.register(1, WINDOW);
        let (_, waker) = counting_waker();
        assert!(matches!(
            poll_write(&mut open, &waker, b"x"),
            Poll::Ready(Ok(1))
        ));
        assert_eq!(outbound.queues(), 1);
        drain_frames(&mut peer).await;
        assert_eq!(outbound.queues(), 0, "an open connection whose DATA left");

        outbound.data(2, data_frame(2, 1));
        outbound.marker(2, close_frame(2));
        drain_frames(&mut peer).await;
        assert_eq!(outbound.queues(), 0, "a closed connection");
        outbound.data(2, data_frame(2, 1));
        assert_eq!(
            drain_frames(&mut peer).await.len(),
            1,
            "a late frame is sent"
        );
        assert_eq!(outbound.queues(), 0, "and leaves no queue behind");

        for conn_id in 100..10_100 {
            outbound.data(conn_id, data_frame(conn_id, 1));
        }
        assert_eq!(outbound.queues(), 10_000);
        assert_eq!(drain_frames(&mut peer).await.len(), 10_000);
        assert_eq!(outbound.queues(), 0);
    }

    /// The server's ERROR, a window violation and the teardown each wake
    /// a writer waiting at its cap, which then fails.
    #[tokio::test]
    async fn a_writer_waiting_at_its_cap_fails_with_its_connection() {
        let peer = peer();
        let mut ios: Vec<_> = (0..4)
            .map(|i| {
                let mut io = peer.connections.register(i as i64 + 1, WINDOW);
                let (wakes, waker) = counting_waker();
                assert_eq!(
                    fill_until_pending(&mut io, &waker),
                    DATA_QUEUE_CAPACITY * CHUNK
                );
                (io, wakes, waker)
            })
            .collect();
        let woken = |ios: &Vec<(TunnelIo, Arc<CountingWaker>, Waker)>| -> Vec<usize> {
            ios.iter()
                .map(|(_, wakes, _)| wakes.0.load(Ordering::SeqCst))
                .collect()
        };
        let failure = |i: usize, ios: &mut Vec<(TunnelIo, Arc<CountingWaker>, Waker)>| {
            let (io, _, waker) = &mut ios[i];
            match poll_write(io, waker, b"x") {
                Poll::Ready(Err(error)) => error.to_string(),
                other => panic!("conn {}: the write fails: {other:?}", i + 1),
            }
        };

        peer.error(1, "public peer reset");
        assert_eq!(woken(&ios), [1, 0, 0, 0], "the server's ERROR");
        assert_eq!(failure(0, &mut ios), "public peer reset");

        feed(&peer, 2, WINDOW as usize + 1);
        assert_eq!(woken(&ios), [1, 1, 0, 0], "the violation");
        assert_eq!(failure(1, &mut ios), WINDOW_EXCEEDED);

        peer.connections.teardown();
        assert_eq!(woken(&ios), [1, 1, 1, 1], "the teardown");
        assert_eq!(failure(2, &mut ios), STREAM_CLOSED);
        assert_eq!(failure(3, &mut ios), STREAM_CLOSED);
    }

    /// gRPC dropping the request stream ends it like the teardown: the
    /// queued frames are freed, a writer at its cap fails, later frames are
    /// dropped.
    #[tokio::test]
    async fn dropping_the_request_stream_fails_a_writer_waiting_at_its_cap() {
        let (outbound, frames) = Outbound::new();
        let connections = Connections::new(
            "test".into(),
            outbound.clone(),
            Arc::default(),
            Limit::new(usize::MAX),
        );
        let mut io = connections.register(1, WINDOW);
        let (wakes, waker) = counting_waker();
        assert_eq!(
            fill_until_pending(&mut io, &waker),
            DATA_QUEUE_CAPACITY * CHUNK
        );

        drop(frames);

        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(Err(error)) = poll_write(&mut io, &waker, b"x") else {
            panic!("the write fails");
        };
        assert_eq!(error.to_string(), STREAM_CLOSED);
        assert_eq!(outbound.queues(), 0, "the queued frames are freed");
        outbound.control(error_frame(2, "x"));
        outbound.marker(1, close_frame(1));
        assert_eq!(outbound.queues(), 0);
        assert!(outbound.lock().control.is_empty());
    }

    /// The request stream waits for a frame, is woken by every kind of
    /// frame queued, and ends with the teardown, dropping what is queued.
    #[tokio::test]
    async fn the_request_stream_waits_for_frames_and_ends_with_the_teardown() {
        let (outbound, frames) = Outbound::new();
        let connections = Connections::new(
            "test".into(),
            outbound.clone(),
            Arc::default(),
            Limit::new(usize::MAX),
        );
        let mut frames = Box::pin(frames);
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        let queue: [&dyn Fn(); 3] = [
            &|| outbound.control(error_frame(1, "x")),
            &|| outbound.data(1, data_frame(1, 1)),
            &|| outbound.marker(1, close_frame(1)),
        ];
        for (i, queue) in queue.iter().enumerate() {
            assert!(frames.as_mut().poll_next(&mut cx).is_pending());
            queue();
            assert_eq!(wakes.0.load(Ordering::SeqCst), i + 1);
            assert!(matches!(
                frames.as_mut().poll_next(&mut cx),
                Poll::Ready(Some(_))
            ));
        }

        assert!(frames.as_mut().poll_next(&mut cx).is_pending());
        outbound.data(1, data_frame(1, 1));
        connections.teardown();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 4);
        assert!(
            matches!(frames.as_mut().poll_next(&mut cx), Poll::Ready(None)),
            "ended, the queued frame dropped"
        );
    }

    /// An ended connection keeps its slot while frames of it are queued, so
    /// the connection limit bounds what ended connections still queue.
    #[tokio::test]
    async fn an_ended_connection_holds_its_slot_until_its_frames_left() {
        let mut peer = peer();
        let limit = Limit::new(usize::MAX);
        let (_, waker) = counting_waker();
        let mut io = peer.connections.register(1, WINDOW);
        io.permit = limit.try_acquire();
        assert!(matches!(
            poll_write(&mut io, &waker, b"x"),
            Poll::Ready(Ok(1))
        ));
        let ending = io.ending();
        drop(io);
        assert_eq!(limit.open(), 1, "the end holds the slot");
        ending.finish();
        assert_eq!(limit.open(), 1, "held while DATA and ERROR are queued");
        assert_eq!(
            labels(&drain_frames(&mut peer).await),
            ["1 DATA", "1 ERROR"]
        );
        assert_eq!(limit.open(), 0, "freed once they left");

        // nothing queued: freed at once
        let mut io = peer.connections.register(2, WINDOW);
        io.permit = limit.try_acquire();
        peer.error(2, "gone");
        let closed = Pin::new(&mut io).poll_shutdown(&mut Context::from_waker(&waker));
        assert!(matches!(closed, Poll::Ready(Ok(()))));
        drain_frames(&mut peer).await;
        io.ending().finish();
        assert_eq!(limit.open(), 0, "nothing was left to send");

        // the stream ends with frames queued: freed with them
        let mut io = peer.connections.register(3, WINDOW);
        io.permit = limit.try_acquire();
        assert!(matches!(
            poll_write(&mut io, &waker, b"x"),
            Poll::Ready(Ok(1))
        ));
        io.ending().finish();
        assert_eq!(limit.open(), 1);
        peer.connections.teardown();
        assert_eq!(limit.open(), 0, "the teardown frees it");
    }

    /// The byte at each position of the test streams.
    fn stream_bytes(from: usize, len: usize) -> Vec<u8> {
        (from..from + len).map(|i| (i % 251) as u8).collect()
    }

    /// Asserts the inbox holds `bytes` within the bound of a full window.
    fn assert_inbox_bound(io: &TunnelIo, bytes: usize) {
        assert_eq!(io.inbox_bytes(), bytes);
        assert!(
            io.inbox_segments() <= max_segments(WINDOW as usize),
            "segments: {}",
            io.inbox_segments()
        );
        assert!(
            io.inbox_capacity() <= WINDOW as usize + 2 * SEGMENT,
            "capacity: {}",
            io.inbox_capacity()
        );
    }

    #[tokio::test]
    async fn tiny_frames_keep_a_bounded_number_of_segments() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let window = WINDOW as usize;
        for i in 0..window {
            peer.data(1, &stream_bytes(i, 1));
        }
        assert!(
            drain_frames(&mut peer).await.is_empty(),
            "precondition: a full window is no violation"
        );
        assert_inbox_bound(&io, window);
        assert_eq!(
            read_exact(&mut io, window).await.unwrap(),
            stream_bytes(0, window)
        );
        assert_eq!(io.inbox_segments(), 0, "read segments are dropped");
    }

    #[tokio::test]
    async fn frames_of_mixed_sizes_keep_a_bounded_number_of_segments() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let window = WINDOW as usize;
        // sizes around the segment size, so frames straddle segment boundaries
        let sizes = [1, SEGMENT - 1, SEGMENT + 1, 2, 3 * SEGMENT, 7, SEGMENT, 1];
        let mut accepted = 0;
        for size in sizes.iter().cycle() {
            if accepted == window {
                break;
            }
            let size = (*size).min(window - accepted);
            peer.data(1, &stream_bytes(accepted, size));
            accepted += size;
        }
        assert!(
            drain_frames(&mut peer).await.is_empty(),
            "precondition: a full window is no violation"
        );
        assert_inbox_bound(&io, window);
        assert_eq!(
            read_exact(&mut io, window).await.unwrap(),
            stream_bytes(0, window)
        );
    }

    /// A frame of a whole window, half read and granted, then the granted
    /// half again: the read half must not stay allocated beside it.
    #[tokio::test]
    async fn the_read_part_of_a_large_frame_is_released() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let window = WINDOW as usize;
        let half = window / 2;
        peer.data(1, &stream_bytes(0, window));
        assert_eq!(
            read_exact(&mut io, half).await.unwrap(),
            stream_bytes(0, half)
        );
        assert_eq!(
            grants(&drain_frames(&mut peer).await),
            vec![WINDOW / 2],
            "precondition: half the window granted back"
        );
        peer.data(1, &stream_bytes(window, half));
        assert!(
            drain_frames(&mut peer).await.is_empty(),
            "precondition: no violation"
        );
        assert_inbox_bound(&io, window);
        assert_eq!(
            read_exact(&mut io, window).await.unwrap(),
            stream_bytes(half, window)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_frames_and_reads_arrive_byte_exact() {
        use tokio::io::AsyncReadExt as _;
        let peer = Arc::new(peer());
        let mut io = peer.connections.register(1, WINDOW);
        let total = WINDOW as usize;
        let producer = {
            let peer = peer.clone();
            tokio::spawn(async move {
                // a small LCG: deterministic frame sizes from 1 byte to 40KB
                let mut seed: u64 = 58;
                let mut sent = 0;
                while sent < total {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let bound = if seed >> 63 == 0 { 16 } else { 40_000 };
                    let size = (1 + (seed >> 33) as usize % bound).min(total - sent);
                    peer.data(1, &stream_bytes(sent, size));
                    sent += size;
                    // paced, so the reader catches up and waits for frames
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
            })
        };
        let mut seed: u64 = 85;
        let mut buffer = vec![0; 70_000];
        let mut read = 0;
        let start = std::time::Instant::now();
        // no CLOSE before the last byte was read: only the frames wake the reader
        while read < total {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let len = 1 + (seed >> 33) as usize % buffer.len();
            // a missed wake shows as a stall to the timer, whose wake then
            // finds the payload: asserted on the elapsed time below
            let n = tokio::time::timeout(Duration::from_secs(5), io.read(&mut buffer[..len]))
                .await
                .expect("the reader wakes")
                .unwrap();
            assert!(n > 0, "EOF at byte {read}");
            assert_eq!(buffer[..n], stream_bytes(read, n)[..], "at byte {read}");
            read += n;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "every frame wakes the reader: {:?}",
            start.elapsed()
        );
        producer.await.unwrap();
        let flow = peer.connections.lookup(1).unwrap();
        assert!(
            flow.lock().reader_waits > 0,
            "precondition: the reader waited"
        );
        peer.send(Body::Close(Close { conn_id: 1 }));
        assert_eq!(
            io.read(&mut buffer).await.unwrap(),
            0,
            "EOF after the CLOSE"
        );
    }

    #[tokio::test]
    async fn a_violation_drops_the_inbox() {
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        feed(&peer, 1, WINDOW as usize);
        assert_eq!(io.inbox_bytes(), WINDOW as usize, "precondition");
        feed(&peer, 1, 1);
        assert_eq!(io.inbox_capacity(), 0);
        let error = read_exact(&mut io, 1).await.unwrap_err();
        assert_eq!(error.to_string(), WINDOW_EXCEEDED);
    }

    #[tokio::test]
    async fn the_teardown_drops_the_inbox_and_wakes_the_reader() {
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let mut idle = peer.connections.register(2, WINDOW);
        feed(&peer, 1, 1000);
        assert_eq!(io.inbox_bytes(), 1000, "precondition");
        let waiting = tokio::spawn(async move { read_exact(&mut idle, 1).await });
        tokio::task::yield_now().await;
        let flow = peer.connections.lookup(2).unwrap();
        assert!(
            flow.lock().reader.is_some(),
            "precondition: the reader waits"
        );
        peer.connections.teardown();
        assert_eq!(io.inbox_capacity(), 0);
        let error = read_exact(&mut io, 1).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            STREAM_CLOSED,
            "the buffered payload is not read"
        );
        let error = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the waiting reader wakes")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.to_string(), STREAM_CLOSED);
    }

    #[tokio::test]
    async fn the_end_of_the_connection_drops_the_inbox() {
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        feed(&peer, 1, 1000);
        assert_eq!(io.inbox_bytes(), 1000, "precondition");
        io.ending().finish();
        assert_eq!(io.inbox_capacity(), 0);
    }

    #[tokio::test]
    async fn payload_after_the_servers_close_is_not_buffered() {
        use tokio::io::AsyncReadExt as _;
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        peer.data(1, b"ab");
        peer.send(Body::Close(Close { conn_id: 1 }));
        peer.data(1, b"cd");
        assert_eq!(io.inbox_bytes(), 2);
        let mut all = Vec::new();
        io.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"ab");
    }

    #[tokio::test]
    async fn the_servers_error_fails_the_reader_after_the_buffered_payload() {
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        peer.data(1, b"ab");
        peer.error(1, "boom");
        assert_eq!(read_exact(&mut io, 2).await.unwrap(), b"ab");
        let error = read_exact(&mut io, 1).await.unwrap_err();
        assert_eq!(error.to_string(), ABORTED);
    }

    /// The sniff peeks the inbox in place, across frames: nothing is copied
    /// out of the window's accounting.
    #[tokio::test]
    async fn the_sniff_peeks_the_inbox_across_frames() {
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        for (i, byte) in preface.iter().enumerate() {
            peer.data(1, std::slice::from_ref(byte));
            if i == 0 {
                assert!(io.fill().await, "bytes arrived");
            }
        }
        assert_eq!(io.buffered(), preface.len());
        assert!(io.peek_prefix(b"PRI"), "the prefix spans three frames");
        assert!(!io.peek_prefix(b"PRX"));
        assert!(io.peek_prefix(preface), "the whole preface");
        assert_eq!(read_exact(&mut io, preface.len()).await.unwrap(), preface);
    }

    #[tokio::test]
    async fn an_empty_payload_ends_the_reader_after_the_buffered_payload() {
        use tokio::io::AsyncReadExt as _;
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        peer.data(1, b"ab");
        peer.data(1, b"");
        peer.data(1, b"cd");
        let mut all = Vec::new();
        io.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"ab");
    }

    #[tokio::test]
    async fn an_error_after_the_servers_close_still_ends_the_reader_with_eof() {
        use tokio::io::AsyncReadExt as _;
        let peer = peer();
        let mut io = peer.connections.register(1, WINDOW);
        peer.data(1, b"ab");
        peer.send(Body::Close(Close { conn_id: 1 }));
        peer.error(1, "boom");
        let mut all = Vec::new();
        io.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"ab");
    }
}
