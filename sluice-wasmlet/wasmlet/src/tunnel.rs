//! The virtual connections of one tunnel stream: dispatch of the server's
//! frames, per-connection credit flow control, and the byte stream adapter
//! hyper serves each connection over.
//!
//! The flow control follows the protocol of the Java `VirtualConnection`
//! (`sluice-proto`). A connection is windowed when the server's CONNECT
//! announces a receive window:
//!
//! - the server may have at most [`WINDOW`] payload bytes in flight beyond what
//!   this side granted back. Payload past it fails only that connection
//!   (ERROR [`WINDOW_EXCEEDED`]); the receive loop never waits on a windowed
//!   connection;
//! - bytes handed to hyper are granted back (WindowUpdate) once they reach
//!   half the window;
//! - hyper's writes send only against the server's credit; a write without
//!   credit waits on its own task until a grant, the server's ERROR or the
//!   stream's end;
//! - a connection ending before the server's CLOSE / ERROR arrived sends ERROR
//!   ("connection closed"), since the payload the server still sends would be
//!   dropped and never granted.
//!
//! A CONNECT without a window (a server predating the protocol) runs the
//! connection in legacy mode: a bounded channel, on which the receive loop
//! waits, so one slow connection stalls the stream as before.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures_util::stream::{self, PollNext, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_util::sync::PollSender;

use crate::pb::frame::Body;
use crate::pb::{Close, Connect, Data, Error, Frame, WindowUpdate};
use crate::wasm_host::{self, Handler};

/// Receive window of every windowed connection, announced in `Advertise`;
/// the Java side's `VirtualConnection.DEFAULT_WINDOW`.
pub const WINDOW: u32 = 1024 * 1024;

/// ERROR message for payload beyond the granted window; the Java side's
/// `VirtualConnection.WINDOW_EXCEEDED`.
pub const WINDOW_EXCEEDED: &str = "flow control window exceeded";

/// ERROR message of a windowed connection ending while the server may still
/// send.
pub const CONNECTION_CLOSED: &str = "connection closed";

/// Failure of every connection still registered when the stream ends.
const STREAM_CLOSED: &str = "tunnel stream closed";

/// Upper bound for a single DATA frame emitted by the hyper write path.
const CHUNK: usize = 16 * 1024;

/// Legacy connections: frames buffered before the receive loop waits for
/// the connection's reader.
const LEGACY_BUFFER: usize = 32;

/// DATA / CLOSE / connection-end ERROR frames of every connection, in one
/// FIFO so that each connection's frames keep their order.
const DATA_BUFFER: usize = 64;

/// Downstream control for one virtual connection, consumed by its reader.
pub enum Ctrl {
    /// Bytes to write into the connection (hyper's read side).
    Payload(Vec<u8>),
    /// The server half-closed: no more tunnel -> hyper data.
    WriteClose,
    /// The server failed the connection: tear it down.
    Abort,
}

// ---------------------------------------------------------------------------
// Outbound

/// The two lanes of the outbound stream. Control frames (Advertise,
/// WindowUpdate, ERRORs no DATA precedes) go on an unbounded lane that is
/// always sent first, so queueing one never waits; connection frames go on
/// the bounded data lane.
#[derive(Clone)]
pub struct Outbound {
    control: mpsc::UnboundedSender<Frame>,
    data: mpsc::Sender<Frame>,
}

impl Outbound {
    /// The lanes and the request stream merging them, control first.
    pub fn new() -> (Outbound, impl Stream<Item = Frame> + Send + Sync + 'static) {
        let (control, control_rx) = mpsc::unbounded_channel();
        let (data, data_rx) = mpsc::channel(DATA_BUFFER);
        let frames = stream::select_with_strategy(
            UnboundedReceiverStream::new(control_rx),
            ReceiverStream::new(data_rx),
            |_: &mut ()| PollNext::Left,
        );
        (Outbound { control, data }, frames)
    }

    /// Queues a control frame; never waits. Dropped once the stream is gone.
    pub fn control(&self, frame: Frame) {
        let _ = self.control.send(frame);
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
// Per-connection flow state

/// State shared by the dispatch and the connection's reader / writer.
struct Flow {
    /// Receive window announced to the server; 0 = legacy connection.
    window: u32,
    state: Mutex<FlowState>,
}

#[derive(Default)]
struct FlowState {
    /// Payload the server still accepts (windowed).
    credit: u64,
    /// Payload received minus payload granted back (windowed).
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
        }
    }

    fn windowed(&self) -> bool {
        self.window > 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FlowState> {
        self.state.lock().unwrap()
    }

    /// Accounts received payload against the window (windowed only). A
    /// violation resets the connection and wakes its writer.
    fn receive(&self, len: usize) -> Receive {
        let waker = {
            let mut state = self.lock();
            if state.reset || state.remote_failed {
                return Receive::Drop;
            }
            state.outstanding += len as u64;
            if state.outstanding <= u64::from(self.window) {
                return Receive::Accept;
            }
            state.reset = true;
            state.error_sent = true;
            state.failure = Some(WINDOW_EXCEEDED.to_string());
            state.writer.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Receive::Exceeded
    }

    /// Adds the server's grant (saturating) and wakes the writer.
    fn grant(&self, increment: u32) {
        let waker = {
            let mut state = self.lock();
            state.credit = state.credit.saturating_add(u64::from(increment));
            state.writer.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// The server's CLOSE; true the first time.
    fn remote_close(&self) -> bool {
        let mut state = self.lock();
        !std::mem::replace(&mut state.remote_closed, true)
    }

    /// The server's ERROR: writes fail from now on. True the first time.
    fn remote_fail(&self, message: &str) -> bool {
        let (first, waker) = {
            let mut state = self.lock();
            let first = !std::mem::replace(&mut state.remote_failed, true);
            state.failure.get_or_insert_with(|| message.to_string());
            (first, state.writer.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        first
    }

    /// Local teardown: reader and writer fail, nothing more is sent.
    fn reset(&self, message: &str) {
        let waker = {
            let mut state = self.lock();
            state.reset = true;
            state.failure.get_or_insert_with(|| message.to_string());
            state.writer.take()
        };
        if let Some(waker) = waker {
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

enum CtrlTx {
    Windowed(mpsc::UnboundedSender<Ctrl>),
    Legacy(mpsc::Sender<Ctrl>),
}

enum CtrlRx {
    Windowed(mpsc::UnboundedReceiver<Ctrl>),
    Legacy(mpsc::Receiver<Ctrl>),
}

impl CtrlRx {
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Ctrl>> {
        match self {
            CtrlRx::Windowed(rx) => rx.poll_recv(cx),
            CtrlRx::Legacy(rx) => rx.poll_recv(cx),
        }
    }
}

struct Conn {
    ctrl: CtrlTx,
    flow: Arc<Flow>,
}

/// conn id -> registered connection; one per stream attempt, since conn ids
/// are the server's.
type Registry = Arc<Mutex<HashMap<i64, Conn>>>;

/// A send on a legacy connection's bounded channel; the receive loop awaits
/// it after the dispatch returned.
pub struct Deferred(mpsc::Sender<Ctrl>, Ctrl);

impl Deferred {
    async fn run(self) {
        let _ = self.0.send(self.1).await;
    }
}

/// The virtual connections of one stream attempt.
pub struct Connections {
    /// Session label for the log.
    key: String,
    outbound: Outbound,
    /// Dial address (the advertised target) -> its handler.
    routes: Arc<HashMap<String, Arc<Handler>>>,
    registry: Registry,
}

impl Connections {
    pub fn new(
        key: String,
        outbound: Outbound,
        routes: Arc<HashMap<String, Arc<Handler>>>,
    ) -> Self {
        Connections {
            key,
            outbound,
            routes,
            registry: Registry::default(),
        }
    }

    /// Hands one connection frame of the server to its connection. Waits
    /// only for a legacy connection's reader; never for a windowed one.
    pub async fn deliver(&self, body: Body) {
        if let Some(deferred) = self.dispatch(body) {
            deferred.run().await;
        }
    }

    /// Routes one frame to its connection. A plain function: it cannot await,
    /// so a slow connection never stalls the receive loop here; keep it
    /// non-`async`. The legacy channel's send is returned for the caller.
    fn dispatch(&self, body: Body) -> Option<Deferred> {
        match body {
            Body::Connect(connect) => {
                self.connect(connect);
                None
            }
            Body::Data(data) => self.data(data),
            Body::Close(close) => {
                let (ctrl, flow) = self.lookup(close.conn_id)?;
                if !flow.remote_close() {
                    return None;
                }
                send(ctrl, Ctrl::WriteClose)
            }
            Body::Error(error) => {
                println!(
                    "[{}] conn {} failed: {}",
                    self.key, error.conn_id, error.message
                );
                let (ctrl, flow) = self.lookup(error.conn_id)?;
                if !flow.remote_fail(&error.message) {
                    return None;
                }
                send(ctrl, Ctrl::Abort)
            }
            Body::WindowUpdate(update) => {
                // ignored for unknown and legacy connections, as on the Java side
                if let Some((_, flow)) = self.lookup(update.conn_id)
                    && flow.windowed()
                {
                    flow.grant(update.increment);
                }
                None
            }
            // session frames are the receive loop's; KeepAlive is a no-op
            _ => None,
        }
    }

    fn lookup(&self, conn_id: i64) -> Option<(CtrlTx, Arc<Flow>)> {
        let registry = self.registry.lock().unwrap();
        let conn = registry.get(&conn_id)?;
        let ctrl = match &conn.ctrl {
            CtrlTx::Windowed(tx) => CtrlTx::Windowed(tx.clone()),
            CtrlTx::Legacy(tx) => CtrlTx::Legacy(tx.clone()),
        };
        Some((ctrl, conn.flow.clone()))
    }

    fn connect(&self, connect: Connect) {
        let Some(handler) = self.routes.get(&connect.address) else {
            self.outbound.control(error_frame(
                connect.conn_id,
                &format!("no wasm route for '{}'", connect.address),
            ));
            return;
        };
        let io = self.register(connect.conn_id, connect.connection_window);
        tokio::spawn(wasm_host::serve(io, handler.clone()));
    }

    fn data(&self, data: Data) -> Option<Deferred> {
        let (ctrl, flow) = self.lookup(data.conn_id)?;
        let tx = match ctrl {
            CtrlTx::Legacy(tx) => return Some(Deferred(tx, Ctrl::Payload(data.payload))),
            CtrlTx::Windowed(tx) => tx,
        };
        match flow.receive(data.payload.len()) {
            Receive::Accept => {
                let _ = tx.send(Ctrl::Payload(data.payload));
            }
            Receive::Drop => {}
            Receive::Exceeded => {
                eprintln!("[{}] conn {}: {WINDOW_EXCEEDED}", self.key, data.conn_id);
                self.outbound
                    .control(error_frame(data.conn_id, WINDOW_EXCEEDED));
                // drops the sender: the reader wakes and fails
                self.registry.lock().unwrap().remove(&data.conn_id);
            }
        }
        None
    }

    /// Registers a connection and returns the byte stream hyper serves it
    /// over. `server_window` > 0 makes it windowed, with that initial credit.
    fn register(&self, conn_id: i64, server_window: u32) -> TunnelIo {
        let (ctrl, rx, flow) = if server_window > 0 {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                CtrlTx::Windowed(tx),
                CtrlRx::Windowed(rx),
                Flow::new(WINDOW, u64::from(server_window)),
            )
        } else {
            let (tx, rx) = mpsc::channel(LEGACY_BUFFER);
            (CtrlTx::Legacy(tx), CtrlRx::Legacy(rx), Flow::new(0, 0))
        };
        let flow = Arc::new(flow);
        self.registry.lock().unwrap().insert(
            conn_id,
            Conn {
                ctrl,
                flow: flow.clone(),
            },
        );
        TunnelIo {
            conn_id,
            rx,
            outbound: self.outbound.clone(),
            data: PollSender::new(self.outbound.data.clone()),
            flow,
            registry: self.registry.clone(),
            buf: bytes::BytesMut::new(),
            eof: false,
            consumed: 0,
        }
    }

    /// The stream ended: fails every connection without waiting. Readers and
    /// writers wake with an error; nothing more is sent for them.
    pub fn teardown(&self) {
        let drained: Vec<_> = self.registry.lock().unwrap().drain().collect();
        for (_, conn) in drained {
            conn.flow.reset(STREAM_CLOSED);
        }
    }
}

/// Windowed: an unbounded send, done here. Legacy: deferred to the caller.
fn send(ctrl: CtrlTx, message: Ctrl) -> Option<Deferred> {
    match ctrl {
        CtrlTx::Windowed(tx) => {
            let _ = tx.send(message);
            None
        }
        CtrlTx::Legacy(tx) => Some(Deferred(tx, message)),
    }
}

// ---------------------------------------------------------------------------
// Tunnel <-> hyper byte stream adapter

/// AsyncRead/AsyncWrite over one virtual connection: reads consume `Ctrl`
/// messages, writes emit DATA frames (against the server's credit when
/// windowed).
pub struct TunnelIo {
    conn_id: i64,
    rx: CtrlRx,
    outbound: Outbound,
    data: PollSender<Frame>,
    flow: Arc<Flow>,
    registry: Registry,
    buf: bytes::BytesMut,
    eof: bool,
    /// Bytes handed to hyper and not yet granted back (windowed).
    consumed: u64,
}

impl TunnelIo {
    pub fn conn_id(&self) -> i64 {
        self.conn_id
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    pub fn peek_prefix(&self, prefix: &[u8]) -> bool {
        self.buf.starts_with(prefix)
    }

    /// Pulls one control message into the buffer; false on close/abort.
    pub async fn fill(&mut self) -> bool {
        match std::future::poll_fn(|cx| self.rx.poll_recv(cx)).await {
            Some(Ctrl::Payload(data)) => {
                self.buf.extend_from_slice(&data);
                true
            }
            Some(Ctrl::WriteClose) => {
                self.eof = true;
                false
            }
            Some(Ctrl::Abort) | None => false,
        }
    }

    /// What the serve task needs once hyper is done with the connection.
    pub fn ending(&self) -> Ending {
        Ending {
            conn_id: self.conn_id,
            flow: self.flow.clone(),
            data: self.outbound.data.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Hands up to `buf.remaining()` buffered bytes to hyper.
    fn hand_off(&mut self, buf: &mut ReadBuf<'_>) {
        let n = self.buf.len().min(buf.remaining());
        let data = self.buf.split_to(n);
        buf.put_slice(&data);
        self.consumed(n);
    }

    /// Grants the consumed bytes back once they reach half the window. The
    /// grant goes on the control lane: it never waits.
    fn consumed(&mut self, n: usize) {
        if !self.flow.windowed() {
            return;
        }
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
            })),
        });
    }

    fn send_close(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        {
            let state = self.flow.lock();
            if state.close_sent || state.error_sent || state.reset {
                return Poll::Ready(Ok(()));
            }
        }
        match self.data.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                self.flow.lock().close_sent = true;
                let _ = self.data.send_item(close_frame(self.conn_id));
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::other(e.to_string()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for TunnelIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if let Some(failure) = this.flow.reset_failure() {
            return Poll::Ready(Err(std::io::Error::other(failure)));
        }
        if !this.buf.is_empty() {
            this.hand_off(buf);
            return Poll::Ready(Ok(()));
        }
        if this.eof {
            // Unfilled ReadBuf on return signals EOF to hyper.
            return Poll::Ready(Ok(()));
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(Ctrl::Payload(data))) => {
                this.buf.extend_from_slice(&data);
                this.hand_off(buf);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ctrl::WriteClose)) => {
                this.eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ctrl::Abort)) => Poll::Ready(Err(std::io::Error::other(
                "connection aborted by the tunnel peer",
            ))),
            Poll::Ready(None) => match this.flow.reset_failure() {
                Some(failure) => Poll::Ready(Err(std::io::Error::other(failure))),
                None => {
                    this.eof = true;
                    Poll::Ready(Ok(()))
                }
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TunnelIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        let windowed = this.flow.windowed();
        if windowed {
            let mut state = this.flow.lock();
            if let Some(failure) = &state.failure {
                return Poll::Ready(Err(std::io::Error::other(failure.clone())));
            }
            if state.credit == 0 {
                // a grant, the server's ERROR or the teardown wakes it
                state.writer = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }
        match this.data.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => {
                return Poll::Ready(Err(std::io::Error::other(format!("tunnel closed: {e}"))));
            }
            Poll::Pending => return Poll::Pending,
        }
        let n = if windowed {
            let mut state = this.flow.lock();
            if let Some(failure) = &state.failure {
                let failure = failure.clone();
                drop(state);
                this.data.abort_send();
                return Poll::Ready(Err(std::io::Error::other(failure)));
            }
            // only this writer takes credit: it is still >= what was seen
            let n = buf.len().min(CHUNK).min(state.credit as usize);
            state.credit -= n as u64;
            n
        } else {
            buf.len().min(CHUNK)
        };
        let _ = this.data.send_item(Frame {
            body: Some(Body::Data(Data {
                conn_id: this.conn_id,
                payload: buf[..n].to_vec(),
            })),
        });
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // Half-close the tunnel direction when hyper is done with the connection.
        self.send_close(cx)
    }
}

/// The end of one connection, run by its serve task after hyper returned.
pub struct Ending {
    conn_id: i64,
    flow: Arc<Flow>,
    data: mpsc::Sender<Frame>,
    registry: Registry,
}

impl Ending {
    /// Unregisters the connection and sends its terminal frame on the data
    /// lane, after its DATA: ERROR ("connection closed") for a windowed
    /// connection the server may still send on, else CLOSE unless one went
    /// out (hyper skips its shutdown on an error).
    pub async fn finish(self) {
        self.registry.lock().unwrap().remove(&self.conn_id);
        let frame = {
            let mut state = self.flow.lock();
            if state.reset || state.error_sent {
                None
            } else if self.flow.window > 0 && !state.remote_closed && !state.remote_failed {
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
            let _ = self.data.send(frame).await;
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
            let (outbound, frames) = Outbound::new();
            Peer {
                connections: Connections::new("test".into(), outbound, Arc::new(routes)),
                frames: Box::pin(frames),
                auto_grant: true,
            }
        }

        /// One frame from the server, delivered with the receive loop's
        /// semantics.
        pub(crate) async fn send(&self, body: Body) {
            self.connections.deliver(body).await;
        }

        pub(crate) async fn connect(&self, conn_id: i64, address: &str, window: u32) {
            self.send(Body::Connect(Connect {
                conn_id,
                address: address.into(),
                connection_window: window,
            }))
            .await;
        }

        pub(crate) async fn data(&self, conn_id: i64, payload: &[u8]) {
            self.send(Body::Data(Data {
                conn_id,
                payload: payload.to_vec(),
            }))
            .await;
        }

        pub(crate) async fn error(&self, conn_id: i64, message: &str) {
            self.send(Body::Error(Error {
                conn_id,
                message: message.into(),
            }))
            .await;
        }

        /// The next frame the wasmlet sent; None once every sender is gone.
        pub(crate) async fn recv(&mut self) -> Option<Frame> {
            use futures_util::StreamExt as _;
            let frame = self.frames.next().await?;
            if let (true, Some(Body::Data(data))) = (self.auto_grant, &frame.body) {
                self.connections.dispatch(Body::WindowUpdate(WindowUpdate {
                    conn_id: data.conn_id,
                    increment: data.payload.len() as u32,
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

    fn data_body(conn_id: i64, len: usize) -> Body {
        Body::Data(Data {
            conn_id,
            payload: vec![0x5a; len],
        })
    }

    /// Feeds `total` bytes to `conn_id` in 16KiB frames (more frames than a
    /// bounded channel would buffer); panics when the receive loop would
    /// wait.
    async fn feed(peer: &Peer, conn_id: i64, total: usize) {
        let mut left = total;
        while left > 0 {
            let n = left.min(CHUNK);
            tokio::time::timeout(Duration::from_secs(1), peer.send(data_body(conn_id, n)))
                .await
                .expect("the receive loop never waits on a windowed connection");
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
        feed(&peer, 1, WINDOW as usize).await;
        feed(&peer, 2, 5).await;

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
        feed(&peer, 1, WINDOW as usize).await;
        assert!(
            drain_frames(&mut peer).await.is_empty(),
            "a full window is no violation"
        );

        feed(&peer, 1, 1).await;

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
        feed(&peer, 1, 64 * 1024).await;
        feed(&peer, 2, 3).await;
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
        }))
        .await;
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the grant wakes the writer"
        );
        let Poll::Ready(Ok(n)) = poll_write(&mut io, &waker, &[2; 6]) else {
            panic!("the grant is sendable");
        };
        assert_eq!(n, 4);

        let sizes: Vec<usize> = drain_frames(&mut peer)
            .await
            .into_iter()
            .map(|body| match body {
                Body::Data(data) => data.payload.len(),
                other => panic!("only DATA: {other:?}"),
            })
            .collect();
        assert_eq!(sizes, vec![10, 4], "never more than the credit on the wire");
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

        peer.error(1, "public peer reset").await;

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
        feed(&peer, 1, WINDOW as usize).await;

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
        feed(&peer, 1, WINDOW as usize).await;
        read_exact(&mut io, WINDOW as usize).await.unwrap();
        let frames = drain_frames(&mut peer).await;
        assert_eq!(grants(&frames), vec![WINDOW / 2, WINDOW / 2], "{frames:?}");
    }

    #[tokio::test]
    async fn a_legacy_connection_neither_grants_nor_enforces_a_window() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, 0);
        let total = 4 * WINDOW as usize;
        let reader = tokio::spawn(async move {
            let received = read_exact(&mut io, total).await.map(|bytes| bytes.len());
            (io, received)
        });
        // the receive loop waits for the reader here (legacy backpressure)
        let mut left = total;
        while left > 0 {
            let n = left.min(64 * 1024);
            peer.send(data_body(1, n)).await;
            left -= n;
        }
        let (_io, received) = reader.await.unwrap();
        assert_eq!(received.unwrap(), total);
        assert!(
            drain_frames(&mut peer).await.is_empty(),
            "no WindowUpdate, no ERROR"
        );
    }

    #[tokio::test]
    async fn a_legacy_connection_backpressures_the_receive_loop() {
        let peer = peer();
        let _never_read = peer.connections.register(1, 0);
        for _ in 0..LEGACY_BUFFER {
            peer.send(data_body(1, 1)).await;
        }
        let waited = tokio::time::timeout(Duration::from_millis(200), peer.send(data_body(1, 1)))
            .await
            .is_err();
        assert!(waited, "the bounded channel is kept for legacy peers");
    }

    #[tokio::test]
    async fn a_legacy_write_ignores_credit() {
        let mut peer = peer();
        peer.auto_grant = false;
        let mut io = peer.connections.register(1, 0);
        // two windows' worth, never granted: no credit applies
        for _ in 0..(2 * WINDOW as usize / CHUNK) {
            let written = tokio::time::timeout(
                Duration::from_secs(1),
                std::future::poll_fn(|cx| Pin::new(&mut io).poll_write(cx, &[0; 64 * 1024])),
            )
            .await
            .expect("no credit wait");
            assert_eq!(written.unwrap(), CHUNK);
            assert!(matches!(
                peer.recv().await.and_then(|frame| frame.body),
                Some(Body::Data(data)) if data.payload.len() == CHUNK
            ));
        }
    }

    #[tokio::test]
    async fn control_frames_overtake_queued_data() {
        let (outbound, frames) = Outbound::new();
        let mut frames = Box::pin(frames);
        for _ in 0..DATA_BUFFER {
            outbound.data.try_send(close_frame(1)).unwrap();
        }
        outbound.control(error_frame(2, "x"));
        use futures_util::StreamExt as _;
        let first = frames.next().await.unwrap();
        assert!(
            matches!(first.body, Some(Body::Error(_))),
            "control first: {first:?}"
        );
    }

    #[tokio::test]
    async fn a_connect_without_a_route_is_answered_on_the_control_lane() {
        let (outbound, frames) = Outbound::new();
        let mut frames = Box::pin(frames);
        // the data lane is full: an answer queued there would wait
        for _ in 0..DATA_BUFFER {
            outbound.data.try_send(close_frame(9)).unwrap();
        }
        let connections = Connections::new("test".into(), outbound, Arc::default());
        let dispatched = connections.dispatch(Body::Connect(Connect {
            conn_id: 7,
            address: "wasm:missing".into(),
            connection_window: WINDOW,
        }));
        assert!(dispatched.is_none());
        use futures_util::StreamExt as _;
        let first = frames.next().await.unwrap();
        assert!(
            matches!(&first.body, Some(Body::Error(error))
                if error.conn_id == 7 && error.message.contains("no wasm route")),
            "{first:?}"
        );
    }

    #[tokio::test]
    async fn teardown_never_waits_on_a_full_legacy_channel() {
        let peer = peer();
        let mut io = peer.connections.register(1, 0);
        for _ in 0..LEGACY_BUFFER {
            peer.send(data_body(1, 1)).await;
        }
        peer.connections.teardown();
        let error = read_exact(&mut io, 1).await.unwrap_err();
        assert_eq!(error.to_string(), STREAM_CLOSED);
    }

    async fn finish(io: TunnelIo) {
        io.ending().finish().await;
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
        peer.send(Body::Close(Close { conn_id: 1 })).await;
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(matches!(&frames[..], [Body::Close(_)]), "{frames:?}");
    }

    #[tokio::test]
    async fn a_windowed_connection_ending_after_the_servers_error_sends_no_error() {
        let mut peer = peer();
        let io = peer.connections.register(1, WINDOW);
        peer.error(1, "gone").await;
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(matches!(&frames[..], [Body::Close(_)]), "{frames:?}");
    }

    #[tokio::test]
    async fn a_legacy_connection_ending_sends_close_once() {
        let mut peer = peer();
        let mut io = peer.connections.register(1, 0);
        for _ in 0..2 {
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx))
                .await
                .unwrap();
        }
        finish(io).await;
        let frames = drain_frames(&mut peer).await;
        assert!(matches!(&frames[..], [Body::Close(_)]), "{frames:?}");
    }
}
