//! The output of the processes `/init` starts: their stdout and stderr, read from pipes
//! as whole lines and sent to the wicket on the log stream (`wire::LOG_PORT`), which the
//! wicket dials -- at boot, before the process starts, and again after each restore of a
//! clone (a restore resets every vsock stream). Lines go on the newest stream.
//!
//! The lines wait for the stream in a bounded buffer, so a process never waits for the
//! wicket: past [`MAX_LINES`] or [`MAX_BYTES`] a line is dropped and counted, the count
//! sent as a gap in its place. The wicket's sync has the lines written before it sent
//! first (the pipes read to their end of the moment); it takes a template's lines
//! before the snapshot. A restore notice discards what the pipes and the buffer still
//! hold, the snapshot's: a clone sends its own lines only.

use std::collections::VecDeque;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::unix::pipe;
use tokio::sync::{Notify, mpsc};
use tokio_vsock::VsockListener;

use crate::wire::{self, LogFrame, Stream};

/// Lines waiting for the stream at most.
const MAX_LINES: usize = 4096;

/// Their bytes at most.
const MAX_BYTES: usize = 1024 * 1024;

/// Bytes of lines sent in one write at most (one is sent whole).
const BATCH: usize = 64 * 1024;

pub struct Log {
    state: Mutex<State>,
    /// Signalled when an item was queued.
    queued: Notify,
    /// The pipes being read.
    pipes: Mutex<Vec<Arc<Pipe>>>,
}

#[derive(Default)]
struct State {
    queue: VecDeque<LogFrame>,
    /// The lines in `queue`, and their bytes.
    lines: usize,
    bytes: usize,
    /// Counts the restores: what was taken before one is the snapshot's.
    epoch: u64,
}

impl State {
    fn add(&mut self, frame: LogFrame) {
        if let LogFrame::Line(_, line) = &frame {
            self.lines += 1;
            self.bytes += line.len();
        }
        self.queue.push_back(frame);
    }

    fn remove(&mut self) -> Option<LogFrame> {
        let frame = self.queue.pop_front()?;
        if let LogFrame::Line(_, line) = &frame {
            self.lines -= 1;
            self.bytes -= line.len();
        }
        Some(frame)
    }
}

/// A pipe a process writes its output to.
struct Pipe {
    stream: Stream,
    receiver: pipe::Receiver,
    /// The start of a line not ended yet.
    partial: Mutex<Vec<u8>>,
}

impl Pipe {
    /// Reads what the pipe holds now, without waiting: its lines queued to `log`, or
    /// discarded (`keep` false). Whether the pipe is still open.
    fn drain(&self, log: &Log, keep: bool) -> std::io::Result<bool> {
        let mut partial = lock(&self.partial);
        let mut buffer = [0u8; 4096];
        loop {
            let n = match self.receiver.try_read(&mut buffer) {
                Ok(0) => return Ok(false),
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if !keep {
                continue;
            }
            let mut rest = &buffer[..n];
            while let Some(newline) = rest.iter().position(|byte| *byte == b'\n') {
                partial.extend_from_slice(&rest[..newline]);
                log.line(self.stream, &partial);
                partial.clear();
                rest = &rest[newline + 1..];
            }
            partial.extend_from_slice(rest);
            // a line without end goes in pieces
            if partial.len() >= wire::MAX_LINE {
                log.line(self.stream, &partial);
                partial.clear();
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // a panic under a lock leaves the state consistent enough for a log
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

impl Log {
    pub fn new() -> Arc<Log> {
        Arc::new(Log {
            state: Mutex::default(),
            queued: Notify::new(),
            pipes: Mutex::default(),
        })
    }

    /// Queues a line of `stream` (cut in pieces of at most `wire::MAX_LINE`); past the
    /// buffer's bounds it is counted dropped. Never waits.
    pub fn line(&self, stream: Stream, line: &[u8]) {
        let mut state = lock(&self.state);
        if line.is_empty() {
            Self::queue(&mut state, stream, line);
        }
        for piece in line.chunks(wire::MAX_LINE) {
            Self::queue(&mut state, stream, piece);
        }
        drop(state);
        self.queued.notify_one();
    }

    fn queue(state: &mut State, stream: Stream, line: &[u8]) {
        if state.lines < MAX_LINES && state.bytes + line.len() <= MAX_BYTES {
            state.add(LogFrame::Line(stream, line.to_vec()));
            return;
        }
        if !matches!(state.queue.back(), Some(LogFrame::Gap { .. })) {
            state.queue.push_back(LogFrame::Gap {
                stdout: 0,
                stderr: 0,
            });
        }
        if let Some(LogFrame::Gap { stdout, stderr }) = state.queue.back_mut() {
            match stream {
                Stream::Stdout => *stdout += 1,
                Stream::Stderr => *stderr += 1,
            }
        }
    }

    /// Reads `output` (a process's stdout or stderr: the read end of its pipe) to its
    /// end, its lines queued; a last line without its newline is one too.
    pub fn capture(self: &Arc<Self>, stream: Stream, output: impl Into<OwnedFd>) {
        let receiver = match pipe::Receiver::from_owned_fd(output.into()) {
            Ok(receiver) => receiver,
            Err(e) => {
                eprintln!("guest: the {} pipe: {e}", stream.name());
                return;
            }
        };
        let pipe = Arc::new(Pipe {
            stream,
            receiver,
            partial: Mutex::default(),
        });
        lock(&self.pipes).push(pipe.clone());
        let log = self.clone();
        tokio::spawn(async move {
            while pipe.receiver.readable().await.is_ok() {
                if !matches!(pipe.drain(&log, true), Ok(true)) {
                    break;
                }
            }
            lock(&log.pipes).retain(|other| !Arc::ptr_eq(other, &pipe));
            let partial = std::mem::take(&mut *lock(&pipe.partial));
            if !partial.is_empty() {
                log.line(stream, &partial);
            }
        });
    }

    fn pipes(&self) -> Vec<Arc<Pipe>> {
        lock(&self.pipes).clone()
    }

    /// The wicket's sync: queues the lines the pipes hold now, then the answer.
    fn sync(&self) {
        for pipe in self.pipes() {
            let _ = pipe.drain(self, true);
        }
        lock(&self.state).add(LogFrame::Synced);
        self.queued.notify_one();
    }

    /// A restore notice: what the pipes and the buffer hold are the snapshot's, its
    /// template's lines (or a line's start), discarded.
    pub fn restored(&self) {
        for pipe in self.pipes() {
            let _ = pipe.drain(self, false);
            lock(&pipe.partial).clear();
        }
        let mut state = lock(&self.state);
        state.queue.clear();
        state.lines = 0;
        state.bytes = 0;
        state.epoch += 1;
    }

    /// The frames to send next: [`BATCH`] bytes of lines at most, at least one frame;
    /// with the epoch they were taken in.
    fn take(&self) -> (Vec<LogFrame>, u64) {
        let mut state = lock(&self.state);
        let mut frames = Vec::new();
        let mut bytes = 0;
        while bytes < BATCH
            && let Some(frame) = state.remove()
        {
            if let LogFrame::Line(_, line) = &frame {
                bytes += line.len();
            }
            frames.push(frame);
        }
        (frames, state.epoch)
    }

    /// Puts `frames` back at the queue's front: they were not sent. Unless a restore
    /// came since they were taken: they were the snapshot's.
    fn untake(&self, frames: Vec<LogFrame>, epoch: u64) {
        let mut state = lock(&self.state);
        if state.epoch != epoch {
            return;
        }
        for frame in frames.into_iter().rev() {
            if let LogFrame::Line(_, line) = &frame {
                state.lines += 1;
                state.bytes += line.len();
            }
            state.queue.push_front(frame);
        }
    }

    /// Waits for a frame queued.
    async fn queued(&self) {
        // a frame queued meanwhile left a permit: no wake-up is lost
        while lock(&self.state).queue.is_empty() {
            self.queued.notified().await;
        }
    }

    /// Serves the log streams the wicket dials on `listener`, for good.
    pub async fn serve(self: Arc<Self>, listener: VsockListener) {
        let (streams, dialed) = mpsc::channel(1);
        tokio::spawn(self.send(dialed));
        loop {
            if let Ok((stream, _)) = listener.accept().await
                && streams.send(stream).await.is_err()
            {
                return;
            }
        }
    }

    /// Sends the queue on the newest of `streams` until they end. A stream failing (a
    /// restore resets it) is given up, what it did not take kept for the next.
    pub async fn send<S>(self: Arc<Self>, mut streams: mpsc::Receiver<S>)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        enum Event<S> {
            Dialed(Option<S>),
            Read(std::io::Result<u8>),
            Queued,
        }
        let mut current = None;
        let mut out = Vec::new();
        loop {
            let Some(stream) = current.as_mut() else {
                match streams.recv().await {
                    Some(stream) => current = Some(stream),
                    None => return,
                }
                continue;
            };
            let event = tokio::select! {
                dialed = streams.recv() => Event::Dialed(dialed),
                // one byte: cancelled, it read nothing
                read = stream.read_u8() => Event::Read(read),
                () = self.queued() => Event::Queued,
            };
            match event {
                Event::Dialed(Some(newer)) => current = Some(newer),
                Event::Dialed(None) => return,
                Event::Read(Ok(wire::SYNC)) => self.sync(),
                // reset, ended, or not the wicket's
                Event::Read(_) => current = None,
                Event::Queued => {
                    let (frames, epoch) = self.take();
                    out.clear();
                    for frame in &frames {
                        wire::put_log_frame(&mut out, frame);
                    }
                    if stream.write_all(&out).await.is_err() {
                        self.untake(frames, epoch);
                        current = None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use tokio::io::BufReader;
    use tokio::net::UnixStream;

    use super::*;

    const HANG: Duration = Duration::from_secs(10);

    /// The wicket's side of a log stream.
    struct Wicket(BufReader<UnixStream>);

    impl Wicket {
        async fn dial(streams: &mpsc::Sender<UnixStream>) -> Wicket {
            let (wicket, guest) = UnixStream::pair().unwrap();
            streams.send(guest).await.unwrap();
            Wicket(BufReader::new(wicket))
        }

        async fn next(&mut self) -> LogFrame {
            tokio::time::timeout(HANG, wire::read_log_frame(&mut self.0))
                .await
                .expect("hang guard")
                .unwrap()
                .expect("a frame")
        }

        /// The frames up to the answer to a sync.
        async fn sync(&mut self) -> Vec<LogFrame> {
            self.0.get_mut().write_all(&[wire::SYNC]).await.unwrap();
            let mut frames = Vec::new();
            loop {
                match self.next().await {
                    LogFrame::Synced => return frames,
                    frame => frames.push(frame),
                }
            }
        }
    }

    fn line(stream: Stream, text: &str) -> LogFrame {
        LogFrame::Line(stream, text.as_bytes().to_vec())
    }

    /// Lines wait for a stream within the buffer's bounds, the rest counted in a gap
    /// in place, by stream. A process's stdout and stderr reach the stream as lines,
    /// its last line without a newline too, an overlong one in pieces; a sync answers
    /// once what was written before it was sent. A restore discards what waits, the
    /// snapshot's, and the newest stream takes the lines.
    #[tokio::test]
    async fn the_output_reaches_the_newest_stream_as_lines_bounded() {
        let log = Log::new();
        let (streams, dialed) = mpsc::channel(1);
        tokio::spawn(log.clone().send(dialed));
        for n in 0..MAX_LINES + 2 {
            log.line(Stream::Stdout, n.to_string().as_bytes());
        }
        log.line(Stream::Stderr, b"dropped too");
        let mut wicket = Wicket::dial(&streams).await;
        let frames = wicket.sync().await;
        assert_eq!(frames.len(), MAX_LINES + 1);
        assert_eq!(frames[0], line(Stream::Stdout, "0"));
        assert_eq!(
            frames[MAX_LINES],
            LogFrame::Gap {
                stdout: 2,
                stderr: 1
            }
        );

        let long = "x".repeat(wire::MAX_LINE + 1);
        let mut child = Command::new("sh")
            .args([
                "-c",
                &format!("echo out; echo err >&2; echo {long}; printf tail"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        log.capture(Stream::Stdout, child.stdout.take().unwrap());
        log.capture(Stream::Stderr, child.stderr.take().unwrap());
        assert!(child.wait().unwrap().success());
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while stdout.len() < 4 || stderr.is_empty() {
            match wicket.next().await {
                LogFrame::Line(Stream::Stdout, line) => stdout.push(line),
                LogFrame::Line(Stream::Stderr, line) => stderr.push(line),
                frame => panic!("{frame:?}"),
            }
        }
        assert_eq!(
            stdout,
            [
                b"out".to_vec(),
                long.as_bytes()[..wire::MAX_LINE].to_vec(),
                b"x".to_vec(),
                b"tail".to_vec()
            ]
        );
        assert_eq!(stderr, [b"err".to_vec()]);

        // in the pipe, read or not yet, before the sync's answer
        let (reader, mut writer) = std::io::pipe().unwrap();
        log.capture(Stream::Stdout, reader);
        writer.write_all(b"before\n").unwrap();
        assert_eq!(wicket.sync().await, [line(Stream::Stdout, "before")]);

        // the restore resets the stream: what waits is the template's
        log.line(Stream::Stdout, b"the template's");
        drop(wicket);
        log.restored();
        log.line(Stream::Stdout, b"the clone's");
        let mut wicket = Wicket::dial(&streams).await;
        assert_eq!(wicket.sync().await, [line(Stream::Stdout, "the clone's")]);
    }
}
