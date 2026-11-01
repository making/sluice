//! The framing on every vsock stream between the wicket and the guest's `/init`.
//!
//! Firecracker's vsock device does not carry a half-close: the host side's EOF reaches
//! the guest as a full shutdown (its unsent answer is lost), and a guest's
//! `shutdown(SHUT_WR)` never reaches the host. A tunnel connection's CLOSE therefore
//! travels in band: `[type: u8][len: u32 BE][payload]`, `DATA` or `CLOSE` (no payload).
//! A stream ending without a `CLOSE` from its side failed.
//!
//! The guest's output travels on a stream of its own, framed alike ([`LOG_PORT`]).
//!
//! The guest has a copy of this module (`guest/src/wire.rs`): keep them alike.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const DATA: u8 = 0;
const CLOSE: u8 = 1;

/// The log stream's frames: a line of the processes' stdout, of their stderr, the lines
/// dropped, the answer to a sync -- whose request, the wicket's, is this one byte.
const STDOUT: u8 = 2;
const STDERR: u8 = 3;
const GAP: u8 = 4;
pub const SYNC: u8 = 5;

/// A frame's head: its type and its payload's length.
const HEAD: usize = 5;

/// The most payload [`relay`] puts in a frame.
const MAX_PAYLOAD: usize = 16 * 1024;

/// The vsock port of the guest's control stream, which carries the wicket's
/// notice to a VM just restored from a snapshot (a clone), [`Notice`], answered `OK\n`
/// once the guest set its clock (given a time), put the seed into its kernel's random
/// pool and reseeded it, and ran the image's restore hook. A clone's memory is its
/// snapshot's, random state included: the kernel reseeds on the VM generation
/// change (VMGenID) a moment after the restore, a process's own generator never.
pub const CONTROL_PORT: u32 = 1024;

/// The vsock port of the guest's log stream: the output of the processes its `/init`
/// starts, as lines. The wicket dials it once `/init` runs, before the process starts
/// (and a clone's after its restore notice: a restore resets every stream); `/init` sends
/// its lines on the newest stream. Its frames ([`LogFrame`]) are a line (`STDOUT` /
/// `STDERR`, without its newline, at most [`MAX_LINE`] bytes: a longer line goes in
/// pieces), the lines dropped there (`GAP`: stdout's and stderr's, two u64 BE) and
/// `SYNC` (no payload): the answer to the wicket's byte `SYNC` once every line the
/// processes wrote before it was sent.
pub const LOG_PORT: u32 = 1025;

/// The longest line of the log stream.
pub const MAX_LINE: usize = 16 * 1024;

/// A process's output stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    /// Its name: a line's prefix and its kind in the live access log.
    #[allow(dead_code)] // the wicket's: each copy has it
    pub fn name(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

/// A frame of the log stream.
#[derive(Debug, PartialEq, Eq)]
pub enum LogFrame {
    /// A line of a process's output, without its newline.
    Line(Stream, Vec<u8>),
    /// The lines dropped here: the guest's buffer of them was full.
    Gap { stdout: u64, stderr: u64 },
    /// Every line written before the wicket's sync came before this.
    Synced,
}

/// Appends `frame` to `out`.
#[allow(dead_code)] // the guest's half: each copy has both
pub fn put_log_frame(out: &mut Vec<u8>, frame: &LogFrame) {
    let (kind, payload): (u8, &[u8]) = match frame {
        LogFrame::Line(Stream::Stdout, line) => (STDOUT, line),
        LogFrame::Line(Stream::Stderr, line) => (STDERR, line),
        LogFrame::Gap { stdout, stderr } => {
            out.push(GAP);
            out.extend_from_slice(&16u32.to_be_bytes());
            out.extend_from_slice(&stdout.to_be_bytes());
            out.extend_from_slice(&stderr.to_be_bytes());
            return;
        }
        LogFrame::Synced => (SYNC, &[]),
    };
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
}

/// The next frame of a log stream; None once it ended between frames. A frame of an
/// unknown type, or longer than its type allows, is an error: what the wicket takes of
/// a guest is bounded.
#[allow(dead_code)] // the wicket's half: each copy has both
pub async fn read_log_frame<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> std::io::Result<Option<LogFrame>> {
    if reader.fill_buf().await?.is_empty() {
        return Ok(None);
    }
    let mut head = [0u8; HEAD];
    reader.read_exact(&mut head).await?;
    let len = u32::from_be_bytes(head[1..].try_into().expect("4 bytes")) as usize;
    let fits = match head[0] {
        STDOUT | STDERR => len <= MAX_LINE,
        GAP => len == 16,
        SYNC => len == 0,
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown frame type {other}"),
            ));
        }
    };
    if !fits {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of type {} and {len} bytes", head[0]),
        ));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    let count = |at: usize| u64::from_be_bytes(payload[at..at + 8].try_into().expect("8 bytes"));
    Ok(Some(match head[0] {
        STDOUT => LogFrame::Line(Stream::Stdout, payload),
        STDERR => LogFrame::Line(Stream::Stderr, payload),
        GAP => LogFrame::Gap {
            stdout: count(0),
            stderr: count(8),
        },
        _ => LogFrame::Synced,
    }))
}

/// The wicket's notice to a clone just restored: `RESTORED <seed: 64 hex digits>[ <time:
/// nanoseconds since the Unix epoch>]\n`. The time where the restore left the guest's
/// clock at its snapshot's (aarch64: Firecracker advances kvmclock on x86_64 alone); on
/// x86_64 the notice goes without, as a guest predating the time reads it.
#[derive(Debug, PartialEq, Eq)]
pub struct Notice {
    pub seed: [u8; 32],
    pub time: Option<std::time::Duration>,
}

impl Notice {
    /// Its line, newline included.
    #[allow(dead_code)] // the wicket's half: each copy has both
    pub fn line(&self) -> String {
        let hex: String = self.seed.iter().map(|byte| format!("{byte:02x}")).collect();
        match self.time {
            Some(time) => format!("RESTORED {hex} {}\n", time.as_nanos()),
            None => format!("RESTORED {hex}\n"),
        }
    }

    /// The notice of a line (its newline trimmed); None for another line.
    #[allow(dead_code)] // the guest's half: each copy has both
    pub fn parse(line: &str) -> Option<Notice> {
        let rest = line.strip_prefix("RESTORED ")?;
        let (hex, time) = match rest.split_once(' ') {
            Some((hex, nanos)) => (hex, Some(nanos.parse::<u64>().ok()?)),
            None => (rest, None),
        };
        if hex.len() != 64 {
            return None;
        }
        let mut seed = [0u8; 32];
        for (i, byte) in seed.iter_mut().enumerate() {
            *byte = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
        }
        Some(Notice {
            seed,
            time: time.map(std::time::Duration::from_nanos),
        })
    }
}

/// Writes `payload` as one DATA frame, in one write: a vsock packet per write.
#[allow(dead_code)] // the wicket's (its template's priming): each copy has it
pub async fn write_data<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut frame = Vec::with_capacity(HEAD + payload.len());
    frame.push(DATA);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    writer.write_all(&frame).await
}

/// Writes the half-close: no DATA follows from this side.
pub async fn write_close<W: AsyncWrite + Unpin>(writer: &mut W) -> std::io::Result<()> {
    writer.write_all(&[CLOSE, 0, 0, 0, 0]).await
}

/// What [`Reader::read`] got.
#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// This many payload bytes went into the buffer.
    Data(usize),
    /// The peer half-closed.
    Close,
    /// The stream ended between frames, without a CLOSE: the peer failed or is gone.
    Eof,
}

/// Reads the frames of one direction, payload in pieces as large as the caller
/// takes; buffered: a read takes the heads and the payloads that arrived together.
pub struct Reader<R> {
    inner: BufReader<R>,
    /// Payload bytes of the current DATA frame not read yet.
    remaining: usize,
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub fn new(inner: R) -> Self {
        Reader {
            // a page: a small frame whole, a large one's payload read past it
            inner: BufReader::with_capacity(4096, inner),
            remaining: 0,
        }
    }

    /// Reads at most `buf.len()` (non-zero) payload bytes, or the next CLOSE / EOF.
    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<Read> {
        while self.remaining == 0 {
            if self.inner.fill_buf().await?.is_empty() {
                return Ok(Read::Eof);
            }
            let mut head = [0u8; HEAD];
            self.inner.read_exact(&mut head).await?;
            match head[0] {
                DATA => {
                    self.remaining =
                        u32::from_be_bytes(head[1..].try_into().expect("4 bytes")) as usize
                }
                CLOSE => return Ok(Read::Close),
                other => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown frame type {other}"),
                    ));
                }
            }
        }
        let wanted = buf.len().min(self.remaining);
        let n = self.inner.read(&mut buf[..wanted]).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        self.remaining -= n;
        Ok(Read::Data(n))
    }
}

/// Relays a plain byte stream (`plain`) and a framed one both ways until both
/// directions closed; an EOF without CLOSE on the framed side is an error, and so is
/// reading nothing from either side for `idle` (`TimedOut`).
pub async fn relay<P, F>(plain: P, framed: F, idle: Option<Duration>) -> std::io::Result<()>
where
    P: AsyncRead + AsyncWrite + Unpin,
    F: AsyncRead + AsyncWrite + Unpin,
{
    let (mut plain_reader, mut plain_writer) = tokio::io::split(plain);
    let (framed_reader, mut framed_writer) = tokio::io::split(framed);
    let mut framed_reader = Reader::new(framed_reader);
    // when a byte was read last, either way: milliseconds since the start
    let start = tokio::time::Instant::now();
    let last = AtomicU64::new(0);
    let read = || last.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
    let inbound = async {
        let mut buffer = vec![0u8; MAX_PAYLOAD];
        loop {
            let frame = framed_reader.read(&mut buffer).await?;
            read();
            match frame {
                Read::Data(n) => plain_writer.write_all(&buffer[..n]).await?,
                Read::Close => return plain_writer.shutdown().await,
                Read::Eof => return Err(std::io::Error::other("peer gone without CLOSE")),
            }
        }
    };
    let outbound = async {
        // the payload read behind room for its head: a frame is one write
        let mut frame = vec![0u8; HEAD + MAX_PAYLOAD];
        loop {
            let n = plain_reader.read(&mut frame[HEAD..]).await?;
            read();
            if n == 0 {
                return write_close(&mut framed_writer).await;
            }
            frame[0] = DATA;
            frame[1..HEAD].copy_from_slice(&(n as u32).to_be_bytes());
            framed_writer.write_all(&frame[..HEAD + n]).await?;
        }
    };
    let idled = async {
        let Some(idle) = idle else {
            return std::future::pending().await;
        };
        loop {
            let due = start + Duration::from_millis(last.load(Ordering::Relaxed)) + idle;
            if tokio::time::Instant::now() >= due {
                return std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("no byte relayed for {}ms (idle-ms)", idle.as_millis()),
                );
            }
            tokio::time::sleep_until(due).await;
        }
    };
    tokio::select! {
        relayed = async { tokio::try_join!(inbound, outbound) } => relayed.map(|_| ()),
        e = idled => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn half_closes_travel_both_ways_and_the_other_direction_goes_on() {
        // client <-> plain | relay | framed <-> peer speaking the framing
        let (client, plain) = tokio::io::duplex(64);
        let (framed, peer) = tokio::io::duplex(64);
        let relay = tokio::spawn(relay(plain, framed, None));
        let (mut client_reader, mut client_writer) = tokio::io::split(client);
        let (peer_reader, mut peer_writer) = tokio::io::split(peer);
        let mut peer_reader = Reader::new(peer_reader);

        client_writer.write_all(b"request").await.unwrap();
        client_writer.shutdown().await.unwrap();
        let mut got = Vec::new();
        let mut buffer = [0u8; 3];
        loop {
            match peer_reader.read(&mut buffer).await.unwrap() {
                Read::Data(n) => got.extend_from_slice(&buffer[..n]),
                Read::Close => break,
                Read::Eof => panic!("EOF before CLOSE"),
            }
        }
        assert_eq!(got, b"request");

        // half-closed toward the peer, the answer still comes back, then its CLOSE
        write_data(&mut peer_writer, b"answer").await.unwrap();
        write_close(&mut peer_writer).await.unwrap();
        let mut answer = Vec::new();
        client_reader.read_to_end(&mut answer).await.unwrap();
        assert_eq!(answer, b"answer");
        relay.await.unwrap().unwrap();
    }

    #[test]
    fn a_restore_notice_carries_its_seed_and_the_time_where_given() {
        let seed: [u8; 32] = std::array::from_fn(|i| i as u8 * 7);
        for time in [
            None,
            Some(std::time::Duration::new(1_793_000_000, 123_456_789)),
        ] {
            let notice = Notice { seed, time };
            assert_eq!(Notice::parse(notice.line().trim_end()), Some(notice));
        }
        assert_eq!(
            Notice::parse(&format!("RESTORED {} soon", "00".repeat(32))),
            None
        );
        assert_eq!(Notice::parse("RESTORED 00"), None);
        assert_eq!(Notice::parse("HELLO"), None);
    }

    /// A relay fails on a framed stream ending without its CLOSE, and on reading
    /// nothing from either side for its idle bound, counted from the last byte read.
    #[tokio::test(start_paused = true)]
    async fn a_stream_ending_without_close_or_idling_fails_the_relay() {
        let (_client, plain) = tokio::io::duplex(64);
        let (framed, peer) = tokio::io::duplex(64);
        let relayed = tokio::spawn(relay(plain, framed, None));
        drop(peer);
        assert!(relayed.await.unwrap().is_err());

        let (mut client, plain) = tokio::io::duplex(64);
        let (framed, _peer) = tokio::io::duplex(64);
        let started = tokio::time::Instant::now();
        let relayed = tokio::spawn(relay(plain, framed, Some(Duration::from_secs(10))));
        tokio::time::sleep(Duration::from_secs(6)).await;
        client.write_all(b"x").await.unwrap();
        let failed = relayed.await.unwrap().unwrap_err();
        assert_eq!(failed.kind(), std::io::ErrorKind::TimedOut, "{failed}");
        let idled = started.elapsed();
        assert!(
            idled >= Duration::from_secs(16) && idled < Duration::from_secs(17),
            "{idled:?}"
        );
    }
}
