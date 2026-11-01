//! The framing on every vsock stream between the wicket and the guest's `/init`.
//!
//! Firecracker's vsock device does not carry a half-close: the host side's EOF reaches
//! the guest as a full shutdown (its unsent answer is lost), and a guest's
//! `shutdown(SHUT_WR)` never reaches the host. A tunnel connection's CLOSE therefore
//! travels in band: `[type: u8][len: u32 BE][payload]`, `DATA` or `CLOSE` (no payload).
//! A stream ending without a `CLOSE` from its side failed.
//!
//! The guest has a copy of this module (`guest/src/wire.rs`): keep them alike.

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const DATA: u8 = 0;
const CLOSE: u8 = 1;

/// A frame's head: its type and its payload's length.
const HEAD: usize = 5;

/// The most payload [`relay`] puts in a frame.
const MAX_PAYLOAD: usize = 16 * 1024;

/// The vsock port of the guest's control stream, which carries the wicket's
/// notice to a VM just restored from a snapshot (a clone): `RESTORED <seed: 64 hex
/// digits>\n`, answered `OK\n` once the guest put the seed into its kernel's random
/// pool and reseeded it, and ran the image's restore hook. A clone's memory is its
/// snapshot's, random state included: the kernel reseeds on the VM generation
/// change (VMGenID) a moment after the restore, a process's own generator never.
pub const CONTROL_PORT: u32 = 1024;

/// The restore notice carrying `seed`.
#[allow(dead_code)] // the wicket's half: each copy has both
pub fn restored(seed: &[u8; 32]) -> String {
    let hex: String = seed.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("RESTORED {hex}\n")
}

/// The seed of a restore notice's line (its newline trimmed); None for another line.
#[allow(dead_code)] // the guest's half: each copy has both
pub fn seed(line: &str) -> Option<[u8; 32]> {
    let hex = line.strip_prefix("RESTORED ")?;
    if hex.len() != 64 {
        return None;
    }
    let mut seed = [0u8; 32];
    for (i, byte) in seed.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(seed)
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
/// directions closed; an EOF without CLOSE on the framed side is an error.
pub async fn relay<P, F>(plain: P, framed: F) -> std::io::Result<()>
where
    P: AsyncRead + AsyncWrite + Unpin,
    F: AsyncRead + AsyncWrite + Unpin,
{
    let (mut plain_reader, mut plain_writer) = tokio::io::split(plain);
    let (framed_reader, mut framed_writer) = tokio::io::split(framed);
    let mut framed_reader = Reader::new(framed_reader);
    let inbound = async {
        let mut buffer = vec![0u8; MAX_PAYLOAD];
        loop {
            match framed_reader.read(&mut buffer).await? {
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
            if n == 0 {
                return write_close(&mut framed_writer).await;
            }
            frame[0] = DATA;
            frame[1..HEAD].copy_from_slice(&(n as u32).to_be_bytes());
            framed_writer.write_all(&frame[..HEAD + n]).await?;
        }
    };
    tokio::try_join!(inbound, outbound).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn half_closes_travel_both_ways_and_the_other_direction_goes_on() {
        // client <-> plain | relay | framed <-> peer speaking the framing
        let (client, plain) = tokio::io::duplex(64);
        let (framed, peer) = tokio::io::duplex(64);
        let relay = tokio::spawn(relay(plain, framed));
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

    #[tokio::test]
    async fn a_stream_ending_without_close_fails_the_relay() {
        let (_client, plain) = tokio::io::duplex(64);
        let (framed, peer) = tokio::io::duplex(64);
        let relay = tokio::spawn(relay(plain, framed));
        drop(peer);
        assert!(relay.await.unwrap().is_err());
    }
}
