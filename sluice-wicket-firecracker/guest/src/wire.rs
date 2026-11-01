//! The framing on every vsock stream between the wicket and the guest's `/init`.
//!
//! Firecracker's vsock device does not carry a half-close: the host side's EOF reaches
//! the guest as a full shutdown (its unsent answer is lost), and a guest's
//! `shutdown(SHUT_WR)` never reaches the host. A tunnel connection's CLOSE therefore
//! travels in band: `[type: u8][len: u32 BE][payload]`, `DATA` or `CLOSE` (no payload).
//! A stream ending without a `CLOSE` from its side failed.
//!
//! A copy of the wicket's `src/wire.rs` (with its tests): keep them alike.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const DATA: u8 = 0;
const CLOSE: u8 = 1;

/// Writes `payload` as one DATA frame.
pub async fn write_data<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut head = [DATA, 0, 0, 0, 0];
    head[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    writer.write_all(&head).await?;
    writer.write_all(payload).await
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

/// Reads the frames of one direction, payload in pieces as large as the caller takes.
pub struct Reader<R> {
    inner: R,
    /// Payload bytes of the current DATA frame not read yet.
    remaining: usize,
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub fn new(inner: R) -> Self {
        Reader {
            inner,
            remaining: 0,
        }
    }

    /// Reads at most `buf.len()` (non-zero) payload bytes, or the next CLOSE / EOF.
    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<Read> {
        while self.remaining == 0 {
            let mut head = [0u8; 5];
            match self.inner.read(&mut head[..1]).await? {
                0 => return Ok(Read::Eof),
                _ => self.inner.read_exact(&mut head[1..]).await?,
            };
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
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            match framed_reader.read(&mut buffer).await? {
                Read::Data(n) => plain_writer.write_all(&buffer[..n]).await?,
                Read::Close => return plain_writer.shutdown().await,
                Read::Eof => return Err(std::io::Error::other("peer gone without CLOSE")),
            }
        }
    };
    let outbound = async {
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            let n = plain_reader.read(&mut buffer).await?;
            if n == 0 {
                return write_close(&mut framed_writer).await;
            }
            write_data(&mut framed_writer, &buffer[..n]).await?;
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
