//! What `--listen` reads of a connection to route it, as the server's data plane
//! does: an http/1.1 request head, or the h2c preface and the frames up to the
//! first header block (`hpack`), within [`MAX_HEAD`] bytes. What was read is
//! relayed on as read.

use sluice_wicket_tunnel::listener::Replay;
use tokio::io::AsyncRead;

use crate::hpack;

/// The most read before a connection's head ends (the server's bound).
pub const MAX_HEAD: usize = 64 * 1024;

/// The h2 connection preface (RFC 9113 3.4).
pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The most header fields of an http/1.1 head.
pub const MAX_FIELDS: usize = 256;

const HEADERS: u8 = 0x1;
const CONTINUATION: u8 = 0x9;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY: u8 = 0x20;

/// A connection's head: the host its first request names (empty for none).
#[derive(Debug, PartialEq, Eq)]
pub enum Head {
    Http1 {
        host: String,
    },
    /// h2c, its first request on `stream`.
    H2 {
        stream: u32,
        host: String,
    },
}

impl Head {
    pub fn host(&self) -> &str {
        match self {
            Head::Http1 { host } | Head::H2 { host, .. } => host,
        }
    }
}

/// Reads `io` until its head ended; an error for a stream that is not HTTP,
/// ended before, or whose head runs past [`MAX_HEAD`].
pub async fn read<S: AsyncRead + Unpin>(io: &mut Replay<S>) -> Result<Head, String> {
    loop {
        if let Some(head) = parse(io.read_so_far())? {
            return Ok(head);
        }
        let room = MAX_HEAD - io.read_so_far().len();
        if room == 0 {
            return Err(format!("no request head within {MAX_HEAD} bytes"));
        }
        match io.fill(room.min(16 * 1024)).await {
            Ok(0) => return Err("closed before its request head".into()),
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// The head of what was read; None while it may still end.
pub fn parse(read: &[u8]) -> Result<Option<Head>, String> {
    if let Some(frames) = read.strip_prefix(PREFACE) {
        return h2(frames);
    }
    if PREFACE.starts_with(read) {
        return Ok(None);
    }
    let mut fields = [httparse::EMPTY_HEADER; MAX_FIELDS];
    let mut request = httparse::Request::new(&mut fields);
    match request.parse(read) {
        Ok(httparse::Status::Partial) => Ok(None),
        Ok(httparse::Status::Complete(_)) => {
            let mut hosts = request
                .headers
                .iter()
                .filter(|field| field.name.eq_ignore_ascii_case("host"));
            let host = match (hosts.next(), hosts.next()) {
                (None, _) => String::new(),
                (Some(host), None) => String::from_utf8(host.value.to_vec())
                    .map_err(|_| "a host not UTF-8".to_string())?,
                (Some(_), Some(_)) => return Err("more than one Host".into()),
            };
            Ok(Some(Head::Http1 { host }))
        }
        Err(e) => Err(format!("not HTTP: {e}")),
    }
}

/// The frames after the preface up to the end of the first header block (its
/// HEADERS frame and CONTINUATIONs); the frames before it, SETTINGS and the
/// like, are relayed alike.
fn h2(mut frames: &[u8]) -> Result<Option<Head>, String> {
    let mut block = Vec::new();
    let mut stream = None;
    loop {
        let Some(header) = frames.get(..9) else {
            return Ok(None);
        };
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let (kind, flags) = (header[3], header[4]);
        let id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        let Some(payload) = frames.get(9..9 + len) else {
            return Ok(None);
        };
        frames = &frames[9 + len..];
        match (stream, kind) {
            (None, HEADERS) => {
                if id == 0 {
                    return Err("HEADERS on stream 0".into());
                }
                block.extend_from_slice(fragment(payload, flags)?);
                stream = Some(id);
            }
            (None, CONTINUATION) => return Err("a CONTINUATION without HEADERS".into()),
            (None, _) => continue,
            (Some(first), CONTINUATION) if id == first => block.extend_from_slice(payload),
            (Some(_), _) => return Err("a header block not continued".into()),
        }
        if flags & END_HEADERS != 0 {
            let host = hpack::host(&block)?.unwrap_or_default();
            let stream = stream.expect("a HEADERS frame read");
            return Ok(Some(Head::H2 { stream, host }));
        }
    }
}

/// A HEADERS frame's block fragment: its padding and priority left out.
fn fragment(payload: &[u8], flags: u8) -> Result<&[u8], String> {
    let malformed = || "a malformed HEADERS frame".to_string();
    let mut fragment = payload;
    if flags & PADDED != 0 {
        let (&pad, rest) = fragment.split_first().ok_or_else(malformed)?;
        fragment = rest
            .get(..rest.len().checked_sub(pad as usize).ok_or_else(malformed)?)
            .ok_or_else(malformed)?;
    }
    if flags & PRIORITY != 0 {
        fragment = fragment.get(5..).ok_or_else(malformed)?;
    }
    Ok(fragment)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let len = (payload.len() as u32).to_be_bytes();
        [&len[1..], &[kind, flags], &stream.to_be_bytes(), payload].concat()
    }

    /// The host of an http/1.1 head and of an h2c connection's first header
    /// block (padded, with a priority, continued), read to their ends; what is
    /// no head.
    #[test]
    fn a_head_names_its_host() {
        let settings = frame(0x4, 0, 0, &[0, 3, 0, 0, 0, 100]);
        // :method GET, :authority a.local (literal, indexed name)
        let block = [&[0x82, 0x41, 0x07][..], b"a.local"].concat();
        // the HEADERS frame's padding and priority around the block's start
        let padded = [&[2u8][..], &[0; 5], &block[..2], &[0, 0]].concat();
        let continued = [
            PREFACE,
            &settings,
            &frame(HEADERS, PADDED | PRIORITY, 3, &padded),
            &frame(CONTINUATION, END_HEADERS, 3, &block[2..]),
        ]
        .concat();
        let http1 = |host: &str| Head::Http1 { host: host.into() };
        type Case<'a> = (&'a [u8], Result<Option<Head>, &'a str>);
        let cases: [Case; 9] = [
            (
                b"GET / HTTP/1.1\r\nHost: A.local:8080\r\n\r\n",
                Ok(Some(http1("A.local:8080"))),
            ),
            (b"GET / HTTP/1.0\r\n\r\n", Ok(Some(http1("")))),
            (b"GET / HTTP/1.1\r\nhost: a.local\r\n", Ok(None)),
            (b"PRI * HTTP/2.0\r\n", Ok(None)),
            (
                &continued,
                Ok(Some(Head::H2 {
                    stream: 3,
                    host: "a.local".into(),
                })),
            ),
            (&continued[..continued.len() - 1], Ok(None)),
            (b"\x16\x03\x01\x02\x00", Err("not HTTP: invalid token")),
            (
                b"GET / HTTP/1.1\r\nhost: a\r\nhost: b\r\n\r\n",
                Err("more than one Host"),
            ),
            (
                &[
                    PREFACE,
                    &frame(HEADERS, 0, 1, &block),
                    &frame(0x0, 0, 1, b"x"),
                ]
                .concat()[..],
                Err("a header block not continued"),
            ),
        ];
        for (read, expected) in cases {
            assert_eq!(
                parse(read),
                expected.map_err(String::from),
                "{}",
                String::from_utf8_lossy(read)
            );
        }
    }

    /// A head past the bound, or a stream ended before its head, is none.
    #[tokio::test]
    async fn a_head_ends_within_the_bound() {
        let long = format!("GET / HTTP/1.1\r\nx: {}\r\n\r\n", "y".repeat(MAX_HEAD));
        for (sent, expected) in [
            (long.as_bytes(), "no request head within 65536 bytes"),
            (&b"GET / HTTP/1.1\r\n"[..], "closed before its request head"),
        ] {
            let (mut client, server) = tokio::io::duplex(MAX_HEAD * 2);
            tokio::io::AsyncWriteExt::write_all(&mut client, sent)
                .await
                .unwrap();
            drop(client);
            let read = read(&mut Replay::new(server)).await;
            assert_eq!(read, Err(expected.to_string()));
        }
    }
}
