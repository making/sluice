//! Whether a connection relayed on `--listen` has a request in progress, or waits on its client
//! alone ([`Standing`]): read from the bytes going by, which are relayed as they are. A request is
//! in progress from the end of its head until its answer ended and its body too, an h2 connection's
//! while a stream is open.
//!
//! What cannot be told is verbatim for good, its progress never its client's to lose: another
//! protocol (a guest speaking first, what parses as no HTTP), an upgraded connection or a CONNECT's,
//! an answer running until its close, a message framed ambiguously.

use std::collections::{HashMap, VecDeque};

use crate::head::{MAX_FIELDS, MAX_HEAD, PREFACE};

/// The most http/1.1 requests pipelined and not answered yet read, or h2 streams open.
const MAX_IN_PROGRESS: usize = 4096;

/// How much of a head is parsed however it arrives, to tell no HTTP from a head going on. Past it
/// only a line's end makes it worth parsing again: a head dripped costs no more than it is long.
const SNIFFED: usize = 64;

const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const RST_STREAM: u8 = 0x3;
const PUSH_PROMISE: u8 = 0x5;
const GOAWAY: u8 = 0x7;
const END_STREAM: u8 = 0x1;

/// What a connection's exchanges wait on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    /// Its client alone: no request in progress, a head on its way not being one yet.
    Quiet,
    /// A request's body its client did not finish, with no request sent whole to wait for.
    Sending,
    /// A request sent whole, or what cannot be told.
    Busy,
}

/// A connection's exchanges so far, fed what it relayed.
pub struct Exchanges(Wire);

enum Wire {
    /// What was read while it may be the h2 preface.
    Unknown(Vec<u8>),
    Http1(Http1),
    H2(H2),
    Verbatim,
}

impl Default for Exchanges {
    fn default() -> Exchanges {
        Exchanges(Wire::Unknown(Vec::new()))
    }
}

impl Exchanges {
    /// What the client sent.
    pub fn client_sent(&mut self, bytes: &[u8]) {
        let read = match &mut self.0 {
            Wire::Unknown(read) => {
                read.extend_from_slice(bytes);
                if read.len() < PREFACE.len() && PREFACE.starts_with(read) {
                    return;
                }
                if let Some(frames) = read.strip_prefix(PREFACE) {
                    let frames = frames.to_vec();
                    self.0 = Wire::H2(H2::default());
                    return self.client_sent(&frames);
                }
                let read = std::mem::take(read);
                self.0 = Wire::Http1(Http1::default());
                return self.client_sent(&read);
            }
            Wire::Http1(http1) => http1.client_sent(bytes),
            Wire::H2(h2) => h2.client_sent(bytes),
            Wire::Verbatim => Ok(()),
        };
        if read.is_err() {
            self.0 = Wire::Verbatim;
        }
    }

    /// What the guest sent, as far as the client's stream took it.
    pub fn guest_sent(&mut self, bytes: &[u8]) {
        let read = match &mut self.0 {
            Wire::Unknown(_) => Err(Unreadable),
            Wire::Http1(http1) => http1.guest_sent(bytes),
            Wire::H2(h2) => h2.guest_sent(bytes),
            Wire::Verbatim => Ok(()),
        };
        if read.is_err() {
            self.0 = Wire::Verbatim;
        }
    }

    pub fn standing(&self) -> Standing {
        match &self.0 {
            Wire::Unknown(_) => Standing::Quiet,
            Wire::Http1(http1) => match (&http1.request, http1.owed.len()) {
                (Part::Head(_), 0) => Standing::Quiet,
                // the one owed, if any, is the one sending: its answer begun or not
                (Part::Body(_), 0 | 1) => Standing::Sending,
                _ => Standing::Busy,
            },
            Wire::H2(h2) => match (h2.streams.len(), h2.sent_whole) {
                (0, _) => Standing::Quiet,
                (_, 0) => Standing::Sending,
                _ => Standing::Busy,
            },
            Wire::Verbatim => Standing::Busy,
        }
    }

    /// The GOAWAY telling an h2 client its connection ends, where one can be written: its server
    /// preface sent and between two frames.
    pub fn goaway(&self) -> Option<Vec<u8>> {
        let Wire::H2(h2) = &self.0 else {
            return None;
        };
        if !h2.guest.spoke || !h2.guest.between_frames() {
            return None;
        }
        let payload = [h2.last_stream.to_be_bytes(), 0u32.to_be_bytes()].concat();
        let head = [0, 0, payload.len() as u8, GOAWAY, 0, 0, 0, 0, 0];
        Some([&head[..], &payload].concat())
    }
}

/// Bytes these exchanges cannot follow: the connection is verbatim from there on.
struct Unreadable;

/// One direction of an http/1.1 connection.
enum Part {
    /// The head so far.
    Head(Vec<u8>),
    Body(Body),
}

impl Default for Part {
    fn default() -> Part {
        Part::Head(Vec::new())
    }
}

/// What a request's answer will be read by.
#[derive(Clone, Copy)]
struct Owed {
    head: bool,
    connect: bool,
}

#[derive(Default)]
struct Http1 {
    request: Part,
    response: Part,
    /// The requests read whose answer did not end, the first's in progress.
    owed: VecDeque<Owed>,
}

impl Http1 {
    fn client_sent(&mut self, mut bytes: &[u8]) -> Result<(), Unreadable> {
        while !bytes.is_empty() {
            match &mut self.request {
                Part::Body(body) => {
                    let Some(taken) = body.take(bytes)? else {
                        return Ok(());
                    };
                    bytes = &bytes[taken..];
                    self.request = Part::default();
                }
                Part::Head(head) => {
                    let Some((taken, (owed, body))) = read_head(head, bytes, parse_request)? else {
                        return Ok(());
                    };
                    bytes = &bytes[taken..];
                    head.clear();
                    if self.owed.len() == MAX_IN_PROGRESS {
                        return Err(Unreadable);
                    }
                    self.owed.push_back(owed);
                    match body {
                        None | Some(Body::Length(0)) => {}
                        Some(body) => self.request = Part::Body(body),
                    }
                }
            }
        }
        Ok(())
    }

    fn guest_sent(&mut self, mut bytes: &[u8]) -> Result<(), Unreadable> {
        while !bytes.is_empty() {
            match &mut self.response {
                Part::Body(body) => {
                    let Some(taken) = body.take(bytes)? else {
                        return Ok(());
                    };
                    bytes = &bytes[taken..];
                    self.response = Part::default();
                    self.owed.pop_front();
                }
                Part::Head(head) => {
                    // nothing asked for it: not an answer
                    let owed = *self.owed.front().ok_or(Unreadable)?;
                    let Some((taken, (status, body))) = read_head(head, bytes, parse_response)?
                    else {
                        return Ok(());
                    };
                    bytes = &bytes[taken..];
                    head.clear();
                    match (status, body) {
                        // upgraded
                        (101, _) => return Err(Unreadable),
                        (100..=199, _) => {}
                        (200..=299, _) if owed.connect => return Err(Unreadable),
                        (204 | 304, _) | (_, Some(Body::Length(0))) => {
                            self.owed.pop_front();
                        }
                        _ if owed.head => {
                            self.owed.pop_front();
                        }
                        (_, Some(body)) => self.response = Part::Body(body),
                        // running until its close
                        (_, None) => return Err(Unreadable),
                    }
                }
            }
        }
        Ok(())
    }
}

/// A head's end and what it said, once whole.
type Parsed<T> = Result<Option<(usize, T)>, Unreadable>;

/// Parses `head` with `bytes` behind it: how many of those ended it, and what it said.
fn read_head<T>(head: &mut Vec<u8>, bytes: &[u8], parse: impl Fn(&[u8]) -> Parsed<T>) -> Parsed<T> {
    let before = head.len();
    head.extend_from_slice(bytes);
    if (before < SNIFFED || bytes.contains(&b'\n'))
        && let Some((end, what)) = parse(head)?
    {
        return Ok(Some((end - before, what)));
    }
    if head.len() >= MAX_HEAD {
        return Err(Unreadable);
    }
    Ok(None)
}

/// How a request's answer reads and the body it announces, when `head` is a whole one.
fn parse_request(head: &[u8]) -> Parsed<(Owed, Option<Body>)> {
    let mut fields = [httparse::EMPTY_HEADER; MAX_FIELDS];
    let mut request = httparse::Request::new(&mut fields);
    match request.parse(head).map_err(|_| Unreadable)? {
        httparse::Status::Partial => Ok(None),
        httparse::Status::Complete(end) => {
            let owed = Owed {
                head: request.method == Some("HEAD"),
                connect: request.method == Some("CONNECT"),
            };
            Ok(Some((end, (owed, framing(request.headers)?))))
        }
    }
}

/// A response's status and the body it announces, when `head` is a whole one.
fn parse_response(head: &[u8]) -> Parsed<(u16, Option<Body>)> {
    let mut fields = [httparse::EMPTY_HEADER; MAX_FIELDS];
    let mut response = httparse::Response::new(&mut fields);
    match response.parse(head).map_err(|_| Unreadable)? {
        httparse::Status::Partial => Ok(None),
        httparse::Status::Complete(end) => {
            let status = response.code.ok_or(Unreadable)?;
            Ok(Some((end, (status, framing(response.headers)?))))
        }
    }
}

/// The body a message's fields announce: None for none, which a response then runs until its close.
fn framing(fields: &[httparse::Header<'_>]) -> Result<Option<Body>, Unreadable> {
    let value = |field: &httparse::Header<'_>| {
        std::str::from_utf8(field.value)
            .map(str::trim)
            .map_err(|_| Unreadable)
            .map(str::to_ascii_lowercase)
    };
    let mut length = None;
    let mut chunked = false;
    for field in fields {
        if field.name.eq_ignore_ascii_case("content-length") {
            let announced: u64 = value(field)?.parse().map_err(|_| Unreadable)?;
            if length.is_some_and(|length| length != announced) {
                return Err(Unreadable);
            }
            length = Some(announced);
        } else if field.name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !value(field)?.ends_with("chunked") {
                return Err(Unreadable);
            }
            chunked = true;
        }
    }
    match (length, chunked) {
        (Some(_), true) => Err(Unreadable),
        (None, true) => Ok(Some(Body::Chunked(Chunks::Size(0, false)))),
        (Some(length), false) => Ok(Some(Body::Length(length))),
        (None, false) => Ok(None),
    }
}

/// An http/1.1 body going by.
enum Body {
    /// The bytes left.
    Length(u64),
    Chunked(Chunks),
}

impl Body {
    /// How many of `bytes` ended it; None with all of them taken and more to come.
    fn take(&mut self, bytes: &[u8]) -> Result<Option<usize>, Unreadable> {
        match self {
            Body::Length(left) => {
                let taken = (*left).min(bytes.len() as u64);
                *left -= taken;
                Ok((*left == 0).then_some(taken as usize))
            }
            Body::Chunked(chunks) => chunks.take(bytes),
        }
    }
}

/// Where a chunked body is.
#[derive(Clone, Copy)]
enum Chunks {
    /// A chunk's size so far, and whether it has a digit.
    Size(u64, bool),
    /// The line of a chunk of this size, past the digits.
    Extension(u64),
    /// The chunk's bytes left.
    Data(u64),
    /// The line's end behind a chunk.
    DataEnd,
    /// The trailers; true at the start of a line.
    Trailer(bool),
}

impl Chunks {
    fn take(&mut self, bytes: &[u8]) -> Result<Option<usize>, Unreadable> {
        let mut at = 0;
        while at < bytes.len() {
            if let Chunks::Data(left) = self {
                let taken = (*left).min((bytes.len() - at) as u64);
                *left -= taken;
                at += taken as usize;
                if *left == 0 {
                    *self = Chunks::DataEnd;
                }
                continue;
            }
            let byte = bytes[at];
            at += 1;
            *self = match (*self, byte) {
                (Chunks::Size(size, _), digit) if digit.is_ascii_hexdigit() => {
                    let digit = (digit as char).to_digit(16).expect("a hex digit") as u64;
                    let size = size.checked_mul(16).ok_or(Unreadable)?;
                    Chunks::Size(size.checked_add(digit).ok_or(Unreadable)?, true)
                }
                (Chunks::Size(size, true), b'\r' | b';' | b' ' | b'\t') => Chunks::Extension(size),
                (Chunks::Size(0, true) | Chunks::Extension(0), b'\n') => Chunks::Trailer(true),
                (Chunks::Size(size, true) | Chunks::Extension(size), b'\n') => Chunks::Data(size),
                (Chunks::Size(..), _) => return Err(Unreadable),
                (Chunks::Extension(size), _) => Chunks::Extension(size),
                (Chunks::DataEnd, b'\r') => Chunks::DataEnd,
                (Chunks::DataEnd, b'\n') => Chunks::Size(0, false),
                (Chunks::DataEnd, _) => return Err(Unreadable),
                (Chunks::Trailer(true), b'\n') => return Ok(Some(at)),
                (Chunks::Trailer(start), b'\r') => Chunks::Trailer(start),
                (Chunks::Trailer(_), byte) => Chunks::Trailer(byte == b'\n'),
                (Chunks::Data(_), _) => unreachable!("taken above"),
            };
        }
        Ok(None)
    }
}

/// One direction's frames going by: their heads read, their payloads skipped.
#[derive(Default)]
struct Frames {
    head: [u8; 9],
    filled: usize,
    skipping: usize,
    spoke: bool,
}

impl Frames {
    /// The next frame's type, flags and stream ending its head in `bytes`, and the bytes left.
    fn next<'a>(&mut self, mut bytes: &'a [u8]) -> (Option<(u8, u8, u32)>, &'a [u8]) {
        let skipped = self.skipping.min(bytes.len());
        self.skipping -= skipped;
        bytes = &bytes[skipped..];
        let taken = (self.head.len() - self.filled).min(bytes.len());
        self.head[self.filled..self.filled + taken].copy_from_slice(&bytes[..taken]);
        self.filled += taken;
        bytes = &bytes[taken..];
        if self.filled < self.head.len() {
            return (None, bytes);
        }
        let head = self.head;
        self.filled = 0;
        self.skipping = u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize;
        self.spoke = true;
        let stream = u32::from_be_bytes([head[5], head[6], head[7], head[8]]) & 0x7fff_ffff;
        (Some((head[3], head[4], stream)), bytes)
    }

    fn between_frames(&self) -> bool {
        self.filled == 0 && self.skipping == 0
    }
}

/// Which sides of an h2 stream ended.
#[derive(Default)]
struct Stream {
    client: bool,
    guest: bool,
}

#[derive(Default)]
struct H2 {
    client: Frames,
    guest: Frames,
    streams: HashMap<u32, Stream>,
    /// How many of the streams their client ended.
    sent_whole: usize,
    last_stream: u32,
}

impl H2 {
    fn client_sent(&mut self, mut bytes: &[u8]) -> Result<(), Unreadable> {
        while !bytes.is_empty() {
            let (frame, rest) = self.client.next(bytes);
            bytes = rest;
            let Some((kind, flags, id)) = frame else {
                continue;
            };
            let ended = flags & END_STREAM != 0;
            match kind {
                HEADERS => {
                    if self.streams.len() == MAX_IN_PROGRESS {
                        return Err(Unreadable);
                    }
                    self.last_stream = self.last_stream.max(id);
                    self.streams.entry(id).or_default();
                    self.ended(id, ended, false);
                }
                DATA => self.ended(id, ended, false),
                RST_STREAM => self.remove(id),
                _ => {}
            }
        }
        Ok(())
    }

    fn guest_sent(&mut self, mut bytes: &[u8]) -> Result<(), Unreadable> {
        while !bytes.is_empty() {
            let (frame, rest) = self.guest.next(bytes);
            bytes = rest;
            let Some((kind, flags, id)) = frame else {
                continue;
            };
            let ended = flags & END_STREAM != 0;
            match kind {
                HEADERS | DATA => self.ended(id, false, ended),
                RST_STREAM => self.remove(id),
                // an answer no client stream stands for
                PUSH_PROMISE => return Err(Unreadable),
                _ => {}
            }
        }
        Ok(())
    }

    fn ended(&mut self, id: u32, client: bool, guest: bool) {
        if let Some(stream) = self.streams.get_mut(&id) {
            self.sent_whole += usize::from(client && !stream.client);
            stream.client |= client;
            stream.guest |= guest;
            if stream.client && stream.guest {
                self.remove(id);
            }
        }
    }

    fn remove(&mut self, id: u32) {
        if let Some(stream) = self.streams.remove(&id) {
            self.sent_whole -= usize::from(stream.client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let len = (payload.len() as u32).to_be_bytes();
        [&len[1..], &[kind, flags], &stream.to_be_bytes(), payload].concat()
    }

    /// Bytes of one side, what the connection then stands at.
    enum Step {
        Client(&'static [u8], Standing),
        Guest(&'static [u8], Standing),
    }
    use Standing::{Busy, Quiet, Sending};
    use Step::{Client, Guest};

    /// An exchange ends where its message framing says, each of its bytes arriving alone or all
    /// together; what cannot be followed never waits on its client again. A body its client did
    /// not finish leaves a request not sent whole, whatever came of its answer.
    #[test]
    fn a_request_is_in_progress_until_its_answer_ended() {
        const GET: &[u8] = b"GET / HTTP/1.1\r\nhost: a\r\n\r\n";
        const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi";
        let cases: &[(&str, &[Step])] = &[
            ("nothing yet", &[]),
            (
                "keep-alive",
                &[
                    Client(GET, Busy),
                    Guest(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nh", Busy),
                    Guest(b"i", Quiet),
                    Client(b"GET / HT", Quiet),
                    Client(b"TP/1.1\r\n\r\n", Busy),
                    Guest(OK, Quiet),
                ],
            ),
            (
                "an answer without a body",
                &[
                    Client(b"HEAD / HTTP/1.1\r\n\r\n", Busy),
                    Guest(b"HTTP/1.1 200 OK\r\ncontent-length: 9\r\n\r\n", Quiet),
                    Client(GET, Busy),
                    Guest(b"HTTP/1.1 204 No Content\r\n\r\n", Quiet),
                    Client(GET, Busy),
                    Guest(b"HTTP/1.1 304 Not Modified\r\n\r\n", Quiet),
                    Client(GET, Busy),
                    Guest(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n", Quiet),
                ],
            ),
            (
                "a request's body and a chunked answer, an interim one first",
                &[
                    Client(b"POST / HTTP/1.1\r\ncontent-length: 5\r\n\r\nab", Sending),
                    Guest(b"HTTP/1.1 100 Continue\r\n\r\n", Sending),
                    Guest(
                        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, Chunked\r\n\r\n",
                        Sending,
                    ),
                    Guest(b"2;x=y\r\nhi\r\nA\r\n0123456789\r\n", Sending),
                    Guest(b"0\r\nx: y\r\n\r\n", Sending),
                    // the answer ended, the body has not
                    Client(b"cde", Quiet),
                ],
            ),
            (
                "a body whose answer is owed once it ended, behind a request sent whole",
                &[
                    Client(GET, Busy),
                    Client(b"POST / HTTP/1.1\r\ncontent-length: 1\r\n\r\n", Busy),
                    Guest(OK, Sending),
                    Client(b"a", Busy),
                    Guest(OK, Quiet),
                ],
            ),
            (
                "a chunked request's body, pipelined requests",
                &[
                    Client(
                        b"POST / HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n\
                          GET / HTTP/1.1\r\n\r\n",
                        Busy,
                    ),
                    Guest(OK, Busy),
                    Guest(OK, Quiet),
                ],
            ),
            (
                "a long poll's head with its body to come",
                &[
                    Client(GET, Busy),
                    Guest(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n", Busy),
                ],
            ),
            ("an upgrade", &[Client(GET, Busy), Guest(b"HTTP/1.1 101 Switching\r\n\r\n", Busy), Guest(OK, Busy)]),
            (
                "a CONNECT",
                &[
                    Client(b"CONNECT a:1 HTTP/1.1\r\n\r\n", Busy),
                    Guest(b"HTTP/1.1 200 OK\r\n\r\n", Busy),
                    Guest(OK, Busy),
                ],
            ),
            (
                "an answer running until its close",
                &[Client(GET, Busy), Guest(b"HTTP/1.0 200 OK\r\n\r\nhi", Busy), Guest(OK, Busy)],
            ),
            ("a guest speaking first", &[Guest(b"220 hello\r\n", Busy), Client(GET, Busy)]),
            (
                "a guest speaking unasked",
                &[Client(GET, Busy), Guest(OK, Quiet), Guest(b"more", Busy)],
            ),
            ("no HTTP", &[Client(b"\x16\x03\x01", Busy), Guest(OK, Busy)]),
            (
                "another protocol after the guest's answer",
                &[Client(GET, Busy), Guest(b"+PONG\r\n", Busy), Guest(OK, Busy)],
            ),
            (
                "a body framed two ways",
                &[
                    Client(
                        b"POST / HTTP/1.1\r\ncontent-length: 1\r\ntransfer-encoding: chunked\r\n\r\n",
                        Busy,
                    ),
                    Guest(OK, Busy),
                ],
            ),
            (
                "a chunk not read as one",
                &[Client(GET, Busy), Guest(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nxyz\r\n", Busy), Guest(OK, Busy)],
            ),
        ];
        for (name, steps) in cases {
            for alone in [false, true] {
                let mut exchanges = Exchanges::default();
                assert_eq!(exchanges.standing(), Quiet, "{name}");
                for step in steps.iter() {
                    let (bytes, standing, client_sent) = match step {
                        Client(bytes, standing) => (bytes, standing, true),
                        Guest(bytes, standing) => (bytes, standing, false),
                    };
                    let pieces: Vec<&[u8]> = match alone {
                        true => bytes.chunks(1).collect(),
                        false => vec![bytes],
                    };
                    for piece in pieces {
                        match client_sent {
                            true => exchanges.client_sent(piece),
                            false => exchanges.guest_sent(piece),
                        }
                    }
                    assert_eq!(
                        exchanges.standing(),
                        *standing,
                        "{name}, one byte at a time: {alone}: {}",
                        String::from_utf8_lossy(bytes)
                    );
                }
            }
        }
    }

    /// An h2 connection's request is in progress while its stream is open, and a GOAWAY can be
    /// written between two frames of the guest's, once it spoke, naming the last stream opened.
    #[test]
    fn an_h2_request_is_in_progress_while_its_stream_is_open() {
        let settings = frame(0x4, 0, 0, &[]);
        let ping = frame(0x6, 0, 0, &[0; 8]);
        let goaway = |last: u8| {
            let payload = [0, 0, 0, last, 0, 0, 0, 0];
            frame(GOAWAY, 0, 0, &payload)
        };
        for alone in [false, true] {
            let feed = |exchanges: &mut Exchanges, client_sent: bool, bytes: &[u8]| {
                let size = if alone { 1 } else { bytes.len().max(1) };
                for piece in bytes.chunks(size) {
                    match client_sent {
                        true => exchanges.client_sent(piece),
                        false => exchanges.guest_sent(piece),
                    }
                }
            };
            let mut exchanges = Exchanges::default();
            feed(&mut exchanges, true, &[PREFACE, &settings].concat());
            assert_eq!(exchanges.standing(), Quiet);
            assert_eq!(exchanges.goaway(), None, "the guest's preface not sent");
            feed(&mut exchanges, false, &settings);
            assert_eq!(exchanges.goaway(), Some(goaway(0)));
            // a PING is no request; a stream, its response begun, then ended, is
            feed(&mut exchanges, true, &ping);
            assert_eq!(exchanges.standing(), Quiet);
            feed(&mut exchanges, true, &frame(HEADERS, END_STREAM, 1, b"x"));
            assert_eq!(exchanges.standing(), Busy);
            feed(&mut exchanges, false, &frame(HEADERS, 0, 1, b"y"));
            feed(&mut exchanges, false, &frame(DATA, 0, 1, b"z"));
            assert_eq!(exchanges.standing(), Busy);
            let answer = frame(DATA, END_STREAM, 1, b"z");
            feed(&mut exchanges, false, &answer[..answer.len() - 1]);
            assert_eq!(exchanges.goaway(), None, "within a frame");
            feed(&mut exchanges, false, &answer[answer.len() - 1..]);
            assert_eq!(exchanges.standing(), Quiet);
            assert_eq!(exchanges.goaway(), Some(goaway(1)));
            // its client's side still open, sending but for another stream's request sent whole;
            // reset either way
            feed(&mut exchanges, true, &frame(HEADERS, 0, 3, b"x"));
            assert_eq!(exchanges.standing(), Sending);
            feed(&mut exchanges, true, &frame(HEADERS, END_STREAM, 9, b"x"));
            assert_eq!(exchanges.standing(), Busy);
            feed(&mut exchanges, false, &frame(RST_STREAM, 0, 9, &[0; 4]));
            feed(&mut exchanges, false, &frame(HEADERS, END_STREAM, 3, b"y"));
            assert_eq!(exchanges.standing(), Sending);
            feed(&mut exchanges, true, &frame(DATA, END_STREAM, 3, b"x"));
            assert_eq!(exchanges.standing(), Quiet);
            for (client_sent, reset) in [(true, 5), (false, 7)] {
                feed(&mut exchanges, true, &frame(HEADERS, 0, reset, b"x"));
                assert_eq!(exchanges.standing(), Sending);
                feed(
                    &mut exchanges,
                    client_sent,
                    &frame(RST_STREAM, 0, reset, &[0; 4]),
                );
                assert_eq!(exchanges.standing(), Quiet);
            }
            assert_eq!(exchanges.goaway(), Some(goaway(9)));
            // pushing, the guest answers what no stream of the client's stands for
            feed(&mut exchanges, false, &frame(PUSH_PROMISE, 0, 1, &[0; 4]));
            assert_eq!(exchanges.standing(), Busy);
            assert_eq!(exchanges.goaway(), None);
        }
    }

    /// A head past the bound cannot be followed, its line not ended either.
    #[test]
    fn a_head_past_the_bound_cannot_be_followed() {
        let mut exchanges = Exchanges::default();
        let long = format!("GET /{}", "x".repeat(MAX_HEAD - 5));
        for piece in long.as_bytes().chunks(4096) {
            assert_eq!(exchanges.standing(), Standing::Quiet);
            exchanges.client_sent(piece);
        }
        assert_eq!(exchanges.standing(), Standing::Busy);
    }
}
