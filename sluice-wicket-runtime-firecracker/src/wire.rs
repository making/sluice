//! The framing on every vsock stream between the wicket and the guest's `/init`.
//!
//! Firecracker's vsock device does not carry a half-close: the host side's EOF reaches
//! the guest as a full shutdown (its unsent answer is lost), and a guest's
//! `shutdown(SHUT_WR)` never reaches the host. A tunnel connection's CLOSE therefore
//! travels in band: `[type: u8][len: u32 BE][payload]`, `DATA` or `CLOSE` (no payload).
//! A stream ending without a `CLOSE` from its side failed.
//!
//! The guest's output travels on a stream of its own, framed alike ([`LOG_PORT`]), and
//! so do its egress connections, after their request ([`EGRESS_PORT`]).
//!
//! The guest has a copy of this module (`guest/src/wire.rs`): keep them alike.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const DATA: u8 = 0;
const CLOSE: u8 = 1;

/// The log stream's frames: a line of the processes' stdout, of their stderr, the lines
/// dropped, the answer to a sync -- whose request, the wicket's, is this one byte -- and
/// the image's process ended.
const STDOUT: u8 = 2;
const STDERR: u8 = 3;
const GAP: u8 = 4;
pub const SYNC: u8 = 5;
const EXITED: u8 = 6;

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
/// processes wrote before it was sent, and `EXITED` (no payload): the image's process ended.
pub const LOG_PORT: u32 = 1025;

/// The longest line of the log stream.
pub const MAX_LINE: usize = 16 * 1024;

/// The vsock port of the host the guest's `/init` dials for each connection a process
/// opens to a destination of its route's egress; Firecracker connects the stream to the
/// wicket's socket `<the VM's vsock socket>_1026`. It opens with the guest's request,
/// `CONNECT <destination>\n` (a [`Rule`] naming one), answered `OK\n` once the wicket
/// connected to the destination, else `ERR <why>\n` and the end of the stream; then it
/// carries the connection, framed as the others.
pub const EGRESS_PORT: u32 = 1026;

/// The guest kernel's command line parameter naming its route's egress:
/// `sluice.egress=<rule>,<rule>...` ([`Rule`]). A parameter of a module the kernel does
/// not have: no process gets it, `/init` reads `/proc/cmdline`.
pub const EGRESS_PARAMETER: &str = "sluice.egress";

/// The vsock port of the guest's env stream: the wicket dials it once `/init` runs, when
/// the VM's kernel command line says so ([`ENV_PARAMETER`]), and writes the variables of
/// the process's environment: `[len: u32 BE][a JSON object of name -> value]`, at most
/// [`MAX_ENV`] bytes, answered `OK\n`. `/init` starts nothing before: a clone has them in
/// its snapshot's memory, and only there. They may hold secrets, so they never pass
/// through a file or the kernel's command line (which any process of the guest reads).
pub const ENV_PORT: u32 = 1027;

/// The guest kernel's command line parameter telling `/init` to wait for the env stream
/// ([`ENV_PORT`]): `sluice.env=1`. As [`EGRESS_PARAMETER`], no process gets it.
pub const ENV_PARAMETER: &str = "sluice.env";

/// The most an env stream's JSON holds: any env of 128 KiB, escaped.
pub const MAX_ENV: usize = 1 << 20;

/// Writes the env stream's `json` and reads its answer (the wicket's half).
#[allow(dead_code)] // the wicket's half: each copy has both
pub async fn send_env<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    json: &[u8],
) -> std::io::Result<()> {
    if json.len() > MAX_ENV {
        return Err(std::io::Error::other(format!(
            "an env of {} bytes: {MAX_ENV} at most",
            json.len()
        )));
    }
    let mut frame = Vec::with_capacity(4 + json.len());
    frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
    frame.extend_from_slice(json);
    stream.write_all(&frame).await?;
    let answer = read_short_line(stream, MAX_EGRESS_LINE).await?;
    match answer.as_slice() {
        b"OK" => Ok(()),
        other => Err(std::io::Error::other(format!(
            "not an answer: {}",
            String::from_utf8_lossy(other)
        ))),
    }
}

/// Reads the env stream's JSON (the guest's half); [`answer_env`] answers it.
#[allow(dead_code)] // the guest's half: each copy has both
pub async fn read_env<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    reader.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_ENV {
        return Err(std::io::Error::other(format!(
            "an env of {len} bytes: {MAX_ENV} at most"
        )));
    }
    let mut json = vec![0u8; len];
    reader.read_exact(&mut json).await?;
    Ok(json)
}

/// Answers the env stream (the guest's half).
#[allow(dead_code)] // the guest's half: each copy has both
pub async fn answer_env<W: AsyncWrite + Unpin>(writer: &mut W) -> std::io::Result<()> {
    writer.write_all(b"OK\n").await
}

/// The guest kernel's command line parameter naming where `/init` mounts the VM's disk, the
/// drive after its rootfs ([`DISK_DEVICE`]), before the process starts: `sluice.disk=<path>`.
/// As [`EGRESS_PARAMETER`], no process gets it.
pub const DISK_PARAMETER: &str = "sluice.disk";

/// The VM's disk, as its guest sees it: Firecracker attaches the root device first.
pub const DISK_DEVICE: &str = "/dev/vdb";

/// The control stream's question of a VM with a disk, once `/init` runs: answered `OK\n` with
/// the disk mounted, else `ERR <why>\n`, the process not started (a guest predating disks
/// answers as to a line it does not know). The disk's own errors reach the wicket so.
pub const DISK_QUESTION: &str = "DISK";

/// The control stream's stop notice, [`stop_line`]: `/init` has the process end, SIGTERM and
/// `grace` to do it (SIGKILL after), then writes what is dirty and unmounts the disk, and
/// answers `OK\n`. What was only in the guest's page cache is gone with the VM otherwise.
#[allow(dead_code)] // the wicket's half: each copy has both
pub fn stop_line(grace: Duration) -> String {
    format!("STOP {}\n", grace.as_millis())
}

/// The grace of a stop notice's line (its newline trimmed); None for another line.
#[allow(dead_code)] // the guest's half: each copy has both
pub fn parse_stop(line: &str) -> Option<Duration> {
    let millis = line.strip_prefix("STOP ")?.parse().ok()?;
    Some(Duration::from_millis(millis))
}

/// The longest line of an egress stream's request or answer.
const MAX_EGRESS_LINE: usize = 512;

/// A destination of a route's egress: a name or an address range, and a port --
/// `db.internal:5432`, `10.0.0.5:5432`, `10.0.0.0/24:5432`, `[2001:db8::5]:443`,
/// `[2001:db8::/32]:443`. A name is lowercase, its last label starts with a letter (no
/// address in disguise, `10.1` or `0x7f.1`); a range's address has no bit set past its
/// prefix. An egress request names one destination: a name, or one address.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rule {
    pub host: Host,
    pub port: u16,
}

/// What a [`Rule`] names: a host name, or an address and the bits of its prefix (one
/// address: its family's all).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Host {
    Name(String),
    Net(IpAddr, u8),
}

/// The bits of an address of `ip`'s family.
pub fn bits(ip: IpAddr) -> u8 {
    match ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

/// An address's bits from the top: an IPv4 address's are the first 32.
fn top(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(ip) => u128::from(u32::from(ip)) << 96,
        IpAddr::V6(ip) => u128::from(ip),
    }
}

/// Whether the range of `net` and `prefix` holds `ip` (an IPv4 address mapped into IPv6
/// is IPv4's).
#[allow(dead_code)] // the wicket's: each copy has it
pub fn holds(net: IpAddr, prefix: u8, ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    // the first `prefix` bits alike
    net.is_ipv4() == ip.is_ipv4()
        && (top(net) ^ top(ip))
            .checked_shr(128 - u32::from(prefix))
            .unwrap_or(0)
            == 0
}

impl Rule {
    /// The rule of one address and port (an IPv4 address mapped into IPv6 is IPv4's).
    pub fn address(address: SocketAddr) -> Rule {
        let ip = address.ip().to_canonical();
        Rule {
            host: Host::Net(ip, bits(ip)),
            port: address.port(),
        }
    }

    /// Whether it names one destination: a name, or one address.
    pub fn is_one(&self) -> bool {
        match self.host {
            Host::Name(_) => true,
            Host::Net(ip, prefix) => prefix == bits(ip),
        }
    }
}

impl std::str::FromStr for Rule {
    type Err = String;

    fn from_str(text: &str) -> Result<Rule, String> {
        let (host, port) = match text.strip_prefix('[') {
            Some(rest) => {
                let (net, port) = rest
                    .split_once("]:")
                    .ok_or_else(|| format!("{text}: expected [<IPv6 address>[/<bits>]]:<port>"))?;
                (Rule::net(net, true)?, port)
            }
            None => {
                let (host, port) = text
                    .rsplit_once(':')
                    .ok_or_else(|| format!("{text}: expected <host>:<port>"))?;
                let host = match host.contains('/') || host.parse::<Ipv4Addr>().is_ok() {
                    true => Rule::net(host, false)?,
                    false => Host::Name(Rule::name(host)?),
                };
                (host, port)
            }
        };
        match port.parse::<u16>() {
            Ok(port) if port > 0 => Ok(Rule { host, port }),
            _ => Err(format!("{text}: port {port}: expected 1 to 65535")),
        }
    }
}

impl Rule {
    /// An address and its prefix's bits (`<address>[/<bits>]`), of IPv6 or of IPv4.
    fn net(text: &str, v6: bool) -> Result<Host, String> {
        let (address, prefix) = match text.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (text, None),
        };
        let ip = match v6 {
            true => address.parse().map(IpAddr::V6),
            false => address.parse().map(IpAddr::V4),
        }
        .map_err(|_| format!("{address}: not an IPv{} address", if v6 { 6 } else { 4 }))?;
        let prefix = match prefix {
            None => bits(ip),
            Some(prefix) => prefix
                .parse::<u8>()
                .ok()
                .filter(|prefix| *prefix <= bits(ip))
                .ok_or_else(|| format!("{text}: /{prefix}: expected 0 to {}", bits(ip)))?,
        };
        // the range starts at its address
        if top(ip).checked_shl(u32::from(prefix)).unwrap_or(0) != 0 {
            return Err(format!("{text}: bits set past the prefix"));
        }
        Ok(Host::Net(ip, prefix))
    }

    /// A host name, lowercased: labels of letters, digits, `-` and `_`, the last one
    /// starting with a letter.
    fn name(text: &str) -> Result<String, String> {
        let name = text.to_ascii_lowercase();
        let label = |label: &str| {
            (1..=63).contains(&label.len())
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                && !label.starts_with('-')
                && !label.ends_with('-')
        };
        let last = name.rsplit('.').next().unwrap_or_default();
        if name.len() > 253
            || !name.split('.').all(label)
            || !last.starts_with(|first: char| first.is_ascii_lowercase())
        {
            return Err(format!(
                "{text}: not a host name (labels of letters, digits, - and _, the last one \
                 starting with a letter) nor an address"
            ));
        }
        Ok(name)
    }
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let port = self.port;
        match &self.host {
            Host::Name(name) => write!(f, "{name}:{port}"),
            Host::Net(IpAddr::V4(ip), 32) => write!(f, "{ip}:{port}"),
            Host::Net(IpAddr::V4(ip), prefix) => write!(f, "{ip}/{prefix}:{port}"),
            Host::Net(IpAddr::V6(ip), 128) => write!(f, "[{ip}]:{port}"),
            Host::Net(IpAddr::V6(ip), prefix) => write!(f, "[{ip}/{prefix}]:{port}"),
        }
    }
}

/// Sends the egress request for `to` on `stream` and reads the answer (the guest's
/// half): Ok once the wicket connected to it.
#[allow(dead_code)] // the guest's half: each copy has both
pub async fn request_egress<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    to: &Rule,
) -> std::io::Result<()> {
    stream
        .write_all(format!("CONNECT {to}\n").as_bytes())
        .await?;
    let answer = read_short_line(stream, MAX_EGRESS_LINE).await?;
    match answer.strip_prefix(b"ERR ") {
        _ if answer == b"OK" => Ok(()),
        Some(why) => Err(std::io::Error::other(String::from_utf8_lossy(why))),
        None => Err(std::io::Error::other(format!(
            "not an answer: {}",
            String::from_utf8_lossy(&answer)
        ))),
    }
}

/// Reads an egress request (the wicket's half): the destination it names.
#[allow(dead_code)] // the wicket's half: each copy has both
pub async fn read_egress_request<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Rule> {
    let invalid = |why: String| std::io::Error::new(std::io::ErrorKind::InvalidData, why);
    let line = read_short_line(reader, MAX_EGRESS_LINE).await?;
    let line = String::from_utf8_lossy(&line);
    let to: Rule = line
        .strip_prefix("CONNECT ")
        .ok_or_else(|| invalid(format!("not a request: {line}")))?
        .parse()
        .map_err(invalid)?;
    if !to.is_one() {
        return Err(invalid(format!("{to}: a range, not a destination")));
    }
    Ok(to)
}

/// Answers an egress request (the wicket's half): connected, else why not.
#[allow(dead_code)] // the wicket's half: each copy has both
pub async fn answer_egress<W: AsyncWrite + Unpin>(
    writer: &mut W,
    answer: Result<(), &str>,
) -> std::io::Result<()> {
    let line = match answer {
        Ok(()) => "OK\n".to_string(),
        Err(why) => format!("ERR {}\n", why.replace('\n', " ")),
    };
    writer.write_all(line.as_bytes()).await
}

/// Reads a line of at most `max` bytes, its newline dropped, byte by byte: what follows
/// it stays unread. A longer line is an error, and so is the end of the stream before it.
pub async fn read_short_line<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::with_capacity(16);
    loop {
        let byte = reader.read_u8().await?;
        if byte == b'\n' {
            return Ok(line);
        }
        if line.len() == max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("a line longer than {max} bytes"),
            ));
        }
        line.push(byte);
    }
}

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
    /// The image's process ended; the VM stays up.
    Exited,
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
        LogFrame::Exited => (EXITED, &[]),
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
        SYNC | EXITED => len == 0,
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
        EXITED => LogFrame::Exited,
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
        let grace = Duration::from_millis(2500);
        assert_eq!(parse_stop(stop_line(grace).trim_end()), Some(grace));
        assert_eq!(parse_stop("STOP soon"), None);
        assert_eq!(parse_stop(DISK_QUESTION), None);
    }

    /// A rule prints as it parses (a name lowercased); a range holds the addresses of its
    /// prefix; what is not a rule is refused, a request names one destination and is
    /// answered.
    #[tokio::test]
    async fn egress_rules_and_requests_read_as_written() {
        // the env stream's frame is read as written, and a longer one refused
        let (mut wicket, mut guest) = tokio::io::duplex(64);
        let sent = tokio::spawn(async move { send_env(&mut wicket, b"{\"K\":\"v\"}").await });
        assert_eq!(read_env(&mut guest).await.unwrap(), b"{\"K\":\"v\"}");
        answer_env(&mut guest).await.unwrap();
        sent.await.unwrap().unwrap();
        let (mut wicket, _guest) = tokio::io::duplex(64);
        let large = vec![b' '; MAX_ENV + 1];
        assert!(send_env(&mut wicket, &large).await.is_err());

        for (text, printed) in [
            ("DB.Internal:5432", "db.internal:5432"),
            ("my_db:6379", "my_db:6379"),
            ("10.0.0.5:5432", "10.0.0.5:5432"),
            ("10.0.0.0/24:5432", "10.0.0.0/24:5432"),
            ("0.0.0.0/0:443", "0.0.0.0/0:443"),
            ("[2001:db8::5]:443", "[2001:db8::5]:443"),
            ("[2001:db8::/32]:443", "[2001:db8::/32]:443"),
        ] {
            let rule: Rule = text.parse().unwrap();
            assert_eq!(rule.to_string(), printed);
            assert_eq!(printed.parse::<Rule>(), Ok(rule));
        }
        for (net, prefix, ip, held) in [
            ("10.0.0.0", 24, "10.0.0.77", true),
            ("10.0.0.0", 24, "10.0.1.0", false),
            ("10.0.0.0", 24, "::ffff:10.0.0.77", true),
            ("0.0.0.0", 0, "192.0.2.1", true),
            ("0.0.0.0", 0, "2001:db8::1", false),
            ("2001:db8::", 32, "2001:db8:ffff::1", true),
            ("2001:db8::", 32, "2001:db9::1", false),
            ("2001:db8::5", 128, "2001:db8::5", true),
        ] {
            let (net, ip) = (net.parse().unwrap(), ip.parse().unwrap());
            assert_eq!(holds(net, prefix, ip), held, "{net}/{prefix} {ip}");
        }
        for wrong in [
            "db.internal",
            "db.internal:0",
            "db.internal:65536",
            "10.1:5432",
            "0x7f.1:80",
            "-db:5432",
            "2001:db8::5:443",
            "[10.0.0.5]:443",
            "10.0.0.5/24:5432",
            "10.0.0.0/33:5432",
            "[2001:db8::/129]:443",
        ] {
            assert!(wrong.parse::<Rule>().is_err(), "{wrong}");
        }

        let (mut guest, mut wicket) = tokio::io::duplex(1024);
        let to: Rule = "[2001:db8::5]:443".parse().unwrap();
        let answered = async {
            assert_eq!(read_egress_request(&mut wicket).await.unwrap(), to);
            answer_egress(&mut wicket, Err("denied\nfor good")).await
        };
        let (requested, answered) = tokio::join!(request_egress(&mut guest, &to), answered);
        answered.unwrap();
        assert_eq!(requested.unwrap_err().to_string(), "denied for good");
        for request in ["CONNECT 10.0.0.0/24:5432\n", "OPEN db:5432\n"] {
            let (mut guest, mut wicket) = tokio::io::duplex(1024);
            guest.write_all(request.as_bytes()).await.unwrap();
            assert!(read_egress_request(&mut wicket).await.is_err(), "{request}");
        }
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
