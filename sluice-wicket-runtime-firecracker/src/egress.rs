//! The guests' egress: the connections a guest's processes open to the destinations of
//! its route's `egress` (`wire::Rule`), which the guest's `/init` relays over vsock
//! (`wire::EGRESS_PORT`) and the wicket connects to, if the route's rules have the
//! destination:
//!
//! ```text
//! process --TCP--> /init --vsock, framed--> wicket --TCP--> db.internal:5432
//! ```
//!
//! A VM's listener is the socket Firecracker connects the guest's streams of the port to,
//! `<the VM's vsock socket>_1026`, bound before the VM starts or is restored: a stream is
//! the listener's VM's. Nothing of the guest is taken on trust: `/init` asks for the
//! destinations of the rules alone, a guest's root for any. A name is connected to as the
//! host resolves it; a VM's connections open at once are [`MAX_CONNECTIONS`] at most.
//!
//! Each stream is recorded once it ended or was refused: a line on the wicket's stdout
//! (`egress [app.local vm-3] db.internal:5432 (10.0.0.5:5432): 1532ms, 1234 bytes out,
//! 15000 in`) and an `egress` line of the route's first host in the live access log
//! (`vm`, `to`, `peer`, `ms`, `out`, `in`, `error`).

use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::AbortHandle;

use crate::BoxError;
use crate::output::{Hosts, Sink};
use crate::wire::{self, Host, Rule, Stream};

/// A VM's egress connections open at once at most: past them a request is refused.
pub const MAX_CONNECTIONS: usize = 256;

/// How long the guest may take to send its request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a connection to a destination may take, its name resolved.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A route's egress: its rules, and where its records go.
pub struct Egress {
    rules: Vec<Rule>,
    hosts: Arc<Hosts>,
    sink: Arc<Sink>,
    /// A VM's connections open at once at most.
    max: usize,
    /// Where its connections are relayed.
    workers: Arc<Workers>,
}

/// A VM's listener being served ([`Egress::listen`]); dropped with the VM, it ends.
pub struct Listening(AbortHandle);

impl Drop for Listening {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What became of an egress stream.
struct Record {
    started: Instant,
    /// The destination asked for, once read.
    to: Option<Rule>,
    /// The address connected to.
    peer: Option<SocketAddr>,
    /// The bytes relayed to the destination, and from it.
    out: u64,
    came: u64,
}

impl Egress {
    /// The egress of a route's `rules`, its records naming the route's `hosts`, to `sink`.
    pub fn new(
        rules: Vec<Rule>,
        hosts: Arc<Hosts>,
        sink: Arc<Sink>,
        workers: Arc<Workers>,
    ) -> Arc<Egress> {
        Arc::new(Egress {
            rules,
            hosts,
            sink,
            max: MAX_CONNECTIONS,
            workers,
        })
    }

    /// Listens for the egress streams of the VM `vm` whose vsock socket is `vsock` (on the
    /// host), before the VM starts. Other users may write to the socket: the VM's
    /// `firecracker` connects to it, in jail mode as an id of its own; the VM's directory
    /// lets no one else in.
    pub fn listen(self: &Arc<Self>, vsock: &Path, vm: String) -> Result<Listening, BoxError> {
        let mut socket = vsock.as_os_str().to_owned();
        socket.push(format!("_{}", wire::EGRESS_PORT));
        let socket = Path::new(&socket);
        let listener =
            UnixListener::bind(socket).map_err(|e| format!("{}: {e}", socket.display()))?;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o602))?;
        let accepting = tokio::spawn(self.clone().accept(listener, vm.into()));
        Ok(Listening(accepting.abort_handle()))
    }

    /// Accepts each stream at once, served in a task of its own: Firecracker connects from
    /// its VM's thread, which waits while the backlog is full.
    async fn accept(self: Arc<Self>, listener: UnixListener, vm: Arc<str>) {
        let open = Arc::new(Semaphore::new(self.max));
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let permit = open.clone().try_acquire_owned().ok();
                    let Ok(stream) = stream.into_std() else {
                        continue;
                    };
                    let (egress, vm) = (self.clone(), vm.clone());
                    self.workers.place().spawn(async move {
                        // the worker's from here on
                        if let Ok(stream) = UnixStream::from_std(stream) {
                            egress.serve(stream, vm, permit).await;
                        }
                    });
                }
                // out of files, for one: a moment for some to close
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    /// Serves one stream of the VM `vm`, `permit` its place among the VM's connections
    /// (None: they are all taken), and records it.
    async fn serve(
        self: Arc<Self>,
        stream: UnixStream,
        vm: Arc<str>,
        permit: Option<OwnedSemaphorePermit>,
    ) {
        let mut record = Record {
            started: Instant::now(),
            to: None,
            peer: None,
            out: 0,
            came: 0,
        };
        let served = self.relay(stream, permit, &mut record).await;
        self.record(&vm, record, served.err()).await;
    }

    /// Reads the request on `stream`, refuses it or connects to its destination, and
    /// relays the connection until both directions closed, `permit` held until then.
    async fn relay(
        &self,
        mut stream: UnixStream,
        permit: Option<OwnedSemaphorePermit>,
        record: &mut Record,
    ) -> Result<(), String> {
        let request = tokio::time::timeout(REQUEST_TIMEOUT, wire::read_egress_request(&mut stream));
        let to = request
            .await
            .map_err(|_| format!("failed: no request within {REQUEST_TIMEOUT:?}"))?
            .map_err(|e| format!("failed: request: {e}"))?;
        record.to = Some(to.clone());
        let refused = match &permit {
            None => Some(format!(
                "the VM holds {} egress connection(s) already",
                self.max
            )),
            Some(_) if !self.allows(&to) => Some("not in the route's egress".to_string()),
            Some(_) => None,
        };
        if let Some(why) = refused {
            let _ = wire::answer_egress(&mut stream, Err(&why)).await;
            return Err(format!("denied: {why}"));
        }
        let connected = match tokio::time::timeout(CONNECT_TIMEOUT, connect(&to)).await {
            Ok(connected) => connected.map_err(|e| e.to_string()),
            Err(_) => Err(format!("no connection within {CONNECT_TIMEOUT:?}")),
        };
        let destination = match connected {
            Ok(destination) => destination,
            Err(why) => {
                let _ = wire::answer_egress(&mut stream, Err(&why)).await;
                return Err(format!("failed: {why}"));
            }
        };
        record.peer = destination.peer_addr().ok();
        let mut destination = Counted {
            inner: destination,
            read: 0,
            written: 0,
        };
        let relayed = match wire::answer_egress(&mut stream, Ok(())).await {
            Ok(()) => wire::relay(&mut destination, stream, None).await,
            Err(e) => Err(e),
        };
        (record.out, record.came) = (destination.written, destination.read);
        relayed.map_err(|e| {
            // reset, not ended: the destination takes the connection as failed
            let _ = destination.inner.set_zero_linger();
            format!("failed: {e}")
        })
    }

    /// Whether the rules have `to`, one destination: its name, or a range holding its
    /// address, at its port.
    fn allows(&self, to: &Rule) -> bool {
        self.rules.iter().any(|rule| {
            rule.port == to.port
                && match (&rule.host, &to.host) {
                    (Host::Name(allowed), Host::Name(name)) => allowed == name,
                    (Host::Net(net, prefix), Host::Net(ip, _)) => wire::holds(*net, *prefix, *ip),
                    _ => false,
                }
        })
    }

    /// Records the stream of the VM `vm`, failed or refused for `error`: a line on the
    /// wicket's stdout, a line in the live access log.
    async fn record(&self, vm: &str, record: Record, error: Option<String>) {
        let named = self.hosts.current();
        let to = record.to.as_ref().map_or("-".to_string(), Rule::to_string);
        let mut line = format!("egress [{} {vm}] {to}", named.label);
        let mut fields = vec![("vm", vm.to_string()), ("to", to)];
        if let Some(peer) = record.peer {
            let ms = record.started.elapsed().as_millis();
            line.push_str(&format!(
                " ({peer}): {ms}ms, {} bytes out, {} in",
                record.out, record.came
            ));
            fields.extend([
                ("peer", peer.to_string()),
                ("ms", ms.to_string()),
                ("out", record.out.to_string()),
                ("in", record.came.to_string()),
            ]);
        }
        if let Some(error) = error {
            line.push_str(if record.peer.is_some() { "; " } else { ": " });
            line.push_str(&error);
            fields.push(("error", error));
        }
        line.push('\n');
        if self.sink.tap.on() {
            self.sink.tap.publish("egress", named.key.clone(), fields);
        }
        self.sink
            .printer
            .print(Stream::Stdout, line.into_bytes())
            .await;
    }
}

/// Connects to `to`: a name as the host resolves it, each of its addresses in turn.
async fn connect(to: &Rule) -> std::io::Result<TcpStream> {
    let stream = match &to.host {
        Host::Name(name) => TcpStream::connect((name.as_str(), to.port)).await?,
        Host::Net(ip, _) => TcpStream::connect(SocketAddr::new(ip.to_canonical(), to.port)).await?,
    };
    // the relay adds no delay to the guest's writes
    stream.set_nodelay(true)?;
    sluice_wicket_tunnel::listener::keep_alive(&stream)?;
    Ok(stream)
}

/// A destination's connection, counting the bytes read from it and written to it.
struct Counted {
    inner: TcpStream,
    read: u64,
    written: u64,
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        self.read += (buf.filled().len() - before) as u64;
        polled
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = polled {
            self.written += n as u64;
        }
        polled
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use sluice_wicket_tunnel::flume::Tap;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;
    use crate::output::{Printer, Written};

    /// A guest's egress stream to `to` through the listener at `socket`, its request
    /// answered.
    async fn open(socket: &Path, to: &str) -> std::io::Result<UnixStream> {
        let mut stream = UnixStream::connect(socket).await?;
        wire::request_egress(&mut stream, &to.parse().unwrap()).await?;
        Ok(stream)
    }

    /// What a destination answers to `request` sent on `stream`, half-closed after it.
    async fn exchange(stream: &mut UnixStream, request: &[u8]) -> Vec<u8> {
        wire::write_data(stream, request).await.unwrap();
        wire::write_close(stream).await.unwrap();
        let mut reader = wire::Reader::new(stream);
        let (mut answer, mut buffer) = (Vec::new(), [0u8; 64]);
        loop {
            match reader.read(&mut buffer).await.unwrap() {
                wire::Read::Data(n) => answer.extend_from_slice(&buffer[..n]),
                wire::Read::Close => return answer,
                wire::Read::Eof => panic!("no CLOSE"),
            }
        }
    }

    /// A guest's destinations of the rules -- a name, an address of a range, at their
    /// port -- are connected to and relayed both ways, their half-closes passed on;
    /// another, and one past the VM's bound, are refused. Each stream is recorded on the
    /// printer and the tap.
    #[tokio::test]
    async fn a_guests_destinations_are_checked_relayed_and_recorded() {
        // the destination: answers what it read, once its client half-closed (a
        // connection cut is reset)
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = server.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut connection, _)) = server.accept().await {
                tokio::spawn(async move {
                    let mut got = Vec::new();
                    if connection.read_to_end(&mut got).await.is_ok() {
                        let _ = connection.write_all(&[b"got ", &got[..]].concat()).await;
                    }
                });
            }
        });
        let stdout = Written::default();
        let tap = Tap::new();
        let sink = Arc::new(Sink {
            printer: Printer::new(stdout.clone(), Written::default()),
            tap: tap.clone(),
        });
        let rules = [format!("localhost:{port}"), format!("127.0.0.0/8:{port}")];
        let egress = Arc::new(Egress {
            rules: rules.iter().map(|rule| rule.parse().unwrap()).collect(),
            hosts: Hosts::new(&["a.local".into()]),
            sink,
            max: 2,
            workers: Arc::new(Workers::start(1, "relay").unwrap()),
        });
        let dir = std::env::temp_dir().join(format!("sfc-egress-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let _listening = egress.listen(&dir.join("v.sock"), "vm-3".into()).unwrap();
        let socket = dir.join(format!("v.sock_{}", wire::EGRESS_PORT));

        for to in [format!("localhost:{port}"), format!("127.0.0.1:{port}")] {
            let mut stream = open(&socket, &to).await.unwrap();
            assert_eq!(exchange(&mut stream, b"ping").await, b"got ping", "{to}");
        }
        // a destination gone without a FIN or reset gives its slot back
        let connected = connect(&rules[0].parse().unwrap()).await.unwrap();
        assert!(socket2::SockRef::from(&connected).keepalive().unwrap());
        for to in [
            format!("localhost:{}", port + 1),
            format!("db.internal:{port}"),
            format!("[::1]:{port}"),
        ] {
            let refused = open(&socket, &to).await.unwrap_err();
            assert_eq!(refused.to_string(), "not in the route's egress", "{to}");
        }
        let held = [
            open(&socket, &format!("localhost:{port}")).await.unwrap(),
            open(&socket, &format!("127.0.0.1:{port}")).await.unwrap(),
        ];
        let refused = open(&socket, &format!("localhost:{port}"))
            .await
            .unwrap_err();
        assert_eq!(
            refused.to_string(),
            "the VM holds 2 egress connection(s) already"
        );
        // gone without CLOSE: the guest's VM ended
        drop(held);

        let expected = [
            format!("egress [a.local vm-3] localhost:{port} (127.0.0.1:{port}): "),
            format!("egress [a.local vm-3] 127.0.0.1:{port} (127.0.0.1:{port}): "),
            format!(
                "egress [a.local vm-3] localhost:{}: denied: not in the route's egress",
                port + 1
            ),
            format!("egress [a.local vm-3] db.internal:{port}: denied: not in the route's egress"),
            format!("egress [a.local vm-3] [::1]:{port}: denied: not in the route's egress"),
            format!(
                "egress [a.local vm-3] localhost:{port}: denied: the VM holds 2 egress \
                 connection(s) already"
            ),
        ];
        let deadline = Instant::now() + Duration::from_secs(10);
        let lines = loop {
            let text = stdout.text();
            let lines: Vec<String> = text.lines().map(String::from).collect();
            if lines.len() == expected.len() + 2 {
                break lines;
            }
            assert!(Instant::now() < deadline, "{text}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        for (line, expected) in lines.iter().zip(&expected) {
            assert!(line.starts_with(expected.as_str()), "{line}");
        }
        assert!(
            lines[0].ends_with(", 4 bytes out, 8 in"),
            "the bytes relayed: {}",
            lines[0]
        );
        // the held ones, cut
        for line in &lines[expected.len()..] {
            assert!(
                line.ends_with("bytes out, 0 in; failed: peer gone without CLOSE"),
                "{line}"
            );
        }
        let tapped: Vec<_> = tap
            .recent()
            .iter()
            .map(|line| line.fields.clone())
            .collect();
        assert_eq!(tapped.len(), lines.len());
        let field = |name: &str| tapped[2].iter().find(|(key, _)| *key == name).cloned();
        assert_eq!(field("to"), Some(("to", format!("localhost:{}", port + 1))));
        assert_eq!(
            field("error"),
            Some(("error", "denied: not in the route's egress".to_string()))
        );
        assert!(
            tap.recent()
                .iter()
                .all(|line| line.kind == "egress" && line.route_key == "a.local")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
