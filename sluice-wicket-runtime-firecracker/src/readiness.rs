//! A route's readiness check (`readiness`, as a CF app's readiness health check): what tells
//! the VMs a route keeps ready to take connections, and the probe of one of them.
//!
//! The wicket probes a VM through its guest's relay, as a connection reaches the process: a
//! vsock stream to the route's port, which `/init` relays to the process. The probe so takes
//! the way a connection takes, and needs nothing of the guest but what serves the connections:
//! any rootfs takes it.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::vm::Vm;
use crate::wire;

/// Default of `readiness-timeout-ms`: how long a probe may take, as CF's invocation timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

/// Default of `readiness-interval-ms`: between the probes of a VM ready, as CF's interval.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);

/// Between the probes of a VM not ready, from its start or since a probe failed (its interval,
/// if shorter): a VM serves a moment after its check passes. Diego's is 2s.
pub const UNTIL_READY: Duration = Duration::from_millis(500);

/// The bytes of an answer a probe reads at most: its status line.
const STATUS_LINE: usize = 80;

/// The default endpoint of `readiness=http`.
pub const DEFAULT_ENDPOINT: &str = "/";

/// What tells a route's VMs ready, and how often it is asked.
#[derive(Clone, Debug, PartialEq)]
pub struct Readiness {
    pub check: Check,
    /// How long a probe may take.
    pub timeout: Duration,
    /// Between the probes of a VM ready.
    pub interval: Duration,
}

impl Default for Readiness {
    fn default() -> Self {
        Readiness {
            check: Check::Process,
            timeout: DEFAULT_TIMEOUT,
            interval: DEFAULT_INTERVAL,
        }
    }
}

impl Readiness {
    /// The wait before the next probe of a VM, `ready` or not as its last one found it; None for
    /// a check that probes nothing.
    pub fn next(&self, ready: bool) -> Option<Duration> {
        match (&self.check, ready) {
            (Check::Process, _) => None,
            (_, true) => Some(self.interval),
            (_, false) => Some(UNTIL_READY.min(self.interval)),
        }
    }
}

/// How a VM is found ready.
#[derive(Clone, Debug, PartialEq)]
pub enum Check {
    /// Up: its guest accepting on the route's port, nothing more asked.
    Process,
    /// A connection to the process's port, which it accepts.
    Port,
    /// A `GET` of the endpoint (a path, a query with it), answered 200.
    Http(Arc<str>),
}

impl Check {
    /// `name` of `readiness=<name>`, the endpoint of `http` the default one.
    pub fn named(name: &str) -> Result<Check, String> {
        match name {
            "process" => Ok(Check::Process),
            "port" => Ok(Check::Port),
            "http" => Ok(Check::Http(DEFAULT_ENDPOINT.into())),
            _ => Err(format!(
                "readiness={name}: process, port or http, as a CF app's readiness check"
            )),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Check::Process => "process",
            Check::Port => "port",
            Check::Http(_) => "http",
        }
    }
}

/// The endpoint `value` of `readiness-endpoint`: a path (a query with it, no fragment) of what
/// a request line takes, no space or control character.
pub fn endpoint(value: &str) -> Result<Arc<str>, String> {
    let visible = value.bytes().all(|byte| byte.is_ascii_graphic());
    if !value.starts_with('/') || !visible || value.contains('#') {
        return Err(format!(
            "readiness-endpoint={value}: a path from /, of visible ASCII and no fragment"
        ));
    }
    Ok(value.into())
}

/// Probes the process behind the guest's `port` of `vm` as `readiness` asks, `host` the host
/// an `http` request names: within `readiness.timeout`; why not, when it fails.
pub async fn probe(vm: &Vm, port: u32, readiness: &Readiness, host: &str) -> Result<(), String> {
    let within = readiness.timeout;
    let late = || format!("no answer within {}ms", within.as_millis());
    match &readiness.check {
        Check::Process => Ok(()),
        Check::Port => {
            let dialed = tokio::time::timeout(within, vm.dial(port)).await;
            let stream = dialed.map_err(|_| late())?.map_err(|e| e.to_string())?;
            // the guest drops what the process refuses at once
            match Vm::accepted(stream, within.min(crate::vm::REFUSAL_TIMEOUT)).await {
                true => Ok(()),
                false => Err(format!("its port {port} refuses")),
            }
        }
        Check::Http(endpoint) => {
            let asked = async {
                let stream = vm.dial(port).await.map_err(|e| e.to_string())?;
                http(stream, endpoint, host).await
            };
            tokio::time::timeout(within, asked)
                .await
                .map_err(|_| format!("GET {endpoint}: {}", late()))?
        }
    }
}

/// `GET endpoint` on the guest stream `stream`, in the vsock framing: its answer's status line
/// read, a 200 Ok, any other why not. The rest of the answer is not read: the stream is dropped,
/// the request half-closed after it (`Connection: close`).
async fn http<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    endpoint: &str,
    host: &str,
) -> Result<(), String> {
    let request = format!(
        "GET {endpoint} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: sluice-wicket readiness check\r\n\
         Connection: close\r\n\r\n"
    );
    let failed = |e: std::io::Error| format!("GET {endpoint}: {e}");
    wire::write_data(&mut stream, request.as_bytes())
        .await
        .map_err(failed)?;
    wire::write_close(&mut stream).await.map_err(failed)?;
    let mut reader = wire::Reader::new(stream);
    let mut head = Vec::with_capacity(STATUS_LINE);
    let mut buffer = [0u8; STATUS_LINE];
    let line = loop {
        if let Some(end) = head.iter().position(|&byte| byte == b'\r' || byte == b'\n') {
            break &head[..end];
        }
        if head.len() >= STATUS_LINE {
            break &head[..];
        }
        match reader.read(&mut buffer[..STATUS_LINE - head.len()]).await {
            Ok(wire::Read::Data(n)) => head.extend_from_slice(&buffer[..n]),
            Ok(wire::Read::Close | wire::Read::Eof) if head.is_empty() => {
                return Err(format!("GET {endpoint}: no answer"));
            }
            Ok(wire::Read::Close | wire::Read::Eof) => break &head[..],
            Err(e) => return Err(failed(e)),
        }
    };
    let line = String::from_utf8_lossy(line);
    let status = line.strip_prefix("HTTP/1.").and_then(|rest| rest.get(2..5));
    match status {
        Some("200") => Ok(()),
        _ => Err(format!("GET {endpoint}: {line}")),
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::DuplexStream;

    use super::*;

    /// The wicket's end of a stream whose guest double answers with `frames`, its CLOSE after
    /// them, once it read the request.
    fn guest(frames: Vec<&'static [u8]>) -> DuplexStream {
        let (wicket, guest) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut guest = guest;
            let mut reader = wire::Reader::new(&mut guest);
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while let Ok(wire::Read::Data(n)) = reader.read(&mut buffer).await {
                request.extend_from_slice(&buffer[..n]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(
                request.starts_with("GET /ready?after=3 HTTP/1.1\r\nHost: a.local\r\n"),
                "{request}"
            );
            for frame in frames {
                let _ = wire::write_data(&mut guest, frame).await;
            }
            let _ = wire::write_close(&mut guest).await;
        });
        wicket
    }

    /// A probe asks the endpoint as the route's host and takes a 200 alone, the status line
    /// read across frames and no further; an answer without one fails it.
    #[tokio::test]
    async fn an_http_check_takes_a_200_alone() {
        let frames = |frames: &[&'static str]| -> Vec<&'static [u8]> {
            frames.iter().map(|frame| frame.as_bytes()).collect()
        };
        for (answer, expected) in [
            (frames(&["HTTP/1.1 200 OK\r\n\r\nready"]), Ok(())),
            (frames(&["HTTP/1.0 20", "0 OK\r\n"]), Ok(())),
            (
                frames(&["HTTP/1.1 503 Service Unavailable\r\n\r\n"]),
                Err("GET /ready?after=3: HTTP/1.1 503 Service Unavailable"),
            ),
            (
                frames(&["HTTP/1.1 204 No Content\r\n\r\n"]),
                Err("GET /ready?after=3: HTTP/1.1 204 No Content"),
            ),
            (frames(&[]), Err("GET /ready?after=3: no answer")),
            (frames(&["SSH-2.0"]), Err("GET /ready?after=3: SSH-2.0")),
        ] {
            let said = format!("{answer:?}");
            let probed = http(guest(answer), "/ready?after=3", "a.local").await;
            assert_eq!(probed, expected.map_err(String::from), "{said}");
        }
    }

    /// What a route's options make of a check: its names, an endpoint of a request line, the
    /// cadence of the probes.
    #[test]
    fn a_check_is_named_and_probed_in_its_cadence() {
        for name in ["process", "port", "http"] {
            assert_eq!(Check::named(name).unwrap().name(), name);
        }
        assert!(
            Check::named("tcp")
                .unwrap_err()
                .contains("process, port or http")
        );
        assert_eq!(&*endpoint("/ready?after=3").unwrap(), "/ready?after=3");
        for refused in ["ready", "/a b", "/a\r\nX: y", "/a#b", ""] {
            assert!(endpoint(refused).is_err(), "{refused:?}");
        }
        let http = Readiness {
            check: Check::Http("/".into()),
            ..Readiness::default()
        };
        assert_eq!(http.next(true), Some(DEFAULT_INTERVAL));
        assert_eq!(http.next(false), Some(UNTIL_READY));
        let often = Readiness {
            interval: Duration::from_millis(200),
            ..http
        };
        assert_eq!(often.next(false), Some(Duration::from_millis(200)));
        assert_eq!(Readiness::default().next(false), None);
    }
}
