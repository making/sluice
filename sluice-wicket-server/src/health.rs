//! A wicket's readiness probe (`--health`): `GET /ready` answers 200 while the wicket serves,
//! with what it notes if anything, 503 saying why while it does not; no credentials asked, nothing
//! else served. [`probe`] asks it,
//! where no HTTP client is at hand (a distroless image's healthcheck).

use std::io::{Read as _, Write as _};
use std::net::ToSocketAddrs as _;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::accept;

/// Connections open at once: probes, one at a time each.
const MAX_CONNECTIONS: usize = 16;

/// Serves the probe on `listener` for good, `ready` telling whether the wicket serves (Ok: what it
/// notes, if anything; Err: why not).
pub async fn run<F>(listener: TcpListener, ready: F)
where
    F: Fn() -> Result<Option<String>, String> + Send + Sync + 'static,
{
    let ready = Arc::new(ready);
    accept::run(
        listener,
        MAX_CONNECTIONS,
        "--health",
        move |stream, _, slot| {
            let ready = ready.clone();
            tokio::spawn(async move {
                crate::http::serve(stream, slot, |request| {
                    let answer = answer(request.method(), request.uri().path(), &*ready);
                    async move { answer }
                })
                .await;
            });
        },
    )
    .await
}

/// How long [`probe`] waits for the connection, and then for the answer.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Asks the readiness probe at `address` (`host:port`), blocking: Ok with its answer's body when it
/// says ready, Err with it (or the failure) when not.
pub fn probe(address: &str) -> Result<String, String> {
    let asked = || -> std::io::Result<String> {
        let to = address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no address"))?;
        let mut stream = std::net::TcpStream::connect_timeout(&to, PROBE_TIMEOUT)?;
        stream.set_read_timeout(Some(PROBE_TIMEOUT))?;
        stream.set_write_timeout(Some(PROBE_TIMEOUT))?;
        stream.write_all(b"GET /ready HTTP/1.1\r\nhost: probe\r\nconnection: close\r\n\r\n")?;
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer)?;
        Ok(String::from_utf8_lossy(&answer).into_owned())
    };
    let answer = asked().map_err(|e| format!("{address}: {e}"))?;
    let (head, body) = answer.split_once("\r\n\r\n").unwrap_or((&answer, ""));
    match head.split(' ').nth(1) {
        Some("200") => Ok(body.to_string()),
        _ => Err(match body.is_empty() {
            true => format!("{address}: {}", head.lines().next().unwrap_or_default()),
            false => body.to_string(),
        }),
    }
}

fn answer(
    method: &hyper::Method,
    path: &str,
    ready: &impl Fn() -> Result<Option<String>, String>,
) -> hyper::Response<String> {
    let (status, body) = match (path, method) {
        ("/ready", &hyper::Method::GET | &hyper::Method::HEAD) => match ready() {
            Ok(None) => (200, "ready\n".to_string()),
            Ok(Some(note)) => (200, format!("ready; {note}\n")),
            Err(why) => (503, format!("not ready: {why}\n")),
        },
        ("/ready", _) => (405, "GET /ready\n".to_string()),
        _ => (404, "GET /ready\n".to_string()),
    };
    hyper::Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("cache-control", "no-store")
        .body(body)
        .expect("a valid answer")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// The answer to `request` on a connection of its own, read to its end.
    async fn asked(address: std::net::SocketAddr, request: &str) -> String {
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!("{request} HTTP/1.1\r\nhost: probe\r\nconnection: close\r\n\r\n");
        client.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        client.read_to_string(&mut answer).await.unwrap();
        answer
    }

    /// `/ready` says what the wicket says, 200 and what it notes or 503 and why; no other path is
    /// served.
    #[tokio::test]
    async fn ready_answers_as_the_wicket_says() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let said: Arc<Mutex<Result<Option<String>, String>>> =
            Arc::new(Mutex::new(Err("starting".into())));
        let says = said.clone();
        tokio::spawn(run(listener, move || says.lock().unwrap().clone()));

        let answer = asked(address, "GET /ready").await;
        assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
        assert!(
            answer.ends_with("\r\n\r\nnot ready: starting\n"),
            "{answer}"
        );
        let asking = address.to_string();
        let probed = tokio::task::spawn_blocking(move || probe(&asking));
        assert_eq!(probed.await.unwrap(), Err("not ready: starting\n".into()));
        *said.lock().unwrap() = Ok(None);
        let answer = asked(address, "GET /ready").await;
        assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        assert!(answer.ends_with("\r\n\r\nready\n"), "{answer}");
        let asking = address.to_string();
        let probed = tokio::task::spawn_blocking(move || probe(&asking));
        assert_eq!(probed.await.unwrap(), Ok("ready\n".into()));
        *said.lock().unwrap() = Ok(Some("no instance ready for a.local".into()));
        let answer = asked(address, "GET /ready").await;
        assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        assert!(
            answer.ends_with("\r\n\r\nready; no instance ready for a.local\n"),
            "{answer}"
        );
        assert!(
            asked(address, "HEAD /ready")
                .await
                .starts_with("HTTP/1.1 200 ")
        );
        assert!(
            asked(address, "POST /ready")
                .await
                .starts_with("HTTP/1.1 405 ")
        );
        assert!(asked(address, "GET /").await.starts_with("HTTP/1.1 404 "));
    }
}
