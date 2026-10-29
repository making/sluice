//! Local mode: `--listen` alone (no sluice server) serves the routes over
//! plain TCP, routed by the request's host, at most `--max-connections` at
//! once; a keyvalue route keeps its count in the sqlite file, across a
//! restart and shared by two wickets on the file, or in a redis store two
//! wickets share as wickets on two hosts would. Nothing drains: SIGTERM ends
//! the process at once, with 0. The console deploys an uploaded component as
//! a route at once, and moves it to another host as it is.
#![cfg(unix)]

#[path = "support/redis_server.rs"]
mod redis_server;

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Hang guard of every wait; the first one covers the components' load.
const HANG: Duration = Duration::from_secs(60);

/// The directory of the hello and counter guests, built as the unit tests
/// build them.
fn guests() -> PathBuf {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let output = Command::new("cargo")
        .args(["build", "--release", "-p", "hello", "-p", "counter"])
        .env_remove("RUSTUP_TOOLCHAIN")
        .env_remove("CARGO")
        .current_dir(&examples)
        .output()
        .expect("run cargo (rustup) for the guest build");
    assert!(
        output.status.success(),
        "guest build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| examples.join("target"))
        .join("wasm32-wasip3/release")
}

/// A wicket in local mode on an ephemeral port, killed on drop.
struct Wicket {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Wicket {
    /// Starts one with `args` besides `--listen`; [`Wicket::address`] waits
    /// until it listens.
    fn start(args: &[&str]) -> Wicket {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-wasi"))
            .args(["--listen", "127.0.0.1:0", "--watch-interval", "0"])
            .args(args)
            .stdout(Stdio::piped())
            .spawn()
            .expect("run sluice-wicket-wasi");
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (lines, received) = mpsc::channel();
        std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                if lines.send(line).is_err() {
                    return;
                }
            }
        });
        Wicket {
            child,
            lines: received,
        }
    }

    /// The address it listens on, from its log.
    fn address(&self) -> String {
        self.logged("listening on ")
    }

    /// The address its console listens on, from its log (after
    /// [`Wicket::address`]).
    fn console(&self) -> String {
        self.logged("console on http://")
    }

    /// The first word after `prefix` of the next line of its log starting
    /// with it.
    fn logged(&self, prefix: &str) -> String {
        let deadline = Instant::now() + HANG;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(left)
                .unwrap_or_else(|e| panic!("never logged '{prefix}': {e}"));
            if let Some(rest) = line.strip_prefix(prefix) {
                return rest.split(' ').next().unwrap().to_string();
            }
        }
    }
}

impl Drop for Wicket {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// An http/1.0 GET of `path` with `host`: the status and the body (read to
/// the close).
async fn get(address: &str, host: &str, path: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!("GET {path} HTTP/1.0\r\nhost: {host}\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(HANG, stream.read_to_end(&mut raw))
        .await
        .expect("answered")
        .unwrap();
    let text = String::from_utf8(raw).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").expect("a response");
    (head[9..12].parse().unwrap(), body.to_string())
}

/// An h2c (prior knowledge) GET of `path` with the authority `host`: the
/// body.
async fn get_h2(address: &str, host: &str, path: &str) -> String {
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (client, connection) = h2::client::handshake(stream).await.unwrap();
    tokio::spawn(connection);
    let mut client = client.ready().await.unwrap();
    let request = http::Request::get(format!("http://{host}{path}"))
        .body(())
        .unwrap();
    let (response, _) = client.send_request(request, true).unwrap();
    let mut body = response.await.unwrap().into_body();
    let mut text = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        let _ = body.flow_control().release_capacity(chunk.len());
        text.extend_from_slice(&chunk);
    }
    String::from_utf8(text).unwrap()
}

/// An http/1.1 request to the console at `address`, closed after it: the
/// status, the head (in lower case) and the body.
async fn console(
    address: &str,
    request: &str,
    headers: &[&str],
    body: &[u8],
) -> (u16, String, String) {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut head = format!(
        "{request} HTTP/1.1\r\nhost: {address}\r\nconnection: close\r\ncontent-length: {}\r\n",
        body.len()
    );
    for header in headers {
        head.push_str(&format!("{header}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(HANG, stream.read_to_end(&mut raw))
        .await
        .expect("answered")
        .unwrap();
    let text = String::from_utf8(raw).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").expect("a response");
    (
        head[9..12].parse().unwrap(),
        head.to_lowercase(),
        body.to_string(),
    )
}

/// Two wickets counting on a.local at once, 20 increments and 20 cas.
async fn count_at_once(a: &str, b: &str) {
    let mut counting = tokio::task::JoinSet::new();
    for n in 0..40 {
        let address = if n % 2 == 0 { a } else { b }.to_string();
        let path = if n % 4 < 2 { "/" } else { "/cas" };
        counting.spawn(async move { get(&address, "a.local", path).await.0 });
    }
    while let Some(status) = counting.join_next().await {
        assert_eq!(status.unwrap(), 200);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn local_mode_routes_by_host_and_keeps_the_keyvalue_file() {
    let guests = guests();
    let dir = std::env::temp_dir().join(format!("sluice-local-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("kv.db");
    let _ = std::fs::remove_file(&file);
    let counter = format!("{}?wasi=keyvalue", guests.join("counter.wasm").display());
    let hello = guests.join("hello.wasm").display().to_string();
    let file = file.to_str().unwrap();
    let routes = [
        format!("a.local={counter}"),
        format!("b.local={counter}"),
        format!("hello.local={hello}"),
    ];
    let several: Vec<&str> = ["--keyvalue-file", file]
        .into_iter()
        .chain(routes.iter().flat_map(|route| ["--wasm", route.as_str()]))
        .collect();
    let sole = [
        "--keyvalue-file",
        file,
        "--max-connections",
        "1",
        "--wasm",
        &routes[0],
    ];

    // both open the new file at once: the second switches it to WAL while
    // the first does
    let mut wicket = Wicket::start(&several);
    let other = Wicket::start(&sole);
    let (address, other) = (wicket.address(), other.address());

    assert_eq!(
        get(&address, "a.local", "/").await,
        (200, "hits 1\n".into())
    );
    assert_eq!(
        get(&address, "b.local:8080", "/").await,
        (200, "hits 1\n".into()),
        "its own namespace, the port left out"
    );
    let (status, body) = get(&address, "hello.local", "/").await;
    assert_eq!(status, 200);
    assert!(body.starts_with("hello from wasm"), "{body}");
    let (status, body) = get(&address, "unknown.local", "/").await;
    assert_eq!(
        (status, body.as_str()),
        (404, "no wasm route for host 'unknown.local'")
    );
    assert_eq!(get_h2(&address, "a.local", "/").await, "hits 2\n");
    assert_eq!(
        get(&other, "anything", "/").await,
        (200, "hits 3\n".into()),
        "the sole route, sharing a.local's namespace"
    );

    count_at_once(&address, &other).await;
    assert_eq!(get(&address, "a.local", "/").await.1, "hits 24\n");
    assert_eq!(get(&other, "a.local", "/kv/cas-hits").await.1, "20");

    // its one connection taken, the next one waits in the backlog
    let held = tokio::net::TcpStream::connect(&other).await.unwrap();
    let waiting = tokio::spawn(async move { get(&other, "a.local", "/kv/cas-hits").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiting.is_finished(),
        "answered beyond --max-connections 1"
    );
    drop(held);
    assert_eq!(
        waiting.await.unwrap().1,
        "20",
        "answered once the first closed"
    );

    drop(wicket);
    wicket = Wicket::start(&several);
    let address = wicket.address();
    assert_eq!(
        get(&address, "a.local", "/").await.1,
        "hits 25\n",
        "kept across the restart"
    );
    let status = Command::new("kill")
        .args(["-TERM", &wicket.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(15);
    let exited = loop {
        if let Some(exited) = wicket.child.try_wait().unwrap() {
            break exited;
        }
        assert!(Instant::now() < deadline, "still running after SIGTERM");
        std::thread::sleep(Duration::from_millis(20));
    };
    // its handler, not the default action: the init of a container has none
    assert!(exited.success(), "no drain: {exited:?}");
    drop(wicket);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two wickets sharing a redis store (over TLS, the server's certificate
/// issued by the store's ca-cert) count right at once, by increment and by
/// cas.
#[tokio::test(flavor = "multi_thread")]
async fn two_wickets_count_on_a_shared_redis_store() {
    let guests = guests();
    let dir = std::env::temp_dir().join(format!("sluice-redis-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    std::fs::write(dir.join("ca.pem"), ca.self_signed(&ca_key).unwrap().pem()).unwrap();
    let issuer = rcgen::Issuer::new(ca, ca_key);
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])
        .unwrap()
        .signed_by(&key, &issuer)
        .unwrap();
    let (cert_file, key_file) = (dir.join("redis.pem"), dir.join("redis.key"));
    std::fs::write(&cert_file, cert.pem()).unwrap();
    std::fs::write(&key_file, key.serialize_pem()).unwrap();
    let server = redis_server::RedisServer::tls(&cert_file, &key_file);
    let config = dir.join("wicket.yaml");
    std::fs::write(
        &config,
        format!(
            "keyvalue:
  stores:
    shared: {{type: redis, url: '{}/0', ca-cert: ca.pem, prefix: 'counter:'}}
routes:
  - host: a.local
    component: {}
    sandbox: {{wasi: [keyvalue]}}
",
            server.url,
            guests.join("counter.wasm").display()
        ),
    )
    .unwrap();
    let args = ["--config", config.to_str().unwrap()];
    let wickets = [Wicket::start(&args), Wicket::start(&args)];
    let (a, b) = (wickets[0].address(), wickets[1].address());

    count_at_once(&a, &b).await;
    assert_eq!(get(&b, "a.local", "/").await.1, "hits 21\n");
    assert_eq!(get(&a, "a.local", "/kv/cas-hits").await.1, "20");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The console deploys a component uploaded to it as a route at once: served
/// on the listener, listed -- a long poll answering the change -- and kept
/// across a restart, until removed. It asks for its token, and refuses an
/// edit from another origin and a body that is no wasm.
#[tokio::test(flavor = "multi_thread")]
async fn the_console_deploys_an_uploaded_component_as_a_route() {
    let hello = std::fs::read(guests().join("hello.wasm")).unwrap();
    let dir = std::env::temp_dir().join(format!("sluice-console-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let kept = dir.join("deployed");
    let args = [
        "--console",
        "127.0.0.1:0",
        "--console-token",
        "s3cret",
        "--console-dir",
        kept.to_str().unwrap(),
    ];
    let mut wicket = Wicket::start(&args);
    let (address, at) = (wicket.address(), wicket.console());
    // admin:s3cret
    let basic = "authorization: Basic YWRtaW46czNjcmV0";
    let bearer = "authorization: Bearer s3cret";

    let (status, head, _) = console(&at, "GET /api/routes", &[], b"").await;
    assert_eq!(status, 401);
    assert!(head.contains("www-authenticate: basic realm="), "{head}");
    let (status, head, page) = console(&at, "GET /", &[basic], b"").await;
    assert_eq!(status, 200);
    assert!(head.contains("content-type: text/html"), "{head}");
    assert!(
        head.contains("content-security-policy: default-src 'none'"),
        "{head}"
    );
    assert!(page.contains("/console.js"), "{page}");
    let (status, _, listing) = console(&at, "GET /api/routes", &[bearer], b"").await;
    assert_eq!(status, 200);
    let listing: serde_json::Value = serde_json::from_str(&listing).unwrap();
    assert_eq!(listing["routes"], serde_json::json!([]));
    let revision = listing["revision"].as_u64().unwrap();
    let polled = {
        let at = at.clone();
        tokio::spawn(async move {
            let request = format!("GET /api/routes?after={revision}");
            console(&at, &request, &[bearer], b"").await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!polled.is_finished(), "held while nothing changed");

    let deploy = "PUT /api/routes/Hello.localhost?name=hello.wasm\
                  &wasi=cli%2Cinherit-network&budget-ms=5000";
    let foreign = [bearer, "origin: http://rebound.example"];
    let (status, _, body) = console(&at, deploy, &foreign, &hello).await;
    assert_eq!(
        (status, body.as_str()),
        (403, r#"{"error":"an edit from another origin is refused"}"#)
    );
    let (status, _, body) = console(&at, deploy, &[bearer], b"<html>").await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("not a wasm binary"), "{body}");
    let same_origin = format!("origin: http://{at}");
    let (status, _, body) = console(&at, deploy, &[bearer, &same_origin], &hello).await;
    assert_eq!(
        (status, body.as_str()),
        (200, r#"{"generation":2,"message":"added hello.localhost"}"#)
    );
    let (status, _, listing) = tokio::time::timeout(HANG, polled).await.unwrap().unwrap();
    assert_eq!(status, 200);
    let listing: serde_json::Value = serde_json::from_str(&listing).unwrap();
    assert_ne!(
        listing["revision"].as_u64(),
        Some(revision),
        "answered the change"
    );
    let route = &listing["routes"][0];
    assert_eq!(route["host"], "hello.localhost");
    assert_eq!(route["origin"], "console");
    assert_eq!(route["component"]["name"], "hello.wasm");
    assert_eq!(route["component"]["bytes"], hello.len());
    assert_eq!(
        route["sandbox"]["wasi"],
        serde_json::json!(["cli", "inherit-network"])
    );
    assert_eq!(route["sandbox"]["budget_ms"], 5000);
    let (status, body) = get(&address, "hello.localhost", "/").await;
    assert_eq!(status, 200);
    assert!(body.starts_with("hello from wasm"), "{body}");

    // a move takes the host alone: a sandbox left out would read as the defaults
    let mixed = "PATCH /api/routes/hello.localhost?host=greet.localhost&pool=1";
    let (status, _, body) = console(&at, mixed, &[bearer], b"").await;
    assert_eq!(status, 400, "{body}");
    let moved = "PATCH /api/routes/hello.localhost?host=greet.localhost";
    let (status, _, body) = console(&at, moved, &[bearer], b"").await;
    assert_eq!(
        (status, body.as_str()),
        (
            200,
            r#"{"generation":3,"message":"added greet.localhost; removed hello.localhost"}"#
        )
    );

    drop(wicket);
    wicket = Wicket::start(&args);
    let (address, at) = (wicket.address(), wicket.console());
    let (_, _, listing) = console(&at, "GET /api/routes", &[bearer], b"").await;
    let listing: serde_json::Value = serde_json::from_str(&listing).unwrap();
    let route = &listing["routes"][0];
    assert_eq!(
        (
            route["host"].as_str(),
            route["sandbox"]["budget_ms"].as_u64()
        ),
        (Some("greet.localhost"), Some(5000)),
        "moved as it was, and kept across the restart"
    );
    let (status, body) = get(&address, "greet.localhost", "/").await;
    assert_eq!(status, 200, "{body}");
    let removal = "DELETE /api/routes/greet.localhost";
    let (status, _, body) = console(&at, removal, &[basic], b"").await;
    assert_eq!(
        (status, body.as_str()),
        (
            200,
            r#"{"generation":2,"message":"removed greet.localhost"}"#
        )
    );
    assert_eq!(get(&address, "greet.localhost", "/").await.0, 404);
    let (status, _, _) = console(&at, removal, &[basic], b"").await;
    assert_eq!(status, 404, "no longer the console's");
    drop(wicket);
    let _ = std::fs::remove_dir_all(&dir);
}
