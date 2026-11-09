//! The signals of the process: SIGHUP reloads the routes; SIGTERM starts the
//! evacuation of the console's apps (`--evacuation-grace`), a second one the
//! drain and a third one ends it at once; with `--drain-grace 0` SIGTERM ends
//! the process at once, through its handler (exit 0, not the default action:
//! the init of a container has none). The readiness probe (`--health`) says
//! ready once the server took the routes, not from the drain on (an evacuation
//! is none).
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use sha2::Digest as _;

/// Hang guard of every wait; the first one covers the component's load.
const HANG: Duration = Duration::from_secs(60);

/// The console's token.
const TOKEN: &str = "s3cret";

/// The hello guest, built as the unit tests build it (the fetch guest beside
/// it).
fn hello_wasm() -> std::path::PathBuf {
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let output = Command::new("cargo")
        .args(["build", "--release", "-p", "hello", "-p", "fetch"])
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
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| examples.join("target"))
        .join("wasm32-wasip3/release/hello.wasm")
}

/// A tunnel server acknowledging every advertise on every Connect stream
/// (an empty `AdvertiseAck`, field 7 of `Frame`) and keeping the stream open;
/// without `ListNodes` (UNIMPLEMENTED), so the wicket streams to it.
async fn acking_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("grpc://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                let mut held = Vec::new();
                while let Some(Ok((request, mut respond))) = connection.accept().await {
                    if request.uri().path() != "/sluice.v1.Tunnel/Connect" {
                        let unimplemented = http::Response::builder()
                            .header("content-type", "application/grpc")
                            .header("grpc-status", "12")
                            .body(())
                            .unwrap();
                        let _ = respond.send_response(unimplemented, true);
                        continue;
                    }
                    let response = http::Response::builder()
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap();
                    let mut send = respond.send_response(response, false).unwrap();
                    let ack: &[u8] = &[0, 0, 0, 0, 2, 7 << 3 | 2, 0];
                    send.send_data(bytes::Bytes::from_static(ack), false)
                        .unwrap();
                    held.push((request.into_body(), send));
                }
            });
        }
    });
    url
}

/// A wicket process, killed on drop: a failed wait leaves none running.
struct Wicket(Child);

impl std::ops::Deref for Wicket {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for Wicket {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Wicket {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The wicket on `server` serving `component` for `hello.local`, polling
/// nothing (SIGHUP alone reloads), its readiness probe on a port of its
/// choosing, `more` besides, its stdout lines on the receiver.
fn wicket(
    server: &str,
    drain_grace: &str,
    component: &std::path::Path,
    more: &[&str],
) -> (Wicket, mpsc::Receiver<String>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-runtime-wasi"))
        .args(["--server", server, "--drain-grace", drain_grace])
        .args(["--watch-interval", "0", "--health", "127.0.0.1:0"])
        .args(more)
        .arg("--wasm")
        .arg(format!("hello.local={}", component.display()))
        .stdout(Stdio::piped())
        .spawn()
        .expect("run sluice-wicket-runtime-wasi");
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let (lines, received) = mpsc::channel();
    std::thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            if lines.send(line).is_err() {
                return;
            }
        }
    });
    (Wicket(child), received)
}

/// Waits for a line containing `text`; the line.
fn await_line(lines: &mpsc::Receiver<String>, text: &str) -> String {
    let deadline = Instant::now() + HANG;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(left)
            .unwrap_or_else(|e| panic!("no line with '{text}': {e}"));
        if line.contains(text) {
            return line;
        }
    }
}

/// What `--probe` makes of the readiness probe its line (`url`) names: whether ready, and what
/// it printed.
fn probe(url: &str) -> (bool, String) {
    let address = url
        .strip_prefix("readiness probe on http://")
        .and_then(|rest| rest.strip_suffix("/ready"))
        .unwrap_or_else(|| panic!("a probe's line: {url}"));
    let output = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-runtime-wasi"))
        .args(["--probe", address])
        .output()
        .expect("run sluice-wicket-runtime-wasi --probe");
    let said = match output.status.success() {
        true => output.stdout,
        false => output.stderr,
    };
    (
        output.status.success(),
        String::from_utf8_lossy(&said).into_owned(),
    )
}

/// A request of the console at `address`, with its token: the status and the body.
fn console(address: &str, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nhost: {address}\r\nauthorization: Bearer {TOKEN}\r\n\
         connection: close\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    let (head, body) = answer.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, body.to_string())
}

fn sigterm(child: &Child) {
    signal(child, "-TERM");
}

fn signal(child: &Child, signal: &str) {
    let status = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

/// Waits for the exit, at most `within`.
fn exit_within(child: &mut Child, within: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("still running after {within:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_evacuates_then_drains_each_ended_by_the_next() {
    let server = acking_server().await;

    // a copy of the guest, replaced below
    let hello = hello_wasm();
    let component =
        std::env::temp_dir().join(format!("sluice-signals-{}.wasm", std::process::id()));
    std::fs::copy(&hello, &component).unwrap();

    // the server never acknowledges the withdrawal: only the third signal
    // ends the drain before its 60s grace
    let console_dir =
        std::env::temp_dir().join(format!("sluice-signals-console-{}", std::process::id()));
    let evacuating = [
        "--evacuation-grace",
        "60",
        "--console",
        "127.0.0.1:0",
        "--console-token",
        TOKEN,
        "--console-dir",
        console_dir.to_str().unwrap(),
    ];
    let (mut child, lines) = wicket(&server, "60", &component, &evacuating);
    let probed = await_line(&lines, "readiness probe on ");
    let at = await_line(&lines, "console on http://");
    let at = at["console on http://".len()..].split(' ').next().unwrap();
    await_line(&lines, "advertise acked by");
    assert_eq!(probe(&probed), (true, "ready\n".to_string()));
    // SIGHUP reloads the routes, and ends nothing
    signal(&child, "-HUP");
    await_line(&lines, "routes reload (SIGHUP): no change");
    std::fs::copy(hello.with_file_name("fetch.wasm"), &component).unwrap();
    signal(&child, "-HUP");
    await_line(
        &lines,
        "routes reloaded (SIGHUP), generation 2: updated hello.local",
    );
    // an app of the console's API, which the wicket told to stop serves on for until its
    // controller moved it or the second signal
    let app = std::fs::read(&hello).unwrap();
    let sha256: String = (sha2::Sha256::digest(&app).iter())
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let uploaded = console(at, "PUT", &format!("/api/components/{sha256}"), &app);
    assert_eq!(uploaded.0, 200, "{uploaded:?}");
    let body = format!(
        r#"{{"owner": "cf", "hosts": ["app.local"], "component": {{"sha256": "{sha256}", "name": "hello.wasm"}}}}"#
    );
    let put = console(at, "PUT", "/api/apps/app", body.as_bytes());
    assert_eq!(put.0, 200, "{put:?}");
    sigterm(&child);
    await_line(
        &lines,
        "leaving: serving on while the controllers move 1 app(s) elsewhere, 60s at most",
    );
    // not draining (this server acknowledges a stream's first routes alone)
    let (_, said) = probe(&probed);
    assert!(!said.contains("draining"), "{said}");
    let (_, apps) = console(at, "GET", "/api/apps", b"");
    assert!(apps.contains(r#""leaving":{"since":"#), "{apps}");
    sigterm(&child);
    await_line(&lines, "withdrawing the routes");
    assert_eq!(probe(&probed), (false, "not ready: draining\n".to_string()));
    sigterm(&child);
    let status = exit_within(&mut child, Duration::from_secs(15));
    assert!(status.success(), "{status:?}");
    let _ = std::fs::remove_dir_all(&console_dir);

    // no drain: the handler ends the process at once
    let (mut child, lines) = wicket(&server, "0", &hello, &[]);
    await_line(&lines, "advertise acked by");
    sigterm(&child);
    await_line(&lines, "SIGTERM: ending at once (--drain-grace 0)");
    let status = exit_within(&mut child, Duration::from_secs(15));
    assert!(status.success(), "{status:?}");
    let _ = std::fs::remove_file(&component);
}
