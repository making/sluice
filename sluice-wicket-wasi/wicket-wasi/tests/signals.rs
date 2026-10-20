//! The signals of the process: SIGHUP reloads the routes; SIGTERM starts the
//! drain and a second one ends it at once; with `--drain-grace 0` SIGTERM ends
//! the process as the default action does.
#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Hang guard of every wait; the first one covers the component's load.
const HANG: Duration = Duration::from_secs(60);

/// The hello guest, built as the unit tests build it (the fetch guest beside
/// it).
fn hello_wasm() -> std::path::PathBuf {
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("examples");
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
/// (an empty `AdvertiseAck`, field 7 of `Frame`) and keeping the stream open.
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

/// The wicket on `server` serving `component` for `hello.local`, polling
/// nothing (SIGHUP alone reloads), its stdout lines on the receiver.
fn wicket(
    server: &str,
    drain_grace: &str,
    component: &std::path::Path,
) -> (Child, mpsc::Receiver<String>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-wasi"))
        .args(["--server", server, "--drain-grace", drain_grace])
        .args(["--watch-interval", "0", "--wasm"])
        .arg(format!("hello.local={}", component.display()))
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
    (child, received)
}

/// Waits for a line containing `text`.
fn await_line(lines: &mpsc::Receiver<String>, text: &str) {
    let deadline = Instant::now() + HANG;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(left)
            .unwrap_or_else(|e| panic!("no line with '{text}': {e}"));
        if line.contains(text) {
            return;
        }
    }
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
    let _ = child.kill();
    panic!("still running after {within:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_drains_and_a_second_one_ends_the_drain() {
    let server = acking_server().await;

    // a copy of the guest, replaced below
    let hello = hello_wasm();
    let component =
        std::env::temp_dir().join(format!("sluice-signals-{}.wasm", std::process::id()));
    std::fs::copy(&hello, &component).unwrap();

    // the server never acknowledges the withdrawal: only the second signal
    // ends the drain before its 60s grace
    let (mut child, lines) = wicket(&server, "60", &component);
    await_line(&lines, "advertise acked by");
    // SIGHUP reloads the routes, and ends nothing
    signal(&child, "-HUP");
    await_line(&lines, "routes reload (SIGHUP): no change");
    std::fs::copy(hello.with_file_name("fetch.wasm"), &component).unwrap();
    signal(&child, "-HUP");
    await_line(
        &lines,
        "routes reloaded (SIGHUP), generation 2: updated hello.local",
    );
    sigterm(&child);
    await_line(&lines, "withdrawing the routes");
    sigterm(&child);
    let status = exit_within(&mut child, Duration::from_secs(15));
    assert!(status.success(), "{status:?}");

    // no drain: the default action ends the process
    let (mut child, lines) = wicket(&server, "0", &hello);
    await_line(&lines, "advertise acked by");
    sigterm(&child);
    let status = exit_within(&mut child, Duration::from_secs(15));
    assert_eq!(status.signal(), Some(15), "{status:?}");
    let _ = std::fs::remove_file(&component);
}
