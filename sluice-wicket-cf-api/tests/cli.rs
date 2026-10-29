//! The OSS cf CLI (`SLUICE_CF_CLI`, else `cf8` on the PATH; the Tanzu
//! distribution refuses any other platform) against the cf-api and a wicket
//! in local mode: login, push, map-route, scale, a rolling push with
//! requests all along, stop and start, a restart of the cf-api, a component
//! the wicket refuses, delete.

use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Hang guard of every wait; the first covers a cold build of the wicket.
const HANG: Duration = Duration::from_secs(120);

const TOKEN: &str = "s3cret";

fn wicket_crate() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../sluice-wicket-wasi")
}

/// Runs cargo in `dir` (through the rustup proxy, the toolchain the
/// directory pins).
fn cargo(dir: &Path, args: &[&str]) {
    let output = Command::new("cargo")
        .args(args)
        .env_remove("RUSTUP_TOOLCHAIN")
        .env_remove("CARGO")
        .current_dir(dir)
        .output()
        .expect("run cargo (rustup)");
    assert!(
        output.status.success(),
        "cargo {} in {} failed: {}",
        args.join(" "),
        dir.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The wicket: `SLUICE_WICKET_WASI`, else built from the crate next door.
fn wicket_binary() -> PathBuf {
    if let Ok(given) = std::env::var("SLUICE_WICKET_WASI") {
        return PathBuf::from(given);
    }
    let dir = wicket_crate();
    cargo(&dir, &["build", "--locked"]);
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dir.join("target"))
        .join("debug/sluice-wicket-wasi")
}

/// The directory of the hello and counter guests: `SLUICE_WICKET_GUESTS`,
/// else built as the wicket's tests build them.
fn guests() -> PathBuf {
    if let Ok(given) = std::env::var("SLUICE_WICKET_GUESTS") {
        return PathBuf::from(given);
    }
    let examples = wicket_crate().join("examples");
    cargo(
        &examples,
        &["build", "--release", "-p", "hello", "-p", "counter"],
    );
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| examples.join("target"))
        .join("wasm32-wasip3/release")
}

fn cf_cli() -> PathBuf {
    PathBuf::from(std::env::var("SLUICE_CF_CLI").unwrap_or_else(|_| "cf8".into()))
}

/// A child process whose log is read line by line, killed on drop.
struct Process {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Process {
    fn spawn(program: &Path, args: &[&str]) -> Process {
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("run {}: {e}", program.display()));
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (lines, received) = mpsc::channel();
        std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                if lines.send(line).is_err() {
                    return;
                }
            }
        });
        Process {
            child,
            lines: received,
        }
    }

    /// The word after `marker` in the next line of the log holding it.
    fn logged(&self, marker: &str) -> String {
        let deadline = Instant::now() + HANG;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(left)
                .unwrap_or_else(|e| panic!("never logged '{marker}': {e}"));
            if let Some((_, rest)) = line.split_once(marker) {
                return rest.split(' ').next().unwrap().to_string();
            }
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

/// The cf CLI with its own config.
struct Cf {
    cli: PathBuf,
    home: PathBuf,
}

impl Cf {
    /// Runs the command: whether it succeeded, and its output.
    fn run(&self, args: &[&str]) -> (bool, String) {
        let output = Command::new(&self.cli)
            .args(args)
            .env("CF_HOME", &self.home)
            .env("CF_COLOR", "false")
            .output()
            .unwrap_or_else(|e| {
                panic!(
                    "run the cf CLI {}: {e} (SLUICE_CF_CLI names the OSS CLI v8)",
                    self.cli.display()
                )
            });
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        println!("$ cf {}\n{text}", args.join(" "));
        (output.status.success(), text)
    }

    fn ok(&self, args: &[&str]) -> String {
        let (ok, text) = self.run(args);
        assert!(ok, "cf {} failed:\n{text}", args.join(" "));
        text
    }

    fn fails(&self, args: &[&str]) -> String {
        let (ok, text) = self.run(args);
        assert!(!ok, "cf {} succeeded:\n{text}", args.join(" "));
        text
    }
}

/// An http/1.0 GET of `path` at `address` with `host` (no chunking: the
/// body is read to the close): the status and the body.
fn get(address: &str, host: &str, path: &str, authorization: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(address).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let auth = authorization.map_or(String::new(), |token| {
        format!("Authorization: Bearer {token}\r\n")
    });
    stream
        .write_all(
            format!("GET {path} HTTP/1.0\r\nHost: {host}\r\n{auth}Connection: close\r\n\r\n")
                .as_bytes(),
        )
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    let (head, body) = response.split_once("\r\n\r\n").expect("a head");
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status");
    (status, body.to_string())
}

/// The wicket's apps, through its console.
fn listing(console: &str) -> Value {
    let (status, body) = get(console, console, "/api/apps", Some(TOKEN));
    assert_eq!(status, 200, "{body}");
    serde_json::from_str(&body).unwrap()
}

fn sha256(path: &Path) -> String {
    Sha256::digest(std::fs::read(path).unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// `hello.wasm` with a custom section appended: another component (its
/// digest) that behaves the same.
fn variant(hello: &[u8]) -> Vec<u8> {
    let name = b"sluice-variant";
    let mut section = vec![name.len() as u8];
    section.extend_from_slice(name);
    section.extend_from_slice(b"v2");
    let mut component = hello.to_vec();
    component.push(0);
    component.push(section.len() as u8);
    component.extend(section);
    component
}

fn start_api(listen: &str, console: &str, data: &Path) -> (Process, String) {
    let api = Process::spawn(
        Path::new(env!("CARGO_BIN_EXE_sluice-wicket-cf-api")),
        &[
            "--listen",
            listen,
            "--wicket",
            &format!("http://{console}"),
            "--wicket-token",
            TOKEN,
            "--data-dir",
            data.to_str().unwrap(),
        ],
    );
    let url = api.logged("listening on ");
    (api, url)
}

#[test]
fn the_cf_cli_deploys_to_the_wicket() {
    let guests = guests();
    let wicket_binary = wicket_binary();
    let dir = std::env::temp_dir().join(format!("sluice-cf-api-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["app", "app2", "counter", "cfhome"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    let hello = std::fs::read(guests.join("hello.wasm")).unwrap();
    std::fs::write(dir.join("app/hello.wasm"), &hello).unwrap();
    std::fs::write(dir.join("app2/hello.wasm"), variant(&hello)).unwrap();
    std::fs::copy(
        guests.join("counter.wasm"),
        dir.join("counter/counter.wasm"),
    )
    .unwrap();

    let wicket = Process::spawn(
        &wicket_binary,
        &[
            "--listen",
            "127.0.0.1:0",
            "--console",
            "127.0.0.1:0",
            "--console-token",
            TOKEN,
            "--console-dir",
            dir.join("deployed").to_str().unwrap(),
            "--keyvalue-file",
            dir.join("kv.db").to_str().unwrap(),
            "--watch-interval",
            "0",
        ],
    );
    let listen = wicket.logged("listening on ");
    let console = wicket.logged("console on http://");
    // a port of its own, kept across the restart (the CLI targets it)
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .unwrap()
        .port();
    let api_listen = format!("127.0.0.1:{port}");
    let (mut api, url) = start_api(&api_listen, &console, &dir.join("cf"));
    let cf = Cf {
        cli: cf_cli(),
        home: dir.join("cfhome"),
    };

    cf.ok(&["api", &url]);
    cf.ok(&["login", "-u", "me", "-p", "x"]);

    // push from a directory: served on the wicket's listener
    cf.ok(&["push", "hello", "-p", dir.join("app").to_str().unwrap()]);
    let (status, body) = get(&listen, "hello.localhost", "/", None);
    assert_eq!(status, 200, "{body}");
    assert!(body.starts_with("hello from wasm"), "{body}");
    assert!(
        cf.ok(&["logs", "hello", "--recent"])
            .contains("Staged hello.wasm")
    );

    // routes and instances apply at once
    cf.ok(&["map-route", "hello", "localhost", "--hostname", "hi"]);
    assert_eq!(get(&listen, "hi.localhost", "/", None).0, 200);
    let apps = listing(&console);
    assert_eq!(apps["apps"].as_array().unwrap().len(), 1);
    assert_eq!(
        apps["apps"][0]["hosts"],
        serde_json::json!(["hello.localhost", "hi.localhost"])
    );
    cf.ok(&["scale", "hello", "-i", "2"]);
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["pool"], 2);

    // a rolling push of another component, with no failed request during it
    let stop = Arc::new(AtomicBool::new(false));
    let (served, failed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let hammer = {
        let (stop, served, failed, listen) =
            (stop.clone(), served.clone(), failed.clone(), listen.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match std::panic::catch_unwind(|| get(&listen, "hello.localhost", "/", None)) {
                    Ok((200, body)) if body.starts_with("hello from wasm") => {
                        served.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
    };
    cf.ok(&[
        "push",
        "hello",
        "-p",
        dir.join("app2").to_str().unwrap(),
        "--strategy",
        "rolling",
    ]);
    stop.store(true, Ordering::Relaxed);
    hammer.join().unwrap();
    assert_eq!(failed.load(Ordering::Relaxed), 0);
    assert!(served.load(Ordering::Relaxed) > 0);
    let apps = listing(&console);
    assert_eq!(
        apps["apps"][0]["component"]["sha256"],
        sha256(&dir.join("app2/hello.wasm"))
    );
    assert_eq!(
        apps["apps"][0]["sandbox"]["pool"], 2,
        "kept across the push"
    );

    // stopped: no app in the wicket; started: served again
    cf.ok(&["stop", "hello"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 404);
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    cf.ok(&["start", "hello"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 200);

    // the cf-api restarted: its state kept (the token too), the wicket's
    // generation unchanged
    let generation = listing(&console)["generation"].as_u64().unwrap();
    api.kill();
    let (api, _) = start_api(&api_listen, &console, &dir.join("cf"));
    api.logged("Found in the wicket as wanted");
    assert!(cf.ok(&["app", "hello"]).contains("hi.localhost"));
    assert_eq!(listing(&console)["generation"], generation);

    // a component the wicket refuses: its reason reaches the user
    let output = cf.fails(&[
        "push",
        "counter",
        "-p",
        dir.join("counter").to_str().unwrap(),
    ]);
    assert!(output.contains("needs wasi=keyvalue"), "{output}");
    assert!(output.contains("crashed"), "{output}");

    // gone from the wicket
    cf.ok(&["delete", "hello", "-r", "-f"]);
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 404);
    assert!(!cf.ok(&["apps"]).contains("hello"));

    drop(api);
    drop(wicket);
    let _ = std::fs::remove_dir_all(&dir);
}
