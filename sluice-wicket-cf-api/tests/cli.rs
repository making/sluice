//! The OSS cf CLI (`SLUICE_CF_CLI`, else `cf8` on the PATH; the Tanzu
//! distribution refuses any other platform) against the cf-api and a wicket
//! in local mode, both on a flume: login, push, the app's logs, map-route,
//! scale keeping the warm instance, a rolling push with requests all along,
//! stop and start, a scale to 0, the limit of a scale and of its `concurrency`, an
//! instance of a request's own kept by `keep-ms`, the
//! user's environment variables and VCAP_*, a private component in an OCI
//! registry (a stub) pushed by tag -- its credentials, a restart keeping
//! the digest staged when the tag moves, a restage taking the new one --, a
//! restart of the cf-api with an OpenID Provider (a mock) and TLS of its
//! own -- `cf login --sso` through the passcode page, a refresh, the
//! clients configured, who is refused, who may edit the security groups --, a second
//! wicket -- the one wicket's indices placed again, an app's instances spread over both,
//! one stopped and back, a rolling push a wicket at a time, one refused, taken up by a restart
//! of the cf-api and canceled, a canary continued, a scale, one told to stop moving its apps to
//! the other first and back taking an app's spread again, one killed and gone, its apps started on
//! the other, back holding them until removed --, a component the wicket refuses
//! until a service binding grants what it needs, the services' lifecycle -- a store of each
//! wicket's host keeping its apps on one wicket, one they share (Redis) counting once over
//! both --, a service broker
//! (a stub) -- who registers it, who sees its plans, its operations at once and polled
//! across a restart of the cf-api, failed, its bindings' credentials, a manifest's and an
//! app's delete waiting for it, across a restart of the cf-api too, its catalog fetched again --,
//! delete. And against a Firecracker wicket, where `/dev/kvm` is usable: a
//! private image made into a rootfs by the wicket, its process's user, files and
//! environment seen from the VM, its connections through the security groups, a scale keeping
//! the VMs running, each
//! mode and lifetime by annotation, a VM per connection warned of and finding no room, a scale
//! to 0, a rootfs of a zip, source staged by a builder's buildpacks (a sample one, with the
//! lifecycle of `SLUICE_CNB_LIFECYCLE`, else `~/.local/cnb/lifecycle`) in a VM of the wicket,
//! and by one given by its image, its droplet kept by the cf-api, the wicket's
//! directory lost and the rootfs of the image and of that droplet made again, of the digest
//! the image's had, a second wicket making its own of both; then the
//! cf-api itself an app of that wicket, in a VM with a disk, the flume and a provider
//! through its egress: refused with no word on who logs in, its apps and logins kept
//! across a restart of the VM, a wicket killed right after a push and an upgrade.

mod broker;
#[path = "../../sluice-wicket-runtime-wasi/tests/support/redis_server.rs"]
mod redis_server;
mod registry;

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use broker::Broker;
use registry::Registry;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sluice_wicket_oidc::mock::MockProvider;

/// Hang guard of every wait; the first covers a cold build of the wicket.
const HANG: Duration = Duration::from_secs(120);

const TOKEN: &str = "s3cret";

const FLUME_TOKEN: &str = "f1ume";

fn wicket_crate() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../sluice-wicket-runtime-wasi")
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

/// The wicket: `SLUICE_WICKET_RUNTIME_WASI`, else built from the crate next door.
fn wicket_binary() -> PathBuf {
    if let Ok(given) = std::env::var("SLUICE_WICKET_RUNTIME_WASI") {
        return PathBuf::from(given);
    }
    let dir = wicket_crate();
    cargo(&dir, &["build", "--locked"]);
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dir.join("target"))
        .join("debug/sluice-wicket-runtime-wasi")
}

/// The flume: `SLUICE_FLUME`, else built from the crate next door.
fn flume_binary() -> PathBuf {
    if let Ok(given) = std::env::var("SLUICE_FLUME") {
        return PathBuf::from(given);
    }
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../sluice-flume");
    cargo(&dir, &["build", "--locked"]);
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dir.join("target"))
        .join("debug/sluice-flume")
}

/// The directory of the hello, counter and relay guests:
/// `SLUICE_WICKET_GUESTS`, else built as the wicket's tests build them.
fn guests() -> PathBuf {
    if let Ok(given) = std::env::var("SLUICE_WICKET_GUESTS") {
        return PathBuf::from(given);
    }
    let examples = wicket_crate().join("examples");
    cargo(
        &examples,
        &[
            "build",
            "--release",
            "-p",
            "hello",
            "-p",
            "counter",
            "-p",
            "relay",
        ],
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
        let mut command = Command::new(program);
        command.args(args);
        Process::start(command)
    }

    fn start(mut command: Command) -> Process {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("run {:?}: {e}", command.get_program()));
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
        let line = self.line(marker);
        let (_, rest) = line.split_once(marker).expect("holds it");
        rest.split(' ').next().unwrap().to_string()
    }

    /// The next line of the log holding `marker`.
    fn line(&self, marker: &str) -> String {
        let deadline = Instant::now() + HANG;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(left)
                .unwrap_or_else(|e| panic!("never logged '{marker}': {e}"));
            if line.contains(marker) {
                return line;
            }
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// SIGTERM: a wicket evacuates, then ends.
    fn terminate(&self) {
        let pid = self.child.id().to_string();
        let killed = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        assert!(killed.success());
    }

    /// Waits for its exit, a success, [`HANG`] at most.
    fn exited(&mut self) {
        let deadline = Instant::now() + HANG;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "{status:?}");
                return;
            }
            assert!(Instant::now() < deadline, "still running");
            std::thread::sleep(Duration::from_millis(50));
        }
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
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(&self.cli);
        command
            .args(args)
            .env("CF_HOME", &self.home)
            .env("CF_COLOR", "false");
        command
    }

    /// Runs the command: whether it succeeded, and its output.
    fn run(&self, args: &[&str]) -> (bool, String) {
        self.run_with_stdin(args, "")
    }

    /// Runs the command with `input` on its stdin (what a prompt reads).
    fn run_with_stdin(&self, args: &[&str], input: &str) -> (bool, String) {
        self.output(self.command(args), input)
    }

    /// Runs `command` (of [`Cf::command`]) with `input` on its stdin:
    /// whether it succeeded, and its output.
    fn output(&self, mut command: Command, input: &str) -> (bool, String) {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "run the cf CLI {}: {e} (SLUICE_CF_CLI names the OSS CLI v8)",
                    self.cli.display()
                )
            });
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let args: Vec<String> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        println!("$ cf {}\n{text}", args.join(" "));
        (output.status.success(), text)
    }

    /// The tokens the CLI holds: `config.json`'s AccessToken (`bearer ...`)
    /// and RefreshToken.
    fn tokens(&self) -> (String, String) {
        let config: Value =
            serde_json::from_slice(&std::fs::read(self.home.join(".cf/config.json")).unwrap())
                .unwrap();
        (
            config["AccessToken"].as_str().unwrap().to_string(),
            config["RefreshToken"].as_str().unwrap().to_string(),
        )
    }

    fn ok(&self, args: &[&str]) -> String {
        let (ok, text) = self.run(args);
        assert!(ok, "cf {} failed:\n{text}", args.join(" "));
        text
    }

    fn ok_with_stdin(&self, args: &[&str], input: &str) -> String {
        let (ok, text) = self.run_with_stdin(args, input);
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
    fetch(address, host, path, authorization).expect("a head")
}

/// [`get`], None on a connection cut or ended with no answer.
fn fetch(
    address: &str,
    host: &str,
    path: &str,
    authorization: Option<&str>,
) -> Option<(u16, String)> {
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
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let (head, body) = response.split_once("\r\n\r\n")?;
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status");
    Some((status, body.to_string()))
}

/// A request at `path` whose answer has started, held open until the stream is dropped.
fn held(address: &str, host: &str, path: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).expect("connect");
    stream.set_read_timeout(Some(HANG)).unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes())
        .unwrap();
    let mut started = [0u8; 12];
    stream.read_exact(&mut started).unwrap();
    let started = String::from_utf8_lossy(&started);
    assert_eq!(started, "HTTP/1.1 200", "GET {path} of {host}");
    stream
}

/// [`get`] on a thread of its own: the status, once answered.
fn answered(address: &str, host: &str, path: &str) -> mpsc::Receiver<u16> {
    let (status, answer) = mpsc::channel();
    let (address, host, path) = (address.to_string(), host.to_string(), path.to_string());
    std::thread::spawn(move || {
        let _ = status.send(get(&address, &host, &path, None).0);
    });
    answer
}

/// The wicket's apps, through its console.
fn listing(console: &str) -> Value {
    let (status, body) = get(console, console, "/api/apps", Some(TOKEN));
    assert_eq!(status, 200, "{body}");
    serde_json::from_str(&body).unwrap()
}

/// What `cf app` shows of the instances: `<running>/<listed>`, those alive once the wicket took
/// the app.
fn instances_shown(cf: &Cf, app: &str) -> String {
    let shown = cf.ok(&["app", app]);
    let line = shown.lines().find(|line| line.starts_with("instances:"));
    let count = line.and_then(|line| line.split_whitespace().nth(1));
    count
        .unwrap_or_else(|| panic!("no instances: {shown}"))
        .to_string()
}

/// Waits for `cf app` to show `count` of the instances: they come and go as it reads them.
fn instances_come_to(cf: &Cf, app: &str, count: &str) {
    let deadline = Instant::now() + HANG;
    loop {
        let shown = instances_shown(cf, app);
        if shown == count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{app} shows {shown}, not {count}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Merges `annotations` into the app's through the v3 API: the answer, an error's too.
fn annotate(cf: &Cf, app: &str, annotations: &Value) -> String {
    let guid = cf.ok(&["app", app, "--guid"]);
    let body = json!({"metadata": {"annotations": annotations}}).to_string();
    let path = format!("/v3/apps/{}", guid.trim());
    cf.ok(&["curl", "-X", "PATCH", &path, "-d", &body])
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

/// The cf-api on `listen` of the wickets `wickets` (each `--wicket`'s value), its URL once it
/// listens.
fn start_api(
    listen: &str,
    wickets: &[String],
    flume: &str,
    data: &Path,
    more: &[&str],
) -> (Process, String) {
    let mut args = vec![
        "--listen",
        listen,
        "--wicket-token",
        TOKEN,
        "--data-dir",
        data.to_str().unwrap(),
        "--flume",
        flume,
        "--flume-token",
        FLUME_TOKEN,
    ];
    for wicket in wickets {
        args.extend_from_slice(&["--wicket", wicket]);
    }
    args.extend_from_slice(more);
    let api = Process::spawn(Path::new(env!("CARGO_BIN_EXE_sluice-wicket-cf-api")), &args);
    let started = api.line("listening on ");
    assert!(started.contains(&format!("flume {flume})")), "{started}");
    let (_, rest) = started.split_once("listening on ").unwrap();
    let url = rest.split(' ').next().unwrap().to_string();
    (api, url)
}

/// Waits for `done`, polled.
fn eventually(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + HANG;
    while !done() {
        assert!(Instant::now() < deadline, "never {what}");
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// Requests to a host's `/` on listeners in turn, until stopped.
struct Hammer {
    stop: Arc<AtomicBool>,
    requests: std::thread::JoinHandle<(usize, usize)>,
}

impl Hammer {
    /// Requests `host` on each of `listens` in turn, counting the answers `hello from wasm` and
    /// the others.
    fn start(listens: &[&str], host: &str) -> Hammer {
        let stop = Arc::new(AtomicBool::new(false));
        let listens: Vec<String> = listens.iter().map(|listen| listen.to_string()).collect();
        let (stopped, host) = (stop.clone(), host.to_string());
        let requests = std::thread::spawn(move || {
            let (mut served, mut failed) = (0, 0);
            for listen in listens.iter().cycle() {
                if stopped.load(Ordering::Relaxed) {
                    break;
                }
                match std::panic::catch_unwind(|| get(listen, &host, "/", None)) {
                    Ok((200, body)) if body.starts_with("hello from wasm") => served += 1,
                    _ => failed += 1,
                }
            }
            (served, failed)
        });
        Hammer { stop, requests }
    }

    /// Stops it: some requests answered, none failed.
    fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        let (served, failed) = self.requests.join().unwrap();
        assert_eq!(failed, 0, "of {}", served + failed);
        assert!(served > 0);
    }
}

/// `rules` as a file `cf create-security-group` takes.
fn security_group_file(dir: &Path, name: &str, rules: &Value) -> String {
    let file = dir.join(format!("{name}.json"));
    std::fs::write(&file, rules.to_string()).unwrap();
    file.to_str().unwrap().to_string()
}

/// HTTP(S) requests of the test to the cf-api (its certificate self-signed)
/// and the mock provider, redirects not followed.
struct Http(reqwest::blocking::Client);

impl Http {
    fn new() -> Http {
        // reqwest brings no crypto provider of its own here
        let _ = rustls::crypto::ring::default_provider().install_default();
        Http(
            reqwest::blocking::Client::builder()
                .danger_accept_invalid_certs(true)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        )
    }

    /// The status, the Location header and the body.
    fn get(&self, url: &str, bearer: Option<&str>) -> (u16, String, String) {
        let mut request = self.0.get(url);
        if let Some(token) = bearer {
            request = request.header("Authorization", token);
        }
        let response = request.send().unwrap();
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get("location")
            .map(|at| at.to_str().unwrap().to_string())
            .unwrap_or_default();
        (status, location, response.text().unwrap())
    }

    /// A token request as the CLI makes it: the status and the JSON.
    fn token(&self, api: &str, form: &str) -> (u16, Value) {
        let response = self
            .0
            .post(format!("{api}/uaa/oauth/token"))
            .basic_auth("cf", Some(""))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form.to_string())
            .send()
            .unwrap();
        (response.status().as_u16(), response.json().unwrap())
    }
}

/// `cf login --sso`'s browser side: `/uaa/passcode` to the provider, back
/// to the callback: the status and the page.
fn passcode_page(http: &Http, api: &str) -> (u16, String) {
    let (status, authorize, _) = http.get(&format!("{api}/uaa/passcode"), None);
    assert_eq!(status, 302);
    let callback = form_urlencoded::byte_serialize(format!("{api}/uaa/callback").as_bytes())
        .collect::<String>();
    for expected in [
        "response_type=code",
        "code_challenge_method=S256",
        &format!("redirect_uri={callback}"),
        "scope=openid+email+profile+groups",
    ] {
        assert!(authorize.contains(expected), "{expected}: {authorize}");
    }
    let (status, callback, _) = http.get(&authorize, None);
    assert_eq!(status, 302, "{authorize}");
    let (status, _, page) = http.get(&callback, None);
    (status, page)
}

/// The code of the passcode page, and the page.
fn passcode(http: &Http, api: &str) -> (String, String) {
    let (status, page) = passcode_page(http, api);
    assert_eq!(status, 200, "{page}");
    let (_, rest) = page.split_once("<code id=\"passcode\">").expect("the code");
    (rest.split_once("</code>").unwrap().0.to_string(), page)
}

#[test]
fn the_cf_cli_deploys_to_the_wicket() {
    let guests = guests();
    let wicket_binary = wicket_binary();
    let dir = std::env::temp_dir().join(format!("sluice-cf-api-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["app", "app2", "counter", "relay", "cfhome"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }

    // a data directory it cannot write: no listener first, which a VM's /init takes for ready
    let unwritable = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-cf-api"))
        .args(["--wicket", "http://127.0.0.1:1", "--listen", "127.0.0.1:0"])
        .args(["--any-credentials", "--data-dir", "/proc/sluice-cf-api"])
        .output()
        .unwrap();
    assert!(!unwritable.status.success());
    let said = String::from_utf8_lossy(&unwritable.stdout);
    assert!(!said.contains("listening on"), "{said}");
    let why = String::from_utf8_lossy(&unwritable.stderr);
    assert!(why.contains("--data-dir /proc/sluice-cf-api"), "{why}");
    let hello = std::fs::read(guests.join("hello.wasm")).unwrap();
    std::fs::write(dir.join("app/hello.wasm"), &hello).unwrap();
    std::fs::write(dir.join("app2/hello.wasm"), variant(&hello)).unwrap();
    for guest in ["counter", "relay"] {
        std::fs::copy(
            guests.join(format!("{guest}.wasm")),
            dir.join(guest).join(format!("{guest}.wasm")),
        )
        .unwrap();
    }

    let flume = Process::spawn(
        &flume_binary(),
        &["--host", "127.0.0.1", "--port", "0", "--token", FLUME_TOKEN],
    );
    let flume_url = flume.logged("listening on ");
    let redis = redis_server::RedisServer::tcp();
    // a wicket on the flume, its console at `console`, of a console directory and a keyvalue
    // file of its own (`deployed<suffix>`, `kv<suffix>.db`, the default store `local`), the
    // Redis every wicket shares (`redis`), and `more`: its listener and its console
    let start_wicket = |console: &str, suffix: &str, more: &[&str]| {
        let console_dir = dir.join(format!("deployed{suffix}"));
        let config = dir.join(format!("wicket{suffix}.yaml"));
        std::fs::write(
            &config,
            format!(
                "keyvalue:\n  default: local\n  stores:\n    local: {{type: sqlite, path: \
                 kv{suffix}.db}}\n    redis: {{type: redis, url: '{}/0'}}\n",
                redis.url
            ),
        )
        .unwrap();
        let mut args = vec![
            "--flume",
            &flume_url,
            "--flume-token",
            FLUME_TOKEN,
            "--listen",
            "127.0.0.1:0",
            "--console",
            console,
            "--console-token",
            TOKEN,
            "--console-dir",
            console_dir.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "--watch-interval",
            "0",
        ];
        args.extend_from_slice(more);
        let wicket = Process::spawn(&wicket_binary, &args);
        let listen = wicket.logged("listening on ");
        let console = wicket.logged("console on http://");
        (wicket, listen, console)
    };
    let (mut wicket, listen, console) = start_wicket("127.0.0.1:0", "", &[]);
    // ports of its own, kept across the restarts (the CLI targets it; the cf-api names the
    // second wicket's console)
    let free = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .unwrap()
            .to_string()
    };
    let api_listen = free();
    let one_wicket = [format!("http://{console}")];
    let (mut api, url) = start_api(
        &api_listen,
        &one_wicket,
        &flume_url,
        &dir.join("cf"),
        &["--any-credentials"],
    );
    let provider = MockProvider::start("cf-api", "s3");
    let http = Http::new();
    let cf = Cf {
        cli: cf_cli(),
        home: dir.join("cfhome"),
    };

    cf.ok(&["api", &url]);
    cf.ok(&["login", "-u", "me", "-p", "x"]);

    // the security groups: whoever logs in reads them, nobody edits them unless named, on
    // an API that takes any credentials above all
    let tcp_rule = json!([{"protocol": "tcp", "destination": "10.0.0.5", "ports": "443"}]);
    let group = security_group_file(&dir, "group", &tcp_rule);
    let refused = cf.fails(&["create-security-group", "group", &group]);
    assert!(
        refused.contains("--security-group-editor names no one"),
        "{refused}"
    );
    assert!(
        cf.ok(&["security-groups"])
            .contains("No security groups found")
    );
    // nor the service brokers: a broker's URL is any the cf-api reaches
    let refused = cf.fails(&[
        "create-service-broker",
        "db-broker",
        "broker",
        "br0ker",
        "http://127.0.0.1:1",
    ]);
    assert!(
        refused.contains("--service-broker-editor names no one"),
        "{refused}"
    );

    // push from a directory: served on the wicket's listener
    cf.ok(&["push", "hello", "-p", dir.join("app").to_str().unwrap()]);
    let (status, body) = get(&listen, "hello.localhost", "/", None);
    assert_eq!(status, 200, "{body}");
    assert!(body.starts_with("hello from wasm"), "{body}");
    assert!(
        cf.ok(&["logs", "hello", "--recent"])
            .contains("Staged hello.wasm")
    );
    assert!(cf.ok(&["stacks"]).contains("wasip3 "));
    let buildpacks = cf.ok(&["buildpacks"]);
    for buildpack in ["wasm", "rootfs", "site"] {
        assert!(buildpacks.contains(buildpack), "{buildpacks}");
    }

    // organizations and spaces: without --admin whoever logs in is an admin; a space holds
    // its own apps, and its maker manages and develops in it (the CLI gives the roles)
    cf.ok(&["create-org", "team"]);
    cf.ok(&["create-space", "dev", "-o", "team"]);
    cf.ok(&["target", "-o", "team", "-s", "dev"]);
    assert!(cf.ok(&["apps"]).contains("No apps found"));
    let users = cf.ok(&["space-users", "team", "dev"]);
    assert!(
        users.contains("SPACE MANAGER\n  me") && users.contains("SPACE DEVELOPER\n  me"),
        "{users}"
    );
    cf.ok(&["set-org-role", "me", "team", "OrgManager"]);
    let users = cf.ok(&["org-users", "team"]);
    assert!(users.contains("ORG MANAGER\n  me"), "{users}");
    cf.ok(&["rename-space", "dev", "development"]);
    cf.ok(&["delete-org", "team", "-f"]);
    assert!(!cf.ok(&["orgs"]).contains("team"));
    cf.ok(&["target", "-o", "sluice", "-s", "wicket"]);

    // the app's output and requests, from the flume: recent, then streamed
    assert_eq!(get(&listen, "hello.localhost", "/log", None).0, 200);
    let router = "[RTR/0] OUT hello.localhost - [";
    let request = |path: &str| format!("\"GET {path} HTTP/1.0\" 200 ");
    let output = [
        "[APP/PROC/WEB/0] OUT hello stdout: Method::Get /log body_bytes=0",
        "[APP/PROC/WEB/0] ERR hello stderr: Method::Get /log",
    ];
    let deadline = Instant::now() + HANG;
    loop {
        let recent = cf.ok(&["logs", "hello", "--recent"]);
        if output.iter().all(|line| recent.contains(line))
            && recent
                .lines()
                .any(|line| line.contains(router) && line.contains(&request("/log")))
        {
            break;
        }
        assert!(Instant::now() < deadline, "the app's lines never came");
        std::thread::sleep(Duration::from_millis(500));
    }
    let watching = api.line("cf logs: watching the flume ");
    assert!(
        watching.ends_with(&format!("{flume_url} for hello.localhost")),
        "{watching}"
    );
    let streaming = Process::start(cf.command(&["logs", "hello"]));
    streaming.logged("Retrieving logs");
    assert_eq!(get(&listen, "hello.localhost", "/streamed", None).0, 200);
    // the word after the request: the bytes received, unknown
    assert_eq!(streaming.logged(&request("/streamed")), "-");
    drop(streaming);

    // routes and instances apply at once
    cf.ok(&["map-route", "hello", "localhost", "--hostname", "hi"]);
    assert_eq!(get(&listen, "hi.localhost", "/", None).0, 200);
    let apps = listing(&console);
    assert_eq!(apps["apps"].as_array().unwrap().len(), 1);
    assert_eq!(
        apps["apps"][0]["hosts"],
        serde_json::json!(["hello.localhost", "hi.localhost"])
    );
    // a scale keeps the warm instance: what it holds counts on, its target stays
    let served = || {
        let (status, body) = get(&listen, "hello.localhost", "/served", None);
        assert_eq!(status, 200, "{body}");
        body.trim().parse::<u64>().unwrap()
    };
    let advertised = || {
        let (status, body) = get(&console, &console, "/api/routes", Some(TOKEN));
        assert_eq!(status, 200, "{body}");
        let routes: Value = serde_json::from_str(&body).unwrap();
        (
            routes["generation"].clone(),
            routes["routes"][0]["target"].clone(),
        )
    };
    let count = (0..3).map(|_| served()).max().unwrap();
    assert!(count >= 3, "{count}");
    let before = advertised();
    cf.ok(&["scale", "hello", "-i", "2"]);
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["warm"], 2);
    assert_eq!(advertised(), before, "nothing to advertise");
    let deadline = Instant::now() + HANG;
    while served() <= count {
        assert!(Instant::now() < deadline, "the warm instance was replaced");
    }
    // the second warm instance takes its turn: its index in the output and in
    // app_index (a request before the units are up runs on an instance of its
    // own, named as one of them)
    let deadline = Instant::now() + HANG;
    loop {
        assert_eq!(get(&listen, "hello.localhost", "/log", None).0, 200);
        let recent = cf.ok(&["logs", "hello", "--recent"]);
        if recent.contains("[APP/PROC/WEB/1] OUT hello stdout: Method::Get /log")
            && recent
                .lines()
                .any(|line| line.contains(&request("/log")) && line.contains("app_index:\"1\""))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "instance 1 never served: {recent}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // a rolling push of another component, with no failed request during it
    let hammering = Hammer::start(&[&listen], "hello.localhost");
    cf.ok(&[
        "push",
        "hello",
        "-p",
        dir.join("app2").to_str().unwrap(),
        "--strategy",
        "rolling",
    ]);
    hammering.stop();
    let apps = listing(&console);
    assert_eq!(
        apps["apps"][0]["component"]["sha256"],
        sha256(&dir.join("app2/hello.wasm"))
    );
    assert_eq!(
        apps["apps"][0]["sandbox"]["warm"], 2,
        "kept across the push"
    );

    // stopped: no app in the wicket; started: served again
    cf.ok(&["stop", "hello"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 404);
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    cf.ok(&["start", "hello"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 200);

    // scaled to 0: as stopped, there and on CF; a push of 0 waits for no instance
    cf.ok(&["scale", "hello", "-i", "0"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 404);
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    assert_eq!(instances_shown(&cf, "hello"), "0/0");
    cf.ok(&["restart", "hello"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 404);
    let app = dir.join("app");
    cf.ok(&["push", "none", "-p", app.to_str().unwrap(), "-i", "0"]);
    assert_eq!(get(&listen, "none.localhost", "/", None).0, 404);
    assert_eq!(instances_shown(&cf, "none"), "0/0");
    cf.ok(&["delete", "none", "-f"]);
    cf.ok(&["scale", "hello", "-i", "2"]);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 200);
    // no warm instance is the annotation's, at once; VMs' annotations are refused
    let warm = json!({"wicket.sluice.ik.am/warm": "0"});
    assert!(annotate(&cf, "hello", &warm).contains("\"guid\""));
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["warm"], 0);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 200);
    // `cf app` lists what is alive: none between requests, which nothing waiting for a start
    // takes for a failure, and a request's own while it runs
    instances_come_to(&cf, "hello", "0/0");
    cf.ok(&["restart", "hello"]);
    cf.ok(&["restart", "hello", "--strategy", "rolling"]);
    let deadline = Instant::now() + HANG;
    loop {
        let stream = held(&listen, "hello.localhost", "/stall");
        let shown = cf.ok(&["app", "hello"]);
        drop(stream);
        if shown
            .lines()
            .any(|line| line.starts_with("#0") && line.contains("running"))
        {
            assert_eq!(shown.matches("\n#").count(), 1, "{shown}");
            break;
        }
        assert!(Instant::now() < deadline, "{shown}");
    }
    instances_come_to(&cf, "hello", "0/0");
    // refused, a PATCH changes nothing, of what came with the annotation neither
    let guid = cf.ok(&["app", "hello", "--guid"]);
    let path = format!("/v3/apps/{}", guid.trim());
    let before = cf.ok(&["curl", &path]);
    let patch = json!({"name": "renamed", "metadata": {"labels": {"kept": "no"},
        "annotations": {"wicket.sluice.ik.am/mode": "pool"}}})
    .to_string();
    let refused = cf.ok(&["curl", "-X", "PATCH", &path, "-d", &patch]);
    assert!(refused.contains("no VMs"), "{refused}");
    assert_eq!(cf.ok(&["curl", &path]), before);
    let space = cf.ok(&["space", "wicket", "--guid"]);
    let create = json!({"name": "refused", "metadata": {"annotations":
        {"wicket.sluice.ik.am/mode": "pool"}},
        "relationships": {"space": {"data": {"guid": space.trim()}}}})
    .to_string();
    let refused = cf.ok(&["curl", "-X", "POST", "/v3/apps", "-d", &create]);
    assert!(refused.contains("no VMs"), "{refused}");
    assert!(!cf.ok(&["apps"]).contains("refused"), "no app comes of it");
    // a scale holds as annotated, refused naming it
    let warm = json!({"wicket.sluice.ik.am/warm": "2"});
    annotate(&cf, "hello", &warm);
    let refused = cf.fails(&["scale", "hello", "-i", "1"]);
    assert!(refused.contains("annotation"), "{refused}");
    annotate(&cf, "hello", &json!({"wicket.sluice.ik.am/warm": null}));
    instances_come_to(&cf, "hello", "2/2");
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["warm"], 2);

    // `cf scale` the limit: an instance takes `concurrency` requests at once (200 unless
    // annotated), the next waits for one of them to end; what serves beside them is named as
    // `cf app` lists it
    let limited = "limited.localhost";
    let manifest = dir.join("limited.yml");
    let budget = "applications:\n- name: limited\n  metadata:\n    annotations:\n      wicket.sluice.ik.am/budget-ms: 30000\n";
    std::fs::write(&manifest, budget).unwrap();
    cf.ok(&[
        "push",
        "-f",
        manifest.to_str().unwrap(),
        "-p",
        dir.join("app").to_str().unwrap(),
    ]);
    // a stalled response holds its instance until its client's reset (`held` leaves it unread)
    let beyond = |room: usize| {
        let streams: Vec<TcpStream> = (0..room)
            .map(|_| held(&listen, limited, "/stall"))
            .collect();
        let waiting = answered(&listen, limited, "/");
        let wait = waiting.recv_timeout(Duration::from_millis(500));
        assert_eq!(wait.ok(), None, "past {room} at once");
        drop(streams);
        assert_eq!(waiting.recv_timeout(HANG).ok(), Some(200));
    };
    let stream = held(&listen, limited, "/stall");
    assert_eq!(get(&listen, limited, "/log", None).0, 200);
    drop(stream);
    let deadline = Instant::now() + HANG;
    let recent = loop {
        let recent = cf.ok(&["logs", "limited", "--recent"]);
        if recent.contains(&request("/log")) {
            break recent;
        }
        assert!(Instant::now() < deadline, "the request's line never came");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(
        recent.contains("[APP/PROC/WEB/0] OUT hello stdout: Method::Get /log")
            && !recent.contains("[APP/PROC/WEB/1]")
            && !recent.contains("app_index:\"1\""),
        "{recent}"
    );
    // one at a time is the annotation's, at once; the limit follows the scale
    let app_of = || {
        let listed = listing(&console);
        let apps = listed["apps"].as_array().unwrap().clone();
        apps.into_iter()
            .find(|app| app["hosts"][0] == limited)
            .unwrap()
    };
    let limits = || {
        let sandbox = app_of()["sandbox"].clone();
        let count = |key: &str| sandbox[key].as_u64();
        (
            count("warm"),
            count("instances"),
            count("concurrency"),
            count("keep-ms"),
        )
    };
    assert_eq!(limits(), (Some(1), Some(1), Some(200), None));
    annotate(
        &cf,
        "limited",
        &json!({"wicket.sluice.ik.am/concurrency": "1"}),
    );
    assert_eq!(limits(), (Some(1), Some(1), Some(1), None));
    beyond(1);
    cf.ok(&["scale", "limited", "-i", "2"]);
    assert_eq!(limits(), (Some(2), Some(2), Some(1), None));
    // a request past the warm instance has one of its own kept `keep-ms` after it, at once; none
    // kept without it
    let alive_come_to = |count: usize| {
        let deadline = Instant::now() + HANG;
        while app_of()["alive"].as_array().unwrap().len() != count {
            assert!(Instant::now() < deadline, "{}", app_of()["alive"]);
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let keep = json!({"wicket.sluice.ik.am/warm": "1", "wicket.sluice.ik.am/keep-ms": "60000"});
    assert!(annotate(&cf, "limited", &keep).contains("\"guid\""));
    assert_eq!(limits(), (Some(1), Some(2), Some(1), Some(60_000)));
    alive_come_to(1);
    let stream = held(&listen, limited, "/stall");
    assert_eq!(get(&listen, limited, "/", None).0, 200);
    drop(stream);
    alive_come_to(2);
    annotate(
        &cf,
        "limited",
        &json!({"wicket.sluice.ik.am/keep-ms": null}),
    );
    alive_come_to(1);
    let refused = annotate(
        &cf,
        "limited",
        &json!({"wicket.sluice.ik.am/instances": "4"}),
    );
    assert!(
        refused.contains("cf scale -i") && refused.contains("concurrency"),
        "{refused}"
    );
    cf.ok(&["delete", "limited", "-f"]);

    // the user's variables, VCAP_APPLICATION and VCAP_SERVICES with a
    // user-provided instance's credentials: the guest gets them from the
    // next start on; no log and no listing of the wicket holds a value
    let variable = |name: &str| get(&listen, "hello.localhost", &format!("/env/{name}"), None);
    let password = || {
        let (status, vcap) = variable("VCAP_SERVICES");
        assert_eq!(status, 200, "{vcap}");
        let vcap: Value = serde_json::from_str(&vcap).unwrap();
        vcap["user-provided"][0]["credentials"]["password"].clone()
    };
    cf.ok(&["set-env", "hello", "GREETING", "s3cr3t-greeting"]);
    let credentials = |password: &str| format!(r#"{{"password":"{password}"}}"#);
    cf.ok(&[
        "create-user-provided-service",
        "creds",
        "-p",
        &credentials("s3cr3t-1"),
        "-t",
        "db",
    ]);
    for (args, refusal) in [
        (
            &["bind-service", "hello", "creds", "-c", r#"{"x":1}"#][..],
            "Binding parameters are not supported",
        ),
        (
            &[
                "create-user-provided-service",
                "drain",
                "-l",
                "syslog://logs:514",
            ][..],
            "a syslog drain is not supported",
        ),
    ] {
        let output = cf.fails(args);
        assert!(output.contains(refusal), "{output}");
    }
    cf.ok(&["bind-service", "hello", "creds"]);
    assert_eq!(variable("GREETING").0, 404, "until the next start");
    cf.ok(&["restart", "hello"]);
    assert_eq!(variable("GREETING"), (200, "s3cr3t-greeting".into()));
    let (status, vcap) = variable("VCAP_APPLICATION");
    assert_eq!(status, 200, "{vcap}");
    let vcap: Value = serde_json::from_str(&vcap).unwrap();
    assert_eq!(vcap["application_name"], "hello");
    assert_eq!(password(), "s3cr3t-1");
    cf.ok(&[
        "update-user-provided-service",
        "creds",
        "-p",
        &credentials("s3cr3t-2"),
    ]);
    assert_eq!(password(), "s3cr3t-1", "until the next start");
    let env = cf.ok(&["env", "hello"]);
    assert!(
        env.contains("GREETING: s3cr3t-greeting") && env.contains("\"password\": \"s3cr3t-2\""),
        "{env}"
    );
    for shown in [cf.ok(&["services"]), cf.ok(&["service", "creds"])] {
        assert!(
            shown.contains("user-provided") && shown.contains("hello"),
            "{shown}"
        );
    }
    let (_, apps) = get(&console, &console, "/api/apps", Some(TOKEN));
    assert!(
        apps.contains("\"GREETING\"") && !apps.contains("s3cr3t"),
        "{apps}"
    );
    let recent = cf.ok(&["logs", "hello", "--recent"]);
    assert!(
        recent.contains("[PRIVATE DATA HIDDEN]") && !recent.contains("s3cr3t"),
        "{recent}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let state = std::fs::metadata(dir.join("cf/state.json")).unwrap();
        assert_eq!(
            state.permissions().mode() & 0o777,
            0o600,
            "it holds the env"
        );
    }
    cf.ok(&["unset-env", "hello", "GREETING"]);
    let env = cf.ok(&["env", "hello"]);
    assert!(
        env.contains("No user-provided env variables have been set"),
        "{env}"
    );
    cf.ok(&["restart", "hello"]);
    assert_eq!(password(), "s3cr3t-2");
    cf.ok(&["unbind-service", "hello", "creds"]);
    cf.ok(&["delete-service", "creds", "-f"]);
    assert!(!cf.ok(&["services"]).contains("creds"));
    cf.ok(&["restart", "hello"]);
    assert_eq!(variable("VCAP_SERVICES"), (200, "{}".into()));

    // a component in an OCI registry (the test's, as ghcr.io: a token for
    // every pull, the user's for a private image), pushed by tag: its
    // credentials kept with the app, the password shown nowhere; the tag
    // pinned to its digest, so a restart keeps the component staged when
    // the tag moves, and a restage pulls the tag again. Pushed with bits,
    // the app is a zip's again
    let registry = Registry::start("robot", "p4ss");
    let first = registry.push("private/hello", "1", &hello);
    let image = format!("{}/private/hello:1", registry.address);
    let output = cf.fails(&["push", "img", "--docker-image", &image]);
    assert!(
        output.contains("it is private: --docker-username"),
        "{output}"
    );
    let mut push = cf.command(&[
        "push",
        "img",
        "--docker-image",
        &image,
        "--docker-username",
        "robot",
    ]);
    push.env("CF_DOCKER_PASSWORD", "p4ss");
    let (ok, output) = cf.output(push, "");
    assert!(ok, "{output}");
    let (status, body) = get(&listen, "img.localhost", "/", None);
    assert_eq!(status, 200, "{body}");
    let shown = cf.ok(&["app", "img"]);
    assert!(
        shown.contains("docker image:") && shown.contains(&format!("{image}@{first}")),
        "{shown}"
    );
    let (token, _) = cf.tokens();
    let (_, _, packages) = http.get(&format!("{url}/v3/packages"), Some(&token));
    assert!(
        packages.contains(r#""password":"***""#) && !packages.contains("p4ss"),
        "{packages}"
    );
    assert!(!cf.ok(&["logs", "img", "--recent"]).contains("p4ss"));
    let served = || {
        listing(&console)["apps"]
            .as_array()
            .unwrap()
            .iter()
            .find(|app| app["hosts"] == json!(["img.localhost"]))
            .map(|app| app["component"]["sha256"].as_str().unwrap().to_string())
            .unwrap()
    };
    let (hello_sha256, variant_sha256) = (
        sha256(&dir.join("app/hello.wasm")),
        sha256(&dir.join("app2/hello.wasm")),
    );
    assert_eq!(served(), hello_sha256);
    let second = registry.push("private/hello", "1", &variant(&hello));
    let pulls = registry.pulls();
    cf.ok(&["restart", "img"]);
    assert_eq!(registry.pulls(), pulls, "a restart pulls nothing");
    assert_eq!(served(), hello_sha256);
    cf.ok(&["restage", "img"]);
    assert_eq!(served(), variant_sha256);
    assert!(
        cf.ok(&["app", "img"])
            .contains(&format!("{image}@{second}"))
    );
    cf.ok(&["push", "img", "-p", dir.join("app").to_str().unwrap()]);
    assert_eq!(served(), hello_sha256);
    assert!(!cf.ok(&["app", "img"]).contains("docker image:"));

    // an upload to a package a newer one of its app replaced (two pushes at once): refused,
    // and the API goes on
    {
        let (token, _) = cf.tokens();
        let (_, _, apps) = http.get(&format!("{url}/v3/apps?names=hello"), Some(&token));
        let apps: Value = serde_json::from_str(&apps).unwrap();
        let guid = apps["resources"][0]["guid"].as_str().unwrap().to_string();
        let package = || {
            let response = http
                .0
                .post(format!("{url}/v3/packages"))
                .header("Authorization", &token)
                .body(
                    json!({"type": "bits", "relationships": {"app": {"data": {"guid": guid}}}})
                        .to_string(),
                )
                .send()
                .unwrap();
            let created: Value = response.json().unwrap();
            created["guid"].as_str().unwrap().to_string()
        };
        let older = package();
        package();
        let replaced = http
            .0
            .post(format!("{url}/v3/packages/{older}/upload"))
            .header("Authorization", &token)
            .header("Content-Type", "multipart/form-data; boundary=xyz")
            .body("--xyz\r\nContent-Disposition: form-data; name=\"bits\"\r\n\r\nPK\r\n--xyz--\r\n")
            .send();
        let status = replaced.map(|response| response.status().as_u16());
        assert!(matches!(status, Ok(422)), "{status:?}");
        assert_eq!(http.get(&format!("{url}/v3/apps"), Some(&token)).0, 200);
    }

    // the cf-api restarted with an OpenID Provider and TLS of its own: its
    // state kept, the wicket's generation unchanged; the open mode's token
    // is no good any more, nor is a password
    let generation = listing(&console)["generation"].as_u64().unwrap();
    let (open_token, _) = cf.tokens();
    api.kill();

    // a lost state (a new data directory): the wicket's apps of the owner stay, and the
    // log says so
    let (lost, _) = start_api(
        &api_listen,
        &one_wicket,
        &flume_url,
        &dir.join("cf-lost"),
        &["--any-credentials"],
    );
    let left = lost.line("CELL: left");
    assert!(left.contains("left in the wicket"), "{left}");
    assert_eq!(listing(&console)["generation"], generation);
    assert_eq!(listing(&console)["apps"].as_array().unwrap().len(), 2);
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 200);
    drop(lost);

    let tls = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
    let (cert_file, key_file) = (dir.join("tls.crt"), dir.join("tls.key"));
    std::fs::write(&cert_file, tls.cert.pem()).unwrap();
    std::fs::write(&key_file, tls.signing_key.serialize_pem()).unwrap();
    let oidc = [
        "--oidc-issuer",
        &provider.issuer,
        "--oidc-client-id",
        "cf-api",
        "--oidc-client-secret",
        "s3",
        "--oidc-scopes",
        "openid,email,profile,groups",
        "--client-credentials",
        "robot:r0bot",
        "--client-credentials",
        "other:0ther",
        "--security-group-editor",
        "groups=ops",
        "--security-group-editor-client",
        "robot",
        "--service-broker-editor-client",
        "robot",
        "--group-role",
        "devs=space_developer@sluice/wicket,ops=admin",
        "--admin-client",
        "robot",
        "--tls-cert",
        cert_file.to_str().unwrap(),
        "--tls-key",
        key_file.to_str().unwrap(),
    ];
    let with_rules = |wickets: &[String], rules: &[&str]| {
        let mut args: Vec<&str> = oidc.to_vec();
        for rule in rules {
            args.extend_from_slice(&["--oidc-claim", rule]);
        }
        start_api(&api_listen, wickets, &flume_url, &dir.join("cf"), &args)
    };
    let (api, url) = with_rules(&one_wicket, &["email=*@example.com", "groups=devs"]);
    assert!(url.starts_with("https://"), "{url}");
    api.logged("Found in the wicket as wanted");
    assert_eq!(listing(&console)["generation"], generation);
    let (status, _, body) = http.get(&format!("{url}/v3/apps"), Some(&open_token));
    assert_eq!(status, 401, "{body}");
    assert!(body.contains("log in again"), "{body}");
    cf.ok(&["api", &url, "--skip-ssl-validation"]);
    let output = cf.fails(&["login", "-u", "me", "-p", "x"]);
    assert!(
        output.contains(&format!(
            "cf login --sso, with a one-time code from {url}/uaa/passcode"
        )),
        "{output}"
    );

    // alice, allowed, signs in through the provider: the passcode page's
    // code logs the CLI in, once
    provider.sign_in(json!({
        "sub": "1", "email": "alice@example.com", "preferred_username": "alice", "groups": ["devs"]
    }));
    let (code, page) = passcode(&http, &url);
    assert_eq!(code.len(), 8);
    assert!(
        page.contains(
            "Your groups (the ID token's claim groups): [&quot;devs&quot;]; \
             --group-role gives devs=space_developer@sluice/wicket."
        ),
        "{page}"
    );
    // its style, the wickets' consoles', with no token
    for asset in ["console.css", "fonts/overpass.woff2"] {
        let (status, _, _) = http.get(&format!("{url}/uaa/{asset}"), None);
        assert_eq!(status, 200, "{asset}");
    }
    let output = cf.ok_with_stdin(&["login", "--sso"], &format!("{code}\n"));
    assert!(output.contains("Authenticating...\nOK"), "{output}");
    assert!(cf.ok(&["target"]).contains("user:           alice"));
    // devs develop in sluice/wicket (--group-role), and are no admins
    assert!(cf.ok(&["app", "hello"]).contains("hi.localhost"));
    let refused = cf.fails(&["create-org", "team"]);
    assert!(refused.contains("not authorized"), "{refused}");
    let (_, refresh_token) = cf.tokens();
    // (a failed login drops the CLI's tokens: taken above)
    let output = cf.fails(&["login", "--sso-passcode", &code]);
    assert!(output.contains("used already"), "{output}");

    // the refresh token is good once; bob, out of the group, is refused at
    // the callback
    let refresh = |token: &str| {
        http.token(
            &url,
            &format!("grant_type=refresh_token&refresh_token={token}"),
        )
    };
    let (status, refreshed) = refresh(&refresh_token);
    assert_eq!(status, 200, "{refreshed}");
    let alice_refresh = refreshed["refresh_token"].as_str().unwrap().to_string();
    assert_ne!(alice_refresh, refresh_token);
    let (status, again) = refresh(&refresh_token);
    assert_eq!(status, 401, "{again}");
    assert!(
        again["error_description"]
            .as_str()
            .unwrap()
            .contains("used already")
    );
    provider.sign_in(json!({
        "sub": "2", "email": "bob@example.com", "preferred_username": "bob", "groups": ["ops"]
    }));
    let (status, page) = passcode_page(&http, &url);
    assert_eq!(status, 403, "{page}");
    assert!(
        page.contains("the claim groups is [&quot;ops&quot;], not 'devs'"),
        "{page}"
    );

    // who edits the security groups: the rule at the request, whoever else logged in
    let login = |who: Value| {
        provider.sign_in(who);
        let (code, _) = passcode(&http, &url);
        cf.ok_with_stdin(&["login", "--sso"], &format!("{code}\n"));
    };
    login(json!({
        "sub": "1", "email": "alice@example.com", "preferred_username": "alice", "groups": ["devs"]
    }));
    let refused = cf.fails(&["create-security-group", "group", &group]);
    assert!(
        refused.contains("the claim groups is [\"devs\"], not 'ops'"),
        "{refused}"
    );
    login(json!({
        "sub": "3", "email": "carol@example.com", "preferred_username": "carol",
        "groups": ["devs", "ops"]
    }));
    cf.ok(&["create-security-group", "group", &group]);
    api.logged("SECURITY GROUPS: carol: POST /v3/security_groups");

    // carol, of ops, is an admin (--group-role): she makes an organization and gives dave a
    // space's role in it, who sees that space and the group's
    cf.ok(&["create-org", "team"]);
    cf.ok(&["create-space", "dev", "-o", "team"]);
    cf.ok(&["set-space-role", "dave", "team", "dev", "SpaceDeveloper"]);
    api.logged("ORGS: carol: gave dave the role space_developer");
    provider.sign_in(json!({
        "sub": "4", "email": "dave@example.com", "preferred_username": "dave", "groups": ["devs"]
    }));
    let (code, _) = passcode(&http, &url);
    cf.ok_with_stdin(
        &["login", "--sso", "-o", "team", "-s", "dev"],
        &format!("{code}\n"),
    );
    assert!(cf.ok(&["apps"]).contains("No apps found"));
    let orgs = cf.ok(&["orgs"]);
    assert!(orgs.contains("sluice") && orgs.contains("team"), "{orgs}");
    // a space developer gives no roles
    let refused = cf.fails(&["set-space-role", "alice", "team", "dev", "SpaceAuditor"]);
    assert!(refused.contains("not authorized"), "{refused}");
    cf.ok(&["auth", "other", "0ther", "--client-credentials"]);
    let refused = cf.fails(&["delete-security-group", "group", "-f"]);
    assert!(
        refused.contains("the client other is not of --security-group-editor-client"),
        "{refused}"
    );
    assert!(cf.ok(&["security-groups"]).contains("group"));

    // a client configured: cf auth, with its secret; the rest of the test
    // runs as it
    let output = cf.fails(&["auth", "robot", "wrong", "--client-credentials"]);
    assert!(
        output.contains("unknown client 'robot' or wrong secret"),
        "{output}"
    );
    cf.ok(&["auth", "robot", "r0bot", "--client-credentials"]);
    cf.ok(&["target", "-o", "sluice", "-s", "wicket"]);
    assert!(cf.ok(&["app", "hello"]).contains("hi.localhost"));

    cf.ok(&["delete-security-group", "group", "-f"]);

    // a second wicket, the two named: an app's instances spread over them. The one wicket's
    // indices are placed again at the restart, its name no --wicket's: hello's second goes to
    // the second wicket
    let console_b = free();
    let (mut wicket_b, mut listen_b, _) = start_wicket(&console_b, "-b", &[]);
    let two_wickets = [
        format!("a=http://{console}"),
        format!("b=http://{console_b}"),
    ];
    // one wicket named twice, by two URLs: refused at the start, by the id its listing names
    let port = console.rsplit(':').next().unwrap();
    let twice = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-cf-api"))
        .args(["--wicket", &two_wickets[0], "--wicket"])
        .arg(format!("c=http://localhost:{port}"))
        .args(["--wicket-token", TOKEN, "--listen", "127.0.0.1:0"])
        .args(["--any-credentials", "--data-dir"])
        .arg(dir.join("twice"))
        .output()
        .unwrap();
    assert!(!twice.status.success());
    let why = String::from_utf8_lossy(&twice.stderr);
    assert!(
        why.contains("--wicket a and --wicket c name one wicket (id "),
        "{why}"
    );
    drop(api);
    let (api, _) = with_rules(&two_wickets, &["email=*@example.com", "groups=devs"]);
    let guid_of = |app: &str| cf.ok(&["app", app, "--guid"]).trim().to_string();
    let hello = guid_of("hello");
    let placed = api.line(&format!("[{hello}] CELL: index 1 placed on the wicket b"));
    assert!(placed.ends_with(": no --wicket names wicket"), "{placed}");
    // the sandbox of the app of `host` as a wicket lists it, if it has it
    let share = |console: &str, host: &str| {
        let apps = listing(console)["apps"].as_array().unwrap().clone();
        let app = apps.into_iter().find(|app| app["hosts"][0] == host);
        app.map(|app| app["sandbox"].clone())
    };
    eventually("hello's second instance on the second wicket", || {
        [&console, &console_b].into_iter().all(|console| {
            share(console, "hello.localhost").is_some_and(|sandbox| sandbox["instances"] == 1)
        })
    });

    // pushed at -i 2: an instance on each wicket, each listing the app at 1 and answering it on
    // its listener; `cf app` the two running, each on its wicket, index 0 on the second: the
    // first, holding the apps pushed before, has less room
    let app = dir.join("app");
    cf.ok(&["push", "spread", "-p", app.to_str().unwrap(), "-i", "2"]);
    for (console, listen) in [(&console, &listen), (&console_b, &listen_b)] {
        assert_eq!(share(console, "spread.localhost").unwrap()["instances"], 1);
        assert_eq!(get(listen, "spread.localhost", "/", None).0, 200);
    }
    let stats = format!("/v3/apps/{}/processes/web/stats", guid_of("spread"));
    let hosted = || {
        let stats: Value = serde_json::from_str(&cf.ok(&["curl", &stats])).unwrap();
        let entries = stats["resources"].as_array().unwrap().clone();
        let entry = |entry: &Value| {
            let text = |key: &str| entry[key].as_str().unwrap().to_string();
            (
                entry["index"].as_u64().unwrap(),
                text("state"),
                text("host"),
            )
        };
        entries.iter().map(entry).collect::<Vec<_>>()
    };
    let at = |index, state: &str, host: &str| (index, state.to_string(), host.to_string());
    let both = [at(0, "RUNNING", "b"), at(1, "RUNNING", "a")];
    eventually("an instance running on each wicket", || hosted() == both);
    let shown = cf.ok(&["app", "spread"]);
    for index in ["#0", "#1"] {
        let running = |line: &&str| line.starts_with(index) && line.contains("running");
        assert!(shown.lines().any(|line| running(&line)), "{shown}");
    }
    // `cf logs` names each line by the wicket that wrote it: its output #0 and #1, its requests
    // by the router of each, the wicket's place
    for listen in [&listen, &listen_b] {
        assert_eq!(get(listen, "spread.localhost", "/log", None).0, 200);
    }
    eventually("spread's lines of each wicket", || {
        let recent = cf.ok(&["logs", "spread", "--recent"]);
        let routed = |router: u64, index: u64| {
            recent.lines().any(|line| {
                line.contains(&format!("[RTR/{router}] OUT spread.localhost"))
                    && line.ends_with(&format!("app_index:\"{index}\""))
            })
        };
        [(1, 0), (0, 1)].into_iter().all(|(router, index)| {
            let out = format!("[APP/PROC/WEB/{index}] OUT hello stdout: Method::Get /log");
            recent.contains(&out) && routed(router, index)
        })
    });

    // the second wicket stopped: its index starting. Back, it serves what it kept once the
    // cf-api, finding it as wanted, says so, putting nothing again
    let deployed = |console: &str| {
        let apps = listing(console)["apps"].as_array().unwrap().clone();
        let app = apps
            .into_iter()
            .find(|app| app["hosts"][0] == "spread.localhost");
        app.map(|app| app["deployed"].clone())
    };
    let kept = deployed(&console_b);
    wicket_b.kill();
    let away = [at(0, "STARTING", "b"), at(1, "RUNNING", "a")];
    eventually("the second wicket's index starting", || hosted() == away);
    let shown = cf.ok(&["app", "spread"]);
    let starting = |line: &&str| line.starts_with("#0") && line.contains("starting");
    assert!(shown.lines().any(|line| starting(&line)), "{shown}");
    // back with room for 3 instances: hello's, spread's and one more; told to stop, it serves on
    // while its apps move, 60s at most
    let restarted = ["--max-instances", "3", "--evacuation-grace", "60"];
    (wicket_b, listen_b, _) = start_wicket(&console_b, "-b", &restarted);
    let spread = guid_of("spread");
    api.line(&format!("[{spread}] CELL: Found in the wicket b as wanted"));
    assert_eq!(deployed(&console_b), kept);
    eventually("spread served by the second wicket again", || {
        get(&listen_b, "spread.localhost", "/", None).0 == 200
    });
    eventually("an instance running on each wicket again", || {
        hosted() == both
    });

    // a rolling push of another component puts the first wicket's share, then the second's, both
    // listeners answering all along
    let component = |console: &str| {
        let apps = listing(console)["apps"].as_array().unwrap().clone();
        let app = apps
            .into_iter()
            .find(|app| app["hosts"][0] == "spread.localhost");
        app.map(|app| app["component"]["sha256"].as_str().unwrap().to_string())
    };
    let (hello_sha, variant_sha) = (
        sha256(&dir.join("app/hello.wasm")),
        sha256(&dir.join("app2/hello.wasm")),
    );
    let hammering = Hammer::start(&[&listen, &listen_b], "spread.localhost");
    let app2 = dir.join("app2");
    cf.ok(&[
        "push",
        "spread",
        "-p",
        app2.to_str().unwrap(),
        "--strategy",
        "rolling",
    ]);
    hammering.stop();
    for console in [&console, &console_b] {
        assert_eq!(component(console), Some(variant_sha.clone()));
    }
    // in order in the cf-api's log (the app's has its requests' lines, past its ring)
    for wicket in ["a", "b"] {
        let step = api.line(&format!("[{spread}] API: The rolling deployment "));
        assert!(step.ends_with(&format!("on the wicket {wicket}")), "{step}");
    }
    api.line(&format!("[{spread}] API: Finished the rolling deployment "));

    // one the wicket refuses (counter needs wasi=keyvalue) stops at the first wicket, active, its
    // instance crashed with the wicket's reason, the second serving the one before; each listener
    // answers the one before. Canceled, the first is back on it
    cf.ok(&[
        "push",
        "spread",
        "-p",
        dir.join("counter").to_str().unwrap(),
        "--strategy",
        "rolling",
        "--no-wait",
    ]);
    let stopped = [at(0, "RUNNING", "b"), at(1, "CRASHED", "a")];
    eventually("the deployment stopped at the first wicket", || {
        hosted() == stopped
    });
    let active =
        format!("/v3/deployments?app_guids={spread}&status_values=ACTIVE&order_by=-created_at");
    let active: Value = serde_json::from_str(&cf.ok(&["curl", &active])).unwrap();
    let error = &active["resources"][0]["status"]["details"]["error"];
    assert!(
        error.as_str().unwrap().contains("needs wasi=keyvalue"),
        "{active}"
    );
    for listen in [&listen, &listen_b] {
        let (status, body) = get(listen, "spread.localhost", "/", None);
        assert!(
            status == 200 && body.starts_with("hello from wasm"),
            "{body}"
        );
    }
    // the cf-api restarted takes it up where it was
    drop(api);
    let (api, _) = with_rules(&two_wickets, &["email=*@example.com", "groups=devs"]);
    eventually("the deployment taken up", || hosted() == stopped);
    let shown = cf.ok(&["app", "spread"]);
    assert!(
        shown.contains("Active deployment with status DEPLOYING"),
        "{shown}"
    );
    cf.ok(&["cancel-deployment", "spread"]);
    eventually("the first wicket back", || hosted() == both);
    eventually("the deployment canceled", || {
        !cf.ok(&["app", "spread"]).contains("Active deployment")
    });
    for console in [&console, &console_b] {
        assert_eq!(component(console), Some(variant_sha.clone()));
    }

    // a canary puts the first wicket's share and pauses, until continued
    let paused = cf.ok(&[
        "push",
        "spread",
        "-p",
        dir.join("app").to_str().unwrap(),
        "--strategy",
        "canary",
    ]);
    assert!(
        paused.contains("Active deployment with status PAUSED")
            && paused.contains("cf continue-deployment spread"),
        "{paused}"
    );
    assert_eq!(
        (component(&console), component(&console_b)),
        (Some(hello_sha.clone()), Some(variant_sha.clone()))
    );
    cf.ok(&["continue-deployment", "spread"]);
    for console in [&console, &console_b] {
        assert_eq!(component(console), Some(hello_sha.clone()));
    }

    // `cf scale -i 3` gives the wicket of the most room two, `-i 1` keeps index 0 alone, the
    // app removed from the other
    let instances = |console: &str, host: &str| {
        share(console, host).map(|sandbox| sandbox["instances"].clone())
    };
    let spread_on = |console: &str| instances(console, "spread.localhost");
    cf.ok(&["scale", "spread", "-i", "3"]);
    assert_eq!(
        (spread_on(&console), spread_on(&console_b)),
        (Some(json!(2)), Some(json!(1)))
    );
    cf.ok(&["scale", "spread", "-i", "1"]);
    assert_eq!(
        (spread_on(&console), spread_on(&console_b)),
        (None, Some(json!(1)))
    );
    assert_eq!(get(&listen, "spread.localhost", "/", None).0, 404);

    // the second wicket told to stop: spread's one index and hello's second start on the
    // first, then leave the second, which exits once they are gone, before its bound, the first
    // answering spread from then on
    let stopped = Instant::now();
    wicket_b.terminate();
    eventually("spread put on the first wicket", || {
        spread_on(&console) == Some(json!(1))
    });
    let hammering = Hammer::start(&[&listen], "spread.localhost");
    for step in [
        "moves to the wicket a: the wicket b is leaving",
        "runs on the wicket a: out of the wicket b's share",
    ] {
        api.line(&format!("[{spread}] CELL: Index 0 {step}"));
    }
    wicket_b.line("evacuated: no app left to wait for");
    wicket_b.exited();
    assert!(stopped.elapsed() < Duration::from_secs(60), "the bound");
    hammering.stop();
    assert_eq!(instances(&console, "hello.localhost"), Some(json!(2)));
    // back, it takes hello's second index again the same way round: the spread restored
    (wicket_b, listen_b, _) = start_wicket(&console_b, "-b", &restarted);
    api.line(&format!(
        "[{hello}] CELL: Index 1 moves to the wicket b: the app's instances spread over the \
         wickets again"
    ));
    eventually("hello spread over both wickets again", || {
        let hello_on = |console: &str| instances(console, "hello.localhost");
        (hello_on(&console), hello_on(&console_b)) == (Some(json!(1)), Some(json!(1)))
    });
    assert_eq!(spread_on(&console_b), None, "of one index: none to spread");
    cf.ok(&["delete", "spread", "-r", "-f"]);

    // the second wicket's budget taken but for hello's (a route of its console keeping 2 warm):
    // a new app's indices go to the first
    let console_route = format!("http://{console_b}/api/routes/full.localhost");
    let http_b = reqwest::blocking::Client::new();
    let deployed = http_b
        .put(format!("{console_route}?name=hello.wasm&warm=2"))
        .bearer_auth(TOKEN)
        .body(std::fs::read(app.join("hello.wasm")).unwrap())
        .send()
        .unwrap();
    assert_eq!(deployed.status(), 200, "{:?}", deployed.text());
    // the cf-api lists the second wicket's change at once
    std::thread::sleep(Duration::from_secs(1));
    cf.ok(&["push", "crowded", "-p", app.to_str().unwrap(), "-i", "2"]);
    assert_eq!(
        (
            instances(&console, "crowded.localhost"),
            instances(&console_b, "crowded.localhost")
        ),
        (Some(json!(2)), None)
    );
    cf.ok(&["delete", "crowded", "-r", "-f"]);
    let removed = http_b
        .delete(&console_route)
        .bearer_auth(TOKEN)
        .send()
        .unwrap();
    assert_eq!(removed.status(), 200);

    // the first wicket killed: gone once it has not answered for --wicket-away-grace, the indices
    // on it -- img's one, hello's first -- start on the second, which then has no room left.
    // Started again, it holds them until the cf-api removed them, serving neither, and takes
    // hello's second index once it has answered for as long
    assert_eq!(
        (
            instances(&console, "img.localhost"),
            instances(&console_b, "img.localhost")
        ),
        (Some(json!(1)), None)
    );
    drop(api);
    let mut args: Vec<&str> = oidc.to_vec();
    for rule in ["email=*@example.com", "groups=devs"] {
        args.extend_from_slice(&["--oidc-claim", rule]);
    }
    args.extend_from_slice(&["--wicket-away-grace", "2"]);
    let (api, _) = start_api(
        &api_listen,
        &two_wickets,
        &flume_url,
        &dir.join("cf"),
        &args,
    );
    let img = guid_of("img");
    api.line(&format!("[{img}] CELL: Found in the wicket a as wanted"));
    wicket.kill();
    api.line("CELL: The wicket a has not answered for 2s: placed on no more");
    for step in [
        "moves to the wicket b: the wicket a has not answered for 2s",
        "runs on the wicket b: out of the wicket a's share",
    ] {
        api.line(&format!("[{img}] CELL: Index 0 {step}"));
    }
    assert_eq!(get(&listen_b, "img.localhost", "/", None).0, 200);
    eventually("hello's first index on the second wicket", || {
        instances(&console_b, "hello.localhost") == Some(json!(2))
    });
    (wicket, _, _) = start_wicket(&console, "", &["--listen", &listen]);
    assert_ne!(get(&listen, "img.localhost", "/", None).0, 200, "held");
    eventually("img and hello removed from the first wicket", || {
        listing(&console)["apps"].as_array().unwrap().is_empty()
    });
    api.line(&format!(
        "[{hello}] CELL: Index 1 moves to the wicket a: the app's instances spread over the \
         wickets again"
    ));
    eventually("hello spread over both wickets again", || {
        let hello_on = |console: &str| instances(console, "hello.localhost");
        (hello_on(&console), hello_on(&console_b)) == (Some(json!(1)), Some(json!(1)))
    });
    assert_eq!(instances(&console_b, "img.localhost"), Some(json!(1)));

    // a component the wicket refuses: its reason reaches the user
    let output = cf.fails(&[
        "push",
        "counter",
        "-p",
        dir.join("counter").to_str().unwrap(),
    ]);
    assert!(output.contains("needs wasi=keyvalue"), "{output}");
    assert!(output.contains("crashed"), "{output}");

    // a keyvalue store as a service: bound, the app counts in the
    // instance's namespace from its next start on, across a restart and a
    // rename of the instance
    let marketplace = cf.ok(&["marketplace"]);
    for offered in [
        "keyvalue",
        "local",
        "redis",
        "wasi",
        "inherit-network",
        "sluice-wicket",
    ] {
        assert!(marketplace.contains(offered), "{offered}: {marketplace}");
    }
    cf.ok(&["create-service", "keyvalue", "local", "kv"]);
    cf.ok(&["bind-service", "counter", "kv"]);
    cf.ok(&["restart", "counter"]);
    let count = |path: &str| get(&listen, "counter.localhost", path, None).1;
    assert_eq!(count("/"), "hits 1\n");
    assert_eq!(count("/"), "hits 2\n");
    cf.ok(&["rename-service", "kv", "counts"]);
    cf.ok(&["restart", "counter"]);
    assert_eq!(count("/"), "hits 3\n");
    // another instance at another identifier; at the same one, refused
    cf.ok(&["create-service", "keyvalue", "local", "sessions"]);
    let identifier = r#"{"identifier":"sessions"}"#;
    cf.ok(&["bind-service", "counter", "sessions", "-c", identifier]);
    cf.ok(&["create-service", "keyvalue", "local", "spare"]);
    let output = cf.fails(&["bind-service", "counter", "spare"]);
    assert!(
        output.contains("counter binds counts at the keyvalue identifier \"\" already"),
        "{output}"
    );
    let env = cf.ok(&["env", "counter"]);
    assert!(env.contains("\"identifier\": \"sessions\""), "{env}");
    cf.ok(&["restart", "counter"]);
    assert_eq!(count("/?store=sessions"), "hits 1\n");
    let service = cf.ok(&["service", "counts"]);
    for shown in ["keyvalue", "local", "sluice-wicket", "counter"] {
        assert!(service.contains(shown), "{shown}: {service}");
    }
    // an instance bound is not deleted; unbound and deleted, its data goes
    // once no app maps it (the restart), and a new instance counts from 1
    let output = cf.fails(&["delete-service", "counts", "-f"]);
    assert!(output.contains("counts is bound to counter"), "{output}");
    cf.ok(&["unbind-service", "counter", "counts"]);
    cf.ok(&["delete-service", "counts", "-f"]);
    assert!(!cf.ok(&["services"]).contains("counts"));
    cf.ok(&["create-service", "keyvalue", "local", "kv"]);
    cf.ok(&["bind-service", "counter", "kv"]);
    cf.ok(&["restart", "counter"]);
    assert_eq!(count("/"), "hits 1\n");
    // through each wicket: the first held them, a file of its host
    let purged: BTreeSet<String> = [(); 2]
        .map(|()| api.line("SERVICES: deleted the data of service instance counts on the wicket "))
        .into_iter()
        .map(|line| {
            let (on, keys) = line
                .split_once(" on the wicket ")
                .unwrap()
                .1
                .split_once(' ')
                .unwrap();
            format!("{on}{}", keys.rsplit(':').next().unwrap())
        })
        .collect();
    assert_eq!(
        purged,
        ["a 1 key(s)", "b 0 key(s)"].map(String::from).into()
    );
    // its store each wicket's host's own, its instances are kept on the wicket of its data
    cf.ok(&["scale", "counter", "-i", "2"]);
    assert_eq!(
        (
            instances(&console, "counter.localhost"),
            instances(&console_b, "counter.localhost")
        ),
        (Some(json!(2)), None)
    );
    // a store the wickets share: an app's instances spread over both count once, its data
    // deleted through one wicket
    cf.ok(&["create-service", "keyvalue", "redis", "tallies"]);
    let tally_manifest = dir.join("counter/tally.yml");
    std::fs::write(
        &tally_manifest,
        "applications:\n- name: tally\n  instances: 2\n  routes:\n  - route: tally.localhost\n  services:\n  - tallies\n",
    )
    .unwrap();
    let counter_dir = dir.join("counter");
    cf.ok(&[
        "push",
        "-f",
        tally_manifest.to_str().unwrap(),
        "-p",
        counter_dir.to_str().unwrap(),
    ]);
    let tally = |listen: &str| get(listen, "tally.localhost", "/", None).1;
    assert_eq!(
        [tally(&listen), tally(&listen_b), tally(&listen)],
        ["hits 1\n", "hits 2\n", "hits 3\n"]
    );
    cf.ok(&["delete", "tally", "-r", "-f"]);
    cf.ok(&["delete-service", "tallies", "-f"]);
    let purged = api.line("SERVICES: deleted the data of service instance tallies on the wicket ");
    assert!(purged.ends_with(": 1 key(s)"), "{purged}");

    // a capability as a service, bound by the manifest: cli links relay's
    // sockets, every address denied; inherit-network lets it relay
    let upstream = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    std::thread::spawn(move || {
        for mut stream in upstream.incoming().map_while(Result::ok) {
            let mut head = [0u8; 1024];
            let _ = stream.read(&mut head);
            let _ = stream.write_all(b"HTTP/1.0 200 OK\r\n\r\nupstream\n");
        }
    });
    cf.ok(&["create-service", "wasi", "cli", "sockets"]);
    let manifest = dir.join("relay/manifest.yml");
    std::fs::write(
        &manifest,
        "applications:\n- name: relay\n  routes:\n  - route: relay.localhost\n  services:\n  - sockets\n",
    )
    .unwrap();
    let relay_dir = dir.join("relay");
    let pushed = [
        "push",
        "-f",
        manifest.to_str().unwrap(),
        "-p",
        relay_dir.to_str().unwrap(),
    ];
    cf.ok(&pushed);
    let relayed = || {
        get(
            &listen,
            "relay.localhost",
            &format!("/?addr={upstream_addr}"),
            None,
        )
    };
    let (status, body) = relayed();
    assert_eq!(status, 502, "{body}");
    assert!(body.contains("AccessDenied"), "{body}");
    cf.ok(&["create-service", "wasi", "inherit-network", "net"]);
    cf.ok(&["bind-service", "relay", "net"]);
    cf.ok(&["restart", "relay"]);
    let (status, body) = relayed();
    assert_eq!(status, 200, "{body}");
    assert!(
        body.starts_with(&format!("relayed from {upstream_addr}\n"))
            && body.ends_with("upstream\n"),
        "{body}"
    );

    // a service broker, registered by an editor of them: its plans seen by none until
    // enabled; its operations at once or polled as it answers them, across a restart of the
    // cf-api; a binding's credentials in VCAP_SERVICES from the app's next start
    let broker = Broker::start("broker", "br0ker");
    let output = cf.fails(&[
        "create-service-broker",
        "db-broker",
        "broker",
        "wrong",
        &broker.url,
    ]);
    assert!(
        output.contains("Service broker error (status 401)"),
        "{output}"
    );
    cf.ok(&[
        "create-service-broker",
        "db-broker",
        "broker",
        "br0ker",
        &broker.url,
    ]);
    assert!(!cf.ok(&["marketplace"]).contains("db-broker"));
    let output = cf.fails(&["create-service", "db", "small", "db1"]);
    assert!(output.contains("Invalid service plan"), "{output}");
    cf.ok(&["enable-service-access", "db"]);
    let marketplace = cf.ok(&["marketplace", "-e", "db"]);
    assert!(
        marketplace.contains("broker: db-broker") && marketplace.contains("USD 9.50/MONTHLY"),
        "{marketplace}"
    );
    cf.ok(&[
        "create-service",
        "db",
        "small",
        "db1",
        "-c",
        r#"{"size":1}"#,
        "-t",
        "t1",
    ]);
    let provisioned = broker.provisioned();
    assert_eq!(provisioned["parameters"], json!({"size": 1}));
    assert_eq!(provisioned["context"]["instance_name"], "db1");
    assert!(
        broker.requests().iter().any(|(request, identity)| {
            request.starts_with("PUT /v2/service_instances/")
                && identity
                    .as_deref()
                    .is_some_and(|identity| identity.starts_with("cloudfoundry "))
        }),
        "{:?}",
        broker.requests()
    );
    assert_eq!(
        cf.ok(&["service", "db1", "--params"]).trim(),
        "{\n  \"size\": 1\n}"
    );
    cf.ok(&["bind-service", "hello", "db1"]);
    broker.hold(true);
    let output = cf.ok(&[
        "create-service",
        "db",
        "small",
        "db2",
        "-c",
        r#"{"async":true}"#,
    ]);
    assert!(output.contains("Create in progress"), "{output}");
    let shown = cf.ok(&["service", "db2"]);
    assert!(
        shown.contains("create in progress") && shown.contains("working on it"),
        "{shown}"
    );
    let output = cf.fails(&["bind-service", "hello", "db2"]);
    assert!(
        output.contains("An operation for service instance db2 is in progress"),
        "{output}"
    );
    drop(api);
    let (api, _) = with_rules(&two_wickets, &["email=*@example.com", "groups=devs"]);
    broker.hold(false);
    eventually("the instance made after the restart", || {
        cf.ok(&["service", "db2"]).contains("create succeeded")
    });
    let output = cf.ok(&[
        "bind-service",
        "hello",
        "db2",
        "-c",
        r#"{"async":true}"#,
        "--wait",
    ]);
    assert!(
        output.contains("Waiting for the operation to complete"),
        "{output}"
    );
    let vcap = || {
        let (status, vcap) = get(&listen, "hello.localhost", "/env/VCAP_SERVICES", None);
        assert_eq!(status, 200, "{vcap}");
        serde_json::from_str::<Value>(&vcap).unwrap()
    };
    assert!(vcap()["db"].is_null(), "until the next start");
    // (pushed again: a package left awaiting its upload above is its newest)
    cf.ok(&["push", "hello", "-p", dir.join("app").to_str().unwrap()]);
    let bound = vcap()["db"].as_array().cloned().unwrap();
    assert_eq!(bound.len(), 2, "{bound:?}");
    for (entry, name) in bound.iter().zip(["db1", "db2"]) {
        assert_eq!(entry["instance_name"], name);
        assert_eq!(entry["plan"], "small");
        assert_eq!(entry["credentials"]["password"], "s3cr3t-db");
        let binding = entry["binding_guid"].as_str().unwrap();
        assert_eq!(
            entry["credentials"]["uri"],
            format!("db://db.example.com/{binding}")
        );
    }
    assert_eq!(bound[0]["tags"], json!(["sql", "t1"]));
    // a new plan, taken once the broker made it; failures, at once and polled
    cf.ok(&[
        "update-service",
        "db1",
        "-p",
        "large",
        "-c",
        r#"{"async":true}"#,
        "--wait",
    ]);
    let shown = cf.ok(&["service", "db1"]);
    assert!(
        shown.contains("large") && shown.contains("update succeeded"),
        "{shown}"
    );
    let output = cf.fails(&[
        "create-service",
        "db",
        "small",
        "bad",
        "-c",
        r#"{"fail":"no capacity"}"#,
    ]);
    assert!(
        output.contains(
            "creating service instance bad failed: Service broker error (status 400): BadRequest: no capacity"
        ),
        "{output}"
    );
    let output = cf.fails(&[
        "create-service",
        "db",
        "small",
        "bad2",
        "-c",
        r#"{"async":true,"fail_async":"disk full"}"#,
        "--wait",
    ]);
    assert!(
        output.contains("creating service instance bad2 failed: disk full"),
        "{output}"
    );
    assert!(cf.ok(&["service", "bad"]).contains("create failed"));
    cf.ok(&["delete-service", "bad", "-f"]);
    cf.ok(&["purge-service-instance", "bad2", "-f"]);
    // unbound, and deleted, as the broker does it
    cf.ok(&["unbind-service", "hello", "db2", "--wait"]);
    cf.ok(&["delete-service", "db2", "-f", "--wait"]);
    assert!(!cf.ok(&["services"]).contains("db2"));
    assert_eq!(broker.instances().len(), 1);
    // a manifest's binding to it waits for its broker
    std::fs::write(
        &manifest,
        "applications:\n- name: relay\n  routes:\n  - route: relay.localhost\n  services:\n  - sockets\n  - name: db1\n    parameters:\n      async: true\n",
    )
    .unwrap();
    cf.ok(&pushed);
    let shown = cf.ok(&["service", "db1"]);
    assert!(
        shown.contains("relay") && !shown.contains("in progress"),
        "{shown}"
    );
    // a plan gone from its catalog stays while it has an instance, deactivated
    broker.offer(&["small"]);
    let output = cf.ok(&[
        "update-service-broker",
        "db-broker",
        "broker",
        "br0ker",
        &broker.url,
    ]);
    assert!(output.contains("the plans large are gone"), "{output}");
    assert!(cf.ok(&["marketplace", "-e", "db"]).contains("small"));
    let output = cf.fails(&["delete-service-broker", "db-broker", "-f"]);
    assert!(
        output.contains("associated service instances: db1"),
        "{output}"
    );

    // a static site: a directory of no component with an index.html, served by the site
    // guest from its files, read-only; its dot files never reach the wicket
    let site = dir.join("site");
    std::fs::create_dir_all(site.join("docs")).unwrap();
    for (name, body) in [
        ("index.html", "<h1>home</h1>"),
        ("docs/index.html", "<h1>docs</h1>"),
        ("style.css", "h1{}"),
        (".env", "SECRET=s3cr3t"),
    ] {
        std::fs::write(site.join(name), body).unwrap();
    }
    cf.ok(&["push", "site", "-p", site.to_str().unwrap()]);
    let page = |path: &str| get(&listen, "site.localhost", path, None);
    assert_eq!(page("/"), (200, "<h1>home</h1>".into()));
    assert_eq!(page("/docs/"), (200, "<h1>docs</h1>".into()));
    assert_eq!(page("/.env").0, 404);
    let kept: Vec<PathBuf> = std::fs::read_dir(dir.join("deployed/files"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(kept.len(), 1, "{kept:?}");
    assert!(kept[0].join("style.css").is_file() && !kept[0].join(".env").exists());
    let staged = cf.ok(&["logs", "site", "--recent"]);
    assert!(
        staged.contains("Staged site.wasm") && staged.contains("serving the files of a"),
        "{staged}"
    );
    // pushed again with a page changed, and a .wasm it serves: a site by the buildpack named,
    // one of the name of none refused
    std::fs::write(site.join("index.html"), "<h1>home, again</h1>").unwrap();
    std::fs::write(site.join("lib.wasm"), "for the browser").unwrap();
    let site_path = site.to_str().unwrap();
    // the staging log's last lines are in cf push's output: the CLI stops following it once
    // the build failed
    let refused = cf.fails(&[
        "push",
        "site",
        "-p",
        site_path,
        "-b",
        "staticfile_buildpack",
    ]);
    assert!(
        refused.contains("   Staging failed: there is no buildpack 'staticfile_buildpack'"),
        "{refused}"
    );
    assert_eq!(page("/"), (200, "<h1>home</h1>".into()));
    cf.ok(&["push", "site", "-p", site_path, "-b", "site"]);
    assert_eq!(page("/"), (200, "<h1>home, again</h1>".into()));
    assert_eq!(page("/lib.wasm"), (200, "for the browser".into()));
    let shown = cf.ok(&["app", "site"]);
    assert!(
        shown.contains("buildpacks:") && shown.contains("site "),
        "{shown}"
    );

    // relay's delete waits for its unbind, which the broker has polled: a restart of the cf-api
    // meanwhile takes it up
    let relay = cf.ok(&["app", "relay", "--guid"]).trim().to_string();
    broker.hold(true);
    cf.ok(&["curl", "-X", "DELETE", &format!("/v3/apps/{relay}")]);
    drop(api);
    let (api, _) = with_rules(&two_wickets, &["email=*@example.com", "groups=devs"]);
    broker.hold(false);
    eventually("relay deleted after the restart", || {
        !cf.ok(&["apps"]).contains("relay")
    });

    // gone from the wickets; their instances stay, unbound
    for app in ["hello", "counter", "img", "site"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    for (console, listen) in [(&console, &listen), (&console_b, &listen_b)] {
        let left = listing(console);
        assert!(left["apps"].as_array().unwrap().is_empty(), "{left}");
        assert_eq!(get(listen, "hello.localhost", "/", None).0, 404);
    }
    assert!(!cf.ok(&["apps"]).contains("hello"));
    let services = cf.ok(&["services"]);
    assert!(
        services.contains("sessions") && !services.contains("counter"),
        "{services}"
    );
    // the broker unbound them first (relay's polled); then the instance goes, and the broker
    assert!(broker.bindings().is_empty(), "{:?}", broker.bindings());
    cf.ok(&["delete-service", "db1", "-f", "--wait"]);
    cf.ok(&["delete-service-broker", "db-broker", "-f"]);
    assert!(broker.instances().is_empty());
    assert!(!cf.ok(&["service-brokers"]).contains("db-broker"));

    // alice no longer allowed (the rules changed): her next refresh is
    // refused, and her session with it
    drop(api);
    let (api, url) = with_rules(&two_wickets, &["email=*@example.com", "groups=ops"]);
    let (status, refused) = http.token(
        &url,
        &format!("grant_type=refresh_token&refresh_token={alice_refresh}"),
    );
    assert_eq!(status, 401, "{refused}");
    let why = refused["error_description"].as_str().unwrap();
    assert!(
        why.starts_with("alice is no longer allowed (the claim groups is [\"devs\"]"),
        "{why}"
    );

    drop(api);
    drop(wicket);
    drop(wicket_b);
    drop(flume);
    drop(provider);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A file of a layer the test pushes.
fn layer_file(tar: &mut tar::Builder<Vec<u8>>, path: &str, mode: u32, owner: u64, body: &[u8]) {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_mode(mode);
    header.set_uid(owner);
    header.set_gid(owner + 1);
    header.set_size(body.len() as u64);
    tar.append_data(&mut header, path, body).unwrap();
}

/// `build`'s tar, gzipped as `level` says.
fn layer_of(level: flate2::Compression, build: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
    let mut tar = tar::Builder::new(Vec::new());
    build(&mut tar);
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), level);
    gzip.write_all(&tar.into_inner().unwrap()).unwrap();
    gzip.finish().unwrap()
}

/// `build`'s tar, gzipped.
fn layer(build: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
    layer_of(flate2::Compression::fast(), build)
}

/// Firecracker and the guest kernel the wicket's VM test takes (its environment, else its
/// defaults); None where they are not, or `/dev/kvm` is not usable.
fn firecracker_tools() -> Option<(PathBuf, PathBuf)> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .ok()?;
    let firecracker = std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER")
        .map_or_else(|| PathBuf::from("firecracker"), PathBuf::from);
    let runs = Command::new(&firecracker).arg("--version").output();
    let kernel = std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL")
        .map(PathBuf::from)
        .or_else(|| {
            let home = PathBuf::from(std::env::var_os("HOME")?);
            Some(home.join(".local/firecracker/vmlinux"))
        })?;
    (runs.is_ok_and(|output| output.status.success()) && kernel.is_file())
        .then_some((firecracker, kernel))
}

#[test]
fn the_cf_cli_deploys_to_a_firecracker_wicket() {
    let Some((firecracker, kernel)) = firecracker_tools() else {
        eprintln!("skipped: needs a usable /dev/kvm, Firecracker and a guest kernel");
        return;
    };
    let wicket_dir =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../sluice-wicket-runtime-firecracker");
    cargo(&wicket_dir, &["build", "--locked"]);
    let wicket_binary = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| wicket_dir.join("target"))
        .join("debug/sluice-wicket-runtime-firecracker");
    let script = |script: &str, args: &[&Path]| {
        let output = Command::new(wicket_dir.join(script))
            .args(args)
            .env_remove("RUSTUP_TOOLCHAIN")
            .env_remove("CARGO")
            .env_remove("CARGO_TARGET_DIR")
            .current_dir(&wicket_dir)
            .output()
            .expect("run the wicket's script");
        assert!(
            output.status.success(),
            "{script}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    };
    let guest = wicket_dir.join(script("scripts/guest.sh", &[]));
    let sample = std::fs::read(guest.with_file_name("sample-process")).unwrap();

    let dir = std::env::temp_dir().join(format!("sluice-cf-api-fc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["rootfs", "cfhome"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    script("scripts/build-guest.sh", &[&dir.join("rootfs/guest.ext4")]);

    let registry = Registry::start("robot", "p4ss");
    let base = layer(|tar| {
        layer_file(
            tar,
            "etc/passwd",
            0o644,
            0,
            b"root:x:0:0::/root:/bin/sh\napp:x:1000:1001::/home/app:/bin/sh\n",
        );
    });
    let app = layer(|tar| {
        layer_file(tar, "app/serve", 0o755, 0, &sample);
        layer_file(tar, "app/data", 0o640, 1000, b"data");
        layer_file(tar, "app/tool", 0o4755, 0, b"tool");
    });
    let config = json!({
        "architecture": if cfg!(target_arch = "x86_64") { "amd64" } else { "arm64" },
        "os": "linux",
        "config": {
            "Entrypoint": ["/app/serve", "/app/data", "/app/tool"],
            "Env": ["IMAGE_VAR=from-image"],
            "User": "app",
            "WorkingDir": "/app",
        },
    });
    let manifest = registry.push_image("private/sample", "1", &config, &[base, app]);
    let image = format!("{}/private/sample:1", registry.address);
    // a builder of one buildpack, where there is a lifecycle
    let builder = cnb_lifecycle().map(|lifecycle| cnb_builder(&registry, &lifecycle, &guest));
    if builder.is_none() {
        eprintln!(
            "skipped: source staged by buildpacks needs the lifecycle and launcher binaries \
             (SLUICE_CNB_LIFECYCLE, else ~/.local/cnb/lifecycle)"
        );
    }
    let (mut cnb, mut uncredentialed) = (Vec::new(), Vec::new());
    if let Some((builder, run_image, _)) = &builder {
        uncredentialed.extend_from_slice(&["--cnb-builder", builder]);
        uncredentialed.extend_from_slice(&["--cnb-egress", "localhost:9"]);
        cnb.extend_from_slice(&uncredentialed);
        cnb.extend_from_slice(&["--cnb-builder-username", "robot", "--cnb-builder-password"]);
        cnb.extend_from_slice(&["p4ss", "--cnb-memory-mib", "512"]);
        cnb.extend_from_slice(&["--cnb-run-image", run_image]);
    }

    let flume = Process::spawn(
        &flume_binary(),
        &["--host", "127.0.0.1", "--port", "0", "--token", FLUME_TOKEN],
    );
    let flume_url = flume.logged("listening on ");
    // ports of its own, kept across the wicket's restart (the cf-api names its console)
    let free = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .unwrap()
            .to_string()
    };
    let (listen, console, api_listen) = (free(), free(), free());
    // a state directory each: a killed wicket's stays, and is refused
    let starts = AtomicUsize::new(0);
    // the wicket on `listen` of the console `console` and its directory `deployed`, its VMs'
    // /init `guest`
    let start_wicket_on = |listen: &str, console: &str, deployed: &str, guest: &Path| {
        let (firecracker, kernel, state) = (
            firecracker.to_str().unwrap(),
            kernel.to_str().unwrap(),
            dir.join(format!("state{}", starts.fetch_add(1, Ordering::Relaxed))),
        );
        let wicket = Process::spawn(
            &wicket_binary,
            &[
                "--kernel",
                kernel,
                "--firecracker",
                firecracker,
                "--state-dir",
                state.to_str().unwrap(),
                "--flume",
                &flume_url,
                "--flume-token",
                FLUME_TOKEN,
                "--listen",
                listen,
                "--console",
                console,
                "--console-token",
                TOKEN,
                "--console-dir",
                dir.join(deployed).to_str().unwrap(),
                "--guest",
                guest.to_str().unwrap(),
                "--watch-interval",
                "0",
            ],
        );
        wicket.logged("console on http://");
        wicket
    };
    let start_wicket = || start_wicket_on(&listen, &console, "deployed", &guest);
    let mut wicket = start_wicket();
    // an API taking any credentials names who edits the security groups by the name given
    let (api, url) = start_api(
        &api_listen,
        &[format!("http://{console}")],
        &flume_url,
        &dir.join("cf"),
        &[
            &[
                "--any-credentials",
                "--security-group-editor",
                "user_name=me",
            ],
            &cnb[..],
        ]
        .concat(),
    );
    let cf = Cf {
        cli: cf_cli(),
        home: dir.join("cfhome"),
    };
    cf.ok(&["api", &url]);
    cf.ok(&["login", "-u", "me", "-p", "x"]);

    // the image's credentials go to the wicket, which pulls it
    let refused = cf.fails(&["push", "fc", "--docker-image", &image]);
    assert!(
        refused.contains("the image does not exist, or it is private"),
        "{refused}"
    );
    let mut push = cf.command(&[
        "push",
        "fc",
        "--docker-image",
        &image,
        "--docker-username",
        "robot",
        "--no-start",
    ]);
    push.env("CF_DOCKER_PASSWORD", "p4ss");
    let (ok, output) = cf.output(push, "");
    assert!(ok, "{output}");
    cf.ok(&["set-env", "fc", "GREETING", "hello world"]);
    // services: a user-provided one, its credentials in VCAP_SERVICES
    cf.ok(&[
        "create-user-provided-service",
        "creds",
        "-p",
        r#"{"password":"s3cr3t"}"#,
    ]);
    cf.ok(&["bind-service", "fc", "creds"]);
    cf.ok(&["start", "fc"]);
    let shown = cf.ok(&["app", "fc"]);
    assert!(shown.contains(&format!("{image}@{manifest}")), "{shown}");

    // from inside the VM: the image's user, its files' owners and modes, the env, its
    // services
    let (status, body) = get(&listen, "fc.localhost", "/", None);
    assert_eq!(status, 200, "{body}");
    for expected in [
        "uid 1000 gid 1001\n",
        // its output opened by path, as an image's logs linked to /dev/stderr are
        "/dev/stdout: opened\n",
        "/dev/stderr: opened\n",
        "/app/data uid 1000 gid 1001 mode 640\n",
        "/app/tool uid 0 gid 1 mode 4755\n",
        "GREETING=hello world\n",
        "HOME=/home/app\n",
        "IMAGE_VAR=from-image\n",
        "PORT=8080\n",
        "VCAP_APPLICATION={",
        "VCAP_SERVICES={\"user-provided\":[",
        "\"credentials\":{\"password\":\"s3cr3t\"}",
    ] {
        assert!(body.contains(expected), "{expected}: {body}");
    }
    let listed = listing(&console);
    let fc = &listed["apps"][0];
    assert_eq!(listed["runtime"], "firecracker");
    assert_eq!(fc["hosts"], json!(["fc.localhost"]));
    // cloned from a snapshot at any count: a scale keeps what runs
    assert_eq!(
        (&fc["sandbox"]["mode"], &fc["sandbox"]["instances"]),
        (&"pool".into(), &1.into())
    );
    // as Cloud Controller's defaults: what a JVM needs of memory and of time to listen
    assert_eq!(fc["sandbox"]["memory-mib"], 1024);
    assert_eq!(fc["sandbox"]["boot-ms"], 60_000);
    assert!(
        fc["sandbox"]["env"]
            .as_array()
            .unwrap()
            .contains(&"GREETING".into()),
        "{fc}"
    );
    let sha256 = fc["component"]["sha256"].as_str().unwrap().to_string();
    assert_eq!(fc["component"]["name"], "sample.ext4");

    // the process's output, from the flume (this wicket feeds no requests)
    let deadline = Instant::now() + HANG;
    loop {
        assert_eq!(get(&listen, "fc.localhost", "/log", None).0, 200);
        if cf
            .ok(&["logs", "fc", "--recent"])
            .contains("[APP/PROC/WEB/0] OUT GET /log HTTP/1.0")
        {
            break;
        }
        assert!(Instant::now() < deadline, "the app's lines never came");
        std::thread::sleep(Duration::from_millis(500));
    }

    // the app connects through the security groups, at its next start. A rule's text is 18
    // bytes here, a range's: its rules fill what a guest's kernel command line takes
    let destination = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = destination.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in destination.incoming().flatten() {
            let mut stream = stream;
            let mut got = String::new();
            let _ = stream.read_to_string(&mut got);
            let _ = stream.write_all(b"pong");
        }
    });
    let connects = |to: &str| {
        let path = format!("/egress?to={to}");
        let (status, answer) = get(&listen, "fc.localhost", &path, None);
        assert_eq!(status, 200, "{answer}");
        answer.ends_with("answer: pong\n")
    };
    let egress = || listing(&console)["apps"][0]["sandbox"]["egress"].clone();
    let by_name = format!("localhost:{port}");
    assert!(!connects(&by_name));
    let rules = json!([
        {"protocol": "tcp", "destination": "localhost", "ports": port.to_string()},
        {"protocol": "tcp", "destination": "db.invalid", "ports": port.to_string()},
        {"protocol": "tcp", "destination": "127.0.0.2-127.0.0.3",
            "ports": format!("{}-{port}", port - 51), "description": "a port range"},
    ]);
    let group = security_group_file(&dir, "web", &rules);
    cf.ok(&["create-security-group", "web", &group]);
    cf.ok(&["bind-security-group", "web", "sluice", "--space", "wicket"]);
    assert!(!connects(&by_name), "before a restart");
    cf.ok(&["restart", "fc"]);
    let listed = egress();
    let rule_count = listed.as_array().unwrap().len();
    assert_eq!(rule_count, 54, "{listed}");
    let texts: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|rule| rule.as_str().unwrap())
        .collect();
    assert!(texts.join(",").len() > 1000, "near the bound: {listed}");
    assert!(
        texts.contains(&format!("127.0.0.2/31:{port}").as_str()),
        "{listed}"
    );
    assert!(connects(&by_name));
    assert!(
        connects(&format!("127.0.0.3:{port}")),
        "a range's last port"
    );
    assert!(!connects(&format!("127.0.0.4:{port}")), "not in the rules");
    assert!(
        cf.ok(&["logs", "fc", "--recent"])
            .contains(&format!("guest: egress to db.invalid:{port} at 127.1.0.1")),
        "a name of its own in the guest"
    );

    // the same bound by the org's global group; staging's have no effect, no VM stages
    cf.ok(&["unbind-security-group", "web", "sluice", "wicket"]);
    cf.ok(&["bind-running-security-group", "web"]);
    cf.ok(&["restart", "fc"]);
    assert_eq!(egress(), listed);
    assert!(connects(&by_name));
    cf.ok(&["unbind-running-security-group", "web"]);
    cf.ok(&["bind-staging-security-group", "web"]);
    cf.ok(&["restart", "fc"]);
    assert_eq!(egress(), Value::Null);
    assert!(!connects(&by_name));
    cf.ok(&["unbind-staging-security-group", "web"]);

    // what the wicket cannot do is refused: a protocol, ports past a guest's command line
    // at the edit; groups past it together at the start, naming them; the process's own
    // port, which only /init knows, by /init's starting no process, its line in the logs
    let other = security_group_file(
        &dir,
        "udp",
        &json!([{"protocol": "udp", "destination": "10.0.0.5", "ports": "53"}]),
    );
    let refused = cf.fails(&["create-security-group", "other", &other]);
    assert!(refused.contains("the wicket relays tcp alone"), "{refused}");
    let every_port = security_group_file(
        &dir,
        "every-port",
        &json!([{"protocol": "tcp", "destination": "0.0.0.0/0", "ports": "1-65535"}]),
    );
    let refused = cf.fails(&["create-security-group", "other", &every_port]);
    assert!(refused.contains("a guest takes 1024 bytes"), "{refused}");
    let more = security_group_file(
        &dir,
        "more",
        &json!([{"protocol": "tcp", "destination": "127.0.0.4-127.0.0.5",
            "ports": format!("{}-{port}", port - 51)}]),
    );
    cf.ok(&["create-security-group", "more", &more]);
    cf.ok(&["bind-running-security-group", "web"]);
    cf.ok(&["bind-running-security-group", "more"]);
    let refused = cf.fails(&["restart", "fc"]);
    assert!(
        refused.contains("a guest's kernel command line takes 1024"),
        "{refused}"
    );
    cf.ok(&["delete-security-group", "more", "-f"]);
    let own = security_group_file(
        &dir,
        "own",
        &json!([{"protocol": "tcp", "destination": "10.0.0.0/8", "ports": "8080"}]),
    );
    cf.ok(&["update-security-group", "web", &own]);
    cf.fails(&["restart", "fc"]);
    let logs = cf.ok(&["logs", "fc", "--recent"]);
    assert!(
        logs.contains("port 8080 is the process's own (PORT): it cannot listen there"),
        "{logs}"
    );
    cf.ok(&["delete-security-group", "web", "-f"]);
    cf.ok(&["restart", "fc"]);
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200);

    // a process ending: the wicket starts its VM again by itself
    assert_eq!(get(&listen, "fc.localhost", "/exit", None).0, 200);
    let deadline = Instant::now() + HANG;
    loop {
        let logs = cf.ok(&["logs", "fc", "--recent"]);
        if logs.contains("exit status: 83")
            && fetch(&listen, "fc.localhost", "/", None).is_some_and(|got| got.0 == 200)
        {
            break;
        }
        assert!(Instant::now() < deadline, "no process again: {logs}");
        std::thread::sleep(Duration::from_millis(100));
    }

    // a process exiting before it listens is refused at once, not at the health check's
    // timeout (60s), its output in the logs; a push over the crashed app goes through
    cf.ok(&["set-env", "fc", "SAMPLE_EXIT", "1"]);
    let started = Instant::now();
    cf.fails(&["restart", "fc"]);
    assert!(started.elapsed() < Duration::from_secs(30), "not at once");
    let logs = cf.ok(&["logs", "fc", "--recent"]);
    assert!(logs.contains("sample process: exiting as told"), "{logs}");
    assert!(logs.contains("exited before it listened"), "{logs}");
    cf.ok(&["unset-env", "fc", "SAMPLE_EXIT"]);
    let pushed_as = |app: &'static str, extra: &[&str]| {
        let mut args = vec![
            "push",
            app,
            "--docker-image",
            &image,
            "--docker-username",
            "robot",
        ];
        args.extend_from_slice(extra);
        let mut push = cf.command(&args);
        push.env("CF_DOCKER_PASSWORD", "p4ss");
        cf.output(push, "")
    };
    let pushed = |extra: &[&str]| pushed_as("fc", extra);
    let (ok, output) = pushed(&["-m", "512M"]);
    assert!(ok, "{output}");
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["memory-mib"], 512);
    // cf push -t is how long it may take to listen
    cf.ok(&["set-env", "fc", "SAMPLE_WAIT_MS", "3000"]);
    let (ok, output) = pushed(&["-t", "1"]);
    assert!(!ok, "{output}");
    assert!(output.contains("within 1s"), "{output}");
    let (ok, output) = pushed(&["-t", "20"]);
    assert!(ok, "{output}");
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["boot-ms"], 20_000);
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200);
    cf.ok(&["unset-env", "fc", "SAMPLE_WAIT_MS"]);
    // cf set-health-check, taken at the restart: an http one the route's liveness check, its VMs
    // up once the endpoint answered 200 and probed from then on
    let set = ["set-health-check", "fc", "http", "--endpoint", "/ready"];
    cf.ok(&[&set[..], &["--invocation-timeout", "2"]].concat());
    cf.ok(&["restart", "fc"]);
    let shown = cf.ok(&["get-health-check", "fc"]);
    let web = shown.lines().find(|line| line.starts_with("web"));
    let web: Vec<&str> = web.unwrap_or_default().split_whitespace().collect();
    assert_eq!(web, ["web", "http", "/ready", "2"], "{shown}");
    let apps = listing(&console)["apps"].as_array().unwrap().clone();
    let listed = apps
        .iter()
        .find(|app| app["hosts"] == json!(["fc.localhost"]));
    let sandbox = &listed.expect("the app listed")["sandbox"];
    assert_eq!(
        (
            &sandbox["liveness"],
            &sandbox["liveness-endpoint"],
            &sandbox["liveness-timeout-ms"],
            &sandbox["boot-ms"]
        ),
        (
            &json!("http"),
            &json!("/ready"),
            &json!(2000),
            &json!(20_000)
        ),
        "{sandbox}"
    );
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200);
    // port again at the next start, the timeout kept
    cf.ok(&["set-health-check", "fc", "port"]);
    let web = format!(
        "/v3/apps/{}/processes/web",
        cf.ok(&["app", "fc", "--guid"]).trim()
    );
    let web: Value = serde_json::from_str(&cf.ok(&["curl", &web])).unwrap();
    let check = &web["health_check"];
    assert_eq!(
        (
            &check["type"],
            &check["data"]["timeout"],
            &check["data"]["endpoint"]
        ),
        (&json!("port"), &json!(20), &Value::Null),
        "{web}"
    );

    // a readiness check, the manifest's alone (the CLI has no command setting it): kept, shown,
    // the route's options; the instance running, not routable until its endpoint answers 200,
    // each of its first 40 checks answered 503
    let manifest = dir.join("ready.yml");
    let ready = format!(
        "applications:\n- name: ready\n  docker:\n    image: {image}\n    username: robot\n  \
         routes:\n  - route: ready.localhost\n  readiness-health-check-type: http\n  \
         readiness-health-check-http-endpoint: /ready?after=40\n  \
         readiness-health-check-interval: 1\n"
    );
    std::fs::write(&manifest, ready).unwrap();
    let mut push = cf.command(&["push", "-f", manifest.to_str().unwrap()]);
    push.env("CF_DOCKER_PASSWORD", "p4ss");
    let (ok, output) = cf.output(push, "");
    assert!(ok, "{output}");
    let stats = format!(
        "/v3/apps/{}/processes/web/stats",
        cf.ok(&["app", "ready", "--guid"]).trim()
    );
    let routable = || {
        let stats: Value = serde_json::from_str(&cf.ok(&["curl", &stats])).unwrap();
        let entry = &stats["resources"][0];
        (entry["state"].clone(), entry["routable"].clone())
    };
    assert_eq!(routable(), (json!("RUNNING"), json!(false)));
    let shown = cf.ok(&["get-readiness-health-check", "ready"]);
    let web = shown.lines().find(|line| line.starts_with("web"));
    let web: Vec<&str> = web.unwrap_or_default().split_whitespace().collect();
    assert_eq!(web, ["web", "http", "/ready?after=40", "1"], "{shown}");
    let apps = listing(&console)["apps"].as_array().unwrap().clone();
    let listed = apps
        .iter()
        .find(|app| app["hosts"] == json!(["ready.localhost"]));
    let sandbox = &listed.expect("the app listed")["sandbox"];
    assert_eq!(
        (
            &sandbox["readiness"],
            &sandbox["readiness-endpoint"],
            &sandbox["readiness-interval-ms"]
        ),
        (&json!("http"), &json!("/ready?after=40"), &json!(1000)),
        "{sandbox}"
    );
    let deadline = Instant::now() + HANG;
    while routable() != (json!("RUNNING"), json!(true)) {
        assert!(
            Instant::now() < deadline,
            "never routable: {:?}",
            routable()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(get(&listen, "ready.localhost", "/", None).0, 200);
    cf.ok(&["delete", "ready", "-f"]);

    let shows = |app: &str, state: &str| {
        let deadline = Instant::now() + HANG;
        loop {
            let shown = cf.ok(&["app", app]);
            if shown
                .lines()
                .any(|line| line.starts_with("#0") && line.contains(state))
            {
                return;
            }
            assert!(Instant::now() < deadline, "{app} is not {state}: {shown}");
            std::thread::sleep(Duration::from_millis(500));
        }
    };

    // a process that lives and does not listen takes its start the health check's timeout, and
    // no other app's edit waits for it; the start's client giving up, its outcome still shows
    let (ok, output) = pushed_as("slow", &["-t", "40", "--no-start"]);
    assert!(ok, "{output}");
    cf.ok(&["set-env", "slow", "SAMPLE_WAIT_MS", "600000"]);
    let mut starting = cf
        .command(&["start", "slow"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let waiting = || {
        cf.ok(&["logs", "slow", "--recent"])
            .matches("sample process: waiting")
            .count()
    };
    let deadline = Instant::now() + HANG;
    while waiting() == 0 {
        assert!(Instant::now() < deadline, "the process never started");
        std::thread::sleep(Duration::from_millis(200));
    }
    cf.ok(&["set-env", "fc", "OTHER", "edit"]);
    cf.ok(&["restart", "fc"]);
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200);
    assert!(
        starting.try_wait().unwrap().is_none(),
        "the other app's edits waited for its start"
    );
    starting.kill().unwrap();
    starting.wait().unwrap();
    shows("slow", "crashed");
    let logs = cf.ok(&["logs", "slow", "--recent"]);
    assert!(logs.contains("within 40s"), "{logs}");
    // the wicket's revision moving, it is not tried again: nothing it depends on changed
    cf.ok(&["unset-env", "fc", "OTHER"]);
    cf.ok(&["restart", "fc"]);
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(waiting(), 1, "tried again");
    cf.ok(&["delete", "slow", "-f"]);

    // the wicket restarted, back at the revision it had: the app shows starting while the
    // wicket is down, and running again once it is back
    wicket.kill();
    drop(wicket);
    shows("fc", "starting");
    wicket = start_wicket();
    shows("fc", "running");

    // a scale touches no VM it keeps: their snapshot, the requests they served, the target
    let served_by = |body: &str| {
        let line = |name: &str| {
            let line = body.lines().find_map(|line| line.strip_prefix(name));
            line.unwrap_or_else(|| panic!("no {name} in {body}"))
                .to_string()
        };
        (line("boot id: "), line("served: ").parse::<u64>().unwrap())
    };
    let answered_by = || {
        let (status, body) = get(&listen, "fc.localhost", "/", None);
        assert_eq!(status, 200, "{body}");
        served_by(&body)
    };
    let (boot_id, count) = answered_by();
    let target = || {
        let (status, body) = get(&console, &console, "/api/routes", Some(TOKEN));
        assert_eq!(status, 200, "{body}");
        let routes: Value = serde_json::from_str(&body).unwrap();
        (
            routes["generation"].clone(),
            routes["routes"][0]["target"].clone(),
        )
    };
    let advertised = target();

    // two instances, up to three and back: VMs cloned from a snapshot, env and all
    cf.ok(&["scale", "fc", "-i", "2"]);
    let scaled = &listing(&console)["apps"][0]["sandbox"];
    assert_eq!(
        (&scaled["mode"], &scaled["instances"]),
        (&"pool".into(), &2.into())
    );
    assert_eq!(target(), advertised, "nothing to advertise");
    instances_come_to(&cf, "fc", "2/2");
    // the first VM counts on, the second counts from 1
    let deadline = Instant::now() + HANG;
    let mut counts = std::collections::BTreeSet::new();
    while !(counts.contains(&1) && counts.iter().any(|served| *served > count)) {
        let (of, served) = answered_by();
        assert_eq!(of, boot_id, "another snapshot");
        counts.insert(served);
        assert!(Instant::now() < deadline, "{count}: {counts:?}");
    }
    let (status, body) = get(&listen, "fc.localhost", "/", None);
    assert!(
        status == 200 && body.contains("GREETING=hello world\n"),
        "{status}: {body}"
    );
    // the second VM takes its turn: its index in the output, one `cf app` shows
    let deadline = Instant::now() + HANG;
    loop {
        assert_eq!(get(&listen, "fc.localhost", "/log", None).0, 200);
        let recent = cf.ok(&["logs", "fc", "--recent"]);
        if recent.contains("[APP/PROC/WEB/1] OUT GET /log HTTP/1.0") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "instance 1 never served: {recent}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    let shown = cf.ok(&["app", "fc"]);
    assert!(shown.contains("#1 "), "{shown}");
    cf.ok(&["scale", "fc", "-i", "3"]);
    let deadline = Instant::now() + HANG;
    loop {
        assert_eq!(answered_by().0, boot_id);
        assert_eq!(get(&listen, "fc.localhost", "/log", None).0, 200);
        let recent = cf.ok(&["logs", "fc", "--recent"]);
        if recent.contains("[APP/PROC/WEB/2] OUT GET /log HTTP/1.0") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "instance 2 never served: {recent}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    instances_come_to(&cf, "fc", "3/3");
    // the last one past the limit: no request reaches it, whatever it keeps of its time, and it
    // is named as one of those within it while it is there
    cf.ok(&["scale", "fc", "-i", "2"]);
    assert_eq!(instances_shown(&cf, "fc"), "2/2");
    assert_eq!(target(), advertised);
    for _ in 0..6 {
        assert_eq!(get(&listen, "fc.localhost", "/after-scale", None).0, 200);
    }
    let deadline = Instant::now() + HANG;
    loop {
        let recent = cf.ok(&["logs", "fc", "--recent"]);
        let lines = recent.matches("OUT GET /after-scale HTTP/1.0").count();
        assert!(
            !recent.contains("[APP/PROC/WEB/2] OUT GET /after-scale"),
            "{recent}"
        );
        if lines == 6 {
            break;
        }
        assert!(Instant::now() < deadline, "the lines never came: {recent}");
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(answered_by().0, boot_id);

    // `cf scale` the cap of VMs, the annotations how long one lives: another mode from the next
    // start
    let sandbox = || {
        let listed = listing(&console);
        let apps = listed["apps"].as_array().unwrap();
        let fc = apps.iter().find(|app| app["hosts"][0] == "fc.localhost");
        fc.map(|fc| fc["sandbox"].clone())
    };
    // a count of the route's VMs, as the wicket's console lists them
    let vms = |count: &str| {
        let (status, body) = get(&console, &console, "/api/routes", Some(TOKEN));
        assert_eq!(status, 200, "{body}");
        let routes: Value = serde_json::from_str(&body).unwrap();
        let routes = routes["routes"].as_array().unwrap().clone();
        let fc = routes.iter().find(|route| route["host"] == "fc.localhost");
        fc.unwrap()["instances"][count].as_u64().unwrap()
    };
    let alive = || vms("alive");
    let scales_to_zero = |what: &str| {
        assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200, "{what}");
        let deadline = Instant::now() + HANG;
        while alive() > 0 {
            assert!(Instant::now() < deadline, "{what}: VMs stay");
            std::thread::sleep(Duration::from_millis(200));
        }
        // none alive: none listed, which nothing waiting for a start takes for a failure, and
        // the VM of a connection while it is there
        assert_eq!(instances_shown(&cf, "fc"), "0/0", "{what}");
        cf.ok(&["restart", "fc"]);
        cf.ok(&["restart", "fc", "--strategy", "rolling"]);
        let holding = held(&listen, "fc.localhost", "/");
        shows("fc", "running");
        assert_eq!(instances_shown(&cf, "fc"), "1/1", "{what}");
        drop(holding);
        instances_come_to(&cf, "fc", "0/0");
    };
    let reuse = json!({
        "wicket.sluice.ik.am/mode": "reuse",
        "wicket.sluice.ik.am/keep-ms": "500",
        "wicket.sluice.ik.am/uses": "2",
        "wicket.sluice.ik.am/lifetime-ms": "60000",
    });
    assert!(annotate(&cf, "fc", &reuse).contains("\"guid\""));
    assert_eq!(sandbox().unwrap()["mode"], "pool", "until the next start");
    let recent = cf.ok(&["logs", "fc", "--recent"]);
    assert!(
        recent.contains("annotations wait for the next start"),
        "{recent}"
    );
    cf.ok(&["restart", "fc"]);
    let fresh = sandbox().unwrap();
    assert_eq!(
        (
            &fresh["mode"],
            &fresh["instances"],
            &fresh["keep-ms"],
            &fresh["uses"],
            &fresh["lifetime-ms"]
        ),
        (
            &"reuse".into(),
            &2.into(),
            &500.into(),
            &2.into(),
            &60_000.into()
        ),
    );
    scales_to_zero("reuse");
    // a pool keeping none, as annotated: a `keep-ms` alone is refused below
    let pool = json!({
        "wicket.sluice.ik.am/mode": "pool",
        "wicket.sluice.ik.am/warm": "0",
        "wicket.sluice.ik.am/concurrency": "3",
        "wicket.sluice.ik.am/uses": null,
        "wicket.sluice.ik.am/lifetime-ms": null,
    });
    assert!(annotate(&cf, "fc", &pool).contains("\"guid\""));
    cf.ok(&["restart", "fc"]);
    let fresh = sandbox().unwrap();
    assert_eq!(
        (
            &fresh["mode"],
            &fresh["instances"],
            &fresh["warm"],
            &fresh["keep-ms"],
            &fresh["concurrency"]
        ),
        (&"pool".into(), &2.into(), &0.into(), &500.into(), &3.into()),
    );
    scales_to_zero("pool");
    // what a mode does not take is refused, as what is no mode and no number
    for (annotation, reason) in [
        (json!({"wicket.sluice.ik.am/uses": "2"}), "uses"),
        (json!({"wicket.sluice.ik.am/mode": "fastest"}), "no mode"),
        (json!({"wicket.sluice.ik.am/keep-ms": "soon"}), "no number"),
        (json!({"wicket.sluice.ik.am/warm": "3"}), "above"),
        (json!({"wicket.sluice.ik.am/warm": null}), "/warm"),
        (
            json!({"wicket.sluice.ik.am/budget-ms": "5000"}),
            "budget-ms",
        ),
        (json!({"wicket.sluice.ik.am/concurrency": "0"}), "no number"),
        (json!({"wicket.sluice.ik.am/instances": "2"}), "cf scale -i"),
        (
            json!({"wicket.sluice.ik.am/mode": "per-connection", "wicket.sluice.ik.am/keep-ms": null}),
            "concurrency",
        ),
        (
            json!({"wicket.sluice.ik.am/mode": "shared", "wicket.sluice.ik.am/warm": null,
                "wicket.sluice.ik.am/keep-ms": null, "wicket.sluice.ik.am/concurrency": null}),
            "no mode",
        ),
        (json!({"wicket.sluice.ik.am/mode": "boot"}), "does not take"),
    ] {
        let refused = annotate(&cf, "fc", &annotation);
        assert!(refused.contains(reason), "{annotation}: {refused}");
    }
    // a manifest meant for the other wicket is refused at the push, not at the start after it
    let manifest = dir.join("budget.yml");
    let budget = "applications:\n- name: fc\n  metadata:\n    annotations:\n      wicket.sluice.ik.am/budget-ms: 5000\n";
    std::fs::write(&manifest, budget).unwrap();
    let refused = cf.fails(&["apply-manifest", "-f", manifest.to_str().unwrap()]);
    assert!(refused.contains("no components"), "{refused}");
    // how many and how long at once, the wicket adjusting its route in place
    let adjusted = json!({"wicket.sluice.ik.am/warm": "2", "wicket.sluice.ik.am/concurrency": "2"});
    assert!(annotate(&cf, "fc", &adjusted).contains("\"guid\""));
    assert_eq!(sandbox().unwrap()["concurrency"], 2);
    // a manifest's count below what the running mode keeps warm waits with another mode for the
    // next start, the wicket's route as it is: `cf push` restarts the app right after
    let manifest = dir.join("scaled.yml");
    let scaled = "applications:\n- name: fc\n  instances: 1\n  metadata:\n    annotations:\n      wicket.sluice.ik.am/mode: boot\n      wicket.sluice.ik.am/warm: null\n      wicket.sluice.ik.am/keep-ms: null\n      wicket.sluice.ik.am/concurrency: null\n";
    std::fs::write(&manifest, scaled).unwrap();
    cf.ok(&["apply-manifest", "-f", manifest.to_str().unwrap()]);
    assert_eq!(sandbox().unwrap()["instances"], 2);
    let recent = cf.ok(&["logs", "fc", "--recent"]);
    assert!(
        recent.contains("instances wait for the next start"),
        "{recent}"
    );
    // booted: a VM of its own per instance, none restored. A scale takes what runs: up, it returns
    // without waiting for the boot, `cf app` listing what is alive; one VM's end is CRASHED at its
    // index alone, the others serving on, until another one is up there
    cf.ok(&["restart", "fc"]);
    assert_eq!(sandbox().unwrap()["mode"], "boot");
    assert_eq!(
        sandbox().unwrap().get("instances"),
        None,
        "one, its default"
    );
    assert_eq!(instances_shown(&cf, "fc"), "1/1");
    let advertised_booted = target();
    let scaled = cf.ok(&["scale", "fc", "-i", "3"]);
    assert!(
        scaled
            .lines()
            .any(|line| line.starts_with("instances:") && line.ends_with("/3")),
        "{scaled}"
    );
    assert_eq!(sandbox().unwrap()["instances"], 3);
    assert_eq!(target(), advertised_booted, "nothing to advertise");
    instances_come_to(&cf, "fc", "3/3");
    let boot_ids: std::collections::BTreeSet<_> = (0..12).map(|_| answered_by().0).collect();
    assert_eq!(boot_ids.len(), 3, "{boot_ids:?}");
    // the first start in its place is at once, a boot long: again until `cf app` sees one, each
    // next one after a longer wait. Each next exit once the last one's VM is up again: one whose
    // end the wicket has yet to see takes a connection only to cut it
    let deadline = Instant::now() + HANG;
    let crashed = 'exits: loop {
        let restarts = vms("restarts");
        assert_eq!(get(&listen, "fc.localhost", "/exit", None).0, 200);
        while vms("restarts") == restarts {
            let shown = cf.ok(&["app", "fc"]);
            if shown.contains("crashed") {
                break 'exits shown;
            }
            assert!(Instant::now() < deadline, "no crashed instance: {shown}");
        }
    };
    assert_eq!(crashed.matches("crashed").count(), 1, "{crashed}");
    assert_eq!(crashed.matches("running").count(), 2, "{crashed}");
    assert!(crashed.contains("its process ended"), "{crashed}");
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200, "{crashed}");
    instances_come_to(&cf, "fc", "3/3");
    cf.ok(&["scale", "fc", "-i", "1"]);
    instances_come_to(&cf, "fc", "1/1");
    assert_eq!(sandbox().unwrap().get("instances"), None);
    // scaling as it runs: its annotation gone, what runs stays booted until the next start
    annotate(&cf, "fc", &json!({"wicket.sluice.ik.am/mode": null}));
    cf.ok(&["scale", "fc", "-i", "2"]);
    assert_eq!(sandbox().unwrap()["mode"], "boot");
    instances_come_to(&cf, "fc", "2/2");
    cf.ok(&["scale", "fc", "-i", "1"]);
    // per connection, from a cap of one
    annotate(
        &cf,
        "fc",
        &json!({"wicket.sluice.ik.am/mode": "per-connection"}),
    );
    cf.ok(&["restart", "fc"]);
    let fresh = sandbox().unwrap();
    assert_eq!(
        (&fresh["mode"], &fresh["instances"]),
        (&"per-connection".into(), &1.into())
    );
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200);
    let deadline = Instant::now() + HANG;
    while alive() > 0 {
        assert!(Instant::now() < deadline, "its VM outlives its connection");
        std::thread::sleep(Duration::from_millis(200));
    }
    // its one VM a connection's: the next, finding no room, is told why
    let holding = held(&listen, "fc.localhost", "/");
    let (status, body) = get(&listen, "fc.localhost", "/", None);
    assert_eq!(status, 502, "{body}");
    let per_connection = "a VM per connection";
    assert!(body.contains(per_connection), "{body}");
    drop(holding);
    // scaled to 0: nothing runs, nothing serves, as stopped; the cap back, it serves again,
    // the scale saying what that count is
    let scaled = cf.ok(&["scale", "fc", "-i", "0"]);
    assert!(!scaled.contains(per_connection), "{scaled}");
    assert_eq!(sandbox(), None);
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 404);
    assert_eq!(instances_shown(&cf, "fc"), "0/0");
    let scaled = cf.ok(&["scale", "fc", "-i", "1"]);
    assert!(scaled.contains(per_connection), "{scaled}");
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 200);
    // none annotated: a pool of a fixed size, as above
    annotate(&cf, "fc", &json!({"wicket.sluice.ik.am/mode": null}));
    cf.ok(&["restart", "fc"]);
    assert_eq!(sandbox().unwrap()["mode"], "pool");

    // a rootfs in a zip: uploaded by the cf-api, as a component is
    cf.ok(&["push", "zipped", "-p", dir.join("rootfs").to_str().unwrap()]);
    let (status, body) = get(&listen, "zipped.localhost", "/env", None);
    assert!(
        status == 200 && body.contains("VCAP_APPLICATION={"),
        "{status}: {body}"
    );
    // the stack names what the wicket runs
    let shown = cf.ok(&["app", "zipped"]);
    assert!(
        shown
            .lines()
            .any(|line| line.starts_with("stack:") && line.ends_with("firecracker")),
        "{shown}"
    );
    assert!(cf.ok(&["stacks"]).contains("firecracker "));
    // a push says what the count is, as a scale does
    let manifest = dir.join("reused.yml");
    let reused = "applications:\n- name: zipped\n  metadata:\n    annotations:\n      wicket.sluice.ik.am/mode: reuse\n";
    std::fs::write(&manifest, reused).unwrap();
    let pushed = cf.ok(&[
        "push",
        "-f",
        manifest.to_str().unwrap(),
        "-p",
        dir.join("rootfs").to_str().unwrap(),
    ]);
    assert!(pushed.contains(per_connection), "{pushed}");

    // source: staged by the builder's buildpacks in a VM of the wicket, with the app's
    // environment, the build's output the staging log; what it built runs on the run image
    // named in the builder's (--cnb-run-image), as its user, the launcher starting the process
    // the buildpack named
    if let Some((_, run_image, image)) = &builder {
        use std::os::unix::fs::PermissionsExt as _;
        let source = dir.join("source");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("sample.app"), b"an app").unwrap();
        let mode = std::fs::Permissions::from_mode(0o750);
        std::fs::set_permissions(source.join("sample.app"), mode).unwrap();
        let manifest = dir.join("src.yml");
        let staged = "applications:\n- name: src\n  env:\n    STAGED_VAR: from-the-manifest\n";
        std::fs::write(&manifest, staged).unwrap();
        let (manifest, source) = (manifest.to_str().unwrap(), source.to_str().unwrap());
        let pushed = cf.ok(&["push", "-f", manifest, "-p", source]);
        let (status, body) = get(&listen, "src.localhost", "/", None);
        assert_eq!(status, 200, "{body}\n{pushed}");
        for expected in [
            "uid 3000 gid 3001\n",
            // the launcher's doing (it takes CNB_APP_DIR for itself): the launch layer's bin
            "PATH=/layers/sample_buildpack/runtime/bin",
            "STAGED_VAR=from-the-manifest\n",
            "VCAP_APPLICATION={",
        ] {
            assert!(body.contains(expected), "{expected}: {body}");
        }
        for (path, mode) in [("sample.app", " mode 750"), ("built.txt", "")] {
            let seen = format!("/workspace/{path} uid ");
            let line = body.lines().find(|line| line.starts_with(&seen));
            assert!(
                line.is_some_and(|line| line.ends_with(mode)),
                "{path}: {body}"
            );
        }
        let logs = cf.ok(&["logs", "src", "--recent"]);
        for expected in [
            "[STG/0] OUT sample buildpack: building as 1001:1000, STAGED_VAR=from-the-manifest",
            "sample buildpack: a line on stderr",
            "sample buildpack: build 1 of the cache, which kept ''",
            // the app's memory, 1G by default, over --cnb-memory-mib
            "1024 MiB, 8192 MiB of disk, 2 vCPUs, reaching localhost:9",
            "the buildpack cnb",
            "for the run image ",
        ] {
            assert!(logs.contains(expected), "{expected}: {logs}");
        }
        // built again by the buildpack named: on the cache of the last build, the wicket's
        let pushed = cf.ok(&[
            "push",
            "-f",
            manifest,
            "-p",
            source,
            "-b",
            "sample/buildpack",
        ]);
        for expected in [
            "(its buildpacks sample/buildpack)",
            "Restoring a cache of ",
            "Restoring data for \"sample_buildpack:cache\" from cache",
            "sample buildpack: build 2 of the cache, which kept 'not the droplet's'",
            "KiB for the next build",
        ] {
            assert!(pushed.contains(expected), "{expected}: {pushed}");
        }
        let guid = cf.ok(&["app", "src", "--guid"]).trim().to_string();
        let cache = dir.join(format!("deployed/caches/{guid}.tar"));
        assert!(cache.is_file());
        assert_eq!(get(&listen, "src.localhost", "/", None).0, 200);
        // a buildpack by its image (pulled with the builder's credentials: of its registry),
        // laid over the builder's; an image of no buildpack fails the build
        let by_image = format!("docker://{image}");
        let pushed = cf.ok(&["push", "-f", manifest, "-p", source, "-b", &by_image]);
        for expected in [
            format!("Buildpack image/buildpack 0.0.2 of {image}@sha256:"),
            "Pulled its layer ".into(),
            "sample buildpack: build 1 of the cache".into(),
        ] {
            assert!(pushed.contains(&expected), "{expected}: {pushed}");
        }
        let (status, body) = get(&listen, "src.localhost", "/", None);
        assert_eq!(status, 200, "{body}");
        assert!(
            body.contains("PATH=/layers/image_buildpack/runtime/bin"),
            "{body}"
        );
        let by_image = format!("docker://{run_image}");
        let refused = cf.fails(&["push", "-f", manifest, "-p", source, "-b", &by_image]);
        assert!(
            refused.contains(": no buildpack image (no io.buildpacks.buildpackage.metadata)"),
            "{refused}"
        );
        // one the builder lacks fails the build, why in cf push's output
        let refused = cf.fails(&[
            "push",
            "-f",
            manifest,
            "-p",
            source,
            "-b",
            "other/buildpack",
        ]);
        assert!(
            refused.contains(
                "   Staging failed: the builder has no buildpack other/buildpack: its order \
                 names sample/buildpack"
            ),
            "{refused}"
        );
        cf.ok(&["push", "-f", manifest, "-p", source, "-b", "cnb"]);
        let droplet = format!("/v3/apps/{guid}/droplets/current");
        let droplet: Value = serde_json::from_str(&cf.ok(&["curl", &droplet])).unwrap();
        assert_eq!(
            (&droplet["buildpacks"][0]["name"], &droplet["stack"]),
            (&json!("cnb"), &json!("firecracker")),
            "{droplet}"
        );
        assert!(cf.ok(&["buildpacks"]).contains("cnb"));
        // a command on a run image without bash, which the launcher runs it with: staging
        // fails, the app runs the droplet it had
        let refused = cf.fails(&["push", "-f", manifest, "-p", source, "-c", "serve"]);
        assert!(
            refused.contains("the image has no /bin/bash its user may run"),
            "{refused}"
        );
        assert_eq!(get(&listen, "src.localhost", "/", None).0, 200);
    }

    // deleted while the wicket is down: removed when it is back
    cf.ok(&["push", "gone", "-p", dir.join("rootfs").to_str().unwrap()]);
    wicket.kill();
    drop(wicket);
    cf.ok(&["delete", "gone", "-r", "-f"]);
    wicket = start_wicket();
    let deadline = Instant::now() + HANG;
    while listing(&console)["apps"]
        .as_array()
        .unwrap()
        .iter()
        .any(|app| app["hosts"][0] == "gone.localhost")
    {
        assert!(Instant::now() < deadline, "the deleted app stays");
        std::thread::sleep(Duration::from_millis(200));
    }

    // the wicket's directory lost: the cf-api puts its apps back, the image's rootfs
    // made again, of the digest it had, and the one of the droplet buildpacks built, which
    // the cf-api kept: nothing is built again
    wicket.kill();
    drop(wicket);
    let _ = std::fs::remove_dir_all(dir.join("deployed"));
    wicket = start_wicket();
    let deadline = Instant::now() + HANG;
    loop {
        if get(&listen, "fc.localhost", "/", None).0 == 200
            && get(&listen, "zipped.localhost", "/env", None).0 == 200
            && (builder.is_none() || get(&listen, "src.localhost", "/", None).0 == 200)
        {
            break;
        }
        assert!(Instant::now() < deadline, "the apps never came back");
        std::thread::sleep(Duration::from_millis(500));
    }
    let listed = listing(&console);
    let fc = listed["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|app| app["hosts"][0] == "fc.localhost");
    assert_eq!(fc.unwrap()["component"]["sha256"], sha256.as_str());

    // a second wicket of another guest, as in a rolling upgrade of the wickets: it makes the
    // image's rootfs of its own digest at its first put and serves it, the first wicket the one
    // staged; neither makes it again, nor is put again once both listed it
    let guest_b = dir.join("guest-b");
    std::fs::copy(&guest, &guest_b).unwrap();
    let mut bytes = std::fs::OpenOptions::new()
        .append(true)
        .open(&guest_b)
        .unwrap();
    bytes.write_all(b"other bytes").unwrap();
    drop(bytes);
    let (listen_b, console_b) = (free(), free());
    let wicket_b = start_wicket_on(&listen_b, &console_b, "deployed-b", &guest_b);
    drop(api);
    let (api, _) = start_api(
        &api_listen,
        &[
            format!("wicket=http://{console}"),
            format!("b=http://{console_b}"),
        ],
        &flume_url,
        &dir.join("cf"),
        // no credentials of the builder's: the run image is pulled with those of the build
        &[&["--any-credentials"], &uncredentialed[..]].concat(),
    );
    cf.ok(&["scale", "fc", "-i", "2"]);
    let served_as = |console: &str| {
        let listed = listing(console);
        let apps = listed["apps"].as_array().unwrap();
        let fc = apps.iter().find(|app| app["hosts"][0] == "fc.localhost");
        let fc = fc.unwrap_or_else(|| panic!("no fc: {listed}"));
        let revision = listed["revision"].as_u64().unwrap();
        (
            fc["component"]["sha256"].as_str().unwrap().to_string(),
            revision,
        )
    };
    let own = served_as(&console_b).0;
    assert_ne!(own, sha256, "a guest of other bytes");
    let made = api.line("made the rootfs of");
    assert!(
        made.contains(&format!("The wicket b made the rootfs of {image}@")),
        "{made}"
    );
    // a restart puts both, each its own
    cf.ok(&["restart", "fc"]);
    let (on_a, on_b) = (served_as(&console), served_as(&console_b));
    assert_eq!((&on_a.0, &on_b.0), (&sha256, &own));
    for listen in [&listen, &listen_b] {
        assert_eq!(get(listen, "fc.localhost", "/", None).0, 200);
    }
    instances_come_to(&cf, "fc", "2/2");
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!((served_as(&console), served_as(&console_b)), (on_a, on_b));
    let made: Vec<String> = (api.lines.try_iter())
        .filter(|line| line.contains("made the rootfs of"))
        .collect();
    assert_eq!(made, Vec::<String>::new(), "made again");
    // the droplet buildpacks built, placed on the second wicket: uploaded there, its rootfs
    // made of it and the run image, no build
    if builder.is_some() {
        let builds = registry.pulls();
        cf.ok(&["scale", "src", "-i", "2"]);
        let made = api.line("made the rootfs of the droplet");
        assert!(made.contains("The wicket b made the rootfs of"), "{made}");
        eventually("the droplet served by the second wicket", || {
            fetch(&listen_b, "src.localhost", "/", None).is_some_and(|got| got.0 == 200)
        });
        assert_eq!(get(&listen, "src.localhost", "/", None).0, 200);
        let logs = cf.ok(&["logs", "src", "--recent"]);
        // the logs are this cf-api's, started after the build: none since
        assert!(!logs.contains("sample buildpack: building"), "{logs}");
        assert!(registry.pulls() > builds, "the run image, pulled by b");
        // deleted: the cache of its builds forgotten (the wicket's directory was lost since)
        let guid = cf.ok(&["app", "src", "--guid"]).trim().to_string();
        let cache = dir.join(format!("deployed/caches/{guid}.tar"));
        std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
        std::fs::write(&cache, b"").unwrap();
        cf.ok(&["delete", "src", "-r", "-f"]);
        assert!(!cache.exists());
    }

    for app in ["fc", "zipped"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    drop(wicket_b);
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 404);
    drop(api);

    // a wasm wicket beside the Firecracker one, under one cf-api: what is pushed decides where
    // it runs, each stack's apps answering on its wicket's listener
    std::fs::create_dir_all(dir.join("component")).unwrap();
    std::fs::copy(
        guests().join("hello.wasm"),
        dir.join("component/hello.wasm"),
    )
    .unwrap();
    let wasi_config = dir.join("wasi.yaml");
    let stores = "keyvalue:\n  default: local\n  stores:\n    local: {type: sqlite, path: kv.db}\n";
    std::fs::write(&wasi_config, stores).unwrap();
    let wasi = Process::spawn(
        &crate::wicket_binary(),
        &[
            "--flume",
            &flume_url,
            "--flume-token",
            FLUME_TOKEN,
            "--listen",
            "127.0.0.1:0",
            "--console",
            "127.0.0.1:0",
            "--console-token",
            TOKEN,
            "--console-dir",
            dir.join("deployed-wasi").to_str().unwrap(),
            "--config",
            wasi_config.to_str().unwrap(),
            "--watch-interval",
            "0",
        ],
    );
    let listen_w = wasi.logged("listening on ");
    let console_w = wasi.logged("console on http://");
    let (api, _) = start_api(
        &api_listen,
        &[
            format!("wicket=http://{console}"),
            format!("wasi=http://{console_w}"),
        ],
        &flume_url,
        &dir.join("cf"),
        &["--any-credentials"],
    );
    eventually("both stacks listed", || {
        let stacks = cf.ok(&["stacks"]);
        stacks.contains("firecracker ") && stacks.contains("wasip3 ")
    });
    // where the app `host` answers, of the Firecracker wicket's listener and the wasm one's
    let answers = |host: &str| {
        let host = format!("{host}.localhost");
        let on = |listen: &str| get(listen, &host, "/", None).0 == 200;
        (on(&listen), on(&listen_w))
    };
    let component = dir.join("component");
    let component = component.to_str().unwrap();
    cf.ok(&["push", "comp", "-p", component]);
    assert_eq!(answers("comp"), (false, true));
    let (ok, output) = pushed_as("img", &[]);
    assert!(ok, "{output}");
    assert_eq!(answers("img"), (true, false));
    let stack_of = |app: &str| {
        let guid = cf.ok(&["app", app, "--guid"]);
        let droplet = cf.ok(&[
            "curl",
            &format!("/v3/apps/{}/droplets/current", guid.trim()),
        ]);
        let droplet: Value = serde_json::from_str(&droplet).unwrap();
        droplet["stack"].as_str().unwrap().to_string()
    };
    assert_eq!(
        (stack_of("comp"), stack_of("img")),
        ("wasip3".into(), "firecracker".into())
    );
    // the wasm wickets' services are a component's: a rootfs's app binds none of them
    cf.ok(&["create-service", "keyvalue", "local", "kv"]);
    let refused = cf.fails(&["bind-service", "img", "kv"]);
    assert!(
        refused.contains("grants a component's sandbox"),
        "{refused}"
    );
    // a rolling deployment of a droplet of another stack is refused: the app serves on
    let (ok, refused) = pushed_as("comp", &["--strategy", "rolling"]);
    assert!(!ok, "{refused}");
    assert!(
        refused.contains(
            "a rolling deployment of a droplet of the stack firecracker is not supported"
        ),
        "{refused}"
    );
    assert_eq!(answers("comp"), (false, true));
    // a push of another stack moves the app: an image over a component, a stop and a start,
    // and back
    let (ok, output) = pushed_as("comp", &[]);
    assert!(ok, "{output}");
    assert_eq!(answers("comp"), (true, false));
    assert_eq!(stack_of("comp"), "firecracker");
    cf.ok(&["push", "comp", "-p", component]);
    assert_eq!(answers("comp"), (false, true));
    // bound to a keyvalue instance as a component, its start as a rootfs is refused, naming it
    cf.ok(&["bind-service", "comp", "kv"]);
    let (ok, refused) = pushed_as("comp", &[]);
    assert!(!ok, "{refused}");
    assert!(
        refused.contains("the app binds the service instance kv (keyvalue)"),
        "{refused}"
    );
    // a pin: the stack of `-s`, one the wickets run; a package of another fails staging
    let refused = cf.fails(&["push", "pinned", "-p", component, "-s", "cflinuxfs4"]);
    assert!(refused.contains("cflinuxfs4"), "{refused}");
    let refused = cf.fails(&["push", "pinned", "-p", component, "-s", "firecracker"]);
    let staging = cf.ok(&["logs", "pinned", "--recent"]);
    assert!(
        staging.contains(
            "is a wasm component (the stack wasip3): the app is pinned to the stack firecracker"
        ),
        "{refused}\n{staging}"
    );
    let pinned = cf.ok(&["curl", "/v3/apps?names=pinned"]);
    assert!(pinned.contains(r#""stack":"firecracker""#), "{pinned}");
    for app in ["comp", "img", "pinned"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    cf.ok(&["delete-service", "kv", "-f"]);
    eventually("the apps removed", || {
        listing(&console)["apps"].as_array().unwrap().is_empty()
    });
    drop(wasi);
    drop(api);

    // the cf-api as an app of the wicket, owner ops, a disk of its own at its working
    // directory, reaching the console and the flume through its egress; the host an
    // address, not to depend on *.localhost resolving here
    let cf_api = cf_api_image(&registry, &dir);
    let http = Http::new();
    let console_api = |method: reqwest::Method, path: &str, body: Value| {
        let response = http
            .0
            .request(method, format!("http://{console}{path}"))
            .bearer_auth(TOKEN)
            .timeout(Duration::from_secs(300))
            .body(body.to_string())
            .send()
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json::<Value>().unwrap())
    };
    let made = |image: &str| {
        let body = json!({"image": image, "username": "robot", "password": "p4ss"});
        let (status, made) = console_api(reqwest::Method::PUT, "/api/images", body);
        assert_eq!(status, 200, "{made}");
        made["sha256"].as_str().unwrap().to_string()
    };
    let port_of = |address: &str| address.rsplit(':').next().unwrap().to_string();
    let (console_port, flume_port) = (port_of(&console), port_of(&flume_url));
    let provider = MockProvider::start("cf-api", "s3");
    let provider_at = provider.issuer.trim_start_matches("http://");
    let provider_at = provider_at.split('/').next().unwrap().to_string();
    let put_cf_api = |sha256: &str, env: Value| {
        let mut all = json!({
            "SLUICE_WICKET_CF_API_WICKET": format!("http://localhost:{console_port}"),
            "SLUICE_WICKET_CF_API_WICKET_TOKEN": TOKEN,
            "SLUICE_WICKET_CF_API_FLUME": format!("grpc://localhost:{flume_port}"),
            "SLUICE_WICKET_CF_API_FLUME_TOKEN": FLUME_TOKEN,
        });
        all.as_object_mut()
            .unwrap()
            .extend(env.as_object().unwrap().clone());
        console_api(
            reqwest::Method::PUT,
            "/api/apps/cf-api",
            json!({
                "owner": "ops",
                "hosts": ["127.0.0.1"],
                "component": {"sha256": sha256, "name": "cf-api.ext4"},
                "sandbox": {
                    "memory-mib": 256,
                    "disk": "/data",
                    "disk-mib": 256,
                    "egress": [
                        format!("localhost:{console_port}"),
                        format!("localhost:{flume_port}"),
                        provider_at,
                    ],
                    "env": all,
                },
            }),
        )
    };
    let sha256 = made(&cf_api);
    // no provider and nothing saying who logs in: nothing starts, its VM's loopback or not
    let (status, put) = put_cf_api(&sha256, json!({}));
    assert_eq!(status, 422, "{put}");
    assert!(
        put.to_string().contains("exited before it listened"),
        "{put}"
    );
    let (status, put) = put_cf_api(
        &sha256,
        json!({"SLUICE_WICKET_CF_API_ANY_CREDENTIALS": "true"}),
    );
    assert_eq!(status, 200, "{put}");
    assert_eq!(put["message"], "added 127.0.0.1");
    let vm_url = format!("http://127.0.0.1:{}", port_of(&listen));
    cf.ok(&["api", &vm_url]);
    cf.ok(&["login", "-u", "me", "-p", "x"]);
    let (status, info) = get(&listen, "127.0.0.1", "/v3/info", None);
    assert_eq!(status, 200, "{info}");
    assert!(info.contains("sluice-wicket-runtime-firecracker"), "{info}");
    let mut push = cf.command(&[
        "push",
        "vm",
        "--docker-image",
        &image,
        "--docker-username",
        "robot",
    ]);
    push.env("CF_DOCKER_PASSWORD", "p4ss");
    let (ok, output) = cf.output(push, "");
    assert!(ok, "{output}");
    let (status, body) = get(&listen, "vm.localhost", "/", None);
    assert_eq!(status, 200, "{body}");

    // through the flume, the cf-api's watch of it over its egress
    let deadline = Instant::now() + HANG;
    loop {
        assert_eq!(get(&listen, "vm.localhost", "/log", None).0, 200);
        if cf
            .ok(&["logs", "vm", "--recent"])
            .contains("[APP/PROC/WEB/0] OUT GET /log HTTP/1.0")
        {
            break;
        }
        assert!(Instant::now() < deadline, "the app's lines never came");
        std::thread::sleep(Duration::from_millis(500));
    }

    // a restart of its VM: the app is served all along, the CLI's token still good, the
    // app the cf-api's still
    let answers = || {
        let deadline = Instant::now() + HANG;
        while !matches!(
            std::panic::catch_unwind(|| get(&listen, "127.0.0.1", "/v3/info", None)),
            Ok((200, _))
        ) {
            assert!(Instant::now() < deadline, "the cf-api never came back");
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    let generation = listing(&console)["generation"].as_u64().unwrap();
    let (status, put) = put_cf_api(
        &sha256,
        json!({"SLUICE_WICKET_CF_API_ANY_CREDENTIALS": "true", "RESTART": "1"}),
    );
    assert_eq!(status, 200, "{put}");
    assert_eq!(put["message"], "restarted 127.0.0.1");
    answers();
    assert!(cf.ok(&["apps"]).contains("vm"));
    shows("vm", "running");
    assert_eq!(get(&listen, "vm.localhost", "/", None).0, 200);
    assert_eq!(
        listing(&console)["generation"],
        generation + 1,
        "its own restart alone"
    );

    // the wicket killed right after a push: the push is on the disk, and served again
    cf.ok(&["push", "kept", "-p", dir.join("rootfs").to_str().unwrap()]);
    wicket.kill();
    drop(wicket);
    wicket = start_wicket();
    answers();
    cf.ok(&["app", "kept"]);
    shows("kept", "running");
    assert_eq!(get(&listen, "kept.localhost", "/env", None).0, 200);

    // an upgrade: another image (the same, of another digest), the disk kept, the old VM
    // stopped for the new one; with a provider through its egress, the CLI's token no good
    // any more but for what the disk keeps: alice signs in
    let mut image =
        std::fs::read(dir.join(format!("deployed/rootfs/{sha256}/cf-api.ext4"))).unwrap();
    image.extend_from_slice(&[0; 4096]);
    let upgraded: String = Sha256::digest(&image)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let uploaded = http
        .0
        .put(format!("http://{console}/api/components/{upgraded}"))
        .bearer_auth(TOKEN)
        .timeout(Duration::from_secs(300))
        .body(image)
        .send()
        .unwrap();
    assert_eq!(uploaded.status().as_u16(), 200);
    let (status, put) = put_cf_api(
        &upgraded,
        json!({
            "SLUICE_WICKET_CF_API_OIDC_ISSUER": provider.issuer,
            "SLUICE_WICKET_CF_API_OIDC_CLIENT_ID": "cf-api",
            "SLUICE_WICKET_CF_API_OIDC_CLIENT_SECRET": "s3",
            "SLUICE_WICKET_CF_API_OIDC_SCOPES": "openid,email,profile,groups",
            "SLUICE_WICKET_CF_API_OIDC_CLAIM": "email=*@example.com",
            "SLUICE_WICKET_CF_API_ADMIN": "email=alice@example.com",
        }),
    );
    assert_eq!(status, 200, "{put}");
    assert_eq!(put["message"], "restarted 127.0.0.1");
    answers();
    provider.sign_in(json!({
        "sub": "1", "email": "alice@example.com", "preferred_username": "alice", "groups": []
    }));
    let (code, _) = passcode(&http, &vm_url);
    let output = cf.ok_with_stdin(&["login", "--sso"], &format!("{code}\n"));
    assert!(output.contains("Authenticating...\nOK"), "{output}");
    assert!(cf.ok(&["apps"]).contains("kept"));

    for app in ["vm", "kept"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    assert_eq!(
        get(&listen, "vm.localhost", "/", None).0,
        404,
        "the cf-api's route, alone, takes its own host only"
    );
    assert_eq!(get(&listen, "127.0.0.1", "/", None).0, 200);
    let path = "/api/apps/cf-api?owner=ops";
    let (status, removed) = console_api(reqwest::Method::DELETE, path, json!({}));
    assert_eq!(status, 200, "{removed}");
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());

    drop(wicket);
    drop(flume);
    drop(provider);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Where the lifecycle and the launcher of Cloud Native Buildpacks are (`SLUICE_CNB_LIFECYCLE`,
/// else `~/.local/cnb/lifecycle`); None where they are not.
fn cnb_lifecycle() -> Option<PathBuf> {
    let dir = std::env::var_os("SLUICE_CNB_LIFECYCLE")
        .map(PathBuf::from)
        .or_else(|| {
            let home = PathBuf::from(std::env::var_os("HOME")?);
            Some(home.join(".local/cnb/lifecycle"))
        })?;
    (dir.join("lifecycle").is_file() && dir.join("launcher").is_file()).then_some(dir)
}

/// A builder of one buildpack, the guest crate's sample one (beside `guest`), with the
/// lifecycle of `lifecycle`, and its run image, a base naming a user; another run image of its
/// target, naming another; a buildpack image of the sample buildpack by another id: their
/// references in `registry`, the builder's first.
fn cnb_builder(registry: &Registry, lifecycle: &Path, guest: &Path) -> (String, String, String) {
    let arch = if cfg!(target_arch = "x86_64") {
        "amd64"
    } else {
        "arm64"
    };
    let read = |path: PathBuf| std::fs::read(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    let entry = |tar: &mut tar::Builder<Vec<u8>>, kind, path: &str, link: Option<&str>| {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_mode(0o755);
        header.set_uid(1001);
        header.set_gid(1000);
        header.set_size(0);
        match link {
            Some(link) => tar.append_link(&mut header, path, link).unwrap(),
            None => tar
                .append_data(&mut header, path, std::io::empty())
                .unwrap(),
        }
    };
    let base = layer(|tar| {
        let users = "root:x:0:0::/root:/bin/sh\ncnb:x:1001:1000::/home/cnb:/bin/sh\n";
        layer_file(tar, "etc/passwd", 0o644, 0, users.as_bytes());
        layer_file(tar, "etc/group", 0o644, 0, b"root:x:0:\ncnb:x:1000:\n");
        entry(tar, tar::EntryType::Directory, "home/cnb/", None);
    });
    // the sample buildpack as `id` of `version`, in its directory, as a builder has it
    let buildpack = |tar: &mut tar::Builder<Vec<u8>>, id: &str, version: &str| {
        let dir = format!("cnb/buildpacks/{}/{version}", id.replace('/', "_"));
        let described = format!(
            "api = \"0.10\"\n\n[buildpack]\n  id = \"{id}\"\n  version = \"{version}\"\n\n\
             [[targets]]\n  os = \"linux\"\n  arch = \"{arch}\"\n"
        );
        let toml = format!("{dir}/buildpack.toml");
        layer_file(tar, &toml, 0o644, 0, described.as_bytes());
        let program = read(guest.with_file_name("sample-buildpack"));
        for phase in ["detect", "build"] {
            layer_file(tar, &format!("{dir}/bin/{phase}"), 0o755, 0, &program);
        }
        let serve = read(guest.with_file_name("sample-process"));
        layer_file(tar, &format!("{dir}/serve"), 0o755, 0, &serve);
    };
    let cnb = layer(|tar| {
        for binary in ["lifecycle", "launcher"] {
            let path = format!("cnb/lifecycle/{binary}");
            layer_file(tar, &path, 0o755, 0, &read(lifecycle.join(binary)));
        }
        for phase in ["detector", "builder"] {
            let path = format!("cnb/lifecycle/{phase}");
            entry(tar, tar::EntryType::Symlink, &path, Some("lifecycle"));
        }
        let order = "[[order]]\n  [[order.group]]\n    id = \"sample/buildpack\"\n    \
                     version = \"0.0.1\"\n";
        layer_file(tar, "cnb/order.toml", 0o644, 0, order.as_bytes());
        buildpack(tar, "sample/buildpack", "0.0.1");
    });
    // a buildpack image, as pack makes one: its layer's diff ID in its labels
    let mut tar = tar::Builder::new(Vec::new());
    buildpack(&mut tar, "image/buildpack", "0.0.2");
    let tar = tar.into_inner().unwrap();
    let diff_id = format!("sha256:{:x}", Sha256::digest(&tar));
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&tar).unwrap();
    let held = json!({"image/buildpack": {"0.0.2": {"api": "0.10", "layerDiffID": diff_id}}});
    let config = json!({
        "architecture": arch,
        "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": [diff_id]},
        "config": {"Labels": {
            "io.buildpacks.buildpackage.metadata":
                json!({"id": "image/buildpack", "version": "0.0.2"}).to_string(),
            "io.buildpacks.buildpack.layers": held.to_string(),
        }},
    });
    registry.push_image("private/buildpack", "1", &config, &[gzip.finish().unwrap()]);
    for (repository, user) in [
        ("private/run", "runner:x:2000:2001"),
        ("private/tiny", "tiny:x:3000:3001"),
    ] {
        let run = layer(|tar| {
            let users = format!("root:x:0:0::/root:/bin/sh\n{user}::/home/runner:/bin/sh\n");
            layer_file(tar, "etc/passwd", 0o644, 0, users.as_bytes());
        });
        let name = &user[..user.find(':').unwrap()];
        let config = json!({"architecture": arch, "os": "linux", "config": {"User": name}});
        registry.push_image(repository, "1", &config, &[run]);
    }
    let metadata = json!({
        "lifecycle": {"apis": {"platform": {"supported": ["0.13"]}}},
        "images": [{"image": format!("{}/private/run:1", registry.address)}],
        "buildpacks": [{"id": "sample/buildpack", "version": "0.0.1"}],
    });
    let config = json!({
        "architecture": arch,
        "os": "linux",
        "config": {
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "CNB_USER_ID=1001",
                "CNB_GROUP_ID=1000",
            ],
            "User": "1001:1000",
            "Labels": {"io.buildpacks.builder.metadata": metadata.to_string()},
        },
    });
    registry.push_image("private/builder", "1", &config, &[base, cnb]);
    let address = &registry.address;
    (
        format!("{address}/private/builder:1"),
        format!("{address}/private/tiny:1"),
        format!("{address}/private/buildpack:1"),
    )
}

/// The cf-api as an image (no docker here: this binary stripped, what it links, the host's
/// CA bundle, the working directory its user's and the port its Dockerfile's, in one layer
/// of no compression: debug builds read it all, more than once); its reference in `registry`.
fn cf_api_image(registry: &Registry, dir: &Path) -> String {
    let built = env!("CARGO_BIN_EXE_sluice-wicket-cf-api");
    let binary = dir.join("cf-api-stripped");
    let stripped = Command::new("strip")
        .args(["--strip-debug", "-o"])
        .arg(&binary)
        .arg(built)
        .status()
        .expect("run strip");
    assert!(stripped.success());
    let linked = Command::new("ldd").arg(built).output().unwrap();
    let linked = String::from_utf8_lossy(&linked.stdout).into_owned();
    let libraries: Vec<&str> = linked
        .split_whitespace()
        .filter(|word| word.starts_with('/'))
        .collect();
    let layer = layer_of(flate2::Compression::none(), |tar| {
        layer_file(tar, "cf-api", 0o755, 0, &std::fs::read(&binary).unwrap());
        for library in &libraries {
            let bytes = std::fs::read(library).unwrap();
            layer_file(tar, &library[1..], 0o755, 0, &bytes);
        }
        let bundle = "/etc/ssl/certs/ca-certificates.crt";
        let roots = std::fs::read(bundle).unwrap();
        layer_file(tar, &bundle[1..], 0o644, 0, &roots);
        let mut data = tar::Header::new_gnu();
        data.set_entry_type(tar::EntryType::Directory);
        data.set_mode(0o755);
        data.set_uid(65532);
        data.set_gid(65532);
        data.set_size(0);
        tar.append_data(&mut data, "data/", std::io::empty())
            .unwrap();
    });
    let config = json!({
        "architecture": if cfg!(target_arch = "x86_64") { "amd64" } else { "arm64" },
        "os": "linux",
        "config": {
            "Entrypoint": ["/cf-api"],
            "User": "65532:65532",
            "WorkingDir": "/data",
            "ExposedPorts": {"9091/tcp": {}},
        },
    });
    registry.push_image("private/cf-api", "1", &config, &[layer]);
    format!("{}/private/cf-api:1", registry.address)
}
