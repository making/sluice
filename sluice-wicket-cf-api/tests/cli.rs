//! The OSS cf CLI (`SLUICE_CF_CLI`, else `cf8` on the PATH; the Tanzu
//! distribution refuses any other platform) against the cf-api and a wicket
//! in local mode, both on a flume: login, push, the app's logs, map-route,
//! scale, a rolling push with requests all along, stop and start, the
//! user's environment variables and VCAP_*, a private component in an OCI
//! registry (a stub) pushed by tag -- its credentials, a restart keeping
//! the digest staged when the tag moves, a restage taking the new one --, a
//! restart of the cf-api with an OpenID Provider (a mock) and TLS of its
//! own -- `cf login --sso` through the passcode page, a refresh, the
//! clients configured, who is refused, who may edit the security groups --, a component
//! the wicket refuses
//! until a service binding grants what it needs, the services' lifecycle,
//! delete. And against a Firecracker wicket, where `/dev/kvm` is usable: a
//! private image made into a rootfs by the wicket, its process's user, files and
//! environment seen from the VM, its connections through the security groups, a scale, a
//! rootfs of a zip, the wicket's
//! directory lost and the image's rootfs made again, of the digest it had; then the
//! cf-api itself an app of that wicket, in a VM with a disk, the flume and a provider
//! through its egress: its apps and logins kept across a restart of the VM, a wicket
//! killed right after a push and an upgrade.

mod registry;

use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

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

fn start_api(
    listen: &str,
    console: &str,
    flume: &str,
    data: &Path,
    more: &[&str],
) -> (Process, String) {
    let wicket = format!("http://{console}");
    let mut args = vec![
        "--listen",
        listen,
        "--wicket",
        &wicket,
        "--wicket-token",
        TOKEN,
        "--data-dir",
        data.to_str().unwrap(),
        "--flume",
        flume,
        "--flume-token",
        FLUME_TOKEN,
    ];
    args.extend_from_slice(more);
    let api = Process::spawn(Path::new(env!("CARGO_BIN_EXE_sluice-wicket-cf-api")), &args);
    let url = api.logged("listening on ");
    (api, url)
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

/// The code of the passcode page.
fn passcode(http: &Http, api: &str) -> String {
    let (status, page) = passcode_page(http, api);
    assert_eq!(status, 200, "{page}");
    let (_, rest) = page
        .split_once("id=\"passcode\" class=\"big\">")
        .expect("the code");
    rest.split_once("</code>").unwrap().0.to_string()
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
        .args(["--data-dir", "/proc/sluice-cf-api"])
        .output()
        .unwrap();
    assert!(!unwritable.status.success());
    let said = String::from_utf8_lossy(&unwritable.stdout);
    assert!(!said.contains("listening on"), "{said}");
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
    let wicket = Process::spawn(
        &wicket_binary,
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
    let (mut api, url) = start_api(&api_listen, &console, &flume_url, &dir.join("cf"), &[]);
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
    cf.ok(&["scale", "hello", "-i", "2"]);
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["pool"], 2);
    // the second warm instance takes its turn: its index in the output and in
    // app_index (a request before the units are up runs on an instance of its
    // own, index 2)
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

    // the user's variables, VCAP_APPLICATION and VCAP_SERVICES: the guest
    // gets them from the next start on; no log and no listing of the
    // wicket holds a value
    let variable = |name: &str| get(&listen, "hello.localhost", &format!("/env/{name}"), None);
    cf.ok(&["set-env", "hello", "GREETING", "s3cr3t-greeting"]);
    assert_eq!(variable("GREETING").0, 404, "until the next start");
    cf.ok(&["restart", "hello"]);
    assert_eq!(variable("GREETING"), (200, "s3cr3t-greeting".into()));
    let (status, vcap) = variable("VCAP_APPLICATION");
    assert_eq!(status, 200, "{vcap}");
    let vcap: Value = serde_json::from_str(&vcap).unwrap();
    assert_eq!(vcap["application_name"], "hello");
    assert_eq!(variable("VCAP_SERVICES"), (200, "{}".into()));
    let env = cf.ok(&["env", "hello"]);
    assert!(env.contains("GREETING: s3cr3t-greeting"), "{env}");
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
    let (lost, _) = start_api(&api_listen, &console, &flume_url, &dir.join("cf-lost"), &[]);
    let left = lost.line("CELL: ");
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
        "--tls-cert",
        cert_file.to_str().unwrap(),
        "--tls-key",
        key_file.to_str().unwrap(),
    ];
    let with_rules = |rules: &[&str]| {
        let mut args: Vec<&str> = oidc.to_vec();
        for rule in rules {
            args.extend_from_slice(&["--oidc-claim", rule]);
        }
        start_api(&api_listen, &console, &flume_url, &dir.join("cf"), &args)
    };
    let (api, url) = with_rules(&["email=*@example.com", "groups=devs"]);
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
    let code = passcode(&http, &url);
    assert_eq!(code.len(), 8);
    let output = cf.ok_with_stdin(&["login", "--sso"], &format!("{code}\n"));
    assert!(output.contains("Authenticating...\nOK"), "{output}");
    assert!(cf.ok(&["target"]).contains("user:           alice"));
    assert!(cf.ok(&["app", "hello"]).contains("hi.localhost"));
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
        let code = passcode(&http, &url);
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
    let purged = api.line("SERVICES: deleted the data of service instance counts");
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

    // gone from the wicket; their instances stay, unbound
    for app in ["hello", "counter", "relay", "img"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    assert_eq!(get(&listen, "hello.localhost", "/", None).0, 404);
    assert!(!cf.ok(&["apps"]).contains("hello"));
    let services = cf.ok(&["services"]);
    assert!(
        services.contains("sessions") && !services.contains("counter"),
        "{services}"
    );

    // alice no longer allowed (the rules changed): her next refresh is
    // refused, and her session with it
    drop(api);
    let (api, url) = with_rules(&["email=*@example.com", "groups=ops"]);
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
    let start_wicket = || {
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
                &listen,
                "--console",
                &console,
                "--console-token",
                TOKEN,
                "--console-dir",
                dir.join("deployed").to_str().unwrap(),
                "--guest",
                guest.to_str().unwrap(),
                "--watch-interval",
                "0",
            ],
        );
        wicket.logged("console on http://");
        wicket
    };
    let mut wicket = start_wicket();
    // an API taking any credentials names who edits the security groups by the name given
    let (api, url) = start_api(
        &api_listen,
        &console,
        &flume_url,
        &dir.join("cf"),
        &["--security-group-editor", "user_name=me"],
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
    cf.ok(&["start", "fc"]);
    let shown = cf.ok(&["app", "fc"]);
    assert!(shown.contains(&format!("{image}@{manifest}")), "{shown}");

    // from inside the VM: the image's user, its files' owners and modes, the env
    let (status, body) = get(&listen, "fc.localhost", "/", None);
    assert_eq!(status, 200, "{body}");
    for expected in [
        "uid 1000 gid 1001\n",
        "/app/data uid 1000 gid 1001 mode 640\n",
        "/app/tool uid 0 gid 1 mode 4755\n",
        "GREETING=hello world\n",
        "HOME=/home/app\n",
        "IMAGE_VAR=from-image\n",
        "PORT=8080\n",
        "VCAP_APPLICATION={",
    ] {
        assert!(body.contains(expected), "{expected}: {body}");
    }
    let listed = listing(&console);
    let fc = &listed["apps"][0];
    assert_eq!(listed["runtime"], "firecracker");
    assert_eq!(fc["hosts"], json!(["fc.localhost"]));
    assert_eq!(fc["sandbox"]["mode"], "shared");
    // as Cloud Controller's defaults: what a JVM needs of memory and of time to listen
    assert_eq!(fc["sandbox"]["mem"], 1024);
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
    let pushed = |extra: &[&str]| {
        let mut args = vec![
            "push",
            "fc",
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
    let (ok, output) = pushed(&["-m", "512M"]);
    assert!(ok, "{output}");
    assert_eq!(listing(&console)["apps"][0]["sandbox"]["mem"], 512);
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
    cf.ok(&["restart", "fc"]);

    // the wicket restarted, back at the revision it had: the app shows starting while the
    // wicket is down, and running again once it is back
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
    wicket.kill();
    drop(wicket);
    shows("fc", "starting");
    wicket = start_wicket();
    shows("fc", "running");

    // two instances: a pool of two VMs cloned from a snapshot, env and all
    cf.ok(&["scale", "fc", "-i", "2"]);
    let scaled = &listing(&console)["apps"][0]["sandbox"];
    assert_eq!(
        (&scaled["mode"], &scaled["vms"]),
        (&"pool".into(), &2.into())
    );
    let (status, body) = get(&listen, "fc.localhost", "/", None);
    assert!(
        status == 200 && body.contains("GREETING=hello world\n"),
        "{status}: {body}"
    );

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
    // made again, of the digest it had
    wicket.kill();
    drop(wicket);
    let _ = std::fs::remove_dir_all(dir.join("deployed"));
    wicket = start_wicket();
    let deadline = Instant::now() + HANG;
    loop {
        if get(&listen, "fc.localhost", "/", None).0 == 200
            && get(&listen, "zipped.localhost", "/env", None).0 == 200
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

    for app in ["fc", "zipped"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());
    assert_eq!(get(&listen, "fc.localhost", "/", None).0, 404);
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
                    "mem": 256,
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
    let (status, put) = put_cf_api(&sha256, json!({}));
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
    let (status, put) = put_cf_api(&sha256, json!({"RESTART": "1"}));
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
        }),
    );
    assert_eq!(status, 200, "{put}");
    assert_eq!(put["message"], "restarted 127.0.0.1");
    answers();
    provider.sign_in(json!({
        "sub": "1", "email": "alice@example.com", "preferred_username": "alice", "groups": []
    }));
    let code = passcode(&http, &vm_url);
    let output = cf.ok_with_stdin(&["login", "--sso"], &format!("{code}\n"));
    assert!(output.contains("Authenticating...\nOK"), "{output}");
    assert!(cf.ok(&["apps"]).contains("kept"));

    for app in ["vm", "kept"] {
        cf.ok(&["delete", app, "-r", "-f"]);
    }
    let path = "/api/apps/cf-api?owner=ops";
    let (status, removed) = console_api(reqwest::Method::DELETE, path, json!({}));
    assert_eq!(status, 200, "{removed}");
    assert!(listing(&console)["apps"].as_array().unwrap().is_empty());

    drop(wicket);
    drop(flume);
    drop(provider);
    let _ = std::fs::remove_dir_all(&dir);
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
