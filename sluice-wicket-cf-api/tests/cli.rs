//! The OSS cf CLI (`SLUICE_CF_CLI`, else `cf8` on the PATH; the Tanzu
//! distribution refuses any other platform) against the cf-api and a wicket
//! in local mode, both on a flume: login, push, the app's logs, map-route,
//! scale, a rolling push with requests all along, stop and start, the
//! user's environment variables and VCAP_*, a restart of the cf-api with an
//! OpenID Provider (a mock) and TLS of its own -- `cf login --sso` through
//! the passcode page, a refresh, the clients configured, who is refused --,
//! a component the wicket refuses until a service binding grants what it
//! needs, the services' lifecycle, delete.

use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sluice_oidc::mock::MockProvider;

/// Hang guard of every wait; the first covers a cold build of the wicket.
const HANG: Duration = Duration::from_secs(120);

const TOKEN: &str = "s3cret";

const FLUME_TOKEN: &str = "f1ume";

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
        let mut child = self
            .command(args)
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

    // push from a directory: served on the wicket's listener
    cf.ok(&["push", "hello", "-p", dir.join("app").to_str().unwrap()]);
    let (status, body) = get(&listen, "hello.localhost", "/", None);
    assert_eq!(status, 200, "{body}");
    assert!(body.starts_with("hello from wasm"), "{body}");
    assert!(
        cf.ok(&["logs", "hello", "--recent"])
            .contains("Staged hello.wasm")
    );

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

    // the cf-api restarted with an OpenID Provider and TLS of its own: its
    // state kept, the wicket's generation unchanged; the open mode's token
    // is no good any more, nor is a password
    let generation = listing(&console)["generation"].as_u64().unwrap();
    let (open_token, _) = cf.tokens();
    api.kill();
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
    for app in ["hello", "counter", "relay"] {
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
