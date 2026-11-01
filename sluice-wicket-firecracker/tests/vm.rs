//! The wicket booting VMs, through a tunnel server double (`sluice-tunnel`'s):
//! for a shared VM (serving two hosts: one target) and a VM per connection,
//! http/1.1 with keep-alive, h2c, a 5MB POST under the tunnel's flow control
//! and a client half-closing after its request; a route of one VM at most
//! failing the connection that finds no room in time, its clones restored
//! with the clock of the moment and random state of their own; a reload of
//! the `--config` file, polled, removing a route and replacing a rootfs.
//!
//! Skipped where `/dev/kvm` is not usable. Elsewhere it needs Firecracker
//! (`SLUICE_FIRECRACKER`, else `firecracker` on the PATH) and a guest kernel
//! (`SLUICE_FIRECRACKER_KERNEL`, else `~/.local/firecracker/vmlinux`), see
//! the README's Setup, and builds the sample guest's rootfs
//! (`scripts/build-guest.sh`). `SLUICE_FIRECRACKER_VMS` adds routes (`--vm`
//! values, whitespace-separated, e.g. a CNB image's rootfs), whose answers
//! are checked by their status alone.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use sluice_tunnel::session::testing::{ServerTunnel, dial_address, tunnel_server};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Hang guard of every wait; the first covers the boots and the snapshot.
const HANG: Duration = Duration::from_secs(120);

/// The POST's body.
const UPLOAD: usize = 5_000_000;

#[tokio::test(flavor = "multi_thread")]
async fn every_route_serves_http_through_the_tunnel() {
    if std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .is_err()
    {
        eprintln!("skipped: /dev/kvm is not usable here (the kvm group: sg kvm -c 'cargo test')");
        return;
    }
    let (firecracker, kernel) = tools();
    let rootfs = sample_rootfs();
    let extra: Vec<String> = std::env::var("SLUICE_FIRECRACKER_VMS")
        .unwrap_or_default()
        .split_whitespace()
        .map(String::from)
        .collect();
    // the routes of the config file; the shared ones on a rootfs of their own,
    // replaced below
    let dir = Scratch::new();
    std::fs::copy(&rootfs, dir.0.join("shared.ext4")).unwrap();
    let config = dir.0.join("wicket.yaml");
    let routes = format!(
        "watch-interval: 100ms\nroutes:\n\
         - {{host: shared.local, rootfs: shared.ext4}}\n\
         - {{host: clone.local, rootfs: {rootfs}, mode: per-connection, warm: 2}}\n\
         # the shared target again: its VM serves both hosts\n\
         - {{host: alias.local, rootfs: shared.ext4, mode: shared}}\n",
        rootfs = rootfs.display()
    );
    let capped = format!(
        "- {{host: capped.local, rootfs: {}, mode: per-connection, vms: 1, wait-ms: 300}}\n",
        rootfs.display()
    );
    std::fs::write(&config, format!("{routes}{capped}")).unwrap();
    let mut args = vec!["--config".to_string(), config.display().to_string()];
    args.extend(extra.iter().flat_map(|vm| ["--vm".to_string(), vm.clone()]));
    let (url, mut streams) = tunnel_server(None).await;
    let wicket = Wicket::start(&url, &firecracker, &kernel, &args);
    let stream = tokio::time::timeout(HANG, streams.recv())
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("no tunnel stream; {}", wicket.log()));
    let (tunnel, advertise) = ServerTunnel::new(stream).await;
    let build = advertise.build.clone().expect("the wicket's build");
    assert_eq!(build.implementation, "sluice-wicket-firecracker");
    let hosts: Vec<_> = advertise.upstreams.iter().map(|up| &up.host).collect();
    assert_eq!(hosts.len(), 4 + extra.len(), "{hosts:?}");

    let target = |host: &str| {
        let upstream = advertise.upstreams.iter().find(|up| up.host == host);
        upstream.unwrap().target_url.clone()
    };
    assert_eq!(target("alias.local"), target("shared.local"));
    assert_ne!(target("clone.local"), target("shared.local"));

    let mut conn_id = 0;
    // the requests each target's shared VM served so far
    let mut counts = std::collections::HashMap::new();
    for upstream in &advertise.upstreams {
        let route = Route {
            tunnel: &tunnel,
            host: &upstream.host,
            address: dial_address(&upstream.target_url),
        };
        let sample = !extra
            .iter()
            .any(|vm| vm.starts_with(&format!("{}=", route.host)));
        if route.host == "capped.local" {
            let checked = tokio::time::timeout(HANG, route.capped(&mut conn_id)).await;
            assert!(checked.is_ok(), "capped.local: hung; {}", wicket.log());
            continue;
        }
        let before = counts.entry(upstream.target_url.clone()).or_insert(0);
        let checked = tokio::time::timeout(HANG, route.check(&mut conn_id, sample, before)).await;
        assert!(checked.is_ok(), "{}: hung; {}", route.host, wicket.log());
    }

    // reloads, once a poll found the files quiet: the config file's alone
    // (capped.local removed), then the shared rootfs replaced
    let mut tunnel = tunnel;
    std::fs::write(&config, &routes).unwrap();
    let reloaded = tunnel.advertise().await;
    let hosts: Vec<_> = reloaded.upstreams.iter().map(|up| &up.host).collect();
    assert!(!hosts.contains(&&"capped.local".to_string()), "{hosts:?}");
    assert_eq!(hosts.len(), 3 + extra.len(), "{hosts:?}");
    std::fs::copy(&rootfs, dir.0.join("shared.ext4.new")).unwrap();
    std::fs::rename(dir.0.join("shared.ext4.new"), dir.0.join("shared.ext4")).unwrap();
    let shared = Route {
        tunnel: &tunnel,
        host: "shared.local",
        address: dial_address(&target("shared.local")),
    };
    // the old VM counts on until a new one on the new rootfs serves
    let deadline = Instant::now() + HANG;
    loop {
        conn_id += 1;
        if served(&shared.keep_alive(conn_id).await) == [1, 2, 3] {
            break;
        }
        assert!(Instant::now() < deadline, "shared.local was not restarted");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // the removed target, served until the server acknowledged its removal
    let removed = Route {
        tunnel: &tunnel,
        host: "capped.local",
        address: dial_address(&target("capped.local")),
    };
    let deadline = Instant::now() + HANG;
    loop {
        conn_id += 1;
        if removed.half_closed(conn_id).await.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "capped.local is still served");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A directory of its own under the system's temp dir (short paths: the VMs'
/// sockets), removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Scratch {
        let dir = std::env::temp_dir().join(format!("sfc-vm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One advertised route, reached through the tunnel.
struct Route<'a> {
    tunnel: &'a ServerTunnel,
    host: &'a str,
    address: String,
}

impl Route<'_> {
    /// `scripts/check.sh`'s cases. With the sample guest (`sample`), the
    /// answers tell the VMs apart: a VM per connection starts every
    /// connection at its first request, a shared VM counts on from the
    /// requests it served `before` (for another host of its target), which
    /// this adds its own to.
    async fn check(&self, conn_id: &mut i64, sample: bool, before: &mut u64) {
        let mut next = || {
            *conn_id += 1;
            *conn_id
        };
        let first = self.keep_alive(next()).await;
        let again = self.keep_alive(next()).await;
        if sample {
            let shared = !self.address.contains("mode=per-connection");
            let base = if shared { *before } else { 0 };
            assert_eq!(
                served(&first),
                [base + 1, base + 2, base + 3],
                "{}: {first:?}",
                self.host
            );
            let expected = if shared { base + 4 } else { 1 };
            assert_eq!(served(&again)[0], expected, "{}: {again:?}", self.host);
            // the two connections, h2c, the POST and the half-close
            *before += 9;
        }

        let (status, body) = self.request(next(), true, Bytes::new()).await;
        assert_eq!(status, 200, "{}: h2c", self.host);
        if sample {
            assert!(body.contains("HTTP/2.0"), "{}: {body}", self.host);
        }

        let upload = Bytes::from(vec![b'x'; UPLOAD]);
        let (status, body) = self.request(next(), false, upload).await;
        assert_eq!(status, 200, "{}: POST", self.host);
        if sample {
            assert!(
                body.contains(&format!("{UPLOAD} body bytes")),
                "{}: {body}",
                self.host
            );
        }

        let answer = self.half_closed(next()).await;
        assert!(
            answer.starts_with("HTTP/1.1 200"),
            "{}: half-close: {answer:?}",
            self.host
        );
    }

    /// A route of one VM, cloned for each connection: a second connection,
    /// finding no room while the first holds the VM, fails unanswered once its
    /// wait ran out; a connection after the first ended gets the VM. The
    /// clones, restored seconds after the snapshot, tell the time of the moment
    /// and random values of their own.
    async fn capped(&self, conn_id: &mut i64) {
        let mut next = || {
            *conn_id += 1;
            *conn_id
        };
        // the snapshot (taken at startup) seconds old: a clock resumed from it
        // shows
        tokio::time::sleep(Duration::from_secs(2)).await;
        let io = TokioIo::new(self.tunnel.connect(next(), &self.address));
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(connection);
        let request = hyper::Request::get("/")
            .header("host", self.host)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), 200, "capped.local: the first connection");
        let first = response.into_body().collect().await.unwrap().to_bytes();
        let first = String::from_utf8_lossy(&first).into_owned();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let guest: f64 = field(&first, "time: ").parse().unwrap();
        assert!(
            (now - guest).abs() < 1.0,
            "the clone's clock is {:.3}s off: {first}",
            now - guest
        );

        let refused = self.half_closed(next()).await;
        assert!(refused.is_empty(), "no room, no answer: {refused:?}");

        // the first connection ends, and its VM with it
        drop(sender);
        let answer = self.half_closed(next()).await;
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer:?}");
        assert_ne!(
            field(&answer, "random: "),
            field(&first, "random: "),
            "two clones of one snapshot draw alike"
        );
    }

    /// Three http/1.1 requests on one connection; their bodies.
    async fn keep_alive(&self, conn_id: i64) -> Vec<String> {
        let io = TokioIo::new(self.tunnel.connect(conn_id, &self.address));
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(connection);
        let mut bodies = Vec::new();
        for path in ["/1", "/2", "/3"] {
            let request = hyper::Request::get(path)
                .header("host", self.host)
                .body(Full::new(Bytes::new()))
                .unwrap();
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), 200, "{}: GET {path}", self.host);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            bodies.push(String::from_utf8_lossy(&body).into_owned());
        }
        bodies
    }

    /// One request on a connection of its own, h2c (prior knowledge) or
    /// http/1.1: a POST of `body`, or a GET without one; the status and the
    /// answer.
    async fn request(&self, conn_id: i64, h2: bool, body: Bytes) -> (u16, String) {
        let io = TokioIo::new(self.tunnel.connect(conn_id, &self.address));
        let method = if body.is_empty() { "GET" } else { "POST" };
        let request = hyper::Request::builder()
            .method(method)
            .uri(format!("http://{}/up", self.host))
            .header("host", self.host)
            .body(Full::new(body))
            .unwrap();
        let response = if h2 {
            let (mut sender, connection) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
                    .await
                    .unwrap();
            tokio::spawn(connection);
            sender.send_request(request).await.unwrap()
        } else {
            let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
            tokio::spawn(connection);
            sender.send_request(request).await.unwrap()
        };
        let status = response.status().as_u16();
        let answer = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&answer).into_owned())
    }

    /// A GET whose client half-closes after it; the answer, read to the end.
    async fn half_closed(&self, conn_id: i64) -> String {
        let mut io = self.tunnel.connect(conn_id, &self.address);
        let request = format!("GET /half HTTP/1.1\r\nhost: {}\r\n\r\n", self.host);
        io.write_all(request.as_bytes()).await.unwrap();
        io.shutdown().await.unwrap();
        let mut answer = Vec::new();
        io.read_to_end(&mut answer).await.unwrap();
        String::from_utf8_lossy(&answer).into_owned()
    }
}

/// The value of the sample guest's line starting with `name` in `answer`.
fn field<'a>(answer: &'a str, name: &str) -> &'a str {
    answer
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .unwrap_or_else(|| panic!("no {name:?} in {answer:?}"))
}

/// The sample guest's request counts (`served by this VM: N`) in `bodies`.
fn served(bodies: &[String]) -> Vec<u64> {
    bodies
        .iter()
        .map(|body| {
            body.lines()
                .find_map(|line| line.strip_prefix("served by this VM: "))
                .and_then(|count| count.parse().ok())
                .unwrap_or_else(|| panic!("no request count in {body:?}"))
        })
        .collect()
}

/// Firecracker and the guest kernel.
fn tools() -> (PathBuf, PathBuf) {
    let firecracker = std::env::var_os("SLUICE_FIRECRACKER")
        .map(PathBuf::from)
        .unwrap_or_else(|| "firecracker".into());
    let kernel = std::env::var_os("SLUICE_FIRECRACKER_KERNEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").expect("HOME"))
                .join(".local/firecracker/vmlinux")
        });
    let runs = Command::new(&firecracker)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    assert!(
        runs,
        "/dev/kvm is usable but {} does not run: install Firecracker (README, Setup) or set SLUICE_FIRECRACKER",
        firecracker.display()
    );
    assert!(
        kernel.is_file(),
        "/dev/kvm is usable but there is no guest kernel at {}: fetch one (README, Setup) or set SLUICE_FIRECRACKER_KERNEL",
        kernel.display()
    );
    (firecracker, kernel)
}

/// The sample guest's rootfs (`scripts/build-guest.sh`), built for the tests.
fn sample_rootfs() -> PathBuf {
    let rootfs = Path::new(env!("CARGO_TARGET_TMPDIR")).join("guest.ext4");
    let output = Command::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/build-guest.sh"))
        .arg(&rootfs)
        // the guest workspace's own toolchain and target directory
        .env_remove("RUSTUP_TOOLCHAIN")
        .env_remove("CARGO")
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .expect("run scripts/build-guest.sh");
    assert!(
        output.status.success(),
        "the guest's rootfs: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    rootfs
}

/// A wicket process; stopped on drop as by Ctrl-C, its VMs with it.
struct Wicket {
    child: Child,
    log: PathBuf,
}

impl Wicket {
    fn start(server: &str, firecracker: &Path, kernel: &Path, args: &[String]) -> Wicket {
        let log = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("wicket-{}.log", std::process::id()));
        let file = std::fs::File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-firecracker"))
            // the drain is the tunnel's, tested there: Ctrl-C ends the wicket at once
            .args(["--server", server, "--drain-grace", "0", "--firecracker"])
            .arg(firecracker)
            .arg("--kernel")
            .arg(kernel)
            .args(args)
            .stdin(Stdio::null())
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .expect("run sluice-wicket-firecracker");
        Wicket { child, log }
    }

    /// Its output so far.
    fn log(&self) -> String {
        format!(
            "wicket log:\n{}",
            std::fs::read_to_string(&self.log).unwrap_or_default()
        )
    }
}

impl Drop for Wicket {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
