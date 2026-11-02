//! The wicket booting VMs, through a tunnel server double (`sluice-wicket-tunnel`'s):
//! for a shared VM (serving two hosts: one target) and a VM per connection,
//! http/1.1 with keep-alive, h2c, a 5MB POST under the tunnel's flow control
//! and a client half-closing after its request; a route of two VMs at most,
//! one per client address, failing a client's connection past its share at
//! once and the connection that finds no room in time, its idle connections
//! failed and their VMs ended, its clones restored with the clock of the
//! moment and random state of their own; the VMs'
//! output -- a shared VM's, a template's, a clone's -- on the wicket's stdout /
//! stderr and on a flume (`sluice-wicket-tunnel`'s double); the guests' egress to
//! a destination on the host, by name, address, range and IPv6, from a shared VM
//! and a clone, and its records; the routes through `--listen` as well, by host
//! (http/1.1 and h2c, an unknown one answered 404); a reload of the `--config`
//! file, polled, removing a route and replacing a rootfs; the console deploying
//! one more route from an uploaded rootfs (refusing what it does not take),
//! served on the tunnel, listed with its VMs, and an app of two hosts on one
//! rootfs uploaded by digest, its env seen by the guest, and an app with a disk:
//! what it wrote kept across a restart of its VM, a failed start, a growth and
//! its app's removal, one VM at a time on it; both served again after a restart
//! from the console's directory (not with the image: its directory is the test's).
//!
//! Skipped where `/dev/kvm` is not usable. Elsewhere it needs Firecracker
//! (`SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER`, else `firecracker` on
//! the PATH) and a guest kernel (`SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL`,
//! else `~/.local/firecracker/vmlinux`), the variables of the wicket's own
//! `--firecracker` and `--kernel`; see the README's Setup. It builds the
//! sample guest's rootfs (`scripts/build-guest.sh`).
//! `SLUICE_WICKET_RUNTIME_FIRECRACKER_VMS` adds routes (`--vm` values,
//! whitespace-separated, e.g. a CNB image's rootfs), whose answers are checked
//! by their status alone.
//!
//! `SLUICE_WICKET_RUNTIME_FIRECRACKER_IMAGE=<image>` runs the wicket as that
//! Docker image (`Dockerfile`) instead, with its Firecracker and kernel, as
//! the README runs it: `/dev/kvm` and its group alone, Docker's default
//! seccomp profile, the state dir on a tmpfs.
//!
//! `SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL=1` runs it in jail mode instead,
//! started as root (`sudo -n`, skipped without it), the wicket as the test's user,
//! and checks the jails: each VM a `firecracker` of an id of its own, PID 1 of its
//! namespace, without privileges, under seccomp filters of the keeper's, its own and
//! Firecracker's, in a cgroup limiting its memory (and a route's
//! clones' CPU, `cpu`, theirs alone with the cpu controller); the VMs and the
//! wicket ending with the keeper (SIGKILL).
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use sha2::Digest as _;
use sluice_wicket_tunnel::flume::testing::{FeedStream, flume_server};
use sluice_wicket_tunnel::proto::{AccessFilter, feed_message};
use sluice_wicket_tunnel::session::testing::{ServerTunnel, dial_address, tunnel_server};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Hang guard of every wait; the first covers the boots and the snapshot.
const HANG: Duration = Duration::from_secs(120);

/// The POST's body.
const UPLOAD: usize = 5_000_000;

/// Jail mode's ids.
const JAIL_IDS: (u32, u32) = (1_900_000_000, 1_900_000_127);

/// The image's user (distroless `nonroot`).
const NONROOT: u32 = 65532;

/// Jail mode: clone.local's `cpu`, percent of a host CPU.
const CLONE_CPU: u32 = 50;

/// capped.local's `idle-ms`: above what its checks take before its VMs idle out.
const CAPPED_IDLE: Duration = Duration::from_secs(5);

/// reuse.local's and pool.local's `keep-ms`: above what a check of theirs takes
/// between connections.
const KEEP: Duration = Duration::from_millis(1500);

/// The console's token.
const CONSOLE_TOKEN: &str = "c0ns0le";

/// The launcher (`--launch-vm`) execs the program with the soft open file
/// limit given (Firecracker grows its file table to the limit at its start, a
/// cost that grows with it: 3ms at 1048576), the file size given (a guest's
/// output to its VM's log bounded) and no core (of a VM's memory); holding the
/// disk's lock for it, across the exec, or refusing to start while another does.
#[test]
fn the_launcher_execs_with_the_limits_given() {
    use std::os::fd::AsRawFd as _;
    let launch = |disk: &str, program: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_sluice-wicket-runtime-firecracker"))
            .args([
                "--launch-vm",
                &std::process::id().to_string(),
                "1100",
                "1048576",
                disk,
            ])
            .args(program)
            .output()
            .unwrap()
    };
    let disk = std::env::temp_dir().join(format!("sfc-launch-{}.ext4", std::process::id()));
    std::fs::write(&disk, b"").unwrap();
    let file = disk.clone();
    let disk = disk.to_str().unwrap();
    // the program's own files: the disk among them, while the lock is held by it alone
    let held = launch(
        disk,
        &["/bin/sh", "-c", "ls -l /proc/self/fd; cat /proc/locks"],
    );
    let files = String::from_utf8_lossy(&held.stdout);
    assert!(held.status.success(), "{held:?}");
    assert!(files.contains(disk), "not inherited: {files}");
    assert!(files.contains("FLOCK"), "not locked: {files}");
    let holder = std::fs::File::open(disk).unwrap();
    // SAFETY: flock of an open file
    assert_eq!(
        unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX) },
        0,
        "given up as the program ended"
    );
    let refused = launch(disk, &["/bin/true"]);
    assert!(!refused.status.success(), "{refused:?}");
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(error.contains("in use by another VM"), "{error}");
    drop(holder);
    std::fs::remove_file(&file).unwrap();

    let output = launch("-", &["/bin/cat", "/proc/self/limits"]);
    assert!(output.status.success(), "{output:?}");
    let limits = String::from_utf8_lossy(&output.stdout);
    let limit = |name: &str| -> Vec<String> {
        let line = limits.lines().find(|line| line.starts_with(name)).unwrap();
        line[name.len()..]
            .split_whitespace()
            .take(2)
            .map(String::from)
            .collect()
    };
    assert_eq!(limit("Max open files")[0], "1100", "{limits}");
    assert_eq!(limit("Max file size"), ["1048576", "1048576"], "{limits}");
    assert_eq!(limit("Max core file size"), ["0", "0"], "{limits}");
}

#[tokio::test(flavor = "multi_thread")]
async fn every_route_serves_http_through_the_tunnel() {
    let jailed = std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL")
        .is_some_and(|jail| !jail.is_empty());
    if jailed {
        // the jails' VMs open nodes of their own, the keeper is root
        let sudo = Command::new("sudo").args(["-n", "true"]).status();
        if !Path::new("/dev/kvm").exists() || !sudo.is_ok_and(|sudo| sudo.success()) {
            eprintln!("skipped: jail mode needs /dev/kvm and sudo -n");
            return;
        }
    } else if std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .is_err()
    {
        eprintln!("skipped: /dev/kvm is not usable here (the kvm group: sg kvm -c 'cargo test')");
        return;
    }
    let image = std::env::var("SLUICE_WICKET_RUNTIME_FIRECRACKER_IMAGE")
        .ok()
        .filter(|image| !image.is_empty());
    let rootfs = sample_rootfs();
    let extra: Vec<String> = std::env::var("SLUICE_WICKET_RUNTIME_FIRECRACKER_VMS")
        .unwrap_or_default()
        .split_whitespace()
        .map(String::from)
        .collect();
    // the routes of the config file; the shared ones on a rootfs of their own,
    // replaced below
    let dir = Scratch::new();
    std::fs::copy(&rootfs, dir.0.join("shared.ext4")).unwrap();
    // the pool's VMs told by their files
    std::fs::copy(&rootfs, dir.0.join("pool.ext4")).unwrap();
    let (port, v6) = destination().await;
    // the shared target's egress: the host's localhost, a name the host does not
    // resolve, a range, addresses the guest routes to itself, IPv6 where the host has it
    let mut shared_egress = vec![
        format!("localhost:{port}"),
        "db.invalid:5432".to_string(),
        format!("127.0.0.0/8:{port}"),
        format!("192.0.2.7:{port}"),
        format!("'[2001:db8::/64]:{port}'"),
    ];
    if v6 {
        shared_egress.push(format!("'[::1]:{port}'"));
    }
    let shared_egress = shared_egress.join(", ");
    let config = dir.0.join("wicket.yaml");
    // jailed, the clones' CPU limited by their cgroups
    let cpu = match jailed {
        true => format!(", cpu: {CLONE_CPU}"),
        false => String::new(),
    };
    let routes = format!(
        "watch-interval: 100ms\nroutes:\n\
         - {{host: shared.local, rootfs: shared.ext4, egress: [{shared_egress}]}}\n\
         - {{host: clone.local, rootfs: {rootfs}, mode: per-connection, warm: 2{cpu}, \
         egress: [localhost:{port}]}}\n\
         # the shared target again: its VM serves both hosts\n\
         - {{host: alias.local, rootfs: shared.ext4, mode: shared, egress: [{shared_egress}]}}\n\
         - {{host: reuse.local, rootfs: {rootfs}, mode: reuse, uses: 3, keep-ms: {keep}}}\n\
         - {{host: pool.local, rootfs: pool.ext4, mode: pool, vms: 2, warm: 1, conns: 1, \
         keep-ms: {keep}}}\n",
        rootfs = rootfs.display(),
        keep = KEEP.as_millis()
    );
    let capped = format!(
        "- {{host: capped.local, rootfs: {}, mode: per-connection, vms: 2, wait-ms: 1000, \
         per-client: 1, idle-ms: {}}}\n",
        rootfs.display(),
        CAPPED_IDLE.as_millis()
    );
    std::fs::write(&config, format!("{routes}{capped}")).unwrap();
    // relative: the wicket's working directory is the test's scratch directory, jailed too
    let console_dir = dir.0.join("deployed");
    let console_args = [
        "--console",
        "127.0.0.1:0",
        "--console-token",
        CONSOLE_TOKEN,
        "--console-dir",
        "deployed",
    ]
    .map(String::from);
    let (flume, mut feeds) = flume_server().await;
    let mut args: Vec<String> = [
        "--config",
        &config.display().to_string(),
        "--client-id",
        "vm-test",
        "--flume",
        &flume,
        "--flume-token",
        "f1ume",
        "--listen",
        "127.0.0.1:0",
    ]
    .map(String::from)
    .into();
    args.extend(extra.iter().flat_map(|vm| ["--vm".to_string(), vm.clone()]));
    if image.is_none() {
        args.extend(console_args.iter().cloned());
    }
    let (url, mut streams) = tunnel_server(None).await;
    let cgroup = format!("sluice-vm-test-{}", std::process::id());
    let start = |url: &str, args: &[String]| match &image {
        Some(image) => Wicket::in_image(image, url, &dir.0, args, jailed),
        None if jailed => {
            let (firecracker, kernel) = tools();
            Wicket::jailed(url, &firecracker, &kernel, &cgroup, args)
        }
        None => {
            let (firecracker, kernel) = tools();
            Wicket::start(url, &firecracker, &kernel, args)
        }
    };
    let wicket = start(&url, &args);
    let stream = tokio::time::timeout(HANG, streams.recv())
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("no tunnel stream; {}", wicket.log()));
    let (tunnel, advertise) = ServerTunnel::new(stream).await;
    let mut feed = tokio::time::timeout(HANG, feeds.recv())
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("no feed stream; {}", wicket.log()));
    assert_eq!(feed.authorization.as_deref(), Some("Bearer f1ume"));
    let feed_message::Body::Hello(hello) = feed.next().await else {
        panic!("no hello first");
    };
    assert_eq!(hello.node_id, "wicket:vm-test");
    feed.start(0, AccessFilter::default());
    let build = advertise.build.clone().expect("the wicket's build");
    assert_eq!(build.implementation, "sluice-wicket-runtime-firecracker");
    let hosts: Vec<_> = advertise.upstreams.iter().map(|up| &up.host).collect();
    assert_eq!(hosts.len(), 6 + extra.len(), "{hosts:?}");

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
            listen: None,
        };
        let sample = !extra
            .iter()
            .any(|vm| vm.starts_with(&format!("{}=", route.host)));
        if route.host == "capped.local" {
            let checked = tokio::time::timeout(HANG, route.capped(&mut conn_id)).await;
            assert!(checked.is_ok(), "capped.local: hung; {}", wicket.log());
            continue;
        }
        if route.host == "pool.local" {
            // its VMs killed: they are the wicket's children unjailed
            let pid = (!jailed && image.is_none()).then(|| wicket.child.id() as i32);
            let pool = dir.0.join("pool.ext4");
            let checked = tokio::time::timeout(HANG, route.pooled(&mut conn_id, pid, &pool));
            assert!(checked.await.is_ok(), "pool.local: hung; {}", wicket.log());
        }
        let before = counts.entry(upstream.target_url.clone()).or_insert(0);
        let checked = tokio::time::timeout(HANG, route.check(&mut conn_id, sample, before)).await;
        assert!(checked.is_ok(), "{}: hung; {}", route.host, wicket.log());
        if route.host == "reuse.local" {
            let checked = tokio::time::timeout(HANG, route.reused(&mut conn_id)).await;
            assert!(checked.is_ok(), "reuse.local: hung; {}", wicket.log());
        }
    }

    // through --listen, by host: the shared VM counting on (by Host, then by h2c
    // :authority), a clone of its own, an unknown host answered 404
    let listening = wicket.listening();
    let via = |host| Route {
        tunnel: &tunnel,
        host,
        address: String::new(),
        listen: Some(listening),
    };
    let shared = counts.get_mut(&target("shared.local")).unwrap();
    let listened = async {
        let base = *shared;
        let first = via("shared.local").keep_alive(0).await;
        assert_eq!(served(&first), [base + 1, base + 2, base + 3], "{first:?}");
        let (status, body) = via("alias.local").request(0, true, Bytes::new()).await;
        assert_eq!((status, served(&[body])), (200, vec![base + 4]));
        *shared += 4;
        assert_eq!(served(&via("clone.local").keep_alive(0).await), [1, 2, 3]);
        for h2 in [false, true] {
            let refused = via("unknown.local").request(0, h2, Bytes::new()).await;
            let message = "no firecracker route for host 'unknown.local'";
            assert_eq!(refused, (404, message.to_string()), "h2 {h2}");
        }
    };
    assert!(
        tokio::time::timeout(HANG, listened).await.is_ok(),
        "--listen: hung; {}",
        wicket.log()
    );

    let egressed = egress(&tunnel, &target, &mut conn_id, &wicket, port, v6);
    assert!(
        tokio::time::timeout(HANG, egressed).await.is_ok(),
        "the guests' egress: hung; {}",
        wicket.log()
    );

    let egress = format!("localhost:{port}");
    let output = output(&tunnel, &target, &mut conn_id, &wicket, &mut feed, &egress);
    assert!(
        tokio::time::timeout(HANG, output).await.is_ok(),
        "the VMs' output: hung; {}",
        wicket.log()
    );

    // reloads, once a poll found the files quiet: the config file's alone
    // (capped.local removed), then the shared rootfs replaced
    let mut tunnel = tunnel;
    std::fs::write(&config, &routes).unwrap();
    let reloaded = tunnel.advertise().await;
    let hosts: Vec<_> = reloaded.upstreams.iter().map(|up| &up.host).collect();
    assert!(!hosts.contains(&&"capped.local".to_string()), "{hosts:?}");
    assert_eq!(hosts.len(), 5 + extra.len(), "{hosts:?}");
    std::fs::copy(&rootfs, dir.0.join("shared.ext4.new")).unwrap();
    std::fs::rename(dir.0.join("shared.ext4.new"), dir.0.join("shared.ext4")).unwrap();
    let shared = Route {
        tunnel: &tunnel,
        host: "shared.local",
        address: dial_address(&target("shared.local")),
        listen: None,
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
        listen: None,
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
    if image.is_none() {
        let deployed = deploy(
            &mut tunnel,
            &wicket,
            &rootfs,
            &console_dir,
            &mut conn_id,
            jailed,
        );
        assert!(
            tokio::time::timeout(HANG, deployed).await.is_ok(),
            "the console: hung; {}",
            wicket.log()
        );
    }
    match (jailed, &image) {
        // the image's keeper in the container's cgroup, the wicket its nonroot user
        (true, Some(_)) => jails(&wicket, None, NONROOT),
        (true, None) => {
            assert!(
                tokio::time::timeout(HANG, settled(&wicket)).await.is_ok(),
                "the VMs did not settle: {}",
                wicket.log()
            );
            // SAFETY: getuid
            jails(&wicket, Some(&cgroup), unsafe { libc::getuid() })
        }
        // the wicket's seccomp filter, on the test's (if any)
        (false, None) => assert_eq!(
            filters(wicket.child.id() as i32),
            filters(std::process::id() as i32) + 1,
            "the wicket's filter"
        ),
        (false, Some(_)) => {}
    }

    // the console's route after a restart: started again from its directory alone
    if image.is_none() {
        drop(wicket);
        let (url, mut streams) = tunnel_server(None).await;
        let wicket = start(&url, &console_args);
        let stream = tokio::time::timeout(HANG, streams.recv())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("no tunnel stream after the restart; {}", wicket.log()));
        let (mut tunnel, advertise) = ServerTunnel::new(stream).await;
        let hosts: Vec<_> = advertise.upstreams.iter().map(|up| &up.host).collect();
        assert_eq!(
            hosts,
            ["console.local", "app1.local", "app2.local", "data.local"],
            "{}",
            wicket.log()
        );
        let kept = disk_kept(&mut tunnel, &wicket, &console_dir, &advertise, jailed);
        assert!(
            tokio::time::timeout(HANG, kept).await.is_ok(),
            "the disk after a restart: hung; {}",
            wicket.log()
        );
        let app = Route {
            tunnel: &tunnel,
            host: "app2.local",
            address: dial_address(&advertise.upstreams[2].target_url),
            listen: None,
        };
        let env = tokio::time::timeout(HANG, app.get(2, "/env")).await;
        let env = env.unwrap_or_else(|_| panic!("app2.local: hung; {}", wicket.log()));
        assert_eq!(env, "GREETING=hello world\nPEM=a\nb\n");
        let (status, removed) = console(
            wicket.console(),
            "DELETE",
            "/api/apps/shop?owner=cf",
            true,
            Bytes::new(),
        )
        .await;
        assert_eq!(
            (status, removed["message"].as_str()),
            (200, Some("removed app1.local, app2.local"))
        );
        let again = Route {
            tunnel: &tunnel,
            host: "console.local",
            address: dial_address(&advertise.upstreams[0].target_url),
            listen: None,
        };
        let served_again = tokio::time::timeout(HANG, again.keep_alive(1)).await;
        let bodies =
            served_again.unwrap_or_else(|_| panic!("console.local: hung; {}", wicket.log()));
        assert_eq!(served(&bodies), [1, 2, 3]);
    }
}

/// A request to the console at `address`, with its token or not: the status and
/// the JSON answered.
async fn console(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: bool,
    body: Bytes,
) -> (u16, serde_json::Value) {
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    let mut request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", address.to_string());
    if token {
        request = request.header("authorization", format!("Bearer {CONSOLE_TOKEN}"));
    }
    let response = sender
        .send_request(request.body(Full::new(body)).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

/// The console deploying `rootfs` as console.local, cloned per connection: refused
/// without the token, on a host the config file routes and for a body that is no
/// ext4 image; then advertised on the image kept below `dir` under its digest,
/// served on the tunnel, listed with its VMs and its template's snapshot, written to
/// `dir`'s routes.yaml.
async fn deploy(
    tunnel: &mut ServerTunnel,
    wicket: &Wicket,
    rootfs: &Path,
    dir: &Path,
    conn_id: &mut i64,
    jailed: bool,
) {
    let at = wicket.console();
    let (status, _) = console(at, "GET", "/api/routes", false, Bytes::new()).await;
    assert_eq!(status, 401, "no token");
    let taken = console(
        at,
        "PATCH",
        "/api/routes/shared.local?mem=256",
        true,
        Bytes::new(),
    );
    let (status, refused) = taken.await;
    assert_eq!(status, 409, "{refused}");
    let path = "/api/routes/console.local?name=app.ext4&mode=per-connection&warm=1";
    let not_image = Bytes::from(vec![0u8; 4096]);
    let (status, refused) = console(at, "PUT", path, true, not_image).await;
    assert_eq!(
        (status, refused["error"].as_str()),
        (
            400,
            Some("not an ext4 image (no ext2/3/4 superblock magic at byte 1080)")
        )
    );
    let image = Bytes::from(std::fs::read(rootfs).unwrap());
    let (status, deployed) = console(at, "PUT", path, true, image.clone()).await;
    assert_eq!(status, 200, "{deployed}; {}", wicket.log());
    assert_eq!(deployed["message"], "added console.local");

    let digests: Vec<PathBuf> = std::fs::read_dir(dir.join("rootfs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    let [digest] = &digests[..] else {
        panic!("one image kept: {digests:?}");
    };
    let kept = digest.join("app.ext4");
    assert_eq!(std::fs::read(&kept).unwrap(), image, "kept as it came");
    let routes = std::fs::read_to_string(dir.join("routes.yaml")).unwrap();
    assert!(routes.contains("host: \"console.local\""), "{routes}");

    let advertised = tunnel.advertise().await;
    let up = advertised
        .upstreams
        .iter()
        .find(|up| up.host == "console.local");
    let target = up.expect("console.local advertised").target_url.clone();
    let kept = kept.canonicalize().unwrap();
    assert_eq!(
        target,
        format!("firecracker:{}?mode=per-connection&warm=1", kept.display())
    );
    *conn_id += 1;
    let route = Route {
        tunnel,
        host: "console.local",
        address: dial_address(&target),
        listen: None,
    };
    assert_eq!(served(&route.keep_alive(*conn_id).await), [1, 2, 3]);

    let (status, listing) = console(at, "GET", "/api/routes", true, Bytes::new()).await;
    assert_eq!(status, 200, "{listing}");
    let routes = listing["routes"].as_array().unwrap();
    let route = |host: &str| {
        routes
            .iter()
            .find(|route| route["host"] == host)
            .unwrap_or_else(|| panic!("{host} not listed: {listing}"))
    };
    let deployed = route("console.local");
    assert_eq!(deployed["origin"], "console");
    assert_eq!(
        deployed["rootfs"]["sha256"].as_str(),
        digest.file_name().and_then(|name| name.to_str())
    );
    assert_eq!(
        deployed["options"],
        serde_json::json!({"mode": "per-connection", "warm": "1"})
    );
    assert!(
        deployed["snapshot"]["memory_bytes"].as_u64() > Some(0),
        "{deployed}"
    );
    assert_eq!(route("shared.local")["origin"], "config");
    assert!(route("shared.local")["vms"]["shared"].as_bool().unwrap());
    let budget = &listing["budget"];
    assert!(budget["vms"].as_u64() >= Some(1), "{budget}");
    assert_eq!(budget["max_vms"], 64);

    // an app: refused until its image is uploaded under its digest, then two hosts on
    // one rootfs and VMs of its own, its env seen by the guest and listed by its names
    let mut other = image.to_vec();
    other.extend_from_slice(&[0; 4096]);
    let image = Bytes::from(other);
    let sha256: String = sha2::Sha256::digest(&image)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let app = serde_json::json!({
        "owner": "cf",
        "hosts": ["app2.local", "app1.local"],
        "component": {"sha256": sha256, "name": "app.ext4"},
        "sandbox": {"mem": 128, "env": {"GREETING": "hello world", "PEM": "a\nb"}},
        "tag": "t1",
    });
    let body = || Bytes::from(app.to_string());
    let (status, refused) = console(at, "PUT", "/api/apps/shop", true, body()).await;
    assert_eq!(
        (status, &refused["missing"][0]),
        (409, &sha256.as_str().into())
    );
    let wrong = format!("/api/components/{}", "0".repeat(64));
    let (status, refused) = console(at, "PUT", &wrong, true, image.clone()).await;
    assert_eq!(status, 400, "{refused}");
    let upload = format!("/api/components/{sha256}");
    let (status, uploaded) = console(at, "PUT", &upload, true, image.clone()).await;
    assert_eq!(
        (status, uploaded["bytes"].as_u64()),
        (200, Some(image.len() as u64)),
        "{uploaded}"
    );
    assert!(dir.join(format!("uploads/{sha256}.ext4")).is_file());
    let (status, put) = console(at, "PUT", "/api/apps/shop", true, body()).await;
    assert_eq!(status, 200, "{put}; {}", wicket.log());
    assert_eq!(put["message"], "added app1.local, app2.local");
    let advertised = tunnel.advertise().await;
    let target = |host: &str| {
        let up = advertised.upstreams.iter().find(|up| up.host == host);
        up.unwrap_or_else(|| panic!("{host} not advertised"))
            .target_url
            .clone()
    };
    assert_eq!(target("app1.local"), target("app2.local"), "one target");
    assert!(
        target("app1.local").ends_with("/app.ext4?app=shop"),
        "{}",
        target("app1.local")
    );
    for host in ["app1.local", "app2.local"] {
        *conn_id += 1;
        let route = Route {
            tunnel,
            host,
            address: dial_address(&target(host)),
            listen: None,
        };
        assert_eq!(
            route.get(*conn_id, "/env").await,
            "GREETING=hello world\nPEM=a\nb\n"
        );
    }
    let (status, apps) = console(at, "GET", "/api/apps", true, Bytes::new()).await;
    assert_eq!(status, 200, "{apps}");
    let listed = &apps["apps"][0];
    assert_eq!(apps["runtime"], "firecracker");
    assert_eq!(
        (&listed["name"], &listed["owner"], &listed["tag"]),
        (&"shop".into(), &"cf".into(), &"t1".into())
    );
    assert_eq!(
        listed["hosts"],
        serde_json::json!(["app1.local", "app2.local"])
    );
    assert_eq!(listed["component"], app["component"]);
    assert_eq!(
        listed["sandbox"],
        serde_json::json!({"mode": "shared", "vcpus": 1, "mem": 128, "env": ["GREETING", "PEM"]})
    );
    // kept for its owner's eyes alone, and to a restart; another owner changes nothing
    let manifest = dir.join("routes.yaml");
    assert_eq!(
        std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(&manifest).unwrap().permissions()
        ) & 0o777,
        0o600
    );
    assert!(
        std::fs::read_to_string(&manifest)
            .unwrap()
            .contains("hello world")
    );
    let (status, refused) = console(
        at,
        "DELETE",
        "/api/apps/shop?owner=other",
        true,
        Bytes::new(),
    )
    .await;
    assert_eq!(status, 409, "{refused}");
    let (status, refused) =
        console(at, "DELETE", "/api/routes/app1.local", true, Bytes::new()).await;
    assert_eq!(status, 409, "{refused}");
    let (status, same) = console(at, "PUT", "/api/apps/shop", true, body()).await;
    assert_eq!((status, same["message"].as_str()), (200, Some("no change")));

    disk_app(tunnel, wicket, &sha256, dir, conn_id, jailed).await;
}

/// The app `data` with `sandbox`, of the image `sha256`, on the wicket at `at`.
async fn put_data(
    at: std::net::SocketAddr,
    sha256: &str,
    sandbox: serde_json::Value,
) -> (u16, serde_json::Value) {
    let app = serde_json::json!({
        "owner": "cf",
        "hosts": ["data.local"],
        "component": {"sha256": sha256, "name": "app.ext4"},
        "sandbox": sandbox,
    });
    console(
        at,
        "PUT",
        "/api/apps/data",
        true,
        Bytes::from(app.to_string()),
    )
    .await
}

/// A POST to the app `data` at `address` on a connection of its own; its answer.
async fn on_data(
    tunnel: &ServerTunnel,
    address: &str,
    conn_id: &mut i64,
    path: &str,
    body: &str,
) -> String {
    *conn_id += 1;
    let route = Route {
        tunnel,
        host: "data.local",
        address: address.to_string(),
        listen: None,
    };
    route.post(*conn_id, path, body).await
}

/// An app with a disk: refused in a cloned mode; made once, what the guest wrote kept across
/// its VM's restart, fsynced or not (the stop writes it out), a start that fails (the old VM
/// started again on it) and its growth, a smaller size refused; for the app's VM alone, its
/// id's in a jail; listed, and not removed under its app. One more file left unsynced: the
/// wicket's end writes it out.
async fn disk_app(
    tunnel: &mut ServerTunnel,
    wicket: &Wicket,
    sha256: &str,
    dir: &Path,
    conn_id: &mut i64,
    jailed: bool,
) {
    use std::os::unix::fs::MetadataExt as _;
    let at = wicket.console();
    let (status, refused) = put_data(
        at,
        sha256,
        serde_json::json!({"mode": "pool", "vms": 2, "disk": "/tmp/data"}),
    )
    .await;
    assert_eq!(status, 400, "{refused}");
    let why = refused["error"].as_str().unwrap();
    assert!(why.contains("disk needs mode=shared: one VM"), "{why}");

    let sandbox = |mib: u32, generation: &str| serde_json::json!({"disk": "/tmp/data", "disk-mib": mib, "env": {"RUN": generation}});
    let (status, put) = put_data(at, sha256, sandbox(64, "1")).await;
    assert_eq!(status, 200, "{put}; {}", wicket.log());
    assert_eq!(put["message"], "added data.local");
    let advertised = tunnel.advertise().await;
    let target_of = |advertised: &sluice_wicket_tunnel::proto::Advertise| {
        let up = advertised
            .upstreams
            .iter()
            .find(|up| up.host == "data.local");
        dial_address(&up.expect("data.local advertised").target_url)
    };
    let mut address = target_of(&advertised);
    let answer = on_data(tunnel, &address, conn_id, "/disk", "").await;
    let mib = |answer: &str| -> u64 {
        let line = answer.lines().next().unwrap_or_default();
        line.strip_prefix("mib ")
            .and_then(|mib| mib.parse().ok())
            .unwrap_or(0)
    };
    let first_size = mib(&answer);
    assert!((40..=64).contains(&first_size), "{answer}");
    assert!(answer.ends_with("uid 0 gid 0\n"), "{answer}");
    for (path, body) in [
        ("/disk?write=a&sync=1", "fsynced"),
        ("/disk?write=b", "written"),
    ] {
        let wrote = on_data(tunnel, &address, conn_id, path, body).await;
        assert!(wrote.starts_with("wrote "), "{wrote}");
    }
    let file = dir.join("disks/cf/data.ext4");
    let made = std::fs::metadata(&file).unwrap();
    assert_eq!(made.len(), 64 << 20);
    match jailed {
        true => {
            assert!((JAIL_IDS.0..=JAIL_IDS.1).contains(&made.uid()), "{made:?}");
            assert_eq!(made.mode() & 0o777, 0o660);
        }
        false => assert_eq!(made.mode() & 0o777, 0o600),
    }

    // its VM again (another env, the same target): a stop, then a boot, all kept
    let (status, put) = put_data(at, sha256, sandbox(64, "2")).await;
    assert_eq!(
        (status, put["message"].as_str()),
        (200, Some("restarted data.local")),
        "{put}"
    );
    let read = on_data(tunnel, &address, conn_id, "/disk?read=a", "").await;
    assert_eq!(read, "fsynced");
    let read = on_data(tunnel, &address, conn_id, "/disk?read=b", "").await;
    assert_eq!(read, "written", "the stop wrote it out");

    // a start that fails, a disk nowhere to go, its file grown by then: refused, the old VM
    // started again on it, of the size the file has now
    let (status, refused) = put_data(
        at,
        sha256,
        serde_json::json!({"disk": "/no/such", "disk-mib": 96, "env": {"RUN": "2"}}),
    )
    .await;
    assert_eq!(status, 422, "{refused}");
    let why = refused["error"].as_str().unwrap();
    assert!(why.contains("Read-only file system"), "{why}");
    let read = on_data(tunnel, &address, conn_id, "/disk?read=a", "").await;
    assert_eq!(read, "fsynced", "the old VM, on the disk it left");
    assert_eq!(std::fs::metadata(&file).unwrap().len(), 96 << 20);
    let answer = on_data(tunnel, &address, conn_id, "/disk", "").await;
    assert!(mib(&answer) > first_size + 16, "{first_size}: {answer}");

    // grown: another target, its file and file system larger; smaller: refused before its VM
    // is touched
    let (status, put) = put_data(at, sha256, sandbox(128, "2")).await;
    assert_eq!(status, 200, "{put}; {}", wicket.log());
    address = target_of(&tunnel.advertise().await);
    assert_eq!(std::fs::metadata(&file).unwrap().len(), 128 << 20);
    let answer = on_data(tunnel, &address, conn_id, "/disk", "").await;
    assert!(mib(&answer) > first_size + 32, "{first_size}: {answer}");
    let booted = on_data(tunnel, &address, conn_id, "/", "").await;
    let booted = field(&booted, "boot id").to_string();
    let (status, refused) = put_data(at, sha256, sandbox(64, "2")).await;
    assert_eq!(status, 422, "{refused}");
    let why = refused["error"].as_str().unwrap();
    assert!(why.contains("a disk only grows"), "{why}");
    let same = on_data(tunnel, &address, conn_id, "/", "").await;
    assert_eq!(field(&same, "boot id"), booted, "its VM untouched");

    // a wedged guest (its children, unjailed): its stop gives up in time, the VM killed, the
    // next one started on what it had made durable
    if !jailed {
        let held = vms_on(wicket.child.id() as i32, &file);
        let [vm] = held[..] else {
            panic!("one VM on the disk: {held:?}");
        };
        // SAFETY: a plain kill of the wicket's child
        assert_eq!(unsafe { libc::kill(vm, libc::SIGSTOP) }, 0);
        let (status, put) = put_data(at, sha256, sandbox(128, "3")).await;
        assert_eq!(
            (status, put["message"].as_str()),
            (200, Some("restarted data.local")),
            "{put}"
        );
        let read = on_data(tunnel, &address, conn_id, "/disk?read=a", "").await;
        assert_eq!(read, "fsynced");
    }

    // listed; under its app, not removed
    let (status, apps) = console(at, "GET", "/api/apps", true, Bytes::new()).await;
    assert_eq!(status, 200, "{apps}");
    let listed = apps["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|app| app["name"] == "data")
        .unwrap();
    assert_eq!(listed["sandbox"]["disk"], "/tmp/data");
    assert_eq!(listed["sandbox"]["disk-mib"], 128);
    let (status, disks) = console(at, "GET", "/api/disks", true, Bytes::new()).await;
    assert_eq!(status, 200, "{disks}");
    assert_eq!(
        disks["disks"],
        serde_json::json!([{"owner": "cf", "name": "data", "mib": 128, "app": true}])
    );
    let gone = "/api/disks/data?owner=cf";
    let (status, refused) = console(at, "DELETE", gone, true, Bytes::new()).await;
    assert_eq!(status, 409, "{refused}");

    // for the wicket's end to write out, and the restart to find
    let wrote = on_data(tunnel, &address, conn_id, "/disk?write=c", "at the end").await;
    assert!(wrote.starts_with("wrote "), "{wrote}");
}

/// The disk after the wicket's restart (a kill of the keeper, when `jailed`; else its end):
/// what the app wrote; its app removed, it stays for the next app of that name and owner, and
/// goes once no VM holds it.
async fn disk_kept(
    tunnel: &mut ServerTunnel,
    wicket: &Wicket,
    dir: &Path,
    advertised: &sluice_wicket_tunnel::proto::Advertise,
    jailed: bool,
) {
    let at = wicket.console();
    let up = advertised
        .upstreams
        .iter()
        .find(|up| up.host == "data.local");
    let address = dial_address(&up.expect("data.local advertised").target_url);
    let mut conn_id = 10;
    let read = on_data(tunnel, &address, &mut conn_id, "/disk?read=a", "").await;
    assert_eq!(read, "fsynced");
    if !jailed {
        let read = on_data(tunnel, &address, &mut conn_id, "/disk?read=c", "").await;
        assert_eq!(read, "at the end");
    }
    let sha256 = up.unwrap().target_url.split("/rootfs/").nth(1).unwrap();
    let sha256 = sha256.split('/').next().unwrap().to_string();
    let path = "/api/apps/data?owner=cf";
    let (status, removed) = console(at, "DELETE", path, true, Bytes::new()).await;
    assert_eq!(status, 200, "{removed}");
    tunnel.advertise().await;
    let (_, disks) = console(at, "GET", "/api/disks", true, Bytes::new()).await;
    assert_eq!(disks["disks"][0]["app"], false, "{disks}");

    // the next app finds it; the VM left of the last is stopped for it
    let (status, put) = put_data(
        at,
        &sha256,
        serde_json::json!({"disk": "/tmp/data", "disk-mib": 128}),
    )
    .await;
    assert_eq!(status, 200, "{put}; {}", wicket.log());
    let advertised = tunnel.advertise().await;
    let up = advertised
        .upstreams
        .iter()
        .find(|up| up.host == "data.local");
    let address = dial_address(&up.expect("data.local advertised").target_url);
    let read = on_data(tunnel, &address, &mut conn_id, "/disk?read=a", "").await;
    assert_eq!(read, "fsynced");

    let (status, removed) = console(at, "DELETE", path, true, Bytes::new()).await;
    assert_eq!(status, 200, "{removed}");
    tunnel.advertise().await;
    let deadline = Instant::now() + HANG;
    let gone = "/api/disks/data?owner=cf";
    loop {
        let (status, answer) = console(at, "DELETE", gone, true, Bytes::new()).await;
        match status {
            200 => break,
            // its VM stopping
            409 => assert!(Instant::now() < deadline, "{answer}"),
            _ => panic!("{status}: {answer}"),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!dir.join("disks/cf/data.ext4").exists());
    let (status, none) = console(at, "DELETE", gone, true, Bytes::new()).await;
    assert_eq!(status, 404, "{none}");
}

/// The field `name` of the status of `pid` (empty: none, or no such process).
fn status(pid: i32, name: &str) -> String {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    status
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .map(|value| value.trim().to_string())
        .unwrap_or_default()
}

/// The seccomp filters of `pid`'s main thread.
fn filters(pid: i32) -> u32 {
    status(pid, "Seccomp_filters:").parse().unwrap_or(0)
}

/// The keeper's children: its VMs (an id of `JAIL_IDS`), and the others.
fn jailed_children(keeper: i32) -> (Vec<i32>, Vec<i32>) {
    children(keeper).into_iter().partition(|pid| {
        let uid: u32 = status(*pid, "Uid:")
            .split_whitespace()
            .next()
            .and_then(|uid| uid.parse().ok())
            .unwrap_or(0);
        (JAIL_IDS.0..=JAIL_IDS.1).contains(&uid)
    })
}

/// Until `deploy`'s connection's clone is gone (it ends after the connection, its output
/// first) and the keeper's VMs are the ones the budget counts (it counts one no longer once
/// it asked for its end): `jails` reads each VM, and one ending fails it.
async fn settled(wicket: &Wicket) {
    let keeper = wicket.keeper();
    loop {
        let (_, listing) =
            console(wicket.console(), "GET", "/api/routes", true, Bytes::new()).await;
        let routes = listing["routes"].as_array().unwrap();
        let deployed = routes.iter().find(|route| route["host"] == "console.local");
        let clones = &deployed.expect("console.local listed")["vms"];
        let vms = jailed_children(keeper).0.len() as u64;
        if clones["alive"] == 1 && clones["warm"] == 1 && listing["budget"]["vms"] == vms {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The jails of the VMs alive (the shared VM and the warm clones): each a
/// `firecracker` of an id of its own, PID 1 of its namespace, without capability or
/// new privileges, its seccomp filter on, in a cgroup of the keeper's limiting its
/// memory, and the CPU of clone.local's alone (under `cgroup`, else beside the keeper's
/// own: its default), on aarch64 the
/// host CPU's files Firecracker reads in it; the wicket as `user`. The keeper killed (SIGKILL), they and the wicket end with it (their parent
/// death signal); its files and cgroups are removed here (a container's go with it).
fn jails(wicket: &Wicket, cgroup: Option<&str>, user: u32) {
    let keeper = wicket.keeper();
    // the keeper's seccomp filter (the container's under it, if any), the wicket's and
    // each jail's on it, Firecracker's on the jail's
    assert_eq!(status(keeper, "Seccomp:"), "2", "the keeper's filter");
    let cgroup_of = |pid: i32| {
        let own = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap_or_default();
        own.trim()
            .strip_prefix("0::/")
            .unwrap_or_default()
            .to_string()
    };
    // the keeper leaves its own for a leaf of it (`keeper`), the VMs' cgroups beside
    let cgroup = cgroup.map(String::from).unwrap_or_else(|| {
        let own = cgroup_of(keeper);
        own.strip_suffix("/keeper").unwrap_or(&own).to_string()
    });
    let user = user.to_string();
    let (vms, others) = jailed_children(keeper);
    assert!(
        vms.len() >= 2,
        "the shared VM and warm clones: {vms:?}; {}",
        wicket.log()
    );
    let mut ids = Vec::new();
    let mut cpu_limited = 0;
    for vm in &vms {
        let id = status(*vm, "Uid:");
        assert!(
            id.split_whitespace()
                .all(|each| Some(each) == id.split_whitespace().next()),
            "{vm}: {id}"
        );
        assert_eq!(status(*vm, "Gid:"), id, "{vm}: its gid its uid");
        assert_eq!(status(*vm, "Groups:"), "", "{vm}: no supplementary group");
        assert!(
            status(*vm, "NSpid:").ends_with("\t1"),
            "{vm}: PID 1 of its namespace"
        );
        for (name, value) in [
            ("CapEff:", "0000000000000000"),
            ("CapBnd:", "0000000000000000"),
            ("NoNewPrivs:", "1"),
            ("Seccomp:", "2"),
        ] {
            assert_eq!(status(*vm, name), value, "{vm}: {name}");
        }
        assert!(
            filters(*vm) >= filters(keeper) + 2,
            "{vm}: the jail's filter and Firecracker's"
        );
        let own = cgroup_of(*vm);
        assert!(
            own.starts_with(&format!("{cgroup}/")),
            "{vm}: its cgroup {own}, not in {cgroup}"
        );
        let max = std::fs::read_to_string(format!("/sys/fs/cgroup/{own}/memory.max")).unwrap();
        assert_ne!(max.trim(), "max", "{vm}: its memory limited");
        // clone.local's CPU capped, its cgroup a group of the cpu controller; the
        // others' without the controller
        let cpu = std::fs::read_to_string(format!("/sys/fs/cgroup/{own}/cpu.max"));
        match own.rsplit('/').nth(1) {
            Some("capped") => {
                let limit = format!("{} 100000", CLONE_CPU * 1000);
                assert_eq!(cpu.unwrap().trim(), limit, "{vm}");
                cpu_limited += 1;
            }
            class => {
                assert_eq!(class, Some("uncapped"), "{vm}: its cgroup {own}");
                assert!(cpu.is_err(), "{vm}: its cgroup {own} has cpu.max");
            }
        }
        // aarch64: the host CPU's files Firecracker reads, the host's in the jail
        if cfg!(target_arch = "aarch64") {
            for file in [
                "sys/devices/system/cpu/cpu0/cache/index0/level",
                "sys/devices/system/cpu/cpu0/regs/identification/midr_el1",
            ] {
                let jailed = Command::new("sudo")
                    .args(["-n", "cat"])
                    .arg(format!("/proc/{vm}/root/{file}"))
                    .output()
                    .unwrap();
                let host = std::fs::read(format!("/{file}")).unwrap();
                assert_eq!(jailed.stdout, host, "{vm}: /{file}");
            }
        }
        ids.push(id);
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), vms.len(), "an id per VM: {ids:?}");
    assert!(
        cpu_limited > 0,
        "clone.local's warm clones: their CPU limited"
    );
    assert_eq!(others.len(), 1, "the wicket: {others:?}");
    assert_eq!(
        status(others[0], "Uid:").split_whitespace().next(),
        Some(user.as_str()),
        "the wicket runs as jail-user"
    );
    assert_eq!(
        filters(others[0]),
        filters(keeper) + 1,
        "the wicket's filter on the keeper's"
    );

    let killed = Command::new("sudo")
        .args(["-n", "kill", "-KILL", &keeper.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    let ended = |pid: &i32| matches!(status(*pid, "State:").chars().next(), None | Some('Z'));
    while !vms.iter().chain(&others).all(ended) {
        assert!(
            Instant::now() < deadline,
            "VMs outlived the keeper: {vms:?}, {others:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    if wicket.container.is_none() {
        // what the keeper did not end
        let cleaned = Command::new("sudo")
            .args(["-n", "sh", "-c"])
            .arg(format!(
                "rm -rf /run/sluice-wicket-runtime-firecracker-{keeper}; \
                 find /sys/fs/cgroup/{cgroup} -depth -type d -exec rmdir {{}} +"
            ))
            .status()
            .unwrap();
        assert!(cleaned.success());
    }
}

/// The VMs of the wicket `pid` (unjailed: its children) with `rootfs` open.
fn vms_on(pid: i32, rootfs: &Path) -> Vec<i32> {
    let threads = std::fs::read_dir(format!("/proc/{pid}/task")).unwrap();
    threads
        .filter_map(|thread| thread.ok()?.file_name().to_str()?.parse().ok())
        .flat_map(|thread: i32| {
            std::fs::read_to_string(format!("/proc/{pid}/task/{thread}/children"))
                .unwrap_or_default()
                .split_whitespace()
                .filter_map(|child| child.parse().ok())
                .collect::<Vec<i32>>()
        })
        .filter(|vm| {
            std::fs::read_dir(format!("/proc/{vm}/fd")).is_ok_and(|mut fds| {
                fds.any(|fd| {
                    fd.is_ok_and(|fd| std::fs::read_link(fd.path()).is_ok_and(|to| to == rootfs))
                })
            })
        })
        .collect()
}

/// The children of `pid`.
fn children(pid: i32) -> Vec<i32> {
    std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children"))
        .unwrap_or_default()
        .split_whitespace()
        .filter_map(|child| child.parse().ok())
        .collect()
}

/// The test's egress destination: answers `got <what it read>` once its client
/// half-closed, at 127.0.0.1, 127.0.0.5 and, where the host has IPv6, ::1 (whether it
/// does), all on one port.
async fn destination() -> (u16, bool) {
    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = first.local_addr().unwrap().port();
    let mut listeners = vec![
        first,
        tokio::net::TcpListener::bind(("127.0.0.5", port))
            .await
            .unwrap(),
    ];
    let v6 = tokio::net::TcpListener::bind(("::1", port)).await;
    let v6 = v6.map(|listener| listeners.push(listener)).is_ok();
    for listener in listeners {
        tokio::spawn(async move {
            while let Ok((mut connection, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut got = Vec::new();
                    if connection.read_to_end(&mut got).await.is_ok() {
                        let _ = connection.write_all(&[b"got ", &got[..]].concat()).await;
                    }
                });
            }
        });
    }
    (port, v6)
}

/// The guests' egress to the destination at `port` (with IPv6: `v6`): a shared VM's to
/// the host's localhost, an address of a range and IPv6's are relayed both ways, the
/// half-close passed on; a name the host does not resolve is reset in the guest, after
/// the guest resolved it and connected; addresses outside 127.0.0.0/8 are the guest's to
/// connect to; a clone's egress is its own VM's. Each is recorded on the wicket's
/// stdout, named after the route's hosts and the VM, and the guest set its rules up
/// without an error.
async fn egress(
    tunnel: &ServerTunnel,
    target: &impl Fn(&str) -> String,
    conn_id: &mut i64,
    wicket: &Wicket,
    port: u16,
    v6: bool,
) {
    let route = |host| Route {
        tunnel,
        host,
        address: dial_address(&target(host)),
        listen: None,
    };
    let mut ask = async |route: &Route<'_>, query: String, body: &str| {
        *conn_id += 1;
        route
            .post(*conn_id, &format!("/egress?{query}"), body)
            .await
    };
    let shared = route("shared.local");
    let mut relayed = vec![format!("localhost:{port}"), format!("127.0.0.5:{port}")];
    if v6 {
        relayed.push(format!("[::1]:{port}"));
    }
    for to in &relayed {
        let answer = ask(&shared, format!("to={to}"), to).await;
        assert!(
            answer.ends_with(&format!("answer: got {to}\n")),
            "{to}: {answer}"
        );
    }
    let unresolved = ask(&shared, "to=db.invalid:5432".into(), "x").await;
    assert!(
        unresolved.starts_with("connected in ") && unresolved.contains("error: Connection reset"),
        "{unresolved}"
    );
    for to in [format!("192.0.2.7:{port}"), format!("[2001:db8::9]:{port}")] {
        let connected = ask(&shared, format!("to={to}&rounds=0"), "").await;
        assert!(connected.starts_with("connected in "), "{to}: {connected}");
    }
    let to = format!("localhost:{port}");
    let cloned = ask(&route("clone.local"), format!("to={to}"), "from a clone").await;
    assert!(cloned.ends_with("answer: got from a clone\n"), "{cloned}");

    let vm = "shared.local, alias.local vm-0";
    let mut records: Vec<String> = relayed
        .iter()
        .map(|to| format!("egress [{vm}] {to} ("))
        .collect();
    records.push(format!("egress [{vm}] db.invalid:5432: failed: "));
    records.push(format!(
        "stdout [{vm}] :: guest: egress to db.invalid:5432 at 127.1.0.1, "
    ));
    let deadline = Instant::now() + HANG;
    loop {
        let log = std::fs::read_to_string(&wicket.log).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        let of_clone = lines.iter().any(|line| {
            line.strip_prefix("egress [clone.local vm-")
                .and_then(|rest| rest.split_once("] "))
                .is_some_and(|(n, rest)| {
                    n.parse::<u64>().is_ok() && rest.starts_with(&format!("{to} ("))
                })
        });
        if of_clone
            && records
                .iter()
                .all(|record| lines.iter().any(|line| line.starts_with(record.as_str())))
        {
            let errors = lines
                .iter()
                .filter(|line| line.contains(":: guest: egress ") && line.starts_with("stderr"));
            assert_eq!(errors.count(), 0, "{}", wicket.log());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the egress records: {records:?}; {}",
            wicket.log()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The VMs' output: the sample guest's line at its start and its two lines (stdout,
/// stderr) per `/log` request, of a shared VM (serving two hosts), a template and a
/// clone, reach the wicket's stdout / stderr named after the route's hosts and the VM,
/// and the flume as lines of the route's first host naming the VM. A clone's lines are
/// its own: the template's start line comes once, not again from its clones. The
/// shared VM's egress to `egress` is on the flume too.
async fn output(
    tunnel: &ServerTunnel,
    target: &impl Fn(&str) -> String,
    conn_id: &mut i64,
    wicket: &Wicket,
    feed: &mut FeedStream,
    egress: &str,
) {
    let route = |host| Route {
        tunnel,
        host,
        address: dial_address(&target(host)),
        listen: None,
    };
    *conn_id += 1;
    let shared = route("alias.local").get(*conn_id, "/log").await;
    let (served, booted) = (
        field(&shared, "served by this VM: "),
        field(&shared, "boot id: "),
    );
    *conn_id += 1;
    // a clone counts from its first request, of its template's boot id
    let cloned = route("clone.local").get(*conn_id, "/log").await;
    assert_eq!(field(&cloned, "served by this VM: "), "1", "{cloned}");
    let template = field(&cloned, "boot id: ");
    let request =
        |served: &str, boot: &str| format!("GET /log, request {served} of boot id {boot}");
    let started = |boot: &str| format!("hello server, boot id {boot}");
    let shared = "shared.local, alias.local vm-0";
    let printed = [
        format!("stdout [{shared}] :: {}", started(booted)),
        format!("stdout [{shared}] :: {}", request(served, booted)),
        format!("stderr [{shared}] :: {}", request(served, booted)),
        format!("stdout [clone.local template] :: {}", started(template)),
    ];
    // the clone's VM is the route's vm-<n>
    let of_clone = |stream: &str, line: &str| {
        let rest = line.strip_prefix(&format!("{stream} [clone.local vm-"))?;
        let (n, rest) = rest.split_once("] :: ")?;
        (rest == request("1", template) && n.parse::<u64>().is_ok()).then(|| format!("vm-{n}"))
    };
    let deadline = Instant::now() + HANG;
    let clone = loop {
        let log = std::fs::read_to_string(&wicket.log).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        let clone = lines.iter().find_map(|line| of_clone("stdout", line));
        if let Some(clone) = clone
            && printed.iter().all(|line| lines.contains(&line.as_str()))
            && lines
                .iter()
                .any(|line| of_clone("stderr", line).as_ref() == Some(&clone))
        {
            let again = started(template);
            let starts = lines.iter().filter(|line| line.ends_with(&again)).count();
            assert_eq!(starts, 1, "the template's start, once; {}", wicket.log());
            break clone;
        }
        assert!(
            Instant::now() < deadline,
            "the VMs' lines are not on the wicket's stdout / stderr; {}",
            wicket.log()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let mut fed = Vec::new();
    let expected = [
        ("stdout", "shared.local", "vm-0", started(booted)),
        ("stdout", "shared.local", "vm-0", request(served, booted)),
        ("stderr", "shared.local", "vm-0", request(served, booted)),
        ("stdout", "clone.local", "template", started(template)),
        (
            "stdout",
            "clone.local",
            clone.as_str(),
            request("1", template),
        ),
        (
            "stderr",
            "clone.local",
            clone.as_str(),
            request("1", template),
        ),
        ("egress", "shared.local", "vm-0", egress.to_string()),
    ];
    while !expected.iter().all(|line| fed.contains(line)) {
        let feed_message::Body::Event(event) = feed.next().await else {
            continue;
        };
        let field = |name: &str| {
            let field = event.fields.iter().find(|field| field.name == name);
            field.map(|field| field.value.clone()).unwrap_or_default()
        };
        let kind = match event.kind.as_str() {
            "stdout" => "stdout",
            "egress" => "egress",
            _ => "stderr",
        };
        // of an egress line, its destination
        let line = field(if kind == "egress" { "to" } else { "line" });
        let vm = field("vm");
        let vm = expected
            .iter()
            .map(|(_, _, vm, _)| *vm)
            .find(|known| *known == vm)
            .unwrap_or("another");
        let route = ["shared.local", "clone.local"]
            .into_iter()
            .find(|host| *host == event.route_key)
            .unwrap_or("another");
        fed.push((kind, route, vm, line));
    }
    let again = started(template);
    let starts = fed.iter().filter(|(_, _, _, line)| *line == again).count();
    assert_eq!(starts, 1, "the template's start, once: {fed:?}");
}

/// A directory of its own under the system's temp dir (short paths: the VMs'
/// sockets), removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn path() -> PathBuf {
        std::env::temp_dir().join(format!("sfc-vm-{}", std::process::id()))
    }

    fn new() -> Scratch {
        let dir = Scratch::path();
        std::fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One advertised route, reached through the tunnel, or through `--listen` at
/// `listen`.
struct Route<'a> {
    tunnel: &'a ServerTunnel,
    host: &'a str,
    address: String,
    listen: Option<std::net::SocketAddr>,
}

impl Route<'_> {
    /// Connection `conn_id` to the route: the tunnel's, or one to `--listen`
    /// pumped to it.
    fn connect(&self, conn_id: i64) -> tokio::io::DuplexStream {
        let Some(listen) = self.listen else {
            return self.tunnel.connect(conn_id, &self.address);
        };
        let (client, mut pumped) = tokio::io::duplex(64 * 1024);
        let stream = std::net::TcpStream::connect(listen).unwrap();
        stream.set_nonblocking(true).unwrap();
        let mut stream = tokio::net::TcpStream::from_std(stream).unwrap();
        tokio::spawn(async move {
            let _ = tokio::io::copy_bidirectional(&mut pumped, &mut stream).await;
        });
        client
    }

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
        // a pool's connections take turns on its VMs
        if sample && !self.address.contains("mode=pool") {
            // a reused clone counts on as a shared VM
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

    /// A route of two VMs, one per client address, cloned for each connection:
    /// a client's second connection fails at once while its first holds its VM,
    /// another client's gets the second VM, and a third client's, finding no
    /// room, fails unanswered once its wait ran out. Idle past the route's
    /// `idle-ms`, the connections fail and their VMs end: the first client's
    /// next connection gets a VM. The clones, restored seconds after the
    /// snapshot, tell the time of the moment and random values of their own.
    async fn capped(&self, conn_id: &mut i64) {
        let mut next = || {
            *conn_id += 1;
            *conn_id
        };
        // the snapshot (taken at startup) seconds old: a clock resumed from it
        // shows
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (first_sender, first, first_ended) = self.held(next(), "192.0.2.1").await;
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

        // refused at once: no wait for room
        let asked = Instant::now();
        let refused = self.half_closed_from(next(), "192.0.2.1").await;
        assert!(refused.is_empty(), "past its share, no answer: {refused:?}");
        assert!(
            asked.elapsed() < Duration::from_secs(1),
            "{:?}",
            asked.elapsed()
        );

        let (second_sender, _, second_ended) = self.held(next(), "192.0.2.2").await;
        let refused = self.half_closed_from(next(), "192.0.2.3").await;
        assert!(refused.is_empty(), "no room, no answer: {refused:?}");

        // idle, the connections fail (cut between answers: hyper's outcome
        // aside), and their VMs end; the senders kept
        let _ = first_ended.await.unwrap();
        let _ = second_ended.await.unwrap();
        drop((first_sender, second_sender));
        let answer = self.half_closed_from(next(), "192.0.2.1").await;
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer:?}");
        assert_ne!(
            field(&answer, "random: "),
            field(&first, "random: "),
            "two clones of one snapshot draw alike"
        );
    }

    /// A reuse route of `uses=3`: a clone serves the next connection, until it served
    /// three, or was kept unused for `keep-ms`.
    async fn reused(&self, conn_id: &mut i64) {
        let mut next = || {
            *conn_id += 1;
            *conn_id
        };
        // the clones of the checks before expired
        tokio::time::sleep(KEEP + Duration::from_secs(1)).await;
        for expected in [[1, 2, 3], [4, 5, 6], [7, 8, 9], [1, 2, 3]] {
            assert_eq!(
                served(&self.keep_alive(next()).await),
                expected,
                "{}",
                self.host
            );
        }
        tokio::time::sleep(KEEP + Duration::from_secs(1)).await;
        assert_eq!(
            served(&self.keep_alive(next()).await),
            [1, 2, 3],
            "{}: a clone kept past keep-ms",
            self.host
        );
    }

    /// A pool route of 2 VMs at most, 1 kept, growing as each holds `conns=1`: a
    /// connection held, the next goes to a VM started for it; a third shares one of
    /// them. Idle, the pool shrinks to one, and grows again with a VM of its own. With
    /// the wicket's `pid` (its VMs its children), its VMs on `rootfs` killed: the next
    /// connection gets a VM started in their place.
    async fn pooled(&self, conn_id: &mut i64, pid: Option<i32>, rootfs: &Path) {
        let mut next = || {
            *conn_id += 1;
            *conn_id
        };
        // the first VM has served nothing: its first answer counts 1, another VM's too
        let (x, first, _) = self.held(next(), "").await;
        assert_eq!(served(&[first]), [1], "{}: the first VM", self.host);
        let (y, _) = self.fresh(&mut next).await;
        let (z, third, _) = self.held(next(), "").await;
        assert_ne!(served(&[third]), [1], "{}: past vms=2", self.host);
        drop((x, y, z));
        tokio::time::sleep(KEEP + Duration::from_secs(1)).await;
        // the one left counts on; the next connection gets a VM of its own
        let (p, kept, _) = self.held(next(), "").await;
        assert_ne!(served(&[kept]), [1], "{}: the VM kept", self.host);
        let (q, _) = self.fresh(&mut next).await;
        drop((p, q));
        let Some(pid) = pid else {
            return;
        };
        let killed = vms_on(pid, rootfs);
        assert_eq!(killed.len(), 2, "{}: its VMs: {killed:?}", self.host);
        for &vm in &killed {
            // SAFETY: kill
            assert_eq!(unsafe { libc::kill(vm, libc::SIGKILL) }, 0);
        }
        // ended: gone, or a zombie (its sockets closed)
        let ended = |vm: &i32| {
            std::fs::read_to_string(format!("/proc/{vm}/stat")).map_or(true, |stat| {
                stat.rsplit(") ")
                    .next()
                    .is_some_and(|rest| rest.starts_with('Z'))
            })
        };
        while !killed.iter().all(ended) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let answer = self.half_closed(next()).await;
        assert!(
            answer.starts_with("HTTP/1.1 200"),
            "{}: {answer:?}",
            self.host
        );
        assert_eq!(served(&[answer]), [1], "{}: a VM in their place", self.host);
    }

    /// Connections held on the route, dropped until one gets a VM started for it (its
    /// first answer counting 1); that one, its sender and its answer.
    async fn fresh(
        &self,
        next: &mut impl FnMut() -> i64,
    ) -> (hyper::client::conn::http1::SendRequest<Full<Bytes>>, String) {
        loop {
            let (sender, answer, _) = self.held(next(), "").await;
            if served(std::slice::from_ref(&answer)) == [1] {
                return (sender, answer);
            }
            drop(sender);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A GET / on a connection of its own of a client of `client` (its address),
    /// kept open: its sender, its answer, and the connection's end.
    async fn held(
        &self,
        conn_id: i64,
        client: &str,
    ) -> (
        hyper::client::conn::http1::SendRequest<Full<Bytes>>,
        String,
        tokio::task::JoinHandle<hyper::Result<()>>,
    ) {
        let io = TokioIo::new(self.tunnel.connect_from(conn_id, &self.address, client));
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
        let ended = tokio::spawn(connection);
        let request = hyper::Request::get("/")
            .header("host", self.host)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), 200, "{}: GET / of {client}", self.host);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (sender, String::from_utf8_lossy(&body).into_owned(), ended)
    }

    /// A GET of `path` on a connection of its own; its answer.
    async fn get(&self, conn_id: i64, path: &str) -> String {
        self.send(conn_id, "GET", path, "").await
    }

    /// A POST of `body` to `path` on a connection of its own; its answer.
    async fn post(&self, conn_id: i64, path: &str, body: &str) -> String {
        self.send(conn_id, "POST", path, body).await
    }

    async fn send(&self, conn_id: i64, method: &str, path: &str, body: &str) -> String {
        let io = TokioIo::new(self.connect(conn_id));
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(connection);
        let request = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", self.host)
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), 200, "{}: {method} {path}", self.host);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8_lossy(&body).into_owned()
    }

    /// Three http/1.1 requests on one connection, its end awaited; their bodies.
    async fn keep_alive(&self, conn_id: i64) -> Vec<String> {
        let io = TokioIo::new(self.connect(conn_id));
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
        let ended = tokio::spawn(connection.without_shutdown());
        let mut bodies = Vec::new();
        for path in ["/1", "/2", "/3"] {
            let request = hyper::Request::get(path)
                .header("host", self.host)
                .body(Full::new(Bytes::new()))
                .unwrap();
            // the previous answer read does not make the connection ready at once
            sender.ready().await.unwrap();
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), 200, "{}: GET {path}", self.host);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            bodies.push(String::from_utf8_lossy(&body).into_owned());
        }
        // the wicket's end of the connection read: its VM is done with it
        drop(sender);
        let mut io = ended.await.unwrap().unwrap().io.into_inner();
        io.shutdown().await.unwrap();
        let mut rest = Vec::new();
        io.read_to_end(&mut rest).await.unwrap();
        bodies
    }

    /// One request on a connection of its own, h2c (prior knowledge) or
    /// http/1.1: a POST of `body`, or a GET without one; the status and the
    /// answer.
    async fn request(&self, conn_id: i64, h2: bool, body: Bytes) -> (u16, String) {
        let io = TokioIo::new(self.connect(conn_id));
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
        self.half_closed_from(conn_id, "").await
    }

    /// [`Route::half_closed`] for a client of `client` (its address).
    async fn half_closed_from(&self, conn_id: i64, client: &str) -> String {
        let mut io = self.tunnel.connect_from(conn_id, &self.address, client);
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

/// Firecracker (a path: root's PATH is another) and the guest kernel.
fn tools() -> (PathBuf, PathBuf) {
    let firecracker = std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER")
        .map(PathBuf::from)
        .unwrap_or_else(|| "firecracker".into());
    let firecracker = match firecracker.components().count() {
        1 => std::env::var_os("PATH")
            .iter()
            .flat_map(std::env::split_paths)
            .map(|dir| dir.join(&firecracker))
            .find(|path| path.is_file())
            .unwrap_or(firecracker),
        _ => firecracker,
    };
    let kernel = std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL")
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
        "/dev/kvm is usable but {} does not run: install Firecracker (README, Setup) or set SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER",
        firecracker.display()
    );
    assert!(
        kernel.is_file(),
        "/dev/kvm is usable but there is no guest kernel at {}: fetch one (README, Setup) or set SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL",
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
    /// The container's name, when run as an image.
    container: Option<String>,
}

impl Wicket {
    fn start(server: &str, firecracker: &Path, kernel: &Path, args: &[String]) -> Wicket {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-runtime-firecracker"));
        command
            .args(["--firecracker"])
            .arg(firecracker)
            .arg("--kernel")
            .arg(kernel);
        Wicket::spawn(command, server, args, None)
    }

    /// The wicket in jail mode, started as root (`sudo -n`), as the test's user, its
    /// VMs' cgroups in `cgroup`.
    fn jailed(
        server: &str,
        firecracker: &Path,
        kernel: &Path,
        cgroup: &str,
        args: &[String],
    ) -> Wicket {
        // SAFETY: getuid, getgid
        let user = unsafe { format!("{}:{}", libc::getuid(), libc::getgid()) };
        let mut command = Command::new("sudo");
        command
            .args([
                "-n",
                env!("CARGO_BIN_EXE_sluice-wicket-runtime-firecracker"),
            ])
            .args(["--firecracker"])
            .arg(firecracker)
            .arg("--kernel")
            .arg(kernel)
            .args(["--jail-ids", &format!("{}-{}", JAIL_IDS.0, JAIL_IDS.1)])
            .args(["--jail-user", &user, "--jail-cgroup", cgroup]);
        Wicket::spawn(command, server, args, None)
    }

    /// The wicket as the Docker `image`, with its own Firecracker and kernel,
    /// reading the rootfs images under `target/` and `scratch`.
    /// `jailed`: as the README runs it in jail mode, root in the container (with
    /// `CAP_SYS_ADMIN`, unconfined: Docker's AppArmor profile denies mounts, its seccomp
    /// profile `pivot_root`), the wicket as `nonroot`.
    fn in_image(
        image: &str,
        server: &str,
        scratch: &Path,
        args: &[String],
        jailed: bool,
    ) -> Wicket {
        use std::os::unix::fs::MetadataExt as _;
        let kvm = std::fs::metadata("/dev/kvm").unwrap().gid();
        let name = format!("sfc-vm-{}", std::process::id());
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
        let mount = |dir: &Path| format!("{0}:{0}:ro", dir.display());
        let mut command = Command::new("docker");
        command
            .args(["run", "--rm", "--name", &name, "--network", "host"])
            .args(["--device", "/dev/kvm"]);
        if jailed {
            command
                .args(["-u", "0", "--cap-add", "SYS_ADMIN"])
                .args(["--security-opt", "apparmor=unconfined"])
                .args(["--security-opt", "seccomp=unconfined"]);
        } else {
            command.args(["--group-add", &kvm.to_string()]);
        }
        command.args(["--tmpfs", "/run/sluice"]).args([
            "-v",
            &mount(&target),
            "-v",
            &mount(scratch),
            image,
        ]);
        if jailed {
            command
                .args(["--jail-ids", &format!("{}-{}", JAIL_IDS.0, JAIL_IDS.1)])
                .args(["--jail-user", "nonroot"]);
        }
        Wicket::spawn(command, server, args, Some(name))
    }

    /// The keeper (jail mode): sudo's child, or the container's first process.
    fn keeper(&self) -> i32 {
        let keeper = match &self.container {
            Some(container) => {
                let inspected = Command::new("docker")
                    .args(["inspect", "-f", "{{.State.Pid}}", container])
                    .output()
                    .unwrap();
                String::from_utf8_lossy(&inspected.stdout)
                    .trim()
                    .parse()
                    .ok()
            }
            None => children(self.child.id() as i32).into_iter().next(),
        };
        keeper.unwrap_or_else(|| panic!("no keeper; {}", self.log()))
    }

    fn spawn(
        mut command: Command,
        server: &str,
        args: &[String],
        container: Option<String>,
    ) -> Wicket {
        let log = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("wicket-{}.log", std::process::id()));
        let file = std::fs::File::create(&log).unwrap();
        let child = command
            // the drain is the tunnel's, tested there: Ctrl-C ends the wicket at once
            .args(["--server", server, "--drain-grace", "0"])
            .args(args)
            .current_dir(Scratch::path())
            .stdin(Stdio::null())
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .expect("run sluice-wicket-runtime-firecracker");
        Wicket {
            child,
            log,
            container,
        }
    }

    /// The address `--listen` bound, from its log.
    fn listening(&self) -> std::net::SocketAddr {
        let deadline = Instant::now() + HANG;
        loop {
            let log = std::fs::read_to_string(&self.log).unwrap_or_default();
            let line = log
                .lines()
                .find_map(|line| line.strip_prefix("listening on "));
            if let Some(line) = line {
                return line.split(',').next().unwrap().parse().unwrap();
            }
            assert!(Instant::now() < deadline, "not listening; {}", self.log());
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The address its console listens on, from its log.
    fn console(&self) -> std::net::SocketAddr {
        let deadline = Instant::now() + HANG;
        loop {
            let log = std::fs::read_to_string(&self.log).unwrap_or_default();
            let line = log
                .lines()
                .find_map(|line| line.strip_prefix("console on http://"));
            if let Some(line) = line {
                return line.split(' ').next().unwrap().parse().unwrap();
            }
            assert!(Instant::now() < deadline, "no console; {}", self.log());
            std::thread::sleep(Duration::from_millis(50));
        }
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
        // docker run passes the signal on to the container
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
        if let Some(container) = &self.container {
            let _ = Command::new("docker")
                .args(["rm", "-f", container])
                .output();
        }
    }
}
