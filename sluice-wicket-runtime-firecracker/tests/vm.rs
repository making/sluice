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
//! and a clone, and its records; a reload of the `--config` file, polled,
//! removing a route and replacing a rootfs.
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

/// The launcher (`--launch-vm`) execs the program with the soft open file
/// limit given (Firecracker grows its file table to the limit at its start, a
/// cost that grows with it: 3ms at 1048576), the file size given (a guest's
/// output to its VM's log bounded) and no core (of a VM's memory).
#[test]
fn the_launcher_execs_with_the_limits_given() {
    let output = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-runtime-firecracker"))
        .args([
            "--launch-vm",
            &std::process::id().to_string(),
            "1100",
            "1048576",
        ])
        .args(["/bin/cat", "/proc/self/limits"])
        .output()
        .unwrap();
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
         - {{host: alias.local, rootfs: shared.ext4, mode: shared, egress: [{shared_egress}]}}\n",
        rootfs = rootfs.display()
    );
    let capped = format!(
        "- {{host: capped.local, rootfs: {}, mode: per-connection, vms: 2, wait-ms: 1000, \
         per-client: 1, idle-ms: {}}}\n",
        rootfs.display(),
        CAPPED_IDLE.as_millis()
    );
    std::fs::write(&config, format!("{routes}{capped}")).unwrap();
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
    ]
    .map(String::from)
    .into();
    args.extend(extra.iter().flat_map(|vm| ["--vm".to_string(), vm.clone()]));
    let (url, mut streams) = tunnel_server(None).await;
    let cgroup = format!("sluice-vm-test-{}", std::process::id());
    let wicket = match &image {
        Some(image) => Wicket::in_image(image, &url, &dir.0, &args, jailed),
        None if jailed => {
            let (firecracker, kernel) = tools();
            Wicket::jailed(&url, &firecracker, &kernel, &cgroup, &args)
        }
        None => {
            let (firecracker, kernel) = tools();
            Wicket::start(&url, &firecracker, &kernel, &args)
        }
    };
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
    match (jailed, &image) {
        // the image's keeper in the container's cgroup, the wicket its nonroot user
        (true, Some(_)) => jails(&wicket, None, NONROOT),
        // SAFETY: getuid
        (true, None) => jails(&wicket, Some(&cgroup), unsafe { libc::getuid() }),
        // the wicket's seccomp filter, on the test's (if any)
        (false, None) => assert_eq!(
            filters(wicket.child.id() as i32),
            filters(std::process::id() as i32) + 1,
            "the wicket's filter"
        ),
        (false, Some(_)) => {}
    }
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
    let (vms, others): (Vec<i32>, Vec<i32>) = children(keeper).into_iter().partition(|pid| {
        let uid: u32 = status(*pid, "Uid:")
            .split_whitespace()
            .next()
            .and_then(|uid| uid.parse().ok())
            .unwrap_or(0);
        (JAIL_IDS.0..=JAIL_IDS.1).contains(&uid)
    });
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
        let io = TokioIo::new(self.tunnel.connect(conn_id, &self.address));
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
            // the previous answer read does not make the connection ready at once
            sender.ready().await.unwrap();
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
