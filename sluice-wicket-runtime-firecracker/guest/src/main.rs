//! The guest side of sluice-wicket-runtime-firecracker: the microVM's `/init` (PID 1).
//!
//! Relays every vsock stream on port 8080 (framed, see `wire`: vsock carries no
//! half-close) to a process on `127.0.0.1`, which one of two modes provides:
//!
//! - launcher mode, when `/sluice/init.json` exists (written by `scripts/cnb-rootfs.sh`
//!   from an OCI image's config, e.g. a Cloud Native Buildpacks image): the image's
//!   process, started as the image's user with the image's environment
//! - hello mode otherwise: an HTTP server (http/1.1 and h2c) of its own on port 8081,
//!   answering with what tells the VMs apart (boot id, requests served, time since boot)
//!
//! The vsock port opens once the process accepts: the wicket takes the guest as ready
//! (or snapshots it) when it does.
//!
//! The process's stdout and stderr (and hello mode's own lines) go to the wicket as lines
//! on the log stream (`log`, vsock port `wire::LOG_PORT`), open before the process starts;
//! /init's own output goes to the console.
//!
//! A VM cloned from a snapshot gets the wicket's restore notice on the control port
//! (`wire::CONTROL_PORT`): /init sets the clock to its time (where it carries one: the
//! restore did not advance the clock), puts its seed into the kernel's random pool,
//! reseeds it, and runs the image's restore hook (`restore` in `/sluice/init.json`, as the
//! image's user, `SLUICE_RESTORE_GENERATION` set) before it answers.

use std::convert::Infallible;
use std::ffi::CString;
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};

use log::Log;
use reaper::Reaper;
use wire::Stream;

mod log;
mod reaper;
mod wire;

const VSOCK_PORT: u32 = 8080;

/// Hello mode's HTTP server.
const HELLO_PORT: u16 = 8081;

const LAUNCH_CONFIG: &str = "/sluice/init.json";

static SERVED: AtomicU64 = AtomicU64::new(0);

/// Reaps every child of /init in launcher mode.
static REAPER: Reaper = Reaper::new();

/// Hello mode's random state, of its own (as a process's generator is): seeded at
/// boot, so a clone has its snapshot's until the restore notice reseeds it.
static RANDOM: AtomicU64 = AtomicU64::new(0);

/// The restores of this VM, as the restore hook sees them.
static RESTORES: AtomicU64 = AtomicU64::new(0);

/// `RNDADDENTROPY`: `_IOW('R', 0x03, int[2])`.
const RNDADDENTROPY: libc::c_ulong = 0x4008_5203;

/// `RNDRESEEDCRNG`: `_IO('R', 0x07)`.
const RNDRESEEDCRNG: libc::c_ulong = 0x5207;

/// The process to run, from the image's config.
#[derive(Deserialize)]
struct Launch {
    /// Entrypoint followed by Cmd.
    argv: Vec<String>,
    /// `NAME=value`, the image's Env.
    env: Vec<String>,
    uid: u32,
    gid: u32,
    workdir: String,
    /// The TCP port the process listens on (also set as `PORT`).
    port: u16,
    /// The restore hook: run after each restore of a clone, before it serves.
    #[serde(default)]
    restore: Vec<String>,
}

fn main() {
    let booted = Instant::now();
    mount("proc", "/proc", "proc", 0);
    mount("sysfs", "/sys", "sysfs", 0);
    mount("tmpfs", "/tmp", "tmpfs", 0);
    mount("tmpfs", "/dev/shm", "tmpfs", 0);
    let launch = std::fs::read(LAUNCH_CONFIG).ok().map(|json| {
        serde_json::from_slice::<Launch>(&json).unwrap_or_else(|e| {
            eprintln!("guest: {LAUNCH_CONFIG}: {e}");
            park()
        })
    });
    // a worker per vCPU: the relay (and hello mode's server) on every one; on a
    // single vCPU a runtime of one thread (no handoffs between threads)
    let runtime = match std::thread::available_parallelism().map_or(1, |n| n.get()) {
        1 => tokio::runtime::Builder::new_current_thread(),
        vcpus => {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(vcpus);
            builder
        }
    }
    .enable_all()
    .build()
    .expect("runtime");
    let result = runtime.block_on(async {
        // first: the wicket follows the output from before the process starts
        let log = Log::new();
        let output = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, wire::LOG_PORT))?;
        tokio::spawn(log.clone().serve(output));
        loopback_up()?;
        let hook = launch.as_ref().map(|launch| Hook {
            argv: launch.restore.clone(),
            env: launch.env.clone(),
            uid: launch.uid,
            gid: launch.gid,
            workdir: launch.workdir.clone(),
        });
        let control = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, wire::CONTROL_PORT))?;
        tokio::spawn(control_loop(control, hook, log.clone()));
        let port = match launch {
            Some(launch) => {
                let port = launch.port;
                start(launch, &log)?;
                port
            }
            None => {
                let listener = tokio::net::TcpListener::bind(("127.0.0.1", HELLO_PORT)).await?;
                tokio::spawn(hello(listener, booted, log));
                HELLO_PORT
            }
        };
        relay(port, booted).await
    });
    if let Err(e) = result {
        eprintln!("guest: {e}");
    }
    park()
}

/// PID 1 must never exit: the kernel panics when it does.
fn park() -> ! {
    loop {
        std::thread::park();
    }
}

fn mount(source: &str, target: &str, fstype: &str, flags: libc::c_ulong) {
    let _ = std::fs::create_dir_all(target);
    let (source, target, fstype) = (
        CString::new(source).unwrap(),
        CString::new(target).unwrap(),
        CString::new(fstype).unwrap(),
    );
    // SAFETY: NUL-terminated strings, no data argument
    let mounted = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            flags,
            std::ptr::null(),
        )
    };
    if mounted != 0 {
        eprintln!(
            "guest: mount {}: {}",
            target.to_string_lossy(),
            std::io::Error::last_os_error()
        );
    }
}

/// Brings the loopback interface up: the relay reaches the process on 127.0.0.1.
fn loopback_up() -> std::io::Result<()> {
    // SAFETY: a plain ioctl on a fresh socket with a zeroed, NUL-terminated ifreq
    unsafe {
        let socket = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if socket < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut request: libc::ifreq = std::mem::zeroed();
        for (i, byte) in b"lo".iter().enumerate() {
            request.ifr_name[i] = *byte as libc::c_char;
        }
        let result = if libc::ioctl(socket, libc::SIOCGIFFLAGS as _, &mut request) < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            request.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short;
            if libc::ioctl(socket, libc::SIOCSIFFLAGS as _, &request) < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        };
        libc::close(socket);
        result
    }
}

/// Launcher mode: starts the image's process, its output to `log`.
fn start(launch: Launch, log: &Arc<Log>) -> std::io::Result<()> {
    let hostname = c"sluice";
    // SAFETY: a NUL-terminated name and its length
    unsafe { libc::sethostname(hostname.as_ptr(), hostname.to_bytes().len()) };
    let Some((program, args)) = launch.argv.split_first() else {
        return Err(std::io::Error::other("init.json: empty argv"));
    };
    let program = program.clone();
    let mut command = std::process::Command::new(&program);
    command
        .args(args)
        .env_clear()
        .envs(launch.env.iter().filter_map(|pair| pair.split_once('=')))
        .env("PORT", launch.port.to_string())
        .current_dir(&launch.workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (uid, gid) = (launch.uid, launch.gid);
    // SAFETY: setgroups / setgid / setuid are async-signal-safe. Done here rather than
    // with Command::uid / gid: the closure runs after those, without the right to drop
    // the supplementary groups
    unsafe {
        command.pre_exec(move || {
            if libc::setgroups(0, std::ptr::null()) != 0
                || libc::setgid(gid) != 0
                || libc::setuid(uid) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    capture(&mut child, log);
    println!("guest: started {program} (pid {})", child.id());
    // PID 1 reaps every orphan; the process's exit is logged, the VM stays up
    let pid = child.id() as libc::pid_t;
    std::thread::spawn(move || {
        REAPER.run(|reaped, status| {
            if reaped == pid {
                eprintln!("guest: {program} exited ({status})");
            }
        })
    });
    Ok(())
}

/// Has `log` read the output of `child` (spawned with it piped).
fn capture(child: &mut std::process::Child, log: &Arc<Log>) {
    if let Some(stdout) = child.stdout.take() {
        log.capture(Stream::Stdout, stdout);
    }
    if let Some(stderr) = child.stderr.take() {
        log.capture(Stream::Stderr, stderr);
    }
}

/// What the restore hook runs as: the image's process's user, environment and
/// directory.
struct Hook {
    argv: Vec<String>,
    env: Vec<String>,
    uid: u32,
    gid: u32,
    workdir: String,
}

/// Answers each restore notice of the wicket's (one per restore): the output
/// waiting discarded (the snapshot's), the clock set (given a time), the seed into
/// the kernel's random pool,
/// the pool reseeded, hello mode's generator reseeded, the hook run (its output
/// to `log`); then `OK`.
async fn control_loop(listener: VsockListener, hook: Option<Hook>, log: Arc<Log>) {
    let hook = Arc::new(hook);
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let hook = hook.clone();
        let log = log.clone();
        tokio::spawn(async move {
            let (reader, mut writer) = tokio::io::split(stream);
            let mut line = String::new();
            let mut reader = tokio::io::BufReader::new(reader);
            if reader.read_line(&mut line).await.is_err() {
                return;
            }
            let answer = match wire::Notice::parse(line.trim_end()) {
                Some(notice) => match restored(&notice, &hook, &log).await {
                    Ok(()) => "OK\n".to_string(),
                    Err(e) => format!("ERR {e}\n"),
                },
                None => "ERR not a restore notice\n".to_string(),
            };
            let _ = writer.write_all(answer.as_bytes()).await;
        });
    }
}

async fn restored(
    notice: &wire::Notice,
    hook: &Arc<Option<Hook>>,
    log: &Arc<Log>,
) -> std::io::Result<()> {
    // first: the hook runs at the time of the moment, not the snapshot's
    if let Some(time) = notice.time {
        set_clock(time)?;
    }
    log.restored();
    let seed = &notice.seed;
    reseed(seed)?;
    RANDOM.store(
        u64::from_le_bytes(seed[..8].try_into().unwrap()),
        Ordering::Relaxed,
    );
    let generation = RESTORES.fetch_add(1, Ordering::Relaxed) + 1;
    let Some(hook) = hook.as_ref().as_ref().filter(|hook| !hook.argv.is_empty()) else {
        return Ok(());
    };
    let (program, args) = hook.argv.split_first().expect("not empty");
    let mut command = std::process::Command::new(program);
    command
        .args(args)
        .env_clear()
        .envs(hook.env.iter().filter_map(|pair| pair.split_once('=')))
        .env("SLUICE_RESTORE_GENERATION", generation.to_string())
        .current_dir(&hook.workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (uid, gid) = (hook.uid, hook.gid);
    // SAFETY: as for the process (`start`)
    unsafe {
        command.pre_exec(move || {
            if libc::setgroups(0, std::ptr::null()) != 0
                || libc::setgid(gid) != 0
                || libc::setuid(uid) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    // its status from the reaper's, which reaps any child: a wait of its own would race it
    let (mut child, status) = REAPER.spawn(&mut command)?;
    capture(&mut child, log);
    let status = status
        .await
        .map_err(|_| std::io::Error::other(format!("restore hook {program}: no exit status")))?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "restore hook {program}: {status}"
        )));
    }
    Ok(())
}

/// Sets the wall clock to `since_epoch`, the host's time of the restore notice.
fn set_clock(since_epoch: Duration) -> std::io::Result<()> {
    let time = libc::timespec {
        tv_sec: since_epoch.as_secs() as _,
        tv_nsec: since_epoch.subsec_nanos() as _,
    };
    // SAFETY: clock_settime of a valid timespec
    if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &time) } != 0 {
        return Err(std::io::Error::other(format!(
            "clock_settime: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// Puts `seed` into the kernel's random pool, credited, and reseeds the pool
/// from it at once, ahead of the VM generation change it may not have taken yet.
fn reseed(seed: &[u8; 32]) -> std::io::Result<()> {
    #[repr(C)]
    struct PoolInfo {
        entropy_count: libc::c_int,
        buf_size: libc::c_int,
        buf: [u8; 32],
    }
    let info = PoolInfo {
        entropy_count: 256,
        buf_size: 32,
        buf: *seed,
    };
    let random = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/urandom")?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&random);
    // SAFETY: the ioctls of random(4) on an open /dev/urandom, `info` laid out
    // as its struct rand_pool_info
    unsafe {
        if libc::ioctl(fd, RNDADDENTROPY as _, &info) != 0
            || libc::ioctl(fd, RNDRESEEDCRNG as _) != 0
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Relays every vsock stream to `127.0.0.1:port`, once the port accepts.
async fn relay(port: u16, booted: Instant) -> std::io::Result<()> {
    let target = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    while tokio::net::TcpStream::connect(target).await.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // the vsock port opens only now: the wicket waits (and snapshots) on it
    let listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, VSOCK_PORT))?;
    println!(
        "guest: process accepting on {target} after {}ms; relaying vsock port {VSOCK_PORT}",
        booted.elapsed().as_millis()
    );
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            match tokio::net::TcpStream::connect(target).await {
                Ok(upstream) => {
                    if let Err(e) = wire::relay(upstream, stream, None).await {
                        eprintln!("guest: relay: {e}");
                    }
                }
                Err(e) => eprintln!("guest: {target}: {e}"),
            }
        });
    }
}

/// Hello mode: the guest's own HTTP server, its lines to `log` as a process's
/// output: one at its start, two (stdout, stderr) per request for `/log`.
async fn hello(listener: tokio::net::TcpListener, booted: Instant, log: Arc<Log>) {
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|id| id.trim().to_string())
        .unwrap_or_else(|_| "unknown (no /proc)".into());
    log.line(
        Stream::Stdout,
        format!("hello server, boot id {boot_id}").as_bytes(),
    );
    let mut seed = [0u8; 8];
    // SAFETY: getrandom into a buffer of its length
    unsafe { libc::getrandom(seed.as_mut_ptr().cast(), seed.len(), 0) };
    RANDOM.store(u64::from_le_bytes(seed), Ordering::Relaxed);
    let boot_id: &'static str = Box::leak(boot_id.into_boxed_str());
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let log = log.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                answer(request, booted, boot_id, log.clone())
            });
            let mut builder = auto::Builder::new(TokioExecutor::new());
            // a client half-closing after its request still gets the answer
            builder.http1().half_close(true);
            if let Err(e) = builder
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                eprintln!("guest: connection: {e}");
            }
        });
    }
}

/// The next value of hello mode's generator (splitmix64).
fn next_random() -> u64 {
    let mut z = RANDOM
        .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

async fn answer(
    request: Request<hyper::body::Incoming>,
    booted: Instant,
    boot_id: &str,
    log: Arc<Log>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let served = SERVED.fetch_add(1, Ordering::Relaxed) + 1;
    let (head, body) = request.into_parts();
    if head.uri.path() == "/log" {
        let line = format!(
            "{} {}, request {served} of boot id {boot_id}",
            head.method, head.uri
        );
        log.line(Stream::Stdout, line.as_bytes());
        log.line(Stream::Stderr, line.as_bytes());
    }
    let received = body
        .collect()
        .await
        .map(|b| b.to_bytes().len())
        .unwrap_or(0);
    let forwarded = head
        .headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let text = format!(
        "Hello from a Firecracker microVM\n\
         boot id: {boot_id}\n\
         served by this VM: {served}\n\
         since boot: {}ms\n\
         time: {}.{:03}\n\
         random: {:016x}\n\
         request: {} {} {:?}, {received} body bytes, x-forwarded-for {forwarded}\n",
        booted.elapsed().as_millis(),
        now.as_secs(),
        now.subsec_millis(),
        next_random(),
        head.method,
        head.uri,
        head.version,
    );
    Ok(Response::new(Full::new(Bytes::from(text))))
}
