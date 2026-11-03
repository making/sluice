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
//!
//! The process connects to the destinations of the route's egress as named, through
//! /init and the wicket (`egress`): set up before it starts. Hello mode's
//! `POST /egress?to=<host>:<port>` connects so too.
//!
//! A route's environment variables come from the wicket on the env stream
//! (`wire::ENV_PORT`) when the kernel's command line says so (`wire::ENV_PARAMETER`):
//! read before the process starts, over the image's. Hello mode's `GET /env` answers them.
//!
//! An app's disk (`wire::DISK_PARAMETER`) is mounted before the process starts, grown to the
//! drive's size and its root given to the image's user: a failure is told on the control
//! port (`wire::DISK_QUESTION`) and starts no process. A stop notice on that port
//! (`wire::parse_stop`) ends the process and writes all out before the wicket kills the VM.
//! Hello mode's `/disk` reads and writes it.

use std::convert::Infallible;
use std::ffi::CString;
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};

use egress::Egress;
use log::Log;
use reaper::Reaper;
use wire::Stream;

mod egress;
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

/// What the wicket gave on the env stream, `NAME=value`.
static GIVEN_ENV: OnceLock<Vec<String>> = OnceLock::new();

/// `EXT4_IOC_RESIZE_FS`: `_IOW('f', 16, __u64)`.
const EXT4_IOC_RESIZE_FS: libc::c_ulong = 0x4008_6610;

/// How long a process killed may take to end, before all is written all the same.
const KILLED_WAIT: Duration = Duration::from_secs(1);

/// Hello mode's `POST /exit`: its server ends.
static EXIT: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// On the disk, hello mode's server exits once more, before it listens (`POST /exit?again=1`).
const EXIT_ONCE: &str = ".exit-once";

/// Where the VM's disk is mounted, or why it is not; unset without a disk.
static DISK: OnceLock<Result<String, String>> = OnceLock::new();

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
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    let mut egress = Egress::of(&cmdline);
    // on a tmpfs mounted on /tmp a moment: before /tmp's own
    egress.name();
    mount("tmpfs", "/tmp", "tmpfs", 0);
    mount("tmpfs", "/dev/shm", "tmpfs", 0);
    link_standard_fds();
    let mut launch = std::fs::read(LAUNCH_CONFIG).ok().map(|json| {
        serde_json::from_slice::<Launch>(&json).unwrap_or_else(|e| {
            eprintln!("guest: {LAUNCH_CONFIG}: {e}");
            park()
        })
    });
    let env_given = cmdline
        .split_whitespace()
        .any(|parameter| parameter == format!("{}=1", wire::ENV_PARAMETER));
    let disk = cmdline.split_whitespace().find_map(|parameter| {
        parameter
            .strip_prefix(wire::DISK_PARAMETER)?
            .strip_prefix('=')
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
        if env_given {
            let given = given_env().await?;
            if let Some(launch) = &mut launch {
                launch.env.extend(given.iter().cloned());
            }
            let _ = GIVEN_ENV.set(given);
        }
        if let Some(path) = disk {
            let owner = launch.as_ref().map(|launch| (launch.uid, launch.gid));
            let mounted = mount_disk(path, owner, &log);
            if let Err(e) = &mounted {
                log.line(
                    Stream::Stderr,
                    format!("guest: the disk at {path}: {e}").as_bytes(),
                );
            }
            let _ = DISK.set(
                mounted
                    .map(|()| path.to_string())
                    .map_err(|e| e.to_string()),
            );
        }
        // before the process starts: it may connect at once
        let port_held = egress.listen(launch.as_ref().map(|launch| launch.port), &log);
        let hook = launch.as_ref().map(|launch| Hook {
            argv: launch.restore.clone(),
            env: launch.env.clone(),
            uid: launch.uid,
            gid: launch.gid,
            workdir: launch.workdir.clone(),
        });
        let control = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, wire::CONTROL_PORT))?;
        tokio::spawn(control_loop(control, hook, log.clone()));
        if DISK.get().is_some_and(Result::is_err) {
            // no process without its disk: the wicket asks why
            std::future::pending::<()>().await;
        }
        if port_held {
            // no process: the wicket would take /init's listener for it
            log.line(
                Stream::Stderr,
                b"guest: the process is not started: a rule of the egress holds its port",
            );
            log.exited();
            std::future::pending::<()>().await;
        }
        let port = match launch {
            Some(launch) => {
                let port = launch.port;
                start(launch, &log)?;
                port
            }
            None => {
                if let Some(Ok(mount)) = DISK.get()
                    && std::fs::remove_file(std::path::Path::new(mount).join(EXIT_ONCE)).is_ok()
                {
                    log.line(Stream::Stderr, b"guest: exiting before it listens, as told");
                    log.exited();
                    std::future::pending::<()>().await;
                }
                write_at_start(&log);
                if let Some(ms) = given("LISTEN_AFTER_MS").and_then(|ms| ms.parse().ok()) {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
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

/// The variables of the env stream: the wicket's, one stream, once.
async fn given_env() -> std::io::Result<Vec<String>> {
    let listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, wire::ENV_PORT))?;
    let (mut stream, _) = listener.accept().await?;
    let json = wire::read_env(&mut stream).await?;
    let given: std::collections::BTreeMap<String, String> =
        serde_json::from_slice(&json).map_err(std::io::Error::other)?;
    wire::answer_env(&mut stream).await?;
    Ok(given
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect())
}

/// PID 1 must never exit: the kernel panics when it does.
fn park() -> ! {
    loop {
        std::thread::park();
    }
}

/// `/dev/stdin`, `/dev/stdout`, `/dev/stderr` and `/dev/fd`, which an image's process opens by
/// path (its logs link to them) and which the kernel's devtmpfs does not have: runc makes them.
fn link_standard_fds() {
    for (link, target) in [
        ("/dev/fd", "/proc/self/fd"),
        ("/dev/stdin", "/proc/self/fd/0"),
        ("/dev/stdout", "/proc/self/fd/1"),
        ("/dev/stderr", "/proc/self/fd/2"),
    ] {
        if let Err(e) = std::os::unix::fs::symlink(target, link)
            && e.kind() != std::io::ErrorKind::AlreadyExists
        {
            eprintln!("guest: {link}: {e}");
        }
    }
}

/// Makes `fd` (a pipe `start` made as root) the `uid`'s: a process opening its output again by
/// path needs the owner's access, as runc's `fixStdioPermissions` gives a container's.
fn chown_fd(fd: libc::c_int, uid: u32) -> std::io::Result<()> {
    // SAFETY: fchown on an open descriptor; the gid is left as it is
    if unsafe { libc::fchown(fd, uid, u32::MAX) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
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

/// Mounts the VM's disk at `path` (a directory of the image, or made on a tmpfs), grown to the
/// drive's size, its root given to the image's process's `owner`. `nodioread_nolock`: a VM
/// killed is a crash to its file system, and a file written and renamed without an fsync then
/// reads as NUL bytes under ext4's default, the old or the new one under this.
fn mount_disk(path: &str, owner: Option<(u32, u32)>, log: &Log) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let (source, target, fstype, options) = (
        CString::new(wire::DISK_DEVICE)?,
        CString::new(path)?,
        c"ext4",
        c"nodioread_nolock",
    );
    // SAFETY: NUL-terminated strings, the data ext4's options
    let mounted = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            libc::MS_NOSUID | libc::MS_NODEV,
            options.as_ptr().cast(),
        )
    };
    if mounted != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // a grown drive's room: on a mount, as nothing else in the guest knows; no error of it
    // keeps the disk from serving at the size it has
    if let Err(e) = grow_disk(path) {
        log.line(
            Stream::Stderr,
            format!("guest: the disk at {path} is not grown: {e}").as_bytes(),
        );
    }
    if let Some((uid, gid)) = owner {
        std::os::unix::fs::chown(path, Some(uid), Some(gid))?;
    }
    Ok(())
}

/// Grows the file system mounted at `path` to the drive's size (nothing to do at that size).
fn grow_disk(path: &str) -> std::io::Result<()> {
    use std::io::Seek as _;
    use std::os::fd::AsRawFd as _;
    let bytes = std::fs::File::open(wire::DISK_DEVICE)?.seek(std::io::SeekFrom::End(0))?;
    let root = std::fs::File::open(path)?;
    let mut fs = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: fstatvfs of an open file into a buffer of its type
    if unsafe { libc::fstatvfs(root.as_raw_fd(), fs.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: filled by the successful call
    let block = unsafe { fs.assume_init() }.f_frsize;
    let blocks: u64 = bytes / block;
    // SAFETY: the ioctl of ext4 on its mount's root, the count of blocks it reads
    if unsafe { libc::ioctl(root.as_raw_fd(), EXT4_IOC_RESIZE_FS as _, &blocks) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Whether a process but /init is left: a kernel thread has no command line, nor has a zombie.
fn processes_left() -> bool {
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| *pid != 1)
        .any(|pid| std::fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|line| !line.is_empty()))
}

/// What a stop notice asks: every process ended (SIGTERM, `grace` to do it, then SIGKILL): the
/// image's and what it started, which holds the disk just as well; what is dirty written, the
/// disk unmounted (read-only where something holds it).
async fn stop(grace: Duration) -> std::io::Result<()> {
    let ended = async |within: Duration| {
        let deadline = Instant::now() + within;
        while processes_left() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        !processes_left()
    };
    // SAFETY: a plain kill of every process but PID 1 itself
    let signal = |signal| unsafe {
        libc::kill(-1, signal);
    };
    signal(libc::SIGTERM);
    if !ended(grace).await {
        signal(libc::SIGKILL);
        ended(KILLED_WAIT).await;
    }
    tokio::task::spawn_blocking(|| {
        // SAFETY: plain sync; umount2 and a remount of a NUL-terminated path
        unsafe {
            libc::sync();
            if let Some(Ok(path)) = DISK.get()
                && let Ok(path) = CString::new(path.as_str())
                && libc::umount2(path.as_ptr(), 0) != 0
            {
                libc::mount(
                    std::ptr::null(),
                    path.as_ptr(),
                    std::ptr::null(),
                    libc::MS_REMOUNT | libc::MS_RDONLY,
                    std::ptr::null(),
                );
            }
        }
    })
    .await
    .map_err(std::io::Error::other)
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
            // before setuid, while it may: the pipes are the process's to open by path
            chown_fd(1, uid)?;
            chown_fd(2, uid)?;
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
    // PID 1 reaps every orphan; the process's exit reaches the wicket as its last line (no
    // serial console tells it), the VM stays up
    let pid = child.id() as libc::pid_t;
    let log = log.clone();
    std::thread::spawn(move || {
        REAPER.run(|reaped, status| {
            if reaped == pid {
                let line = format!("guest: {program} exited ({status})");
                log.line(Stream::Stderr, line.as_bytes());
                log.exited();
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
/// to `log`); then `OK`. And its question of the disk, and its stop notice (`stop`).
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
            let line = line.trim_end();
            let done = if line == wire::DISK_QUESTION {
                match DISK.get() {
                    Some(Ok(_)) => Ok(()),
                    Some(Err(e)) => Err(std::io::Error::other(e.clone())),
                    None => Err(std::io::Error::other("no disk given")),
                }
            } else if let Some(grace) = wire::parse_stop(line) {
                stop(grace).await
            } else if let Some(notice) = wire::Notice::parse(line) {
                restored(&notice, &hook, &log).await
            } else {
                Err(std::io::Error::other("not a restore notice"))
            };
            let answer = match done {
                Ok(()) => "OK\n".to_string(),
                Err(e) => format!("ERR {e}\n"),
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
            // before setuid, while it may: the pipes are the process's to open by path
            chown_fd(1, uid)?;
            chown_fd(2, uid)?;
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
/// output: one at its start, two (stdout, stderr) per request for `/log`; until a
/// `POST /exit`, its end told as a process's is.
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
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            () = EXIT.notified() => break,
        };
        let Ok((stream, _)) = accepted else {
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
    drop(listener);
    log.line(Stream::Stderr, b"guest: exited, as told");
    log.exited();
}

/// Hello mode's `POST /exit`: its server ends just after the answer, as a process does, or with
/// `?listening=1` says so and goes on, as a launcher leaving a daemon does; `?again=1` has the
/// next boot's exit once more, before it listens.
fn exit_answer(query: &str, log: &Log) -> String {
    if query == "listening=1" {
        log.exited();
        return "exited, listening\n".into();
    }
    if query == "again=1"
        && let Some(Ok(mount)) = DISK.get()
        && let Err(e) = std::fs::write(std::path::Path::new(mount).join(EXIT_ONCE), b"")
    {
        return format!("error: {e}\n");
    }
    tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        EXIT.notify_one();
    });
    "exiting\n".into()
}

/// Hello mode, given `WRITE_AT_START=<name>`: that file kept on the disk, not durably, and said,
/// as a process does before it listens. Given `LINGER_MS=<ms>` too, a process ignoring SIGTERM for
/// that long: a stop takes as long. Given `LISTEN_AFTER_MS=<ms>`, it listens that much later.
fn write_at_start(log: &Log) {
    if let Some(name) = given("WRITE_AT_START") {
        let wrote = disk_answer(&format!("write={name}"), b"unsynced");
        log.line(Stream::Stdout, wrote.trim_end().as_bytes());
    }
    if let Some(ms) = given("LINGER_MS").and_then(|ms| ms.parse::<libc::c_uint>().ok()) {
        // SAFETY: the child calls what is async-signal-safe alone
        unsafe {
            if libc::fork() == 0 {
                libc::signal(libc::SIGTERM, libc::SIG_IGN);
                libc::usleep(ms * 1000);
                libc::_exit(0);
            }
        }
    }
}

/// The variable `name` the wicket gave on the env stream.
fn given(name: &str) -> Option<&'static str> {
    let variables = GIVEN_ENV.get().into_iter().flatten();
    variables
        .filter_map(|variable| variable.strip_prefix(name)?.strip_prefix('='))
        .next()
}

/// Hello mode's `/disk`: `?write=<name>` keeps the body as that file, by a rename, with
/// `&sync=1` durably; `?read=<name>` answers the file; else the disk's size and its root's
/// owner. A file's lines are what the tests of a disk read back.
fn disk_answer(query: &str, body: &[u8]) -> String {
    use std::os::unix::fs::MetadataExt as _;
    let Some(Ok(mount)) = DISK.get() else {
        return "no disk\n".into();
    };
    let parameter = |name: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    };
    let mount = std::path::Path::new(mount);
    let result = if let Some(name) = parameter("write") {
        let durable = parameter("sync") == Some("1");
        let file = mount.join(name);
        let partial = mount.join(format!("{name}.partial"));
        std::fs::File::create(&partial)
            .and_then(|mut written| {
                use std::io::Write as _;
                written.write_all(body)?;
                if durable {
                    written.sync_all()?;
                }
                std::fs::rename(&partial, &file)
            })
            .and_then(|()| match durable {
                true => std::fs::File::open(mount)?.sync_all(),
                false => Ok(()),
            })
            .map(|()| format!("wrote {name}\n"))
    } else if let Some(name) = parameter("read") {
        std::fs::read(mount.join(name)).map(|file| String::from_utf8_lossy(&file).into_owned())
    } else {
        std::fs::metadata(mount).map(|root| {
            let mut fs = std::mem::MaybeUninit::<libc::statvfs>::uninit();
            let path = CString::new(mount.as_os_str().as_encoded_bytes()).expect("a parameter");
            // SAFETY: statvfs of a NUL-terminated path into a buffer of its type
            let mib = match unsafe { libc::statvfs(path.as_ptr(), fs.as_mut_ptr()) } {
                0 => {
                    // SAFETY: filled by the successful call
                    let fs = unsafe { fs.assume_init() };
                    (fs.f_blocks * fs.f_frsize) >> 20
                }
                _ => 0,
            };
            format!("mib {mib}\nuid {} gid {}\n", root.uid(), root.gid())
        })
    };
    result.unwrap_or_else(|e| format!("error: {e}\n"))
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
    let body = body
        .collect()
        .await
        .map(|b| b.to_bytes())
        .unwrap_or_default();
    if head.uri.path() == "/egress" {
        let answer = egress(head.uri.query().unwrap_or_default(), &body).await;
        return Ok(Response::new(Full::new(Bytes::from(answer))));
    }
    if head.uri.path() == "/env" {
        let mut lines = GIVEN_ENV.get().cloned().unwrap_or_default().join("\n");
        lines.push('\n');
        return Ok(Response::new(Full::new(Bytes::from(lines))));
    }
    if head.uri.path() == "/disk" {
        let answer = disk_answer(head.uri.query().unwrap_or_default(), &body);
        return Ok(Response::new(Full::new(Bytes::from(answer))));
    }
    if head.uri.path() == "/exit" {
        let answer = exit_answer(head.uri.query().unwrap_or_default(), &log);
        return Ok(Response::new(Full::new(Bytes::from(answer))));
    }
    if head.uri.path() == "/log" {
        let line = format!(
            "{} {}, request {served} of boot id {boot_id}",
            head.method, head.uri
        );
        log.line(Stream::Stdout, line.as_bytes());
        log.line(Stream::Stderr, line.as_bytes());
    }
    let received = body.len();
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

/// Hello mode's `POST /egress?to=<host>:<port>[&rounds=N&reply=K]`: a connection of its
/// own to `to`, as a process connects to a destination of the route's egress. Without
/// `rounds` it sends the body, half-closes and reads the answer to its end; with it, it
/// sends the body `rounds` times (0: none), reading `reply` bytes after each, and times
/// the round trips. Its answer: how long connecting took, then the destination's answer,
/// the round trips' times, or the error.
async fn egress(query: &str, body: &[u8]) -> String {
    let parameter = |name: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    };
    let number = |name| parameter(name).and_then(|value| value.parse::<usize>().ok());
    let Some(to) = parameter("to") else {
        return "error: no to=<host>:<port>\n".into();
    };
    let started = Instant::now();
    let mut stream = match tokio::net::TcpStream::connect(to).await {
        Ok(stream) => stream,
        Err(e) => return format!("error: {to}: {e}\n"),
    };
    let mut answer = format!("connected in {}us\n", started.elapsed().as_micros());
    let _ = stream.set_nodelay(true);
    let exchanged = match number("rounds") {
        None => {
            async {
                stream.write_all(body).await?;
                stream.shutdown().await?;
                let mut got = Vec::new();
                stream.read_to_end(&mut got).await?;
                Ok(format!("answer: {}\n", String::from_utf8_lossy(&got)))
            }
            .await
        }
        Some(rounds) => {
            async {
                let mut reply = vec![0u8; number("reply").unwrap_or(0)];
                let mut times = Vec::with_capacity(rounds);
                for _ in 0..rounds {
                    let round = Instant::now();
                    stream.write_all(body).await?;
                    stream.read_exact(&mut reply).await?;
                    times.push(round.elapsed().as_micros());
                }
                Ok::<_, std::io::Error>(timed(&times))
            }
            .await
        }
    };
    match exchanged {
        Ok(more) => answer.push_str(&more),
        Err(e) => answer.push_str(&format!("error: {e}\n")),
    }
    answer
}

/// Round trips' times, microseconds: the first's (the connection's setup with it), the
/// others' percentiles.
fn timed(times: &[u128]) -> String {
    let Some((first, rest)) = times.split_first() else {
        return "no round trip\n".into();
    };
    let mut rest = rest.to_vec();
    rest.sort_unstable();
    let at = |percent: usize| {
        let at = (rest.len() * percent / 100).min(rest.len().saturating_sub(1));
        rest.get(at).copied().unwrap_or_default()
    };
    format!(
        "round trips: the first {first}us; the other {}: p50 {}us, p90 {}us, p99 {}us, max {}us\n",
        rest.len(),
        at(50),
        at(90),
        at(99),
        rest.last().copied().unwrap_or_default()
    )
}
