//! The guest side of sluice-wicket-firecracker: the microVM's `/init` (PID 1).
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

use std::convert::Infallible;
use std::ffi::CString;
use std::os::unix::process::CommandExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};

mod wire;

const VSOCK_PORT: u32 = 8080;

/// Hello mode's HTTP server.
const HELLO_PORT: u16 = 8081;

const LAUNCH_CONFIG: &str = "/sluice/init.json";

static SERVED: AtomicU64 = AtomicU64::new(0);

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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let result = runtime.block_on(async {
        loopback_up()?;
        let port = match launch {
            Some(launch) => {
                let port = launch.port;
                start(launch)?;
                port
            }
            None => {
                let listener = tokio::net::TcpListener::bind(("127.0.0.1", HELLO_PORT)).await?;
                tokio::spawn(hello(listener, booted));
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

/// Launcher mode: starts the image's process.
fn start(launch: Launch) -> std::io::Result<()> {
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
        .current_dir(&launch.workdir);
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
    let child = command.spawn()?;
    println!("guest: started {program} (pid {})", child.id());
    // PID 1 reaps every orphan; the process's exit is logged, the VM stays up
    std::thread::spawn(move || {
        let pid = child.id() as libc::pid_t;
        loop {
            let mut status = 0;
            // SAFETY: a plain wait for any child
            let reaped = unsafe { libc::wait(&mut status) };
            if reaped == pid {
                eprintln!("guest: {program} exited (status {status})");
            } else if reaped < 0 {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    });
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
                    if let Err(e) = wire::relay(upstream, stream).await {
                        eprintln!("guest: relay: {e}");
                    }
                }
                Err(e) => eprintln!("guest: {target}: {e}"),
            }
        });
    }
}

/// Hello mode: the guest's own HTTP server.
async fn hello(listener: tokio::net::TcpListener, booted: Instant) {
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|id| id.trim().to_string())
        .unwrap_or_else(|_| "unknown (no /proc)".into());
    println!("guest: hello server, boot id {boot_id}");
    let boot_id: &'static str = Box::leak(boot_id.into_boxed_str());
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let service =
                hyper::service::service_fn(move |request| answer(request, booted, boot_id));
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

async fn answer(
    request: Request<hyper::body::Incoming>,
    booted: Instant,
    boot_id: &str,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let served = SERVED.fetch_add(1, Ordering::Relaxed) + 1;
    let (head, body) = request.into_parts();
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
    let text = format!(
        "Hello from a Firecracker microVM\n\
         boot id: {boot_id}\n\
         served by this VM: {served}\n\
         since boot: {}ms\n\
         request: {} {} {:?}, {received} body bytes, x-forwarded-for {forwarded}\n",
        booted.elapsed().as_millis(),
        head.method,
        head.uri,
        head.version,
    );
    Ok(Response::new(Full::new(Bytes::from(text))))
}
