//! A sample of what an image runs, for the tests of an image's rootfs: a static HTTP/1.1
//! server on `PORT` answering what the process is as text -- its VM's boot id (a clone's, its
//! snapshot's), the requests it served, its uid and gid, whether it opens `/dev/stdout` and
//! `/dev/stderr` by path, the owner and mode of each path of its arguments, its environment --
//! and printing a line per request, as a process's output. `/egress?to=<host>:<port>` connects
//! as a process does, half-closes and answers with what came, `/exit` answers and exits,
//! `/oom` answers and takes memory until the kernel's OOM killer takes it,
//! `/ready?after=N` answers 503 to the first N of them, as a readiness check sees a process
//! starting, 200 after, counting none served.
//! Told to stop (SIGTERM, a stop notice's) it says so on its stdout and ends.
//! `SAMPLE_WAIT_MS` delays its listening, `SAMPLE_EXIT` has it exit instead, as a process failing
//! to start does; `SAMPLE_SPEAK_FIRST` has it send that line as each connection opens, as a
//! protocol whose server speaks first does.

use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt as _;
use std::sync::atomic::{AtomicU64, Ordering};

/// The requests the process served.
static SERVED: AtomicU64 = AtomicU64::new(0);

/// The `/ready` requests it answered.
static READIES: AtomicU64 = AtomicU64::new(0);

/// SIGTERM, a stop notice's: it says so and ends.
extern "C" fn told_to_stop(_: libc::c_int) {
    const SAID: &[u8] = b"sample process: told to stop\n";
    // SAFETY: write and _exit are async-signal-safe, the line a constant
    unsafe {
        libc::write(1, SAID.as_ptr().cast(), SAID.len());
        libc::_exit(0);
    }
}

fn main() {
    // SAFETY: the handler calls what a signal handler may
    unsafe {
        libc::signal(
            libc::SIGTERM,
            told_to_stop as *const () as libc::sighandler_t,
        );
    }
    if std::env::var_os("SAMPLE_EXIT").is_some() {
        eprintln!("sample process: exiting as told");
        std::process::exit(82);
    }
    if let Some(wait) = std::env::var("SAMPLE_WAIT_MS")
        .ok()
        .and_then(|ms| ms.parse().ok())
    {
        eprintln!("sample process: waiting {wait}ms");
        std::thread::sleep(std::time::Duration::from_millis(wait));
    }
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = TcpListener::bind(("127.0.0.1", port.parse().expect("PORT"))).expect("bind");
    println!("sample process on port {port}");
    let speak_first = std::env::var("SAMPLE_SPEAK_FIRST").ok();
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        if let Some(line) = &speak_first {
            let _ = writeln!(stream, "{line}");
        }
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while !request.windows(4).any(|end| end == b"\r\n\r\n") {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => request.extend_from_slice(&chunk[..read]),
            }
        }
        let line = String::from_utf8_lossy(&request);
        let first = line.lines().next().unwrap_or_default();
        println!("{first}");
        let path = first.split(' ').nth(1).unwrap_or_default();
        // a stream dropped with no request, a dial looking at the process, counts none
        if !first.is_empty() && !path.starts_with("/ready") {
            SERVED.fetch_add(1, Ordering::Relaxed);
        }
        let (status, body) = match path.strip_prefix("/egress?to=") {
            Some(to) => ("200 OK", egress(to)),
            None if path.starts_with("/ready") => ready(path),
            None => ("200 OK", answer()),
        };
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        if path == "/exit" {
            drop(stream);
            std::process::exit(83);
        }
        if path == "/oom" {
            drop(stream);
            take_all_memory();
        }
    }
}

/// Writes to ever more memory of its own until the kernel's OOM killer takes it.
fn take_all_memory() -> ! {
    let mut taken: Vec<Vec<u8>> = Vec::new();
    loop {
        // every page written: the guest's memory, not only its address space
        taken.push(vec![1u8; 1 << 20]);
        std::hint::black_box(&taken);
    }
}

/// `/ready?after=N`: 503 to the first N, 200 after.
fn ready(path: &str) -> (&'static str, String) {
    let after = path
        .split_once("after=")
        .and_then(|(_, after)| after.parse::<u64>().ok())
        .unwrap_or(0);
    match READIES.fetch_add(1, Ordering::Relaxed) < after {
        true => ("503 Service Unavailable", "not ready\n".into()),
        false => ("200 OK", "ready\n".into()),
    }
}

fn egress(to: &str) -> String {
    let exchanged = TcpStream::connect(to).and_then(|mut stream| {
        stream.shutdown(Shutdown::Write)?;
        let mut got = String::new();
        stream.read_to_string(&mut got)?;
        Ok(got)
    });
    match exchanged {
        Ok(got) => format!("answer: {got}\n"),
        Err(e) => format!("error: {to}: {e}\n"),
    }
}

fn answer() -> String {
    // SAFETY: getuid and getgid
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap_or_default();
    let served = SERVED.load(Ordering::Relaxed);
    let mut text = format!("boot id: {boot_id}served: {served}\nuid {uid} gid {gid}\n");
    // by path, as an image's log files linked to them do
    for path in ["/dev/stdout", "/dev/stderr"] {
        let opened = std::fs::OpenOptions::new().append(true).open(path);
        text.push_str(&match opened {
            Ok(_) => format!("{path}: opened\n"),
            Err(e) => format!("{path}: {e}\n"),
        });
    }
    for path in std::env::args().skip(1) {
        match std::fs::metadata(&path) {
            Ok(file) => text.push_str(&format!(
                "{path} uid {} gid {} mode {:o}\n",
                file.uid(),
                file.gid(),
                file.mode() & 0o7777
            )),
            Err(e) => text.push_str(&format!("{path}: {e}\n")),
        }
    }
    let mut env: Vec<String> = std::env::vars()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    env.sort();
    text.push_str(&env.join("\n"));
    text.push('\n');
    text
}
