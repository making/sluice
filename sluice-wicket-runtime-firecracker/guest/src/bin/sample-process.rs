//! A sample of what an image runs, for the tests of an image's rootfs: a static HTTP/1.1
//! server on `PORT` answering what the process is as text -- its uid and gid, whether it opens `/dev/stdout` and `/dev/stderr` by path, the owner and
//! mode of each path of its arguments, its environment -- and printing a line per request,
//! as a process's output. `/egress?to=<host>:<port>` connects as a process does, half-closes
//! and answers with what came, `/exit` answers and exits. `SAMPLE_WAIT_MS` delays its listening,
//! `SAMPLE_EXIT` has it exit instead, as a process failing to start does.

use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt as _;

fn main() {
    if std::env::var_os("SAMPLE_EXIT").is_some() {
        eprintln!("sample process: exiting as told");
        std::process::exit(82);
    }
    if let Some(wait) = std::env::var("SAMPLE_WAIT_MS")
        .ok()
        .and_then(|ms| ms.parse().ok())
    {
        std::thread::sleep(std::time::Duration::from_millis(wait));
    }
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = TcpListener::bind(("127.0.0.1", port.parse().expect("PORT"))).expect("bind");
    println!("sample process on port {port}");
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
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
        let body = match path.strip_prefix("/egress?to=") {
            Some(to) => egress(to),
            None => answer(),
        };
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        if path == "/exit" {
            drop(stream);
            std::process::exit(83);
        }
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
    let mut text = format!("uid {uid} gid {gid}\n");
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
