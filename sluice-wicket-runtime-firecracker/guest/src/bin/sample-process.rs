//! A sample of what an image runs, for the tests of an image's rootfs: a static HTTP/1.1
//! server on `PORT` answering what the process is as text -- its uid and gid, the owner and
//! mode of each path of its arguments, its environment -- and printing a line per request,
//! as a process's output.

use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::os::unix::fs::MetadataExt as _;

fn main() {
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
        println!("{}", line.lines().next().unwrap_or_default());
        let body = answer();
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
}

fn answer() -> String {
    // SAFETY: getuid and getgid
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let mut text = format!("uid {uid} gid {gid}\n");
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
