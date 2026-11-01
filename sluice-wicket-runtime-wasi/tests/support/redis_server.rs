//! A `redis-server` child process for the redis store's tests (the unit
//! tests include this file too), killed on drop: on a unix socket, stopped
//! and started again at its path, or serving TLS alone on a free port.
//! `redis-server` (built with TLS) is on the PATH wherever the tests run;
//! CI installs it.
#![allow(dead_code, reason = "each includer uses some of it")]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Hang guard of a start.
const START: Duration = Duration::from_secs(30);

pub struct RedisServer {
    child: Option<Child>,
    args: Vec<String>,
    socket: Option<PathBuf>,
    /// Where the clients connect.
    pub url: String,
}

impl RedisServer {
    /// One on a unix socket in /tmp: a temp dir's path may be past the
    /// socket path's limit (104 bytes on macOS).
    pub fn unix() -> RedisServer {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let socket = PathBuf::from(format!(
            "/tmp/sluice-redis-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut server = RedisServer {
            child: None,
            args: args(&["--port", "0", "--unixsocket", socket.to_str().unwrap()]),
            url: format!("redis+unix://{}", socket.display()),
            socket: Some(socket),
        };
        server.start();
        server
    }

    /// One serving TLS alone on a free port of 127.0.0.1, with the
    /// certificate and key of the PEM files `cert` and `key`.
    pub fn tls(cert: &Path, key: &Path) -> RedisServer {
        for _ in 0..10 {
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .and_then(|listener| listener.local_addr())
                .unwrap()
                .port()
                .to_string();
            let mut server = RedisServer {
                child: None,
                args: args(&[
                    "--port",
                    "0",
                    "--bind",
                    "127.0.0.1",
                    "--tls-port",
                    &port,
                    "--tls-cert-file",
                    cert.to_str().unwrap(),
                    "--tls-key-file",
                    key.to_str().unwrap(),
                    "--tls-auth-clients",
                    "no",
                ]),
                url: format!("rediss://127.0.0.1:{port}"),
                socket: None,
            };
            match server.try_start() {
                Ok(()) => return server,
                // taken since it was free
                Err(log) if log.contains("in use") => continue,
                Err(log) => panic!("redis-server did not start: {log}"),
            }
        }
        panic!("no free port for redis-server");
    }

    /// Starts it (again).
    pub fn start(&mut self) {
        if let Err(log) = self.try_start() {
            panic!("redis-server did not start: {log}");
        }
    }

    /// Kills it.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Starts it, ready once it logs so ("Ready to accept connections" in
    /// 7.2+, "The server is now ready to accept connections at" on a unix
    /// socket before); Err: its log when it exits first.
    fn try_start(&mut self) -> Result<(), String> {
        let mut child = Command::new("redis-server")
            .args(&self.args)
            .stdout(Stdio::piped())
            .spawn()
            .expect("run redis-server (is it installed?)");
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (lines, received) = mpsc::channel();
        // read to the end: a full pipe would block its log
        std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                let _ = lines.send(line);
            }
        });
        let deadline = Instant::now() + START;
        let mut log = String::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match received.recv_timeout(left) {
                Ok(line)
                    if line
                        .to_ascii_lowercase()
                        .contains("ready to accept connections") =>
                {
                    self.child = Some(child);
                    return Ok(());
                }
                Ok(line) => log.push_str(&(line + "\n")),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = child.wait();
                    return Err(log);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = child.kill();
                    panic!("redis-server not ready in {START:?}: {log}");
                }
            }
        }
    }
}

/// `specific` and what every server takes: nothing kept on disk.
fn args(specific: &[&str]) -> Vec<String> {
    specific
        .iter()
        .chain(&["--save", "", "--appendonly", "no"])
        .map(|arg| arg.to_string())
        .collect()
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop();
        if let Some(socket) = &self.socket {
            let _ = std::fs::remove_file(socket);
        }
    }
}
