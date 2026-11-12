//! A PostgreSQL for the tests of the state's store: one a test binary shares, each test with a
//! database of its own. `SLUICE_CF_API_TEST_POSTGRES` names one running
//! (`postgres://user:password@host:port/database`, its user making databases); else a
//! container of `postgres:18-alpine` (Docker on the PATH; CI's runner has it), which ends with
//! the test process: its stdin's end stops the server.

use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const IMAGE: &str = "postgres:18-alpine";

/// Hang guard of a start, the image pulled.
const START: Duration = Duration::from_secs(120);

/// The server's URL, less its database.
fn server() -> &'static str {
    static SERVER: OnceLock<String> = OnceLock::new();
    SERVER.get_or_init(|| match std::env::var("SLUICE_CF_API_TEST_POSTGRES") {
        Ok(url) => url.rsplit_once('/').expect("a database").0.to_string(),
        Err(_) => container(),
    })
}

fn container() -> String {
    let name = format!("sluice-cf-api-test-{}", std::process::id());
    // no fsync: a test's database is lost with it
    let run = "docker-entrypoint.sh postgres -c fsync=off & cat >/dev/null; kill -INT $!; wait";
    let child = Command::new("docker")
        .args(["run", "--rm", "-i", "--name", &name])
        .args(["-e", "POSTGRES_PASSWORD=test", "-p", "127.0.0.1::5432"])
        .args([IMAGE, "sh", "-c", run])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("docker on the PATH");
    // held open until the process ends
    std::mem::forget(child);
    let began = Instant::now();
    loop {
        assert!(began.elapsed() < START, "{IMAGE} did not start");
        let port = Command::new("docker")
            .args(["port", &name, "5432"])
            .output();
        let port = String::from_utf8(port.expect("docker on the PATH").stdout).unwrap();
        if let Some((_, port)) = port.lines().next().and_then(|at| at.rsplit_once(':')) {
            return format!("postgres://postgres:test@127.0.0.1:{port}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A connection to the database `database` of the server, once it takes one.
async fn connect(database: &str) -> tokio_postgres::Client {
    let url = format!("{}/{database}", server());
    let began = Instant::now();
    loop {
        match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(connection);
                return client;
            }
            Err(e) => assert!(began.elapsed() < START, "{url}: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A new database: its URL, and a connection to it for what a test does behind the store.
pub async fn database() -> (String, tokio_postgres::Client) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let name = format!(
        "t{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let admin = connect("postgres").await;
    let create = format!("CREATE DATABASE {name}");
    admin.batch_execute(&create).await.expect("a database");
    (format!("{}/{name}", server()), connect(&name).await)
}
