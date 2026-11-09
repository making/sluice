//! sluice-wicket-tunnel: the tunnel client of the Rust wickets (`sluice-wicket-runtime-wasi`,
//! `sluice-wicket-runtime-firecracker`), the `sluice.v1.Tunnel` gRPC API of the Java
//! client (`TunnelClient` / `NodeConnection`), and their live access log's feed to a
//! sluice-flume (`sluice.v1.Flume`).
//!
//! ```text
//! sluice server(s) --tunnel stream per node--> wicket --TunnelIo per connection--> the wicket's handler
//! ```
//!
//! The supervisor ([`supervisor`]) asks the `--server` endpoint for the node
//! list (`ListNodes`) only; the membership keeps one tunnel stream
//! ([`session`]) per node at its public url, and a dropped stream reconnects
//! with jittered exponential backoff (ceiling 1s..30s, each wait drawn from
//! its upper half). Exactly one stream per node is kept: the server closes
//! the older registration when a client id re-registers.
//!
//! Every stream advertises the wicket's [`routes::Routes`] that have an
//! instance ready, again on each change of them or of which are ready, and
//! serves the virtual connections the server opens on it
//! ([`tunnel`]: per-connection flow control, the connection limit, the
//! outbound scheduling): a CONNECT is bound to a route of the wicket's
//! [`routes::Table`], whose [`routes::Binding`] serves the connection's byte
//! stream ([`tunnel::TunnelIo`]).
//!
//! On SIGTERM / SIGINT the wicket drains ([`drain`], the Java client's
//! `TunnelClient#drain`): every node stream withdraws the routes and ends
//! once the server acknowledged it and no connection is left on it, within
//! the drain grace ([`serve`]). A wicket evacuating first serves on, its streams
//! advertising its routes, until its apps' controllers moved them elsewhere.
//!
//! A wicket serves its routes over plain TCP as well in local mode
//! ([`listener`]: `--listen`), routed by the host its client asks for.
//!
//! A wicket's readiness probe asks [`ready`] whether it serves.
//!
//! A wicket feeds its live access log -- its guests' output, the requests it
//! answered -- to a sluice-flume as a node does ([`flume`]).

pub mod drain;
pub mod flume;
pub mod listener;
pub mod ready;
pub mod routes;
pub mod session;
pub mod signals;
pub mod supervisor;
pub mod tls;
#[cfg(test)]
mod transport_window_test;
pub mod tunnel;
pub mod workers;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use proto::PeerBuild;
use routes::Table;
use session::{Shared, transport_window};
use signals::Signals;

pub mod proto {
    tonic::include_proto!("sluice.v1");
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Default of `--max-connections`: 256 connections x one 1MiB receive window
/// (plus at most two partly used 16KiB segments and 80 segments' overhead
/// each) = about 265MiB of the server's payload buffered at most.
pub const DEFAULT_MAX_CONNECTIONS: usize = 256;

/// Default of `--tunnel-stall-timeout`, the Java side's
/// `sluice.client.tunnel-stall-timeout`.
pub const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Default of `--drain-grace`, the Java side's `sluice.client.drain-grace`.
pub const DEFAULT_DRAIN_GRACE: Duration = Duration::from_secs(20);

/// How a server that sent no build (it predates the field) is described.
pub const UNKNOWN_BUILD: &str = "unknown (older build)";

/// Longest value kept from each string of a server's build, in chars.
const MAX_FIELD_CHARS: usize = 64;

/// Serves the routes of `shared` through the server at `server` (asked for
/// its nodes, see [`supervisor::supervise`]) until SIGTERM / SIGINT
/// (`signals`), then on until `leave` resolves (the wicket's evacuation: its
/// streams up, following the membership), then drains (see [`drain::drain`])
/// within `drain_grace`. Each signal after the first ends what is under way at
/// once, the evacuation then the drain; a zero `drain_grace` skips the drain.
pub async fn serve<T: Table>(
    shared: Arc<Shared<T>>,
    server: String,
    drain_grace: Duration,
    signals: &mut Signals,
    leave: impl Future<Output = ()>,
) -> Result<(), BoxError> {
    println!("connection limit {}", shared.limit.max());
    println!(
        "gRPC flow control window {} bytes",
        transport_window(shared.limit.max())
    );
    println!(
        "tunnel stall timeout {}s (0 = off)",
        shared.stall_timeout.as_secs()
    );
    println!("drain grace {}s (0 = off)", drain_grace.as_secs());
    let (tx, rx) = mpsc::channel(32);
    let name = {
        let supervisor = supervisor::supervise(rx, tx, shared.clone(), server);
        tokio::pin!(supervisor);
        let name = tokio::select! {
            result = &mut supervisor => return result,
            name = signals.recv() => name,
        };
        tokio::select! {
            result = &mut supervisor => return result,
            () = leave => {}
            name = signals.recv() => eprintln!("{name} during the evacuation: ending it at once"),
        }
        // dropping the supervisor: no membership change opens a stream from now on
        name
    };
    if drain_grace.is_zero() {
        println!("{name}: ending at once (--drain-grace 0)");
        return Ok(());
    }
    println!("{name}: draining");
    let interrupt = async {
        let name = signals.recv().await;
        eprintln!("{name} during the drain: ending it at once");
    };
    drain::drain(&shared, drain_grace, interrupt).await;
    Ok(())
}

/// One line naming a build, e.g. `sluice-wicket-runtime-wasi 0.0.1-SNAPSHOT
/// (<commit>, dirty, built 2026-10-12T01:04:33Z)`; an empty field reads as
/// unknown.
pub fn describe(build: &PeerBuild) -> String {
    let or = |value: &str, unknown: &str| {
        if value.is_empty() {
            unknown.to_string()
        } else {
            value.to_string()
        }
    };
    let mut details = vec![or(&build.commit, "commit unknown")];
    if build.dirty {
        details.push("dirty".into());
    }
    if !build.build_time.is_empty() {
        details.push(format!("built {}", build.build_time));
    }
    format!(
        "{} {} ({})",
        or(&build.implementation, "unknown implementation"),
        or(&build.version, "version unknown"),
        details.join(", ")
    )
}

/// [`describe`] of a build another peer sent: control characters become `?`
/// and each string keeps at most [`MAX_FIELD_CHARS`] chars, so it cannot
/// forge or flood log lines.
pub fn describe_peer(build: &PeerBuild) -> String {
    let bound = |value: &str| -> String {
        let mut bounded: String = value
            .chars()
            .take(MAX_FIELD_CHARS)
            .map(|c| if c.is_control() { '?' } else { c })
            .collect();
        if value.chars().count() > MAX_FIELD_CHARS {
            bounded.push_str("...");
        }
        bounded
    };
    describe(&PeerBuild {
        implementation: bound(&build.implementation),
        version: bound(&build.version),
        commit: bound(&build.commit),
        dirty: build.dirty,
        build_time: bound(&build.build_time),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_names_every_known_part_and_reads_empty_as_unknown() {
        let build = PeerBuild {
            implementation: "sluice-server".into(),
            version: "0.0.1-SNAPSHOT".into(),
            commit: "7312472".into(),
            dirty: true,
            build_time: "2026-10-12T01:04:33Z".into(),
        };
        assert_eq!(
            describe(&build),
            "sluice-server 0.0.1-SNAPSHOT (7312472, dirty, built 2026-10-12T01:04:33Z)"
        );
        assert_eq!(
            describe(&PeerBuild::default()),
            "unknown implementation version unknown (commit unknown)"
        );
    }

    #[test]
    fn describe_peer_bounds_what_the_server_sent() {
        let build = PeerBuild {
            implementation: "sluice-server\nforged".into(),
            version: "9".repeat(MAX_FIELD_CHARS + 1),
            ..Default::default()
        };
        assert_eq!(
            describe_peer(&build),
            format!(
                "sluice-server?forged {}... (commit unknown)",
                "9".repeat(MAX_FIELD_CHARS)
            )
        );
    }
}
