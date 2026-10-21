//! sluice-flume: collects the access log lines of every sluice node while
//! someone watches, and streams them to the consoles.
//!
//! ```text
//! each node --Feed (node dials out)--> flume (ring, one global seq) <--Watch-- console
//! ```

pub mod config;
pub mod hub;
mod service;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Status};

pub mod proto {
    tonic::include_proto!("sluice.v1");
}

use proto::flume_server::FlumeServer;

/// This binary's build, embedded by `build.rs`; an empty commit means git was
/// unavailable when it was built.
pub fn build() -> proto::PeerBuild {
    proto::PeerBuild {
        implementation: "sluice-flume".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("SLUICE_BUILD_COMMIT").into(),
        dirty: env!("SLUICE_BUILD_DIRTY") == "true",
        build_time: env!("SLUICE_BUILD_TIME").into(),
    }
}

/// [`build`] in one line, e.g. `sluice-flume 0.0.1-SNAPSHOT (<commit>, dirty, built
/// 2026-10-12T01:04:33Z)`.
pub fn describe_build() -> String {
    let build = build();
    let mut details = vec![if build.commit.is_empty() {
        "commit unknown".to_string()
    } else {
        build.commit
    }];
    if build.dirty {
        details.push("dirty".into());
    }
    details.push(format!("built {}", build.build_time));
    format!(
        "{} {} ({})",
        build.implementation,
        build.version,
        details.join(", ")
    )
}

/// What the server needs once its listener is bound.
#[derive(Clone, Debug)]
pub struct Settings {
    /// The bearer token every Feed and Watch call must carry.
    pub token: String,
    /// The newest lines kept for backfill and resume.
    pub ring_size: usize,
    /// The newest lines a watch without a cursor begins with.
    pub backfill: usize,
    /// The PEM certificate chain and private key, for TLS.
    pub tls: Option<(Vec<u8>, Vec<u8>)>,
}

/// Serves Feed, Watch and the gRPC health service on `listener` until
/// `shutdown` completes. Streams are not drained: nodes and consoles reconnect.
pub async fn serve(
    listener: TcpListener,
    settings: Settings,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let hub = Arc::new(hub::Hub::new(settings.ring_size, settings.backfill));
    let expected: MetadataValue<_> = format!("Bearer {}", settings.token)
        .parse()
        .expect("the token is a valid header value");
    let flume = FlumeServer::with_interceptor(
        service::FlumeService::new(hub),
        move |request: Request<()>| match request.metadata().get("authorization") {
            Some(given) if constant_time_eq(given.as_bytes(), expected.as_bytes()) => Ok(request),
            _ => Err(Status::unauthenticated("a bearer token is required")),
        },
    );
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("sluice.v1.Flume", tonic_health::ServingStatus::Serving)
        .await;
    let mut server = Server::builder()
        // a node or console gone without a word is noticed and dropped
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if let Some((cert, key)) = settings.tls {
        server =
            server.tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))?;
    }
    let incoming = TcpListenerStream::new(listener);
    tokio::select! {
        served = server.add_service(health).add_service(flume).serve_with_incoming(incoming) => served,
        () = shutdown => Ok(()),
    }
}

/// Compares without an early exit, so the time taken tells nothing of the token.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}
