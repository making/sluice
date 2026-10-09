//! sluice-wasmlet: a wasm platform on the sluice tunnel.
//!
//! Connects to a sluice server with the same `sluice.v1.Tunnel` gRPC API as
//! the Java client, but instead of relaying to TCP upstreams it terminates
//! HTTP on the tunnel and handles every request with a wasi:http 0.3
//! component (`wasi:http/handler@0.3.0`). One process, N routes.
//!
//! Scope (deliberately minimal):
//! - plaintext `grpc://` control plane only, no TLS / mTLS
//! - single server node: no ListNodes bootstrap, no membership / drain handling
//! - response bodies are buffered; one instance per request
//!
//! Usage:
//!
//! ```text
//! sluice-wasmlet --server grpc://127.0.0.1:8001 --token SECRET \
//!     --wasm demo.local=./examples/hello.wasm
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/sluice.v1.rs"));
}

mod wasm_host;

use pb::frame::Body;
use pb::{Advertise, Error, Frame, Upstream};
use wasm_host::{Loaded, Locator};

struct Route {
    host: String,
    component: Locator,
}

struct Config {
    server: String,
    token: String,
    id: String,
    routes: Vec<Route>,
}

/// Downstream control for one virtual connection, consumed by its serve task.
enum Ctrl {
    /// Bytes to write into the connection (hyper's read side).
    Payload(Vec<u8>),
    /// The server half-closed: no more tunnel -> hyper data.
    WriteClose,
    /// The connection failed on the tunnel side: tear it down.
    Abort,
}

/// conn id -> sender into that connection's serve task.
type Registry = Arc<Mutex<HashMap<i64, mpsc::Sender<Ctrl>>>>;

/// Frames received before the serve task starts are absorbed by this buffer.
const SERVE_BUFFER: usize = 32;

#[tokio::main]
async fn main() {
    if let Err(e) = run(parse_args()).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn parse_args() -> Config {
    let mut config = Config {
        server: String::new(),
        token: String::new(),
        id: "sluice-wasmlet".into(),
        routes: Vec::new(),
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| -> String {
            match args.next() {
                Some(v) => v,
                None => panic!("missing value for {name}"),
            }
        };
        match arg.as_str() {
            "--server" => config.server = value("server"),
            "--token" => config.token = value("token"),
            "--id" => config.id = value("id"),
            "--wasm" => {
                let spec = value("wasm");
                let (host, target) = spec
                    .split_once('=')
                    .unwrap_or_else(|| panic!("--wasm must be host=component-locator, got '{spec}'"));
                let component = Locator::parse(target)
                    .unwrap_or_else(|e| panic!("--wasm {spec}: {e}"));
                config.routes.push(Route {
                    host: host.to_string(),
                    component,
                });
            }
            other => panic!("unknown argument '{other}'"),
        }
    }
    if config.server.is_empty() || config.routes.is_empty() {
        panic!("--server and at least one --wasm host=locator are required");
    }
    config
}

async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let authority = config
        .server
        .strip_prefix("grpc://")
        .unwrap_or(&config.server)
        .to_owned();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{authority}"))?
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .connect()
        .await?;

    // The bidi stream: outbound frames flow through `tx`, inbound via `inbound`.
    let (tx, rx) = mpsc::channel::<Frame>(64);
    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if !config.token.is_empty() {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {}", config.token).parse()?);
    }
    request.metadata_mut().insert("x-sluice-id", config.id.clone().parse()?);

    let mut tunnel = <pb::tunnel_client::TunnelClient<_>>::new(channel);
    let mut inbound = tunnel.connect(request).await?.into_inner();

    // Fetch / prewarm every component (fail fast), then advertise. The target
    // is the component locator (`wasm:<url>`); the server passes it through
    // verbatim as the dial address and the client resolves it back.
    let mut components: HashMap<String, Arc<Loaded>> = HashMap::new();
    let mut upstreams = Vec::with_capacity(config.routes.len());
    for route in &config.routes {
        let target = route.component.target().await?;
        let component = match wasm_host::load(&route.component).await {
            Ok(component) => component,
            Err(e) => return Err(format!("wasm route '{}': {e}", route.host).into()),
        };
        components.insert(target.clone(), component);
        upstreams.push(Upstream {
            host: route.host.clone(),
            target_url: target,
            preserve_host: true,
            ..Default::default()
        });
    }
    tx.send(Frame {
        body: Some(Body::Advertise(Advertise { upstreams })),
    })
    .await?;

    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
    println!("connected to {} ({} wasm route(s))", config.server, config.routes.len());

    while let Some(frame) = inbound.message().await? {
        match frame.body {
            Some(Body::AdvertiseAck(ack)) => {
                println!(
                    "advertise acked by {} (rejected ports: {:?})",
                    ack.node_id, ack.rejected_ports
                );
            }
            Some(Body::Connect(connect)) => {
                let Some(component) = components.get(&connect.address) else {
                    let _ = tx
                        .send(Frame {
                            body: Some(Body::Error(Error {
                                conn_id: connect.conn_id,
                                message: format!("no wasm route for '{}'", connect.address),
                            })),
                        })
                        .await;
                    continue;
                };
                let (ctrl_tx, ctrl_rx) = mpsc::channel(SERVE_BUFFER);
                registry.lock().unwrap().insert(connect.conn_id, ctrl_tx);
                let component = component.clone();
                tokio::spawn(wasm_host::serve(
                    tx.clone(),
                    registry.clone(),
                    connect.conn_id,
                    component,
                    ctrl_rx,
                ));
            }
            Some(Body::Data(data)) => {
                if let Some(sender) = registry.lock().unwrap().get(&data.conn_id) {
                    let _ = sender.send(Ctrl::Payload(data.payload)).await;
                }
            }
            Some(Body::Close(close)) => {
                if let Some(sender) = registry.lock().unwrap().get(&close.conn_id) {
                    let _ = sender.send(Ctrl::WriteClose).await;
                }
            }
            Some(Body::Error(error)) => {
                println!("conn {} failed: {}", error.conn_id, error.message);
                if let Some(sender) = registry.lock().unwrap().get(&error.conn_id) {
                    let _ = sender.send(Ctrl::Abort).await;
                }
            }
            Some(Body::Drain(drain)) => println!("server draining: {}", drain.reason),
            Some(Body::MembershipUpdate(update)) => {
                println!(
                    "membership v{}: {:?}",
                    update.membership_version,
                    update
                        .nodes
                        .iter()
                        .map(|n| n.node_id.as_str())
                        .collect::<Vec<_>>()
                );
            }
            // Advertise from the server is unexpected; keep-alive is a no-op.
            _ => {}
        }
    }
    println!("tunnel stream closed");
    Ok(())
}
