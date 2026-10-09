// Compiles the tunnel proto shared with the Java modules (single source of truth).
use std::path::PathBuf;

fn main() {
    // Repo root relative to this crate: ../../sluice-proto.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let root = PathBuf::from(manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .expect("crate below <repo>/sluice-client-rs/client")
        .to_path_buf();
    let proto = root.join("sluice-proto/src/main/proto/sluice/v1/tunnel.proto");
    tonic_prost_build::configure()
        .build_server(false)
        // the generated `connect` convenience constructor collides with the
        // `Tunnel/Connect` rpc method (E0592); build the client from a channel
        .build_transport(false)
        .compile_protos(&[proto], &[root.join("sluice-proto/src/main/proto")])
        .expect("failed to compile tunnel.proto");
}
