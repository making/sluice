// Compiles the tunnel proto shared with the Java modules (single source of truth).
use std::path::PathBuf;

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let protos = manifest_dir
        .parent()
        .expect("crate below <repo>/sluice-tunnel")
        .join("sluice-proto/src/main/proto");
    tonic_prost_build::configure()
        .build_server(false)
        // the generated `connect` convenience constructor collides with the
        // `Tunnel/Connect` rpc method (E0592); build the client from a channel
        .build_transport(false)
        .compile_protos(
            &[
                protos.join("sluice/v1/tunnel.proto"),
                protos.join("sluice/v1/build.proto"),
            ],
            &[protos],
        )
        .expect("failed to compile the protos");
}
