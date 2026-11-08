// Compiles the flume proto shared with the Java modules (single source of truth)
// and embeds the build identification (git commit, dirty flag, build time).
use std::path::PathBuf;

fn main() {
    // Repo root relative to this crate: ../sluice-proto.
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .expect("crate below <repo>/sluice-flume")
        .to_path_buf();
    let protos = root.join("sluice-proto/src/main/proto");
    tonic_prost_build::configure()
        .compile_protos(
            &[
                protos.join("sluice/v1/flume.proto"),
                protos.join("sluice/v1/build.proto"),
            ],
            &[protos],
        )
        .expect("failed to compile flume.proto");
    // The vendored OTLP logs protos, messages only: the exporter sends them, it
    // serves no gRPC.
    let otlp = manifest_dir.join("proto/opentelemetry/proto");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(
            &[
                otlp.join("common/v1/common.proto"),
                otlp.join("resource/v1/resource.proto"),
                otlp.join("logs/v1/logs.proto"),
                otlp.join("collector/logs/v1/logs_service.proto"),
            ],
            &[manifest_dir.join("proto")],
        )
        .expect("failed to compile the OTLP protos");
    sluice_build_info::embed(&[
        "build.rs",
        "Cargo.toml",
        "src",
        "proto",
        "../sluice-proto/src/main/proto",
    ]);
}
