// Compiles the tunnel proto shared with the Java modules (single source of truth)
// and embeds the build identification (git commit, dirty flag, build time).
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "src/build_info.rs"]
mod build_info;

fn main() {
    // Repo root relative to this crate: ../../sluice-proto.
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("crate below <repo>/sluice-wasmlet/wasmlet")
        .to_path_buf();
    let proto = root.join("sluice-proto/src/main/proto/sluice/v1/tunnel.proto");
    tonic_prost_build::configure()
        .build_server(false)
        // the generated `connect` convenience constructor collides with the
        // `Tunnel/Connect` rpc method (E0592); build the client from a channel
        .build_transport(false)
        .compile_protos(&[proto], &[root.join("sluice-proto/src/main/proto")])
        .expect("failed to compile tunnel.proto");
    embed_build_info(&manifest_dir);
}

/// Exports the build identification to the crate as env vars. Any
/// `rerun-if-changed` replaces cargo's default (rerun on any package file), so
/// the sources are listed too: an edit refreshes the dirty flag, a commit or
/// checkout (HEAD, its reflog, the index, the branch ref) refreshes the commit.
fn embed_build_info(manifest_dir: &std::path::Path) {
    for path in [
        "build.rs",
        "Cargo.toml",
        "src",
        "../../sluice-proto/src/main/proto",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    for path in build_info::git_watch_paths("git", manifest_dir) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    let git = build_info::git_state("git", manifest_dir);
    if git.commit.is_empty() {
        println!("cargo:warning=git commit unavailable; the build reports it as unknown");
    }
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=SLUICE_WASMLET_COMMIT={}", git.commit);
    println!("cargo:rustc-env=SLUICE_WASMLET_DIRTY={}", git.dirty);
    println!(
        "cargo:rustc-env=SLUICE_WASMLET_BUILD_TIME={}",
        build_info::rfc3339(secs)
    );
}
