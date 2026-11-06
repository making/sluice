// Compiles the flume proto shared with the Java modules (single source of truth), the
// client of its watch only, embeds the build identification (git commit, dirty flag,
// build time) and the component a static site is staged with: sluice-wicket-site,
// `SLUICE_WICKET_SITE_WASM` when given (a build without its toolchain, as the Docker one),
// else built here with the toolchain it pins.
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Repo root relative to this crate: ../sluice-proto.
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .expect("crate below <repo>/sluice-wicket-cf-api")
        .to_path_buf();
    let protos = root.join("sluice-proto/src/main/proto");
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                protos.join("sluice/v1/flume.proto"),
                protos.join("sluice/v1/build.proto"),
            ],
            &[protos],
        )
        .expect("failed to compile flume.proto");
    embed_site(&root);
    sluice_build_info::embed(&[
        "build.rs",
        "Cargo.toml",
        "src",
        "../sluice-proto/src/main/proto",
    ]);
}

/// Copies sluice-wicket-site's component to `$OUT_DIR/site.wasm`, built unless given.
fn embed_site(root: &Path) {
    println!("cargo:rerun-if-env-changed=SLUICE_WICKET_SITE_WASM");
    let built = match std::env::var_os("SLUICE_WICKET_SITE_WASM") {
        Some(given) => {
            println!("cargo:rerun-if-changed={}", given.display());
            PathBuf::from(given)
        }
        None => build_site(root),
    };
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("site.wasm");
    std::fs::copy(&built, &out)
        .unwrap_or_else(|e| panic!("sluice-wicket-site's component {}: {e}", built.display()));
}

/// Builds sluice-wicket-site: its workspace, its toolchain (the rustup proxy picks it), its
/// target directory. Nothing of this build leaks into it: its environment is the user's, no
/// more.
fn build_site(root: &Path) -> PathBuf {
    let site = root.join("sluice-wicket-site");
    for watched in [
        "sluice-wicket-site/src",
        "sluice-wicket-site/Cargo.toml",
        "sluice-wicket-site/Cargo.lock",
        "sluice-wicket-site/rust-toolchain.toml",
        "sluice-wicket-site/.cargo",
        "sluice-wicket-runtime-wasi/wit",
    ] {
        println!("cargo:rerun-if-changed={}", root.join(watched).display());
    }
    let mut cargo = Command::new("cargo");
    cargo
        .args(["build", "--release", "--locked"])
        .current_dir(&site)
        .env_clear();
    for (name, value) in std::env::vars_os() {
        let name_text = name.to_string_lossy();
        let kept = matches!(
            name_text.as_ref(),
            "PATH" | "HOME" | "USER" | "TMPDIR" | "RUSTUP_HOME" | "CARGO_HOME" | "CARGO_TARGET_DIR"
        ) || name_text.to_ascii_lowercase().ends_with("_proxy")
            || name_text.starts_with("SSL_CERT_")
            || name_text.starts_with("CARGO_NET_")
            || name_text.starts_with("CARGO_HTTP_");
        if kept {
            cargo.env(name, value);
        }
    }
    let output = cargo
        .output()
        .expect("run cargo (rustup) for sluice-wicket-site, or set SLUICE_WICKET_SITE_WASM");
    assert!(
        output.status.success(),
        "the build of {} failed (or set SLUICE_WICKET_SITE_WASM): {}",
        site.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| site.join("target"))
        .join("wasm32-wasip3/release/sluice_wicket_site.wasm")
}
