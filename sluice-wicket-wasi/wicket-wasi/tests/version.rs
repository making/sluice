//! `--version` prints the build `build.rs` embedded.

use std::process::Command;

#[test]
fn version_prints_the_embedded_commit() {
    let output = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-wasi"))
        .arg("--version")
        .output()
        .expect("run sluice-wicket-wasi --version");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8");
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8(output.stdout)
                .expect("utf-8")
                .trim()
                .to_string()
        });
    let commit = head.unwrap_or_else(|| "commit unknown".to_string());
    let expected = format!("sluice-wicket-wasi {} ({commit}", env!("CARGO_PKG_VERSION"));
    assert!(
        stdout.starts_with(&expected) && stdout.contains(", built "),
        "expected '{expected}..., built ...', got '{stdout}'"
    );
}

/// The startup line names the same build as `--version`, before anything can
/// fail (here: a component that does not exist).
#[test]
fn startup_logs_the_build_first() {
    let version = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-wasi"))
        .arg("--version")
        .output()
        .expect("run sluice-wicket-wasi --version");
    let startup = Command::new(env!("CARGO_BIN_EXE_sluice-wicket-wasi"))
        .args([
            "--server",
            "grpc://127.0.0.1:1",
            "--wasm",
            "demo.local=/nonexistent/sluice-wicket-wasi-test.wasm",
        ])
        .output()
        .expect("run sluice-wicket-wasi");
    assert!(!startup.status.success(), "{startup:?}");
    let stdout = String::from_utf8(startup.stdout).expect("utf-8");
    assert_eq!(
        stdout.lines().next(),
        Some(
            format!(
                "wicket build: {}",
                String::from_utf8(version.stdout).expect("utf-8").trim()
            )
            .as_str()
        )
    );
}
