//! A sample buildpack, for the tests of a build (`stage`): its `bin/detect` and `bin/build`,
//! one static program told apart by the name it is run as, in a builder that has no shell.
//!
//! It detects an app holding `sample.app`. Its build says what it sees (the app's
//! `STAGED_VAR`, as `/platform/env` gave it; its user), waits the app's `SAMPLE_BUILD_MS`, puts the buildpack's `serve` -- the
//! sample process -- in a launch layer and names it the `web` process, and keeps a cache
//! layer no droplet holds, counting the builds it was restored to.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

/// A buildpack's exit status for an app that is not its own.
const NOT_DETECTED: i32 = 100;

fn main() {
    let mut args = std::env::args();
    let program = args.next().unwrap_or_default();
    let ran = match Path::new(&program)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some("detect") => {
            if !Path::new("sample.app").is_file() {
                std::process::exit(NOT_DETECTED);
            }
            Ok(())
        }
        Some("build") => build(),
        _ => Err(std::io::Error::other("run as bin/detect or bin/build")),
    };
    if let Err(e) = ran {
        eprintln!("sample buildpack: {e}");
        std::process::exit(1);
    }
}

fn build() -> std::io::Result<()> {
    let dir = |name: &str| {
        std::env::var_os(name)
            .map(PathBuf::from)
            .ok_or_else(|| std::io::Error::other(format!("no {name}")))
    };
    let (layers, buildpack) = (dir("CNB_LAYERS_DIR")?, dir("CNB_BUILDPACK_DIR")?);
    // SAFETY: plain getuid / getgid
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    println!(
        "sample buildpack: building as {uid}:{gid}, STAGED_VAR={}",
        std::env::var("STAGED_VAR").unwrap_or_default()
    );
    eprintln!("sample buildpack: a line on stderr");
    // a build that takes its time, for a test of what happens meanwhile
    if let Some(millis) = std::env::var("SAMPLE_BUILD_MS")
        .ok()
        .and_then(|millis| millis.parse().ok())
    {
        std::thread::sleep(std::time::Duration::from_millis(millis));
    }
    // where a buildpack's tools write: the builder's root is the build's to write
    std::fs::write(dir("HOME")?.join(".sample-build"), b"built")?;
    let bin = layers.join("runtime/bin");
    std::fs::create_dir_all(&bin)?;
    std::fs::copy(buildpack.join("serve"), bin.join("serve"))?;
    std::fs::set_permissions(bin.join("serve"), std::fs::Permissions::from_mode(0o755))?;
    std::fs::write(layers.join("runtime.toml"), "[types]\n  launch = true\n")?;
    // restored: the metadata alone, the types this build's to set again
    let restored = std::fs::read_to_string(layers.join("cache.toml")).unwrap_or_default();
    let builds = restored
        .lines()
        .find_map(|line| line.trim().strip_prefix("builds = "))
        .and_then(|builds| builds.parse::<u32>().ok())
        .filter(|_| !restored.contains("[types]"))
        .unwrap_or(0)
        + 1;
    let kept = std::fs::read_to_string(layers.join("cache/kept")).unwrap_or_default();
    println!("sample buildpack: build {builds} of the cache, which kept '{kept}'");
    std::fs::create_dir_all(layers.join("cache"))?;
    std::fs::write(layers.join("cache/kept"), b"not the droplet's")?;
    std::fs::write(
        layers.join("cache.toml"),
        format!("[types]\n  cache = true\n[metadata]\n  builds = {builds}\n"),
    )?;
    std::fs::write(Path::new("built.txt"), b"built by the sample buildpack\n")?;
    std::fs::write(
        layers.join("launch.toml"),
        "[[processes]]\n  type = \"web\"\n  command = [\"serve\"]\n  \
         args = [\"/workspace/sample.app\", \"/workspace/built.txt\"]\n  default = true\n",
    )
}
