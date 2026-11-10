//! Stage mode (`wire::STAGE_PARAMETER`): the VM is a build's. Its rootfs is a Cloud Native
//! Buildpacks builder's, and /init is the platform of one build, served on the stage stream
//! (`wire::STAGE_PORT`):
//!
//! ```text
//! the source (a tar) -> /workspace
//! /cnb/lifecycle/detector, then /cnb/lifecycle/builder, as the builder's user
//! /layers, /workspace, the launcher -> the droplet (a tar)
//! ```
//!
//! No analyzer runs (no registry is in reach, and nothing is restored): /init writes the
//! `analyzed.toml` naming the run image. No exporter either: the droplet is what the
//! platform spec has an exporter put over the run image -- the launch layers and their
//! `.toml`, `config/metadata.toml`, the launch SBOM, the app directory, the launcher and a
//! link to it per process type -- as a tar of paths below `/`, for the wicket to lay over
//! the run image's layers.
//!
//! The builder's root is read-only (shared, as every rootfs): the phases run in an overlay
//! of it on the build's disk (`wire::STAGE_DISK`), so buildpacks write where they would in a
//! container, `/layers` and `/workspace` at the paths the app runs at.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::io::{self, Write as _};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener, VsockStream};

use crate::log::Log;
use crate::wire;

/// The overlay's mount: the root the phases see.
const ROOT: &str = "/tmp/root";

/// The paths of the build, in that root (the lifecycle's defaults).
const LAYERS: &str = "layers";
const WORKSPACE: &str = "workspace";
const PLATFORM_ENV: &str = "platform/env";
const LIFECYCLE: &str = "/cnb/lifecycle";

/// A piece of the source or the droplet relayed at once.
const PIECE: usize = 64 * 1024;

/// The builder's user and environment, from the image's config.
pub struct Builder {
    pub uid: u32,
    pub gid: u32,
    /// `NAME=value`.
    pub env: Vec<String>,
}

/// What a build is given.
#[derive(Deserialize)]
struct Request {
    /// The app's environment, as `/platform/env` has it.
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// `CNB_PLATFORM_API`.
    platform_api: String,
    /// The run image, pinned to its digest, and as its builder names it.
    run_image: String,
    run_image_name: String,
    os: String,
    arch: String,
}

/// What a build answers: its error, or what the droplet following runs.
#[derive(Serialize, Default)]
struct Outcome {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// The process types, each `/cnb/process/<type>` in the droplet.
    processes: Vec<String>,
    /// The default one, when the buildpacks named one.
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,
}

/// Serves the one build of the VM on the stage stream; then waits for the wicket to end it.
pub async fn serve(builder: Builder, log: Arc<Log>) -> io::Result<()> {
    let listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, wire::STAGE_PORT))?;
    let (mut stream, _) = listener.accept().await?;
    let droplet = Path::new(wire::STAGE_DISK).join("droplet.tar");
    let outcome = match build(&mut stream, &builder, &log, &droplet).await {
        Ok(outcome) => outcome,
        Err(e) => Outcome {
            error: Some(e.to_string()),
            ..Outcome::default()
        },
    };
    let failed = outcome.error.is_some();
    let json = serde_json::to_vec(&outcome).map_err(io::Error::other)?;
    wire::write_json(&mut stream, &json).await?;
    if !failed {
        send(&mut stream, &droplet).await?;
    }
    // the stream stays: its end is the VM's
    std::future::pending::<()>().await;
    Ok(())
}

async fn build(
    stream: &mut VsockStream,
    builder: &Builder,
    log: &Arc<Log>,
    droplet: &Path,
) -> io::Result<Outcome> {
    let request = wire::read_json(stream, wire::MAX_ENV).await?;
    let request: Request = serde_json::from_slice(&request).map_err(io::Error::other)?;
    wire::answer_env(stream).await?;
    let (uid, gid) = (builder.uid, builder.gid);
    blocking(move || root(uid, gid)).await?;
    let source = Path::new(wire::STAGE_DISK).join("source.tar");
    receive(stream, &source).await?;
    let root = Path::new(ROOT);
    blocking(move || {
        let workspace = root.join(WORKSPACE);
        let mut archive = tar::Archive::new(std::fs::File::open(&source)?);
        archive.set_preserve_permissions(true);
        archive.unpack(&workspace)?;
        std::fs::remove_file(&source)?;
        own(&workspace, uid, gid)
    })
    .await?;
    let layers = root.join(LAYERS);
    write_owned(
        &layers.join("analyzed.toml"),
        analyzed(&request).as_bytes(),
        builder,
    )?;
    for (name, value) in &request.env {
        if name.is_empty() || name.contains(['/', '\0']) {
            continue;
        }
        std::fs::write(root.join(PLATFORM_ENV).join(name), value)?;
    }
    for phase in ["detector", "builder"] {
        let status = run(phase, builder, &request.platform_api, log).await?;
        if !status.success() {
            return Err(io::Error::other(match (phase, status.code()) {
                ("detector", Some(20)) => "no buildpack of the builder detected the app".into(),
                _ => format!("the {phase} failed ({status})"),
            }));
        }
    }
    let droplet = droplet.to_path_buf();
    blocking(move || pack(root, &droplet)).await
}

/// Runs `work` off the runtime's threads.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(io::Error::other)?
}

/// The root the phases run in: the builder's, written to on the build's disk, with what a
/// container has of the kernel's, the names of the egress, and the build's directories.
fn root(uid: u32, gid: u32) -> io::Result<()> {
    let disk = Path::new(wire::STAGE_DISK);
    let (upper, work) = (disk.join("upper"), disk.join("work"));
    for dir in [upper.as_path(), work.as_path(), Path::new(ROOT)] {
        std::fs::create_dir_all(dir)?;
    }
    // the upper directory is the root's: as the builder's
    std::fs::set_permissions(&upper, std::fs::Permissions::from_mode(0o755))?;
    std::os::unix::fs::chown(&upper, Some(0), Some(0))?;
    let options = format!(
        "lowerdir=/,upperdir={},workdir={}",
        upper.display(),
        work.display()
    );
    mount("overlay", ROOT, "overlay", Some(&options)).map_err(|e| {
        io::Error::other(format!(
            "the overlay of the builder's root: {e} (the guest kernel needs CONFIG_OVERLAY_FS)"
        ))
    })?;
    let root = Path::new(ROOT);
    for (source, target, fstype) in [
        ("proc", "proc", "proc"),
        ("sysfs", "sys", "sysfs"),
        ("devtmpfs", "dev", "devtmpfs"),
        ("tmpfs", "dev/shm", "tmpfs"),
        ("devpts", "dev/pts", "devpts"),
        ("tmpfs", "tmp", "tmpfs"),
    ] {
        let target = root.join(target);
        std::fs::create_dir_all(&target)?;
        mount(source, &target.to_string_lossy(), fstype, None)
            .map_err(|e| io::Error::other(format!("mount {}: {e}", target.display())))?;
    }
    // the names of the egress are in /init's, bound over the rootfs's: not the overlay's
    std::fs::copy("/etc/hosts", root.join("etc/hosts"))?;
    for dir in [LAYERS, WORKSPACE] {
        let dir = root.join(dir);
        std::fs::create_dir_all(&dir)?;
        std::os::unix::fs::chown(&dir, Some(uid), Some(gid))?;
    }
    std::fs::create_dir_all(root.join(PLATFORM_ENV))
}

/// Mounts `source` of `fstype` at `target`, with the file system's `options`.
pub fn mount(source: &str, target: &str, fstype: &str, options: Option<&str>) -> io::Result<()> {
    let (source, target, fstype) = (
        CString::new(source)?,
        CString::new(target)?,
        CString::new(fstype)?,
    );
    let options = options.map(CString::new).transpose()?;
    // SAFETY: NUL-terminated strings, the data the file system's options or none
    let mounted = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            0,
            options
                .as_ref()
                .map_or(std::ptr::null(), |options| options.as_ptr().cast()),
        )
    };
    match mounted {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Reads `[len: u64 BE][bytes]` of `stream` into the file `to`.
async fn receive(stream: &mut VsockStream, to: &Path) -> io::Result<()> {
    let mut left = stream.read_u64().await?;
    let mut file = std::fs::File::create(to)?;
    let mut piece = vec![0u8; PIECE];
    while left > 0 {
        let wanted = piece.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let n = AsyncReadExt::read(stream, &mut piece[..wanted]).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        std::io::Write::write_all(&mut file, &piece[..n])?;
        left -= n as u64;
    }
    Ok(())
}

/// Writes the file `from` to `stream` as `[len: u64 BE][bytes]`.
async fn send(stream: &mut VsockStream, from: &Path) -> io::Result<()> {
    let mut file = std::fs::File::open(from)?;
    stream.write_u64(file.metadata()?.len()).await?;
    let mut piece = vec![0u8; PIECE];
    loop {
        let n = std::io::Read::read(&mut file, &mut piece)?;
        if n == 0 {
            return Ok(());
        }
        AsyncWriteExt::write_all(stream, &piece[..n]).await?;
    }
}

/// Gives `path` and all below it to `uid` and `gid`, no link followed.
fn own(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
    std::os::unix::fs::lchown(path, Some(uid), Some(gid))?;
    if std::fs::symlink_metadata(path)?.is_dir() {
        for entry in std::fs::read_dir(path)? {
            own(&entry?.path(), uid, gid)?;
        }
    }
    Ok(())
}

fn write_owned(path: &Path, bytes: &[u8], builder: &Builder) -> io::Result<()> {
    std::fs::write(path, bytes)?;
    std::os::unix::fs::chown(path, Some(builder.uid), Some(builder.gid))
}

/// `text` as a TOML basic string.
fn quoted(text: &str) -> String {
    let mut quoted = String::from('"');
    for c in text.chars() {
        match c {
            '"' | '\\' => {
                quoted.push('\\');
                quoted.push(c);
            }
            c if c.is_control() => quoted.push_str(&format!("\\u{:04x}", c as u32)),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

/// What an analyzer would have written of the run image: no previous image, nothing cached.
fn analyzed(request: &Request) -> String {
    format!(
        "[run-image]\n  reference = {}\n  image = {}\n  [run-image.target]\n    os = {}\n    arch = {}\n",
        quoted(&request.run_image),
        quoted(&request.run_image_name),
        quoted(&request.os),
        quoted(&request.arch),
    )
}

/// Runs the lifecycle's `phase` in the root, as the builder's user, its output to `log`.
async fn run(
    phase: &str,
    builder: &Builder,
    platform_api: &str,
    log: &Arc<Log>,
) -> io::Result<std::process::ExitStatus> {
    let mut command = std::process::Command::new(format!("{LIFECYCLE}/{phase}"));
    command
        .env_clear()
        .envs(builder.env.iter().filter_map(|pair| pair.split_once('=')))
        .env("CNB_PLATFORM_API", platform_api)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (uid, gid) = (builder.uid, builder.gid);
    let (root, workspace) = (CString::new(ROOT)?, CString::new(format!("/{WORKSPACE}"))?);
    // SAFETY: chroot, chdir and the set*id calls are async-signal-safe; the program is
    // looked up after them, in the root
    unsafe {
        command.pre_exec(move || {
            if libc::chroot(root.as_ptr()) != 0
                || libc::chdir(workspace.as_ptr()) != 0
                || libc::setgroups(0, std::ptr::null()) != 0
                || libc::setgid(gid) != 0
                || libc::setuid(uid) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let (mut child, status) = crate::REAPER
        .spawn(&mut command)
        .map_err(|e| io::Error::other(format!("{LIFECYCLE}/{phase}: {e}")))?;
    crate::capture(&mut child, log);
    status
        .await
        .map_err(|_| io::Error::other(format!("{phase}: no exit status")))
}

/// Whether the layer's `.toml` says it is a launch layer (`[types]`, or at the top as
/// buildpack APIs before 0.6 had it).
fn launched(toml: &str) -> bool {
    let Ok(table) = toml.parse::<toml::Table>() else {
        return false;
    };
    let launch = |table: &toml::Table| table.get("launch").and_then(toml::Value::as_bool);
    table
        .get("types")
        .and_then(toml::Value::as_table)
        .and_then(launch)
        .or_else(|| launch(&table))
        .unwrap_or(false)
}

/// The process types of `config/metadata.toml`, and the default one.
fn processes(metadata: &str) -> io::Result<(Vec<String>, Option<String>)> {
    let table = metadata
        .parse::<toml::Table>()
        .map_err(|e| io::Error::other(format!("config/metadata.toml: {e}")))?;
    let types = table
        .get("processes")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|process| process.get("type")?.as_str())
        // a link's name: what the launcher's own checks allow, no path
        .filter(|kind| !kind.is_empty() && !kind.contains(['/', '\0']) && !kind.starts_with('.'))
        .map(String::from)
        .collect::<Vec<_>>();
    let default = table
        .get("buildpack-default-process-type")
        .and_then(toml::Value::as_str)
        .filter(|default| types.iter().any(|kind| kind == default))
        .map(String::from);
    Ok((types, default))
}

/// A header of the droplet's own making: root's, of no time.
fn header(kind: tar::EntryType, mode: u32) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(kind);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(0);
    header
}

/// Writes the droplet of the build in `root` to `out`.
fn pack(root: &Path, out: &Path) -> io::Result<Outcome> {
    let layers = root.join(LAYERS);
    let metadata = std::fs::read_to_string(layers.join("config/metadata.toml"))
        .map_err(|e| io::Error::other(format!("config/metadata.toml: {e}")))?;
    let (types, default) = processes(&metadata)?;
    if types.is_empty() {
        return Err(io::Error::other(
            "the buildpacks named no process to run (config/metadata.toml)",
        ));
    }
    let mut tar = tar::Builder::new(io::BufWriter::new(std::fs::File::create(out)?));
    tar.follow_symlinks(false);
    let named = |path: &Path| -> PathBuf {
        path.strip_prefix(root)
            .expect("below the root")
            .to_path_buf()
    };
    tar.append_dir(LAYERS, &layers)?;
    let config = layers.join("config");
    tar.append_dir(named(&config), &config)?;
    let file = config.join("metadata.toml");
    tar.append_path_with_name(&file, named(&file))?;
    let mut buildpacks: Vec<PathBuf> = std::fs::read_dir(&layers)?
        .filter_map(|entry| Some(entry.ok()?.path()))
        .filter(|path| path.is_dir() && !path.ends_with("config") && !path.ends_with("sbom"))
        .collect();
    buildpacks.sort();
    for buildpack in buildpacks {
        let mut tomls: Vec<PathBuf> = std::fs::read_dir(&buildpack)?
            .filter_map(|entry| Some(entry.ok()?.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "toml") && path.is_file())
            .filter(|path| std::fs::read_to_string(path).is_ok_and(|toml| launched(&toml)))
            .collect();
        tomls.sort();
        if tomls.is_empty() {
            continue;
        }
        tar.append_dir(named(&buildpack), &buildpack)?;
        for toml in tomls {
            tar.append_path_with_name(&toml, named(&toml))?;
            let layer = toml.with_extension("");
            if std::fs::symlink_metadata(&layer).is_ok_and(|layer| layer.is_dir()) {
                tar.append_dir_all(named(&layer), &layer)?;
            }
        }
    }
    let sbom = layers.join("sbom");
    let launch_sbom = sbom.join("launch");
    if launch_sbom.is_dir() {
        tar.append_dir(named(&sbom), &sbom)?;
        tar.append_dir_all(named(&launch_sbom), &launch_sbom)?;
    }
    tar.append_dir_all(WORKSPACE, root.join(WORKSPACE))?;
    let lifecycle = root.join(LIFECYCLE.trim_start_matches('/'));
    let cnb = lifecycle.parent().expect("below /cnb");
    tar.append_dir(named(cnb), cnb)?;
    tar.append_dir(named(&lifecycle), &lifecycle)?;
    let launcher = lifecycle.join("launcher");
    if std::fs::metadata(&launcher)?.mode() & 0o111 == 0 {
        return Err(io::Error::other(format!(
            "{LIFECYCLE}/launcher is no program"
        )));
    }
    tar.append_path_with_name(&launcher, named(&launcher))?;
    tar.append_data(
        &mut header(tar::EntryType::Directory, 0o755),
        "cnb/process",
        io::empty(),
    )?;
    for kind in &types {
        tar.append_link(
            &mut header(tar::EntryType::Symlink, 0o777),
            format!("cnb/process/{kind}"),
            format!("{LIFECYCLE}/launcher"),
        )?;
    }
    tar.into_inner()?.flush()?;
    Ok(Outcome {
        error: None,
        processes: types,
        default,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the lifecycle's files say: a launch layer by either form, the process types
    /// that can name a link, the default only when it is one of them; and what `/init`
    /// writes in the analyzer's place parses as TOML.
    #[test]
    fn the_lifecycle_files_read_as_written() {
        assert!(launched("[types]\n  launch = true\n  cache = true\n"));
        assert!(launched("launch = true\n"));
        assert!(!launched("[types]\n  build = true\n  launch = false\n"));
        assert!(!launched("[metadata]\n  launch = true\n"));
        assert!(!launched("not toml ["));

        let metadata = "buildpack-default-process-type = \"web\"\n\
            [[processes]]\n  type = \"web\"\n  command = [\"java\"]\n\
            [[processes]]\n  type = \"../up\"\n\
            [[processes]]\n  type = \"task\"\n";
        assert_eq!(
            processes(metadata).unwrap(),
            (vec!["web".to_string(), "task".into()], Some("web".into()))
        );
        let other = "buildpack-default-process-type = \"gone\"\n[[processes]]\n  type = \"web\"\n";
        assert_eq!(processes(other).unwrap().1, None);

        let request = Request {
            env: BTreeMap::new(),
            platform_api: "0.13".into(),
            run_image: "example.com/run@sha256:ab".into(),
            run_image_name: "example.com/\"run\\:latest".into(),
            os: "linux".into(),
            arch: "arm64".into(),
        };
        let analyzed = analyzed(&request).parse::<toml::Table>().unwrap();
        let run = analyzed["run-image"].as_table().unwrap();
        assert_eq!(run["reference"].as_str(), Some("example.com/run@sha256:ab"));
        assert_eq!(run["image"].as_str(), Some("example.com/\"run\\:latest"));
        assert_eq!(run["target"]["arch"].as_str(), Some("arm64"));
    }
}
