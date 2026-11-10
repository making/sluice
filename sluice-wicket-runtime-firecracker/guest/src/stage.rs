//! Stage mode (`wire::STAGE_PARAMETER`): the VM is a build's. Its rootfs is a Cloud Native
//! Buildpacks builder's, and /init is the platform of one build, served on the stage stream
//! (`wire::STAGE_PORT`):
//!
//! ```text
//! the source (a tar) -> /workspace
//! /cnb/lifecycle/detector, as the builder's user, of the order asked for, else the builder's
//! the cache (a tar, asked for once detected) -> /layers, the layers of the buildpacks detected
//! /cnb/lifecycle/builder, as the builder's user
//! /layers, /workspace, the launcher -> the droplet (a tar)
//! the cache=true layers of /layers -> the cache (a tar)
//! ```
//!
//! The caches are streamed, unpacked and packed as they pass: none is on the build's disk.
//!
//! No analyzer runs (no registry is in reach): /init writes the `analyzed.toml` naming the run
//! image. No restorer either: the cache is a tar of the last build's `cache = true` layers, each
//! with its `<layer>.toml` holding only its `metadata` (the types are the buildpack's to set
//! again) and its SBOM files, as the lifecycle's restorer leaves them; one of a buildpack not
//! detected is left out. No exporter either: the droplet is what the
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
use tokio::sync::mpsc;
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener, VsockStream};

use crate::log::Log;
use crate::wire;

/// The overlay's mount: the root the phases see.
const ROOT: &str = "/tmp/root";

/// The paths of the build, in that root (the lifecycle's defaults).
const LAYERS: &str = "layers";
const WORKSPACE: &str = "workspace";
const PLATFORM_ENV: &str = "platform/env";
/// The order asked for, which the detector is pointed at (`CNB_ORDER_PATH`).
const ORDER: &str = "platform/order.toml";
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
    /// The buildpacks detecting, in one group: none, the builder's order.
    #[serde(default)]
    order: Vec<Buildpack>,
    /// Whether the cache=true layers are sent back after the droplet.
    #[serde(default)]
    cache: bool,
    /// Whether the wicket has a cache to restore, which it sends when asked.
    #[serde(default)]
    restore: bool,
}

/// A buildpack of the order asked for.
#[derive(Deserialize)]
struct Buildpack {
    id: String,
    version: String,
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
    let mut keep = false;
    let outcome = match build(&mut stream, &builder, &log, &droplet, &mut keep).await {
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
        if keep {
            send_cache(&mut stream).await?;
        }
    }
    // the stream stays: its end is the VM's
    std::future::pending::<()>().await;
    Ok(())
}

/// The build of the request on `stream`: the droplet written to `droplet`, and `keep` told
/// whether the cache is to be sent after it.
async fn build(
    stream: &mut VsockStream,
    builder: &Builder,
    log: &Arc<Log>,
    droplet: &Path,
    keep: &mut bool,
) -> io::Result<Outcome> {
    let request = wire::read_json(stream, wire::MAX_ENV).await?;
    let request: Request = serde_json::from_slice(&request).map_err(io::Error::other)?;
    wire::answer_env(stream).await?;
    *keep = request.cache;
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
    let mut detector_env = Vec::new();
    if !request.order.is_empty() {
        std::fs::write(root.join(ORDER), order(&request.order))?;
        detector_env.push(("CNB_ORDER_PATH", format!("/{ORDER}")));
    }
    for phase in ["detector", "builder"] {
        if phase == "builder" && request.restore {
            let group = std::fs::read_to_string(layers.join("group.toml"))?;
            let restored = receive_cache(stream, &layers, detected(&group)?, uid, gid).await?;
            let (restored, failed) = match restored {
                Ok(restored) => (restored, None),
                Err((restored, e)) => (restored, Some(e)),
            };
            for layer in &restored {
                let line = format!("Restoring data for \"{layer}\" from cache");
                log.line(wire::Stream::Stdout, line.as_bytes());
            }
            if let Some(e) = failed {
                let line = format!("The cache was not restored: {e}");
                log.line(wire::Stream::Stdout, line.as_bytes());
                let layers = layers.clone();
                blocking(move || unrestore(&layers, &restored)).await?;
            }
        }
        let env = if phase == "detector" {
            &detector_env[..]
        } else {
            &[]
        };
        let status = run(phase, builder, &request.platform_api, env, log).await?;
        if !status.success() {
            return Err(io::Error::other(match (phase, status.code()) {
                ("detector", Some(20)) if request.order.is_empty() => {
                    "no buildpack of the builder detected the app".into()
                }
                ("detector", Some(20)) => "the buildpacks asked for did not detect the app".into(),
                _ => format!("the {phase} failed ({status})"),
            }));
        }
    }
    let droplet = droplet.to_path_buf();
    blocking(move || pack(root, &droplet)).await
}

/// A [`std::io::Read`] of the pieces a task receives: a blocking thread's.
struct Pieces {
    pieces: mpsc::Receiver<Vec<u8>>,
    piece: Vec<u8>,
    at: usize,
}

impl io::Read for Pieces {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.at == self.piece.len() {
            match self.pieces.blocking_recv() {
                Some(piece) => (self.piece, self.at) = (piece, 0),
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.piece.len() - self.at);
        buf[..n].copy_from_slice(&self.piece[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// A [`std::io::Write`] of pieces a task sends, [`PIECE`] bytes each but the last (the
/// rest, sent when flushed): a blocking thread's.
struct Piecing {
    pieces: mpsc::Sender<Vec<u8>>,
    piece: Vec<u8>,
}

impl io::Write for Piecing {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len().min(PIECE - self.piece.len());
        self.piece.extend_from_slice(&buf[..n]);
        if self.piece.len() == PIECE {
            self.flush()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.piece.is_empty() {
            return Ok(());
        }
        let piece = std::mem::replace(&mut self.piece, Vec::with_capacity(PIECE));
        self.pieces
            .blocking_send(piece)
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }
}

/// Asks the wicket for the cache and lays it over `layers` as it comes (`[len: u64
/// BE][bytes]`), every byte of it read: the layers restored, and if it failed, why (its
/// layers, those restored before, to remove).
#[allow(clippy::type_complexity)]
async fn receive_cache(
    stream: &mut VsockStream,
    layers: &Path,
    detected: Vec<String>,
    uid: u32,
    gid: u32,
) -> io::Result<Result<Vec<String>, (Vec<String>, io::Error)>> {
    wire::write_json(stream, br#"{"restore":true}"#).await?;
    let mut left = stream.read_u64().await?;
    let (pieces, received) = mpsc::channel(16);
    let layers = layers.to_path_buf();
    let unpacking = tokio::task::spawn_blocking(move || {
        let reader = Pieces {
            pieces: received,
            piece: Vec::new(),
            at: 0,
        };
        restore(reader, &layers, &detected, uid, gid)
    });
    let mut piece = vec![0u8; PIECE];
    while left > 0 {
        let wanted = piece.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let n = AsyncReadExt::read(stream, &mut piece[..wanted]).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        left -= n as u64;
        // one failed reads no more: the rest is read all the same
        let _ = pieces.send(piece[..n].to_vec()).await;
    }
    drop(pieces);
    unpacking.await.map_err(io::Error::other)
}

/// Packs the cache of the build's `/layers` and sends it as it is packed: pieces of `[len: u32
/// BE][bytes]`, then a length of 0 for a whole one, `u32::MAX` for one that failed.
async fn send_cache(stream: &mut VsockStream) -> io::Result<()> {
    let (pieces, mut sent) = mpsc::channel(16);
    let packing = tokio::task::spawn_blocking(move || {
        let mut out = Piecing {
            pieces,
            piece: Vec::with_capacity(PIECE),
        };
        pack_cache(&Path::new(ROOT).join(LAYERS), &mut out)?;
        io::Write::flush(&mut out)
    });
    while let Some(piece) = sent.recv().await {
        stream.write_u32(piece.len() as u32).await?;
        AsyncWriteExt::write_all(stream, &piece).await?;
    }
    let packed = packing.await.map_err(io::Error::other)?;
    stream
        .write_u32(if packed.is_ok() { 0 } else { u32::MAX })
        .await
}

/// The order of one group of `buildpacks`, as `order.toml` has it.
fn order(buildpacks: &[Buildpack]) -> String {
    let mut order = String::from("[[order]]\n");
    for buildpack in buildpacks {
        order.push_str(&format!(
            "  [[order.group]]\n    id = {}\n    version = {}\n",
            quoted(&buildpack.id),
            quoted(&buildpack.version)
        ));
    }
    order
}

/// The buildpacks of `group.toml`, by the names of their directories in `/layers`.
fn detected(group: &str) -> io::Result<Vec<String>> {
    let table = group
        .parse::<toml::Table>()
        .map_err(|e| io::Error::other(format!("group.toml: {e}")))?;
    let ids = table
        .get("group")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|buildpack| buildpack.get("id")?.as_str())
        .map(|id| id.replace('/', "_"))
        .collect();
    Ok(ids)
}

/// Lays the cache `tar` over `layers`: the layers of the buildpacks `detected` (by their
/// directories' names), the builder's user's (`uid`, `gid`). The layers restored, as
/// `<buildpack's directory>:<layer>`; else those and why not.
#[allow(clippy::type_complexity)]
fn restore(
    tar: impl io::Read,
    layers: &Path,
    detected: &[String],
    uid: u32,
    gid: u32,
) -> Result<Vec<String>, (Vec<String>, io::Error)> {
    let mut restored = Vec::new();
    let unpacked = (|| {
        let mut archive = tar::Archive::new(tar);
        archive.set_preserve_permissions(true);
        archive.set_preserve_mtime(true);
        archive.set_preserve_ownerships(true);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            let mut parts = path.components();
            let (Some(buildpack), Some(name)) = (parts.next(), parts.next()) else {
                continue;
            };
            let buildpack = buildpack.as_os_str().to_string_lossy().into_owned();
            if !detected.contains(&buildpack) {
                continue;
            }
            let dir = layers.join(&buildpack);
            if !dir.exists() {
                std::fs::create_dir(&dir)?;
                std::os::unix::fs::chown(&dir, Some(uid), Some(gid))?;
            }
            let name = name.as_os_str().to_string_lossy();
            let inside = parts.next().is_some() || entry.header().entry_type().is_dir();
            let layer = format!("{buildpack}:{}", layer_of(&name, inside));
            if restored.last() != Some(&layer) {
                restored.push(layer);
            }
            // a path out of /layers is not unpacked; one of another owner (a builder of
            // another user) is given to the builder's
            let header = entry.header();
            let owned = header.uid()? == u64::from(uid) && header.gid()? == u64::from(gid);
            if entry.unpack_in(layers)? && !owned {
                std::os::unix::fs::lchown(layers.join(&path), Some(uid), Some(gid))?;
            }
        }
        Ok(())
    })();
    match unpacked {
        Ok(()) => Ok(restored),
        Err(e) => Err((restored, e)),
    }
}

/// Removes the layers `restored` (`<buildpack's directory>:<layer>`) of `layers`: a cache
/// that failed.
fn unrestore(layers: &Path, restored: &[String]) -> io::Result<()> {
    for layer in restored {
        let (buildpack, layer) = layer.split_once(':').expect("<buildpack>:<layer>");
        let dir = layers.join(buildpack);
        let _ = std::fs::remove_dir_all(dir.join(layer));
        for file in std::fs::read_dir(&dir)? {
            let file = file?;
            let name = file.file_name();
            if layer_of(&name.to_string_lossy(), false) == layer {
                let _ = std::fs::remove_file(file.path());
            }
        }
    }
    Ok(())
}

/// The layer a name in a buildpack's directory is of: the layer's directory (`inside` it),
/// `<layer>.toml` or `<layer>.sbom.<ext>`.
fn layer_of(name: &str, inside: bool) -> &str {
    if inside {
        return name;
    }
    name.split_once(".sbom.")
        .map(|(layer, _)| layer)
        .or_else(|| name.strip_suffix(".toml"))
        .unwrap_or(name)
}

/// Writes the cache of `layers` to `out`: each buildpack's `cache = true` layers, the
/// `<layer>.toml` holding its `metadata` alone, and their SBOM files; nothing when none is
/// cached. The layers written.
fn pack_cache(layers: &Path, out: impl io::Write) -> io::Result<usize> {
    let mut cached = Vec::new();
    let mut buildpacks = layer_dirs(layers)?;
    buildpacks.sort();
    for buildpack in buildpacks {
        let mut tomls = tomls(&buildpack, |toml| typed(toml, "cache"))?;
        tomls.sort();
        for toml in tomls {
            let layer = toml.with_extension("");
            if std::fs::symlink_metadata(&layer).is_ok_and(|layer| layer.is_dir()) {
                cached.push((buildpack.clone(), toml, layer));
            }
        }
    }
    if cached.is_empty() {
        // no tar: a builder ends one when dropped
        return Ok(0);
    }
    let mut tar = tar::Builder::new(out);
    tar.follow_symlinks(false);
    for (buildpack, toml, layer) in &cached {
        let name = buildpack.file_name().expect("a directory's name");
        let stem = layer.file_name().expect("a layer's name");
        let named = Path::new(name).join(stem);
        let metadata = only_metadata(&std::fs::read_to_string(toml)?);
        // the buildpack's, as the file it is of
        let owner = std::fs::metadata(toml)?;
        let mut header = header(tar::EntryType::Regular, 0o644);
        header.set_uid(owner.uid().into());
        header.set_gid(owner.gid().into());
        header.set_size(metadata.len() as u64);
        tar.append_data(
            &mut header,
            named.with_extension("toml"),
            metadata.as_bytes(),
        )?;
        tar.append_dir_all(&named, layer)?;
        let sbom = format!("{}.sbom.", stem.to_string_lossy());
        for file in std::fs::read_dir(buildpack)? {
            let file = file?;
            if file.file_name().to_string_lossy().starts_with(&sbom) && file.path().is_file() {
                tar.append_path_with_name(file.path(), Path::new(name).join(file.file_name()))?;
            }
        }
    }
    tar.finish()?;
    Ok(cached.len())
}

/// The directories of the buildpacks in `layers`.
fn layer_dirs(layers: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(layers)?
        .filter_map(|entry| Some(entry.ok()?.path()))
        .filter(|path| path.is_dir() && !path.ends_with("config") && !path.ends_with("sbom"))
        .collect())
}

/// The layers' `.toml` files of the buildpack's directory `dir` that `wanted` takes.
fn tomls(dir: &Path, wanted: impl Fn(&str) -> bool) -> io::Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(|entry| Some(entry.ok()?.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml") && path.is_file())
        .filter(|path| std::fs::read_to_string(path).is_ok_and(|toml| wanted(&toml)))
        .collect())
}

/// A layer's `.toml` as the lifecycle restores it: its `metadata` table alone.
fn only_metadata(toml: &str) -> String {
    let mut kept = toml::Table::new();
    if let Some(metadata) = toml
        .parse::<toml::Table>()
        .ok()
        .and_then(|mut table| table.remove("metadata"))
    {
        kept.insert("metadata".into(), metadata);
    }
    kept.to_string()
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

/// Runs the lifecycle's `phase` in the root, as the builder's user, with `env` over the
/// builder's, its output to `log`.
async fn run(
    phase: &str,
    builder: &Builder,
    platform_api: &str,
    env: &[(&str, String)],
    log: &Arc<Log>,
) -> io::Result<std::process::ExitStatus> {
    let mut command = std::process::Command::new(format!("{LIFECYCLE}/{phase}"));
    command
        .env_clear()
        .envs(builder.env.iter().filter_map(|pair| pair.split_once('=')))
        .env("CNB_PLATFORM_API", platform_api)
        .envs(env.iter().map(|(name, value)| (name, value)))
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

/// Whether the layer's `.toml` says it is of the type `kind` (`launch`, `cache`): in
/// `[types]`, or at the top as buildpack APIs before 0.6 had it.
fn typed(toml: &str, kind: &str) -> bool {
    let Ok(table) = toml.parse::<toml::Table>() else {
        return false;
    };
    let of = |table: &toml::Table| table.get(kind).and_then(toml::Value::as_bool);
    table
        .get("types")
        .and_then(toml::Value::as_table)
        .and_then(of)
        .or_else(|| of(&table))
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
    let mut buildpacks = layer_dirs(&layers)?;
    buildpacks.sort();
    for buildpack in buildpacks {
        let mut tomls = tomls(&buildpack, |toml| typed(toml, "launch"))?;
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
        let layer = "[types]\n  launch = true\n  cache = true\n[metadata]\n  version = \"1\"\n";
        assert!(typed(layer, "launch") && typed(layer, "cache"));
        assert!(typed("launch = true\n", "launch"));
        assert!(!typed(
            "[types]\n  build = true\n  launch = false\n",
            "launch"
        ));
        assert!(!typed("[metadata]\n  launch = true\n", "launch"));
        assert!(!typed("not toml [", "cache"));
        // restored as the lifecycle's restorer writes it: the types the buildpack's to set
        let restored = only_metadata(layer).parse::<toml::Table>().unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored["metadata"]["version"].as_str(), Some("1"));
        assert_eq!(only_metadata("[types]\n  cache = true\n"), "");

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
            order: Vec::new(),
            cache: false,
            restore: false,
        };
        let analyzed = analyzed(&request).parse::<toml::Table>().unwrap();
        let run = analyzed["run-image"].as_table().unwrap();
        assert_eq!(run["reference"].as_str(), Some("example.com/run@sha256:ab"));
        assert_eq!(run["image"].as_str(), Some("example.com/\"run\\:latest"));
        assert_eq!(run["target"]["arch"].as_str(), Some("arm64"));

        let asked = [Buildpack {
            id: "paketo-buildpacks/java".into(),
            version: "22.8.0".into(),
        }];
        let order = order(&asked).parse::<toml::Table>().unwrap();
        let group = &order["order"][0]["group"][0];
        assert_eq!(group["id"].as_str(), Some("paketo-buildpacks/java"));
        assert_eq!(group["version"].as_str(), Some("22.8.0"));
    }

    /// The cache holds the cache=true layers alone, their `.toml` the metadata alone, their
    /// SBOM files; restored, of the buildpacks detected alone, given to the builder's user;
    /// one failing tells the layers to remove.
    #[test]
    fn the_cache_keeps_the_cache_layers_and_restores_those_detected() {
        let dir = std::env::temp_dir().join(format!("sluice-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let layers = dir.join("layers");
        let write = |path: &str, body: &str| {
            let path = layers.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        };
        let cached = "[types]\n  cache = true\n  build = true\n[metadata]\n  sha = \"1\"\n";
        write("a_bp/m2.toml", cached);
        write("a_bp/m2/repository/x.jar", "jar");
        write("a_bp/m2.sbom.syft.json", "{}");
        write("a_bp/jre.toml", "[types]\n  launch = true\n");
        write("a_bp/jre/bin/java", "java");
        write("other_bp/deps.toml", cached);
        write("other_bp/deps/lib", "lib");
        write("config/metadata.toml", "");
        let mut tar = Vec::new();
        assert_eq!(pack_cache(&layers, &mut tar).unwrap(), 2);

        std::fs::remove_dir_all(&layers).unwrap();
        std::fs::create_dir_all(&layers).unwrap();
        let group = "[[group]]\n  id = \"a/bp\"\n  version = \"1\"\n";
        let detected = detected(group).unwrap();
        assert_eq!(detected, ["a_bp"]);
        // SAFETY: plain getuid / getgid
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        let restored = restore(&tar[..], &layers, &detected, uid, gid).unwrap();
        assert_eq!(restored, ["a_bp:m2"]);
        let read = |path: &str| std::fs::read_to_string(layers.join(path)).ok();
        assert_eq!(read("a_bp/m2/repository/x.jar").as_deref(), Some("jar"));
        assert_eq!(read("a_bp/m2.sbom.syft.json").as_deref(), Some("{}"));
        assert_eq!(
            read("a_bp/m2.toml").as_deref(),
            Some("[metadata]\nsha = \"1\"\n")
        );
        assert_eq!(
            (read("a_bp/jre.toml"), read("other_bp/deps/lib")),
            (None, None)
        );

        // cut short: what was restored named, to remove
        let cut = &tar[..tar.len() - 1024 - 256];
        let (restored, _) = restore(cut, &layers, &detected, uid, gid).unwrap_err();
        assert_eq!(restored, ["a_bp:m2"]);
        unrestore(&layers, &restored).unwrap();
        assert_eq!(
            (read("a_bp/m2.toml"), read("a_bp/m2.sbom.syft.json")),
            (None, None)
        );
        assert!(!layers.join("a_bp/m2").exists());

        // nothing cached: no cache
        let mut tar = Vec::new();
        assert_eq!(pack_cache(&layers, &mut tar).unwrap(), 0);
        assert!(tar.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
