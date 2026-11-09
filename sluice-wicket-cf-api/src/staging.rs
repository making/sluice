//! A build: the package's zip is read and the component picked -- the one the
//! web process command names, else the package's only `.wasm` -- or a docker
//! package's image pulled (`crate::registry`), and it must be a component
//! (`\0asm`, layer 1). Anything else fails the build, naming what was found.
//! The droplet is the component, by its digest. On a Firecracker wicket the
//! droplet is a rootfs: an `.ext4` of the zip, or the wicket's make of the
//! image, which it keeps (the cf-api the image's pinned reference, to have it
//! made again).
//!
//! A zip of no component with an `index.html` at its root is a static site on a
//! wasm wicket: the droplet is the component of sluice-wicket-site ([`SITE`],
//! embedded at the build) and a tar of the zip's files, the
//! wicket's read-only at `/` for it ([`pack_site`]): dot files (but
//! `.well-known/`) and the manifest are left out, so a `.git`, a `.env` or the
//! env of a `manifest.yml` never reaches the wicket.

use std::io::{Read, Seek, Write};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::controller::Controller;
use crate::logs::Logs;
use crate::registry::Registry;
use crate::store::{DockerImage, Droplet, Files, Package, Store, guid, now};
use crate::wicket::Runtime;

/// A rootfs's file name ends so; a droplet's kind is its file's.
pub const ROOTFS: &str = ".ext4";

/// The component a static site is served by: sluice-wicket-site's (`build.rs`).
pub const SITE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/site.wasm"));

/// Its file name in the wicket.
pub const SITE_FILE: &str = "site.wasm";

/// What tells a zip of a static site: its page at the root.
const SITE_INDEX: &str = "index.html";

/// What a site's files may unpack to, and how many: the wicket's bounds of a tar.
const MAX_SITE_BYTES: u64 = 1 << 30;
const MAX_SITE_FILES: usize = 100_000;

/// What a package's component may unpack to: the largest image a wicket takes. A zip takes
/// any size down to next to nothing.
const MAX_DROPLET_BYTES: u64 = 4 << 30;

/// What a component is told by, before it is unpacked: the superblock's magic of a rootfs.
const HEAD: usize = MAGIC_AT + MAGIC.len();

/// Where the ext2/3/4 superblock's magic is, and what it is (little-endian 0xEF53).
const MAGIC_AT: usize = 1024 + 56;
const MAGIC: [u8; 2] = [0x53, 0xEF];

/// The component a build picked.
#[derive(Debug)]
pub struct Staged {
    /// Its path in the package; its file name for an image.
    pub entry: String,
    /// A component pulled, to keep; empty when kept already (a zip's), or when the wicket
    /// keeps it (a rootfs it made).
    pub bytes: Vec<u8>,
    pub size: u64,
    pub sha256: String,
    /// The image it was pulled from, pinned to its digest.
    pub image: Option<String>,
    /// A static site's files, kept already.
    pub files: Option<Files>,
}

/// Stages the build `build` of `package` and records how it went: STAGED
/// with its droplet, or FAILED naming why. A zip's component is the one
/// `command` names, else its only `.wasm` ([`stage`]); an image's is pulled
/// from its registry with the package's credentials, if any.
pub async fn run(
    store: &Arc<Store>,
    logs: &Logs,
    registry: &Registry,
    controller: &Controller,
    build: &str,
    package: &Package,
    command: Option<&str>,
) {
    let app = package.app.as_str();
    let staged = match &package.docker {
        None => {
            let runtime = controller.runtime().await;
            logs.add(
                app,
                "STG",
                match runtime {
                    Ok(Runtime::Firecracker) => {
                        "Staging a rootfs: the web process command names the image, else the package's only .ext4"
                    }
                    _ => {
                        "Staging with the wasm lifecycle: the web process command names the component, else the package's only .wasm, else an index.html makes it a static site"
                    }
                },
            );
            let site = !matches!(runtime, Ok(Runtime::Firecracker));
            unpack(store.clone(), &package.guid, command, site).await
        }
        Some(docker) => match controller.runtime().await {
            Ok(Runtime::Wasm) => pull(logs, registry, app, docker).await,
            Ok(Runtime::Firecracker) => {
                logs.add(
                    app,
                    "STG",
                    format!("The wicket is making a rootfs of {}", docker.image),
                );
                controller
                    .make_image(&docker.image, docker.credentials())
                    .await
                    .map(|made| Staged {
                        entry: made.name,
                        bytes: Vec::new(),
                        size: made.bytes,
                        sha256: made.sha256,
                        image: Some(made.image),
                        files: None,
                    })
                    .map_err(|e| format!("{}: {e}", docker.image))
            }
            Err(e) => Err(format!("what the image is staged as: {e}")),
        },
    };
    let staged = staged.and_then(|staged| {
        if staged.bytes.is_empty() {
            return Ok(staged);
        }
        store
            .write_droplet(&staged.sha256, &staged.bytes)
            .map(|()| staged)
    });
    let at = now();
    match staged {
        Ok(staged) => {
            logs.add(
                app,
                "STG",
                format!(
                    "Staged {} ({} bytes, sha256 {}){}{}",
                    staged.entry,
                    staged.size,
                    &staged.sha256[..12],
                    staged
                        .image
                        .as_ref()
                        .map_or(String::new(), |image| format!(" from {image}")),
                    staged.files.as_ref().map_or(String::new(), |files| format!(
                        ", serving the files of a {} byte tar (sha256 {})",
                        files.bytes,
                        &files.sha256[..12]
                    ))
                ),
            );
            let droplet = Droplet {
                guid: guid(),
                app: app.to_string(),
                package: package.guid.clone(),
                sha256: staged.sha256,
                file: staged.entry,
                bytes: staged.size,
                image: staged.image,
                files: staged.files,
                created_at: at.clone(),
                updated_at: at.clone(),
            };
            store.update(|state| {
                if let Some(staging) = state.builds.iter_mut().find(|kept| kept.guid == build) {
                    staging.state = "STAGED".into();
                    staging.droplet = Some(droplet.guid.clone());
                    staging.updated_at = at;
                }
                state.droplets.push(droplet);
            });
        }
        Err(e) => {
            logs.add(app, "STG", format!("Staging failed: {e}"));
            store.update(|state| {
                if let Some(staging) = state.builds.iter_mut().find(|kept| kept.guid == build) {
                    staging.state = "FAILED".into();
                    staging.error = Some(format!("StagingError - {e}"));
                    staging.updated_at = at;
                }
            });
        }
    }
}

/// Stages the zip of the package `guid`, its droplet made, on a blocking thread: a VM of one
/// vCPU has one worker, which the API and the watch run on. With `site`, a zip of a static
/// site is one ([`is_site`]).
async fn unpack(
    store: Arc<Store>,
    guid: &str,
    command: Option<&str>,
    site: bool,
) -> Result<Staged, String> {
    let (guid, command) = (guid.to_string(), command.map(str::to_string));
    tokio::task::spawn_blocking(move || {
        let open = || {
            std::fs::File::open(store.package_path(&guid))
                .map_err(|e| format!("the package is gone: {e}"))
        };
        if site && command.is_none() {
            let mut archive =
                zip::ZipArchive::new(open()?).map_err(|e| format!("not a zip: {e}"))?;
            if is_site(&archive) {
                return stage_site(&store, &mut archive);
            }
        }
        let zip = open()?;
        store.make_droplet(|droplet| {
            let staged = stage(zip, command.as_deref(), droplet, MAX_DROPLET_BYTES)?;
            let sha256 = staged.sha256.clone();
            Ok((staged, sha256))
        })
    })
    .await
    .map_err(|e| format!("staging: {e}"))?
}

/// Whether `archive` is a static site: no component, nor a rootfs, an `index.html` at its
/// root.
fn is_site<R: Read + Seek>(archive: &zip::ZipArchive<R>) -> bool {
    let names: Vec<_> = archive.file_names().filter_map(|name| name.ok()).collect();
    let component = |name: &str| name.ends_with(".wasm") || name.ends_with(ROOTFS);
    !names.iter().any(|name| component(name)) && names.iter().any(|name| name == SITE_INDEX)
}

/// Stages the static site `archive`: its files packed, sluice-wicket-site its component.
fn stage_site<R: Read + Seek>(
    store: &Store,
    archive: &mut zip::ZipArchive<R>,
) -> Result<Staged, String> {
    let packed = store.make_files(|tar| {
        let packed = pack_site(archive, tar, MAX_SITE_BYTES)?;
        let sha256 = packed.sha256.clone();
        Ok((packed, sha256))
    })?;
    Ok(Staged {
        entry: SITE_FILE.into(),
        bytes: SITE.to_vec(),
        size: SITE.len() as u64,
        sha256: sha256(SITE),
        image: None,
        files: Some(Files {
            sha256: packed.sha256,
            bytes: packed.bytes,
        }),
    })
}

/// A static site's files as [`pack_site`] packed them.
#[derive(Debug)]
pub struct Packed {
    /// The tar's digest and size.
    pub sha256: String,
    pub bytes: u64,
    /// The files packed, and those left out (dot files, the manifest).
    pub files: usize,
    pub left_out: Vec<String>,
}

/// Writes the files of the zip `archive` to `tar` as the wicket takes them: regular
/// files, sorted by name (the same files make the same digest, however zipped), each with
/// its time (what sluice-wicket-site's `ETag` is made of), `max` bytes of them at most. Dot
/// files and directories (but `.well-known/` at the root) and the manifest at the root
/// are left out; a link fails it, as does a name out of the root.
pub fn pack_site<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    tar: &mut impl Write,
    max: u64,
) -> Result<Packed, String> {
    let mut entries: Vec<(String, usize)> = Vec::new();
    let mut left_out = Vec::new();
    for index in 0..archive.len() {
        let file = archive
            .by_index_raw(index)
            .map_err(|e| format!("not a zip: {e}"))?;
        if file.is_dir() {
            continue;
        }
        let name = file
            .name()
            .map_err(|e| format!("not a zip: {e}"))?
            .to_string();
        if file.is_symlink() {
            return Err(format!(
                "'{name}' is a symbolic link: a site holds files only"
            ));
        }
        let Some(path) = file.enclosed_name() else {
            return Err(format!("'{name}' is no path below the site's root"));
        };
        let path: Vec<String> = path
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect();
        let path = path.join("/");
        if left_out_of_site(&path) {
            left_out.push(path);
            continue;
        }
        entries.push((path, index));
    }
    if entries.len() > MAX_SITE_FILES {
        return Err(format!("more than {MAX_SITE_FILES} files"));
    }
    entries.sort();
    if let Some(twice) = entries.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(format!("'{}' twice", twice[0].0));
    }
    let mut builder = tar::Builder::new(Hashed::new(Counted::new(tar)));
    let mut left = max;
    for (path, index) in &entries {
        let mut file = archive
            .by_index(*index)
            .map_err(|e| format!("cannot read {path}: {e}"))?;
        let size = file.size();
        if size > left {
            return Err(format!("the files are more than {} MiB", max >> 20));
        }
        left -= size;
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(size);
        header.set_mode(0o644);
        header.set_mtime(file.last_modified().map_or(0, unix_time));
        builder
            .append_data(&mut header, path, &mut file)
            .map_err(|e| format!("cannot pack {path}: {e}"))?;
    }
    let hashed = builder
        .into_inner()
        .map_err(|e| format!("cannot pack the site: {e}"))?;
    let bytes = hashed.out.written;
    Ok(Packed {
        sha256: hashed.finish(),
        bytes,
        files: entries.len(),
        left_out,
    })
}

/// Whether the site's file `path` is left out: a dot file or directory (but
/// `.well-known/` at the root), what `cf push` reads at the root (the manifest).
fn left_out_of_site(path: &str) -> bool {
    let mut segments = path.split('/');
    let first = segments.next().unwrap_or_default();
    let manifest = matches!(path, "manifest.yml" | "manifest.yaml");
    let hidden = (first.starts_with('.') && first != ".well-known")
        || segments.any(|segment| segment.starts_with('.'));
    manifest || hidden
}

/// A zip time (local, as MS-DOS has it: taken as UTC) in seconds since the epoch.
fn unix_time(time: zip::DateTime) -> u64 {
    // days from the civil date (Howard Hinnant's days_from_civil)
    let (year, month, day) = (
        i64::from(time.year()),
        i64::from(time.month()),
        i64::from(time.day()),
    );
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let of_era = year - era * 400;
    let of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let of_era_days = of_era * 365 + of_era / 4 - of_era / 100 + of_year;
    let days = era * 146_097 + of_era_days - 719_468;
    let seconds = days * 86_400
        + i64::from(time.hour()) * 3600
        + i64::from(time.minute()) * 60
        + i64::from(time.second());
    u64::try_from(seconds).unwrap_or(0)
}

/// A writer counting what passes it.
pub struct Counted<W> {
    out: W,
    written: u64,
}

impl<W: Write> Counted<W> {
    fn new(out: W) -> Counted<W> {
        Counted { out, written: 0 }
    }
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.out.write(bytes)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// The component of `docker`'s image, pulled from its registry with the package's
/// credentials, if any.
async fn pull(
    logs: &Logs,
    registry: &Registry,
    app: &str,
    docker: &DockerImage,
) -> Result<Staged, String> {
    let credentials = docker.credentials();
    logs.add(
        app,
        "STG",
        format!(
            "Staging with the wasm lifecycle: pulling {}{}",
            docker.image,
            if credentials.is_some() {
                " with the package's credentials"
            } else {
                ""
            }
        ),
    );
    registry
        .pull(&docker.image, credentials)
        .await
        .map(|pulled| Staged {
            entry: pulled.file,
            size: pulled.bytes.len() as u64,
            bytes: pulled.bytes,
            sha256: pulled.sha256,
            image: Some(pulled.image),
            files: None,
        })
        .map_err(|e| format!("{}: {e}", docker.image))
}

/// Stages `package` (a zip) for the web process `command`: the component unpacked to
/// `droplet` as it comes, `max` bytes at most, not a process's memory.
pub fn stage(
    package: impl Read + Seek,
    command: Option<&str>,
    droplet: &mut impl Write,
    max: u64,
) -> Result<Staged, String> {
    let mut archive = zip::ZipArchive::new(package).map_err(|e| format!("not a zip: {e}"))?;
    let names: Vec<String> = archive
        .file_names()
        .filter_map(|name| name.ok())
        .filter(|name| !name.ends_with('/'))
        .map(String::from)
        .collect();
    let components: Vec<&String> = names
        .iter()
        .filter(|name| name.ends_with(".wasm") || name.ends_with(ROOTFS))
        .collect();
    let entry = match command.map(str::trim).filter(|command| !command.is_empty()) {
        Some(command) => names
            .iter()
            .find(|name| *name == command || name.rsplit('/').next() == Some(command))
            .ok_or_else(|| {
                format!(
                    "the command names '{command}', which the package does not hold (found: {})",
                    listed(&names)
                )
            })?
            .clone(),
        None => match components.as_slice() {
            [only] => (*only).clone(),
            [] => {
                return Err(format!(
                    "the package holds no .wasm component, nor an .ext4 rootfs (found: {})",
                    listed(&names)
                ));
            }
            several => {
                return Err(format!(
                    "the package holds {} components ({}): name one with -c (the web process command)",
                    several.len(),
                    listed(several)
                ));
            }
        },
    };
    let unread = |e: std::io::Error| format!("cannot read {entry}: {e}");
    let mut file = archive
        .by_name(&entry)
        .map_err(|e| format!("cannot read {entry}: {e}"))?;
    let mut head = Vec::with_capacity(HEAD);
    (&mut file)
        .take(HEAD as u64)
        .read_to_end(&mut head)
        .map_err(unread)?;
    match entry.ends_with(ROOTFS) {
        true => check_rootfs(&entry, &head)?,
        false => check_component(&entry, &head)?,
    }
    let unwritten = |e: std::io::Error| format!("writing {entry}: {e}");
    let mut hashed = Hashed::new(droplet);
    hashed.write_all(&head).map_err(unwritten)?;
    let mut size = head.len() as u64;
    let mut chunk = vec![0; 1 << 20];
    loop {
        let read = file.read(&mut chunk).map_err(unread)?;
        if read == 0 {
            break;
        }
        size += read as u64;
        if size > max {
            return Err(format!("{entry} unpacks to more than {} MiB", max >> 20));
        }
        hashed.write_all(&chunk[..read]).map_err(unwritten)?;
    }
    drop(file);
    Ok(Staged {
        sha256: hashed.finish(),
        size,
        entry,
        bytes: Vec::new(),
        image: None,
        files: None,
    })
}

/// A writer keeping the digest of what passes it.
pub struct Hashed<W> {
    out: W,
    hasher: Sha256,
}

impl<W: Write> Hashed<W> {
    pub fn new(out: W) -> Hashed<W> {
        Hashed {
            out,
            hasher: Sha256::new(),
        }
    }

    /// The digest, in hex.
    pub fn finish(self) -> String {
        hex(&self.hasher.finalize())
    }
}

impl<W: Write> Write for Hashed<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.out.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// The SHA-256 of `bytes`, in hex.
pub fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// `digest` in hex.
fn hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A wasm component: the magic, then the version and the layer (1; a core
/// module is layer 0).
pub(crate) fn check_component(entry: &str, bytes: &[u8]) -> Result<(), String> {
    match bytes {
        [0, b'a', b's', b'm', _, _, 1, 0, ..] => Ok(()),
        [0, b'a', b's', b'm', 1, 0, 0, 0, ..] => Err(format!(
            "{entry} is a core wasm module, not a component (a wasi:http 0.3 handler)"
        )),
        [0, b'a', b's', b'm', ..] => Err(format!("{entry} is wasm of an unknown layer")),
        _ => Err(format!("{entry} is not wasm")),
    }
}

/// An ext2/3/4 image: its superblock's magic.
fn check_rootfs(entry: &str, bytes: &[u8]) -> Result<(), String> {
    match bytes.get(MAGIC_AT..MAGIC_AT + MAGIC.len()) {
        Some(magic) if magic == MAGIC => Ok(()),
        _ => Err(format!("{entry} is not an ext4 image")),
    }
}

fn listed<S: AsRef<str>>(names: &[S]) -> String {
    if names.is_empty() {
        return "nothing".into();
    }
    names
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    const COMPONENT: &[u8] = b"\0asm\x0d\x00\x01\x00rest";
    const MODULE: &[u8] = b"\0asm\x01\x00\x00\x00rest";

    fn zipped(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let time = zip::DateTime::from_date_and_time(2024, 5, 6, 7, 8, 10).unwrap();
        let options = zip::write::SimpleFileOptions::default().last_modified_time(time);
        for (name, bytes) in entries {
            zip.start_file(*name, options).unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    /// [`stage`] of `zip`, the droplet in memory; within `max` bytes.
    fn staged(zip: &[u8], command: Option<&str>, max: u64) -> Result<(Staged, Vec<u8>), String> {
        let mut droplet = Vec::new();
        stage(Cursor::new(zip), command, &mut droplet, max).map(|staged| (staged, droplet))
    }

    #[test]
    fn picks_the_component_or_packs_a_site() {
        let (hello, droplet) = staged(
            &zipped(&[("hello.wasm", COMPONENT), ("README", b"x")]),
            None,
            1 << 20,
        )
        .unwrap();
        assert_eq!(hello.entry, "hello.wasm");
        assert_eq!(droplet, COMPONENT);
        assert_eq!(hello.sha256, sha256(COMPONENT));
        assert_eq!(hello.size, COMPONENT.len() as u64);
        let two = zipped(&[("a.wasm", COMPONENT), ("dist/b.wasm", COMPONENT)]);
        let picked = |zip: &[u8], command| staged(zip, command, 1 << 20).map(|(staged, _)| staged);
        assert!(
            picked(&two, None)
                .unwrap_err()
                .contains("2 components (a.wasm, dist/b.wasm)")
        );
        assert_eq!(picked(&two, Some("b.wasm")).unwrap().entry, "dist/b.wasm");
        assert_eq!(
            picked(&two, Some("dist/b.wasm")).unwrap().entry,
            "dist/b.wasm"
        );
        assert!(
            picked(&two, Some("c.wasm"))
                .unwrap_err()
                .contains("names 'c.wasm'")
        );
        assert!(
            picked(&zipped(&[("index.html", b"<p>")]), None)
                .unwrap_err()
                .contains("no .wasm component, nor an .ext4 rootfs (found: index.html)")
        );
        let mut rootfs = vec![0; 2048];
        rootfs[MAGIC_AT..MAGIC_AT + 2].copy_from_slice(&MAGIC);
        let rootfs_zip = zipped(&[("app/r.ext4", &rootfs)]);
        let (picked_rootfs, unpacked) = staged(&rootfs_zip, None, 1 << 20).unwrap();
        assert_eq!(picked_rootfs.entry, "app/r.ext4");
        assert_eq!(unpacked, rootfs);
        // what unpacks past the bound, however small its zip
        assert!(
            staged(&rootfs_zip, None, 2047)
                .unwrap_err()
                .contains("app/r.ext4 unpacks to more than")
        );
        assert!(
            picked(&zipped(&[("r.ext4", b"not one")]), None)
                .unwrap_err()
                .contains("not an ext4 image")
        );
        assert!(
            picked(&zipped(&[("m.wasm", MODULE)]), None)
                .unwrap_err()
                .contains("core wasm module")
        );
        assert!(
            picked(&zipped(&[("t.wasm", b"text")]), None)
                .unwrap_err()
                .contains("not wasm")
        );
        assert!(
            picked(b"not a zip", None)
                .unwrap_err()
                .contains("not a zip")
        );

        // a static site: no component, an index.html at the root; its files packed sorted,
        // with their times, but the dot files and the manifest
        let archive = |zip: Vec<u8>| zip::ZipArchive::new(Cursor::new(zip)).unwrap();
        let entries = [
            ("index.html", &b"<p>"[..]),
            ("css/a.css", b"p{}"),
            (".git/config", b"[core]"),
            (".env", b"SECRET=x"),
            (".well-known/security.txt", b"Contact: me"),
            ("docs/.secret", b"x"),
            ("manifest.yml", b"env: {SECRET: x}"),
        ];
        let site = archive(zipped(&entries));
        assert!(is_site(&site));
        assert!(!is_site(&archive(zipped(&[
            ("index.html", b"<p>"),
            ("a.wasm", COMPONENT)
        ]))));
        assert!(!is_site(&archive(zipped(&[("docs/index.html", b"<p>")]))));
        let pack = |entries: &[(&str, &[u8])], max| {
            let mut tar = Vec::new();
            pack_site(&mut archive(zipped(entries)), &mut tar, max).map(|packed| (packed, tar))
        };
        let (packed, tar) = pack(&entries, 1 << 20).unwrap();
        assert_eq!(
            packed.left_out,
            [".git/config", ".env", "docs/.secret", "manifest.yml"]
        );
        assert_eq!((packed.files, packed.bytes), (3, tar.len() as u64));
        assert_eq!(packed.sha256, sha256(&tar));
        let mut unpacked = tar::Archive::new(&tar[..]);
        let files: Vec<(String, u64)> = unpacked
            .entries()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let name = entry.path().unwrap().to_string_lossy().into_owned();
                (name, entry.header().mtime().unwrap())
            })
            .collect();
        let at = 1_714_979_290;
        assert_eq!(
            files,
            [
                (".well-known/security.txt".to_string(), at),
                ("css/a.css".into(), at),
                ("index.html".into(), at)
            ]
        );
        let mut reversed = entries;
        reversed.reverse();
        assert_eq!(
            pack(&reversed, 1 << 20).unwrap().0.sha256,
            packed.sha256,
            "however zipped"
        );
        assert!(
            pack(&entries, 4)
                .unwrap_err()
                .contains("the files are more than")
        );
        let mut linked = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        linked.start_file("index.html", options).unwrap();
        linked
            .add_symlink("passwd", "/etc/passwd", options)
            .unwrap();
        let linked = linked.finish().unwrap().into_inner();
        let refused = pack_site(&mut archive(linked), &mut Vec::new(), 1 << 20).unwrap_err();
        assert!(refused.contains("'passwd' is a symbolic link"), "{refused}");
    }
}
