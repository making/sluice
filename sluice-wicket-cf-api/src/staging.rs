//! A build: the package's zip is read and the component picked -- the one the
//! web process command names, else the package's only `.wasm` -- or a docker
//! package's image pulled (`crate::registry`), and it must be a component
//! (`\0asm`, layer 1). Anything else fails the build, naming what was found.
//! The droplet is the component, by its digest. On a Firecracker wicket the
//! droplet is a rootfs: an `.ext4` of the zip, or the wicket's make of the
//! image, which it keeps (the cf-api the image's pinned reference, to have it
//! made again).

use std::io::{Read, Seek, Write};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::controller::Controller;
use crate::logs::Logs;
use crate::registry::Registry;
use crate::store::{DockerImage, Droplet, Package, Store, guid, now};
use crate::wicket::Runtime;

/// A rootfs's file name ends so; a droplet's kind is its file's.
pub const ROOTFS: &str = ".ext4";

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
            logs.add(
                app,
                "STG",
                match controller.runtime().await {
                    Ok(Runtime::Firecracker) => {
                        "Staging a rootfs: the web process command names the image, else the package's only .ext4"
                    }
                    _ => {
                        "Staging with the wasm lifecycle: the web process command names the component, else the package's only .wasm"
                    }
                },
            );
            unpack(store.clone(), &package.guid, command).await
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
                    .wicket
                    .make_image(&docker.image, docker.credentials())
                    .await
                    .map(|made| Staged {
                        entry: made.name,
                        bytes: Vec::new(),
                        size: made.bytes,
                        sha256: made.sha256,
                        image: Some(made.image),
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
                    "Staged {} ({} bytes, sha256 {}){}",
                    staged.entry,
                    staged.size,
                    &staged.sha256[..12],
                    staged
                        .image
                        .as_ref()
                        .map_or(String::new(), |image| format!(" from {image}"))
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
/// vCPU has one worker, which the API and the watch run on.
async fn unpack(store: Arc<Store>, guid: &str, command: Option<&str>) -> Result<Staged, String> {
    let (guid, command) = (guid.to_string(), command.map(str::to_string));
    tokio::task::spawn_blocking(move || {
        let zip = std::fs::File::open(store.package_path(&guid))
            .map_err(|e| format!("the package is gone: {e}"))?;
        store.make_droplet(|droplet| {
            let staged = stage(zip, command.as_deref(), droplet, MAX_DROPLET_BYTES)?;
            let sha256 = staged.sha256.clone();
            Ok((staged, sha256))
        })
    })
    .await
    .map_err(|e| format!("staging: {e}"))?
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
        for (name, bytes) in entries {
            zip.start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
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
    fn picks_the_only_component_or_the_one_the_command_names() {
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
    }
}
