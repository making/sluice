//! A build: the package's zip is read and the component picked -- the one the
//! web process command names, else the package's only `.wasm` -- or a docker
//! package's image pulled (`crate::registry`), and it must be a component
//! (`\0asm`, layer 1). Anything else fails the build, naming what was found.
//! The droplet is the component, by its digest.

use std::io::{Cursor, Read as _};

use sha2::{Digest, Sha256};

use crate::logs::Logs;
use crate::registry::Registry;
use crate::store::{Droplet, Package, Store, guid, now};

/// The component a build picked.
#[derive(Debug)]
pub struct Staged {
    /// Its path in the package; its file name for an image.
    pub entry: String,
    pub bytes: Vec<u8>,
    pub sha256: String,
    /// The image it was pulled from, pinned to its digest.
    pub image: Option<String>,
}

/// Stages the build `build` of `package` and records how it went: STAGED
/// with its droplet, or FAILED naming why. A zip's component is the one
/// `command` names, else its only `.wasm` ([`stage`]); an image's is pulled
/// from its registry with the package's credentials, if any.
pub async fn run(
    store: &Store,
    logs: &Logs,
    registry: &Registry,
    build: &str,
    package: &Package,
    command: Option<&str>,
) {
    let app = package.app.as_str();
    let staged = match &package.docker {
        None => {
            logs.add(app, "STG", "Staging with the wasm lifecycle: the web process command names the component, else the package's only .wasm");
            std::fs::read(store.package_path(&package.guid))
                .map_err(|e| format!("the package is gone: {e}"))
                .and_then(|zip| stage(&zip, command))
        }
        Some(docker) => {
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
                    bytes: pulled.bytes,
                    sha256: pulled.sha256,
                    image: Some(pulled.image),
                })
                .map_err(|e| format!("{}: {e}", docker.image))
        }
    };
    let staged = staged.and_then(|staged| {
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
                    staged.bytes.len(),
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
                bytes: staged.bytes.len() as u64,
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

/// Stages `package` (a zip) for the web process `command`.
pub fn stage(package: &[u8], command: Option<&str>) -> Result<Staged, String> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(package)).map_err(|e| format!("not a zip: {e}"))?;
    let names: Vec<String> = archive
        .file_names()
        .filter_map(|name| name.ok())
        .filter(|name| !name.ends_with('/'))
        .map(String::from)
        .collect();
    let components: Vec<&String> = names
        .iter()
        .filter(|name| name.ends_with(".wasm"))
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
                    "the package holds no .wasm component (found: {})",
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
    let mut bytes = Vec::new();
    archive
        .by_name(&entry)
        .and_then(|mut file| file.read_to_end(&mut bytes).map_err(Into::into))
        .map_err(|e| format!("cannot read {entry}: {e}"))?;
    check_component(&entry, &bytes)?;
    Ok(Staged {
        sha256: sha256(&bytes),
        entry,
        bytes,
        image: None,
    })
}

/// The SHA-256 of `bytes`, in hex.
pub fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
    use std::io::Write as _;

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

    #[test]
    fn picks_the_only_component_or_the_one_the_command_names() {
        let staged = stage(
            &zipped(&[("hello.wasm", COMPONENT), ("README", b"x")]),
            None,
        )
        .unwrap();
        assert_eq!(staged.entry, "hello.wasm");
        assert_eq!(staged.bytes, COMPONENT);
        assert_eq!(staged.sha256.len(), 64);
        let two = zipped(&[("a.wasm", COMPONENT), ("dist/b.wasm", COMPONENT)]);
        assert!(
            stage(&two, None)
                .unwrap_err()
                .contains("2 components (a.wasm, dist/b.wasm)")
        );
        assert_eq!(stage(&two, Some("b.wasm")).unwrap().entry, "dist/b.wasm");
        assert_eq!(
            stage(&two, Some("dist/b.wasm")).unwrap().entry,
            "dist/b.wasm"
        );
        assert!(
            stage(&two, Some("c.wasm"))
                .unwrap_err()
                .contains("names 'c.wasm'")
        );
        assert!(
            stage(&zipped(&[("index.html", b"<p>")]), None)
                .unwrap_err()
                .contains("no .wasm component (found: index.html)")
        );
        assert!(
            stage(&zipped(&[("m.wasm", MODULE)]), None)
                .unwrap_err()
                .contains("core wasm module")
        );
        assert!(
            stage(&zipped(&[("t.wasm", b"text")]), None)
                .unwrap_err()
                .contains("not wasm")
        );
        assert!(stage(b"not a zip", None).unwrap_err().contains("not a zip"));
    }
}
